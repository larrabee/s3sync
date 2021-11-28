package s3

import (
	"bytes"
	"context"
	"crypto/tls"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/larrabee/ratelimit"
	"github.com/larrabee/s3sync/storage"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// S3Storage configuration.
type S3Storage struct {
	awsSvc        *s3.S3
	awsSession    *session.Session
	awsBucket     *string
	prefix        string
	keysPerReq    int64
	retryCnt      uint
	retryInterval time.Duration
	ctx           context.Context
	listMarker    *string
	rlBucket      ratelimit.Bucket
}

// NewS3Storage return new configured S3 storage.
//
// You should always create new storage with this constructor.
func NewS3Storage(awsNoSign bool, awsAccessKey, awsSecretKey, awsToken, awsRegion, endpoint, bucketName, prefix string, keysPerReq int64, retryCnt uint, retryDelay time.Duration, skipSSLVerify bool) *S3Storage {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	sess.Config.S3ForcePathStyle = aws.Bool(true)
	sess.Config.Region = aws.String(awsRegion)
	sess.Config.Retryer = &Retryer{RetryCnt: retryCnt, RetryDelay: retryDelay}

	if skipSSLVerify {
		tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
		sess.Config.HTTPClient = &http.Client{Transport: tr}
	}

	if awsNoSign {
		sess.Config.Credentials = credentials.AnonymousCredentials
	} else if awsAccessKey != "" || awsSecretKey != "" {
		sess.Config.Credentials = credentials.NewStaticCredentials(awsAccessKey, awsSecretKey, awsToken)
	} else if _, err := sess.Config.Credentials.Get(); err != nil {
		storage.Log.Debugf("Failed to load credentials from default config")
		cred := credentials.NewChainCredentials(
			[]credentials.Provider{
				&credentials.EnvProvider{},
				defaults.RemoteCredProvider(*defaults.Config(), defaults.Handlers()),
			})
		sess.Config.Credentials = cred
	}

	if endpoint != "" {
		sess.Config.Endpoint = aws.String(endpoint)
	}
	if aws.StringValue(sess.Config.Region) == "" {
		sess.Config.Region = aws.String("us-east-1")
	}

	st := S3Storage{
		awsBucket:     &bucketName,
		awsSession:    sess,
		awsSvc:        s3.New(sess),
		prefix:        prefix,
		keysPerReq:    keysPerReq,
		retryCnt:      retryCnt,
		retryInterval: retryDelay,
		ctx:           context.TODO(),
		rlBucket:      ratelimit.NewFakeBucket(),
	}

	return &st
}

// WithContext add's context to storage.
func (st *S3Storage) WithContext(ctx context.Context) {
	st.ctx = ctx
}

// WithRateLimit set rate limit (bytes/sec) for storage.
func (st *S3Storage) WithRateLimit(limit int) error {
	bucket, err := ratelimit.NewBucketWithRate(float64(limit), int64(limit))
	if err != nil {
		return err
	}
	st.rlBucket = bucket
	return nil
}

// List S3 bucket and send founded objects to chan.
func (st *S3Storage) List(output chan<- *storage.Object) error {
	listObjectsFn := func(p *s3.ListObjectsOutput, lastPage bool) bool {
		for _, o := range p.Contents {
			key, _ := url.QueryUnescape(aws.StringValue(o.Key))
			key = strings.Replace(key, st.prefix, "", 1)
			output <- &storage.Object{
				Key:          &key,
				ETag:         storage.StrongEtag(o.ETag),
				Mtime:        o.LastModified,
				StorageClass: o.StorageClass,
				IsLatest:     aws.Bool(true),
			}
		}
		st.listMarker = p.Marker
		return !lastPage // continue paging
	}

	input := &s3.ListObjectsInput{
		Bucket:       st.awsBucket,
		Prefix:       aws.String(st.prefix),
		MaxKeys:      aws.Int64(st.keysPerReq),
		EncodingType: aws.String(s3.EncodingTypeUrl),
		Marker:       st.listMarker,
	}

	if err := st.awsSvc.ListObjectsPagesWithContext(st.ctx, input, listObjectsFn); err != nil {
		return err
	}
	storage.Log.Debugf("Listing bucket finished")
	return nil

}

// PutObject saves object to S3.
// PutObject ignore VersionId, it always save object as latest version.
func (st *S3Storage) PutObject(obj *storage.Object) error {
	objReader := bytes.NewReader(*obj.Content)
	rlReader := ratelimit.NewReadSeeker(objReader, st.rlBucket)

	input := &s3.PutObjectInput{
		Bucket:             st.awsBucket,
		Key:                aws.String(st.prefix + *obj.Key),
		Body:               rlReader,
		ContentType:        obj.ContentType,
		ContentDisposition: obj.ContentDisposition,
		ContentEncoding:    obj.ContentEncoding,
		ContentLanguage:    obj.ContentLanguage,
		ACL:                obj.ACL,
		Metadata:           obj.Metadata,
		CacheControl:       obj.CacheControl,
		StorageClass:       obj.StorageClass,
	}

	if _, err := st.awsSvc.PutObjectWithContext(st.ctx, input); err != nil {
		return err
	}

	if obj.AccessControlPolicy != nil {
		inputAcl := &s3.PutObjectAclInput{
			Bucket:              st.awsBucket,
			Key:                 aws.String(st.prefix + *obj.Key),
			AccessControlPolicy: obj.AccessControlPolicy,
		}

		if _, err := st.awsSvc.PutObjectAclWithContext(st.ctx, inputAcl); err != nil {
			return err
		}
	}

	return nil
}

// GetObjectContent read object content and metadata from S3.
func (st *S3Storage) GetObjectContent(obj *storage.Object) error {
	input := &s3.GetObjectInput{
		Bucket:    st.awsBucket,
		Key:       aws.String(st.prefix + *obj.Key),
		VersionId: obj.VersionId,
	}

	result, err := st.awsSvc.GetObjectWithContext(st.ctx, input)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(make([]byte, 0, aws.Int64Value(result.ContentLength)))
	if _, err := io.Copy(ratelimit.NewWriter(buf, st.rlBucket), result.Body); err != nil {
		return err
	}

	data := buf.Bytes()
	obj.Content = &data
	obj.ContentType = result.ContentType
	obj.ContentDisposition = result.ContentDisposition
	obj.ContentEncoding = result.ContentEncoding
	obj.ContentLanguage = result.ContentLanguage
	obj.ETag = storage.StrongEtag(result.ETag)
	obj.Metadata = result.Metadata
	obj.Mtime = result.LastModified
	obj.CacheControl = result.CacheControl
	obj.StorageClass = result.StorageClass

	return nil
}

// GetObjectACL read object ACL from S3.
func (st *S3Storage) GetObjectACL(obj *storage.Object) error {
	input := &s3.GetObjectAclInput{
		Bucket:    st.awsBucket,
		Key:       aws.String(st.prefix + *obj.Key),
		VersionId: obj.VersionId,
	}

	result, err := st.awsSvc.GetObjectAclWithContext(st.ctx, input)
	if err != nil {
		return err
	}

	obj.AccessControlPolicy = &s3.AccessControlPolicy{
		Grants: result.Grants,
		Owner:  result.Owner,
	}

	return nil
}

// GetObjectMeta update object metadata from S3.
func (st *S3Storage) GetObjectMeta(obj *storage.Object) error {
	input := &s3.HeadObjectInput{
		Bucket:    st.awsBucket,
		Key:       aws.String(st.prefix + *obj.Key),
		VersionId: obj.VersionId,
	}

	result, err := st.awsSvc.HeadObjectWithContext(st.ctx, input)
	if err != nil {
		return err
	}

	obj.ContentType = result.ContentType
	obj.ContentDisposition = result.ContentDisposition
	obj.ContentEncoding = result.ContentEncoding
	obj.ContentLanguage = result.ContentLanguage
	obj.ETag = storage.StrongEtag(result.ETag)
	obj.Metadata = result.Metadata
	obj.Mtime = result.LastModified
	obj.CacheControl = result.CacheControl
	obj.StorageClass = result.StorageClass

	return nil
}

// DeleteObject remove object from S3.
func (st *S3Storage) DeleteObject(obj *storage.Object) error {
	input := &s3.DeleteObjectInput{
		Bucket:    st.awsBucket,
		Key:       aws.String(st.prefix + *obj.Key),
		VersionId: obj.VersionId,
	}

	if _, err := st.awsSvc.DeleteObjectWithContext(st.ctx, input); err != nil {
		return err
	}
	return nil
}
