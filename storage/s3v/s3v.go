package s3v

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/larrabee/ratelimit"
	"github.com/larrabee/s3sync/storage"
	"io"
	"net/url"
	"strings"
	"time"
)

// S3vStorage configuration.
type S3vStorage struct {
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

// NewS3vStorage return new configured S3 storage.
// You should always create new storage with this constructor.
//
// It differs from S3 storage in that it can work with file versions.
func NewS3vStorage(awsAccessKey, awsSecretKey, awsToken, awsRegion, endpoint, bucketName, prefix string, keysPerReq int64, retryCnt uint, retryInterval time.Duration) *S3vStorage {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	sess.Config.S3ForcePathStyle = aws.Bool(true)
	sess.Config.Region = aws.String(awsRegion)

	if awsAccessKey != "" || awsSecretKey != "" {
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

	st := S3vStorage{
		awsBucket:     &bucketName,
		awsSession:    sess,
		awsSvc:        s3.New(sess),
		prefix:        prefix,
		keysPerReq:    keysPerReq,
		retryCnt:      retryCnt,
		retryInterval: retryInterval,
		ctx:           context.TODO(),
		rlBucket:      ratelimit.NewFakeBucket(),
	}

	return &st
}

// WithContext add's context to storage.
func (st *S3vStorage) WithContext(ctx context.Context) {
	st.ctx = ctx
}

// WithRateLimit set rate limit (bytes/sec) for storage.
func (st *S3vStorage) WithRateLimit(limit int) error {
	bucket, err := ratelimit.NewBucketWithRate(float64(limit), int64(limit))
	if err != nil {
		return err
	}
	st.rlBucket = bucket
	return nil
}

// List S3 bucket and send founded objects versions to chan.
func (st *S3vStorage) List(output chan<- *storage.Object) error {
	listObjectsFn := func(p *s3.ListObjectVersionsOutput, lastPage bool) bool {
		for _, o := range p.Versions {
			key, _ := url.QueryUnescape(aws.StringValue(o.Key))
			key = strings.Replace(key, st.prefix, "", 1)
			key = strings.TrimPrefix(key, "/")
			output <- &storage.Object{
				Key:          &key,
				VersionId:    o.VersionId,
				ETag:         storage.StrongEtag(o.ETag),
				Mtime:        o.LastModified,
				IsLatest:     o.IsLatest,
				StorageClass: o.StorageClass,
			}
		}
		st.listMarker = p.VersionIdMarker
		return !lastPage // continue paging
	}

	for i := uint(0); ; i++ {
		input := &s3.ListObjectVersionsInput{
			Bucket:          st.awsBucket,
			Prefix:          aws.String(st.prefix),
			MaxKeys:         aws.Int64(st.keysPerReq),
			EncodingType:    aws.String(s3.EncodingTypeUrl),
			VersionIdMarker: st.listMarker,
		}
		err := st.awsSvc.ListObjectVersionsPagesWithContext(st.ctx, input, listObjectsFn)
		if err == nil {
			storage.Log.Debugf("Listing bucket finished")
			return nil
		} else if storage.IsAwsContextCanceled(err) {
			return err
		} else if (err != nil) && (i < st.retryCnt) {
			storage.Log.Debugf("S3 listing failed with error: %s", err)
			time.Sleep(st.retryInterval)
			continue
		} else if (err != nil) && (i == st.retryCnt) {
			storage.Log.Debugf("S3 listing failed with error: %s", err)
			return err
		}
	}
}

// PutObject saves object to S3.
// PutObject ignore VersionId, it always save object as latest version.
// PutObject saves object to S3.
func (st *S3vStorage) PutObject(obj *storage.Object) error {
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

	for i := uint(0); ; i++ {
		_, err := st.awsSvc.PutObjectWithContext(st.ctx, input)
		if storage.IsAwsContextCanceled(err) {
			return err
		} else if (err != nil) && (i < st.retryCnt) {
			storage.Log.Debugf("S3 obj uploading failed with error: %s", err)
			time.Sleep(st.retryInterval)
			continue
		} else if (err != nil) && (i == st.retryCnt) {
			return err
		}
	}

	return nil
}

// GetObjectContent read object content and metadata from S3.
func (st *S3vStorage) GetObjectContent(obj *storage.Object) error {
	input := &s3.GetObjectInput{
		Bucket:    st.awsBucket,
		Key:       aws.String(st.prefix + *obj.Key),
		VersionId: obj.VersionId,
	}

	for i := uint(0); ; i++ {
		result, err := st.awsSvc.GetObjectWithContext(st.ctx, input)
		if storage.IsAwsContextCanceled(err) {
			return err
		} else if (err != nil) && (i < st.retryCnt) {
			storage.Log.Debugf("S3 obj content downloading request failed with error: %s", err)
			time.Sleep(st.retryInterval)
			continue
		} else if (err != nil) && (i == st.retryCnt) {
			return err
		}

		buf := bytes.NewBuffer(make([]byte, 0, aws.Int64Value(result.ContentLength)))
		_, err = io.Copy(ratelimit.NewWriter(buf, st.rlBucket), result.Body)
		if storage.IsAwsContextCanceled(err) {
			return err
		} else if (err != nil) && (i < st.retryCnt) {
			storage.Log.Debugf("S3 obj content downloading failed with error: %s", err)
			time.Sleep(st.retryInterval)
			continue
		} else if (err != nil) && (i == st.retryCnt) {
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
}

// GetObjectMeta update object metadata from S3.
func (st *S3vStorage) GetObjectMeta(obj *storage.Object) error {
	input := &s3.HeadObjectInput{
		Bucket:    st.awsBucket,
		Key:       aws.String(st.prefix + *obj.Key),
		VersionId: obj.VersionId,
	}

	for i := uint(0); ; i++ {
		result, err := st.awsSvc.HeadObjectWithContext(st.ctx, input)
		if storage.IsAwsContextCanceled(err) {
			return err
		} else if (err != nil) && (i < st.retryCnt) {
			storage.Log.Debugf("S3 obj meta downloading request failed with error: %s", err)
			time.Sleep(st.retryInterval)
			continue
		} else if (err != nil) && (i == st.retryCnt) {
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
}

// DeleteObject remove object from S3.
func (st *S3vStorage) DeleteObject(obj *storage.Object) error {
	input := &s3.DeleteObjectInput{
		Bucket:    st.awsBucket,
		Key:       aws.String(st.prefix + *obj.Key),
		VersionId: obj.VersionId,
	}

	for i := uint(0); ; i++ {
		_, err := st.awsSvc.DeleteObjectWithContext(st.ctx, input)
		if err == nil {
			break
		} else if storage.IsAwsContextCanceled(err) {
			return err
		} else if (err != nil) && (i < st.retryCnt) {
			storage.Log.Debugf("S3 obj removing failed with error: %s", err)
			time.Sleep(st.retryInterval)
			continue
		} else if (err != nil) && (i == st.retryCnt) {
			return err
		}
	}
	return nil
}
