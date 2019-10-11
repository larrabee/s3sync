package s3

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/larrabee/ratelimit"
	"github.com/larrabee/s3sync/storage"
	"io"
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
func NewS3Storage(awsAccessKey, awsSecretKey, awsRegion, endpoint, bucketName, prefix string, keysPerReq int64, retryCnt uint, retryInterval time.Duration) *S3Storage {
	sess := session.Must(session.NewSession())

	sess.Config.S3ForcePathStyle = aws.Bool(true)
	sess.Config.CredentialsChainVerboseErrors = aws.Bool(true)
	sess.Config.Region = aws.String(awsRegion)

	if awsAccessKey != "" && awsSecretKey != "" {
		cred := credentials.NewStaticCredentials(awsAccessKey, awsSecretKey, "")
		sess.Config.WithCredentials(cred)
	} else {
		cred := credentials.NewChainCredentials(
			[]credentials.Provider{
				&credentials.EnvProvider{},
				&credentials.SharedCredentialsProvider{},
				&ec2rolecreds.EC2RoleProvider{
					Client: ec2metadata.New(sess),
				},
			})
		sess.Config.WithCredentials(cred)
	}

	if endpoint != "" {
		sess.Config.Endpoint = aws.String(endpoint)
	}

	st := S3Storage{
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
			}
		}
		st.listMarker = p.Marker
		return !lastPage // continue paging
	}

	for i := uint(0); ; i++ {
		input := &s3.ListObjectsInput{
			Bucket:       st.awsBucket,
			Prefix:       aws.String(st.prefix),
			MaxKeys:      aws.Int64(st.keysPerReq),
			EncodingType: aws.String(s3.EncodingTypeUrl),
			Marker:       st.listMarker,
		}

		err := st.awsSvc.ListObjectsPagesWithContext(st.ctx, input, listObjectsFn)
		if (err != nil) && (i < st.retryCnt) {
			storage.Log.Debugf("S3 listing failed with error: %s", err)
			time.Sleep(st.retryInterval)
			continue
		} else if (err != nil) && (i == st.retryCnt) {
			storage.Log.Debugf("S3 listing failed with error: %s", err)
			return err
		} else {
			storage.Log.Debugf("Listing bucket finished")
			return err
		}
	}
}

// PutObject saves object to S3.
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

	for i := uint(0); ; i++ {
		_, err := st.awsSvc.PutObjectWithContext(st.ctx, input)
		if (err != nil) && (i < st.retryCnt) {
			storage.Log.Debugf("S3 obj uploading failed with error: %s", err)
			time.Sleep(st.retryInterval)
			continue
		} else if (err != nil) && (i == st.retryCnt) {
			return err
		}

		return nil
	}
}

// GetObjectContent read object content and metadata from S3.
func (st *S3Storage) GetObjectContent(obj *storage.Object) error {
	input := &s3.GetObjectInput{
		Bucket: st.awsBucket,
		Key:    aws.String(st.prefix + *obj.Key),
	}

	for i := uint(0); ; i++ {
		result, err := st.awsSvc.GetObjectWithContext(st.ctx, input)
		if (err != nil) && (i < st.retryCnt) {
			storage.Log.Debugf("S3 obj content downloading request failed with error: %s", err)
			time.Sleep(st.retryInterval)
			continue
		} else if (err != nil) && (i == st.retryCnt) {
			return err
		}

		buf := bytes.NewBuffer(make([]byte, 0, aws.Int64Value(result.ContentLength)))
		_, err = io.Copy(ratelimit.NewWriter(buf, st.rlBucket), result.Body)
		if (err != nil) && (i < st.retryCnt) {
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
func (st *S3Storage) GetObjectMeta(obj *storage.Object) error {
	input := &s3.HeadObjectInput{
		Bucket: st.awsBucket,
		Key:    aws.String(st.prefix + *obj.Key),
	}

	for i := uint(0); ; i++ {
		result, err := st.awsSvc.HeadObjectWithContext(st.ctx, input)
		if (err != nil) && (i < st.retryCnt) {
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
func (st *S3Storage) DeleteObject(obj *storage.Object) error {
	input := &s3.DeleteObjectInput{
		Bucket: st.awsBucket,
		Key:    aws.String(st.prefix + *obj.Key),
	}

	for i := uint(0); ; i++ {
		_, err := st.awsSvc.DeleteObjectWithContext(st.ctx, input)
		if (err != nil) && (i < st.retryCnt) {
			storage.Log.Debugf("S3 obj removing failed with error: %s", err)
			time.Sleep(st.retryInterval)
			continue
		} else if (err != nil) && (i == st.retryCnt) {
			return err
		}

		return nil
	}
}

// GetStorageType return storage type.
func (st *S3Storage) GetStorageType() storage.Type {
	return storage.TypeS3
}
