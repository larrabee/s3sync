package storage

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
	"io"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

// S3Storage configuration
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

// NewS3Storage return new configured S3 storage
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

	storage := S3Storage{
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

	return &storage
}

// WithContext add's context to storage
func (storage *S3Storage) WithContext(ctx context.Context) {
	storage.ctx = ctx
}

// WithRateLimit set rate limit (bytes/sec) for storage
func (storage *S3Storage) WithRateLimit(limit int) error {
	bucket, err := ratelimit.NewBucketWithRate(float64(limit), int64(limit))
	if err != nil {
		return err
	}
	storage.rlBucket = bucket
	return nil
}

// List S3 bucket and send founded objects to chan
func (storage *S3Storage) List(output chan<- *Object) error {
	listObjectsFn := func(p *s3.ListObjectsOutput, lastPage bool) bool {
		for _, o := range p.Contents {
			key, _ := url.QueryUnescape(aws.StringValue(o.Key))
			output <- &Object{Key: &key, ETag: strongEtag(o.ETag), Mtime: o.LastModified}
		}
		storage.listMarker = p.Marker
		return !lastPage // continue paging
	}

	for i := uint(0); ; i++ {
		input := &s3.ListObjectsInput{
			Bucket:       storage.awsBucket,
			Prefix:       aws.String(storage.prefix),
			MaxKeys:      aws.Int64(storage.keysPerReq),
			EncodingType: aws.String(s3.EncodingTypeUrl),
			Marker:       storage.listMarker,
		}
		err := storage.awsSvc.ListObjectsPagesWithContext(storage.ctx, input, listObjectsFn)
		if (err != nil) && (i < storage.retryCnt) {
			Log.Debugf("S3 listing failed with error: %s", err)
			time.Sleep(storage.retryInterval)
			continue
		} else if (err != nil) && (i == storage.retryCnt) {
			Log.Debugf("S3 listing failed with error: %s", err)
			return err
		} else {
			Log.Debugf("Listing bucket finished")
			return err
		}
	}
}

// PutObject saves object to S3
func (storage *S3Storage) PutObject(obj *Object) error {
	objReader := bytes.NewReader(*obj.Content)
	rlReader := ratelimit.NewReadSeeker(objReader, storage.rlBucket)

	input := &s3.PutObjectInput{
		Bucket:             storage.awsBucket,
		Key:                aws.String(filepath.Join(storage.prefix, *obj.Key)),
		Body:               rlReader,
		ContentType:        obj.ContentType,
		ContentDisposition: obj.ContentDisposition,
		ContentEncoding:    obj.ContentEncoding,
		ContentLanguage:    obj.ContentLanguage,
		ACL:                obj.ACL,
		Metadata:           obj.Metadata,
		CacheControl:       obj.CacheControl,
	}

	for i := uint(0); ; i++ {
		_, err := storage.awsSvc.PutObjectWithContext(storage.ctx, input)
		if (err != nil) && (i < storage.retryCnt) {
			Log.Debugf("S3 obj uploading failed with error: %s", err)
			time.Sleep(storage.retryInterval)
			continue
		} else if (err != nil) && (i == storage.retryCnt) {
			return err
		}

		return nil
	}
}

// GetObjectContent read object content and metadata from S3
func (storage *S3Storage) GetObjectContent(obj *Object) error {
	input := &s3.GetObjectInput{
		Bucket: storage.awsBucket,
		Key:    obj.Key,
	}

	for i := uint(0); ; i++ {
		result, err := storage.awsSvc.GetObjectWithContext(storage.ctx, input)
		if (err != nil) && (i < storage.retryCnt) {
			Log.Debugf("S3 obj content downloading request failed with error: %s", err)
			time.Sleep(storage.retryInterval)
			continue
		} else if (err != nil) && (i == storage.retryCnt) {
			return err
		}

		buf := bytes.NewBuffer(make([]byte, 0, aws.Int64Value(result.ContentLength)))
		_, err = io.Copy(ratelimit.NewWriter(buf, storage.rlBucket), result.Body)
		if (err != nil) && (i < storage.retryCnt) {
			Log.Debugf("S3 obj content downloading failed with error: %s", err)
			time.Sleep(storage.retryInterval)
			continue
		} else if (err != nil) && (i == storage.retryCnt) {
			return err
		}

		data := buf.Bytes()
		obj.Content = &data
		obj.ContentType = result.ContentType
		obj.ContentDisposition = result.ContentDisposition
		obj.ContentEncoding = result.ContentEncoding
		obj.ContentLanguage = result.ContentLanguage
		obj.ETag = strongEtag(result.ETag)
		obj.Metadata = result.Metadata
		obj.Mtime = result.LastModified
		obj.CacheControl = result.CacheControl

		return nil
	}
}

// GetObjectMeta update object metadata from S3
func (storage *S3Storage) GetObjectMeta(obj *Object) error {
	input := &s3.HeadObjectInput{
		Bucket: storage.awsBucket,
		Key:    obj.Key,
	}

	for i := uint(0); ; i++ {
		result, err := storage.awsSvc.HeadObjectWithContext(storage.ctx, input)
		if (err != nil) && (i < storage.retryCnt) {
			Log.Debugf("S3 obj meta downloading request failed with error: %s", err)
			time.Sleep(storage.retryInterval)
			continue
		} else if (err != nil) && (i == storage.retryCnt) {
			return err
		}

		obj.ContentType = result.ContentType
		obj.ContentDisposition = result.ContentDisposition
		obj.ContentEncoding = result.ContentEncoding
		obj.ContentLanguage = result.ContentLanguage
		obj.ETag = strongEtag(result.ETag)
		obj.Metadata = result.Metadata
		obj.Mtime = result.LastModified
		obj.CacheControl = result.CacheControl

		return nil
	}
}

// DeleteObject remove object from S3
func (storage *S3Storage) DeleteObject(obj *Object) error {
	input := &s3.DeleteObjectInput{
		Bucket: storage.awsBucket,
		Key:    obj.Key,
	}

	for i := uint(0); ; i++ {
		_, err := storage.awsSvc.DeleteObjectWithContext(storage.ctx, input)
		if (err != nil) && (i < storage.retryCnt) {
			Log.Debugf("S3 obj removing failed with error: %s", err)
			time.Sleep(storage.retryInterval)
			continue
		} else if (err != nil) && (i == storage.retryCnt) {
			return err
		}

		return nil
	}
}

// GetStorageType return storage type
func (storage *S3Storage) GetStorageType() Type {
	return TypeS3
}

// strongEtag remove "W/" prefix from ETag.
// In some cases S3 return ETag with "W/" prefix which mean that it not strong ETag.
// For easier compare we remove this prefix.
func strongEtag(s *string) *string {
	etag := strings.TrimPrefix(*s, "W/")
	return &etag
}
