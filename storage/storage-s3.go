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
	"io/ioutil"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"time"
)

//S3Storage configuration
type S3Storage struct {
	awsSvc        *s3.S3
	awsSession    *session.Session
	awsBucket     *string
	prefix        string
	keysPerReq    int64
	retryCnt      uint
	retryInterval time.Duration
	stats         Stats
	ctx           context.Context
	listMarker    *string
}

//NewS3Storage return new configured S3 storage
func NewS3Storage(awsAccessKey, awsSecretKey, awsRegion, endpoint, bucketName, prefix string, keysPerReq int64, listRetry uint, listRetryInterval time.Duration) *S3Storage {
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
		retryCnt:      listRetry,
		retryInterval: listRetryInterval,
		stats:         Stats{},
		ctx:           context.TODO(),
	}

	return &storage
}

func (storage *S3Storage) WithContext(ctx context.Context) {
	storage.ctx = ctx
}

//List S3 bucket and send founded objects to chan
func (storage *S3Storage) List(output chan<- *Object) error {
	listObjectsFn := func(p *s3.ListObjectsOutput, lastPage bool) bool {
		for _, o := range p.Contents {
			atomic.AddUint64(&storage.stats.ListedObjects, 1)
			key, _ := url.QueryUnescape(aws.StringValue(o.Key))
			output <- &Object{Key: &key, ETag: o.ETag, Mtime: o.LastModified}
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
			Log.Errorf("S3 listing failed with error: %s", err)
			return err
		} else {
			Log.Debugf("Listing bucket finished")
			return err
		}
	}
}

//PutObject to bucket
func (storage *S3Storage) PutObject(obj *Object) error {
	input := &s3.PutObjectInput{
		Bucket:             storage.awsBucket,
		Key:                aws.String(filepath.Join(storage.prefix, *obj.Key)),
		Body:               bytes.NewReader(*obj.Content),
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

		atomic.AddUint64(&storage.stats.UploadedObjects, 1)
		return nil
	}
}

//GetObjectContent download object content from S3
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

		data, err := ioutil.ReadAll(result.Body)
		if (err != nil) && (i < storage.retryCnt) {
			Log.Debugf("S3 obj content downloading failed with error: %s", err)
			time.Sleep(storage.retryInterval)
			continue
		} else if (err != nil) && (i == storage.retryCnt) {
			return err
		}

		obj.Content = &data
		obj.ContentType = result.ContentType
		obj.ContentDisposition = result.ContentDisposition
		obj.ContentEncoding = result.ContentEncoding
		obj.ContentLanguage = result.ContentLanguage
		obj.ETag = result.ETag
		obj.Metadata = result.Metadata
		obj.Mtime = result.LastModified
		obj.CacheControl = result.CacheControl

		atomic.AddUint64(&storage.stats.DataLoadedObjects, 1)
		return nil
	}
}

//GetObjectMeta update object metadata from S3
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
		obj.ETag = result.ETag
		obj.Metadata = result.Metadata
		obj.Mtime = result.LastModified
		obj.CacheControl = result.CacheControl

		atomic.AddUint64(&storage.stats.MetaLoadedObjects, 1)
		return nil
	}
}

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

		atomic.AddUint64(&storage.stats.DeletedObjects, 1)
		return nil
	}
}

//GetStorageType return storage type
func (storage *S3Storage) GetStorageType() Type {
	return TypeS3
}

func (storage *S3Storage) GetStats() Stats {
	return storage.stats
}
