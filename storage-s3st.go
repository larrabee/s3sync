package main

import (
	"bytes"
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
type S3StStorage struct {
	awsSvc        *s3.S3
	awsSession    *session.Session
	awsBucket     string
	prefix        string
	acl           string
	keysPerReq    int64
	workers       uint
	retry         uint
	retryInterval time.Duration
}

//NewS3Storage return new configured S3 storage
func NewS3StStorage(awsAccessKey, awsSecretKey, awsRegion, endpoint, bucketName, prefix, acl string, keysPerReq int64, workers, retry uint, retryInterval time.Duration) (storage S3StStorage) {
	sess := session.Must(session.NewSession())

	awsConfig := aws.NewConfig()
	awsConfig.S3ForcePathStyle = aws.Bool(true)
	awsConfig.CredentialsChainVerboseErrors = aws.Bool(true)

	if awsAccessKey != "" && awsSecretKey != "" {
		cred := credentials.NewStaticCredentials(awsAccessKey, awsSecretKey, "")
		awsConfig.WithCredentials(cred)
	} else {
		cred := credentials.NewChainCredentials(
			[]credentials.Provider{
				&credentials.EnvProvider{},
				&credentials.SharedCredentialsProvider{},
				&ec2rolecreds.EC2RoleProvider{
            Client: ec2metadata.New(sess),
        },
			})
		awsConfig.WithCredentials(cred)
	}

	awsConfig.Region = aws.String(awsRegion)
	if endpoint != "" {
		awsConfig.Endpoint = aws.String(endpoint)
	}
	storage.awsBucket = bucketName
	storage.awsSession = session.Must(session.NewSession(awsConfig))
	storage.awsSvc = s3.New(storage.awsSession)
	storage.prefix = prefix
	storage.acl = acl
	storage.keysPerReq = keysPerReq
	storage.workers = workers
	storage.retry = retry
	storage.retryInterval = retryInterval
	return storage
}

//List S3 bucket and send founded objects to chan
func (storage S3StStorage) List(output chan<- Object) error {
	var lastMarker *string
	listObjectsFn := func(p *s3.ListObjectsOutput, lastPage bool) bool {
		for _, o := range p.Contents {
			atomic.AddUint64(&counter.totalObjCnt, 1)
			key, _ := url.QueryUnescape(aws.StringValue(o.Key))
			log.Debugf(key)
			output <- Object{Key: key, ETag: aws.StringValue(o.ETag), Mtime: aws.TimeValue(o.LastModified)}
		}
		log.Debugf("Marker: %s", aws.StringValue(p.Marker))
		lastMarker = p.Marker
		if lastPage {
			close(output)
		}
		return !lastPage // continue paging

	}

	for i:= uint(0);; i++ {
		err := storage.awsSvc.ListObjectsPages(&s3.ListObjectsInput{
			Bucket:       aws.String(storage.awsBucket),
			Prefix:       aws.String(storage.prefix),
			MaxKeys:      aws.Int64(storage.keysPerReq),
			EncodingType: aws.String(s3.EncodingTypeUrl),
			Marker:       lastMarker,
		}, listObjectsFn)
		if (err != nil) && (i < storage.retry) {
			log.Debugf("S3 listing failed with error: %s", err)
			time.Sleep(storage.retryInterval)
			continue
		} else if (err != nil) && (i == storage.retry) {
			log.Errorf("S3 listing failed with error: %s", err)
			return err
		} else {
			log.Debugf("Listing bucket finished")
			return err
		}
	}
}

//PutObject to bucket
func (storage S3StStorage) PutObject(obj *Object) error {
	_, err := storage.awsSvc.PutObject(&s3.PutObjectInput{
		Bucket:             aws.String(storage.awsBucket),
		Key:                aws.String(filepath.Join(storage.prefix, obj.Key)),
		Body:               bytes.NewReader(obj.Content),
		ContentType:        aws.String(obj.ContentType),
		ContentDisposition: aws.String(obj.ContentDisposition),
		ACL:                aws.String(storage.acl),
		Metadata:           obj.Metadata,
	})
	if err != nil {
		return err
	}
	return nil
}

//GetObjectContent download object content from S3
func (storage S3StStorage) GetObjectContent(obj *Object) error {
	result, err := storage.awsSvc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(storage.awsBucket),
		Key:    aws.String(obj.Key),
	})
	if err != nil {
		return err
	}

	obj.Content, err = ioutil.ReadAll(result.Body)
	if err != nil {
		return err
	}

	obj.ContentType = aws.StringValue(result.ContentType)
	obj.ContentDisposition = aws.StringValue(result.ContentDisposition)
	obj.ETag = aws.StringValue(result.ETag)
	obj.Metadata = result.Metadata
	obj.Mtime = aws.TimeValue(result.LastModified)
	return nil
}

//GetObjectMeta update object metadata from S3
func (storage S3StStorage) GetObjectMeta(obj *Object) error {
	result, err := storage.awsSvc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(storage.awsBucket),
		Key:    aws.String(obj.Key),
	})
	if err != nil {
		return err
	}

	obj.ContentType = aws.StringValue(result.ContentType)
	obj.ContentDisposition = aws.StringValue(result.ContentDisposition)
	obj.ETag = aws.StringValue(result.ETag)
	obj.Metadata = result.Metadata
	obj.Mtime = aws.TimeValue(result.LastModified)
	return nil
}

//GetStorageType return storage type
func (storage S3StStorage) GetStorageType() ConnType {
	return s3StConn
}
