package main

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/eapache/channels"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

//S3Storage configuration
type S3Storage struct {
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
func NewS3Storage(awsAccessKey, awsSecretKey, awsRegion, endpoint, bucketName, prefix, acl string, keysPerReq int64, workers, retry uint, retryInterval time.Duration) (storage S3Storage) {
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
func (storage S3Storage) List(output chan<- Object) error {
	prefixChan := channels.NewInfiniteChannel()
	listResultChan := make(chan error, storage.workers)
	wg := sync.WaitGroup{}
	stopListing := false

	listObjectsRecursive := func(prefixChan *channels.InfiniteChannel, output chan<- Object) {
		listObjectsFn := func(p *s3.ListObjectsOutput, lastPage bool) bool {
			for _, o := range p.CommonPrefixes {
				wg.Add(1)
				prefixChan.In() <- aws.StringValue(o.Prefix)
			}
			for _, o := range p.Contents {
				atomic.AddUint64(&counter.totalObjCnt, 1)
				key, _ := url.QueryUnescape(aws.StringValue(o.Key))
				output <- Object{Key: key, ETag: aws.StringValue(o.ETag), Mtime: aws.TimeValue(o.LastModified)}
			}
			return !lastPage // continue paging
		}

		for prefix := range prefixChan.Out() {
			for i := uint(0); i <= storage.retry; i++ {
				if stopListing {
					wg.Done()
					return
				}
				err := storage.awsSvc.ListObjectsPages(&s3.ListObjectsInput{
					Bucket:       aws.String(storage.awsBucket),
					Prefix:       aws.String(prefix.(string)),
					MaxKeys:      aws.Int64(storage.keysPerReq),
					Delimiter:    aws.String("/"),
					EncodingType: aws.String(s3.EncodingTypeUrl),
				}, listObjectsFn)

				if (err != nil) && (i == storage.retry) {
					wg.Done()
					listResultChan <- err
					break
				} else if err == nil {
					wg.Done()
					break
				} else {
					log.Debugf("S3 listing failed with error: %s", err)
					time.Sleep(storage.retryInterval)
					continue
				}
			}
		}
	}

	for i := storage.workers; i != 0; i-- {
		go listObjectsRecursive(prefixChan, output)
	}

	// Start listing from storage.prefix
	wg.Add(1)
	prefixChan.In() <- storage.prefix

	go func() {
		wg.Wait()
		prefixChan.Close()
		listResultChan <- nil
	}()

	select {
	case msg := <-listResultChan:
		stopListing = true
		wg.Wait()
		close(output)
		return msg
	}
}

//PutObject to bucket
func (storage S3Storage) PutObject(obj *Object) error {
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
func (storage S3Storage) GetObjectContent(obj *Object) error {
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
func (storage S3Storage) GetObjectMeta(obj *Object) error {
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
func (storage S3Storage) GetStorageType() ConnType {
	return s3Conn
}
