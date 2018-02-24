package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	//"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	//"bufio"
)

type awsConn struct {
	awsSvc     *s3.S3
	awsSession *session.Session
	awsBucket  string
	// downloader *s3manager.Downloader
	// uploader *s3manager.Uploader
}

type syncGroup struct {
	Source awsConn
	Target awsConn
}

type object struct {
	SyncGroup *syncGroup
	Key       string
	ETag      string
	ErrCount  int
	Content   []byte
  ContentType string
}

func (self *awsConn) Configure(connName string) error {
	if !cfg.IsSet(fmt.Sprintf("connections.%s", connName)) {
		return errors.New(fmt.Sprintf("Connection 'connections.%s' not exist in config file", connName))
	}
	cred := credentials.NewStaticCredentials(cfg.GetString(fmt.Sprintf("connections.%s.awsAccessKey", connName)), cfg.GetString(fmt.Sprintf("connections.%s.awsSecretKey", connName)), "")
	awsConfig := aws.NewConfig()
	awsConfig.WithCredentials(cred)
	awsConfig.Region = aws.String(cfg.GetString(fmt.Sprintf("connections.%s.awsRegion", connName)))
	if endp := cfg.GetString(fmt.Sprintf("connections.%s.endpoint", connName)); endp != "" {
		awsConfig.Endpoint = aws.String(endp)
	}
	self.awsBucket = cfg.GetString(fmt.Sprintf("connections.%s.bucketName", connName))
	self.awsSession = session.Must(session.NewSession(awsConfig))
	self.awsSvc = s3.New(self.awsSession)
	// self.downloader = s3manager.NewDownloader(self.awsSession)
	// self.uploader = s3manager.NewUploader(self.awsSession)
	return nil
}

func (self *syncGroup) SyncObjToChan(ch chan<- object) error {
	listObjectsFn := func(p *s3.ListObjectsOutput, lastPage bool) bool {
		for _, o := range p.Contents {
			totalObjCnf++
			ch <- object{Key: aws.StringValue(o.Key), ETag: aws.StringValue(o.ETag), ErrCount: cfg.GetInt("sync.maxRetry"), SyncGroup: self}
		}
		return true // continue paging
	}

	err := self.Source.awsSvc.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: aws.String(self.Source.awsBucket),
	}, listObjectsFn)
	if err != nil {
		return err
	}
	return nil
}

func (self *object) GetContent() error {
	result, err := self.SyncGroup.Source.awsSvc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(self.SyncGroup.Source.awsBucket),
		Key:    aws.String(self.Key),
	})
	if err != nil {
		return err
	}

	self.Content, err = ioutil.ReadAll(result.Body)
  self.ContentType = aws.StringValue(result.ContentType)
	if err != nil {
		return err
	}
	return nil
}

func (self *object) PutContent() error {
	_, err := self.SyncGroup.Target.awsSvc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(self.SyncGroup.Target.awsBucket),
		Key:    aws.String(self.Key),
		Body:   bytes.NewReader(self.Content),
    ContentType: aws.String(self.ContentType),
	})
	if err != nil {
		return err
	}
	return nil
}
