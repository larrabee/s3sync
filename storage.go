package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type SyncGroup struct {
	Source Storage
	Target Storage
}

type Storage interface {
	List(ch chan<- object) error
	PutObject(object *object) error
	GetObjectContent(obj *object) error
	GetObjectMeta(obj *object) error
}

type AWSStorage struct {
	awsSvc     *s3.S3
	awsSession *session.Session
	awsBucket  string
	prefix     string
}

type FSStorage struct {
	dir string
}

func NewAWSStorage(awsAccessKey, awsSecretKey, awsRegion, endpoint, bucketName, prefix string) (storage AWSStorage) {
	cred := credentials.NewStaticCredentials(awsAccessKey, awsSecretKey, "")
	awsConfig := aws.NewConfig()
	awsConfig.S3ForcePathStyle = aws.Bool(true)
	awsConfig.WithCredentials(cred)
	awsConfig.Region = aws.String(awsRegion)
	if endpoint != "" {
		awsConfig.Endpoint = aws.String(endpoint)
	}
	storage.awsBucket = bucketName
	storage.awsSession = session.Must(session.NewSession(awsConfig))
	storage.awsSvc = s3.New(storage.awsSession)
	storage.prefix = prefix
	return storage
}

func NewFSStorage(dir string) (storage FSStorage) {

	storage.dir = filepath.Clean(dir) + "/"
	return storage
}

func (storage AWSStorage) List(output chan<- object) error {
	prefixChan := make(chan string, cli.Workers*2)
	listResultChan := make(chan error)
	wg := sync.WaitGroup{}

	listObjectsFn := func(p *s3.ListObjectsOutput, lastPage bool) bool {
		for _, o := range p.CommonPrefixes {
			wg.Add(1)
			prefixChan <- aws.StringValue(o.Prefix)
		}
		for _, o := range p.Contents {
			atomic.AddUint64(&totalObjCnt, 1)
			output <- object{Key: aws.StringValue(o.Key), ETag: aws.StringValue(o.ETag), Mtime: aws.TimeValue(o.LastModified)}
		}
		return true // continue paging
	}

	listObjectsRecursive := func(prefixChan chan string, output chan<- object) {
		for prefix := range prefixChan {
			err := storage.awsSvc.ListObjectsPages(&s3.ListObjectsInput{
				Bucket:    aws.String(storage.awsBucket),
				Prefix:    aws.String(prefix),
				MaxKeys:   aws.Int64(s3keysPerReq),
				Delimiter: aws.String("/"),
			}, listObjectsFn)
			wg.Done()
			if err != nil{
				listResultChan <- err
			}
		}
	}

	for i := cli.Workers; i != 0; i-- {
		go listObjectsRecursive(prefixChan, output)
	}

	go func() {
		wg.Wait()
		close(prefixChan)
		listResultChan <- nil
	}()

	// Start listing from storage.prefix
	wg.Add(1)
	prefixChan <- storage.prefix

	select {
	case msg := <-listResultChan:
		close(output)
		return msg
	}
}

func (storage AWSStorage) PutObject(obj *object) error {
	_, err := storage.awsSvc.PutObject(&s3.PutObjectInput{
		Bucket:      aws.String(storage.awsBucket),
		Key:         aws.String(obj.Key),
		Body:        bytes.NewReader(obj.Content),
		ContentType: aws.String(obj.ContentType),
	})
	if err != nil {
		return err
	}
	return nil
}

func (storage AWSStorage) GetObjectContent(obj *object) error {
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
	obj.ETag = aws.StringValue(result.ETag)
	obj.Mtime = aws.TimeValue(result.LastModified)
	return nil
}

func (storage AWSStorage) GetObjectMeta(obj *object) error {
	result, err := storage.awsSvc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(storage.awsBucket),
		Key:    aws.String(obj.Key),
	})
	if err != nil {
		return err
	}

	obj.ContentType = aws.StringValue(result.ContentType)
	obj.ETag = aws.StringValue(result.ETag)
	obj.Mtime = aws.TimeValue(result.LastModified)
	return nil
}

func (storage FSStorage) List(ch chan<- object) error {
	err := filepath.Walk(storage.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		atomic.AddUint64(&totalObjCnt, 1)
		ch <- object{Key: strings.TrimPrefix(path, storage.dir), ETag: EtagFromMetadata(info.ModTime(), info.Size()), Mtime: info.ModTime()}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (storage FSStorage) PutObject(obj *object) error {
	destPath := filepath.Join(storage.dir, obj.Key)
	err := os.MkdirAll(filepath.Dir(destPath), permDir)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(destPath, obj.Content, permFile)
	if err != nil {
		return err
	}
	return nil
}

func (storage FSStorage) GetObjectContent(obj *object) (err error) {
	destPath := filepath.Join(storage.dir, obj.Key)
	obj.Content, err = ioutil.ReadFile(destPath)
	if err != nil {
		return err
	}

	fh, err := os.Open(destPath)
	if err != nil {
		return err
	}
	defer fh.Close()

	n, err := fh.Read(obj.Content)
	if err != nil && err != io.EOF {
		return err
	}

	if n > 512 {
		n = 512
	}

	obj.ContentType = http.DetectContentType(obj.Content[:n])
	fileInfo, err := os.Stat(destPath)
	if err != nil {
		return err
	}
	obj.ETag = EtagFromMetadata(fileInfo.ModTime(), fileInfo.Size())
	obj.Mtime = fileInfo.ModTime()
	return nil
}

func (storage FSStorage) GetObjectMeta(obj *object) (err error) {
	destPath := filepath.Join(storage.dir, obj.Key)
	fh, err := os.Open(destPath)
	if err != nil {
		return err
	}
	defer fh.Close()

	buffer := make([]byte, 512)
	n, err := fh.Read(buffer)
	if err != nil && err != io.EOF {
		return err
	}

	obj.ContentType = http.DetectContentType(obj.Content[:n])
	fileInfo, err := os.Stat(destPath)
	if err != nil {
		return err
	}
	obj.ETag = EtagFromMetadata(fileInfo.ModTime(), fileInfo.Size())
	obj.Mtime = fileInfo.ModTime()
	return nil
}

func EtagFromMetadata(mtime time.Time, size int64) string {
	timeByte := byte(mtime.Unix())
	sizeByte := byte(size)
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, timeByte)
	if err != nil {
		return ""
	}
	err = binary.Write(buf, binary.LittleEndian, sizeByte)
	if err != nil {
		return ""
	}
	hasher := md5.New()
	hasher.Write(buf.Bytes())
	return hex.EncodeToString(hasher.Sum(nil))
}
