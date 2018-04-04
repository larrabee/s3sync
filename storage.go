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
	"github.com/karrick/godirwalk"
	"github.com/eapache/channels"
)

type Object struct {
	Key         string
	ETag        string
	Mtime       time.Time
	Content     []byte
	ContentType string
}

type SyncGroup struct {
	Source Storage
	Target Storage
}

type Storage interface {
	List(ch chan<- Object) error
	PutObject(object *Object) error
	GetObjectContent(obj *Object) error
	GetObjectMeta(obj *Object) error
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

func (storage AWSStorage) List(output chan<- Object) error {

	prefixChan := make(chan string, s3keysPerReq*4)
	listResultChan := make(chan error, cli.Workers)
	wg := sync.WaitGroup{}
	stopListing := false

	listObjectsRecursive := func(prefixChan chan string, output chan<- Object) {
		listObjectsFn := func(p *s3.ListObjectsOutput, lastPage bool) bool {
			for _, o := range p.CommonPrefixes {
				wg.Add(1)
				prefixChan <- aws.StringValue(o.Prefix)
			}
			for _, o := range p.Contents {
				atomic.AddUint64(&counter.totalObjCnt, 1)
				output <- Object{Key: aws.StringValue(o.Key), ETag: aws.StringValue(o.ETag), Mtime: aws.TimeValue(o.LastModified)}
			}
			return true // continue paging
		}

		for prefix := range prefixChan {
			for i := uint(0); i <= cli.Retry; i++ {
				if stopListing {
					wg.Done()
					return
				}

				err := storage.awsSvc.ListObjectsPages(&s3.ListObjectsInput{
					Bucket:    aws.String(storage.awsBucket),
					Prefix:    aws.String(prefix),
					MaxKeys:   aws.Int64(s3keysPerReq),
					Delimiter: aws.String("/"),
				}, listObjectsFn)


				if (err != nil) && (i == cli.Retry) {
					wg.Done()
					listResultChan <- err
					break
				} else if err == nil {
					wg.Done()
					break
				} else {
					log.Debugf("S3 listing failed with error: %s", err)
					time.Sleep(cli.RetrySleepInterval)
					continue
				}
			}
		}
	}

	for i := cli.Workers; i != 0; i-- {
		go listObjectsRecursive(prefixChan, output)
	}

	// Start listing from storage.prefix
	wg.Add(1)
	prefixChan <- storage.prefix

	go func() {
		wg.Wait()
		close(prefixChan)
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

func (storage AWSStorage) PutObject(obj *Object) error {
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

func (storage AWSStorage) GetObjectContent(obj *Object) error {
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

func (storage AWSStorage) GetObjectMeta(obj *Object) error {
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

func (storage FSStorage) List(output chan<- Object) error {
	prefixChan := make(chan string, cli.MaxPrefixKeys)
	listResultChan := make(chan error, cli.Workers)
	wg := sync.WaitGroup{}
	stopListing := false

	listObjectsRecursive := func(prefixChan chan string, output chan<- Object) {
		buffer := make([]byte, 1024*64)

		for prefix := range prefixChan {
			if stopListing {
				wg.Done()
				return
			}
			dirents, err := godirwalk.ReadDirents(prefix, buffer)

			if err != nil {
				wg.Done()
				listResultChan <- err
				return
			}

			for _, dirent := range dirents {
				path := filepath.Join(prefix, dirent.Name())
				if dirent.IsDir() {
					wg.Add(1)
					prefixChan <- path
					continue
				} else {
					atomic.AddUint64(&counter.totalObjCnt, 1)
					output <- Object{Key: strings.TrimPrefix(path, storage.dir)}
				}
			}
			wg.Done()
		}
	}

	for i := cli.Workers; i != 0; i-- {
		go listObjectsRecursive(prefixChan, output)
	}

	// Start listing from storage.prefix
	wg.Add(1)
	prefixChan <- storage.dir

	go func() {
		wg.Wait()
		close(prefixChan)
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

func (storage FSStorage) PutObject(obj *Object) error {
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

func (storage FSStorage) GetObjectContent(obj *Object) (err error) {
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

func (storage FSStorage) GetObjectMeta(obj *Object) (err error) {
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
