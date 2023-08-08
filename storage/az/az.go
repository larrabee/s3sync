package az

import (
	"bytes"
	"context"
	"errors"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/larrabee/s3sync/storage"
	"io"
	"time"

	"github.com/larrabee/ratelimit"
)
import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// S3Storage configuration.
type AzStorage struct {
	client        *azblob.Client
	awsBucket     *string
	prefix        string
	keysPerReq    int64
	retryCnt      uint
	retryInterval time.Duration
	ctx           context.Context
	listMarker    *string
	rlBucket      ratelimit.Bucket
}

// NewAzStorage return new configured AZ storage.
//
// You should always create new storage with this constructor.
func NewAzStorage(awsNoSign bool, awsAccessKey, awsSecretKey, awsToken, awsRegion, endpoint, bucketName, prefix string, keysPerReq int64, retryCnt uint, retryDelay time.Duration, skipSSLVerify bool) *AzStorage {
	credential, err := azblob.NewSharedKeyCredential(awsAccessKey, awsSecretKey)
	cl, err := azblob.NewClientWithSharedKeyCredential(endpoint, credential, nil)

	if err != nil {
		return nil
	}
	st := AzStorage{
		awsBucket:     &bucketName,
		client:        cl,
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
func (st *AzStorage) WithContext(ctx context.Context) {
	st.ctx = ctx
}

// WithRateLimit set rate limit (bytes/sec) for storage.
func (st *AzStorage) WithRateLimit(limit int) error {
	bucket, err := ratelimit.NewBucketWithRate(float64(limit), int64(limit))
	if err != nil {
		return err
	}
	st.rlBucket = bucket
	return nil
}

// List AZ container and send founded objects to chan.
func (st *AzStorage) List(output chan<- *storage.Object) error {
	flatPager := st.client.NewListBlobsFlatPager(*st.awsBucket, nil)
	for flatPager.More() {
		page, err := flatPager.NextPage(st.ctx)
		if err != nil {
			return err
		}
		for _, obj := range page.Segment.BlobItems {
			output <- &storage.Object{
				Key:          obj.Name,
				ETag:         storage.StrongEtag(obj.Metadata["etag"]),
				Mtime:        obj.Properties.LastModified,
				StorageClass: (*string)(obj.Properties.AccessTier),
				IsLatest:     aws.Bool(true),
			}
		}
	}
	storage.Log.Debugf("Listing bucket finished")
	return nil
}

// PutObject saves object to AZ.
// PutObject ignore VersionId, it always save object as latest version.
func (st *AzStorage) PutObject(obj *storage.Object) error {
	var objReader io.ReadSeeker
	if obj.Content == nil {
		if obj.ContentStream == nil {
			return errors.New("object has no content")
		}
		buf := bytes.NewBuffer(make([]byte, 0, aws.Int64Value(obj.ContentLength)))
		if _, err := io.Copy(ratelimit.NewWriter(buf, st.rlBucket), obj.ContentStream); err != nil {
			return err
		}
		obj.ContentStream.Close()
		objReader = bytes.NewReader(buf.Bytes())
	} else {
		objReader = bytes.NewReader(*obj.Content)
	}

	rlReader := ratelimit.NewReadSeeker(objReader, st.rlBucket)
	options := azblob.UploadStreamOptions{
		BlockSize:               0,
		Concurrency:             0,
		TransactionalValidation: nil,
		HTTPHeaders:             nil,
		Metadata:                nil,
		AccessConditions:        nil,
		AccessTier:              nil,
		Tags:                    nil,
		CPKInfo:                 nil,
		CPKScopeInfo:            nil,
	}
	_, err := st.client.UploadStream(st.ctx, *st.awsBucket, st.prefix+*obj.Key, rlReader, &options)
	return err
}

func withAcceptEncoding(e string) request.Option {
	return func(r *request.Request) {
		r.HTTPRequest.Header.Add("Accept-Encoding", e)
	}
}

// GetObjectContent read object content and metadata from AZ Blob.
func (st *AzStorage) GetObjectContent(obj *storage.Object) error {
	options := azblob.DownloadStreamOptions{}
	stream, err := st.client.DownloadStream(st.ctx, *st.awsBucket, *obj.Key, &options)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(make([]byte, 0, aws.Int64Value(stream.ContentLength)))
	if _, err := io.Copy(ratelimit.NewWriter(buf, st.rlBucket), stream.Body); err != nil {
		return err
	}

	data := buf.Bytes()
	obj.Content = &data
	obj.ContentType = stream.ContentType
	obj.ContentLength = stream.ContentLength
	obj.ContentDisposition = stream.ContentDisposition
	obj.ContentEncoding = stream.ContentEncoding
	obj.ContentLanguage = stream.ContentLanguage
	obj.Metadata = stream.Metadata
	obj.Mtime = stream.LastModified
	obj.CacheControl = stream.CacheControl

	return nil
}

// GetObjectACL read object ACL from AZ Blob.
func (st *AzStorage) GetObjectACL(obj *storage.Object) error {
	//TODO implement

	return nil
}

// GetObjectMeta update object metadata from AZ Blob.
func (st *AzStorage) GetObjectMeta(obj *storage.Object) error {
	options := blob.GetPropertiesOptions{}
	properties, err := st.client.ServiceClient().NewContainerClient(*st.awsBucket).NewBlobClient(st.prefix+*obj.Key).GetProperties(st.ctx, &options)
	obj.ContentType = properties.ContentType
	if err != nil {
		return err
	}
	return nil
}

// DeleteObject remove object from AZ Blob.
func (st *AzStorage) DeleteObject(obj *storage.Object) error {
	//TODO Implement
	return nil
}
