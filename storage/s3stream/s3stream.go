package s3stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"crypto/tls"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/larrabee/ratelimit"
	"github.com/larrabee/s3sync/storage"
	s3backend "github.com/larrabee/s3sync/storage/s3"
)

// S3StreamStorage configuration.
type S3StreamStorage struct {
	awsSvc        *awss3.Client
	awsBucket     *string
	prefix        string
	keysPerReq    int64
	retryCnt      uint
	retryInterval time.Duration
	ctx           context.Context
	listMarker    *string
	rlBucket      ratelimit.Bucket
	uploader      *manager.Uploader
}

// NewS3StreamStorage return new configured S3 stream storage.
//
// You should always create new storage with this constructor.
func NewS3StreamStorage(awsNoSign bool, awsAccessKey, awsSecretKey, awsToken, awsRegion, endpoint, bucketName, prefix string, keysPerReq int64, retryCnt uint, retryDelay time.Duration) *S3StreamStorage {
	region := awsRegion
	if region == "" {
		region = "us-east-1"
	}

	cfgOpts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
		config.WithRetryer(func() aws.Retryer {
			return &s3backend.Retryer{RetryCnt: retryCnt, RetryInterval: retryDelay}
		}),
	}

	if awsNoSign {
		cfgOpts = append(cfgOpts, config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	} else if awsAccessKey != "" || awsSecretKey != "" {
		cfgOpts = append(cfgOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(awsAccessKey, awsSecretKey, awsToken),
		))
	}

	if skipSSLVerify := false; skipSSLVerify {
		tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
		cfgOpts = append(cfgOpts, config.WithHTTPClient(&http.Client{Transport: tr}))
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), cfgOpts...)
	if err != nil {
		storage.Log.Debugf("Failed to load AWS config: %v", err)
	}

	s3ClientOpts := []func(*awss3.Options){
		func(o *awss3.Options) { o.UsePathStyle = true },
	}
	if endpoint != "" {
		s3ClientOpts = append(s3ClientOpts, func(o *awss3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}

	client := awss3.NewFromConfig(cfg, s3ClientOpts...)

	st := S3StreamStorage{
		awsBucket:     &bucketName,
		awsSvc:        client,
		prefix:        prefix,
		keysPerReq:    keysPerReq,
		retryCnt:      retryCnt,
		retryInterval: retryDelay,
		ctx:           context.TODO(),
		rlBucket:      ratelimit.NewFakeBucket(),
		uploader:      manager.NewUploader(client),
	}

	return &st
}

// WithContext add's context to storage.
func (st *S3StreamStorage) WithContext(ctx context.Context) {
	st.ctx = ctx
}

// WithRateLimit set rate limit (bytes/sec) for storage.
func (st *S3StreamStorage) WithRateLimit(limit int) error {
	bucket, err := ratelimit.NewBucketWithRate(float64(limit), int64(limit))
	if err != nil {
		return err
	}
	st.rlBucket = bucket
	return nil
}

// List S3 bucket and send founded objects to chan.
func (st *S3StreamStorage) List(output chan<- *storage.Object) error {
	input := &awss3.ListObjectsV2Input{
		Bucket:            st.awsBucket,
		Prefix:            aws.String(st.prefix),
		MaxKeys:           aws.Int32(int32(st.keysPerReq)),
		EncodingType:      s3types.EncodingTypeUrl,
		ContinuationToken: st.listMarker,
	}

	paginator := awss3.NewListObjectsV2Paginator(st.awsSvc, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(st.ctx)
		if err != nil {
			return err
		}
		for _, o := range page.Contents {
			key, _ := url.QueryUnescape(aws.ToString(o.Key))
			key = strings.Replace(key, st.prefix, "", 1)
			isLatest := true
			output <- &storage.Object{
				Key:          &key,
				ETag:         storage.StrongEtag(o.ETag),
				Mtime:        o.LastModified,
				StorageClass: aws.String(string(o.StorageClass)),
				IsLatest:     &isLatest,
			}
		}
		st.listMarker = page.NextContinuationToken
	}
	storage.Log.Debugf("Listing bucket finished")
	return nil
}

// PutObject saves object to S3.
// PutObject ignore VersionId, it always save object as latest version.
func (st *S3StreamStorage) PutObject(obj *storage.Object) error {
	var readStream io.Reader
	if obj.ContentStream == nil {
		if obj.Content != nil {
			readStream = bytes.NewReader(*obj.Content)
		}
		return errors.New("object has no contentStream")
	} else {
		readStream = obj.ContentStream
		defer obj.ContentStream.Close()
	}

	rlReader := ratelimit.NewReader(readStream, st.rlBucket)
	input := &awss3.PutObjectInput{
		Bucket:               st.awsBucket,
		Key:                  aws.String(st.prefix + *obj.Key),
		Body:                 rlReader,
		ContentType:          obj.ContentType,
		ContentDisposition:   obj.ContentDisposition,
		ContentEncoding:      obj.ContentEncoding,
		ContentLanguage:      obj.ContentLanguage,
		ACL:                  s3types.ObjectCannedACL(aws.ToString(obj.ACL)),
		Metadata:             toAWSMetadata(obj.Metadata),
		CacheControl:         obj.CacheControl,
		StorageClass:         s3types.StorageClass(aws.ToString(obj.StorageClass)),
		ServerSideEncryption: s3types.ServerSideEncryption(aws.ToString(obj.ServerSideEncryption)),
	}

	if _, err := st.uploader.Upload(st.ctx, input); err != nil {
		return err
	}

	if obj.AccessControlPolicy != nil {
		inputAcl := &awss3.PutObjectAclInput{
			Bucket:              st.awsBucket,
			Key:                 aws.String(st.prefix + *obj.Key),
			AccessControlPolicy: obj.AccessControlPolicy,
		}

		if _, err := st.awsSvc.PutObjectAcl(st.ctx, inputAcl); err != nil {
			return err
		}
	}

	return nil
}

// GetObjectContent read object content and metadata from S3.
func (st *S3StreamStorage) GetObjectContent(obj *storage.Object) error {
	input := &awss3.GetObjectInput{
		Bucket:    st.awsBucket,
		Key:       aws.String(st.prefix + *obj.Key),
		VersionId: obj.VersionId,
	}

	result, err := st.awsSvc.GetObject(st.ctx, input)
	if err != nil {
		return err
	}

	obj.Content = nil
	obj.ContentStream = result.Body
	obj.ContentType = result.ContentType
	obj.ContentLength = result.ContentLength
	obj.ContentDisposition = result.ContentDisposition
	obj.ContentEncoding = result.ContentEncoding
	obj.ContentLanguage = result.ContentLanguage
	obj.ETag = storage.StrongEtag(result.ETag)
	obj.Metadata = fromAWSMetadata(result.Metadata)
	obj.Mtime = result.LastModified
	obj.CacheControl = result.CacheControl
	obj.StorageClass = aws.String(string(result.StorageClass))

	return nil
}

// GetObjectACL read object ACL from S3.
func (st *S3StreamStorage) GetObjectACL(obj *storage.Object) error {
	input := &awss3.GetObjectAclInput{
		Bucket:    st.awsBucket,
		Key:       aws.String(st.prefix + *obj.Key),
		VersionId: obj.VersionId,
	}

	result, err := st.awsSvc.GetObjectAcl(st.ctx, input)
	if err != nil {
		return err
	}

	obj.AccessControlPolicy = &s3types.AccessControlPolicy{
		Grants: result.Grants,
		Owner:  result.Owner,
	}

	return nil
}

// GetObjectMeta update object metadata from S3.
func (st *S3StreamStorage) GetObjectMeta(obj *storage.Object) error {
	input := &awss3.HeadObjectInput{
		Bucket:    st.awsBucket,
		Key:       aws.String(st.prefix + *obj.Key),
		VersionId: obj.VersionId,
	}

	result, err := st.awsSvc.HeadObject(st.ctx, input)
	if err != nil {
		return err
	}

	obj.ContentType = result.ContentType
	obj.ContentDisposition = result.ContentDisposition
	obj.ContentEncoding = result.ContentEncoding
	obj.ContentLanguage = result.ContentLanguage
	obj.ETag = storage.StrongEtag(result.ETag)
	obj.Metadata = fromAWSMetadata(result.Metadata)
	obj.Mtime = result.LastModified
	obj.CacheControl = result.CacheControl
	obj.StorageClass = aws.String(string(result.StorageClass))

	return nil
}

// DeleteObject remove object from S3.
func (st *S3StreamStorage) DeleteObject(obj *storage.Object) error {
	input := &awss3.DeleteObjectInput{
		Bucket:    st.awsBucket,
		Key:       aws.String(st.prefix + *obj.Key),
		VersionId: obj.VersionId,
	}

	if _, err := st.awsSvc.DeleteObject(st.ctx, input); err != nil {
		return err
	}
	return nil
}

// toAWSMetadata converts map[string]*string to the map[string]string expected by SDK v2.
func toAWSMetadata(m map[string]*string) map[string]string {
	if m == nil {
		return nil
	}
	result := make(map[string]string, len(m))
	for k, v := range m {
		if v != nil {
			result[k] = *v
		}
	}
	return result
}

// fromAWSMetadata converts the map[string]string returned by SDK v2 to map[string]*string.
func fromAWSMetadata(m map[string]string) map[string]*string {
	if m == nil {
		return nil
	}
	result := make(map[string]*string, len(m))
	for k, v := range m {
		v := v
		result[k] = &v
	}
	return result
}
