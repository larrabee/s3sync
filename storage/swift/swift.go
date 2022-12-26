package swift

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/objectstorage/v1/objects"
	"github.com/gophercloud/gophercloud/pagination"
	"github.com/larrabee/ratelimit"

	"github.com/larrabee/s3sync/storage"
)

// Storage configuration.
type Storage struct {
	conn     *gophercloud.ServiceClient
	bucket   string
	prefix   string
	ctx      context.Context
	rlBucket ratelimit.Bucket
}

// NewStorage return new configured S3 storage.
//
// You should always create new storage with this constructor.
func NewStorage(user, key, tenant, domain, authUrl string, bucketName, prefix string, skipSSLVerify bool) (*Storage, error) {
	st := &Storage{
		ctx:      context.TODO(),
		rlBucket: ratelimit.NewFakeBucket(),
		bucket:   bucketName,
		prefix:   prefix,
	}

	auth := gophercloud.AuthOptions{
		IdentityEndpoint: authUrl,
		Username:         user,
		Password:         key,
		TenantName:       tenant,
		DomainName:       domain,
	}

	provider, err := openstack.AuthenticatedClient(auth)
	if err != nil {
		return nil, err
	}

	if skipSSLVerify {
		tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
		provider.HTTPClient = http.Client{Transport: tr}
	}

	client, err := openstack.NewObjectStorageV1(provider, gophercloud.EndpointOpts{})
	if err != nil {
		return nil, err
	}

	st.conn = client

	return st, nil
}

// WithContext add's context to storage.
func (st *Storage) WithContext(ctx context.Context) {
	st.ctx = ctx
	st.conn.Context = ctx
}

// WithRateLimit set rate limit (bytes/sec) for storage.
func (st *Storage) WithRateLimit(limit int) error {
	bucket, err := ratelimit.NewBucketWithRate(float64(limit), int64(limit))
	if err != nil {
		return err
	}
	st.rlBucket = bucket
	return nil
}

// List S3 bucket and send founded objects to chan.
func (st *Storage) List(output chan<- *storage.Object) error {
	opts := &objects.ListOpts{Full: true, Prefix: st.prefix}
	pager := objects.List(st.conn, st.bucket, opts)

	err := pager.EachPage(func(page pagination.Page) (bool, error) {
		objectList, err := objects.ExtractInfo(page)
		if err != nil {
			return false, err
		}
		for _, n := range objectList {
			key := strings.Replace(n.Name, st.prefix, "", 1)
			key = strings.TrimLeft(key, "/")
			output <- &storage.Object{
				Key:           &key,
				ETag:          &n.Hash,
				ContentType:   &n.ContentType,
				Mtime:         &n.LastModified,
				ContentLength: &n.Bytes,
				IsLatest:      aws.Bool(true),
			}
		}
		return true, nil
	})
	if err != nil {
		return err
	}

	storage.Log.Debugf("Listing bucket finished")
	return nil

}

// PutObject saves object to S3.
// PutObject ignore VersionId, it always save object as latest version.
func (st *Storage) PutObject(obj *storage.Object) error {
	opts := objects.CreateOpts{}
	if obj.ContentType != nil {
		opts.ContentType = *obj.ContentType
	}
	if obj.Content != nil {
		opts.Content = bytes.NewReader(*obj.Content)
	}
	if obj.ContentDisposition != nil {
		opts.ContentDisposition = *obj.ContentDisposition
	}
	if obj.ContentEncoding != nil {
		opts.ContentEncoding = *obj.ContentEncoding
	}
	if obj.CacheControl != nil {
		opts.CacheControl = *obj.CacheControl
	}

	res := objects.Create(st.conn, st.bucket, path.Join(st.prefix, *obj.Key), opts)
	if res.Err != nil {
		return res.Err
	}

	return nil
}

// GetObjectContent read object content and metadata from S3.
func (st *Storage) GetObjectContent(obj *storage.Object) error {
	opts := objects.DownloadOpts{}

	// Download everything into a DownloadResult struct
	res := objects.Download(st.conn, st.bucket, path.Join(st.prefix, *obj.Key), opts)
	if res.Err != nil {
		return res.Err
	}
	defer res.Body.Close()

	header, err := res.Extract()
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(make([]byte, 0, header.ContentLength))
	if _, err := io.Copy(ratelimit.NewWriter(buf, st.rlBucket), res.Body); err != nil {
		return err
	}

	data := buf.Bytes()
	obj.Content = &data
	obj.ContentType = &header.ContentType
	obj.ContentLength = &header.ContentLength
	obj.ContentDisposition = &header.ContentDisposition
	obj.ContentEncoding = &header.ContentEncoding
	obj.ETag = storage.StrongEtag(&header.ETag)
	obj.Mtime = &header.LastModified

	return nil
}

// GetObjectACL read object ACL from S3.
func (st *Storage) GetObjectACL(obj *storage.Object) error {
	return nil
}

// GetObjectMeta update object metadata from S3.
func (st *Storage) GetObjectMeta(obj *storage.Object) error {
	opts := objects.GetOpts{}

	res := objects.Get(st.conn, st.bucket, path.Join(st.prefix, *obj.Key), opts)
	if res.Err != nil {
		return res.Err
	}

	header, err := res.Extract()
	if err != nil {
		return err
	}

	obj.ContentType = &header.ContentType
	obj.ContentLength = &header.ContentLength
	obj.ContentDisposition = &header.ContentDisposition
	obj.ContentEncoding = &header.ContentEncoding
	obj.ETag = storage.StrongEtag(&header.ETag)
	obj.Mtime = &header.LastModified

	return nil
}

// DeleteObject remove object from S3.
func (st *Storage) DeleteObject(obj *storage.Object) error {
	opts := objects.DeleteOpts{}

	res := objects.Delete(st.conn, st.bucket, path.Join(st.prefix, *obj.Key), opts)
	if res.Err != nil {
		return res.Err
	}

	return nil
}
