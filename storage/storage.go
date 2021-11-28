// Package storage provides interface for working with different storage's like local FS and Amazon S3.
package storage

import (
	"context"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sirupsen/logrus"
)

// Log implement Logrus logger for debug logging.
var Log = logrus.New()

// Type of Storage.
type Type int

// Storage types.
const (
	TypeS3 Type = iota + 1
	TypeS3Versioned
	TypeFS
	TypeS3Stream
)

// Object contain content and metadata of S3 object.
type Object struct {
	Key                 *string                 `json:"-"`
	ETag                *string                 `json:"e_tag"`
	Mtime               *time.Time              `json:"mtime"`
	Content             *[]byte                 `json:"-"`
	ContentStream       io.ReadCloser           `json:"-"`
	ContentLength       *int64                  `json:"-"`
	ContentType         *string                 `json:"content_type"`
	ContentDisposition  *string                 `json:"content_disposition"`
	ContentEncoding     *string                 `json:"content_encoding"`
	ContentLanguage     *string                 `json:"content_language"`
	Metadata            map[string]*string      `json:"metadata"`
	ACL                 *string                 `json:"acl"`
	CacheControl        *string                 `json:"cache_control"`
	VersionId           *string                 `json:"version_id"`
	IsLatest            *bool                   `json:"-"`
	StorageClass        *string                 `json:"storage_class"`
	AccessControlPolicy *s3.AccessControlPolicy `json:"access_control_policy"`
}

// Storage interface.
type Storage interface {
	WithContext(ctx context.Context)
	WithRateLimit(limit int) error
	List(ch chan<- *Object) error
	PutObject(object *Object) error
	GetObjectContent(obj *Object) error
	GetObjectMeta(obj *Object) error
	GetObjectACL(obj *Object) error
	DeleteObject(obj *Object) error
}
