package storage

import (
	"context"
	"github.com/sirupsen/logrus"
	"time"
)

var Log = logrus.New()

type Type int

const (
	TypeS3 Type = iota + 1
	TypeS3Versioned
	TypeFS
)

//Object contain content and metadata of S3 object
type Object struct {
	Key                *string            `json:"-"`
	ETag               *string            `json:"e_tag"`
	Mtime              *time.Time         `json:"mtime"`
	Content            *[]byte            `json:"-"`
	ContentType        *string            `json:"content_type"`
	ContentDisposition *string            `json:"content_disposition"`
	ContentEncoding    *string            `json:"content_encoding"`
	ContentLanguage    *string            `json:"content_language"`
	Metadata           map[string]*string `json:"metadata"`
	ACL                *string            `json:"acl"`
	CacheControl       *string            `json:"cache_control"`
	VersionId          *string            `json:"version_id"`
}

//Storage interface
type Storage interface {
	WithContext(ctx context.Context)
	WithRateLimit(limit int) error
	List(ch chan<- *Object) error
	PutObject(object *Object) error
	GetObjectContent(obj *Object) error
	GetObjectMeta(obj *Object) error
	DeleteObject(obj *Object) error
	GetStorageType() Type
}
