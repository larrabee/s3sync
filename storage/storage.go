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
	TypeFS
)

//Object contain content and metadata of S3 object
type Object struct {
	Key                *string
	ETag               *string
	Mtime              *time.Time
	Content            *[]byte
	ContentType        *string
	ContentDisposition *string
	ContentEncoding    *string
	ContentLanguage    *string
	Metadata           map[string]*string
	ACL                *string
	CacheControl       *string
}

type Stats struct {
	ListedObjects     uint64
	DataLoadedObjects uint64
	MetaLoadedObjects uint64
	UploadedObjects   uint64
	DeletedObjects    uint64
}

//Storage interface
type Storage interface {
	WithContext(ctx context.Context)
	List(ch chan<- *Object) error
	PutObject(object *Object) error
	GetObjectContent(obj *Object) error
	GetObjectMeta(obj *Object) error
	DeleteObject(obj *Object) error
	GetStorageType() Type
	GetStats() Stats
}
