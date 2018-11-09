package main

import (
	"time"
)

//Object contain content and metadata of S3 object
type Object struct {
	Key         string
	ETag        string
	Mtime       time.Time
	Content     []byte
	ContentType string
}

//SyncGroup contain Source and Target configuration. Thread safe
type SyncGroup struct {
	Source Storage
	Target Storage
}

//Storage interface
type Storage interface {
	List(ch chan<- Object) error
	PutObject(object *Object) error
	GetObjectContent(obj *Object) error
	GetObjectMeta(obj *Object) error
	GetStorageType() ConnType
}
