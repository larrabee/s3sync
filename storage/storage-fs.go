package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/karrick/godirwalk"
	"github.com/larrabee/ratelimit"
	"github.com/pkg/xattr"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// FSStorage configuration
type FSStorage struct {
	Dir      string
	filePerm os.FileMode
	dirPerm  os.FileMode
	bufSize  int
	xattr    bool
	ctx      context.Context
	rlBucket ratelimit.Bucket
}

// NewFSStorage return new configured FS storage
func NewFSStorage(dir string, filePerm, dirPerm os.FileMode, bufSize int, extendedMeta bool) *FSStorage {
	storage := FSStorage{
		Dir:      filepath.Clean(dir) + "/",
		filePerm: filePerm,
		dirPerm:  dirPerm,
		xattr:    extendedMeta,
		rlBucket: ratelimit.NewFakeBucket(),
	}
	if bufSize < godirwalk.MinimumScratchBufferSize {
		storage.bufSize = godirwalk.DefaultScratchBufferSize
	} else {
		storage.bufSize = bufSize
	}
	return &storage
}

// WithContext add's context to storage
func (storage *FSStorage) WithContext(ctx context.Context) {
	storage.ctx = ctx
}

// WithRateLimit set rate limit (bytes/sec) for storage
func (storage *FSStorage) WithRateLimit(limit int) error {
	bucket, err := ratelimit.NewBucketWithRate(float64(limit), int64(limit))
	if err != nil {
		return err
	}
	storage.rlBucket = bucket
	return nil
}

// List FS and send founded objects to chan
func (storage *FSStorage) List(output chan<- *Object) error {
	listObjectsFn := func(path string, de *godirwalk.Dirent) error {
		select {
		case <-storage.ctx.Done():
			return storage.ctx.Err()
		default:
			if de.IsRegular() {
				key := strings.TrimPrefix(path, storage.Dir)
				output <- &Object{Key: &key}
			}
			if de.IsSymlink() {
				pathTarget, err := filepath.EvalSymlinks(path)
				if err != nil {
					return err
				}
				symStat, err := os.Stat(pathTarget)
				if err != nil {
					return err
				}
				if !symStat.IsDir() {
					key := strings.TrimPrefix(path, storage.Dir)
					output <- &Object{Key: &key}
				}
			}
			return nil
		}
	}

	err := godirwalk.Walk(storage.Dir, &godirwalk.Options{
		FollowSymbolicLinks: true,
		Unsorted:            true,
		ScratchBuffer:       make([]byte, storage.bufSize),
		Callback:            listObjectsFn,
	})
	if err != nil {
		return err
	}
	return nil
}

// PutObject saves object to FS
func (storage *FSStorage) PutObject(obj *Object) error {
	destPath := filepath.Join(storage.Dir, *obj.Key)
	err := os.MkdirAll(filepath.Dir(destPath), storage.dirPerm)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, storage.filePerm)
	if err != nil {
		return err
	}
	defer f.Close()

	objReader := bytes.NewReader(*obj.Content)
	if _, err := io.Copy(f, ratelimit.NewReader(objReader, storage.rlBucket)); err != nil {
		return err
	}

	if storage.xattr {
		data, err := json.Marshal(obj)
		if err != nil {
			return err
		}

		if err := xattr.FSet(f, "user.s3sync.meta", data); err != nil {
			return err
		}
	}

	return nil
}

// GetObjectContent read object content and metadata from FS
func (storage *FSStorage) GetObjectContent(obj *Object) error {
	destPath := filepath.Join(storage.Dir, *obj.Key)
	f, err := os.Open(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	fileInfo, err := f.Stat()
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(make([]byte, 0, fileInfo.Size()))
	if _, err := io.Copy(buf, ratelimit.NewReader(f, storage.rlBucket)); err != nil {
		return err
	}

	data := buf.Bytes()

	obj.Content = &data

	if storage.xattr {
		if data, err := xattr.FGet(f, "user.s3sync.meta"); err == nil {
			err := json.Unmarshal(data, obj)
			if err != nil {
				return err
			}
		} else {
			switch err.(type) {
			case *xattr.Error:
				if err.(*xattr.Error).Err == syscall.ENODATA {
					contentType := mime.TypeByExtension(filepath.Ext(destPath))
					Mtime := fileInfo.ModTime()
					obj.ContentType = &contentType
					obj.Mtime = &Mtime
					break
				} else {
					return err
				}
			default:
				return err
			}
		}
	} else {
		contentType := mime.TypeByExtension(filepath.Ext(destPath))
		Mtime := fileInfo.ModTime()
		obj.ContentType = &contentType
		obj.Mtime = &Mtime
	}

	return nil
}

// GetObjectMeta update object metadata from FS
func (storage *FSStorage) GetObjectMeta(obj *Object) error {
	destPath := filepath.Join(storage.Dir, *obj.Key)
	f, err := os.Open(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	fileInfo, err := f.Stat()
	if err != nil {
		return err
	}

	if storage.xattr {
		if data, err := xattr.FGet(f, "user.s3sync.meta"); err == nil {
			err := json.Unmarshal(data, obj)
			if err != nil {
				return err
			}
		} else {
			switch err.(type) {
			case *xattr.Error:
				if err.(*xattr.Error).Err == syscall.ENODATA {
					contentType := mime.TypeByExtension(filepath.Ext(destPath))
					Mtime := fileInfo.ModTime()
					obj.ContentType = &contentType
					obj.Mtime = &Mtime
					break
				} else {
					return err
				}
			default:
				return err
			}
		}
	} else {
		contentType := mime.TypeByExtension(filepath.Ext(destPath))
		Mtime := fileInfo.ModTime()
		obj.ContentType = &contentType
		obj.Mtime = &Mtime
	}

	return nil
}

// DeleteObject remove object from FS
func (storage *FSStorage) DeleteObject(obj *Object) error {
	destPath := filepath.Join(storage.Dir, *obj.Key)
	err := os.Remove(destPath)
	if err != nil {
		return err
	}

	return nil
}

// GetStorageType return storage type
func (storage *FSStorage) GetStorageType() Type {
	return TypeFS
}
