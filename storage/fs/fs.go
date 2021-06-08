package fs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"github.com/karrick/godirwalk"
	"github.com/larrabee/ratelimit"
	"github.com/larrabee/s3sync/storage"
	"github.com/pkg/xattr"
)

const tempFileSuffixLen = 8

// FSStorage configuration.
type FSStorage struct {
	dir           string
	filePerm      os.FileMode
	dirPerm       os.FileMode
	bufSize       int
	xattr         bool
	ctx           context.Context
	rlBucket      ratelimit.Bucket
	listErrorMask storage.ErrHandlingMask
	atomicWrite   bool
}

// NewFSStorage return new configured FS storage.
//
// You should always create new storage with this constructor.
func NewFSStorage(dir string, filePerm, dirPerm os.FileMode, bufSize int, extendedMeta bool, listErrorMode storage.ErrHandlingMask, atomicWrite bool) *FSStorage {
	st := FSStorage{
		dir:           filepath.Clean(dir) + "/",
		filePerm:      filePerm,
		dirPerm:       dirPerm,
		xattr:         extendedMeta && isXattrSupported(),
		rlBucket:      ratelimit.NewFakeBucket(),
		listErrorMask: listErrorMode,
	}

	if extendedMeta && !isXattrSupported() {
		storage.Log.Warnf("Xattr switch enabled, but your system does not support xattr, it will be disabled.")
	}

	if bufSize < godirwalk.MinimumScratchBufferSize {
		st.bufSize = godirwalk.MinimumScratchBufferSize
	} else {
		st.bufSize = bufSize
	}
	return &st
}

// WithContext add's context to storage.
func (st *FSStorage) WithContext(ctx context.Context) {
	st.ctx = ctx
}

// WithRateLimit set rate limit (bytes/sec) for storage.
func (st *FSStorage) WithRateLimit(limit int) error {
	bucket, err := ratelimit.NewBucketWithRate(float64(limit), int64(limit))
	if err != nil {
		return err
	}
	st.rlBucket = bucket
	return nil
}

// List FS and send founded objects to chan.
func (st *FSStorage) List(output chan<- *storage.Object) error {
	listObjectsFn := func(path string, de *godirwalk.Dirent) error {
		select {
		case <-st.ctx.Done():
			return st.ctx.Err()
		default:
			if de.IsRegular() {
				key := strings.TrimPrefix(path, st.dir)
				output <- &storage.Object{Key: &key}
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
					key := strings.TrimPrefix(path, st.dir)
					output <- &storage.Object{Key: &key}
				}
			}
			return nil
		}
	}

	listObjectsErrorFn := func(path string, err error) godirwalk.ErrorAction {

		if st.listErrorMask.Has(storage.HandleErrPermission) && errors.Is(err, os.ErrPermission) {
			storage.Log.Debugf("FS Listing: %s, err: Permission Denied, skipping", path)
			return godirwalk.SkipNode
		} else if st.listErrorMask.Has(storage.HandleErrNotExist) && errors.Is(err, os.ErrNotExist) {
			storage.Log.Debugf("FS Listing: %s, err: No such file or directory, skipping", path)
			return godirwalk.SkipNode
		} else if st.listErrorMask.Has(storage.HandleErrOther) {
			storage.Log.Debugf("FS Listing: %s, err: %s, skipping", path, err)
			return godirwalk.SkipNode
		}

		return godirwalk.Halt
	}

	err := godirwalk.Walk(st.dir, &godirwalk.Options{
		FollowSymbolicLinks: true,
		Unsorted:            true,
		ScratchBuffer:       make([]byte, st.bufSize),
		Callback:            listObjectsFn,
		ErrorCallback:       listObjectsErrorFn,
		AllowNonDirectory:   true,
	})
	if err != nil {
		return err
	}
	return nil
}

// PutObject saves object to FS.
func (st *FSStorage) PutObject(obj *storage.Object) error {
	originalPath := filepath.Join(st.dir, *obj.Key)
	destPath := originalPath
	if st.atomicWrite {
		destPath += ".temp." + storage.GetInsecureRandString(tempFileSuffixLen)
	}

	err := os.MkdirAll(filepath.Dir(destPath), st.dirPerm)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, st.filePerm)
	if err != nil {
		return err
	}
	defer f.Close()

	objReader := bytes.NewReader(*obj.Content)
	if _, err := io.Copy(f, ratelimit.NewReader(objReader, st.rlBucket)); err != nil {
		return err
	}

	if st.xattr {
		data, err := json.Marshal(obj)
		if err != nil {
			return err
		}

		if err := xattr.FSet(f, "user.s3sync.meta", data); err != nil {
			return err
		}
	}

	if st.atomicWrite {
		if err := os.Rename(destPath, originalPath); err != nil {
			return err
		}
	}

	return nil
}

// GetObjectContent read object content and metadata from FS.
func (st *FSStorage) GetObjectContent(obj *storage.Object) error {
	destPath := filepath.Join(st.dir, *obj.Key)
	f, err := os.Open(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	data, err := ioutil.ReadAll(ratelimit.NewReader(f, st.rlBucket))
	if err != nil {
		return err
	}

  dataSize := int64(len(data))

	obj.Content = &data
  obj.ContentLength = &dataSize

	if err := st.GetObjectMeta(obj); err != nil {
		return err
	}

	return nil
}

func (st *FSStorage) GetObjectACL(obj *storage.Object) error {
	return st.GetObjectMeta(obj)
}

// GetObjectMeta update object metadata from FS.
func (st *FSStorage) GetObjectMeta(obj *storage.Object) error {
	destPath := filepath.Join(st.dir, *obj.Key)
	f, err := os.Open(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	fileInfo, err := f.Stat()
	if err != nil {
		return err
	}

	if st.xattr {
		if data, err := xattr.FGet(f, "user.s3sync.meta"); err == nil {
			err := json.Unmarshal(data, obj)
			if err != nil {
				return err
			}
		} else {
			switch err.(type) {
			case *xattr.Error:
				if isNoXattrData(err) {
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

// DeleteObject remove object from FS.
func (st *FSStorage) DeleteObject(obj *storage.Object) error {
	destPath := filepath.Join(st.dir, *obj.Key)
	err := os.Remove(destPath)
	if err != nil {
		return err
	}

	return nil
}
