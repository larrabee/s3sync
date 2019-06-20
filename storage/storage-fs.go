package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"github.com/karrick/godirwalk"
	"io/ioutil"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"
)

//FSStorage configuration
type FSStorage struct {
	Dir      string
	filePerm os.FileMode
	dirPerm  os.FileMode
	bufSize  int
	ctx      context.Context
}

//NewFSStorage return new configured FS storage
func NewFSStorage(dir string, filePerm, dirPerm os.FileMode, bufSize int) *FSStorage {
	storage := FSStorage{
		Dir:      filepath.Clean(dir) + "/",
		filePerm: filePerm,
		dirPerm:  dirPerm,
	}
	if bufSize < godirwalk.MinimumScratchBufferSize {
		storage.bufSize = godirwalk.DefaultScratchBufferSize
	} else {
		storage.bufSize = bufSize
	}
	return &storage
}

func (storage *FSStorage) WithContext(ctx context.Context) {
	storage.ctx = ctx
}

//List FS and send founded objects to chan
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

//PutObject save object to FS
func (storage *FSStorage) PutObject(obj *Object) error {
	destPath := filepath.Join(storage.Dir, *obj.Key)
	err := os.MkdirAll(filepath.Dir(destPath), storage.dirPerm)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(destPath, *obj.Content, storage.filePerm)
	if err != nil {
		return err
	}

	return nil
}

//GetObjectContent read object content from FS
func (storage *FSStorage) GetObjectContent(obj *Object) error {
	destPath := filepath.Join(storage.Dir, *obj.Key)
	data, err := ioutil.ReadFile(destPath)
	if err != nil {
		return err
	}

	fileInfo, err := os.Stat(destPath)
	if err != nil {
		return err
	}

	contentType := mime.TypeByExtension(filepath.Ext(destPath))
	ETag := etagFromMetadata(fileInfo.ModTime(), fileInfo.Size())
	Mtime := fileInfo.ModTime()

	obj.Content = &data
	obj.ETag = &ETag
	obj.ContentType = &contentType
	obj.Mtime = &Mtime

	return nil
}

//GetObjectMeta update object metadata from FS
func (storage *FSStorage) GetObjectMeta(obj *Object) error {
	destPath := filepath.Join(storage.Dir, *obj.Key)
	fileInfo, err := os.Stat(destPath)
	if err != nil {
		return err
	}

	contentType := mime.TypeByExtension(filepath.Ext(destPath))
	ETag := etagFromMetadata(fileInfo.ModTime(), fileInfo.Size())
	Mtime := fileInfo.ModTime()

	obj.ETag = &ETag
	obj.ContentType = &contentType
	obj.Mtime = &Mtime

	return nil
}

//DeleteObject remove object from FS
func (storage *FSStorage) DeleteObject(obj *Object) error {
	destPath := filepath.Join(storage.Dir, *obj.Key)
	err := os.Remove(destPath)
	if err != nil {
		return err
	}

	return nil
}

//GetStorageType return storage type
func (storage *FSStorage) GetStorageType() Type {
	return TypeFS
}

//etagFromMetadata generate ETAG from FS attributes. Useful for further use
func etagFromMetadata(mtime time.Time, size int64) string {
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
