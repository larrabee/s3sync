package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"github.com/eapache/channels"
	"github.com/karrick/godirwalk"
	"io/ioutil"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//FSStorage configuration
type FSStorage struct {
	Dir      string
	filePerm os.FileMode
	dirPerm  os.FileMode
	workers  uint
}

//NewFSStorage return new configured FS storage
func NewFSStorage(dir string, filePerm, dirPerm os.FileMode, workers uint) (storage FSStorage) {
	storage.Dir = filepath.Clean(dir) + "/"
	storage.filePerm = filePerm
	storage.dirPerm = dirPerm
	storage.workers = workers
	return storage
}

//List FS and send founded objects to chan
func (storage FSStorage) List(output chan<- Object) error {
	prefixChan := channels.NewInfiniteChannel()
	listResultChan := make(chan error, storage.workers)
	wg := sync.WaitGroup{}
	stopListing := false

	listObjectsRecursive := func(prefixChan *channels.InfiniteChannel, output chan<- Object) {
		buffer := make([]byte, 1024*64)

		for prefix := range prefixChan.Out() {
			if stopListing {
				wg.Done()
				return
			}
			dirents, err := godirwalk.ReadDirents(prefix.(string), buffer)

			if err != nil {
				wg.Done()
				listResultChan <- err
				return
			}

			for _, dirent := range dirents {
				path := filepath.Join(prefix.(string), dirent.Name())
				if dirent.IsDir() {
					wg.Add(1)
					prefixChan.In() <- path
					continue
				} else {
					atomic.AddUint64(&counter.totalObjCnt, 1)
					output <- Object{Key: strings.TrimPrefix(path, storage.Dir)}
				}
			}
			wg.Done()
		}
	}

	for i := storage.workers; i != 0; i-- {
		go listObjectsRecursive(prefixChan, output)
	}

	// Start listing from storage.prefix
	wg.Add(1)
	prefixChan.In() <- storage.Dir

	go func() {
		wg.Wait()
		prefixChan.Close()
		listResultChan <- nil
	}()

	select {
	case msg := <-listResultChan:
		stopListing = true
		wg.Wait()
		close(output)
		return msg
	}
}

//PutObject save object to FS
func (storage FSStorage) PutObject(obj *Object) error {
	destPath := filepath.Join(storage.Dir, obj.Key)
	err := os.MkdirAll(filepath.Dir(destPath), storage.dirPerm)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(destPath, obj.Content, storage.filePerm)
	if err != nil {
		return err
	}
	return nil
}

//GetObjectContent read object content from FS
func (storage FSStorage) GetObjectContent(obj *Object) (err error) {
	destPath := filepath.Join(storage.Dir, obj.Key)
	obj.Content, err = ioutil.ReadFile(destPath)
	if err != nil {
		return err
	}

	obj.ContentType = mime.TypeByExtension(filepath.Ext(destPath))
	fileInfo, err := os.Stat(destPath)
	if err != nil {
		return err
	}
	obj.ETag = etagFromMetadata(fileInfo.ModTime(), fileInfo.Size())
	obj.Mtime = fileInfo.ModTime()
	return nil
}

//GetObjectMeta update object metadata from FS
func (storage FSStorage) GetObjectMeta(obj *Object) (err error) {
	destPath := filepath.Join(storage.Dir, obj.Key)

	obj.ContentType = mime.TypeByExtension(filepath.Ext(destPath))
	fileInfo, err := os.Stat(destPath)
	if err != nil {
		return err
	}
	obj.ETag = etagFromMetadata(fileInfo.ModTime(), fileInfo.Size())
	obj.Mtime = fileInfo.ModTime()
	return nil
}

//GetStorageType return storage type
func (storage FSStorage) GetStorageType() ConnType {
	return fsConn
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
