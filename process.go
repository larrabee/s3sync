package main

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

//Counter collect statistic and sync progress
type Counter struct {
	sucObjCnt   uint64
	failObjCnt  uint64
	skipObjCnt  uint64
	totalObjCnt uint64
	startTime   time.Time
}

func failedObjAction(obj *Object, objErr error) {
	atomic.AddUint64(&counter.failObjCnt, 1)
	switch cli.OnFail {
	case onFailLog:
		log.Errorf("Failed to sync object: %s, err: %s, skipping it\n", obj.Key, objErr)
	case onFailFatal:
		log.Fatalf("Failed to sync object: %s, err: %s, exiting\n", obj.Key, objErr)
	case onFailIgnoreMissing:
		if objErr != nil {
			if aerr, ok := objErr.(awserr.Error); ok {
				switch aerr.Code() {
				case s3.ErrCodeNoSuchKey:
					log.Errorf("Object not found: %s, skipping it\n", obj.Key)
				case "NotFound":
					log.Errorf("Object not found: %s, skipping it\n", obj.Key)
				default:
					log.Fatalf("Failed to sync object: %s, err: %s, exiting\n", obj.Key, objErr)
				}
			} else {
				log.Fatalf("Failed to sync object: %s, err: %s, exiting\n", obj.Key, objErr)
			}
		}
	}
}

func filterObject(obj *Object) bool {
	// Filter object by extension
	if len(cli.FilterExtension) > 0 {
		flag := false
		fileExt := filepath.Ext(obj.Key)
		for _, ext := range cli.FilterExtension {
			if fileExt == ext {
				flag = true
				break
			}
		}
		if flag == false {
			return true
		}
	}

	// Filter object by Content-Type
	if len(cli.FilterContentType) > 0 {
		flag := false
		for _, ct := range cli.FilterContentType {
			if obj.ContentType == ct {
				flag = true
				break
			}
		}
		if flag == false {
			return true
		}
	}

	// Revert Filter object by extension
	if len(cli.FilterRevertExtension) > 0 {
		flag := false
		fileExt := filepath.Ext(obj.Key)
		for _, ext := range cli.FilterRevertExtension {
			if fileExt == ext {
				flag = true
				break
			}
		}
		if flag == true {
			return true
		}
	}

	// Revert Filter object by Content-Type
	if len(cli.FilterRevertContentType) > 0 {
		flag := false
		for _, ct := range cli.FilterRevertContentType {
			if obj.ContentType == ct {
				flag = true
				break
			}
		}
		if flag == true {
			return true
		}
	}

	// Filter object by modify time
	if (cli.FilterTimestamp > 0) && (obj.Mtime.Unix() < cli.FilterTimestamp) {
		return true
	}

	// Revert Filter object by modify time
	if (cli.FilterRevertTimestamp > 0) && (obj.Mtime.Unix() > cli.FilterRevertTimestamp) {
		return true
	}

	return false
}

func processObj(ch <-chan Object, wg *sync.WaitGroup) {
Main:
	for obj := range ch {

		// Get Obj Metadata
		for i := uint(0); i <= cli.Retry; i++ {
			if err := syncGr.Source.GetObjectMeta(&obj); err == nil {
				break
			} else {
				log.Debugf("Getting obj metadata %s failed with err: %s", obj.Key, err)
				if i == cli.Retry {
					failedObjAction(&obj, err)
					continue Main
				}
				time.Sleep(cli.RetryInterval)
				continue
			}
		}

		// Filter objects
		if filterObject(&obj) {
			atomic.AddUint64(&counter.skipObjCnt, 1)
			continue
		}

		// Download object
		for i := uint(0); i <= cli.Retry; i++ {
			if err := syncGr.Source.GetObjectContent(&obj); err == nil {
				break
			} else {
				log.Debugf("Getting obj %s failed with err: %s", obj.Key, err)
				if i == cli.Retry {
					failedObjAction(&obj, err)
					continue Main
				}
				time.Sleep(cli.RetryInterval)
				continue
			}
		}

		// Upload object
		for i := uint(0); i <= cli.Retry; i++ {
			if err := syncGr.Target.PutObject(&obj); err == nil {
				break
			} else {
				log.Debugf("Putting obj %s failed with err: %s", obj.Key, err)
				if i == cli.Retry {
					failedObjAction(&obj, err)
					continue Main
				}
				time.Sleep(cli.RetryInterval)
				continue
			}
		}
		atomic.AddUint64(&counter.sucObjCnt, 1)
	}
	wg.Done()
}
