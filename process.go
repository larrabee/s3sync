package main

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type Counter struct {
	sucObjCnt   uint64
	failObjCnt  uint64
	skipObjCnt  uint64
	totalObjCnt uint64
	startTime   time.Time
}

func FailedObjAction(obj Object) {
	atomic.AddUint64(&counter.failObjCnt, 1)
	log.Fatalf("Failed to sync object: %s", obj.Key)
}

func FilterObject(obj *Object) bool {
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

	// Filter object by modify time
	if (cli.FilterTimestamp > 0) && (obj.Mtime.Unix() < cli.FilterTimestamp) {
		return true
	}
	return false
}

func ProcessObj(ch <-chan Object, wg *sync.WaitGroup) {
Main:
	for obj := range ch {
		// Filter objects
		if FilterObject(&obj) {
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
					FailedObjAction(obj)
					continue Main
				}
				time.Sleep(cli.RetrySleepInterval)
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
					FailedObjAction(obj)
					continue Main
				}
				time.Sleep(cli.RetrySleepInterval)
				continue
			}
		}
		atomic.AddUint64(&counter.sucObjCnt, 1)
	}
	wg.Done()
}
