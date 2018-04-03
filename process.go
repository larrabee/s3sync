package main

import (
	"path/filepath"
	"sync/atomic"
	"time"
	"sync"
)

type object struct {
	Key         string
	ETag        string
	Mtime       time.Time
	Content     []byte
	ContentType string
}

func FailedObjAction(obj object) {
	log.Fatalf("Failed to sync object: %s", obj.Key)
}

func ProcessObj(ch <-chan object, wg *sync.WaitGroup) {
Main:
	for obj := range ch {
		// Filter objects by extension
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
				atomic.AddUint64(&skipObjCnt, 1)
				continue
			}
		}

		// Filter objects by modify time
		if (cli.FilterTimestamp > 0) && (obj.Mtime.Unix() < cli.FilterTimestamp) {
			atomic.AddUint64(&skipObjCnt, 1)
			continue
		}
		for i := uint(0); i <= cli.Retry; i++ {
			if err := syncGr.Source.GetObjectContent(&obj); err == nil {
				break
			} else {
				log.Debugf("Getting obj %s failed with err: %s", obj.Key, err)
				if i == cli.Retry {
					FailedObjAction(obj)
					continue Main
				}
				time.Sleep(onFailSleepDuration)
				continue
			}
		}
		for i := uint(0); i <= cli.Retry; i++ {
			if err := syncGr.Target.PutObject(&obj); err == nil {
				break
			} else {
				log.Debugf("Putting obj %s failed with err: %s", obj.Key, err)
				if i == cli.Retry {
					FailedObjAction(obj)
					continue Main
				}
				time.Sleep(onFailSleepDuration)
				continue
			}
		}
		atomic.AddUint64(&sucObjCnt, 1)
	}
	wg.Done()
}
