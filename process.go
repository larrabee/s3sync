package main

import (
	"path/filepath"
	"time"
	"sync/atomic"
)

type object struct {
	Key         string
	ETag        string
	Mtime       time.Time
	ErrCount    uint
	Content     []byte
	ContentType string
}

func ProcessFailedObj(ch chan<- object, failCh <-chan object) {
	for obj := range failCh {
		if obj.ErrCount == cli.Retry {
			log.Fatalf("Failed to sync object: %s", obj.Key)
			//failObjCnt++
			//continue
		}
		obj.ErrCount++
		ch <- obj
	}
}

func ProcessObj(ch chan object, failCh chan<- object) {
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

		if err := syncGr.Source.GetObjectContent(&obj); err != nil {
			log.Debugf("Getting obj %s failed with err: %s", obj.Key, err)
			failCh <- obj
			continue
		}
		if err := syncGr.Target.PutObject(&obj); err != nil {
			log.Debugf("Putting obj %s failed with err: %s", obj.Key, err)
			failCh <- obj
			continue
		}
		atomic.AddUint64(&sucObjCnt, 1)
	}
}
