package main

import (
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/storage"
	"path/filepath"
	"sync/atomic"
)

var FilterObjectsByExt pipeline.PipelineFn = func(group *pipeline.Group, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			errChan <- group.Ctx.Err()
			return
		default:
			flag := false
			fileExt := filepath.Ext(*obj.Key)
			for _, ext := range cli.FilterExt {
				if fileExt == ext {
					flag = true
					break
				}
			}
			if flag {
				output <- obj
			} else {
				atomic.AddUint64(&filteredCnt, 1)
			}
		}
	}
}

var FilterObjectsByExtNot pipeline.PipelineFn = func(group *pipeline.Group, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			errChan <- group.Ctx.Err()
			return
		default:
			flag := false
			fileExt := filepath.Ext(*obj.Key)
			for _, ext := range cli.FilterExtNot {
				if fileExt == ext {
					flag = true
					break
				}
			}
			if !flag {
				output <- obj
			} else {
				atomic.AddUint64(&filteredCnt, 1)
			}
		}
	}
}

var FilterObjectsByCT pipeline.PipelineFn = func(group *pipeline.Group, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			errChan <- group.Ctx.Err()
			return
		default:
			flag := false
			for _, ct := range cli.FilterCT {
				if *obj.ContentType == ct {
					flag = true
					break
				}
			}
			if flag {
				output <- obj
			} else {
				atomic.AddUint64(&filteredCnt, 1)
			}
		}
	}
}

var FilterObjectsByCTNot pipeline.PipelineFn = func(group *pipeline.Group, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			errChan <- group.Ctx.Err()
			return
		default:
			flag := false
			for _, ct := range cli.FilterCTNot {
				if *obj.ContentType == ct {
					flag = true
					break
				}
			}
			if !flag {
				output <- obj
			} else {
				atomic.AddUint64(&filteredCnt, 1)
			}
		}
	}
}

var FilterObjectsByMtimeAfter pipeline.PipelineFn = func(group *pipeline.Group, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			errChan <- group.Ctx.Err()
			return
		default:
			if obj.Mtime.Unix() > cli.FilterMtimeAfter {
				output <- obj
			} else {
				atomic.AddUint64(&filteredCnt, 1)
			}
		}
	}
}

var FilterObjectsByMtimeBefore pipeline.PipelineFn = func(group *pipeline.Group, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			errChan <- group.Ctx.Err()
			return
		default:
			if obj.Mtime.Unix() < cli.FilterMtimeBefore {
				output <- obj
			} else {
				atomic.AddUint64(&filteredCnt, 1)
			}
		}
	}
}
