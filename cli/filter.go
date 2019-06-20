package main

import (
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/storage"
	"path/filepath"
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
			}
		}
	}
}

var FilterObjectsByTimestamp pipeline.PipelineFn = func(group *pipeline.Group, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			errChan <- group.Ctx.Err()
			return
		default:
			if obj.Mtime.Unix() > cli.FilterMtimeAfter {
				output <- obj
			}
		}
	}
}

var FilterObjectsByTimestampNot pipeline.PipelineFn = func(group *pipeline.Group, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			errChan <- group.Ctx.Err()
			return
		default:
			if obj.Mtime.Unix() < cli.FilterMtimeBefore {
				output <- obj
			}
		}
	}
}
