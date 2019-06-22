package main

import (
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/storage"
)

var Terminator pipeline.PipelineFn = func(group *pipeline.Group, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for range input {
		select {
		case <-group.Ctx.Done():
			errChan <- group.Ctx.Err()
			return
		default:
			continue
		}
	}
}

var Logger pipeline.PipelineFn = func(group *pipeline.Group, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			errChan <- group.Ctx.Err()
			return
		default:
			log.Infof("Key: %s", *obj.Key)
			output <- obj
		}
	}
}

var ACLUpdater pipeline.PipelineFn = func(group *pipeline.Group, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			errChan <- group.Ctx.Err()
			return
		default:
			obj.ACL = &cli.S3Acl
			output <- obj
		}
	}
}
