package main

import (
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/storage"
)

var ListSourceStorage pipeline.PipelineFn = func(group *pipeline.Group, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	select {
	case <-group.Ctx.Done():
		errChan <- group.Ctx.Err()
	default:
		err := group.Source.List(output)
		if err != nil {
			errChan <- err
		}
	}
	return
}
