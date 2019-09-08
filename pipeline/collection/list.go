package collection

import (
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/storage"
)

// ListSourceStorage list files in source storage and send it's to next pipeline steps.
var ListSourceStorage pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	err := group.Source.List(output)
	if err != nil {
		errChan <- err
	}
	return
}
