package collection

import (
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/storage"
)

var ListSourceStorage pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	select {
	case <-group.Ctx.Done():
		return
	default:
		err := group.Source.List(output)
		if err != nil {
			errChan <- err
		}
	}
	return
}
