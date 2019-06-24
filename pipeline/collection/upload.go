package collection

import (
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/storage"
)

var UploadObjectData pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			return
		default:
			err := group.Target.PutObject(obj)
			if err != nil {
				errChan <- err
			} else {
				output <- obj
			}
		}
	}
}
