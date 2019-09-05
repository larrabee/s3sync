// Package collection contains different StepFn functions to do different pipeline actions.
package collection

import (
	"context"
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/storage"
)

// LoadObjectMeta accepts an input object and downloads its metadata.
var LoadObjectMeta pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			return
		default:
			err := group.Source.GetObjectMeta(obj)
			if err != nil {
				if err == context.Canceled {
					errChan <- err
				} else {
					errChan <- &pipeline.ObjectError{Object:obj, Err: err}
				}
			} else {
				output <- obj
			}
		}
	}
}

// LoadObjectData accepts an input object and downloads its content and metadata.
var LoadObjectData pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			return
		default:
			err := group.Source.GetObjectContent(obj)
			if err != nil {
				if err == context.Canceled {
					errChan <- err
				} else {
					errChan <- &pipeline.ObjectError{Object:obj, Err: err}
				}
			} else {
				output <- obj
			}
		}
	}
}
