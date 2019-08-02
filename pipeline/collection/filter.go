package collection

import (
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/storage"
	"github.com/sirupsen/logrus"
	"path/filepath"
)

// FilterObjectsByExt accepts an input object and checks if it matches the filter
// This filter read configuration from Step.Config and assert it type to []string type
// This filter skips objects with extensions that are not specified in the config.
var FilterObjectsByExt pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	info := group.GetStepInfo(stepNum)
	cfg, ok := info.Config.([]string)
	if !ok {
		errChan <- &pipeline.StepConfigurationError{StepName: info.Name, StepNum: stepNum}
		return
	}
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			return
		default:
			flag := false
			fileExt := filepath.Ext(*obj.Key)
			for _, ext := range cfg {
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

// FilterObjectsByExtNot accepts an input object and checks if it matches the filter
// This filter read configuration from Step.Config and assert it type to []string type
// This filter skips objects with extensions that are specified in the config.
var FilterObjectsByExtNot pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	info := group.GetStepInfo(stepNum)
	cfg, ok := info.Config.([]string)
	if !ok {
		errChan <- &pipeline.StepConfigurationError{StepName: info.Name, StepNum: stepNum}
		return
	}
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			return
		default:
			flag := false
			fileExt := filepath.Ext(*obj.Key)
			for _, ext := range cfg {
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

// FilterObjectsByCT accepts an input object and checks if it matches the filter
// This filter read configuration from Step.Config and assert it type to []string type
// This filter skips objects with Content-Type that are not specified in the config.
var FilterObjectsByCT pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	info := group.GetStepInfo(stepNum)
	cfg, ok := info.Config.([]string)
	if !ok {
		errChan <- &pipeline.StepConfigurationError{StepName: info.Name, StepNum: stepNum}
		return
	}
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			return
		default:
			flag := false
			for _, ct := range cfg {
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

// FilterObjectsByCTNot accepts an input object and checks if it matches the filter
// This filter read configuration from Step.Config and assert it type to []string type
// This filter skips objects with Content-Type that are specified in the config.
var FilterObjectsByCTNot pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	info := group.GetStepInfo(stepNum)
	cfg, ok := info.Config.([]string)
	if !ok {
		errChan <- &pipeline.StepConfigurationError{StepName: info.Name, StepNum: stepNum}
		return
	}
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			return
		default:
			flag := false
			for _, ct := range cfg {
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

// FilterObjectsByMtimeAfter accepts an input object and checks if it matches the filter
// This filter read configuration from Step.Config and assert it type to int64 type
// This filter accepts objects that modified after given unix timestamp
var FilterObjectsByMtimeAfter pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	info := group.GetStepInfo(stepNum)
	cfg, ok := info.Config.(int64)
	if !ok {
		errChan <- &pipeline.StepConfigurationError{StepName: info.Name, StepNum: stepNum}
		return
	}
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			return
		default:
			if obj.Mtime.Unix() > cfg {
				output <- obj
			}
		}
	}
}

// FilterObjectsByMtimeBefore accepts an input object and checks if it matches the filter
// This filter read configuration from Step.Config and assert it type to int64 type
// This filter accepts objects that modified before given unix timestamp
var FilterObjectsByMtimeBefore pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	info := group.GetStepInfo(stepNum)
	cfg, ok := info.Config.(int64)
	if !ok {
		errChan <- &pipeline.StepConfigurationError{StepName: info.Name, StepNum: stepNum}
		return
	}
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			return
		default:
			if obj.Mtime.Unix() < cfg {
				output <- obj
			}
		}
	}
}

// FilterObjectsModified accepts an input object and checks if it matches the filter
// This filter gets object meta from target storage and compare object ETags. If Etags are equal object will be skipped
// For FS storage xattr support are required for proper work.
var FilterObjectsModified pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			return
		default:
			destObj := &storage.Object{
				Key:       obj.Key,
				VersionId: obj.VersionId,
			}
			err := group.Target.GetObjectMeta(destObj)
			if (err != nil) || (obj.ETag == nil || destObj.ETag == nil) || (*obj.ETag != *destObj.ETag) {
				logrus.Infof("%s : %s", obj.ETag, destObj.ETag)
				output <- obj
			}
		}
	}
}
