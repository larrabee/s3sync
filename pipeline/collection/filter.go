package collection

import (
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/storage"
	"path/filepath"
	"strings"
)

// FilterObjectsByExt accepts an input object and checks if it matches the filter.
// This filter skips objects with extensions that are not specified in the config.
//
// This filter read configuration from Step.Config and assert it type to []string type.
var FilterObjectsByExt pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	info := group.GetStepInfo(stepNum)
	cfg, ok := info.Config.([]string)
	if !ok {
		errChan <- &pipeline.StepConfigurationError{StepName: info.Name, StepNum: stepNum}
	}
	for obj := range input {
		if ok {
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

// FilterObjectsByExtNot accepts an input object and checks if it matches the filter.
// This filter skips objects with extensions that are specified in the config.
//
// This filter read configuration from Step.Config and assert it type to []string type.
var FilterObjectsByExtNot pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	info := group.GetStepInfo(stepNum)
	cfg, ok := info.Config.([]string)
	if !ok {
		errChan <- &pipeline.StepConfigurationError{StepName: info.Name, StepNum: stepNum}
	}
	for obj := range input {
		if ok {
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

// FilterObjectsByCT accepts an input object and checks if it matches the filter.
// This filter skips objects with Content-Type that are not specified in the config.
//
// This filter read configuration from Step.Config and assert it type to []string type.
var FilterObjectsByCT pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	info := group.GetStepInfo(stepNum)
	cfg, ok := info.Config.([]string)
	if !ok {
		errChan <- &pipeline.StepConfigurationError{StepName: info.Name, StepNum: stepNum}
	}
	for obj := range input {
		if ok {
			flag := false
			for _, ct := range cfg {
				if obj.ContentType == nil && ct == "" {
					flag = true
					break
				} else if obj.ContentType != nil && *obj.ContentType == ct {
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

// FilterObjectsByCTNot accepts an input object and checks if it matches the filter.
// This filter skips objects with Content-Type that are specified in the config.
//
// This filter read configuration from Step.Config and assert it type to []string type.
var FilterObjectsByCTNot pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	info := group.GetStepInfo(stepNum)
	cfg, ok := info.Config.([]string)
	if !ok {
		errChan <- &pipeline.StepConfigurationError{StepName: info.Name, StepNum: stepNum}
	}
	for obj := range input {
		if ok {
			flag := false
			for _, ct := range cfg {
				if obj.ContentType == nil && ct == "" {
					flag = true
					break
				} else if obj.ContentType != nil && *obj.ContentType == ct {
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

// FilterObjectsByMtimeAfter accepts an input object and checks if it matches the filter.
// This filter accepts objects that modified after given unix timestamp.
//
// This filter read configuration from Step.Config and assert it type to int64 type.
var FilterObjectsByMtimeAfter pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	info := group.GetStepInfo(stepNum)
	cfg, ok := info.Config.(int64)
	if !ok {
		errChan <- &pipeline.StepConfigurationError{StepName: info.Name, StepNum: stepNum}
	}
	for obj := range input {
		if ok {
			if obj.Mtime.Unix() >= cfg {
				output <- obj
			}
		}
	}
}

// FilterObjectsByMtimeBefore accepts an input object and checks if it matches the filter.
// This filter accepts objects that modified before given unix timestamp.
//
// This filter read configuration from Step.Config and assert it type to int64 type.
var FilterObjectsByMtimeBefore pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	info := group.GetStepInfo(stepNum)
	cfg, ok := info.Config.(int64)
	if !ok {
		errChan <- &pipeline.StepConfigurationError{StepName: info.Name, StepNum: stepNum}
	}
	for obj := range input {
		if ok {
			if obj.Mtime.Unix() < cfg {
				output <- obj
			}
		}
	}
}

// FilterObjectsModified accepts an input object and checks if it matches the filter
// This filter read object meta from target storage and compare object ETags. If Etags are equal object will be skipped
// For FS storage xattr support are required for proper work.
var FilterObjectsModified pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		destObj := &storage.Object{
			Key:       obj.Key,
			VersionId: obj.VersionId,
		}
		err := group.Target.GetObjectMeta(destObj)
		if (err != nil) || (obj.ETag == nil || destObj.ETag == nil) || (*obj.ETag != *destObj.ETag) {
			output <- obj
		}
	}
}

// FilterObjectsExist accepts an input object and checks if it exist in target storage
// This filter read object meta from target storage. Object will be processed only when it exist in target storage.
var FilterObjectsExist pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		destObj := &storage.Object{
			Key: obj.Key,
		}
		err := group.Target.GetObjectMeta(destObj)
		if err == nil {
			output <- obj
		} else if storage.IsErrNotExist(err) {
			continue
		} else {
			errChan <- &pipeline.ObjectError{Object: obj, Err: err}
		}
	}
}

// FilterObjectsExist accepts an input object and checks if it exist in target storage
// This filter read object meta from target storage. Object will be processed only when it doesn't exist in target storage.
var FilterObjectsExistNot pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		destObj := &storage.Object{
			Key: obj.Key,
		}
		err := group.Target.GetObjectMeta(destObj)
		if err == nil {
			continue
		} else if storage.IsErrNotExist(err) {
			output <- obj
		} else {
			errChan <- &pipeline.ObjectError{Object: obj, Err: err}
		}
	}
}

// FilterObjectsDirs accepts an input object and checks if ends with "/"
// Accept only files  ended with "/"
var FilterObjectsDirs pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		if strings.HasSuffix(*obj.Key, "/") {
			output <- obj
		}
	}
}

// FilterObjectsDirsNot accepts an input object and checks if ends with "/"
// Accept only files NOT ended with "/"
var FilterObjectsDirsNot pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		if !strings.HasSuffix(*obj.Key, "/") {
			output <- obj
		}
	}
}
