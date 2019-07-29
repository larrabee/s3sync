package collection

import (
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/storage"
	"github.com/sirupsen/logrus"
	"path/filepath"
)

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
