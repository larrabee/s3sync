package collection

import (
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/storage"
	"github.com/sirupsen/logrus"
)

var Terminator pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for range input {
		select {
		case <-group.Ctx.Done():
			return
		default:
			continue
		}
	}
}

var Logger pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	info := group.GetStepInfo(stepNum)
	cfg, ok := info.Config.(*logrus.Logger)
	if !ok {
		errChan <- &pipeline.StepConfigurationError{StepName: info.Name, StepNum: stepNum}
		return
	}
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			return
		default:
			cfg.Infof("Key: %s", *obj.Key)
			output <- obj
		}
	}
}

var ACLUpdater pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	info := group.GetStepInfo(stepNum)
	cfg, ok := info.Config.(string)
	if !ok {
		errChan <- &pipeline.StepConfigurationError{StepName: info.Name, StepNum: stepNum}
		return
	}
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			return
		default:
			obj.ACL = &cfg
			output <- obj
		}
	}
}
