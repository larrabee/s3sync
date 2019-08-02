package collection

import (
	"github.com/larrabee/ratelimit"
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/storage"
	"github.com/sirupsen/logrus"
)

// Terminator like a /dev/null
//
// It read objects from input and do not nothing.
// Pipeline should end with Terminator.
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

// Logger read objects from input, print object name with Log and send object no next pipeline steps.
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

// ACLUpdater read objects from input and update its ACL.
// This filter read configuration from Step.Config and assert it type to string type.
// ACL is S3 attribute, its not related with FS permissions.
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

// PipelineRateLimit read objects from input and slow down pipeline processing speed to given rate (obj/sec).
//
// This filter read configuration from Step.Config and assert it type to uint type.
var PipelineRateLimit pipeline.StepFn = func(group *pipeline.Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	info := group.GetStepInfo(stepNum)
	cfg, ok := info.Config.(uint)
	if !ok {
		errChan <- &pipeline.StepConfigurationError{StepName: info.Name, StepNum: stepNum}
		return
	}
	bucket, err := ratelimit.NewBucketWithRate(float64(cfg), int64(cfg*2))
	if err != nil {
		errChan <- err
		return
	}
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			return
		default:
			bucket.Wait(1)
			output <- obj
		}
	}
}
