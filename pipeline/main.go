// Package pipeline provides functions to build synchronisation pipeline.
package pipeline

import (
	"github.com/larrabee/s3sync/storage"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

// Log implement Logrus logger for debug logging.
var Log = logrus.New()

// Group store a Source and Target storage's and pipeline configuration.
type Group struct {
	Source    storage.Storage
	Target    storage.Storage
	StartTime time.Time
	steps     []Step
	errChan   chan error
	errWg     *sync.WaitGroup
}

// NewGroup return a new prepared Group.
// You should always create new Group{} with this constructor.
func NewGroup() Group {
	group := Group{
		errChan: make(chan error),
		errWg:   &sync.WaitGroup{},
		steps:   make([]Step, 0),
	}
	return group
}

// SetSource configure source storage for group.
func (group *Group) SetSource(st storage.Storage) {
	group.Source = st
}

// SetTarget configure target storage for group.
func (group *Group) SetTarget(st storage.Storage) {
	group.Target = st
}

// AddPipeStep add pipeline step to group.
// Steps will executed sequentially, in order of addition.
func (group *Group) AddPipeStep(step Step) {
	step.errChan = make(chan error)
	step.workerWg = &sync.WaitGroup{}
	step.intOutChan = make(chan *storage.Object, step.ChanSize)
	step.intInChan = make(chan *storage.Object)
	step.outChan = make(chan *storage.Object)
	group.steps = append(group.steps, step)
}

// GetStepsInfo return info about all pipeline steps.
func (group *Group) GetStepsInfo() []StepInfo {
	res := make([]StepInfo, len(group.steps))
	for i := range group.steps {
		res[i] = StepInfo{Stats: &group.steps[i].stats,
			Name:   group.steps[i].Name,
			Num:    i,
			Config: group.steps[i].Config,
		}
	}
	return res
}

// GetStepInfo return info about step with given sequential number.
func (group *Group) GetStepInfo(stepNum int) StepInfo {
	return StepInfo{Stats: &group.steps[stepNum].stats,
		Name:   group.steps[stepNum].Name,
		Num:    stepNum,
		Config: group.steps[stepNum].Config,
	}
}

// Run start the pipeline execution.
//
// For result and error handling see ErrChan() function.
func (group *Group) Run() {
	for i := 0; i < len(group.steps); i++ {

		group.errWg.Add(1)

		go copyErr(group, i)
		go copyOutput(group, i)
		go copyInput(group, i)
		go startWorkers(group, i)
	}
	group.StartTime = time.Now()
}

// ErrChan return a Group error chan.
// All pipeline errors will be sent errors to this channel.
//
// "nil" message mean that all pipeline functions completed and pipeline was terminated.
// To prevent leakage of resources in the event of a context cancellation, you should read all messages from this channel.
// ErrChan will be closed after receiving a "nil" message.
func (group *Group) ErrChan() <-chan error {
	return group.errChan
}

func copyErr(group *Group, stepNum int) {
	for e := range group.steps[stepNum].errChan {
		if !IsContextCancelErr(e) {
			Log.Debugf("Recv pipeline err: %s", e)
			group.steps[stepNum].stats.Error.Add(1)
			group.errChan <- &PipelineError{StepName: group.steps[stepNum].Name, StepNum: stepNum, Err: e}
		}
	}
	group.errWg.Done()
}

func copyOutput(group *Group, stepNum int) {
	for obj := range group.steps[stepNum].intOutChan {
		group.steps[stepNum].stats.Output.Add(1)
		group.steps[stepNum].outChan <- obj
	}
	close(group.steps[stepNum].outChan)
}

func copyInput(group *Group, stepNum int) {
	if stepNum > 0 {
		for obj := range group.steps[stepNum-1].outChan {
			group.steps[stepNum].stats.Input.Add(1)
			group.steps[stepNum].intInChan <- obj
		}
	}
	close(group.steps[stepNum].intInChan)
}

func startWorkers(group *Group, stepNum int) {
	for w := uint(0); w <= group.steps[stepNum].AddWorkers; w++ {
		group.steps[stepNum].workerWg.Add(1)
		go func(i int) {
			if i == 0 {
				group.steps[i].Fn(group, i, nil, group.steps[i].intOutChan, group.steps[i].errChan)
			} else {
				group.steps[i].Fn(group, i, group.steps[i].intInChan, group.steps[i].intOutChan, group.steps[i].errChan)
			}
			group.steps[i].workerWg.Done()
		}(stepNum)
	}

	group.steps[stepNum].workerWg.Wait()
	close(group.steps[stepNum].intOutChan)
	close(group.steps[stepNum].errChan)
	Log.Debugf("Pipeline step: %s finished", group.steps[stepNum].Name)
	if stepNum+1 == len(group.steps) {
		Log.Debugf("All pipeline steps finished")
		group.errWg.Wait()
		group.errChan <- nil
		close(group.errChan)
		Log.Debugf("Pipeline terminated")
	}
}
