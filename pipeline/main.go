package pipeline

import (
	"context"
	"github.com/larrabee/s3sync/storage"
	"github.com/sirupsen/logrus"
	"sync"
)

var Log = logrus.New()

type Group struct {
	Source  storage.Storage
	Target  storage.Storage
	Ctx     context.Context
	steps   []Step
	errChan chan error
	errWg   *sync.WaitGroup
}

func NewGroup() Group {
	group := Group{
		errChan: make(chan error),
		errWg:   &sync.WaitGroup{},
		Ctx:     context.Background(),
		steps:   make([]Step, 0),
	}
	return group
}

func (group *Group) WithContext(ctx context.Context) {
	group.Ctx = ctx
}

func (group *Group) SetSource(st storage.Storage) {
	group.Source = st
}

func (group *Group) SetTarget(st storage.Storage) {
	group.Target = st
}

func (group *Group) AddPipeStep(step Step) {
	step.errChan = make(chan error)
	step.workerWg = &sync.WaitGroup{}
	step.intOutChan = make(chan *storage.Object, step.ChanSize)
	step.intInChan = make(chan *storage.Object)
	step.outChan = make(chan *storage.Object)
	group.steps = append(group.steps, step)
}

func (group *Group) GetStepsInfo() []StepInfo {
	res := make([]StepInfo, len(group.steps))
	for i := range group.steps {
		res[i] = StepInfo{Stats: group.steps[i].stats,
			Name:   group.steps[i].Name,
			Num:    i,
			Config: group.steps[i].Config,
		}
	}
	return res
}

func (group *Group) GetStepInfo(stepNum int) StepInfo {
	return StepInfo{Stats: group.steps[stepNum].stats,
		Name:   group.steps[stepNum].Name,
		Num:    stepNum,
		Config: group.steps[stepNum].Config,
	}
}

func (group *Group) Run() {
	for i := 0; i < len(group.steps); i++ {

		group.errWg.Add(1)
		go func(i int) {
			for e := range group.steps[i].errChan {
				Log.Debugf("Recv pipeline err: %s", e)
				group.steps[i].stats.Error += 1
				group.errChan <- &PipelineError{StepName: group.steps[i].Name, StepNum: i, Err: e}
			}
			group.errWg.Done()
		}(i)

		go func(i int) {
			for obj := range group.steps[i].intOutChan {
				group.steps[i].stats.Output += 1
				group.steps[i].outChan <- obj
			}
			close(group.steps[i].outChan)
		}(i)

		if i > 0 {
			go func(i int) {
				for obj := range group.steps[i-1].outChan {
					group.steps[i].stats.Input += 1
					group.steps[i].intInChan <- obj
				}
				close(group.steps[i].intInChan)
			}(i)
		}

		go func(i int) {
			for w := uint(0); w <= group.steps[i].AddWorkers; w++ {
				group.steps[i].workerWg.Add(1)
				go func(i int) {
					defer group.steps[i].workerWg.Done()
					if i == 0 {
						group.steps[i].Fn(group, i, nil, group.steps[i].intOutChan, group.steps[i].errChan)
					} else {
						group.steps[i].Fn(group, i, group.steps[i].intInChan, group.steps[i].intOutChan, group.steps[i].errChan)
					}
				}(i)
			}

			group.steps[i].workerWg.Wait()
			Log.Debugf("Pipeline step: %s finished", group.steps[i].Name)
			close(group.steps[i].intOutChan)
			close(group.steps[i].errChan)
			if i+1 == len(group.steps) {
				Log.Debugf("All pipeline steps finished")
				group.errWg.Wait()
				group.errChan <- nil
				close(group.errChan)
			}
		}(i)

	}
}

func (group *Group) ErrChan() chan error {
	return group.errChan
}
