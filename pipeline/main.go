package pipeline

import (
	"context"
	"github.com/larrabee/s3sync/storage"
	"github.com/sirupsen/logrus"
	"sync"
)

var Log = logrus.New()

func init() {
	storage.Log = Log
}

type PipelineFn func(group *Group, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error)

type Step struct {
	Name       string
	Fn         PipelineFn
	ChanSize   uint
	AddWorkers uint
	dataChan   chan *storage.Object
	errChan    chan error
	workerWg   *sync.WaitGroup
}

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
	step.dataChan = make(chan *storage.Object, step.ChanSize)
	group.steps = append(group.steps, step)
}

//func (group *Group) ResetPipeline(fn PipelineFn) {
//	group.steps = make([]Step, 0)
//}

func (group *Group) Run() {
	for i := 0; i < len(group.steps); i++ {

		group.errWg.Add(1)
		go func(i int) {
			for e := range group.steps[i].errChan {
				Log.Debugf("Recv pipeline err: %s", e)
				group.errChan <- &PipelineError{StepName: group.steps[i].Name, Err: e}
			}
			group.errWg.Done()
		}(i)

		go func(i int) {
			for w := uint(0); w <= group.steps[i].AddWorkers; w++ {
				group.steps[i].workerWg.Add(1)
				go func(i int) {
					defer group.steps[i].workerWg.Done()
					if i == 0 {
						group.steps[i].Fn(group, nil, group.steps[i].dataChan, group.steps[i].errChan)
					} else {
						group.steps[i].Fn(group, group.steps[i-1].dataChan, group.steps[i].dataChan, group.steps[i].errChan)
					}
				}(i)

			}

			group.steps[i].workerWg.Wait()
			Log.Debugf("Pipeline step: %s finished", group.steps[i].Name)
			Log.Debugf("Close datachan for step %s, %d", group.steps[i].Name, i)
			close(group.steps[i].dataChan)
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
