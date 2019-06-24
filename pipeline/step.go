package pipeline

import (
	"github.com/larrabee/s3sync/storage"
	"sync"
)

type StepFn func(group *Group, stepNum int, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error)

type Step struct {
	Name       string
	Fn         StepFn
	AddWorkers uint
	Config     interface{}
	outChan    chan *storage.Object
	intOutChan chan *storage.Object
	intInChan  chan *storage.Object
	errChan    chan error
	workerWg   *sync.WaitGroup
	stats      StepStats
}

type StepStats struct {
	Input  uint64
	Output uint64
	Error  uint64
}

type StepInfo struct {
	Stats  StepStats
	Name   string
	Num    int
	Config interface{}
}
