package main

import (
	"fmt"
	"github.com/gosuri/uilive"
	"github.com/mattn/go-isatty"
	"github.com/sirupsen/logrus"
	"os"
	"runtime"
	"sync"
	"time"
)

var syncGr = SyncGroup{}

var counter = Counter{}
var cli ArgsParsed
var log = logrus.New()

const (
	permDir         os.FileMode = 0750
	permFile        os.FileMode = 0640
	s3keysPerReq                = 10000
	goThreadsPerCPU             = 8
)

func main() {
	var err error
	cli, err = GetCliArgs()
	if err != nil {
		log.Fatalf("cli args parsing failed with error: %s", err)
	}

	ConfigureLogging()
	runtime.GOMAXPROCS(runtime.NumCPU() * goThreadsPerCPU)
	objChan := make(chan Object, cli.Workers*4)
	wg := sync.WaitGroup{}
	prgBarQuit := make(chan bool)

	for i := cli.Workers; i != 0; i-- {
		wg.Add(1)
		go ProcessObj(objChan, &wg)
	}

	switch cli.Source.Type {
	case S3Conn:
		syncGr.Source = NewAWSStorage(cli.SourceKey, cli.SourceSecret, cli.SourceRegion, cli.SourceEndpoint, cli.Source.Bucket, cli.Source.Path)
	case FSConn:
		syncGr.Source = NewFSStorage(cli.Source.Path)
	}
	switch cli.Target.Type {
	case S3Conn:
		syncGr.Target = NewAWSStorage(cli.TargetKey, cli.TargetSecret, cli.TargetRegion, cli.TargetEndpoint, cli.Target.Bucket, cli.Target.Path)
	case FSConn:
		syncGr.Target = NewFSStorage(cli.Target.Path)
	}

	log.Info("Starting sync\n")
	counter.startTime = time.Now()

	if isatty.IsTerminal(os.Stdout.Fd()) {
		go StartProgressBar(prgBarQuit)
	}

	if err := syncGr.Source.List(objChan); err != nil {
		log.Fatalf("Listing objects failed: %s\n", err)
	}

	wg.Wait()
	if isatty.IsTerminal(os.Stdout.Fd()) {
		prgBarQuit <- true
	}
	dur := time.Since(counter.startTime).Seconds()
	log.Info("Sync finished successfully")
	log.Infof("Synced: %d; Skipped: %d; Failed: %d; Total processed: %d", counter.sucObjCnt, counter.skipObjCnt, counter.failObjCnt, counter.totalObjCnt)
	log.Infof("Avg syncing speed: %9.f obj/sec; Avg listing speed: %9.f obj/sec; Duration: %9.f sec\n", float64(counter.sucObjCnt)/dur, float64(counter.totalObjCnt)/dur, dur)
}

func ConfigureLogging() {
	if cli.Debug {
		log.SetLevel(logrus.DebugLevel)
	} else {
		log.SetLevel(logrus.InfoLevel)
	}
	log.Formatter = &logrus.TextFormatter{}
	log.Out = os.Stdout
}

func StartProgressBar(quit <-chan bool) {
	writer := uilive.New()
	writer.Start()
	for {
		select {
		case <- quit:
			return
		default:
			dur := time.Since(counter.startTime).Seconds()
			fmt.Fprintf(writer, "Synced: %d; Skipped: %d; Failed: %d; Total processed: %d\nAvg syncing speed: %.f obj/sec; Avg listing speed: %.f obj/sec\n",
				counter.sucObjCnt, counter.skipObjCnt, counter.failObjCnt, counter.totalObjCnt, float64(counter.sucObjCnt)/dur, float64(counter.totalObjCnt)/dur)
			time.Sleep(time.Second)
		}
	}
}