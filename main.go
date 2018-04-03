package main

import (
	"fmt"
	"github.com/gosuri/uilive"
	"github.com/mattn/go-isatty"
	"github.com/sirupsen/logrus"
	"os"
	"time"
	"sync"
)

var sucObjCnt uint64
var failObjCnt uint64
var skipObjCnt uint64
var totalObjCnt uint64

var syncGr SyncGroup

var cli ArgsParsed
var log = logrus.New()

const (
	permDir       os.FileMode = 0750
	permFile      os.FileMode = 0640
	sleepDuration             = 5 * time.Second
	s3keysPerReq              = 10000
	onFailSleepDuration       = time.Second
)

func main() {
	var err error
	cli, err = GetCliArgs()
	if err != nil {
		log.Fatalf("cli args parsing failed with error: %s", err)
	}

	ConfigureLogging()

	objChan := make(chan object, cli.Workers*4)
	wg := sync.WaitGroup{}

	for i := cli.Workers; i != 0; i-- {
		wg.Add(1)
		go ProcessObj(objChan, &wg)
	}

	syncGr = SyncGroup{}
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

	log.Info("Starting sync")
	if isatty.IsTerminal(os.Stdout.Fd()) {
		writer := uilive.New()
		writer.Start()
		go func() {
			for {
				fmt.Fprintf(writer, "Downloaded: %d; Skiped: %d; Failed: %d; Total processed: %d\n", sucObjCnt, skipObjCnt, failObjCnt, totalObjCnt)
				time.Sleep(time.Second * 3)
			}
		}()
	}

	err = syncGr.Source.List(objChan)
	if err != nil {
		log.Fatalf("Listing objects failed: %s\n", err)
	}

	wg.Wait()
	log.Info("Sync finished successfully")
	log.Infof("Downloaded: %d; Skiped: %d; Failed: %d; Total processed: %d", sucObjCnt, skipObjCnt, failObjCnt, totalObjCnt)
}

func ConfigureLogging() {

	//stderrLogger := logging.NewLogBackend(os.Stderr, "", 0)
	//stderrFormat := logging.MustStringFormatter(`%{color}%{time:15:04:05.000} %{shortfunc} [%{level:.4s}] %{id:03x}%{color:reset} %{message}`)
	//stderrFormatter := logging.NewBackendFormatter(stderrLogger, stderrFormat)
	//stderrLeveled := logging.AddModuleLevel(stderrLogger)
	if cli.Debug {
		log.SetLevel(logrus.DebugLevel)
	} else {
		log.SetLevel(logrus.InfoLevel)
	}
	log.Formatter = &logrus.TextFormatter{}
	log.Out = os.Stdout
}
