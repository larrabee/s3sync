package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gosuri/uilive"
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/pipeline/collection"
	"github.com/larrabee/s3sync/storage"
	"github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

var cli argsParsed
var log = logrus.New()
var live *uilive.Writer

const (
	fsListBufSize   = 32 * 1024 * 1024
	goThreadsPerCPU = 8
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU() * goThreadsPerCPU)
	var err error
	cli, err = GetCliArgs()
	if err != nil {
		log.Fatalf("cli args parsing failed with error: %s", err)
	}
	if cli.ShowProgress {
		live = uilive.New()
		live.Start()
		log.SetOutput(live.Bypass())
		log.SetFormatter(&logrus.TextFormatter{ForceColors: true})
	}
	if cli.Debug {
		log.SetLevel(logrus.DebugLevel)
	}
	pipeline.Log = log
	storage.Log = log
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	syncGroup := pipeline.NewGroup()
	syncGroup.WithContext(ctx)

	sysStopChan := make(chan os.Signal, 1)
	signal.Notify(sysStopChan, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	var sourceStorage, targetStorage storage.Storage
	switch cli.Source.Type {
	case storage.TypeS3:
		sourceStorage = storage.NewS3Storage(cli.SourceKey, cli.SourceSecret, cli.SourceRegion, cli.SourceEndpoint,
			cli.Source.Bucket, cli.Source.Path, cli.S3KeysPerReq, cli.S3Retry, cli.S3RetryInterval,
		)
	case storage.TypeFS:
		sourceStorage = storage.NewFSStorage(cli.Source.Path, cli.FSFilePerm, cli.FSDirPerm, fsListBufSize)
	}

	switch cli.Target.Type {
	case storage.TypeS3:
		targetStorage = storage.NewS3Storage(cli.TargetKey, cli.TargetSecret, cli.TargetRegion, cli.TargetEndpoint,
			cli.Target.Bucket, cli.Target.Path, cli.S3KeysPerReq, cli.S3Retry, cli.S3RetryInterval,
		)
	case storage.TypeFS:
		targetStorage = storage.NewFSStorage(cli.Target.Path, cli.FSFilePerm, cli.FSDirPerm, 0)
	}

	sourceStorage.WithContext(ctx)
	targetStorage.WithContext(ctx)
	syncGroup.SetSource(sourceStorage)
	syncGroup.SetTarget(targetStorage)

	syncGroup.AddPipeStep(pipeline.Step{
		Name:       "ListSource",
		Fn:         collection.ListSourceStorage,
		ChanSize: cli.ListBuffer,
	})

	if len(cli.FilterExt) > 0 {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:   "FilterObjByExt",
			Fn:     collection.FilterObjectsByExt,
			Config: cli.FilterExt,
		})
	}

	if len(cli.FilterExtNot) > 0 {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:   "FilterObjByExtNot",
			Fn:     collection.FilterObjectsByExtNot,
			Config: cli.FilterExtNot,
		})
	}

	loadObjMetaStep := pipeline.Step{
		Name:       "LoadObjMeta",
		Fn:         collection.LoadObjectMeta,
		AddWorkers: cli.Workers,
	}
	if (cli.Source.Type == storage.TypeFS) && ((cli.FilterMtimeAfter > 0) || (cli.FilterMtimeBefore > 0)) {
		syncGroup.AddPipeStep(loadObjMetaStep)
	} else if (len(cli.FilterCT) > 0) || (len(cli.FilterCTNot) > 0) {
		syncGroup.AddPipeStep(loadObjMetaStep)
	}

	if cli.FilterMtimeAfter > 0 {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:   "FilterObjectsByMtimeAfter",
			Fn:     collection.FilterObjectsByMtimeAfter,
			Config: cli.FilterMtimeAfter,
		})
	}

	if cli.FilterMtimeBefore > 0 {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:   "FilterObjectsByMtimeBefore",
			Fn:     collection.FilterObjectsByMtimeBefore,
			Config: cli.FilterMtimeBefore,
		})
	}

	if len(cli.FilterCT) > 0 {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:   "FilterObjByCT",
			Fn:     collection.FilterObjectsByCT,
			Config: cli.FilterCT,
		})
	}

	if len(cli.FilterCTNot) > 0 {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:   "FilterObjByCTNot",
			Fn:     collection.FilterObjectsByCTNot,
			Config: cli.FilterCTNot,
		})
	}

	syncGroup.AddPipeStep(pipeline.Step{
		Name:       "LoadObjData",
		Fn:         collection.LoadObjectData,
		AddWorkers: cli.Workers,
	})

	if cli.Target.Type == storage.TypeS3 {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:   "ACLUpdater",
			Fn:     collection.ACLUpdater,
			Config: cli.S3Acl,
		})
	}

	syncGroup.AddPipeStep(pipeline.Step{
		Name:       "UploadObj",
		Fn:         collection.UploadObjectData,
		AddWorkers: cli.Workers,
	})

	if cli.SyncLog {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:   "Logger",
			Fn:     collection.Logger,
			Config: log,
		})
	}

	syncGroup.AddPipeStep(pipeline.Step{
		Name: "Terminator",
		Fn:   collection.Terminator,
	})

	log.Info("Starting sync\n")
	syncStartTime := time.Now()
	syncGroup.Run()

	progressChanQ := make(chan bool)
	if cli.ShowProgress {
		go func(quit <-chan bool) {
			for {
				select {
				case <-quit:
					return
				default:
					dur := time.Since(syncStartTime).Seconds()
					for _, val := range syncGroup.GetStepsInfo() {
						_, _ = fmt.Fprintf(live, "%d %s: Input: %d; Output: %d (%.f obj/sec); Errors: %d\n", val.Num, val.Name, val.Stats.Input, val.Stats.Output, float64(val.Stats.Output)/dur, val.Stats.Error)
					}
					time.Sleep(time.Second)
				}
			}
		}(progressChanQ)
	}

	syncStatus := 0

WaitLoop:
	for {
		select {
		case recSignal := <-sysStopChan:
			log.Warnf("Receive signal: %s, terminating", recSignal.String())
			cancel()
			syncStatus = 2
			break WaitLoop
		case err := <-syncGroup.ErrChan():
			if err == nil {
				log.Infof("Sync Done")
				break WaitLoop
			}
			if cli.OnFail == onFailSkip && err.(*pipeline.PipelineError).Err != context.Canceled {
				log.Errorf("Sync err: %s, skipping", err)
				continue WaitLoop
			}
			aerr, ok := err.(*pipeline.PipelineError).Err.(awserr.Error)
			if (cli.OnFail == onFailSkipMissing) && ok && ((aerr.Code() == s3.ErrCodeNoSuchKey) || (aerr.Code() == "NotFound")) {
				log.Infof("Skip missing object, err: %s", aerr.Error())
				continue WaitLoop
			}

			log.Errorf("Sync error: %s, terminating", err)
			syncStatus = 1
			cancel()
			break WaitLoop
		}
	}

	if cli.ShowProgress {
		progressChanQ <- true
	}

	for _, val := range syncGroup.GetStepsInfo() {
		dur := time.Since(syncStartTime).Seconds()
		log.Infof("%d %s: Input: %d; Output: %d (%.f obj/sec); Errors: %d\n", val.Num, val.Name, val.Stats.Input, val.Stats.Output, float64(val.Stats.Output)/dur, val.Stats.Error)
	}

	log.Exit(syncStatus)
}
