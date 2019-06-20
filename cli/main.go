package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gosuri/uilive"
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/storage"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh/terminal"
	"os"
	"runtime"
	"time"
)

var cli argsParsed
var log = logrus.New()
var live uilive.Writer

const (
	permDir         os.FileMode = 0750
	permFile        os.FileMode = 0640
	s3keysPerReq                = 10000
	fsListBufSize               = 32 * 1024 * 1024
	goThreadsPerCPU             = 8
)

func init() {
	var err error
	cli, err = GetCliArgs()
	if err != nil {
		log.Fatalf("cli args parsing failed with error: %s", err)
	}
	if cli.ShowProgress && terminal.is
	log.SetLevel(logrus.DebugLevel)
	pipeline.Log = log
}

func main() {
	if cli.DisableHTTP2 {
		_ = os.Setenv("GODEBUG", os.Getenv("GODEBUG")+"http2client=0")
	}

	runtime.GOMAXPROCS(runtime.NumCPU() * goThreadsPerCPU)
	ctx, cancel := context.WithCancel(context.Background())

	syncGroup := pipeline.NewGroup()
	syncGroup.WithContext(ctx)

	var sourceStorage, targetStorage storage.Storage
	switch cli.Source.Type {
	case storage.TypeS3:
		sourceStorage = storage.NewS3Storage(cli.SourceKey, cli.SourceSecret, cli.SourceRegion, cli.SourceEndpoint,
			cli.Source.Bucket, cli.Source.Path, s3keysPerReq, cli.S3Retry, cli.S3RetryInterval,
		)
	case storage.TypeFS:
		sourceStorage = storage.NewFSStorage(cli.Source.Path, permFile, permDir, fsListBufSize)
	}

	switch cli.Target.Type {
	case storage.TypeS3:
		targetStorage = storage.NewS3Storage(cli.TargetKey, cli.TargetSecret, cli.TargetRegion, cli.TargetEndpoint,
			cli.Target.Bucket, cli.Target.Path, s3keysPerReq, cli.S3Retry, cli.S3RetryInterval,
		)

	case storage.TypeFS:
		targetStorage = storage.NewFSStorage(cli.Target.Path, permFile, permDir, 0)
	}

	sourceStorage.WithContext(ctx)
	targetStorage.WithContext(ctx)
	syncGroup.SetSource(sourceStorage)
	syncGroup.SetTarget(targetStorage)

	log.Info("Starting sync\n")

	syncGroup.AddPipeStep(pipeline.Step{
		Name:     "ListSource",
		Fn:       ListSourceStorage,
		ChanSize: cli.Workers,
	})

	syncGroup.AddPipeStep(pipeline.Step{
		Name:     "TestTransform",
		Fn:       TestTransformer,
		ChanSize: cli.Workers,
	})

	if len(cli.FilterExt) > 0 {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:     "FilterObjByExt",
			Fn:       FilterObjectsByExt,
			ChanSize: cli.Workers,
		})
	}

	if len(cli.FilterExtNot) > 0 {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:     "FilterObjByExtNot",
			Fn:       FilterObjectsByExtNot,
			ChanSize: cli.Workers,
		})
	}

	loadObjMetaStep := pipeline.Step{
		Name:       "LoadObjMeta",
		Fn:         ListSourceStorage,
		ChanSize:   cli.Workers,
		AddWorkers: cli.Workers,
	}
	if (cli.Source.Type == storage.TypeFS) && ((cli.FilterMtimeAfter > 0) || (cli.FilterMtimeBefore > 0)) {
		syncGroup.AddPipeStep(loadObjMetaStep)
	} else if (len(cli.FilterCT) > 0) || (len(cli.FilterCTNot) > 0) {
		syncGroup.AddPipeStep(loadObjMetaStep)
	}

	if len(cli.FilterCT) > 0 {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:     "FilterObjByCT",
			Fn:       FilterObjectsByCT,
			ChanSize: cli.Workers,
		})
	}

	if len(cli.FilterCTNot) > 0 {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:     "FilterObjByCTNot",
			Fn:       FilterObjectsByCTNot,
			ChanSize: cli.Workers,
		})
	}

	syncGroup.AddPipeStep(pipeline.Step{
		Name:     "ACLUpdater",
		Fn:       ACLUpdater,
		ChanSize: cli.Workers,
	})

	if cli.SyncLog {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:     "Logger",
			Fn:       Logger,
			ChanSize: cli.Workers,
		})
	}

	syncGroup.AddPipeStep(pipeline.Step{
		Name:       "LoadObjData",
		Fn:         LoadObjectData,
		ChanSize:   cli.Workers,
		AddWorkers: cli.Workers,
	})

	syncGroup.AddPipeStep(pipeline.Step{
		Name:       "UploadObj",
		Fn:         UploadObjectData,
		ChanSize:   cli.Workers,
		AddWorkers: cli.Workers,
	})

	syncGroup.AddPipeStep(pipeline.Step{
		Name:       "Terminator",
		Fn:         Terminator,
		ChanSize:   cli.Workers,
	})

	//syncStartTime := time.Now()
	syncGroup.Run()

	progressChanQ := make(chan bool)
	if cli.ShowProgress {

	}

	syncStatus := 0
WaitLoop:
	for err := range syncGroup.ErrChan() {
		if err == nil {
			log.Infof("Sync Done")
			break WaitLoop
		}
		if cli.OnFail == onFailSkip {
			log.Errorf("Sync err: %s, skipping", err)
			continue WaitLoop
		}

		aerr, ok := err.(*pipeline.PipelineError).Err.(awserr.Error)
		if (cli.OnFail == onFailSkipMissing) && ok && ((aerr.Code() == s3.ErrCodeNoSuchKey) || (aerr.Code() == "NotFound")) {
			log.Infof("Skip missing object, err: %s", aerr.Error())
			continue WaitLoop
		}

		log.Fatalf("Sync error: %s, terminating", err)
		syncStatus = 1
		cancel()
	}

	if cli.ShowProgress {
		progressChanQ <- true
	}

	os.Exit(syncStatus)
}

func startProgressBar(quit <-chan bool) {
	writer := uilive.New()
	writer.Start()
	for {
		select {
		case <-quit:
			writer.Stop()
			return
		default:
			dur := time.Since(counter.startTime).Seconds()
			fmt.Fprintf(writer, "Synced: %d; Skipped: %d; Failed: %d; Total processed: %d\nAvg syncing speed: %.f obj/sec; Avg listing speed: %.f obj/sec\n",
				counter.sucObjCnt, counter.skipObjCnt, counter.failObjCnt, counter.totalObjCnt, float64(counter.sucObjCnt)/dur, float64(counter.totalObjCnt)/dur)
			time.Sleep(time.Second)
		}
	}
}
