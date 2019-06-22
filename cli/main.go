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
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

var cli argsParsed
var log = logrus.New()
var live *uilive.Writer
var filteredCnt uint64 = 0

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
	if cli.ShowProgress {
		live = uilive.New()
		live.Start()
		log.SetOutput(live.Bypass())
		log.SetFormatter(&logrus.TextFormatter{ForceColors:true})
	}
	if cli.Debug {
		log.SetLevel(logrus.DebugLevel)
	}
	pipeline.Log = log
	storage.Log = log
}

func main() {
	if cli.DisableHTTP2 {
		_ = os.Setenv("GODEBUG", os.Getenv("GODEBUG")+"http2client=0")
	}

	runtime.GOMAXPROCS(runtime.NumCPU() * goThreadsPerCPU)
	ctx, cancel := context.WithCancel(context.Background())

	syncGroup := pipeline.NewGroup()
	syncGroup.WithContext(ctx)

	sysStopChan := make(chan os.Signal, 1)
	signal.Notify(sysStopChan, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

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
		AddWorkers: 1,
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

	if cli.FilterMtimeAfter > 0 {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:     "FilterObjectsByMtimeAfter",
			Fn:       FilterObjectsByMtimeAfter,
			ChanSize: cli.Workers,
		})
	}

	if cli.FilterMtimeBefore > 0 {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:     "FilterObjectsByMtimeBefore",
			Fn:       FilterObjectsByMtimeBefore,
			ChanSize: cli.Workers,
		})
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

	syncStartTime := time.Now()
	syncGroup.Run()

	progressChanQ := make(chan bool)
	getStatsStr := func() string {
		dur := time.Since(syncStartTime).Seconds()
		sStats := syncGroup.Source.GetStats()
		tStats := syncGroup.Target.GetStats()
		str := fmt.Sprintf("SOURCE: Listed: %d (%.f obj/sec); MetaLoaded: %d; DataLoaded: %d;\n",
			sStats.ListedObjects, float64(sStats.ListedObjects)/dur, sStats.MetaLoadedObjects, sStats.DataLoadedObjects)
		str += fmt.Sprintf("FILTERED: Total: %d;\n", filteredCnt)
		str += fmt.Sprintf("TARGET: Uploaded: %d (%.f obj/sec);\n", tStats.UploadedObjects, float64(tStats.UploadedObjects)/dur)
		str += fmt.Sprintf("TIME: Duration: %.f;\n", dur)
		return str
	}

	if cli.ShowProgress {
		go func(quit <-chan bool) {
			for {
				select {
				case <-quit:
					return
				default:
					msg := getStatsStr()
					_, _ = fmt.Fprint(live, msg)
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
		case err := <- syncGroup.ErrChan():
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

			if err.(*pipeline.PipelineError).Err == context.Canceled {
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

	msg := getStatsStr()
	log.Infof("Stats: %s\n", msg)

	log.Exit(syncStatus)
}