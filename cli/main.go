// Package provides the cli util s3sync.
package main

import (
	"context"
	"errors"
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
)

var cli argsParsed
var log = logrus.New()
var live *uilive.Writer

const (
	fsListBufSize   = 32 * 1024 * 1024
	goThreadsPerCPU = 8
)

type SyncStatus int

const (
	SyncStatusUnknown SyncStatus = iota - 1
	SyncStatusOk
	SyncStatusFailed
	SyncStatusAborted
	SyncStatusConfError
)

// init program runtime: parse cli args and set logger
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
	sysStopChan := make(chan os.Signal, 1)
	signal.Notify(sysStopChan, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	err := SetupStorages(&syncGroup, &cli, ctx)
	if err != nil {
		log.Fatalf("Failed to setup storage, error: %s", err)
	}
	SetupPipeline(&syncGroup, &cli)

	log.Info("Starting sync")
	syncGroup.Run()

	if cli.ShowProgress {
		go LiveStats(&syncGroup, ctx)
	}

	syncStatus := SyncStatusUnknown

WaitLoop:
	for {
		select {
		case recSignal := <-sysStopChan:
			log.Warnf("Receive signal: %s, terminating", recSignal.String())
			cancel()
			syncStatus = SyncStatusAborted
		case err := <-syncGroup.ErrChan():
			if err == nil {
				if syncStatus == SyncStatusUnknown {
					syncStatus = SyncStatusOk
				}
				break WaitLoop
			}

			var confErr *pipeline.StepConfigurationError
			if errors.As(err, &confErr) {
				log.Errorf("Pipeline configuration error: %s, terminating", confErr)
				syncStatus = SyncStatusConfError
				cancel()
				continue WaitLoop
			}

			if (cli.OnFail == onFailSkipMissing) || cli.OnFail == onFailSkip {
				var aErr awserr.Error
				if errors.As(err, &aErr) {
					if (aErr.Code() == s3.ErrCodeNoSuchKey) || (aErr.Code() == "NotFound") {
						var objErr *pipeline.ObjectError
						if errors.As(err, &objErr) {
							log.Warnf("Skip missing object: %s", *objErr.Object.Key)
						} else {
							log.Warnf("Skip missing object, err: %s", aErr.Error())
						}
						continue WaitLoop
					}
				}
			}

			if cli.OnFail == onFailSkip {
				var objErr *pipeline.ObjectError
				if errors.As(err, &objErr) {
					log.Warnf("Failed to sync object: %s, error: %s, skipping", *objErr.Object.Key, objErr.Err)
				} else {
					log.Warnf("Sync err: %s, skipping", err)
				}
				continue WaitLoop
			}

			if syncStatus == SyncStatusUnknown {
				log.Errorf("Sync error: %s, terminating", err)
				syncStatus = SyncStatusFailed
				cancel()
			}
		}
	}

	PrintStats(&syncGroup, syncStatus)
	log.Exit(int(syncStatus))
}
