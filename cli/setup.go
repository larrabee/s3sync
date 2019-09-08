package main

import (
	"context"
	"fmt"
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/pipeline/collection"
	"github.com/larrabee/s3sync/storage"
)

func setupStorages(ctx context.Context, syncGroup *pipeline.Group, cli *argsParsed) error {
	var sourceStorage, targetStorage storage.Storage
	switch cli.Source.Type {
	case storage.TypeS3:
		sourceStorage = storage.NewS3Storage(cli.SourceKey, cli.SourceSecret, cli.SourceRegion, cli.SourceEndpoint,
			cli.Source.Bucket, cli.Source.Path, cli.S3KeysPerReq, cli.S3Retry, cli.S3RetryInterval,
		)
	case storage.TypeFS:
		sourceStorage = storage.NewFSStorage(cli.Source.Path, cli.FSFilePerm, cli.FSDirPerm, fsListBufSize, !cli.FSDisableXattr)
	}

	switch cli.Target.Type {
	case storage.TypeS3:
		targetStorage = storage.NewS3Storage(cli.TargetKey, cli.TargetSecret, cli.TargetRegion, cli.TargetEndpoint,
			cli.Target.Bucket, cli.Target.Path, cli.S3KeysPerReq, cli.S3Retry, cli.S3RetryInterval,
		)
	case storage.TypeFS:
		targetStorage = storage.NewFSStorage(cli.Target.Path, cli.FSFilePerm, cli.FSDirPerm, 0, !cli.FSDisableXattr)
	}

	if sourceStorage == nil {
		return fmt.Errorf("source storage is nil")
	} else if targetStorage == nil {
		return fmt.Errorf("target storage is nil")
	}

	sourceStorage.WithContext(ctx)
	targetStorage.WithContext(ctx)
	if cli.RateLimitBandwidth > 0 {
		err := sourceStorage.WithRateLimit(cli.RateLimitBandwidth)
		if err != nil {
			log.Fatalf("Bandwidth limit error: %s", err)
		}
	}

	syncGroup.SetSource(sourceStorage)
	syncGroup.SetTarget(targetStorage)
	return nil
}

func setupPipeline(syncGroup *pipeline.Group, cli *argsParsed) {
	syncGroup.AddPipeStep(pipeline.Step{
		Name:     "ListSource",
		Fn:       collection.ListSourceStorage,
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
	if (cli.Source.Type == storage.TypeFS) && ((cli.FilterMtimeAfter > 0) || (cli.FilterMtimeBefore > 0) || cli.FilterModified) {
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

	if cli.FilterModified {
		syncGroup.AddPipeStep(pipeline.Step{
			Name: "FilterObjectsModified",
			Fn:   collection.FilterObjectsModified,
		})
	}

	syncGroup.AddPipeStep(pipeline.Step{
		Name:       "LoadObjData",
		Fn:         collection.LoadObjectData,
		AddWorkers: cli.Workers,
	})

	if cli.S3Acl != "" {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:   "ACLUpdater",
			Fn:     collection.ACLUpdater,
			Config: cli.S3Acl,
		})
	}

	if cli.S3StorageClass != "" {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:   "StorageClassUpdater",
			Fn:     collection.StorageClassUpdater,
			Config: cli.S3StorageClass,
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

	if cli.RateLimitObjPerSec > 0 {
		syncGroup.AddPipeStep(pipeline.Step{
			Name:   "RateLimit",
			Fn:     collection.PipelineRateLimit,
			Config: cli.RateLimitObjPerSec,
		})
	}

	syncGroup.AddPipeStep(pipeline.Step{
		Name: "Terminator",
		Fn:   collection.Terminator,
	})
}
