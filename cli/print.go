package main

import (
	"context"
	"fmt"
	"github.com/larrabee/s3sync/pipeline"
	"time"
)

func LiveStats(syncGroup *pipeline.Group, ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			dur := time.Since(syncGroup.StartTime).Seconds()
			for _, val := range syncGroup.GetStepsInfo() {
				_, _ = fmt.Fprintf(live, "%d %s: Input: %d; Output: %d (%.f obj/sec); Errors: %d\n", val.Num, val.Name, val.Stats.Input, val.Stats.Output, float64(val.Stats.Output)/dur, val.Stats.Error)
			}
			_, _ = fmt.Fprintf(live, "Duration: %s\n", time.Since(syncGroup.StartTime).String())
			time.Sleep(time.Second)
		}
	}
}

func PrintStats(syncGroup *pipeline.Group, status SyncStatus) {
	dur := time.Since(syncGroup.StartTime).Seconds()
	for _, val := range syncGroup.GetStepsInfo() {
		log.Infof("%d %s: Input: %d; Output: %d (%.f obj/sec); Errors: %d\n", val.Num, val.Name, val.Stats.Input, val.Stats.Output, float64(val.Stats.Output)/dur, val.Stats.Error)
	}
	log.Infof("Duration: %s", time.Since(syncGroup.StartTime).String())

	switch status {
	case SyncStatusOk:
		log.Infof("Sync Done")
	case SyncStatusFailed:
		log.Error("Sync Failed")
	case SyncStatusAborted:
		log.Warnf("Sync Aborted")
	case SyncStatusConfError:
		log.Errorf("Sync Configuration error")
	default:
		log.Warnf("Sync Unknown status")
	}
}
