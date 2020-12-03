package main

import (
	"context"
	"fmt"
	"github.com/larrabee/s3sync/pipeline"
	"github.com/sirupsen/logrus"
	"time"
)

func printLiveStats(ctx context.Context, syncGroup *pipeline.Group) {
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

func printFinalStats(syncGroup *pipeline.Group, status syncStatus) {
	dur := time.Since(syncGroup.StartTime).Seconds()
	for _, val := range syncGroup.GetStepsInfo() {
		log.WithFields(logrus.Fields{
			"stepNum":    val.Num,
			"stepName": val.Name,
			"InputObj": val.Stats.Input,
			"OutputObj": val.Stats.Output,
			"ErrorObj": val.Stats.Error,
			"InputObjSpeed": float64(val.Stats.Input)/dur,
			"OutputObjSpeed": float64(val.Stats.Output)/dur,
		}).Info("Pipeline step finished")
	}
	log.WithFields(logrus.Fields{
		"durationSec":    time.Since(syncGroup.StartTime).Seconds(),
	}).Infof("Duration: %s", time.Since(syncGroup.StartTime).String())

	switch status {
	case syncStatusOk:
		log.WithFields(logrus.Fields{
			"status":    status,
		}).Infof("Sync Done")
	case syncStatusFailed:
		log.WithFields(logrus.Fields{
			"status":    status,
		}).Error("Sync Failed")
	case syncStatusAborted:
		log.WithFields(logrus.Fields{
			"status":    status,
		}).Warnf("Sync Aborted")
	case syncStatusConfError:
		log.WithFields(logrus.Fields{
			"status":    status,
		}).Errorf("Sync Configuration error")
	default:
		log.WithFields(logrus.Fields{
			"status":    status,
		}).Warnf("Sync Unknown status")
	}
}
