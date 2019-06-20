package main

import (
	"github.com/larrabee/s3sync/pipeline"
	"github.com/larrabee/s3sync/storage"
)

var LoadObjectMeta pipeline.PipelineFn = func(group *pipeline.Group, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			errChan <- group.Ctx.Err()
			return
		default:
			err := group.Source.GetObjectMeta(obj)
			if err != nil {
				errChan <- err
			} else {
				output <- obj
			}
		}
	}
}

var LoadObjectData pipeline.PipelineFn = func(group *pipeline.Group, input <-chan *storage.Object, output chan<- *storage.Object, errChan chan<- error) {
	for obj := range input {
		select {
		case <-group.Ctx.Done():
			errChan <- group.Ctx.Err()
			return
		default:
			err := group.Source.GetObjectContent(obj)
			if err != nil {
				errChan <- err
			} else {
				output <- obj
			}
		}
	}
}

//func failedObjAction(obj *Object, objErr error) {
//	atomic.AddUint64(&counter.failObjCnt, 1)
//	switch cli.OnFail {
//	case onFailSkip:
//		log.Errorf("Failed to wg object: %s, err: %s, skipping it\n", obj.Key, objErr)
//	case onFailFatal:
//		log.Fatalf("Failed to wg object: %s, err: %s, exiting\n", obj.Key, objErr)
//	case onFailSkipMissing:
//		if objErr != nil {
//			if aerr, ok := objErr.(awserr.errChan); ok {
//				switch aerr.Code() {
//				case s3.ErrCodeNoSuchKey:
//					log.Errorf("Object not found: %s, skipping it\n", obj.Key)
//				case "NotFound":
//					log.Errorf("Object not found: %s, skipping it\n", obj.Key)
//				default:
//					log.Fatalf("Failed to wg object: %s, err: %s, exiting\n", obj.Key, objErr)
//				}
//			} else {
//				log.Fatalf("Failed to wg object: %s, err: %s, exiting\n", obj.Key, objErr)
//			}
//		}
//	}
//}
