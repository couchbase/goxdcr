package cng

import (
	"context"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
)

func (n *Nozzle) worker(ctx context.Context) {
	for {
		select {
		case <-n.stopCh:
			return
		case req := <-n.dataCh:
			n.processReqWithRetry(ctx, req)
			n.RecycleDataObj(req)
		}
	}
}

func (n *Nozzle) processReqWithRetry(ctx context.Context, req *base.WrappedMCRequest) {
	var err error
	// CNG TODO: Disabled panic recovery for now to surface issues during testing
	// Raised MB-69560 for tracking
	//defer func() {
	//	if r := recover(); r != nil {
	//		n.Logger().Errorf("panic recovered in processReqWithRetry r=%v, stack=%v", r, debug.Stack())
	//		err = ErrProcessPanic
	//	}
	//}()

	for {
		select {
		case <-n.stopCh:
			return
		default:
			trace := Trace{}
			childCtx := startTrace(ctx, &trace)
			err = n.processReq(childCtx, req)
			trace.commitTime = time.Since(req.Start_time)

			n.handleNozzleStats(&trace, err)
			n.raiseEvents(req, &trace, err)
			if err == nil {
				return
			}

			if !isErrorRetryable(err) {
				return
			}

			time.Sleep(ProcessRetryInterval)
		}
	}
}
