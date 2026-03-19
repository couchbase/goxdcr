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
			dontRecycle := n.processReqWithRetry(ctx, req)
			if !dontRecycle {
				n.RecycleDataObj(req)
			}
		}
	}
}

// processReqWithRetry processes the request with retry logic for retryable errors.
// These retryable errors are not network related but instead are errors due to temporary state of the system
// (e.g resource not available)
// The function returns a boolean indicating whether the request should not be recycled
func (n *Nozzle) processReqWithRetry(ctx context.Context, req *base.WrappedMCRequest) (dontRecycle bool) {
	var err error
	// CNG TODO: Disabled panic recovery for now to surface issues during testing
	// Raised MB-69560 for tracking
	//defer func() {
	//	if r := recover(); r != nil {
	//		n.Logger().Errorf("panic recovered in processReqWithRetry r=%v, stack=%v", r, debug.Stack())
	//		err = ErrProcessPanic
	//	}
	//}()

	attemptNo := 0

	for {
		select {
		case <-n.stopCh:
			return
		default:
			trace := Trace{}
			childCtx := startTrace(ctx, &trace)
			attemptNo++
			err = n.processReq(childCtx, req)

			if err != nil {
				// It is possible that the log below may spam. But at the moment (being new nozzle) the advantages
				// outweigh the disadvantages. So far this log has been very helpful in
				// troubleshooting and will be revisited in future.
				n.Logger().Errorf("error processing req, attempt=%[1]d, key=%[2]s%[3]s%[4]s, opcode=%[5]s, cas=%[6]d err=%[7]s",
					attemptNo,
					base.UdTagBegin, req.OriginalKey, base.UdTagEnd,
					req.Req.Opcode,
					req.Req.Cas,
					err.Error())
			}
			trace.commitTime = time.Since(req.Start_time)

			n.handleNozzleStats(&trace, err)
			// Don't mark for recycling of req as it gets already recycled
			// in upstream error callback.
			if n.raiseUpstreamErr(req, err) {
				dontRecycle = true
			}
			n.handleVBError(req, err)
			n.raiseEvents(req, &trace, err)
			if err == nil {
				return
			}

			if !isMutationRetryable(err) {
				n.Logger().Errorf("req failed due non-retryable error key=%[1]s%[2]s%[3]s, opcode=%[5]s, cas=%[6]d err=%[4]v",
					base.UdTagBegin, req.OriginalKey, base.UdTagEnd,
					err,
					req.Req.Opcode, req.Req.Cas)
				return
			}

			time.Sleep(ProcessRetryInterval)
		}
	}
}

// raiseUpstreamErr raises errors back to upstream if applicable
// returns true if upstream error was raised
func (n *Nozzle) raiseUpstreamErr(req *base.WrappedMCRequest, err error) (raised bool) {
	if err == nil {
		return
	}

	if !isErrorUpstreamReportable(err) {
		return
	}

	vbno := req.GetSourceVB()
	reportFn, exists := n.upstreamErrReporterMap[vbno]
	if !exists {
		n.stats.IncErrReporterMissingCount(1)
		return
	}

	raised = true
	n.Logger().Errorf("setting error to upstream, err=%v", err)
	reportFn(req)
	return
}
