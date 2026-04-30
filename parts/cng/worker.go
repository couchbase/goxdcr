package cng

import (
	"context"
	"math/rand"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
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

// sleepBeforeRetry implements additive backoff with jitter for retryable, non-network errors.
// It updates baseWait (per request) and sleeps up to MaxProcessRetryInterval.
// Returns true if the nozzle was stopped while waiting.
func (n *Nozzle) sleepBeforeRetry(rng *rand.Rand, baseWait *time.Duration) (stopped bool) {
	if *baseWait < MaxProcessRetryInterval {
		jitterSec := 1 + rng.Intn(int(JitterDuration/time.Second))
		jitter := time.Duration(jitterSec) * time.Second
		*baseWait += 5*time.Second + jitter
	}

	if *baseWait > MaxProcessRetryInterval {
		*baseWait = MaxProcessRetryInterval
	}

	t := time.NewTimer(*baseWait)
	select {
	case <-n.stopCh:
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		return true
	case <-t.C:
		return false
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

	attemptNum := 0
	// baseWait accumulates per-request retry sleep time for temporary, non-network errors.
	// It starts at 0 and increments by (5s + jitter) on each retryable failure, capped by MaxProcessRetryInterval.
	baseWait := time.Duration(0)
	// rng is local to this request processing so concurrent workers don't contend and to help de-sync retries.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// attemptNumAtMaxWait tracks the attempt number when baseWait hits MaxProcessRetryInterval, used for logging throttling.
	attemptNumAtMaxWait := 0

	for {
		select {
		case <-n.stopCh:
			return
		default:
			trace := Trace{}
			childCtx := startTrace(ctx, &trace)
			attemptNum++
			err = n.processReq(childCtx, req)

			if err != nil {
				// Log errors only when:
				// 1. Log level is Debug or lower (i.e. more verbose), or
				// 2. We just hit the max wait time for retries (to log at least error once for long retry scenarios)
				if n.Logger().GetLogLevel() >= log.LogLevelDebug || attemptNum-attemptNumAtMaxWait == 1 {
					n.Logger().Errorf("error processing req, attempt=%[1]d, key=%[2]s%[3]s%[4]s, opcode=%[5]s, cas=%[6]d err=%[7]s",
						attemptNum,
						base.UdTagBegin, req.OriginalKey, base.UdTagEnd,
						req.Req.Opcode,
						req.Req.Cas,
						err.Error())
				}
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
				n.stats.IncNonRetryableErrorCount(1)

				// Log errors only when:
				// 1. Log level is Debug or lower (i.e. more verbose), or
				// 2. We just hit the max wait time for retries (to log at least error once for long retry scenarios)
				if n.Logger().GetLogLevel() >= log.LogLevelDebug || attemptNum-attemptNumAtMaxWait == 1 {
					n.Logger().Errorf("req failed due non-retryable error attemptNum=%[7]d, key=%[1]s%[2]s%[3]s, opcode=%[5]s, cas=%[6]d err=%[4]v",
						base.UdTagBegin, req.OriginalKey, base.UdTagEnd,
						err,
						req.Req.Opcode, req.Req.Cas, attemptNum)
				}
				return
			}

			if n.sleepBeforeRetry(rng, &baseWait) {
				return
			}

			// If we've hit the max wait time, we want to log the error once
			if baseWait == MaxProcessRetryInterval {
				attemptNumAtMaxWait = attemptNum
			}
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
	n.Logger().Debugf("setting error to upstream, err=%v", err)
	reportFn(req)
	return
}
