package cng

import (
	"context"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/parts"
)

func (n *Nozzle) worker(ctx context.Context) {
	for {
		select {
		case <-n.stopCh:
			return
		case req := <-n.dataCh:
			trace, err := n.processReq(ctx, req)
			trace.commitTime = time.Since(req.Start_time)

			if err == nil {
				//n.handleSuccess(req, trace)
			} else {
				n.handleProcessError(req, trace, err)
			}

			n.RecycleDataObj(req)
		}
	}
}

func (n *Nozzle) handleProcessError(req *base.WrappedMCRequest, t mutationTrace, err error) {
	n.Logger().Errorf("Failed to process req: key=%s, details=%v, err=%v",
		string(req.OriginalKey), t.String(), err)
}

func (n *Nozzle) handleSuccess(req *base.WrappedMCRequest, t mutationTrace) {
	// CNG TODO: check if failedTargetCR is correct or not
	failedTargetCR := (t.checked && t.conflictCheckRsp.reason == ConflictReasonDocNewer) || (t.pushed && t.pushRsp.isConflict)

	additionalInfo := parts.DataSentEventAdditional{
		Opcode:         req.Req.Opcode,
		VbucketCommon:  parts.VbucketCommon{VBucket: req.GetTargetVB()},
		NozzleType:     base.CNG,
		Seqno:          req.Seqno,
		ManifestId:     req.GetManifestId(),
		IsOptRepd:      t.optimistic,
		IsExpiration:   t.isExpiry,
		FailedTargetCR: failedTargetCR,
		Req_size:       req.Req.Size(),
		Commit_time:    t.commitTime,

		UncompressedReqSize:  req.Req.Size() - req.GetBodySize() + req.GetUncompressedBodySize(),
		SkippedRecompression: req.SkippedRecompression,
		ImportMutation:       req.ImportMutation,
		Cloned:               req.Cloned,
		CloneSyncCh:          req.ClonedSyncCh,
	}

	if t.pushed {
		additionalInfo.Commit_time = t.pushRsp.latency
	}

	evt := common.NewEvent(common.DataSent, nil, n, []any{req.GetSourceVB(), req.Seqno}, additionalInfo)
	n.RaiseEvent(evt)

	n.Logger().Infof("successfully processed req: key=%s, details=%v", string(req.OriginalKey), t.String())
}
