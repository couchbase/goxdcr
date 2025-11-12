package cng

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/parts"
)

func (n *Nozzle) raiseEvents(req *base.WrappedMCRequest, t *Trace, err error) {
	n.raiseSuccessEvent(req, t, err)
	n.raiseConflictEvent(req, t, err)
	n.raiseErrorEvent(req, t, err)
}

// raiseErrorEvent only handles errors and raises relevant events
func (n *Nozzle) raiseErrorEvent(req *base.WrappedMCRequest, t *Trace, err error) {
	if err == nil {
		return
	}

	n.Logger().Tracef("mutation failed, err=%v", err)

	errCode := mapErrorToCode(err)
	if errCode == ERR_UNKNOWN {
		n.RaiseEvent(common.NewEvent(common.DataSentFailedUnknownStatus, nil, n, []any{req.GetSourceVB(), req.GetTargetVB(), req.Seqno}, nil))
	} else {
		n.RaiseEvent(common.NewEvent(common.DataSentFailed, errCode, n, []any{req.GetSourceVB(), req.GetTargetVB(), req.Seqno, errCode}, nil))
	}
}

// raiseConflictEvent checks for conflicts only and raises relevant events
func (n *Nozzle) raiseConflictEvent(req *base.WrappedMCRequest, t *Trace, err error) {
	opcode := base.SET_WITH_META
	if t.isDelete {
		opcode = base.DELETE_WITH_META
	}

	if t.checked && t.conflictCheckRsp.reason != "" && !t.conflictCheckRsp.sourceWon {
		additionalInfo := parts.DataFailedCRSourceEventAdditional{
			Seqno:          req.Seqno,
			VbucketCommon:  parts.VbucketCommon{VBucket: req.Req.VBucket},
			Opcode:         opcode,
			IsExpirySet:    t.isExpiry,
			ManifestId:     req.GetManifestId(),
			Cloned:         req.Cloned,
			CloneSyncCh:    req.ClonedSyncCh,
			ImportMutation: req.ImportMutation,
		}
		if req.OrigSrcVB != nil {
			additionalInfo.SetOrigSrcVB(*req.OrigSrcVB)
		}
		n.RaiseEvent(common.NewEvent(common.DataFailedCRSource, nil, n, nil, additionalInfo))
	}
}

// raiseSuccessEvent handles when there is no error and raises relevant events
func (n *Nozzle) raiseSuccessEvent(req *base.WrappedMCRequest, t *Trace, err error) {
	if err != nil {
		return
	}

	// CNG TODO: check if failedTargetCR is correct or not
	// failedTargetCR is true there was a conflict when pushing the doc and not checking for conflicts
	failedTargetCR := t.pushed && t.pushRsp.isConflict

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

	evt := common.NewEvent(common.DataSent, nil, n, nil, additionalInfo)
	n.RaiseEvent(evt)
}
