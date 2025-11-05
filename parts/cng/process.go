package cng

import (
	"context"
	"fmt"

	mc "github.com/couchbase/gomemcached"
	"github.com/couchbase/goxdcr/v8/base"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (n *Nozzle) processReq(ctx context.Context, req *base.WrappedMCRequest) (err error) {
	err = n.connPool.WithConn(func(client XDCRClient) (err error) {
		return n.transfer(ctx, client, req)
	})
	return
}

// transfer handles the mutation transfer logic
// It does expect the Trace object to be present in the context.
// At present it is mainly consists of 2 steps:
//  1. conflict check (unless optimistic replication is used)
//  2. push document
//
// The entire transfer is retried in case of network errors.
// In CNG Phase 1 the flow of transfer is documented at:
// https://docs.google.com/document/d/1aWKUgNo3icXfEX6uZBGWSaQNTJpzRXzePqGzsQA5ZfM/edit?tab=t.0#heading=h.wm68yvvhrnh1
func (n *Nozzle) transfer(ctx context.Context, client XDCRClient, req *base.WrappedMCRequest) (err error) {
	trace, err := getTrace(ctx)
	if err != nil {
		return
	}
	trace.vbno = req.GetSourceVB()
	trace.opcode = req.Req.Opcode

	expiry, err := req.Expiry()
	if err != nil {
		return
	}
	flags, err := req.Flags()
	if err != nil {
		return
	}

	revSeqNo, err := req.RevSeqNo()
	if err != nil {
		return
	}

	n.Logger().Tracef("processing key=%s, seqno=%d, cas=%d, opcode=%s, dataType=%v, hasXattrs=%v, needsReCompression=%v, snappy?=%v, revSeqNo=%v, expiry=%v, flags=%v",
		req.OriginalKey, req.Seqno, req.Req.Cas, req.Req.Opcode, req.Req.DataType, base.HasXattr(req.Req.DataType),
		req.NeedToRecompress,
		req.Req.DataType&base.SnappyDataType > 0,
		revSeqNo,
		expiry,
		flags)

	isOptimistic := n.isOptimistic(req)
	trace.optimistic = isOptimistic
	trace.isDelete = req.Req.Opcode == mc.UPR_DELETION || req.Req.Opcode == mc.UPR_EXPIRATION
	trace.isExpiry = expiry > 0

	if !isOptimistic {
		trace.checked = true
		conflictCheckRsp, err := n.conflictCheck(ctx, client, req)
		trace.conflictCheckRsp = conflictCheckRsp
		if err != nil {
			return err
		}

		if !conflictCheckRsp.sourceWon {
			return nil
		}
	}

	trace.pushed = true
	trace.pushRsp, err = n.PushDocument(ctx, client, req)
	if err != nil {
		return
	}

	return
}

type conflictCheckRsp struct {
	sourceWon bool
	reason    string
	exists    bool
}

func (r *conflictCheckRsp) Equal(other conflictCheckRsp) bool {
	return r.sourceWon == other.sourceWon && r.reason == other.reason
}

func (c *conflictCheckRsp) String() string {
	return fmt.Sprintf("sourceWon: %v, reason: %v", c.sourceWon, c.reason)
}

// conflictCheck checks for conflicts and returns true if the source document should be transferred
// An error returned indicates that the conflict check failed.
// The caller must check sourceWon only if err is nil
func (n *Nozzle) conflictCheck(ctx context.Context, client XDCRClient, req *base.WrappedMCRequest) (rsp conflictCheckRsp, err error) {
	// CNG TODO: check for optimistic replication
	_, err = n.CheckDocument(ctx, client, req)
	err = handleConflictCheckErr(err, &rsp)
	return
}

func handleConflictCheckErr(origErr error, rsp *conflictCheckRsp) (err error) {
	err = origErr
	if err == nil {
		rsp.sourceWon = true
		rsp.reason = ConflictReasonSuccess
		return
	}

	st, ok := status.FromError(err)
	if !ok {
		return
	}

	details := st.Details()
	if len(details) == 0 {
		return
	}

	switch st.Code() {
	case codes.NotFound:
		for _, d := range details {
			switch info := d.(type) {
			case *errdetails.ResourceInfo:
				switch info.ResourceType {
				case ResourceTypeDocument:
					err = nil
					rsp.sourceWon = true
					rsp.reason = ConflictReasonDocMissing
				case ResourceTypeCollection:
					err = ErrCollectionNotFound
				case ResourceTypeScope:
					err = ErrScopeNotFound
				}
			}
		}

	case codes.Aborted:
		for _, d := range details {
			switch info := d.(type) {
			case *errdetails.ErrorInfo:
				if info.Reason == ConflictReasonDocNewer {
					err = nil
					rsp.exists = true
					rsp.sourceWon = false
					rsp.reason = ConflictReasonDocNewer
				}
			}
		}
	}

	return
}

func (n *Nozzle) isOptimistic(req *base.WrappedMCRequest) bool {
	// CNG TODO: make threshold dynamic i.e. settable at runtime
	return req.Req.Size() < n.cfg.Tunables.OptimisticThresholdSize
}
