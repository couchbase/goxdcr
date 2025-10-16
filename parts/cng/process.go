package cng

import (
	"context"
	"fmt"
	"strings"
	"time"

	mc "github.com/couchbase/gomemcached"
	"github.com/couchbase/goxdcr/v8/base"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

// mutationTrace keeps track of the operations performed for a mutation
// The fields are set during various stages of processing
type mutationTrace struct {
	opcode mc.CommandCode

	// source vbucket number
	vbno uint16

	// optimistic indicates whether replication is done in optimistic mode or not
	optimistic bool

	// isExpiry indicates whether the mutation has an expiration
	isExpiry bool

	// isDelete indicates whether the mutation is a deletion
	isDelete bool

	// commitTime is the time taken for from DCP to processing completion
	commitTime time.Duration

	// checked indicates whether conflict check was performed
	checked bool
	// conflictCheckRsp holds the response from the conflict check
	// This is relevant only if checked is true
	conflictCheckRsp conflictCheckRsp

	// pushed indicates whether pushDocument was performed
	pushed bool
	// pushRsp holds the response from the pushDocument
	// This is relevant only if pushed is true
	pushRsp PushDocRsp
}

func (d mutationTrace) String() string {
	sbuf := strings.Builder{}
	sbuf.WriteString(fmt.Sprintf("opcode=%s, vbno=%d, optimistic=%v, isDel=%v, isExp=%v, checked: %v",
		d.opcode.String(), d.vbno, d.optimistic, d.isDelete, d.isExpiry, d.checked))
	if d.checked {
		sbuf.WriteString(fmt.Sprintf(", conflictRsp: (%v)", d.conflictCheckRsp))
	}
	sbuf.WriteString(fmt.Sprintf(", pushed: %v", d.pushed))
	if d.pushed {
		sbuf.WriteString(fmt.Sprintf(", pushRsp: (%s)", d.pushRsp.String()))
	}

	return sbuf.String()
}

func (n *Nozzle) processReq(ctx context.Context, req *base.WrappedMCRequest) (trace mutationTrace, err error) {
	err = n.connPool.WithConn(func(client XDCRClient) (err error) {
		trace, err = n.transfer(ctx, client, req)
		return err
	})
	return
}

func (n *Nozzle) transfer(ctx context.Context, client XDCRClient, req *base.WrappedMCRequest) (trace mutationTrace, err error) {
	trace.opcode = req.Req.Opcode

	n.Logger().Infof(">> processing key=%s, dataType=%v, hasXattrs=%v, needsReCompression=%v, snappy?=%v",
		string(req.OriginalKey), req.Req.DataType, base.HasXattr(req.Req.DataType),
		req.NeedToRecompress,
		req.Req.DataType&base.SnappyDataType > 0)

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
	n.Logger().Infof("processing key=%s, seqNo=%v, cas=%v, opcode=%v, revSeqNo=%v, flags=%v, expiry=%v",
		string(req.OriginalKey), req.Seqno, req.Req.Cas, req.Req.Opcode, revSeqNo, flags, expiry)

	isOptimistic := n.isOptimistic(req)
	trace.optimistic = isOptimistic
	trace.isDelete = req.Req.Opcode == mc.UPR_DELETION
	trace.isExpiry = expiry > 0

	if !isOptimistic {
		trace.checked = true
		conflictRsp, err := n.conflictCheck(ctx, client, req)
		if err != nil {
			st, ok := status.FromError(err)
			if !ok {
				return trace, err
			}

			// CNG TODO: make it a util to log grpc errors
			n.Logger().Errorf("conflictCheck failed, code=%v, details=%v, message=%v",
				st.Code(), st.Details(), st.Message())
			return trace, err
		}

		trace.conflictCheckRsp = conflictRsp

		n.Logger().Infof("conflictCheck response=%v", conflictRsp)
		if !conflictRsp.sourceWon {
			return trace, nil
		}
	}

	trace.pushed = true
	pushRsp, err := n.PushDocument(ctx, client, req)
	if err != nil {
		return
	}

	trace.pushRsp = pushRsp
	return
}

type conflictCheckRsp struct {
	sourceWon bool
	reason    string
}

func (c conflictCheckRsp) String() string {
	return fmt.Sprintf("sourceWon: %v, reason: %v", c.sourceWon, c.reason)
}

// conflictCheck checks for conflicts and returns true if the source document should be transferred
// An error returned indicates that the conflict check failed. The caller must check sourceWon only if err is nil
func (n *Nozzle) conflictCheck(ctx context.Context, client XDCRClient, req *base.WrappedMCRequest) (rsp conflictCheckRsp, err error) {
	// CNG TODO: check for optimistic replication

	checkRsp, err := n.CheckDocument(ctx, client, req)
	if err == nil {
		rsp.sourceWon = !checkRsp.Exists
		rsp.reason = ConflictReasonDocMissing
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

	// We check only the first detail for now
	// Have not seen multiple details so far

	detail, ok := details[0].(*errdetails.ErrorInfo)
	if !ok || detail.Reason != ConflictReasonDocNewer {
		return
	}

	rsp.sourceWon = false
	rsp.reason = ConflictReasonDocNewer

	err = nil

	return
}

// cngConflictError checks if the error is a CNG conflict error and returns the ErrorInfo
// and err is returned as nil. If the error is not a CNG conflict error, the original
// error is returned
func cngConflictError(cngErr error) (errInfo *errdetails.ErrorInfo, err error) {
	st, ok := status.FromError(cngErr)
	if !ok {
		err = cngErr
		return
	}

	details := st.Details()
	if len(details) == 0 {
		err = cngErr
		return
	}

	// We check only the first detail for now
	// Have not seen multiple details so far

	detail, ok := details[0].(*errdetails.ErrorInfo)
	if !ok || detail.Reason != ConflictReasonDocNewer {
		err = cngErr
		return
	}

	errInfo = detail
	return
}

func (n *Nozzle) isOptimistic(req *base.WrappedMCRequest) bool {
	// CNG TODO: remove hard coded limit of 256 bytes
	return req.Req.Size() <= 256
}
