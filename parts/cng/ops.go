package cng

import (
	"context"
	"fmt"
	"strings"
	"time"

	mc "github.com/couchbase/gomemcached"
	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (n *Nozzle) CheckDocument(ctx context.Context, client XDCRClient, req *base.WrappedMCRequest) (rsp *internal_xdcr_v1.CheckDocumentResponse, err error) {
	revSeqNo, err := req.RevSeqNo()
	if err != nil {
		return
	}

	expiry, err := req.Expiry()
	if err != nil {
		return
	}

	n.Logger().Tracef("checkDocument req key=%s, seqNo=%v, cas=%v, revSeqNo=%v, expiry=%v",
		req.OriginalKey, req.Seqno, req.Req.Cas, revSeqNo, expiry)

	expiryTime := timestamppb.Timestamp{Seconds: int64(expiry), Nanos: 0}

	checkDocReq := &internal_xdcr_v1.CheckDocumentRequest{
		// CNG TODO: use vbuuid
		BucketName:     n.cfg.Replication.TargetBucketName,
		ScopeName:      req.TgtColNamespace.ScopeName,
		CollectionName: req.TgtColNamespace.CollectionName,
		// Use OriginalKey which is the key before any collection prefix is added
		Key:        req.OriginalKey,
		StoreCas:   req.Req.Cas,
		Revno:      revSeqNo,
		IsDeleted:  req.Req.Opcode == mc.UPR_DELETION,
		ExpiryTime: &expiryTime,
	}

	// CNG TODO: use context for timeout and cancellation
	// Return values from CheckDocument RPC
	// if Mutation:
	// 	- Document does not exist, err == nil
	// 	- Document exists
	//       - Source wins, err == nil
	//       - Target wins, err == GRPC Error with Code=Aborted, and ErrorInfo with reason=doc_newer
	// if Deletion:
	//  - Document does not exist, err == GRPC Error with Code=NotFound
	//  - Document exists
	//       - Source wins, err == nil
	//       - Target wins, err == GRPC Error with Code=Aborted, and ErrorInfo with reason=doc_newer
	ctxWithTimeout, cancel := context.WithTimeout(ctx, n.cfg.Tunables.Deadline)
	defer cancel()
	rsp, err = client.CheckDocument(ctxWithTimeout, checkDocReq)
	return
}

type PushDocRsp struct {
	cas             uint64
	seqNo           uint64
	bytesReplicated int
	isConflict      bool
	conflictReason  string
	latency         time.Duration
}

func (r PushDocRsp) Equal(other PushDocRsp) bool {
	return r.cas == other.cas &&
		r.seqNo == other.seqNo &&
		r.bytesReplicated == other.bytesReplicated &&
		r.isConflict == other.isConflict &&
		r.conflictReason == other.conflictReason
}

func (r PushDocRsp) String() string {
	sbuf := strings.Builder{}

	sbuf.WriteString(fmt.Sprintf("cas=%v, seqNo=%v, bytesRepl=%v, isConflict=%v",
		r.cas, r.seqNo, r.bytesReplicated, r.isConflict))

	if r.isConflict {
		sbuf.WriteString(fmt.Sprintf(", reason=%s", r.conflictReason))
	}

	return sbuf.String()
}

func (n *Nozzle) PushDocument(ctx context.Context, client XDCRClient, req *base.WrappedMCRequest) (rsp PushDocRsp, err error) {
	revSeqNo, err := req.RevSeqNo()
	if err != nil {
		return
	}

	expiry, err := req.Expiry()
	if err != nil {
		return
	}

	flags, err := req.Flags()
	if err != nil {
		return
	}

	// CNG TODO: Check if expiry is already in nanoseconds or seconds
	expiryTime := timestamppb.Timestamp{Seconds: int64(expiry), Nanos: 0}

	pushDocReq := &internal_xdcr_v1.PushDocumentRequest{
		// CNG TODO: use vbuuid
		BucketName:     n.cfg.Replication.TargetBucketName,
		ScopeName:      req.TgtColNamespace.ScopeName,
		CollectionName: req.TgtColNamespace.CollectionName,
		ContentFlags:   flags,
		// Use OriginalKey which is the key before any collection prefix is added
		Key: req.OriginalKey,
		// CNG TODO: handle binary docs
		//ContentFlags: uint32(internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON),
		IsDeleted:  req.Req.Opcode == mc.UPR_DELETION,
		Revno:      revSeqNo,
		ExpiryTime: &expiryTime,
		StoreCas:   req.Req.Cas,
	}

	if req.Req.Opcode == mc.UPR_MUTATION {
		content, err := getContent(n.Logger(), req)
		if err != nil {
			return rsp, err
		}

		if content.IsJson {
			pushDocReq.ContentType = internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON
		} else {
			pushDocReq.ContentType = internal_xdcr_v1.ContentType_CONTENT_TYPE_NONJSON
		}

		if content.NotCompressed {
			pushDocReq.Content = &internal_xdcr_v1.PushDocumentRequest_ContentUncompressed{
				ContentUncompressed: content.Body,
			}
		} else {
			pushDocReq.Content = &internal_xdcr_v1.PushDocumentRequest_ContentCompressed{
				ContentCompressed: content.Body,
			}
		}
		rsp.bytesReplicated = len(content.Body)

		if len(content.Xattrs) > 0 {
			pushDocReq.Xattrs = content.Xattrs
		}
		n.Logger().Debugf("PushDocument content, key=%s, dataType=%v, hasXattrs=%v, notCompressed=%v, isJson=%v",
			req.OriginalKey, req.Req.DataType, len(content.Xattrs) > 0, content.NotCompressed, content.IsJson)
	}

	now := time.Now()
	// CNG TODO: use context for timeout and cancellation
	ctxWithTimeout, cancel := context.WithTimeout(ctx, n.cfg.Tunables.Deadline)
	defer cancel()
	rpcRsp, err := client.PushDocument(ctxWithTimeout, pushDocReq)
	rsp.latency = time.Since(now)
	err = handlePushDocErr(err, &rsp)
	if err != nil {
		return
	}

	if rpcRsp != nil { // rpcRsp will be nil in case of conflict error
		rsp.cas = rpcRsp.Cas
		rsp.seqNo = rpcRsp.Seqno
	}
	return
}

func handlePushDocErr(origErr error, rsp *PushDocRsp) (err error) {
	err = origErr
	if err == nil {
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
	case codes.Aborted:
		for _, d := range details {
			switch info := d.(type) {
			case *errdetails.ErrorInfo:
				if info.Reason == ConflictReasonDocNewer {
					err = nil
					rsp.isConflict = true
					rsp.conflictReason = ConflictReasonDocNewer
				}
			}
		}
	}

	return
}
