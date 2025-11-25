package cng

import (
	"context"
	"fmt"
	"math"
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

// CheckDocument is a wrapper on the actual CheckDocument RPC call + timeout.
// It mainly maps req to the input params needed by the RPC, which a lot of boilerplate code.
func (n *Nozzle) CheckDocument(ctx context.Context, client XDCRClient, req *base.WrappedMCRequest) (rsp *internal_xdcr_v1.CheckDocumentResponse, err error) {
	revSeqNo, err := req.RevSeqNo()
	if err != nil {
		return
	}

	expiry, err := req.Expiry()
	if err != nil {
		return
	}

	n.Logger().Tracef("checkDocument req key=%[1]s%[3]s%[2]s, seqNo=%[4]v, cas=%[5]v, revSeqNo=%[6]v, expiry=%[7]v",
		base.UdTagBegin,
		base.UdTagEnd,
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
		IsDeleted:  req.Req.Opcode == mc.UPR_DELETION || req.Req.Opcode == mc.UPR_EXPIRATION,
		ExpiryTime: &expiryTime,
	}

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

func (r *PushDocRsp) Equal(other PushDocRsp) bool {
	return r.cas == other.cas &&
		r.seqNo == other.seqNo &&
		r.bytesReplicated == other.bytesReplicated &&
		r.isConflict == other.isConflict &&
		r.conflictReason == other.conflictReason
}

func (r *PushDocRsp) String() string {
	sbuf := strings.Builder{}

	sbuf.WriteString(fmt.Sprintf("cas=%v, seqNo=%v, bytesRepl=%v, isConflict=%v",
		r.cas, r.seqNo, r.bytesReplicated, r.isConflict))

	if r.isConflict {
		sbuf.WriteString(fmt.Sprintf(", reason=%s", r.conflictReason))
	}

	return sbuf.String()
}

// PushDocument is a thin wrapper on the actual PushDocument RPC call + timeout.
// It mainly maps req to the input params needed by the RPC. There is a lot
// of boilerplate code, hence the need for this wrapper.
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

	targetVBNo := req.GetTargetVB()
	if targetVBNo == math.MaxUint16 {
		err = fmt.Errorf("invalid target vbucket number %v for key %s", targetVBNo, req.OriginalKey)
		return
	}
	vbuuid, err := n.getTargetVBUUID(targetVBNo)
	if err != nil {
		return
	}

	expiryTime := timestamppb.Timestamp{Seconds: int64(expiry), Nanos: 0}

	pushDocReq := &internal_xdcr_v1.PushDocumentRequest{
		// CNG TODO: use vbuuid
		BucketName:     n.cfg.Replication.TargetBucketName,
		ScopeName:      req.TgtColNamespace.ScopeName,
		CollectionName: req.TgtColNamespace.CollectionName,
		ContentFlags:   flags,
		// Use OriginalKey which is the key before any collection prefix is added
		Key:        req.OriginalKey,
		IsDeleted:  req.Req.Opcode == mc.UPR_DELETION || req.Req.Opcode == mc.UPR_EXPIRATION,
		Revno:      revSeqNo,
		ExpiryTime: &expiryTime,
		StoreCas:   req.Req.Cas,
		VbUuid:     &vbuuid,
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
	}

	now := time.Now()
	ctxWithTimeout, cancel := context.WithTimeout(ctx, n.cfg.Tunables.Deadline)
	defer cancel()
	// See: https://docs.google.com/document/d/1aWKUgNo3icXfEX6uZBGWSaQNTJpzRXzePqGzsQA5ZfM/edit?tab=t.0#heading=h.dnhuz9b2hhsp
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

// getTargetVBUUID gets the target vbuuid for a given vbucket number
func (n *Nozzle) getTargetVBUUID(vbNo uint16) (vbuuid string, err error) {
	vbuuid, ok := n.cfg.Replication.vbUUIDMap[vbNo]
	if !ok {
		err = fmt.Errorf("unable to find vbuuid for vbucket %v", vbNo)
		return
	}

	return
}
