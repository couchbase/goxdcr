package cng

import (
	"context"
	"fmt"
	"strings"
	"time"

	mc "github.com/couchbase/gomemcached"
	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/golang/snappy"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
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

	n.Logger().Infof("checkDocument req key=%s, seqNo=%v, cas=%v, revSeqNo=%v, expiry=%v",
		string(req.OriginalKey), req.Seqno, req.Req.Cas, revSeqNo, expiry)

	expiryTime := timestamppb.Timestamp{Seconds: int64(expiry), Nanos: 0}

	checkDocReq := &internal_xdcr_v1.CheckDocumentRequest{
		// CNG TODO: use vbuuid
		BucketName:     n.cfg.Replication.TargetBucketName,
		ScopeName:      req.TgtColNamespace.ScopeName,
		CollectionName: req.TgtColNamespace.CollectionName,
		// Use OriginalKey which is the key before any collection prefix is added
		Key:        string(req.OriginalKey),
		StoreCas:   req.Req.Cas,
		Revno:      revSeqNo,
		IsDeleted:  req.Req.Opcode == mc.UPR_DELETION,
		ExpiryTime: &expiryTime,
	}

	// CNG TODO: use context for timeout and cancellation
	rsp, err = client.CheckDocument(ctx, checkDocReq)
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

	// CNG TODO: Check if expiry is already in nanoseconds or seconds
	expiryTime := timestamppb.Timestamp{Seconds: int64(expiry), Nanos: 0}

	pushDocReq := &internal_xdcr_v1.PushDocumentRequest{
		// CNG TODO: use vbuuid
		BucketName:     n.cfg.Replication.TargetBucketName,
		ScopeName:      req.TgtColNamespace.ScopeName,
		CollectionName: req.TgtColNamespace.CollectionName,
		// Use OriginalKey which is the key before any collection prefix is added
		Key: string(req.OriginalKey),
		// CNG TODO: handle binary docs
		ContentFlags: uint32(internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON),
		IsDeleted:    req.Req.Opcode == mc.UPR_DELETION,
		Revno:        revSeqNo,
		ExpiryTime:   &expiryTime,
		StoreCas:     req.Req.Cas,
	}

	content, err := getContent(n.Logger(), req)
	if err != nil {
		return
	}

	pushDocReq.Content = &internal_xdcr_v1.PushDocumentRequest_ContentCompressed{
		ContentCompressed: content.Body,
	}
	rsp.bytesReplicated = len(req.Req.Body)

	if len(content.Xattrs) > 0 {
		pushDocReq.Xattrs = content.Xattrs
	}

	now := time.Now()
	// CNG TODO: use context for timeout and cancellation
	rpcRsp, err := client.PushDocument(ctx, pushDocReq)
	if err != nil {
		var errInfo *errdetails.ErrorInfo
		errInfo, err = cngConflictError(err)
		if err != nil || errInfo == nil { // Non conflict CNG error
			return rsp, err
		}
		if errInfo.Reason != ConflictReasonDocNewer { // Non conflict CNG error
			return rsp, err
		}

		// Handle CNG conflict error
		err = nil
		rsp.isConflict = true
		rsp.conflictReason = ConflictReasonDocNewer
	}

	rsp.latency = time.Since(now)

	if rpcRsp != nil { // rpcRsp will be nil in case of conflict error
		rsp.cas = rpcRsp.Cas
		rsp.seqNo = rpcRsp.Seqno
	}
	return
}

type content struct {
	// The returned Body is always be compressed
	Body []byte
	// Xattrs will be nil if there is no xattr
	Xattrs map[string][]byte
}

// getContent extracts the body and xattr key-values into a map
// If xattr is not present, XattrMap will be nil
// If body is not compressed, it will be compressed before returning
// If body is compressed and xattr is present, xattr will be extracted and body will be recompressed
// If body is compressed and xattr is not present, body will be returned as is
func getContent(logger *log.CommonLogger, req *base.WrappedMCRequest) (c content, err error) {
	if req.Req.DataType&base.JSONDataType == 0 {
		c.Body = req.Req.Body
		return c, nil
	}

	if base.HasXattr(req.Req.DataType) {
		var body []byte
		if req.NeedToRecompress {
			// If true, then body is not compressed
			body = req.Req.Body
		} else {
			buflen := req.GetUncompressedBodySize()
			body = make([]byte, buflen)
			_, err = snappy.Decode(body, req.Req.Body)
			if err != nil {
				logger.Errorf("Failed to snappy decode body for key=%s, err=%v",
					string(req.OriginalKey), err)
				return
			}
		}
		c.Xattrs, err = getXattrMap(body)
		if err != nil {
			logger.Errorf("Failed to get xattr map for key=%s, err=%v",
				string(req.OriginalKey), err)
			return
		}

		bodyWithoutXattr, err := base.StripXattrAndGetBody(body)
		if err != nil {
			logger.Errorf("Failed to strip xattr for key=%s, err=%v",
				string(req.OriginalKey), err)
			return c, err
		}

		cbuf := make([]byte, snappy.MaxEncodedLen(len(bodyWithoutXattr)))
		c.Body = snappy.Encode(cbuf, bodyWithoutXattr)
		req.NeedToRecompress = false
		return c, nil
	}

	if req.NeedToRecompress {
		cbuf := make([]byte, snappy.MaxEncodedLen(len(req.Req.Body)))
		c.Body = snappy.Encode(cbuf, req.Req.Body)
		req.NeedToRecompress = false
	} else {
		c.Body = req.Req.Body
	}
	return
}

// getXattrMap extracts the xattr key-values into a map
// The body is expected to have xattrs and uncompressed values
// At the time of this writing, values are slices of bytes on the
// original buf.
func getXattrMap(body []byte) (m map[string][]byte, err error) {
	itr, err := base.NewXattrIterator(body)
	if err != nil {
		return nil, err
	}

	m = make(map[string][]byte)
	for itr.HasNext() {
		key, value, err := itr.Next()
		if err != nil {
			return nil, err
		}

		m[string(key)] = value
	}

	return
}
