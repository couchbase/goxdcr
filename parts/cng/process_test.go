package cng

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	mc "github.com/couchbase/gomemcached"
	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	basecng "github.com/couchbase/goxdcr/v8/base/cng"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	service_mocks "github.com/couchbase/goxdcr/v8/service_def/mocks"
	utilsMock "github.com/couchbase/goxdcr/v8/utils/mocks"
)

type transferParams struct {
	name                 string
	req                  *base.WrappedMCRequest
	checkDocRsp          basecng.CheckDocumentRsp
	pushDocRsp           basecng.PushDocumentRsp
	shouldFail           bool
	trace                Trace
	grpcStatus           *status.Status
	CNGErrorCode         CNGErrorCode
	isUpstreamReportable bool
}

func makeExtras(flags, expiry uint32, revSeqno, cas uint64) []byte {
	extras := make([]byte, 24)
	binary.BigEndian.PutUint32(extras[0:4], flags)
	binary.BigEndian.PutUint32(extras[4:8], expiry)
	binary.BigEndian.PutUint64(extras[8:16], revSeqno)
	binary.BigEndian.PutUint64(extras[16:24], cas)
	return extras
}

func makeWrappedMCRequest(opcode mc.CommandCode, value []byte, flags, expiry uint32, revSeqNo, cas uint64) *base.WrappedMCRequest {
	if cas == 0 {
		cas = uint64(time.Now().Unix())
	}
	key := "testKey"
	extras := makeExtras(flags, expiry, revSeqNo, cas)
	return &base.WrappedMCRequest{
		Req: &mc.MCRequest{
			Opcode:  opcode,
			VBucket: 73,
			Key:     []byte(key),
			Body:    value,
			Extras:  extras,
		},
		OriginalKey:     key,
		TgtColNamespace: &base.CollectionNamespace{ScopeName: "_default", CollectionName: "_default"},
	}
}

func TestTransfer(t *testing.T) {
	nonOptimisticPayload := []byte(`{
	"1": "abcedefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789",
	"2": "abcedefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789",
	"3": "abcedefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789",
	"4": "abcedefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789"
	}`)
	optimisticPayload := []byte(`{"a": 1}`)

	optPayloadCompressed := snappy.Encode(nil, optimisticPayload)
	nonOptPayloadCompressed := snappy.Encode(nil, nonOptimisticPayload)

	tests := []*transferParams{
		{
			name: "ok: optimistic, simple transfer",
			req:  makeWrappedMCRequest(mc.UPR_MUTATION, optimisticPayload, 7, 0, 0, 0),
			checkDocRsp: basecng.CheckDocumentRsp{
				Rsp: internal_xdcr_v1.CheckDocumentResponse{},
				Err: nil,
			},
			pushDocRsp: basecng.PushDocumentRsp{
				Rsp: internal_xdcr_v1.PushDocumentResponse{Cas: 12345, Seqno: 1},
				Err: nil,
			},
			trace: Trace{
				opcode:     mc.UPR_MUTATION,
				vbno:       73,
				optimistic: true,
				checked:    false,
				pushed:     true,
				pushRsp:    PushDocRsp{cas: 12345, seqNo: 1, bytesReplicated: len(optPayloadCompressed), isConflict: false},
			},
		},
		{
			name: "ok: non-optimistic, simple transfer",
			req:  makeWrappedMCRequest(mc.UPR_MUTATION, nonOptimisticPayload, 7, 0, 0, 0),
			checkDocRsp: basecng.CheckDocumentRsp{
				Rsp: internal_xdcr_v1.CheckDocumentResponse{},
				Err: nil,
			},
			pushDocRsp: basecng.PushDocumentRsp{
				Rsp: internal_xdcr_v1.PushDocumentResponse{Cas: 12345, Seqno: 1},
				Err: nil,
			},
			trace: Trace{
				opcode:           mc.UPR_MUTATION,
				vbno:             73,
				checked:          true,
				conflictCheckRsp: conflictCheckRsp{sourceWon: true, reason: ConflictReasonSuccess},
				pushed:           true,
				pushRsp:          PushDocRsp{cas: 12345, seqNo: 1, bytesReplicated: len(nonOptPayloadCompressed), isConflict: false},
			},
		},
		{
			name: "ok: delete optimistic",
			req:  makeWrappedMCRequest(mc.UPR_DELETION, optimisticPayload, 7, 0, 0, 0),
			checkDocRsp: basecng.CheckDocumentRsp{
				Rsp: internal_xdcr_v1.CheckDocumentResponse{},
				Err: nil,
			},
			pushDocRsp: basecng.PushDocumentRsp{
				Rsp: internal_xdcr_v1.PushDocumentResponse{Cas: 12345, Seqno: 1},
				Err: nil,
			},
			trace: Trace{
				opcode:           mc.UPR_DELETION,
				vbno:             73,
				isDelete:         true,
				optimistic:       true,
				conflictCheckRsp: conflictCheckRsp{sourceWon: true, reason: ConflictReasonDocMissing},
				pushed:           true,
				pushRsp:          PushDocRsp{cas: 12345, seqNo: 1, bytesReplicated: 0, isConflict: false},
			},
		},
		{
			name: "ok: delete non-optimistic",
			req:  makeWrappedMCRequest(mc.UPR_DELETION, nonOptimisticPayload, 7, 0, 0, 0),
			checkDocRsp: basecng.CheckDocumentRsp{
				Rsp: internal_xdcr_v1.CheckDocumentResponse{},
				Err: nil,
			},
			pushDocRsp: basecng.PushDocumentRsp{
				Rsp: internal_xdcr_v1.PushDocumentResponse{Cas: 12345, Seqno: 1},
				Err: nil,
			},
			trace: Trace{
				opcode:           mc.UPR_DELETION,
				vbno:             73,
				isDelete:         true,
				checked:          true,
				conflictCheckRsp: conflictCheckRsp{sourceWon: true, reason: ConflictReasonSuccess},
				pushed:           true,
				pushRsp:          PushDocRsp{cas: 12345, seqNo: 1, bytesReplicated: 0, isConflict: false},
			},
		},
		{
			name: "ok: optimistic, source lost conflict",
			req:  makeWrappedMCRequest(mc.UPR_MUTATION, optimisticPayload, 7, 0, 0, 0),
			checkDocRsp: basecng.CheckDocumentRsp{
				Rsp: internal_xdcr_v1.CheckDocumentResponse{
					Exists: true,
				},
				Err: nil,
			},
			pushDocRsp: basecng.PushDocumentRsp{
				Rsp: internal_xdcr_v1.PushDocumentResponse{Cas: 12345, Seqno: 1},
				Err: nil,
			},
			trace: Trace{
				opcode:     mc.UPR_MUTATION,
				vbno:       73,
				optimistic: true,
				checked:    false,
				pushed:     true,
				pushRsp:    PushDocRsp{cas: 12345, seqNo: 1, bytesReplicated: len(optPayloadCompressed), isConflict: false},
			},
		},
		{
			name: "fail: optimistic, push doc fails due to collection missing",
			req:  makeWrappedMCRequest(mc.UPR_MUTATION, optimisticPayload, 7, 0, 0, 0),
			checkDocRsp: basecng.CheckDocumentRsp{
				Rsp: internal_xdcr_v1.CheckDocumentResponse{},
				Err: nil,
			},
			pushDocRsp: basecng.PushDocumentRsp{
				Rsp: internal_xdcr_v1.PushDocumentResponse{Cas: 12345, Seqno: 1},
				Err: newCollectionMissingGRPCError(),
			},
			trace: Trace{
				opcode:     mc.UPR_MUTATION,
				vbno:       73,
				optimistic: true,
				checked:    false,
				pushed:     true,
				pushRsp:    PushDocRsp{cas: 12345, seqNo: 1, bytesReplicated: len(optPayloadCompressed), isConflict: false},
			},
			shouldFail:           true,
			CNGErrorCode:         ERR_COLLECTION_NOT_FOUND,
			isUpstreamReportable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := log.DefaultLoggerContext
			vbUUIDMap := map[uint16]uint64{}
			for i := range uint16(1024) {
				vbUUIDMap[i] = uint64(i + 1000)
			}

			n, err := New("testNozzle", logger, &Config{
				Replication: ReplicationConfig{
					SourceClusterUUID: "source1",
					SourceBucketName:  "B1",
					SourceBucketUUID:  "sourceB1",
					TargetBucketName:  "B1",
					TargetBucketUUID:  "targetB1",
					TargetClusterUUID: "target1",
					vbUUIDMap:         vbUUIDMap,
				},
				Services: Services{
					RemoteClusterSvc: service_mocks.NewRemoteClusterSvc(t),
					Utils:            utilsMock.NewUtilsIface(t),
				},
			})
			assert.NoError(t, err)

			n.cfg.Tunables.SetOptimisticThresholdSize(256)
			n.cfg.Tunables.Deadline = 5 * time.Second

			ctx := context.Background()
			mockClient := &basecng.MockCngXdcrClient{
				CheckDocRsp: tt.checkDocRsp,
				PushDocRsp:  tt.pushDocRsp,
			}

			childCtx := startTrace(ctx, &Trace{})
			err = n.transfer(childCtx, mockClient, tt.req)
			if tt.shouldFail {
				assert.Error(t, err)
				if tt.grpcStatus != nil {
					assert.True(t, isSameGrpcStatus(err, tt.grpcStatus), "expected grpc status \n%+v\ngot\n%+v", tt.grpcStatus, err)
				}

				cngErr := mapToCNGError(err)
				if tt.CNGErrorCode != NO_ERROR {
					assert.Equal(t, tt.CNGErrorCode, cngErr.Code, "expected CNG error code %v, got %v", tt.CNGErrorCode, cngErr.Code)
				}

				assert.Equal(t, tt.isUpstreamReportable, isErrorUpstreamReportable(cngErr),
					"isErrorUpstreamReportable mismatch cngErrorCode=%v", cngErr.Code)

				return
			}
			assert.NoError(t, err)
			trace, _ := getTrace(childCtx)
			assert.True(t, tt.trace.Equal(*trace), "expected trace \n%+v\n%+v", tt.trace, trace)
		})
	}
}

func isSameGrpcStatus(err error, st *status.Status) bool {
	if err == nil && st == nil {
		return true
	}
	if err == nil || st == nil {
		return false
	}

	errStatus, ok := status.FromError(err)
	if !ok {
		return false
	}

	if errStatus.Code() != st.Code() {
		return false
	}

	details := errStatus.Details()
	expectedDetails := st.Details()
	if len(details) != len(expectedDetails) {
		return false
	}

	for i, d := range details {
		switch info := d.(type) {
		case *errdetails.ResourceInfo:
			expectedInfo, ok := expectedDetails[i].(*errdetails.ResourceInfo)
			if !ok {
				return false
			}
			if info.ResourceType != expectedInfo.ResourceType || info.ResourceName != expectedInfo.ResourceName {
				return false
			}
		case *errdetails.ErrorInfo:
			expectedInfo, ok := expectedDetails[i].(*errdetails.ErrorInfo)
			if !ok {
				return false
			}
			if info.Reason != expectedInfo.Reason {
				return false
			}
		default:
			// If there are other types of details, we can add more cases here
			return false
		}
	}

	return true
}

func newCollectionMissingGRPCError() error {
	st := status.New(codes.NotFound, "Collection is missing")
	st, _ = st.WithDetails(&errdetails.ResourceInfo{
		ResourceType: "collection",
		ResourceName: "Collection is missing",
	})
	return st.Err()
}
