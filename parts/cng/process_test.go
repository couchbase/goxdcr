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
	"github.com/stretchr/testify/assert"

	service_mocks "github.com/couchbase/goxdcr/v8/service_def/mocks"
	utilsMock "github.com/couchbase/goxdcr/v8/utils/mocks"
)

type transferParams struct {
	name        string
	req         *base.WrappedMCRequest
	checkDocRsp basecng.CheckDocumentRsp
	pushDocRsp  basecng.PushDocumentRsp
	shouldFail  bool
	trace       mutationTrace
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
		OriginalKey:     []byte(key),
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

	tests := []transferParams{
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
			trace: mutationTrace{
				opcode:     mc.UPR_MUTATION,
				vbno:       73,
				optimistic: true,
				checked:    false,
				pushed:     true,
				pushRsp:    PushDocRsp{cas: 12345, seqNo: 1, bytesReplicated: len(optimisticPayload), isConflict: false},
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
			trace: mutationTrace{
				opcode:           mc.UPR_MUTATION,
				vbno:             73,
				checked:          true,
				conflictCheckRsp: conflictCheckRsp{sourceWon: true, reason: ConflictReasonDocMissing},
				pushed:           true,
				pushRsp:          PushDocRsp{cas: 12345, seqNo: 1, bytesReplicated: len(nonOptimisticPayload), isConflict: false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := log.DefaultLoggerContext
			n, err := New("testNozzle", logger, Config{
				Replication: ReplicationConfig{
					SNGAddr:           "cnghost:18098",
					SourceClusterUUID: "source1",
					SourceBucketName:  "B1",
					SourceBucketUUID:  "sourceB1",
					TargetBucketName:  "B1",
					TargetBucketUUID:  "targetB1",
					TargetClusterUUID: "target1",
				},
				Services: Services{
					RemoteClusterSvc: service_mocks.NewRemoteClusterSvc(t),
					Utils:            utilsMock.NewUtilsIface(t),
				},
			})
			assert.NoError(t, err)
			ctx := context.Background()
			mockClient := &basecng.MockCngXdcrClient{
				CheckDocRsp: tt.checkDocRsp,
				PushDocRsp:  tt.pushDocRsp,
			}

			trace, err := n.transfer(ctx, mockClient, tt.req)
			if tt.shouldFail {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.True(t, tt.trace.Equal(trace), "expected trace \n%+v\n%+v", tt.trace, trace)
		})
	}
}
