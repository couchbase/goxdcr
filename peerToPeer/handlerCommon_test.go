package peerToPeer

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def/mocks"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestHandlerCommon_RegisterOpaque(t *testing.T) {
	type fields struct {
		opaqueMapPreset map[uint32]bool
	}
	type args struct {
		request Request
		opts    *SendOpts
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "[NEGATIVE] register one opaque already present",
			fields: fields{opaqueMapPreset: map[uint32]bool{
				0: true,
			}},
			args: args{
				request: &VBMasterCheckReq{RequestCommon: NewRequestCommon("", "", "", "", 0)},
				opts:    NewSendOpts(false, 1*time.Minute, base.PeerToPeerMaxRetry),
			},
			wantErr: true,
		},
		{
			name: "[POSITIVE] register one opaque no one else present",
			args: args{
				request: &VBMasterCheckReq{RequestCommon: NewRequestCommon("", "", "", "", 0)},
				opts:    NewSendOpts(false, 1*time.Minute, base.PeerToPeerMaxRetry),
			},
		},
	}
	for _, tt := range tests {
		id := base.NewUuid()
		logger := log.NewLogger(id, log.DefaultLoggerContext)

		t.Run(tt.name, func(t *testing.T) {
			h := &HandlerCommon{
				id:                 id,
				logger:             logger,
				opaqueMap:          make(OpaqueMap),
				opaqueReqMap:       make(OpaqueReqMap),
				opaqueReqRespCbMap: make(OpaqueReqRespCbMap),
				opaqueMapMtx:       sync.RWMutex{},
				opaquesClearCh:     make(chan uint32),
				finCh:              make(chan bool),
				receiveReqCh:       make(chan interface{}),
				receiveRespCh:      make(chan interface{}),
				replSpecSvc:        &mocks.ReplicationSpecSvc{},
				replSpecFinCh:      make(map[string]chan bool),
				replSpecFinMtx:     sync.RWMutex{},
			}

			if len(tt.fields.opaqueMapPreset) > 0 {
				for k, _ := range tt.fields.opaqueMapPreset {
					h.opaqueMap[k] = nil
				}
			}

			assert := assert.New(t)
			err := h.RegisterOpaque(tt.args.request, tt.args.opts)
			if tt.wantErr {
				assert.NotNil(err)
			} else {
				assert.Nil(err)
			}
		})
	}
}
