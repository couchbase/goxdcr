package Component

import (
	"testing"

	mcc "github.com/couchbase/gomemcached/client/mocks"
	mccReal "github.com/couchbase/gomemcached/client"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	utilsMock "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestRemoteMemcachedComponent_GetOneTimeTgtFailoverLogs(t *testing.T) {
	type args struct {
		vbsList []uint16
	}
	tests := []struct {
		name    string
		args    args
		want    map[uint16]*mccReal.FailoverLog
		wantErr bool
	}{
		{
			name: "RetrievesFailoverLogsSuccessfully",
			args: args{
				vbsList: []uint16{0, 1},
			},
			want: map[uint16]*mccReal.FailoverLog{
				0: {},
				1: {},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			finishCh := make(chan bool)
			defer close(finishCh)

			logger := log.NewLogger("test", log.DefaultLoggerContext)
			mockUtils := &utilsMock.UtilsIface{}
			mockClient := &mcc.ClientIface{}
			mockUprFeed := &mcc.UprFeedIface{}

			r := NewRemoteMemcachedComponent(logger, finishCh, mockUtils, "testBucket")

			r.SetTargetKvVbMapGetter(func() (base.KvVBMapType, error) {
				return base.KvVBMapType{
					"127.0.0.1:11210": []uint16{0, 1},
				}, nil
			})

			r.SetRefGetter(func() *metadata.RemoteClusterReference {
				ref := &metadata.RemoteClusterReference{}
				return ref
			})

			r.SetTargetUsernameGetter(func() string { return "testUser" })
			r.SetTargetPasswordGetter(func() string { return "testPass" })
			r.SetUserAgent("testAgent")

			mockUtils.On("GetRemoteMemcachedConnection",
				"127.0.0.1:11210",
				"testUser",
				"testPass",
				"testBucket",
				"testAgent",
				true,
				base.KeepAlivePeriod,
				logger).Return(mockClient, nil)

			mockClient.On("NewUprFeedIface").Return(mockUprFeed, nil)
			mockUprFeed.On("UprOpen", "testAgent", uint32(0), base.MaxDCPConnectionBufferSize).Return(nil)
			mockUprFeed.On("Close").Return(nil)
			mockClient.On("UprGetFailoverLog", []uint16{0, 1}, mock.Anything).Return(tt.want, nil)
			mockClient.On("Close").Return(nil)

			got, err := r.GetOneTimeTgtFailoverLogs(tt.args.vbsList)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetOneTimeTgtFailoverLogs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRemoteMemcachedComponent_GetOneTimeTgtFailoverLogs_BytesUsedCallbackInvoked(t *testing.T) {
	finishCh := make(chan bool)
	defer close(finishCh)

	logger := log.NewLogger("test", log.DefaultLoggerContext)
	mockUtils := &utilsMock.UtilsIface{}
	mockClient := &mcc.ClientIface{}
	mockUprFeed := &mcc.UprFeedIface{}

	r := NewRemoteMemcachedComponent(logger, finishCh, mockUtils, "testBucket")

	r.SetTargetKvVbMapGetter(func() (base.KvVBMapType, error) {
		return base.KvVBMapType{
			"127.0.0.1:11210": []uint16{0, 1},
		}, nil
	})

	r.SetRefGetter(func() *metadata.RemoteClusterReference {
		ref := &metadata.RemoteClusterReference{}
		return ref
	})

	r.SetTargetUsernameGetter(func() string { return "testUser" })
	r.SetTargetPasswordGetter(func() string { return "testPass" })
	r.SetUserAgent("testAgent")

	var capturedBytesUsed int
	r.SetDataTransferredIncrementer(func(bytes int) {
		capturedBytesUsed = bytes
	})

	mockUtils.On("GetRemoteMemcachedConnection",
		"127.0.0.1:11210",
		"testUser",
		"testPass",
		"testBucket",
		"testAgent",
		true,
		base.KeepAlivePeriod,
		logger).Return(mockClient, nil)

	mockClient.On("NewUprFeedIface").Return(mockUprFeed, nil)
	mockUprFeed.On("UprOpen", "testAgent", uint32(0), base.MaxDCPConnectionBufferSize).Return(nil)
	mockUprFeed.On("Close").Return(nil)

	expectedBytesUsed := 123
	mockClient.On("UprGetFailoverLog", []uint16{0, 1}, mock.Anything).Run(func(args mock.Arguments) {
		ctx := args.Get(1).(*mccReal.ClientContext)
		if ctx.BytesUsedCallback != nil {
			ctx.BytesUsedCallback(expectedBytesUsed)
		}
	}).Return(map[uint16]*mccReal.FailoverLog{
		0: {},
		1: {},
	}, nil)
	mockClient.On("Close").Return(nil)

	got, err := r.GetOneTimeTgtFailoverLogs([]uint16{0, 1})

	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, expectedBytesUsed, capturedBytesUsed, "DataTransferredIncrementer should be called with the same value as BytesUsedCallback")
}
