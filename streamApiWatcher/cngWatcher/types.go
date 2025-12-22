package cngWatcher

import (
	"context"
	"sync"
	"time"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/utils"
)

// GrpcStreamManager defines an interface for managing/orchestrating streaming gRPC calls
// Mainly used to manage the lifecycle of long running streaming gRPC calls
type GrpcStreamManager[Resp any] interface {
	// Start starts the streaming gRPC call
	Start()
	// Stop stops the streaming gRPC call
	Stop()
	// GetResult returns the latest result obtained from the streaming gRPC call.
	// It blocks until the result is available or the context is done.
	// If ctx is done before the result is available, it returns a zero value of Resp and ctx.Err().
	GetResult(ctx context.Context) (Resp, error)
}

// VBucketInfoResponse is a map of vBucket number to vBucket info response
type VBucketInfoResponse map[uint32]*internal_xdcr_v1.GetVbucketInfoResponse_VbucketState

func (vbr VBucketInfoResponse) GetBucketFailoverLog() *base.BucketFailoverLog {
	bucketFailoverLog := &base.BucketFailoverLog{FailoverLogMap: make(base.FailoverLogMapType)}
	for vbno, vbInfo := range vbr {
		history := vbInfo.GetHistory()
		numEntries := uint64(len(history))
		vbFailoverLog := &base.FailoverLog{
			NumEntries: numEntries,
			LogTable:   make([]*base.FailoverEntry, numEntries),
		}
		for i, entry := range history {
			vbFailoverLog.LogTable[i] = &base.FailoverEntry{
				Uuid:      entry.GetUuid(),
				HighSeqno: entry.GetSeqno(),
			}
		}
		bucketFailoverLog.SetFailoverLog(uint16(vbno), vbFailoverLog)
	}
	return bucketFailoverLog
}

func (vbr VBucketInfoResponse) GetBucketVBStats() *base.BucketVBStats {
	bucketVBStats := &base.BucketVBStats{VBStatsMap: make(base.VBucketStatsMap)}
	for vbno, value := range vbr {
		vbStats := &base.VBucketStats{
			Uuid:      value.GetUuid(),
			HighSeqno: value.GetHighSeqno(),
			MaxCas:    value.GetMaxCas(),
		}
		bucketVBStats.SetVBStats(uint16(vbno), vbStats)
	}
	return bucketVBStats
}

// HandlerCache is a generic cache for storing the latest value received from the underlying stream
type HandlerCache[Resp any] struct {
	// currVal denotes the latest value received from the underlying stream
	currVal Resp
	// mutex protects 'currVal' from concurrent access
	mutex sync.RWMutex
	// once ensures that initDone is closed only once
	once sync.Once
	// initDone is closed when we have set the currVal
	initDone chan struct{}
}

// VbucketInfoRequestOpts represents the options for the GetVbucketInfo stream request
type VbucketInfoRequestOpts struct {
	// Bucket denotes the name of the bucket for which vbucket info is requested
	Bucket string
	// IncludeHistory denotes whether to include failover logs in the response
	IncludeHistory bool
	// IncludeMaxCas denotes whether to include the max CAS in the response
	IncludeMaxCas bool
	// VbucketIds denotes the list of vbucket IDs for which vbucket info is requested
	VbucketIds []uint32
}

// VbucketInfoHandler is a handler for the GetVbucketInfo stream
type VbucketInfoHandler struct {
	// cache denotes the cache of the VbucketInfoHandler
	cache HandlerCache[VBucketInfoResponse]
	// errorCh to signal that an error has occurred
	errorCh chan error
}

// WatchCollectionsOpState monitors the state of the watch collections operation
type WatchCollectionsOpState struct {
	// active denotes whether the operation is active
	active bool
	// activeOpCancelFunc denotes the cancellation function for the current active stream
	activeOpCancelFunc context.CancelCauseFunc
	// doneCh denotes the signal to indicate that the stream operation has completed
	doneCh chan struct{}
	// errorCh to signal that the stream rpc has terminated with an error
	errorCh chan error
	// mutex protects the opState from concurrent access
	mutex sync.RWMutex
}

// CollectionsWatcher manages the lifecycle of the CNG WatchCollections server stream RPC.
// It orchestrates stream initialization, termination and response retrieval from the stream.
type CollectionsWatcher struct {
	// bucketName denotes the name of the bucket whose manifest is being watched
	bucketName string
	// getGrpcOpts denotes the function to fetch the gRPC options
	getGrpcOpts func() *base.GrpcOptions
	// opState denotes the state of the watch collections operation
	opState WatchCollectionsOpState
	// cache denotes the cache of the CollectionsWatcher storing the validated CollectionsManifest
	cache HandlerCache[*metadata.CollectionsManifest]
	// finCh denotes the signal to terminate the watcher
	finCh chan struct{}
	// utils service here is used to issue the stream request
	utils utils.CngUtils
	// logger for logging
	logger *log.CommonLogger
	// waitGrp
	waitGrp sync.WaitGroup
	// waitTime denotes the time to wait before retrying the stream operation
	waitTime time.Duration
	// backoffFactor denotes the factor to multiply the wait time by
	backoffFactor int
	// maxWaitTime denotes the maximum time to wait before retrying the stream operation
	maxWaitTime time.Duration
	// retry denotes whether the stream operation should be retried on error
	retry bool
	// CngConn denotes the CNG connection to the target couchbase cluster
	// Darshan TODO: Ideally we should use the global connection pool instead of creating a new connection here
	// This TODO is a placeholder until we have the conn pool checked in
	cngConn *base.CngConn
	// closeOnce is used to ensure that the cngConn is closed only once
	closeOnce sync.Once
}

// NewCollectionsWatcher creates a new CollectionsWatcher
func NewCollectionsWatcher(bucketName string, getGrpcOpts func() *base.GrpcOptions, utils utils.CngUtils, waitTime time.Duration, backoffFactor int, maxWaitTime time.Duration, retry bool, logger *log.CommonLogger) GrpcStreamManager[*metadata.CollectionsManifest] {
	return &CollectionsWatcher{
		bucketName:  bucketName,
		getGrpcOpts: getGrpcOpts,
		cache: HandlerCache[*metadata.CollectionsManifest]{
			initDone: make(chan struct{}),
		},
		utils:         utils,
		waitTime:      waitTime,
		backoffFactor: backoffFactor,
		maxWaitTime:   maxWaitTime,
		retry:         retry,
		logger:        logger,
		finCh:         make(chan struct{}),
	}
}

// NewVbucketInfoHandler creates a new VbucketInfoHandler
func NewVbucketInfoHandler() *VbucketInfoHandler {
	return &VbucketInfoHandler{
		cache: HandlerCache[VBucketInfoResponse]{
			currVal:  make(VBucketInfoResponse),
			initDone: make(chan struct{}),
		},
		errorCh: make(chan error, 1),
	}
}
