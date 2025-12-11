package pipeline_svc

import (
	"fmt"
	"sync"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
)

// remoteOps is a struct that encapsulates the checkpointMgr remote operations
type remoteOps struct {
	// bucketStatsProvider is the service that provides the bucket stats
	bucketStatsProvider service_def.BucketStatsOps
	// remoteBucket is the remote bucket information
	remoteBucket *service_def.RemoteBucketInfo
	// bucketFailoverLogForPreReplicate is the failover log for pre-replicate
	bucketFailoverLogForPreReplicate *base.BucketFailoverLog
	// Mutex is used to protect the failover log for pre-replicate
	mutex sync.Mutex
	// srcVbList is the list of vbuckets for which the remote operations are performed in legacy mode
	srcVbList []uint16
	// tgtVbList is the list of vbuckets for which the remote operations are performed in variable VB mode
	tgtVbList []uint16
	// isVariableVBMode is a flag to indicate if the variable VB mode is enabled
	isVariableVBMode bool
	// logger is the logger for the remoteOps
	logger *log.CommonLogger
	// finCh is the channel that is used to signal the completion of the remote operations
	finCh chan bool
	// capiSvc is the service that provides the capi service
	capiSvc service_def.CAPIService
}

// NewRemoteOps is the constructor for the remoteOps struct
func NewRemoteOps(bucketStatsProvider service_def.BucketStatsOps, srcVbList, tgtVbList []uint16, isVariableVBMode bool, remoteBucket *service_def.RemoteBucketInfo, finCh chan bool, capi_svc service_def.CAPIService, logger *log.CommonLogger) *remoteOps {
	return &remoteOps{
		bucketStatsProvider: bucketStatsProvider,
		srcVbList:           srcVbList,
		tgtVbList:           tgtVbList,
		remoteBucket:        remoteBucket,
		logger:              logger,
		finCh:               finCh,
		isVariableVBMode:    isVariableVBMode,
		capiSvc:             capi_svc,
	}
}

// GetHighSeqNos retrieves the high sequence numbers for the given vbuckets from the remote bucket
func (r *remoteOps) GetHighSeqNos() (*base.BucketVBStats, base.ErrorMap, error) {
	vbList := r.srcVbList
	if r.isVariableVBMode {
		vbList = r.tgtVbList
	}
	return r.bucketStatsProvider.GetVBucketStats(&base.VBucketStatsRequest{
		VBuckets:   vbList,
		MaxCasOnly: false,
		FinCh:      r.finCh,
	})
}

// GetFailoverLog retrieves the failover log for the given vbuckets from the remote bucket
func (r *remoteOps) GetFailoverLog() (*base.BucketFailoverLog, base.ErrorMap, error) {
	vbList := r.srcVbList
	if r.isVariableVBMode {
		vbList = r.tgtVbList
	}
	requestOpts := &base.FailoverLogRequest{
		VBuckets: vbList,
		FinCh:    r.finCh,
	}
	return r.bucketStatsProvider.GetFailoverLog(requestOpts)
}

// LegacyPreReplicate is the legacy pre-replicate function
func (r *remoteOps) LegacyPreReplicate(tgtTs *service_def.RemoteVBReplicationStatus) (bool, metadata.TargetVBOpaque, error) {
	return r.capiSvc.PreReplicate(r.remoteBucket, tgtTs, false)
}

// PrepareForLocalPreReplicate prepares the failover log for local pre-replicate
// It ensures that the failover log is in a state where it can be used for local pre-replicate
// It modifies the failover log in place
func (r *remoteOps) PrepareForLocalPreReplicate(bucketFailoverLog *base.BucketFailoverLog) error {
	bucketFailoverLog.Mutex.Lock()
	defer bucketFailoverLog.Mutex.Unlock()

	for _, failoverLog := range bucketFailoverLog.FailoverLogMap {
		err := prepareForLocalPreReplicate(failoverLog)
		if err != nil {
			return err
		}
	}
	return nil
}

// PreReplicate verifies if the tgtTs is a valid with respect to the failover log
// If returns:
// 1. match: true if the tgtTs is valid with respect to the failover log
// 2. targetVBOpaque: denotes the latest/current vbucketUUID of  the vbucket
// 3. error: any error that occurred during the operation
func (r *remoteOps) PreReplicate(tgtTs *service_def.RemoteVBReplicationStatus) (bool, metadata.TargetVBOpaque, error) {
	if base.UseLegacyPreReplicate {
		return r.LegacyPreReplicate(tgtTs)
	}

	r.mutex.Lock()
	if r.bucketFailoverLogForPreReplicate == nil {
		// If the failover log is not yet fetched, fetch it and prepare for local pre-replicate
		bucketFailoverLog, errMap, err := r.GetFailoverLog()
		if err != nil {
			return false, nil, fmt.Errorf("failed to fetch failover log: %w", err)
		}
		if len(errMap) > 0 {
			// For pre-replicate we need to have failover log for all the requested vbuckets.
			// If not, we should return an error.
			return false, nil, fmt.Errorf("failed to get failover log for all vbuckets: %v", errMap)
		}
		if err := r.PrepareForLocalPreReplicate(bucketFailoverLog); err != nil {
			return false, nil, fmt.Errorf("failed to prepare for local pre-replicate: %w", err)
		}
		r.bucketFailoverLogForPreReplicate = bucketFailoverLog
	}
	r.mutex.Unlock()

	// Get the failover log for the given vbucket
	failoverLog, err := r.bucketFailoverLogForPreReplicate.GetFailoverLog(tgtTs.VBNo)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get failover log for vbucket %d: %w", tgtTs.VBNo, err)
	}

	match := true

	if !tgtTs.IsEmpty() {
		// Create the commit opaque from tgtTs
		commitOpaque := base.NewCommitOpaque(tgtTs.VBOpaque.Value().(uint64), tgtTs.VBSeqno)
		// Verify if the tgtTs is valid with respect to the failover log
		match, err = localPreReplicate(failoverLog, commitOpaque)
		if err != nil {
			return false, nil, fmt.Errorf("failed to verify if the tgtTs is valid with respect to the failover log: %w", err)
		}
	}

	// Get the latest UUID from the failover log
	latestUUID := failoverLog.GetLatestUUID()
	if latestUUID == 0 {
		return false, nil, fmt.Errorf("failed to get latest UUID for vbucket %d", tgtTs.VBNo)
	}
	targetVBOpaque := &metadata.TargetVBUuid{Target_vb_uuid: latestUUID}

	return match, targetVBOpaque, nil
}
