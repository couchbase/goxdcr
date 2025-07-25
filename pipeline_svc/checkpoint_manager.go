// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package pipeline_svc

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/v8/peerToPeer"

	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	component "github.com/couchbase/goxdcr/v8/component"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/parts"
	"github.com/couchbase/goxdcr/v8/pipeline_utils"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"
)

const (
	XDCRCheckpointing string = "xdcrCheckpointing"
	CheckpointMgrId   string = "CheckpointMgr"
	StatsMgrId        string = "StatsMgr"
	TimeCommiting     string = "time_commiting"
	Vbno              string = "vbno"
)

var CHECKPOINT_INTERVAL = "checkpoint_interval"

var ckptRecordMismatch error = errors.New("Checkpoint Records internal version mismatch")
var targetVbuuidChangedError error = errors.New("target vbuuid has changed")
var ckptMgrStopped = errors.New("Ckptmgr has stopped")
var ckptSplitInternalSpecIds = fmt.Errorf("There is split decision on internalSpecIDs")
var errorPrevCheckpointInProgress = errors.New("Previous checkpointing operation is still in progress")

type CheckpointMgrSvc interface {
	common.PipelineService

	CheckpointsExist(topic string) (bool, error)
	DelSingleVBCheckpoint(topic string, vbno uint16, internalId string) error
	MergePeerNodesCkptInfo(genericResponse interface{}) error
}

type CheckpointManager struct {
	*component.AbstractComponent
	*component.RemoteMemcachedComponent

	pipeline common.Pipeline
	// obtained from the replication-spec when the CheckpointManager was attached to its corresponding pipeline
	pipelineReinitHash string

	srcBucketName              string
	bucketTopologySubscriberId string

	//checkpoints_svc handles CRUD operation of checkpoint docs in the metadata store
	checkpoints_svc service_def.CheckpointsService

	//capi_svc to do rest call to the remote cluster for checkpointing
	capi_svc service_def.CAPIService

	//remote cluster reference to refresh the remote bucket
	remote_cluster_svc service_def.RemoteClusterSvc

	//replication specification service
	rep_spec_svc service_def.ReplicationSpecSvc

	//xdcr topology service
	xdcr_topology_svc service_def.XDCRCompTopologySvc

	through_seqno_tracker_svc service_def.ThroughSeqnoTrackerSvc

	uiLogSvc service_def.UILogSvc

	//the interval between checkpointing in seconds
	ckpt_interval uint32

	//the channel to communicate finish signal with statistic updater
	finish_ch chan bool
	wait_grp  *sync.WaitGroup

	//chan for checkpointing tickers -- new tickers are added each time checkpoint interval is changed
	checkpoint_ticker_ch chan *time.Ticker

	//remote bucket
	remote_bucket *service_def.RemoteBucketInfo

	support_ckpt bool

	// A map of vbucket# -> CheckPointRecord
	cur_ckpts  map[uint16]*checkpointRecordWithLock
	active_vbs map[string][]uint16
	// A map of vbucket# -> FailoverLog
	failoverlog_map      map[uint16]*failoverlogWithLock
	snapshot_history_map map[uint16]*snapshotHistoryWithLock

	logger *log.CommonLogger

	target_cluster_ref *metadata.RemoteClusterReference

	utils    utilities.UtilsIface
	statsMgr service_def.StatsMgrIface

	/*
	 * Collections related
	 */
	collectionEnabledVar uint32
	cachedBrokenMap      brokenMappingWithLock
	// When Checkpoint manager is attached to a backfill pipeline,
	// The pipeline's VB first task is noted here as a starting point
	// If there is no checkpoint, then use these as starting point instead of 0
	// When a rollback is called, if rollback to 0 is requested, then these
	// starting points should be erased
	backfillTsMtx      sync.RWMutex
	backfillStartingTs map[uint16]*base.VBTimestamp
	backfillEndingTs   map[uint16]*base.VBTimestamp

	// Before a pipeline starts, prevent checkpoints from being created
	// If a ckpt is created before old ones are loaded, it could lead to incorrect
	// resuming and potential data loss
	checkpointAllowedHelper *checkpointSyncHelper

	collectionsManifestSvc service_def.CollectionsManifestSvc

	// When a pipeline starts up and if it's a backfill pipeline, and the backfill job is short
	// s.t. no ckpt is created, then it needs to have a way
	// to ensure that DelCheckpoint for a vbno is a no-op if this pipeline does not have ckpt
	// for such a VB so XDCR doesn't hit metakv un-necessarily, which is expensive
	isBackfillPipeline uint32
	bpVbMtx            sync.RWMutex
	bpVbHasCkpt        map[uint16]bool

	backfillReplSvc service_def.BackfillReplSvc

	backfillUILogMtx    sync.Mutex
	backfillLastAdded   metadata.CollectionNamespaceMapping
	backfillLastRemoved metadata.CollectionNamespaceMapping

	getBackfillMgr func() service_def.BackfillMgrIface

	bucketTopologySvc service_def.BucketTopologySvc

	getHighSeqnoAndVBUuidFromTargetCh chan bool

	// Only used to bypass all the un-mockable RPC calls
	unitTest bool

	xdcrDevInjectGcWaitSec uint32

	periodicPushRequested chan bool

	periodicPushDedupMtx sync.RWMutex
	periodicPushDedupArg *MergeCkptArgs
	periodicMerger       periodicMergerType // can be replaced for unit test

	lastHighSeqnoVbUuidMap base.HighSeqnoAndVbUuidMap

	variableVBMode bool

	rollbackCtx    *globalCkptPrereplicateCacheCtx
	rollbackCtxMtx sync.RWMutex

	// Checkpoint manager will have a sorted list of collections against which this pipeline is streaming data,
	// if the pipeline is a backfill pipeline. This list per vb will be stamped on checkpoints created by
	// this CheckpointManager instance.
	backfillCollections    map[uint16][]uint32
	backfillCollectionsMtx sync.RWMutex
}

type checkpointSyncHelper struct {
	checkpointAllowed  bool
	ongoingOps         []bool // mark false when starting, true once done
	setVBTimestampDone bool

	mtx sync.RWMutex
	cv  sync.Cond
}

func (h *checkpointSyncHelper) setVBTimestampOpDone() {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	h.setVBTimestampDone = true
}

func (h *checkpointSyncHelper) isSetVBTimestampOpDone() bool {
	h.mtx.RLock()
	defer h.mtx.RUnlock()

	return h.setVBTimestampDone
}

func (h *checkpointSyncHelper) setCheckpointAllowed() {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	h.checkpointAllowed = true
}

func (h *checkpointSyncHelper) setCheckpointDisallowed() {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	h.checkpointAllowed = false
}

func (h *checkpointSyncHelper) isCheckpointAllowed() bool {
	h.mtx.RLock()
	defer h.mtx.RUnlock()
	return h.checkpointAllowed
}

func (h *checkpointSyncHelper) registerCkptOp(setVBTimestamp bool, allowIfNoOtherOp bool) (int, error) {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	if !setVBTimestamp && !h.checkpointAllowed {
		return -1, fmt.Errorf("cannot register ckptOp because checkpoint op is not currently allowed")
	}

	if allowIfNoOtherOp {
		var numTaskNotDone int
		for _, taskDone := range h.ongoingOps {
			if !taskDone {
				numTaskNotDone++
			}
		}
		if numTaskNotDone > 0 {
			return -1, fmt.Errorf("allowIfNoOtherOp is requested but there are ongoing ops (%v)", numTaskNotDone)
		}
	}

	h.ongoingOps = append(h.ongoingOps, false)
	return len(h.ongoingOps) - 1, nil
}

func (h *checkpointSyncHelper) markTaskDone(idx int) {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	h.ongoingOps[idx] = true
	canGC := true
	for _, taskDone := range h.ongoingOps {
		if !taskDone {
			// At least one task is ongoing
			canGC = false
			break
		}
	}
	if canGC {
		// This was the only or last task, can clean up
		h.ongoingOps = h.ongoingOps[:0]
	}
	h.cv.Broadcast()
}

func (h *checkpointSyncHelper) disableCkptAndWait() {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	h.checkpointAllowed = false
	for len(h.ongoingOps) > 0 {
		// There are checkpoint ongoing
		h.cv.Wait()
	}
}

func newCheckpointSyncHelper() *checkpointSyncHelper {
	helper := &checkpointSyncHelper{}
	helper.cv.L = &helper.mtx
	return helper
}

// globalCkptPrereplicateCtx is used to cache retrieved target pre-replicate information to be reused
// with the caveat that if a period of time has passed, the cache will expire to ensure that
// no outdateed information is reused
type globalCkptPrereplicateCacheCtx struct {
	lastQueriedTimers map[string]*time.Timer
	lastQueriedbMatch map[string]bool
	lastQueriedOpaque map[string]metadata.TargetVBOpaque
	lastQueriedError  map[string]error
	mtx               *sync.RWMutex
	expireThreshold   time.Duration
	errorThreshold    time.Duration

	_preReplicate func(info *service_def.RemoteBucketInfo, status *service_def.RemoteVBReplicationStatus) (bool, metadata.TargetVBOpaque, error)
}

// write-lock must be held already
func (g *globalCkptPrereplicateCacheCtx) createTimer(tgtTs *service_def.RemoteVBReplicationStatus, err error) {
	threshold := g.expireThreshold
	if err != nil {
		threshold = g.errorThreshold
	}
	g.lastQueriedTimers[tgtTs.String()] = time.AfterFunc(threshold, func() {
		g.mtx.Lock()
		delete(g.lastQueriedTimers, tgtTs.String())
		delete(g.lastQueriedbMatch, tgtTs.String())
		delete(g.lastQueriedOpaque, tgtTs.String())
		delete(g.lastQueriedError, tgtTs.String())
		g.mtx.Unlock()
	})
}

func (g *globalCkptPrereplicateCacheCtx) preReplicate(remoteBucket *service_def.RemoteBucketInfo, tgtTs *service_def.RemoteVBReplicationStatus) (bool, metadata.TargetVBOpaque, error) {
	var needToExecuteRPC bool

	for !needToExecuteRPC {
		// First see if there is acceptable value to return
		g.mtx.RLock()
		lastMatch, lastMatchExists := g.lastQueriedbMatch[tgtTs.String()]
		lastOpaque, lastOpaqueExists := g.lastQueriedOpaque[tgtTs.String()]
		lastErr, lastErrExists := g.lastQueriedError[tgtTs.String()]
		_, timerExists := g.lastQueriedTimers[tgtTs.String()]
		g.mtx.RUnlock()
		if lastMatchExists || lastOpaqueExists || lastErrExists {
			return lastMatch, lastOpaque, lastErr
		}

		if timerExists {
			// a RPC call is under way, wait and try again
			time.Sleep(1 * time.Second)
		} else {
			// First, claim myself as the function to execute the RPC
			g.mtx.Lock()
			_, timerExists = g.lastQueriedTimers[tgtTs.String()]
			if !timerExists {
				needToExecuteRPC = true
				g.createTimer(tgtTs, nil)
				// Someone claimed a timer first and is calling the RPC. Let's wait again for it to come back and populate
			}
			g.mtx.Unlock()
		}
	}

	// At this point, need to query
	bMatch, currentRemoteVBOpaque, err := g._preReplicate(remoteBucket, tgtTs)
	// store the info for others to pull
	g.mtx.Lock()
	if _, exists := g.lastQueriedTimers[tgtTs.String()]; !exists {
		// The timer has fired and deleted everything already
		// Recreate a timer since we have exclusive lock and the lack of a timer means that
		// no one else has claimed to try to perform an RPC call
		g.createTimer(tgtTs, err)
	} else if err != nil {
		// timer was created with regular threshold by default
		// In error cases, replace it
		g.lastQueriedTimers[tgtTs.String()].Stop()
		g.createTimer(tgtTs, err)
	}

	// Note that it's possible the timer's clean up func is fired and is attempting to clean up
	// If that's the case, it's unfortunate, but the next call will trigger another clean pre_replicate call
	// which is not the end of the world
	g.lastQueriedbMatch[tgtTs.String()] = bMatch
	g.lastQueriedOpaque[tgtTs.String()] = currentRemoteVBOpaque
	g.lastQueriedError[tgtTs.String()] = err
	g.mtx.Unlock()
	return bMatch, currentRemoteVBOpaque, err
}

func (ckptMgr *CheckpointManager) newGlobalCkptPrereplicateCacheCtx(expireThreshold, errThreshold time.Duration) *globalCkptPrereplicateCacheCtx {
	return &globalCkptPrereplicateCacheCtx{
		lastQueriedTimers: map[string]*time.Timer{},
		lastQueriedbMatch: map[string]bool{},
		lastQueriedOpaque: map[string]metadata.TargetVBOpaque{},
		lastQueriedError:  map[string]error{},
		mtx:               &sync.RWMutex{},
		expireThreshold:   expireThreshold,
		errorThreshold:    errThreshold,
		_preReplicate: func(info *service_def.RemoteBucketInfo, status *service_def.RemoteVBReplicationStatus) (bool, metadata.TargetVBOpaque, error) {
			return ckptMgr.capi_svc.PreReplicate(info, status, true)
		},
	}
}

// Checkpoint Manager keeps track of one checkpointRecord per vbucket
type checkpointRecordWithLock struct {
	ckpt *metadata.CheckpointRecord
	lock *sync.RWMutex
	// Used to keep track internally of whether a write has occurred while lock was released
	versionNum uint64
}

type failoverlogWithLock struct {
	failoverlog *mcc.FailoverLog
	lock        *sync.RWMutex
}

// dcp snapshot
type snapshot struct {
	start_seqno uint64
	end_seqno   uint64
}

type snapshotHistoryWithLock struct {
	snapshot_history []*snapshot
	lock             sync.RWMutex
}

type brokenMappingWithLock struct {
	brokenMap                   metadata.CollectionNamespaceMapping
	correspondingTargetManifest uint64
	lock                        sync.RWMutex

	// To handle corner cases where certain target VBs are slow, we keep max of previous manifestHistories
	// And truncate whenever checkpoint operations take place
	// Key is the "correspondingTargetManifest"
	brokenMapHistories map[uint64]metadata.CollectionNamespaceMapping
}

// 0th element is Main pipeline Vbs->CkptDocs
// 1st element is Backfill pipeline
type VBsCkptsDocMaps []metadata.VBsCkptsDocMap

func (m *VBsCkptsDocMaps) Merge(ckpts VBsCkptsDocMaps) {
	if m == nil || *m == nil || ckpts == nil {
		// m should really not be nil when this is called
		return
	}

	for i := common.MainPipeline; i < common.PipelineTypeInvalidEnd; i++ {
		if (*m)[i] == nil {
			(*m)[i] = ckpts[i]
		} else {
			(*m)[i].MergeAndReplace(ckpts[i])
		}
	}
}

type MergeCkptArgs struct {
	PipelinesCkptDocs       VBsCkptsDocMaps
	BrokenMappingShaMap     metadata.ShaToCollectionNamespaceMap
	GlobalInfoShaMap        metadata.ShaToGlobalInfoMap
	BrokenMapSpecInternalId string
	SrcManifests            *metadata.ManifestsCache
	TgtManifests            *metadata.ManifestsCache
	// Failover logs are used for sorting checkpoint purposes and are optional
	SrcFailoverLogs map[uint16]*mcc.FailoverLog
	TgtFailoverLogs map[uint16]*mcc.FailoverLog
	// push mode only
	PushRespChs []chan error
}

type MergeCkptArgsGetter func() *MergeCkptArgs

type periodicMergerType func()

func NewCheckpointManager(checkpoints_svc service_def.CheckpointsService, capi_svc service_def.CAPIService, remote_cluster_svc service_def.RemoteClusterSvc, rep_spec_svc service_def.ReplicationSpecSvc, xdcr_topology_svc service_def.XDCRCompTopologySvc, through_seqno_tracker_svc service_def.ThroughSeqnoTrackerSvc, active_vbs map[string][]uint16, target_username, target_password, target_bucket_name string, target_kv_vb_map base.KvVBMapType, target_cluster_ref *metadata.RemoteClusterReference, logger_ctx *log.LoggerContext, utilsIn utilities.UtilsIface, statsMgr service_def.StatsMgrIface, uiLogSvc service_def.UILogSvc, collectionsManifestSvc service_def.CollectionsManifestSvc, backfillReplSvc service_def.BackfillReplSvc, getBackfillMgr func() service_def.BackfillMgrIface, bucketTopologySvc service_def.BucketTopologySvc, variableVBMode bool) (*CheckpointManager, error) {
	if checkpoints_svc == nil || capi_svc == nil || remote_cluster_svc == nil || rep_spec_svc == nil || xdcr_topology_svc == nil {
		return nil, errors.New("checkpoints_svc, capi_svc, remote_cluster_svc, rep_spec_svc, cluster_info_svc and xdcr_topology_svc can't be nil")
	}
	logger := log.NewLogger("CheckpointMgr", logger_ctx)

	finCh := make(chan bool, 1)

	ckmgr := &CheckpointManager{
		AbstractComponent: component.NewAbstractComponentWithLogger(CheckpointMgrId, logger),
		RemoteMemcachedComponent: component.NewRemoteMemcachedComponent(logger, finCh, utilsIn,
			target_bucket_name).SetTargetKvVbMapGetter(
			func() (base.KvVBMapType, error) {
				return target_kv_vb_map, nil
			}).SetTargetUsernameGetter(
			func() string {
				return target_username
			}).SetTargetPasswordGetter(
			func() string {
				return target_password
			}).SetRefGetter(
			func() *metadata.RemoteClusterReference {
				return target_cluster_ref
			}).SetAlternateAddressChecker(
			func(ref *metadata.RemoteClusterReference) (bool, error) {
				return remote_cluster_svc.ShouldUseAlternateAddress(ref)
			}),
		pipeline:                          nil,
		pipelineReinitHash:                "",
		checkpoints_svc:                   checkpoints_svc,
		capi_svc:                          capi_svc,
		rep_spec_svc:                      rep_spec_svc,
		remote_cluster_svc:                remote_cluster_svc,
		xdcr_topology_svc:                 xdcr_topology_svc,
		through_seqno_tracker_svc:         through_seqno_tracker_svc,
		finish_ch:                         finCh,
		checkpoint_ticker_ch:              make(chan *time.Ticker, 1000),
		logger:                            logger,
		cur_ckpts:                         make(map[uint16]*checkpointRecordWithLock),
		active_vbs:                        active_vbs,
		wait_grp:                          &sync.WaitGroup{},
		failoverlog_map:                   make(map[uint16]*failoverlogWithLock),
		snapshot_history_map:              make(map[uint16]*snapshotHistoryWithLock),
		target_cluster_ref:                target_cluster_ref,
		utils:                             utilsIn,
		statsMgr:                          statsMgr,
		uiLogSvc:                          uiLogSvc,
		collectionsManifestSvc:            collectionsManifestSvc,
		backfillStartingTs:                make(map[uint16]*base.VBTimestamp),
		backfillEndingTs:                  make(map[uint16]*base.VBTimestamp),
		backfillReplSvc:                   backfillReplSvc,
		getBackfillMgr:                    getBackfillMgr,
		checkpointAllowedHelper:           newCheckpointSyncHelper(),
		bucketTopologySvc:                 bucketTopologySvc,
		getHighSeqnoAndVBUuidFromTargetCh: make(chan bool, 1),
		periodicPushRequested:             make(chan bool, 1),
		variableVBMode:                    variableVBMode,
		backfillCollections:               make(map[uint16][]uint32),
	}

	for _, vbno := range ckmgr.getMyVBs() {
		ckmgr.backfillCollections[vbno] = make([]uint32, 0)
	}

	// So that unit test can override this and test it
	ckmgr.periodicMerger = ckmgr.periodicMergerImpl
	return ckmgr, nil
}

func (ckmgr *CheckpointManager) Attach(pipeline common.Pipeline) error {

	ckmgr.logger.Infof("Attach checkpoint manager\n")

	ckmgr.pipeline = pipeline
	if ckmgr.pipeline.Type() == common.BackfillPipeline {
		err := ckmgr.populateBackfillStartingTs(ckmgr.pipeline.Specification().GetBackfillSpec())
		if err != nil {
			return err
		}
		atomic.StoreUint32(&ckmgr.isBackfillPipeline, 1)
		ckmgr.bpVbMtx.Lock()
		ckmgr.bpVbHasCkpt = make(map[uint16]bool)
		ckmgr.bpVbMtx.Unlock()
	}
	pipelineReplSpec := ckmgr.pipeline.Specification().GetReplicationSpec()
	ckmgr.pipelineReinitHash = pipelineReplSpec.Settings.GetPipelineReinitHash()
	ckmgr.srcBucketName = pipelineReplSpec.SourceBucketName

	//populate the remote bucket information at the time of attaching
	err := ckmgr.populateRemoteBucketInfo(pipeline)
	if err != nil {
		return err
	}

	dcp_parts := pipeline.Sources()
	for _, dcp := range dcp_parts {
		dcp.RegisterComponentEventListener(common.StreamingStart, ckmgr)
		dcp.RegisterComponentEventListener(common.SnapshotMarkerReceived, ckmgr)

		// Checkpoint manager needs to listen to router's collections routing updates
		var found bool
		for _, connector := range dcp.Connectors() {
			if !strings.Contains(connector.Id(), base.ROUTER_NAME_PREFIX) {
				continue
			}
			found = true
			connector.RegisterComponentEventListener(common.BrokenRoutingUpdateEvent, ckmgr)
		}

		if !found {
			return fmt.Errorf("%v is unable to find router", dcp.Id())
		}
	}

	//register pipeline supervisor as ckmgr's error handler
	supervisor := ckmgr.pipeline.RuntimeContext().Service(base.PIPELINE_SUPERVISOR_SVC)
	if supervisor == nil {
		return errors.New("Pipeline supervisor has to exist")
	}
	err = ckmgr.RegisterComponentEventListener(common.ErrorEncountered, supervisor.(common.PipelineSupervisorSvc))
	if err != nil {
		return err
	}
	err = ckmgr.RegisterComponentEventListener(common.VBErrorEncountered, supervisor.(common.PipelineSupervisorSvc))
	if err != nil {
		return err
	}

	ckmgr.initialize()

	return nil
}

func (ckmgr *CheckpointManager) populateBackfillStartingTs(spec *metadata.BackfillReplicationSpec) error {
	var atLeastOneValid bool
	ckmgr.backfillTsMtx.Lock()
	defer ckmgr.backfillTsMtx.Unlock()

	vbTasks := spec.VBTasksMap
	vbTasks.GetLock().RLock()
	for vb, tasks := range vbTasks.VBTasksMap {
		if tasks == nil || tasks.Len() == 0 {
			continue
		}
		topTask, _, unlockFunc := tasks.GetRO(0)
		timestampsClone := topTask.GetTimestampsClone()
		unlockFunc()
		if timestampsClone != nil && timestampsClone.StartingTimestamp != nil {
			ckmgr.backfillStartingTs[vb] = timestampsClone.StartingTimestamp
			ckmgr.backfillEndingTs[vb] = timestampsClone.EndingTimestamp
			atLeastOneValid = true
		}
	}
	vbTasks.GetLock().RUnlock()
	if !atLeastOneValid {
		return fmt.Errorf("Unable to find at least one valid task for backfill spec %v", spec.Id)
	}

	return nil
}

// When rolling back, if rollback is requesting a seqno that is earlier than the backfill task, then
// allow that to happen by removing the startingTs
func (ckmgr *CheckpointManager) clearBackfillStartingTsIfNeeded(vbno uint16, rollbackSeqno uint64) {
	var needToRemove bool
	ckmgr.backfillTsMtx.RLock()
	startTs, exists := ckmgr.backfillStartingTs[vbno]
	if exists {
		if rollbackSeqno < startTs.Seqno {
			needToRemove = true
		}
	}
	ckmgr.backfillTsMtx.RUnlock()

	if needToRemove {
		ckmgr.backfillTsMtx.Lock()
		delete(ckmgr.backfillStartingTs, vbno)
		delete(ckmgr.backfillEndingTs, vbno)
		ckmgr.backfillTsMtx.Unlock()
	}

	if rollbackSeqno == 0 {
		// rollback to 0 means no backfill needs to be done anymore
		ckmgr.NotifyBackfillMgrRollbackTo0(vbno)
	}
}

func (ckmgr *CheckpointManager) populateRemoteBucketInfo(pipeline common.Pipeline) error {
	if ckmgr.unitTest {
		return nil
	}

	spec := pipeline.Specification().GetReplicationSpec()

	remote_bucket, err := service_def.NewRemoteBucketInfo(ckmgr.target_cluster_ref.Name(), spec.TargetBucketName, ckmgr.target_cluster_ref, ckmgr.remote_cluster_svc, ckmgr.logger, ckmgr.utils)
	if err != nil {
		return err
	}
	ckmgr.remote_bucket = remote_bucket

	ckmgr.checkCkptCapability()

	return nil
}

func (ckmgr *CheckpointManager) Start(settings metadata.ReplicationSettingsMap) error {
	err := ckmgr.initializeConfig(settings)
	if err != nil {
		return err
	}

	ckmgr.startRandomizedCheckpointingTicker()

	if err = ckmgr.setBackfillCollections(); err != nil {
		return err
	}

	//initialize connections
	ckmgr.wait_grp.Add(1)
	go ckmgr.initConnBg()

	//start checkpointing loop
	ckmgr.wait_grp.Add(1)
	go ckmgr.checkpointing()

	go ckmgr.periodicMerger()
	return nil
}

func (ckmgr *CheckpointManager) initConnBg() {
	initInBg := func() error {
		return ckmgr.InitConnections()
	}
	defer ckmgr.wait_grp.Done()
	execErr := base.ExecWithTimeout(initInBg, base.TimeoutRuntimeContextStart, ckmgr.Logger())
	if execErr != nil {
		ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, fmt.Errorf("Ckmgr %v initConnection error %v", ckmgr.pipeline.FullTopic(), execErr)))
	}
}

func (ckmgr *CheckpointManager) initializeConfig(settings metadata.ReplicationSettingsMap) error {
	if ckpt_interval_obj, ok := settings[CHECKPOINT_INTERVAL]; ok {
		ckpt_interval, ok := ckpt_interval_obj.(int)
		if !ok {
			return fmt.Errorf("%v %v %v has invalid type. value=%v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), CHECKPOINT_INTERVAL, ckpt_interval_obj)
		}
		ckmgr.setCheckpointInterval(ckpt_interval)
	} else {
		return fmt.Errorf("%v %v %v should be provided in settings", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), CHECKPOINT_INTERVAL)
	}

	devInjWaitSec, err := ckmgr.utils.GetIntSettingFromSettings(settings, metadata.DevCkptMgrForceGCWaitSec)
	if err == nil {
		ckmgr.xdcrDevInjectGcWaitSec = uint32(devInjWaitSec)
	}

	if _, exists := settings[parts.ForceCollectionDisableKey]; exists {
		atomic.StoreUint32(&ckmgr.collectionEnabledVar, 0)
	} else {
		atomic.StoreUint32(&ckmgr.collectionEnabledVar, 1)
	}
	return nil
}

func (ckmgr *CheckpointManager) collectionEnabled() bool {
	return atomic.LoadUint32(&ckmgr.collectionEnabledVar) > 0
}

func (ckmgr *CheckpointManager) startRandomizedCheckpointingTicker() {
	//randomize the starting point so that checkpoint managers for the same
	//replication on different nodes have different starting points
	starting_time := time.Duration(rand.Intn(5000))*time.Millisecond + ckmgr.getCheckpointInterval()
	ckmgr.logger.Infof("Checkpointing starts in %v sec", starting_time.Seconds())
	ckmgr.checkpoint_ticker_ch <- time.NewTicker(starting_time)
}

func (ckmgr *CheckpointManager) initialize() {
	ckmgr.initializeCkptsPerVB()

	ckmgr.composeUserAgent()

	ckmgr.cachedBrokenMap.lock.Lock()
	ckmgr.cachedBrokenMap.brokenMap = make(metadata.CollectionNamespaceMapping)
	ckmgr.cachedBrokenMap.brokenMapHistories = make(map[uint64]metadata.CollectionNamespaceMapping)
	ckmgr.cachedBrokenMap.lock.Unlock()

	ckmgr.getHighSeqnoAndVBUuidFromTargetCh <- true
}

func (ckmgr *CheckpointManager) initializeCkptsPerVB() {
	myVBsList := ckmgr.getMyVBs()
	myTgtVBsList := ckmgr.getMyTgtVBs()
	for _, vbno := range myVBsList {
		ckmgr.initializeCheckpointForVB(vbno, myTgtVBsList)
	}
}

func (ckmgr *CheckpointManager) initializeCheckpointForVB(vbno uint16, tgtVbList []uint16) {
	ckmgr.cur_ckpts[vbno] = &checkpointRecordWithLock{ckpt: &metadata.CheckpointRecord{}, lock: &sync.RWMutex{}}
	ckmgr.failoverlog_map[vbno] = &failoverlogWithLock{failoverlog: nil, lock: &sync.RWMutex{}}
	ckmgr.snapshot_history_map[vbno] = &snapshotHistoryWithLock{
		snapshot_history: make([]*snapshot, 0, base.MaxLengthSnapshotHistory),
	}

	if ckmgr.isVariableVBMode() {
		// Need to decorate the initial checkpoints' global checkpoint data structures
		ckptToEnhance := ckmgr.cur_ckpts[vbno].ckpt
		ckptToEnhance.GlobalTimestamp = make(metadata.GlobalTimestamp)
		ckptToEnhance.GlobalCounters = make(metadata.GlobalTargetCounters)

		for _, tgtVb := range tgtVbList {
			ckptToEnhance.InitializeVbForGlobalInfo(tgtVb)
		}
	}
}

var ckmgrIterationId uint32

// compose user agent string for HELO command
func (ckmgr *CheckpointManager) composeUserAgent() {
	spec := ckmgr.pipeline.Specification().GetReplicationSpec()
	ckmgr.SetUserAgent(base.ComposeHELOMsgKey(base.ComposeUserAgentWithBucketNames("CkptMgr", spec.SourceBucketName, spec.TargetBucketName)))
	ckmgr.bucketTopologySubscriberId = fmt.Sprintf("%v_%v_%v_%v", "ckptMgr", ckmgr.pipeline.Type().String(), ckmgr.pipeline.InstanceId(), base.GetIterationId(&ckmgrIterationId))
}

// If periodicMode is false, there should not be any error returned
// getHighSeqnoAndVBUuidFromTarget returns a map containg all VBs from the target where
// the key is a vbno, and the values are 2 numbers:
// 1. highSeqno for this VB
// 2. Vbuuid for this VB
func (ckmgr *CheckpointManager) getHighSeqnoAndVBUuidFromTarget(fin_ch chan bool, periodicMode bool) (map[uint16][]uint64, error) {
	ckmgr.WaitForInitConnDone()

	// get singular access to run to prevent concurrent calls into this function
	switch periodicMode {
	case true:
		// Periodic mode means that if a previous ckpt wasn't completed yet, don't queue up as this could be a case
		// where targets are slow... and a bunch of go-routines can build up into a long unordered queue
		select {
		case <-ckmgr.getHighSeqnoAndVBUuidFromTargetCh:
			defer func() {
				ckmgr.getHighSeqnoAndVBUuidFromTargetCh <- true
			}()
		default:
			return nil, errorPrevCheckpointInProgress
		}
	case false:
		// Non-Periodic mode means that this ckpt operation must be completed at all costs. Wait for access
		<-ckmgr.getHighSeqnoAndVBUuidFromTargetCh
		defer func() {
			ckmgr.getHighSeqnoAndVBUuidFromTargetCh <- true
		}()
	}

	serverClientStatsMap := make(map[string]base.StringStringMap)
	var serverClientStatsMapMtx sync.RWMutex
	var waitGrp sync.WaitGroup
	kvVbMap, err := ckmgr.TargetKvVbMap()
	if err != nil {
		return nil, err
	}
	for serverAddr, vbnos := range kvVbMap {
		waitGrp.Add(1)
		go ckmgr.getTargetKVStatsMapWithRetry(serverAddr, vbnos, fin_ch, serverClientStatsMap, &serverClientStatsMapMtx, &waitGrp)
	}
	waitGrp.Wait()

	// A map of vbucketID -> slice of 2 elements of 1)HighSeqNo and 2)VbUuid in that order
	highSeqnoAndVbUuidMap := make(base.HighSeqnoAndVbUuidMap)
	invalidVbNos := make([]uint16, 0)
	serverWarningsMap := make(map[string]map[uint16]string)
	for serverAddr, vbnos := range kvVbMap {
		if _, found := serverClientStatsMap[serverAddr]; !found {
			continue
		}
		invalidVbnosSubset, warnings := ckmgr.utils.ParseHighSeqnoAndVBUuidFromStats(vbnos, serverClientStatsMap[serverAddr], highSeqnoAndVbUuidMap)
		invalidVbNos = append(invalidVbNos, invalidVbnosSubset...)
		serverWarningsMap[serverAddr] = warnings
	}
	if len(invalidVbNos) > 0 {
		ckmgr.logger.Warnf("Can't find high seqno or vbuuid for vbnos=%v in stats map or are in invalid format. Target topology may have changed.\n", invalidVbNos)
		ckmgr.logger.Debugf("Warnings encountered during getHighSeqnoAndVBUuidFromTarget are: %v", serverWarningsMap)
	}
	diffMap := highSeqnoAndVbUuidMap.Diff(ckmgr.lastHighSeqnoVbUuidMap)
	if len(diffMap) > 0 {
		ckmgr.logger.Infof("highSeqnoAndVbUuidMap=%v\n", highSeqnoAndVbUuidMap)
	}
	ckmgr.lastHighSeqnoVbUuidMap = highSeqnoAndVbUuidMap
	return highSeqnoAndVbUuidMap, nil
}

// checkpointing cannot be done without high seqno and vbuuid from target
// if retrieval of such stats fails, retry
func (ckmgr *CheckpointManager) getTargetKVStatsMapWithRetry(serverAddr string, vbnos []uint16, fin_ch chan bool,
	serverClientStatsMap map[string]base.StringStringMap, serverClientStatsMapMtx *sync.RWMutex, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	var stats_map map[string]string

	statMapOp := func(param interface{}) (interface{}, error) {
		var err error
		ckmgr.KvMemClientsMtx.RLock()
		client, ok := ckmgr.KvMemClients[serverAddr]
		ckmgr.KvMemClientsMtx.RUnlock()
		if !ok {
			// memcached connection may have been closed in previous retries. create a new one
			client, err = ckmgr.GetNewMemcachedClient(serverAddr, false /*initializing*/)
			if err != nil {
				ckmgr.logger.Warnf("Retrieval of high seqno and vbuuid stats failed. serverAddr=%v, vbnos=%v\n", serverAddr, vbnos)
				return nil, err
			} else {
				ckmgr.KvMemClientsMtx.Lock()
				ckmgr.KvMemClients[serverAddr] = client
				ckmgr.KvMemClientsMtx.Unlock()
			}
		}

		stats_map, err = client.StatsMap(base.VBUCKET_SEQNO_STAT_NAME)
		if err != nil {
			ckmgr.logger.Warnf("Error getting vbucket-seqno stats for serverAddr=%v. vbnos=%v, err=%v", serverAddr, vbnos, err)
			clientCloseErr := client.Close()
			if clientCloseErr != nil {
				ckmgr.logger.Warnf("error from closing connection for %v is %v\n", serverAddr, err)
			}
			ckmgr.KvMemClientsMtx.Lock()
			delete(ckmgr.KvMemClients, serverAddr)
			ckmgr.KvMemClientsMtx.Unlock()
		}
		return nil, err
	}

	_, opErr := ckmgr.utils.ExponentialBackoffExecutorWithFinishSignal("StatsMapOnVBToSeqno", base.RemoteMcRetryWaitTime, base.MaxRemoteMcRetry,
		base.RemoteMcRetryFactor, statMapOp, nil, fin_ch)

	if opErr != nil {
		ckmgr.logger.Errorf("Retrieval of high seqno and vbuuid stats failed after %v retries. serverAddr=%v, vbnos=%v\n",
			base.MaxRemoteMcRetry, serverAddr, vbnos)
	} else {
		serverClientStatsMapMtx.Lock()
		serverClientStatsMap[serverAddr] = stats_map
		serverClientStatsMapMtx.Unlock()
	}
}

func (ckmgr *CheckpointManager) IsStopped() bool {
	select {
	case <-ckmgr.finish_ch:
		return true
	default:
		return false
	}
}

func (ckmgr *CheckpointManager) Stop() error {
	//send signal to checkpoiting routine to exit
	close(ckmgr.finish_ch)

	//close the connections
	ckmgr.CloseConnections()

	ckmgr.wait_grp.Wait()
	return nil
}
func (ckmgr *CheckpointManager) CheckpointBeforeStopWithWait(waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	ckmgr.CheckpointBeforeStop()
}
func (ckmgr *CheckpointManager) CheckpointBeforeStop() {
	if !ckmgr.isCheckpointAllowed() {
		ckmgr.logger.Errorf("Pipeline has not been started (or has been disabled) - checkpointing is skipped")
		return
	}

	var specExists bool
	switch ckmgr.pipeline.Type() {
	case common.MainPipeline:
		spec, _ := ckmgr.rep_spec_svc.ReplicationSpec(ckmgr.pipeline.Topic())
		specExists = spec != nil
	case common.BackfillPipeline:
		backfillSpec, _ := ckmgr.backfillReplSvc.BackfillReplSpec(ckmgr.pipeline.Topic())
		specExists = backfillSpec != nil
		if specExists {
			if !backfillSpec.VBTasksMap.ContainsAtLeastOneTaskForVBs(ckmgr.getMyVBs()) {
				// If Backfill spec does not have any more VB tasks, then it is considered finished
				// and the checkpoints are to be deleted, so do not checkpoint
				specExists = false
			}
		}
	default:
		panic(fmt.Sprintf("unhandled type %v", ckmgr.pipeline.Type().String()))
	}

	if !specExists {
		// do not perform checkpoint if spec has been deleted
		ckmgr.logger.Infof("Skipping checkpointing before stopping since replication spec has been deleted")
		return
	}

	var opDoneIdx int
	err := fmt.Errorf("InitErr")
	registerCkptOpTimeStop := ckmgr.utils.StartDiagStopwatch(fmt.Sprintf("ckmgr.checkpointAllowedHelper.registerCkptOp(false) - %v", ckmgr.pipeline.FullTopic()), base.DiagInternalThreshold)
	for err != nil {
		// We should only checkpoint before stop if no concurrent (periodic) checkpoint operation is in progress
		opDoneIdx, err = ckmgr.checkpointAllowedHelper.registerCkptOp(false, true)
		if err != nil {
			ckmgr.logger.Infof("Checkpointing before stopping skipped because %v", err)
			registerCkptOpTimeStop()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	registerCkptOpTimeStop()
	defer ckmgr.checkpointAllowedHelper.markTaskDone(opDoneIdx)

	ckmgr.logger.Infof("Starting checkpointing before stopping")

	timeout_timer := time.NewTimer(base.TimeoutCheckpointBeforeStop)
	defer timeout_timer.Stop()

	ret := make(chan bool, 1)
	close_ch := make(chan bool, 1)

	go func(done_ch chan bool) {
		ckmgr.PerformCkpt(close_ch)
		done_ch <- true
	}(ret)

	for {
		select {
		case <-ret:
			ckmgr.logger.Infof("Checkpointing completed")
			return
		case <-timeout_timer.C:
			close(close_ch)
			ckmgr.logger.Infof("Checkpointing timed out after %v.", base.TimeoutCheckpointBeforeStop)
			// do not wait for ckmgr.PerformCkpt to complete, which could get stuck if call to target never comes back
			return
		}
	}
}

// get the lis of source vbuckets that this pipleline instance responsible.
// In current deployment - ReplicationManager coexist with source node, it means
// the list of buckets on that source node
func (ckmgr *CheckpointManager) getMyVBs() []uint16 {
	return base.GetVbListFromKvVbMap(ckmgr.active_vbs)
}

func (ckmgr *CheckpointManager) getMyTgtVBs() []uint16 {
	targetKvVbMap, err := ckmgr.TargetKvVbMap()
	if err != nil {
		ckmgr.logger.Errorf("Unable to get targetKvVbMap: %v", err)
		return []uint16{}
	}
	return base.GetVbListFromKvVbMap(targetKvVbMap)
}

func (ckmgr *CheckpointManager) isVariableVBMode() bool {
	return ckmgr.variableVBMode
}

func (ckmgr *CheckpointManager) checkCkptCapability() {
	support_ckpt := false
	bk_capabilities := ckmgr.remote_bucket.Capabilities
	for _, c := range bk_capabilities {
		if c == XDCRCheckpointing {
			support_ckpt = true
			break
		}
	}
	ckmgr.support_ckpt = support_ckpt
	ckmgr.logger.Infof("Remote bucket %v supporting xdcrcheckpointing is %v\n", ckmgr.remote_bucket, ckmgr.support_ckpt)
}

func (ckmgr *CheckpointManager) getCheckpointInterval() time.Duration {
	return time.Duration(atomic.LoadUint32(&ckmgr.ckpt_interval)) * time.Second
}

func (ckmgr *CheckpointManager) setCheckpointInterval(ckpt_interval int) {
	atomic.StoreUint32(&ckmgr.ckpt_interval, uint32(ckpt_interval))
	ckmgr.logger.Infof("set ckpt_interval to %v s\n", ckpt_interval)
}

func (ckmgr *CheckpointManager) updateCurrentVBOpaque(vbno uint16, vbOpaque metadata.TargetVBOpaque) {
	obj, ok := ckmgr.cur_ckpts[vbno]
	if ok {
		obj.lock.Lock()
		defer obj.lock.Unlock()
		record := obj.ckpt

		record.Target_vb_opaque = vbOpaque
	} else {
		err := fmt.Errorf("%v %v Trying to update vbopaque on vb=%v which is not in MyVBList", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
		ckmgr.handleGeneralError(err)
	}
}

func (ckmgr *CheckpointManager) updateCurrentGlobalVBOpaque(vbno, targetVbno uint16, vbOpaque metadata.TargetVBOpaque) {
	obj, ok := ckmgr.cur_ckpts[vbno]
	if ok {
		obj.lock.Lock()
		defer obj.lock.Unlock()
		record := obj.ckpt
		record.GlobalTimestamp.SetTargetOpaque(targetVbno, vbOpaque)
	} else {
		err := fmt.Errorf("%v %v Trying to update vbopaque on vb=%v which is not in MyVBList", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
		ckmgr.handleGeneralError(err)
	}

}

// handle fatal error, raise error event to pipeline supervisor
func (ckmgr *CheckpointManager) handleGeneralError(err error) {
	ckmgr.logger.Error(err.Error())
	ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
}

func getDocsProcessedForReplication(topic string, vb_list []uint16, checkpoints_svc service_def.CheckpointsService,
	logger *log.CommonLogger) (uint64, error) {
	defer logger.Info("Done with GetDocsProcessedForReplication")
	logger.Infof("Start GetDocsProcessedForReplication for replication...")

	logger.Debugf("Getting checkpoint\n")
	ckptDocs, err := checkpoints_svc.CheckpointsDocs(topic, false /*needBrokenMappings*/)
	logger.Debugf("Done getting checkpoint\n")
	if err != nil {
		return 0, err
	}

	var docsProcessed uint64

	for vbno, ckptDoc := range ckptDocs {
		if base.IsVbInList(vbno, vb_list) {
			// if vbno is in vb_list, include its senqo in docs_processed computation

			// if checkpoint records exist, use the seqno in the first checkpoint record, which is the highest in all checkpoint records
			if ckptDoc != nil {
				ckptRecords := ckptDoc.GetCheckpointRecords()
				if ckptRecords != nil && len(ckptRecords) > 0 && ckptRecords[0] != nil {
					docsProcessed += ckptRecords[0].Seqno
				}
			}
		}
	}

	return docsProcessed, nil

}

func (ckmgr *CheckpointManager) CheckpointsExist(topic string) (bool, error) {
	vbDocs, err := ckmgr.checkpoints_svc.CheckpointsDocs(topic, false /*needBrokenMapping*/)
	if err != nil {
		return false, err
	}

	for _, doc := range vbDocs {
		if doc != nil && len(doc.Checkpoint_records) > 0 {
			return true, nil
		}
	}
	return false, nil
}

// Used by backfill pipeline only
func (ckmgr *CheckpointManager) DelSingleVBCheckpoint(topic string, vbno uint16, internalId string) error {
	ckmgr.bpVbMtx.RLock()
	hasCkpt := ckmgr.bpVbHasCkpt[vbno]
	ckmgr.bpVbMtx.RUnlock()

	if !hasCkpt {
		return nil
	}

	err := ckmgr.checkpoints_svc.DelCheckpointsDoc(topic, vbno, internalId)
	if err == service_def.MetadataNotFoundErr {
		err = nil
	}
	return err
}

// Used by backfill pipeline only
func (ckmgr *CheckpointManager) DisableRefCntGC(topic string) {
	ckmgr.checkpoints_svc.DisableRefCntDecrement(topic)
}

func (ckmgr *CheckpointManager) EnableRefCntGC(topic string) {
	ckmgr.checkpoints_svc.EnableRefCntDecrement(topic)
}

/**
 * As part of starting the pipeline, a go routine is launched with this as the entrypoint to figure out the
 * VB timestamp, (see types.go's VBTimestamp struct) for each of the vbucket that this node is responsible for.
 * It does the followings:
 * 1. Gets the checkpoint docs from the checkpoint service (metakv)
 * 2. Removes any invalid ckpt docs.
 * 3. Figures out all VB opaques from remote cluster, and updates the local checkpoint records (with lock) to reflect that.
 * The timestamps are to be consumed by dcp nozzle to determine the start point of dcp stream/replication via settings map (UpdateSettings).
 *
 * Unless the checkpoint manager is stopping, any non-nil returned error MUST raise a corresponding error event
 */
func (ckmgr *CheckpointManager) SetVBTimestamps(topic string) error {
	opDoneIdx, err := ckmgr.checkpointAllowedHelper.registerCkptOp(true, false)
	if err != nil {
		ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
		return err
	}

	startTime := time.Now()
	defer func() {
		ckmgr.logger.Infof("%v Done with SetVBTimestamps (took %v)", ckmgr.pipeline.InstanceId(), time.Since(startTime))
	}()
	defer func() {
		ckmgr.checkpointAllowedHelper.markTaskDone(opDoneIdx)
		ckmgr.checkpointAllowedHelper.setVBTimestampOpDone()
		ckmgr.checkpointAllowedHelper.setCheckpointAllowed()
	}()
	// Pipeline instances may overlap, So use instance ID instead
	ckmgr.logger.Infof("Set start seqnos for %v...", ckmgr.pipeline.InstanceId())

	listOfVbs := ckmgr.getMyVBs()

	// Get persisted checkpoints from metakv - each valid vbucket has a *metadata.CheckpointsDoc
	ckmgr.logger.Infof("Getting checkpoint\n")
	ckptDocs, err := ckmgr.checkpoints_svc.CheckpointsDocs(topic, true)
	if err != nil {
		ckmgr.logger.Errorf("Getting checkpoint had error %v", err)
		ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
		return err
	}
	ckmgr.loadCollectionMappingsFromCkptDocs(ckptDocs)
	ckmgr.logger.Infof("Found %v checkpoint documents for replication\n", len(ckptDocs))

	ckmgr.initBpMapIfNeeded(ckptDocs)

	vbWithStaleReplSpecID := make([]uint16, 0)
	vbWithStalePipelineReinitHash := make([]uint16, 0)
	specInternalId := ckmgr.pipeline.Specification().GetReplicationSpec().InternalId

	genSpec := ckmgr.pipeline.Specification()
	spec := genSpec.GetReplicationSpec()
	if spec == nil {
		ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, fmt.Errorf("Unable to get spec - nil pointer")))
		return base.ErrorNilPtr
	}

	// Figure out if certain checkpoints need to be removed to force a complete resync due to external factors
	oldSpecIds := make(map[string]bool)
	for vbno, ckptDoc := range ckptDocs {
		if ckmgr.IsStopped() {
			return ckptMgrStopped
		}
		if ckptDoc == nil {
			continue
		}
		if !base.IsVbInList(vbno, listOfVbs) {
			// Need to compose an idempotent gc function to clean up VB not owned by this node after some time
			// Keep the checkpoint around in case peer nodes will need it
			err = ckmgr.registerGCFunction(topic, vbno)
			if err != nil {
				ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
				return err
			}
		} else {
			if ckptDoc.SpecInternalId != specInternalId {
				oldSpecIds[ckptDoc.SpecInternalId] = true
				// if specInternalId does not match, replication spec has been deleted and recreated
				// the checkpoint doc is for the old replication spec and needs to be deleted
				// unlike the IsVbInList check above, it is critial for the checkpoint doc deletion to succeed here
				// restart pipeline if it fails
				err = ckmgr.checkpoints_svc.DelCheckpointsDoc(topic, vbno, ckmgr.pipeline.Specification().GetReplicationSpec().InternalId)
				if err != nil {
					ckmgr.logger.Errorf("Deleting checkpoint docs for vb %v had an err %v", vbno, err)
					ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
					return err
				}
				vbWithStaleReplSpecID = append(vbWithStaleReplSpecID, vbno)

			} else if len(ckptDoc.Checkpoint_records) > 0 &&
				ckptDoc.Checkpoint_records[0].PipelineReinitHash != ckmgr.pipelineReinitHash {
				// If PipelineReinitHash of the newest ckpt record does not match, that means the replication spec has been reinitialised and these
				// checkpoint records belong to a different pipeline "initialisation" (can be newer or older) - To be ignored regardless.
				err = ckmgr.checkpoints_svc.DelCheckpointsDoc(topic, vbno, ckmgr.pipeline.Specification().GetReplicationSpec().InternalId)
				if err != nil {
					ckmgr.logger.Errorf("Deleting checkpoint docs for vb %v had an err %v", vbno, err)
					ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
					return err
				}
				vbWithStalePipelineReinitHash = append(vbWithStalePipelineReinitHash, vbno)
			}
		}
	}

	if len(vbWithStaleReplSpecID) > 0 {
		ckmgr.logger.Infof("Checkpoint docs with internalId(s) %v while current spec has internalID %v - deleting outdated ckptdoc for vbs: %v",
			oldSpecIds, specInternalId, vbWithStaleReplSpecID)
		for _, deleted_vbno := range vbWithStaleReplSpecID {
			delete(ckptDocs, deleted_vbno)
		}
	}

	if len(vbWithStalePipelineReinitHash) > 0 {
		ckmgr.logger.Infof("deleting checkpoint docs for VBs whose newest ckpt record was not stamped with the current PipelineReinitHash (%v): %v",
			ckmgr.pipelineReinitHash, vbWithStalePipelineReinitHash)
		for _, deleted_vbno := range vbWithStalePipelineReinitHash {
			delete(ckptDocs, deleted_vbno)
		}
	}

	//divide the workload to several getter and run the getter parallelly
	workload := 100
	start_index := 0

	getter_wait_grp := &sync.WaitGroup{}
	err_ch := make(chan interface{}, len(listOfVbs))
	getter_id := 0

	// shuffles listOfVbs so as to load balance vbs for each outnozzle across startSeqnoGetters
	// this way, if a startSeqnoGetter executes faster than others, it won't overload one particular outnozzle
	base.ShuffleVbList(listOfVbs)
	var lowestLastSuccessfulSourceManifestId uint64
	var vbtsStartedWithNon0 uint32

	var preReplicateCacheCtx *globalCkptPrereplicateCacheCtx
	if ckmgr.isVariableVBMode() {
		preReplicateCacheCtx = ckmgr.newGlobalCkptPrereplicateCacheCtx(time.Duration(base.GlobalPreReplicateCacheExpireTimeSecs)*time.Second,
			time.Duration(base.GlobalPreReplicateCacheErrorExpireTimeSecs)*time.Second)
	}

	for {
		end_index := start_index + workload
		if end_index > len(listOfVbs) {
			end_index = len(listOfVbs)
		}
		vbs_for_getter := listOfVbs[start_index:end_index]
		getter_wait_grp.Add(1)
		go ckmgr.startSeqnoGetter(getter_id, vbs_for_getter, ckptDocs, getter_wait_grp, err_ch, &lowestLastSuccessfulSourceManifestId, &vbtsStartedWithNon0, preReplicateCacheCtx)

		start_index = end_index
		if start_index >= len(listOfVbs) {
			break
		}
		getter_id++
	}

	//wait for all the getter to be done, then gather result
	getter_wait_grp.Wait()
	close(err_ch)
	if len(err_ch) > 0 {
		for err_info_obj := range err_ch {
			err_info_arr := err_info_obj.([]interface{})
			vbno := err_info_arr[0].(uint16)
			err := err_info_arr[1].(error)
			ckmgr.handleVBError(vbno, err)
		}
	}

	// If vbts starts at 0, it means it's a clean slate and no backfill is needed
	if ckmgr.pipeline.Type() == common.MainPipeline && vbtsStartedWithNon0 > 0 {
		go ckmgr.getBackfillMgr().SetLastSuccessfulSourceManifestId(ckmgr.pipeline.Topic(), lowestLastSuccessfulSourceManifestId, false /*rollback*/, ckmgr.finish_ch)
	}

	ckmgr.logger.Infof("Done with setting starting seqno\n")
	return nil
}

// Newer instances of checkpoint manager will take over the same spec and callerID and thus replace older instances
func (ckmgr *CheckpointManager) registerGCFunction(topic string, vbno uint16) error {
	gcFunc := func() error {
		if ckmgr.IsStopped() {
			return fmt.Errorf("Ckptmgr instance stopped")
		}

		// If checkpoint is not allowed then it means SetVBTimestamp hasn't finished executing
		if !ckmgr.isCheckpointAllowed() {
			return fmt.Errorf("Ckptmgr checkpoint for %v %v hasn't started stopped", topic, vbno)
		}

		ckmgr.logger.Infof("garbage collecting old checkpoint for vb %v", vbno)
		stopFunc := ckmgr.utils.StartDiagStopwatch(fmt.Sprintf("ckmgr_%v_%v_gc", topic, vbno), base.DiagStopTheWorldAndMergeCkptThreshold)
		defer stopFunc()

		ckmgr.checkpointAllowedHelper.disableCkptAndWait()
		defer ckmgr.checkpointAllowedHelper.setCheckpointAllowed()

		err := ckmgr.checkpoints_svc.DelCheckpointsDoc(topic, vbno, ckmgr.pipeline.Specification().GetReplicationSpec().InternalId)
		if err != nil {
			ckmgr.logger.Errorf("GC DelCheckpointsDoc err %v", err)
			return err
		}
		return nil
	}
	err := ckmgr.bucketTopologySvc.RegisterGarbageCollect(topic, ckmgr.srcBucketName, vbno, topic, gcFunc, base.P2PVBRelatedGCInterval)
	if err != nil {
		ckmgr.logger.Errorf("Unable to register GC func for - %v due to %v", vbno, err)
	}
	return err
}

func (ckmgr *CheckpointManager) initBpMapIfNeeded(ckptDocs map[uint16]*metadata.CheckpointsDoc) {
	if atomic.LoadUint32(&ckmgr.isBackfillPipeline) == 0 || len(ckptDocs) == 0 {
		return
	}
	ckmgr.bpVbMtx.Lock()
	defer ckmgr.bpVbMtx.Unlock()
	for vbno, ckptDoc := range ckptDocs {
		if ckptDoc == nil {
			continue
		}
		ckmgr.bpVbHasCkpt[vbno] = true
	}
}

func (ckmgr *CheckpointManager) markBpCkptStored(vbno uint16) {
	if atomic.LoadUint32(&ckmgr.isBackfillPipeline) == 0 {
		return
	}
	ckmgr.bpVbMtx.RLock()
	marked := ckmgr.bpVbHasCkpt[vbno]
	ckmgr.bpVbMtx.RUnlock()

	if !marked {
		ckmgr.bpVbMtx.Lock()
		ckmgr.bpVbHasCkpt[vbno] = true
		ckmgr.bpVbMtx.Unlock()
	}
	return
}

func (ckmgr *CheckpointManager) loadCollectionMappingsFromCkptDocs(ckptDocs map[uint16]*metadata.CheckpointsDoc) {
	// When loading, just get the biggest brokenmap "fishing net" possible
	ckmgr.loadBrokenMappings(ckptDocs)
}

func (ckmgr *CheckpointManager) loadBrokenMappings(ckptDocs map[uint16]*metadata.CheckpointsDoc) {
	if ckmgr.isVariableVBMode() {
		ckmgr.loadBrokenMappingsGlobal(ckptDocs)
	} else {
		ckmgr.loadBrokenMappingsLegacy(ckptDocs)
	}
	go ckmgr.updateReplStatusBrokenMap()
}

func (ckmgr *CheckpointManager) loadBrokenMappingsLegacy(ckptDocs map[uint16]*metadata.CheckpointsDoc) {
	var highestManifestId uint64
	cacheLookupMap := make(map[uint64]metadata.CheckpointRecordsList)

	// First find the highest manifestID to load the brokenmaps
	for _, ckptDoc := range ckptDocs {
		if ckptDoc == nil {
			continue
		}
		for _, record := range ckptDoc.Checkpoint_records {
			if record == nil {
				continue
			}
			cacheLookupMap[record.TargetManifest] = append(cacheLookupMap[record.TargetManifest], record)
			if record.TargetManifest > highestManifestId {
				highestManifestId = record.TargetManifest
			}
		}
	}

	// Then only populate from highest manifestID
	ckmgr.loadBrokenMappingCommit(cacheLookupMap, highestManifestId)
}

// ckmgr.cachedBrokenMap.lock should be held
func (ckmgr *CheckpointManager) loadBrokenMappingCommit(cacheLookupMap map[uint64]metadata.CheckpointRecordsList, highestManifestId uint64) {
	ckmgr.cachedBrokenMap.lock.Lock()
	defer ckmgr.cachedBrokenMap.lock.Unlock()

	listToTraverse := cacheLookupMap[highestManifestId]
	for _, record := range listToTraverse {
		if record == nil {
			continue
		}

		brokenMappings := record.BrokenMappings()
		if brokenMappings == nil || len(brokenMappings) == 0 {
			continue
		}

		for _, brokenMapping := range brokenMappings {
			if ckmgr.cachedBrokenMap.brokenMap == nil {
				ckmgr.cachedBrokenMap.brokenMap = make(metadata.CollectionNamespaceMapping)
			}
			if brokenMapping == nil || len(*brokenMapping) == 0 {
				continue
			}
			ckmgr.cachedBrokenMap.brokenMap.Consolidate(*brokenMapping)
		}
	}
	ckmgr.cachedBrokenMap.correspondingTargetManifest = highestManifestId
}

func (ckmgr *CheckpointManager) loadBrokenMappingsGlobal(ckptDocs map[uint16]*metadata.CheckpointsDoc) {
	var highestManifestId uint64
	cacheLookupMap := make(map[uint64]metadata.CheckpointRecordsList)

	// First find the highest manifestID to load the brokenmaps
	for _, ckptDoc := range ckptDocs {
		if ckptDoc == nil {
			continue
		}
		for _, record := range ckptDoc.Checkpoint_records {
			if record == nil {
				continue
			}

			for _, oneGlobalTs := range record.GlobalTimestamp {
				cacheLookupMap[oneGlobalTs.TargetManifest] = append(cacheLookupMap[oneGlobalTs.TargetManifest], record)
				if oneGlobalTs.TargetManifest > highestManifestId {
					highestManifestId = oneGlobalTs.TargetManifest
				}
			}
		}
	}

	// Then only populate from highest manifestID
	ckmgr.loadBrokenMappingCommit(cacheLookupMap, highestManifestId)
}

func (ckmgr *CheckpointManager) setTimestampForVB(vbno uint16, ts *base.VBTimestamp) error {
	// Don't print globalManifetId for ease of reading
	ckmgr.logger.Infof("Set VBTimestamp: vb=%v, ts.Seqno=%v, ts.SourceManifestId=%v ts.TargetManifestId=%v\n",
		vbno, ts.Seqno, ts.ManifestIDs.SourceManifestId, ts.ManifestIDs.TargetManifestId)

	//set the start seqno on through_seqno_tracker_svc
	ckmgr.through_seqno_tracker_svc.SetStartSeqno(vbno, ts.Seqno, ts.ManifestIDs)

	settings := make(map[string]interface{})
	ts_map := make(map[uint16]*base.VBTimestamp)
	ts_map[vbno] = ts
	settings[base.VBTimestamps] = ts_map
	//notify the settings change
	ckmgr.pipeline.UpdateSettings(settings)

	return nil
}

// Restores broken mappings to multiple collection routers and other components
func (ckmgr *CheckpointManager) restoreBrokenMappingManifests(traditionalPair metadata.ManifestIdAndBrokenMapPair, globalPairs map[uint16]metadata.ManifestIdAndBrokenMapPair, isRollBack bool) {
	if traditionalPair.BrokenMap == nil && traditionalPair.ManifestId == 0 && len(globalPairs) == 0 {
		// Nothing valid was entered
		return
	}

	// Restore to routers
	settings := make(map[string]interface{})
	var argsList []interface{}
	if traditionalPair.BrokenMap != nil {
		// Traditional brokenMap to restore
		argsList = append(argsList, traditionalPair.BrokenMap)
		argsList = append(argsList, traditionalPair.ManifestId)
		argsList = append(argsList, isRollBack)
		settings[metadata.BrokenMappingsUpdateKey] = argsList
		ckmgr.pipeline.UpdateSettings(settings)
	} else {
		argsList = append(argsList, globalPairs)
		argsList = append(argsList, isRollBack)
		settings[metadata.GlobalBrokenMappingUpdateKey] = argsList
		ckmgr.pipeline.UpdateSettings(settings)
	}

	// Restore to brokenmap histories
	ckmgr.cachedBrokenMap.lock.Lock()
	defer ckmgr.cachedBrokenMap.lock.Unlock()
	if traditionalPair.BrokenMap != nil || traditionalPair.ManifestId != 0 {
		// Traditional restore
		ckmgr.restoreBrokenMapHistoryNoLock(traditionalPair)
	} else {
		// Global restore
		for _, onePair := range globalPairs {
			ckmgr.restoreBrokenMapHistoryNoLock(onePair)
		}
	}
}

// write lock must be held
func (ckptMgr *CheckpointManager) restoreBrokenMapHistoryNoLock(onePair metadata.ManifestIdAndBrokenMapPair) {
	if onePair.ManifestId == 0 {
		// Not a valid brokenmap to restore
		return
	}

	if ckptMgr.cachedBrokenMap.correspondingTargetManifest == onePair.ManifestId {
		// Consolidate
		if onePair.BrokenMap != nil && len(*onePair.BrokenMap) > 0 {
			ckptMgr.cachedBrokenMap.brokenMap.Consolidate(*onePair.BrokenMap)
		}
	} else if onePair.ManifestId < ckptMgr.cachedBrokenMap.correspondingTargetManifest {
		// Put in history
		brokenMapHistory, historyExists := ckptMgr.cachedBrokenMap.brokenMapHistories[onePair.ManifestId]
		if historyExists {
			if onePair.BrokenMap != nil && len(*onePair.BrokenMap) > 0 {
				brokenMapHistory.Consolidate(*onePair.BrokenMap)
			}
		} else {
			ckptMgr.cachedBrokenMap.correspondingTargetManifest = onePair.ManifestId
			if onePair.BrokenMap != nil && len(*onePair.BrokenMap) > 0 {
				ckptMgr.cachedBrokenMap.brokenMap = onePair.BrokenMap.Clone()
			}
		}
	} else {
		// Latest one we've seen so far
		ckptMgr.cachedBrokenMap.brokenMapHistories[ckptMgr.cachedBrokenMap.correspondingTargetManifest] = ckptMgr.cachedBrokenMap.brokenMap.Clone()
		ckptMgr.cachedBrokenMap.correspondingTargetManifest = onePair.ManifestId
		if onePair.BrokenMap != nil && len(*onePair.BrokenMap) > 0 {
			ckptMgr.cachedBrokenMap.brokenMap = onePair.BrokenMap.Clone()
		}
	}
}

func (ckmgr *CheckpointManager) startSeqnoGetter(getter_id int, listOfVbs []uint16, ckptDocs map[uint16]*metadata.CheckpointsDoc, waitGrp *sync.WaitGroup, err_ch chan interface{}, sharedLowestSuccessfulManifestId *uint64, vbtsStartedNon0 *uint32, preReplicateCacheCtx *globalCkptPrereplicateCacheCtx) {
	ckmgr.logger.Infof("StartSeqnoGetter %v is started to do _pre_prelicate for vbs %v\n", getter_id, listOfVbs)
	defer waitGrp.Done()

	for _, vbno := range listOfVbs {
		seqnoMax := ckmgr.getMaxSeqno(vbno)
		vbts, vbStats, lastSuccessfulBackfillMgrSrcManifestId, traditionalPair, globalPairs, err := ckmgr.getDataFromCkpts(vbno, ckptDocs[vbno], seqnoMax, preReplicateCacheCtx)
		if err != nil {
			err_info := []interface{}{vbno, err}
			err_ch <- err_info
			return
		}
		err = ckmgr.statsMgr.SetVBCountMetrics(vbno, vbStats)
		if err != nil {
			err = fmt.Errorf("%v %v setting vbStat %v for resulted with err %v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno, err)
			err_info := []interface{}{vbno, err}
			err_ch <- err_info
			return
		}
		ckmgr.restoreBrokenMappingManifests(traditionalPair, globalPairs, false /*isRollBack*/)
		err = ckmgr.setTimestampForVB(vbno, vbts)
		if err != nil {
			err_info := []interface{}{vbno, err}
			err_ch <- err_info
			return
		}
		if vbts.Seqno > 0 {
			atomic.CompareAndSwapUint32(vbtsStartedNon0, 0, 1)
		}
		if ckmgr.pipeline.Type() == common.MainPipeline {
			for true {
				curAtomicVal := atomic.LoadUint64(sharedLowestSuccessfulManifestId)
				if curAtomicVal == 0 && lastSuccessfulBackfillMgrSrcManifestId > 0 || lastSuccessfulBackfillMgrSrcManifestId < curAtomicVal {
					swapped := atomic.CompareAndSwapUint64(sharedLowestSuccessfulManifestId, curAtomicVal, lastSuccessfulBackfillMgrSrcManifestId)
					if swapped {
						break
					}
				} else {
					break
				}
			}
		}
	}
}

func (ckmgr *CheckpointManager) populateTargetVBOpaqueIfNeeded(vbno uint16, ctx *globalCkptPrereplicateCacheCtx) error {
	obj, ok := ckmgr.cur_ckpts[vbno]
	if !ok {
		err := fmt.Errorf("trying to check vbopaque on vb=%v which is not in MyVBList", vbno)
		ckmgr.handleGeneralError(err)
		return err
	}

	needToPopulate := true
	if obj != nil {
		obj.lock.RLock()
		if obj.ckpt != nil {
			if obj.ckpt.IsTraditional() {
				needToPopulate = obj.ckpt.Target_vb_opaque == nil
			} else {
				needToPopulate = obj.ckpt.GlobalTimestamp.GetTargetOpaque() == nil
			}
		}
		obj.lock.RUnlock()
	}

	if !needToPopulate {
		return nil
	}

	if ckmgr.isVariableVBMode() {
		globalTargetTimestamp := make(map[uint16]*service_def.RemoteVBReplicationStatus)
		for _, tgtVBno := range ckmgr.getMyTgtVBs() {
			globalTargetTimestamp[tgtVBno] = service_def.NewEmptyRemoteVBReplicationStatus(tgtVBno)
		}
		_, err := ckmgr.populateGlobalTargetVBOpaque(vbno, globalTargetTimestamp, ctx)
		if err != nil {
			ckmgr.logger.Errorf("populateGlobalTargetVBOpaque for vb %v had err %v", vbno, err)
			return err
		}
	} else {
		_, err := ckmgr.populateTargetVBOpaque(vbno, service_def.NewEmptyRemoteVBReplicationStatus(vbno))
		if err != nil {
			ckmgr.logger.Errorf("populateTargetVBOpaque for vb %v had err %v", vbno, err)
			return err
		}
	}
	return nil
}

func (ckmgr *CheckpointManager) populateTargetVBOpaque(vbno uint16, targetTimestamp *service_def.RemoteVBReplicationStatus) (bMatch bool, err error) {
	bMatch, current_remoteVBOpaque, err := ckmgr.capi_svc.PreReplicate(ckmgr.remote_bucket, targetTimestamp, ckmgr.support_ckpt)
	if err != nil {
		ckmgr.logger.Errorf("Pre_replicate failed for %v. err=%v\n", vbno, err)
		return
	}

	ckmgr.updateCurrentVBOpaque(vbno, current_remoteVBOpaque)
	if ckmgr.logger.GetLogLevel() >= log.LogLevelDebug {
		ckmgr.logger.Debugf("Remote vbucket %v has a new opaque %v, update\n", current_remoteVBOpaque, vbno)
		ckmgr.logger.Debugf("Done with _pre_prelicate call for %v for vbno=%v, bMatch=%v", targetTimestamp, vbno, bMatch)
	}
	return
}

func (ckmgr *CheckpointManager) populateGlobalTargetVBOpaque(vbno uint16, globalTs map[uint16]*service_def.RemoteVBReplicationStatus, ctx *globalCkptPrereplicateCacheCtx) (bool, error) {
	for targetVbno, oneTargetTs := range globalTs {
		bMatch, current_remoteVBOpaque, err := ctx.preReplicate(ckmgr.remote_bucket, oneTargetTs)
		if err != nil {
			ckmgr.logger.Errorf("Pre_replicate failed for %v. err=%v\n", vbno, err)
			return false, err
		} else if !bMatch {
			return false, nil
		}

		ckmgr.updateCurrentGlobalVBOpaque(vbno, targetVbno, current_remoteVBOpaque)
	}
	return true, nil
}

// Given a specific vbno and a list of checkpoints and a max possible seqno, return:
// valid VBTimestamp and corresponding VB-specific stats for statsMgr that was stored in the same ckpt doc
func (ckmgr *CheckpointManager) getDataFromCkpts(vbno uint16, ckptDoc *metadata.CheckpointsDoc, max_seqno uint64, ctx *globalCkptPrereplicateCacheCtx) (*base.VBTimestamp, base.VBCountMetric, uint64, metadata.ManifestIdAndBrokenMapPair, map[uint16]metadata.ManifestIdAndBrokenMapPair, error) {
	var agreedIndex int = -1

	ckptRecordsList := ckmgr.ckptRecordsWLock(ckptDoc, vbno)
	/**
	 * If we are fed a vbno and ckptDoc, then the above ckptRecordsWLock should feed us back a list
	 * that are not shared by anyone else so the locking here in this manner should be no problem.
	 * If we are not fed a legit ckptDoc, it means we're using an internal record, which should have
	 * only one record returned from the List, and locking a single element in this for loop should be fine.
	 */
	for index, ckptRecord := range ckptRecordsList {
		var targetTimestamp *service_def.RemoteVBReplicationStatus
		var globalTargetTimestamp map[uint16]*service_def.RemoteVBReplicationStatus
		if ckmgr.isVariableVBMode() {
			globalTargetTimestamp = make(map[uint16]*service_def.RemoteVBReplicationStatus)
		}

		ckptRecord.lock.RLock()
		ckpt_record := ckptRecord.ckpt

		if ckpt_record == nil {
			ckptRecord.lock.RUnlock()
			continue
		}

		if ckpt_record.Seqno <= max_seqno {
			err := ckmgr.extractTargetTimestamp(vbno, &targetTimestamp, globalTargetTimestamp, ckpt_record)
			if err != nil {
				ckptRecord.lock.RUnlock()
				continue
			}
		}
		sourceDCPManifestId := ckpt_record.SourceManifestForDCP
		sourceBackfillManifestId := ckpt_record.SourceManifestForBackfillMgr
		ckptRecord.lock.RUnlock()

		// Check to ensure that the manifests can be used
		spec := ckmgr.pipeline.Specification().GetReplicationSpec()
		if sourceDCPManifestId > 0 {
			_, err := ckmgr.collectionsManifestSvc.GetSpecificSourceManifest(spec, sourceDCPManifestId)
			if err != nil {
				ckmgr.logger.Debugf("Unable to find DCP source manifest ID %v, skipping a record...", sourceDCPManifestId)
				continue
			}
		}
		if sourceBackfillManifestId > 0 {
			_, err := ckmgr.collectionsManifestSvc.GetSpecificSourceManifest(spec, sourceBackfillManifestId)
			if err != nil {
				ckmgr.logger.Debugf("Unable to find BackfillMgr source manifest ID %v, skipping a record...", sourceBackfillManifestId)
				continue
			}
		}

		skip := ckmgr.filterBackfillCheckpointRecordsOnColIDs(vbno, ckpt_record)
		if skip {
			if ckmgr.logger.GetLogLevel() >= log.LogLevelDebug {
				ckmgr.backfillCollectionsMtx.RLock()
				currBackfillColIDs, ok := ckmgr.backfillCollections[vbno]
				ckmgr.logger.Debugf("Skipping checkpoint record for vbno=%v, ckmgrList=%v (ok=%v), ckptRecordList=%v",
					vbno, currBackfillColIDs, ok, ckpt_record.BackfillCollections)
				ckmgr.backfillCollectionsMtx.RUnlock()
			}

			continue
		}

		if targetTimestamp != nil || globalTargetTimestamp != nil {
			bMatch, err := ckmgr.validateTargetTimestampForResume(vbno, targetTimestamp, globalTargetTimestamp, ctx)
			if err != nil {
				return nil, nil, 0, metadata.ManifestIdAndBrokenMapPair{}, nil, err
			}

			if bMatch {
				if ckmgr.logger.GetLogLevel() >= log.LogLevelDebug {
					ckmgr.logger.Debugf("Remote bucket %v vbno %v agreed on the checkpoint above\n", ckmgr.remote_bucket, vbno)
				}
				if ckptDoc != nil {
					agreedIndex = index
				}
				goto POPULATE
			}
		} // end if targetTimestamp
	}
POPULATE:
	vbts, lastSuccessfulBackfillMgrSrcManifestId, traditionalPair, globalMappingPair, err := ckmgr.populateDataFromCkptDoc(ckptDoc, agreedIndex, vbno, ctx)
	if err != nil {
		return vbts, nil, 0, traditionalPair, nil, err
	}
	vbStatMap := NewVBStatsMapFromCkpt(ckptDoc, agreedIndex, vbno)
	return vbts, vbStatMap, lastSuccessfulBackfillMgrSrcManifestId, traditionalPair, globalMappingPair, nil
}

func (ckmgr *CheckpointManager) validateTargetTimestampForResume(vbno uint16, targetTimestamp *service_def.RemoteVBReplicationStatus, globalTs map[uint16]*service_def.RemoteVBReplicationStatus, ctx *globalCkptPrereplicateCacheCtx) (bool, error) {
	if ckmgr.isVariableVBMode() {
		return ckmgr.populateGlobalTargetVBOpaque(vbno, globalTs, ctx)
	} else {
		return ckmgr.populateTargetVBOpaque(vbno, targetTimestamp)
	}
}

func (ckmgr *CheckpointManager) extractTargetTimestamp(vbno uint16, targetTimestamp **service_def.RemoteVBReplicationStatus, globalTimestamp map[uint16]*service_def.RemoteVBReplicationStatus, ckpt_record *metadata.CheckpointRecord) error {
	if !ckmgr.isVariableVBMode() {
		*targetTimestamp = &service_def.RemoteVBReplicationStatus{
			VBOpaque: ckpt_record.Target_vb_opaque,
			VBSeqno:  ckpt_record.Target_Seqno,
			VBNo:     vbno,
		}
		if ckmgr.logger.GetLogLevel() >= log.LogLevelDebug {
			ckmgr.logger.Debugf("Remote bucket %v vbno %v checking to see if it could agreed on the checkpoint %v\n", ckmgr.remote_bucket, vbno, ckpt_record)
		}
	} else {
		for _, tgtVBno := range ckmgr.getMyTgtVBs() {
			if ckpt_record.GlobalTimestamp == nil || ckpt_record.GlobalTimestamp[tgtVBno] == nil {
				ckmgr.logger.Warnf("Global timestamp for target vb %v does not exist", tgtVBno)
				return base.ErrorNilPtr
			}
			globalTimestamp[tgtVBno] = &service_def.RemoteVBReplicationStatus{
				VBNo:     tgtVBno,
				VBSeqno:  ckpt_record.GlobalTimestamp[tgtVBno].Target_Seqno,
				VBOpaque: ckpt_record.GlobalTimestamp[tgtVBno].Target_vb_opaque,
			}
		}
	}
	return nil
}

/**
 * This method does one of two things:
 * 1. If the Checkpoint Docs coming in from metakv is non-nill, this method will convert those records into
 *    a new instantiated list of internal records with locks and return that list.
 * 2. If the Checkpoint docs coming in from metakv is empty, this method will return a list of internal checkpoints,
 *    (currently just one) that could be currently used by another go-routine.
 * The caller is expected to use locking minimally.
 */
func (ckmgr *CheckpointManager) ckptRecordsWLock(ckptDoc *metadata.CheckpointsDoc, vbno uint16) []*checkpointRecordWithLock {
	ret := []*checkpointRecordWithLock{}

	var useInternalDoc bool = true
	var invalidGtsCnt int

	if ckptDoc != nil {
		useInternalDoc = false // assume until proven otherwise
		// We're not going to use the checkpoint manager's internal record, so fake locks and return
		for _, aRecord := range ckptDoc.GetCheckpointRecords() {
			if ckmgr.isVariableVBMode() && !aRecord.IsValidGlobalTs(ckmgr.getMyTgtVBs()) {
				invalidGtsCnt++
				continue
			}
			newLock := &sync.RWMutex{}
			recordWLock := &checkpointRecordWithLock{
				ckpt: aRecord,
				lock: newLock,
			}
			ret = append(ret, recordWLock)
		}
		if len(ret) > 0 {
			ckmgr.logger.Infof("Found checkpoint doc for vb=%v\n", vbno)
		} else {
			useInternalDoc = true
			if invalidGtsCnt > 0 {
				ckmgr.logger.Warnf("Checkpoint doc for vb %v contained %v invalid global checkpoints", invalidGtsCnt)
			}
		}
	}

	if useInternalDoc {
		// First time XDCR checkpoint is executing, thus why ckptDoc is nil
		ret = append(ret, ckmgr.getCurrentCkptWLock(vbno))
	}

	return ret
}

func (ckmgr *CheckpointManager) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	ckmgr.logger.Debugf("Updating settings on checkpoint manager. settings=%v\n", settings)
	// Before interval, check for idle brokenMap update
	diffPair, bmUpdateOk := settings[metadata.CkptMgrBrokenmapIdleUpdateDiffPair].(*metadata.CollectionNamespaceMappingsDiffPair)
	srcManifestsDelta, bmUpdateOk2 := settings[metadata.CkptMgrBrokenmapIdleUpdateSrcManDelta].([]*metadata.CollectionsManifest)
	latestTgtManifestId, bmUpdateOk3 := settings[metadata.CkptMgrBrokenmapIdleUpdateLatestTgtManId].(uint64)
	if bmUpdateOk && bmUpdateOk2 && bmUpdateOk3 {
		ckmgr.CleanupInMemoryBrokenMap(diffPair, srcManifestsDelta, latestTgtManifestId)
		return nil
	}

	bypassCkpt, bypassExists := settings[metadata.CkptMgrBypassCkpt].(bool)
	if bypassExists && bypassCkpt {
		ckmgr.checkpointAllowedHelper.setCheckpointDisallowed()
	}

	devInjWaitSec, err := ckmgr.utils.GetIntSettingFromSettings(settings, metadata.DevCkptMgrForceGCWaitSec)
	if err == nil && devInjWaitSec >= 0 {
		atomic.StoreUint32(&ckmgr.xdcrDevInjectGcWaitSec, uint32(devInjWaitSec))
	}

	checkpoint_interval, err := ckmgr.utils.GetIntSettingFromSettings(settings, CHECKPOINT_INTERVAL)
	if err != nil {
		return err
	}

	if checkpoint_interval < 0 {
		// checkpoint_interval not specified. no op
		return nil
	}

	if int(ckmgr.getCheckpointInterval().Seconds()) == checkpoint_interval {
		// no op if no real updates
		ckmgr.logger.Infof("Skipped update of checkpoint interval since it already has the value of %v.\n", checkpoint_interval)
		return nil
	}

	// update checkpoint interval
	ckmgr.setCheckpointInterval(checkpoint_interval)
	ckmgr.startRandomizedCheckpointingTicker()

	return nil
}

func (ckmgr *CheckpointManager) retrieveCkptDoc(vbno uint16) (*metadata.CheckpointsDoc, error) {
	ckmgr.logger.Infof("retrieve chkpt doc for vb=%v\n", vbno)
	return ckmgr.checkpoints_svc.CheckpointsDoc(ckmgr.pipeline.FullTopic(), vbno)
}

// For global timestamp to resume, each source VB will have > 1 target VBs
// Each target VB (from the source VB's perspective) will have individual VB's manifests
// and brokenmaps to resume from
// At this point, though it's true that a set of source VBs may all share the same
// target VBs' "point in time" manifests and broken maps, we should ensure the code
// is conceptually and semantically correct
type globalCkptResumeData struct {
	srcVb              uint16
	globalTsManifests  map[uint16]uint64
	globalTsBrokenMaps map[uint16]metadata.CollectionNamespaceMapping
}

func (ckmgr *CheckpointManager) populateDataFromCkptDoc(ckptDoc *metadata.CheckpointsDoc, agreedIndex int, vbno uint16, ctx *globalCkptPrereplicateCacheCtx) (*base.VBTimestamp, uint64, metadata.ManifestIdAndBrokenMapPair, map[uint16]metadata.ManifestIdAndBrokenMapPair, error) {
	vbts := &base.VBTimestamp{Vbno: vbno}
	if ckmgr.isVariableVBMode() {
		vbts.ManifestIDs.GlobalTargetManifestIds = make(map[uint16]uint64)
	}
	var brokenMappingsToLoadInCkptObj metadata.ShaToCollectionNamespaceMap
	var targetManifestId uint64
	var backfillBypassAgreedIndex bool

	var traditionalPair metadata.ManifestIdAndBrokenMapPair
	var globalMappingsPair map[uint16]metadata.ManifestIdAndBrokenMapPair
	var lastSucccessfulBackfillMgrSrcManifestId uint64
	var globalTsAgreed metadata.GlobalTimestamp
	if agreedIndex > -1 {
		ckpt_records := ckptDoc.GetCheckpointRecords()
		if len(ckpt_records) < agreedIndex+1 {
			// should never happen
			ckmgr.logger.Warnf("could not find checkpoint record with agreedIndex=%v", agreedIndex)
		} else {
			ckpt_record := ckpt_records[agreedIndex]
			vbts.Vbuuid = ckpt_record.Failover_uuid
			vbts.Seqno = ckpt_record.Seqno
			vbts.SnapshotStart = ckpt_record.Dcp_snapshot_seqno
			vbts.SnapshotEnd = ckpt_record.Dcp_snapshot_end_seqno

			//For all stream requests the snapshot start seqno must be less than or equal
			//to the start seqno and the start seqno must be less than or equal to the snapshot end seqno.
			if vbts.SnapshotStart > vbts.Seqno {
				vbts.SnapshotStart = vbts.Seqno
			}
			if vbts.Seqno > vbts.SnapshotEnd {
				vbts.SnapshotEnd = vbts.Seqno
			}

			vbts.ManifestIDs.SourceManifestId = ckpt_record.SourceManifestForDCP

			lastSucccessfulBackfillMgrSrcManifestId = ckpt_record.SourceManifestForBackfillMgr

			// Meant to be loaded into ckmgr cache... whereas "brokenMapping" above is used as part of populate
			traditionalPair, globalMappingsPair = ckpt_record.BrokenMappingsAndManifestIds()

			if ckpt_record.IsTraditional() {
				vbts.ManifestIDs.TargetManifestId = traditionalPair.ManifestId
			} else {
				var maxManifest uint64
				for _, onePair := range globalMappingsPair {
					if onePair.ManifestId > maxManifest {
						maxManifest = onePair.ManifestId
					}
				}
				// Restore the biggest timestamp
				vbts.ManifestIDs.TargetManifestId = maxManifest

				// Also populate global vbts manifests
				for tgtVb, pair := range globalMappingsPair {
					vbts.ManifestIDs.GlobalTargetManifestIds[tgtVb] = pair.ManifestId
				}
			}

			brokenMappingsToLoadInCkptObj = ckpt_record.BrokenMappings()
			globalTsAgreed = ckpt_record.GlobalTimestamp
		}
	} else if ckmgr.pipeline.Type() == common.BackfillPipeline {
		// Check to see if there's a starting point to use instead of 0
		ckmgr.backfillTsMtx.RLock()
		startingTs, exists := ckmgr.backfillStartingTs[vbno]
		ckmgr.backfillTsMtx.RUnlock()
		if exists {
			backfillBypassAgreedIndex = true
			vbts.Vbuuid = startingTs.Vbuuid
			vbts.Seqno = startingTs.Seqno
			vbts.SnapshotStart = startingTs.SnapshotStart
			vbts.SnapshotEnd = startingTs.SnapshotEnd
			vbts.ManifestIDs = startingTs.ManifestIDs
			// Backfill pipelines don't need the granularity of using manifestID.GlobalTargetManifestIds given
			// that backfill pipelins are not meant to capture target manifest ID bumps (and to create backfills),
			// which is already main pipeline's job
		}
		// Usually, in the cases of Main pipelines, when there are no checkpoints (or no agreeable checkpoints)
		// the checkpoint manager will start at seqno 0, which is clean and guarantees no data loss
		// In the case of backfill pipelines, when there is no backfill checkpoint (or if there is no agreeable checkpoint)
		// the above will set the starting point accordingly
		// However, if there are backfill checkpoints and they all do not match because of
		//    checkpoint's startSeqno > backfillTaskEndSeqno
		// then the vbopaque will never be populated and will need to be populated to ensure that ckpt operations can take place
		err := ckmgr.populateTargetVBOpaqueIfNeeded(vbno, ctx)
		if err != nil {
			return vbts, lastSucccessfulBackfillMgrSrcManifestId, traditionalPair, globalMappingsPair, err
		}
	}

	//update current ckpt map
	obj, ok := ckmgr.cur_ckpts[vbno]
	if ok {
		obj.lock.Lock()
		defer obj.lock.Unlock()

		//populate the next ckpt (in cur_ckpts)'s information based on the previous checkpoint information if it exists
		if agreedIndex > -1 || backfillBypassAgreedIndex {
			obj.ckpt.Failover_uuid = vbts.Vbuuid
			obj.ckpt.Dcp_snapshot_seqno = vbts.SnapshotStart
			obj.ckpt.Dcp_snapshot_end_seqno = vbts.SnapshotEnd
			obj.ckpt.Seqno = vbts.Seqno
			if !ckmgr.isVariableVBMode() {
				if brokenMappingsToLoadInCkptObj != nil {
					obj.ckpt.LoadBrokenMapping(brokenMappingsToLoadInCkptObj)
				}
				obj.ckpt.TargetManifest = targetManifestId
			} else {
				obj.ckpt.GlobalTimestamp = globalTsAgreed
			}
		}
	} else {
		err := fmt.Errorf("%v %v Calling populateDataFromCkptDoc on vb=%v which is not in MyVBList", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
		ckmgr.handleGeneralError(err)
		return nil, 0, traditionalPair, globalMappingsPair, err
	}
	return vbts, lastSucccessfulBackfillMgrSrcManifestId, traditionalPair, globalMappingsPair, nil
}

func (ckmgr *CheckpointManager) checkpointing() {
	ckmgr.logger.Infof("checkpointing rountine started")

	defer ckmgr.logger.Infof("Exits checkpointing routine.")
	defer ckmgr.wait_grp.Done()

	ticker := <-ckmgr.checkpoint_ticker_ch
	defer ticker.Stop()

	children_fin_ch := make(chan bool)
	children_wait_grp := &sync.WaitGroup{}

	first := true

	for {
		select {
		case new_ticker := <-ckmgr.checkpoint_ticker_ch:
			ckmgr.logger.Infof("Received new ticker due to changes to checkpoint interval setting")

			// wait for existing, if any, performCkpt routine to finish before setting new ticker
			close(children_fin_ch)
			children_wait_grp.Wait()
			children_fin_ch = make(chan bool)

			ticker.Stop()
			ticker = new_ticker
			first = true
		case <-ckmgr.finish_ch:
			ckmgr.logger.Infof("Received finish signal")
			// wait for existing, if any, performCkpt routine to finish
			close(children_fin_ch)
			children_wait_grp.Wait()
			goto done
		case <-ticker.C:
			if !pipeline_utils.IsPipelineRunning(ckmgr.pipeline.State()) {
				//pipeline is no longer running, kill itself
				ckmgr.logger.Infof("Pipeline is no longer running, exit.")
				goto done
			}
			children_wait_grp.Add(1)
			go ckmgr.performCkpt(children_fin_ch, children_wait_grp)

			if first {
				// the beginning tick has a randomized time element to randomize start time
				// reset the second and onward ticks to remove the randomized time element
				ticker.Stop()
				ticker = time.NewTicker(ckmgr.getCheckpointInterval())
				first = false
			}
		}
	}

done:
	ticker.Stop()
}

func (ckmgr *CheckpointManager) PreCommitBrokenMapping() {
	// Before actually persisting the checkpoint, ensure that the current brokenmap is available before the checkpoints are populated
	var mapsToPreupsert []metadata.CollectionNamespaceMapping
	ckmgr.cachedBrokenMap.lock.RLock()
	mapsToPreupsert = append(mapsToPreupsert, ckmgr.cachedBrokenMap.brokenMap.Clone())
	for _, oldMap := range ckmgr.cachedBrokenMap.brokenMapHistories {
		mapsToPreupsert = append(mapsToPreupsert, oldMap)
	}
	ckmgr.cachedBrokenMap.lock.RUnlock()

	for _, preUpsertMap := range mapsToPreupsert {
		err := ckmgr.checkpoints_svc.PreUpsertBrokenMapping(ckmgr.pipeline.FullTopic(), ckmgr.pipeline.Specification().GetReplicationSpec().InternalId, &preUpsertMap)
		if err != nil {
			ckmgr.logger.Warnf("Unable to pre-persist brokenMapping %v: %v", preUpsertMap.String(), err)
		}
	}
}

func (ckmgr *CheckpointManager) CommitBrokenMappingUpdates() {
	// If router updated ckptmgr's brokenmap during the checkpointing process, this call will ensure that all the necessary brokenmaps are persisted
	err := ckmgr.checkpoints_svc.UpsertBrokenMapping(ckmgr.pipeline.FullTopic(), ckmgr.pipeline.Specification().GetReplicationSpec().InternalId)
	if err != nil {
		ckmgr.logger.Errorf("Unable to persist brokenMapping: %v", err)
	}
}

func (ckmgr *CheckpointManager) CommitGlobalTimestampDedupMaps() {
	// Let the checkpoint manager itself upsert
	// If router updated ckptmgr's brokenmap during the checkpointing process, this call will ensure that all the necessary brokenmaps are persisted
	err := ckmgr.checkpoints_svc.UpsertGlobalInfo(ckmgr.pipeline.FullTopic(), ckmgr.pipeline.Specification().GetReplicationSpec().InternalId)
	if err != nil {
		ckmgr.logger.Errorf("Unable to persist Global timestamp dedup map: %v", err)
	}
}

// public API. performs one checkpoint operation on request
func (ckmgr *CheckpointManager) PerformCkpt(fin_ch chan bool) {
	ckmgr.logger.Infof("Start one time checkpointing for replication")

	var through_seqno_map map[uint16]uint64
	var high_seqno_and_vbuuid_map map[uint16][]uint64
	var srcManifestIds map[uint16]uint64
	var tgtManifestIds map[uint16]uint64
	var err error

	defer func() {
		if err == nil {
			ckmgr.logger.Infof("Done one time checkpointing for replication")
		} else {
			ckmgr.logger.Errorf("Skipped one time checkpointing for replication failed with error %v", err)
		}
	}()

	// get through seqnos for all vbuckets in the pipeline
	through_seqno_map, srcManifestIds, tgtManifestIds = ckmgr.through_seqno_tracker_svc.GetThroughSeqnosAndManifestIds()
	// Let statsmgr know of the latest through seqno for it to prepare data for checkpointing
	vbSeqnoMap := base.VbSeqnoMapType(through_seqno_map)
	// Clone because statsMgr has collectors that handle things in background and we want to avoid potential modification
	ckmgr.statsMgr.HandleLatestThroughSeqnos(vbSeqnoMap.Clone())
	// get high seqno and vbuuid for all vbuckets in the pipeline
	high_seqno_and_vbuuid_map, err = ckmgr.getHighSeqnoAndVBUuidFromTarget(fin_ch, false)
	if err != nil {
		// Note that per design this call should try not to fail so if err returned above, something is wrong
		return
	}

	//divide the workload to several getter and run the getter parallelly
	vb_list := ckmgr.getMyVBs()
	base.RandomizeUint16List(vb_list)
	number_of_vbs := len(vb_list)

	number_of_workers := base.NumberOfWorkersForCheckpointing
	if number_of_workers > number_of_vbs {
		number_of_workers = number_of_vbs
	}
	load_distribution := base.BalanceLoad(number_of_workers, number_of_vbs)

	ckmgr.PreCommitBrokenMapping()

	worker_wait_grp := &sync.WaitGroup{}
	var total_committing_time int64
	for i := 0; i < number_of_workers; i++ {
		vb_list_worker := make([]uint16, 0)
		for index := load_distribution[i][0]; index < load_distribution[i][1]; index++ {
			vb_list_worker = append(vb_list_worker, vb_list[index])
		}

		worker_wait_grp.Add(1)
		// do not wait between vbuckets
		go ckmgr.performCkpt_internal(vb_list_worker, fin_ch, worker_wait_grp, 0, through_seqno_map, high_seqno_and_vbuuid_map, &total_committing_time, srcManifestIds, tgtManifestIds)
	}

	//wait for all the getter done, then gather result
	worker_wait_grp.Wait()
	ckmgr.CommitBrokenMappingUpdates()
	if ckmgr.isVariableVBMode() {
		ckmgr.CommitGlobalTimestampDedupMaps()
	}
	ckmgr.ClearBrokenmapHistory(tgtManifestIds)
	ckmgr.collectionsManifestSvc.PersistNeededManifests(ckmgr.pipeline.Specification().GetReplicationSpec())
	ckmgr.RaiseEvent(common.NewEvent(common.CheckpointDone, nil, ckmgr, nil, time.Duration(total_committing_time)*time.Nanosecond))
	ckmgr.checkpoints_svc.UpsertCheckpointsDone(ckmgr.pipeline.FullTopic(), ckmgr.pipeline.Specification().GetReplicationSpec().InternalId)

}

// local API. supports periodical checkpoint operations
func (ckmgr *CheckpointManager) performCkpt(fin_ch chan bool, wait_grp *sync.WaitGroup) {
	defer wait_grp.Done()

	if !ckmgr.isCheckpointAllowed() {
		ckmgr.logger.Errorf("Pipeline has not been started (or has been disabled) - checkpointing is skipped")
		return
	}

	opDoneIdx, err := ckmgr.checkpointAllowedHelper.registerCkptOp(false, false)
	if err != nil {
		ckmgr.logger.Errorf("(performCkpt) registerCkptOp failed - checkpointing is skipped")
		return
	}
	defer ckmgr.checkpointAllowedHelper.markTaskDone(opDoneIdx)

	ckmgr.logger.Infof("Start checkpointing for replication")
	defer func() {
		if err == nil {
			ckmgr.logger.Infof("Done checkpointing for replication")
		} else {
			ckmgr.logger.Errorf("Skipped checkpointing for replication due to %v", err)
		}
	}()
	// vbucketID -> ThroughSeqNumber
	var through_seqno_map map[uint16]uint64
	// vBucketID -> slice of 2 elements of 1)HighSeqNo and 2)VbUuid
	var high_seqno_and_vbuuid_map map[uint16][]uint64
	// map of vbucketID -> the seqno that corresponds to the first occurrence of xattribute
	var srcManifestIds map[uint16]uint64
	var tgtManifestIds map[uint16]uint64

	through_seqno_map, srcManifestIds, tgtManifestIds, high_seqno_and_vbuuid_map, err = ckmgr.gatherCkptData(fin_ch, through_seqno_map, srcManifestIds, tgtManifestIds, high_seqno_and_vbuuid_map)
	if err != nil {
		return
	}

	var total_committing_time int64

	if ckmgr.collectionEnabled() {
		ckmgr.PreCommitBrokenMapping()
	}

	var dummyWaitGrp sync.WaitGroup
	dummyWaitGrp.Add(1)
	ckmgr.performCkpt_internal(ckmgr.getMyVBs(), fin_ch, &dummyWaitGrp, ckmgr.getCheckpointInterval(), through_seqno_map, high_seqno_and_vbuuid_map, &total_committing_time, srcManifestIds, tgtManifestIds)
	dummyWaitGrp.Wait()

	if ckmgr.collectionEnabled() {
		ckmgr.CommitBrokenMappingUpdates()
		ckmgr.collectionsManifestSvc.PersistNeededManifests(ckmgr.pipeline.Specification().GetReplicationSpec())
	}

	if ckmgr.isVariableVBMode() {
		ckmgr.CommitGlobalTimestampDedupMaps()
	}

	if ckmgr.collectionEnabled() {
		ckmgr.ClearBrokenmapHistory(tgtManifestIds)
	}
	ckmgr.RaiseEvent(common.NewEvent(common.CheckpointDone, nil, ckmgr, nil, time.Duration(total_committing_time)*time.Nanosecond))
	ckmgr.checkpoints_svc.UpsertCheckpointsDone(ckmgr.pipeline.FullTopic(), ckmgr.pipeline.Specification().GetReplicationSpec().InternalId)

}

func (ckmgr *CheckpointManager) gatherCkptData(fin_ch chan bool, through_seqno_map map[uint16]uint64, srcManifestIds map[uint16]uint64, tgtManifestIds map[uint16]uint64, high_seqno_and_vbuuid_map map[uint16][]uint64) (map[uint16]uint64, map[uint16]uint64, map[uint16]uint64, map[uint16][]uint64, error) {
	// get through seqnos for all vbuckets in the pipeline
	if ckmgr.collectionEnabled() {
		through_seqno_map, srcManifestIds, tgtManifestIds = ckmgr.through_seqno_tracker_svc.GetThroughSeqnosAndManifestIds()
	} else {
		through_seqno_map = ckmgr.through_seqno_tracker_svc.GetThroughSeqnos()
	}
	// Let statsmgr know of the latest through seqno for it to prepare data for checkpointing
	vbSeqnoMap := base.VbSeqnoMapType(through_seqno_map)
	// Clone because statsMgr has collectors that handle things in background and we want to avoid potential races
	ckmgr.statsMgr.HandleLatestThroughSeqnos(vbSeqnoMap.Clone())
	// get high seqno and vbuuid for all vbuckets in the pipeline
	high_seqno_and_vbuuid_map, err := ckmgr.getHighSeqnoAndVBUuidFromTarget(fin_ch, true)
	return through_seqno_map, srcManifestIds, tgtManifestIds, high_seqno_and_vbuuid_map, err
}

func (ckmgr *CheckpointManager) isCheckpointAllowed() bool {
	ckmgr.checkpointAllowedHelper.mtx.RLock()
	defer ckmgr.checkpointAllowedHelper.mtx.RUnlock()

	return ckmgr.checkpointAllowedHelper.checkpointAllowed
}

func (ckmgr *CheckpointManager) performCkpt_internal(vb_list []uint16, fin_ch <-chan bool, wait_grp *sync.WaitGroup, time_to_wait time.Duration, through_seqno_map map[uint16]uint64, high_seqno_and_vbuuid_map map[uint16][]uint64, total_committing_time *int64, srcManifestIds, tgtManifestIds map[uint16]uint64) {

	defer wait_grp.Done()

	var interval_btwn_vb time.Duration
	if time_to_wait != 0 {
		interval_btwn_vb = time.Duration((time_to_wait.Seconds()/float64(len(vb_list)))*1000) * time.Millisecond
	}

	if !ckmgr.isCheckpointAllowed() {
		ckmgr.logger.Errorf("checkpointing is disallowed - checkpointing is skipped")
		return
	}

	opDoneIdx, err := ckmgr.checkpointAllowedHelper.registerCkptOp(false, false)
	if err != nil {
		ckmgr.logger.Errorf("(performCkpt_internal) registerCkptOp failed - checkpointing is skipped")
		return
	}
	defer ckmgr.checkpointAllowedHelper.markTaskDone(opDoneIdx)

	vbListAndSeqnosMap := base.VbSeqnoMapType(through_seqno_map)
	ckmgr.logger.Infof("Checkpointing for vb_list with throughSeqnos=%v", vbListAndSeqnosMap)
	err_map := make(map[uint16]error)

	var totalSize int
	for index, vb := range vb_list {
		select {
		case <-fin_ch:
			ckmgr.logger.Infof("Aborting checkpointing routine with vb list %v since received finish signal. index=%v\n", vb_list, index)
			return
		default:
			start_time_vb := time.Now()
			throughSeqno, ok := vbListAndSeqnosMap[vb]
			if !ok {
				ckmgr.logger.Warnf("skipping checkpointing for vb=%v as it's not found in throughSeqnos", vb)
				continue
			}
			size, err := ckmgr.doCheckpoint(vb, throughSeqno, high_seqno_and_vbuuid_map, srcManifestIds, tgtManifestIds)
			totalSize += size
			committing_time_vb := time.Since(start_time_vb)
			atomic.AddInt64(total_committing_time, committing_time_vb.Nanoseconds())
			if err != nil {
				ckmgr.handleVBError(vb, err)
				err_map[vb] = err
			}

			if interval_btwn_vb != 0 && index < len(vb_list)-1 {
				time.Sleep(interval_btwn_vb)
			}

		}
	}

	ckmgr.logger.Infof("Done checkpointing with vb list %v totalSize=%v\n", vb_list, totalSize)
	if len(err_map) > 0 {
		ckmgr.logger.Infof("Errors encountered in checkpointing: %v\n", err_map)
	}
}

/**
 * Update this check point record only if the versionNumber matches, and increments the internal version number
 * If update is successful, then it will persist the record in the checkpoint service.
 * Returns an error if version number mismatches while updating.
 * If an error occurs during persistence, ignore and override the error and logs an error message.
 * Returns size persisted
 */
func (ckptRecord *checkpointRecordWithLock) updateAndPersist(ckmgr *CheckpointManager, vbno uint16, versionNumberIn uint64, incomingRecord *metadata.CheckpointRecord) (int, error) {

	if ckptRecord == nil {
		return 0, errors.New("Nil ckptRecord")
	}

	ckptRecord.lock.Lock()
	defer ckptRecord.lock.Unlock()

	if ckptRecord.ckpt == nil {
		return 0, errors.New("Nil ckpt")
	}

	if versionNumberIn != ckptRecord.versionNum {
		return 0, ckptRecordMismatch
	}
	// Update the record
	ckptRecord.ckpt.Load(incomingRecord)
	ckptRecord.versionNum++

	// Persist the record
	persistSize, persistErr := ckmgr.persistCkptRecord(vbno, ckptRecord.ckpt)
	if persistErr == nil {
		ckmgr.raiseSuccessCkptForVbEvent(*ckptRecord.ckpt, vbno)
	} else {
		ckmgr.logger.Warnf("skipping checkpointing for vb=%v due to error persisting checkpoint record. err=%v", vbno, persistErr)
	}
	return persistSize, nil
}

/**
 * Given the current information, we want to establish a single checkpoint entry and persist it. We need to populate the followings:
 * 1. The Source vbucket through sequence number
 * 2. Failover UUID of source side (i.e. VBucket UUID)
 * 3. Target Vbucket high sequence number
 * 4. Target VB Opaque (i.e. VBUuid in recent releases) - we don't touch this
 * 5. Snapshot start sequence number (correlating to item 1)
 * 6. Snapshot end sequence number (correlating to item 1)
 * 7. Source and Target Manfiest IDs
 * 8. Most up-to-date Broken Collection Mapping
 */
func (ckmgr *CheckpointManager) doCheckpoint(vbno uint16, throughSeqno uint64, high_seqno_and_vbuuid_map map[uint16][]uint64, srcManifestIds, tgtManifestIds map[uint16]uint64) (size int, err error) {
	//locking the current ckpt record and notsent_seqno list for this vb, no update is allowed during the checkpointing
	ckmgr.logger.Debugf("Checkpointing for vb=%v with throughSeqno %v\n", vbno, throughSeqno)

	ckpt_obj, ok := ckmgr.cur_ckpts[vbno]
	if !ok {
		err = fmt.Errorf("Trying to doCheckpoint on vb=%v which is not in MyVBList", vbno)
		ckmgr.handleGeneralError(err)
		return
	}

	var curCkptTargetVBOpaque metadata.TargetVBOpaque
	// Get all necessary info before processing
	ckpt_obj.lock.RLock()
	// This is the version number that this iteration is working off of. Should the version number change
	// when updating the actual record, then this mean an concurrent operation has jumped ahead of us
	currRecordVersion := ckpt_obj.versionNum
	lastSeqno := ckpt_obj.ckpt.Seqno
	if ckmgr.isVariableVBMode() {
		curCkptTargetVBOpaque = ckpt_obj.ckpt.GlobalTimestamp.GetTargetOpaque()
	} else {
		curCkptTargetVBOpaque = ckpt_obj.ckpt.TargetVBTimestamp.Target_vb_opaque
	}
	ckpt_obj.lock.RUnlock()

	if curCkptTargetVBOpaque == nil || curCkptTargetVBOpaque.Value() == nil {
		err = fmt.Errorf("Target timestamp for vb %v is not populated properly", vbno)
		return
	}

	// Item 1:
	// Update the one and only check point record that checkpoint_manager keeps track of in its "cur_ckpts" with the through_seq_no
	// that was found from "GetThroughSeqno", which has already done the work to single out the number to represent the latest state
	// from the source side perspective
	ckmgr.logger.Debugf("Seqno number used for checkpointing for vb %v is %v\n", vbno, throughSeqno)

	if throughSeqno < lastSeqno {
		ckmgr.logger.Infof("Checkpoint seqno went backward, possibly due to rollback. vb=%v, old_seqno=%v, new_seqno=%v", vbno, lastSeqno, throughSeqno)
	}

	// Do this before through seqno check to ensure we catch failover as soon as we can
	remoteTimestamp, err := ckmgr.getRemoteTimestamp(vbno, high_seqno_and_vbuuid_map, curCkptTargetVBOpaque)
	if err != nil {
		if err == targetVbuuidChangedError {
			// vb uuid on target has changed. rollback may be needed. return error to get pipeline restarted
			return
		} else {
			ckmgr.logger.Warnf("skipping checkpointing for vb=%v. err=%v", vbno, err)
			err = nil
			return
		}
	}

	if throughSeqno == lastSeqno {
		ckmgr.logger.Debugf("No replication has happened in vb %v since replication start or last checkpoint. seqno=%v. Skip checkpointing\\n", vbno, lastSeqno)
		return
	}

	// Go through the local snapshot records repository and figure out which snapshot contains this latest sequence number
	// Item: 5 and 6
	ckRecordDcpSnapSeqno, ckRecordDcpSnapEndSeqno, snapErr := ckmgr.getSnapshotForSeqno(vbno, throughSeqno)
	if snapErr != nil {
		// if we cannot find snapshot for the checkpoint seqno, the checkpoint seqno is still usable in normal cases,
		// just that we may have to rollback to 0 when rollback is needed
		ckmgr.logger.Warnf("%v\n", snapErr.Error())
	}

	// if target vb opaque has not changed, persist checkpoint record
	// Item 2:
	// Get the failover_UUID here
	ckRecordFailoverUuid, failoverUuidErr := ckmgr.getFailoverUUIDForSeqno(vbno, ckRecordDcpSnapEndSeqno)
	if failoverUuidErr != nil {
		// if we cannot find uuid for the checkpoint seqno, the checkpoint seqno is unusable
		// skip checkpointing of this vb
		// return nil so that we can continue to checkpoint the next vb
		ckmgr.logger.Warnf("%v\n", failoverUuidErr.Error())
		err = nil
		return
	}

	// Get stats that need to persist
	vbCountMetrics, err := ckmgr.statsMgr.GetVBCountMetrics(vbno)
	if err != nil {
		ckmgr.logger.Warnf("unable to get metrics from stats manager. err=%v\n", err)
		return
	}

	// Collection-Related items
	remoteTimestamp, err = ckmgr.enhanceTgtTimestampWithCollection(vbno, tgtManifestIds, remoteTimestamp)
	if err != nil {
		return
	}

	srcManifestIdForDcp, srcManifestIdForBackfill := ckmgr.getSrcCollectionItems(vbno, srcManifestIds)

	var backfillCollections []uint32
	if ckmgr.pipeline.Type() == common.BackfillPipeline {
		backfillCollections = []uint32{}
		ckmgr.backfillCollectionsMtx.RLock()
		colIDs, ok := ckmgr.backfillCollections[vbno]
		if ok {
			backfillCollections = base.Uint32List(colIDs).Clone()
		} else {
			ckmgr.logger.Warnf("Backfill collection ids not found for vb=%v, using []", vbno)
		}
		ckmgr.backfillCollectionsMtx.RUnlock()
	}

	// Write-operation - feed the temporary variables and update them into the record and also write them to metakv
	newCkpt, err := metadata.NewCheckpointRecord(ckRecordFailoverUuid, throughSeqno, ckRecordDcpSnapSeqno, ckRecordDcpSnapEndSeqno,
		remoteTimestamp, vbCountMetrics, srcManifestIdForDcp, srcManifestIdForBackfill, uint64(time.Now().Unix()),
		ckmgr.pipelineReinitHash, backfillCollections)
	if err != nil {
		ckmgr.logger.Errorf("Unable to create a checkpoint record due to - %v", err)
		err = nil
		return
	}

	if ckmgr.isVariableVBMode() {
		errMap := ckmgr.checkpoints_svc.PreUpsertGlobalInfo(ckmgr.pipeline.FullTopic(),
			ckmgr.pipeline.Specification().GetReplicationSpec().InternalId, &newCkpt.GlobalTimestamp, &newCkpt.GlobalCounters)
		if len(errMap) > 0 {
			ckmgr.logger.Warnf("checkpointing for vb=%v had issues registering global info %v",
				vbno, errMap)
		}
	}

	size, err = ckpt_obj.updateAndPersist(ckmgr, vbno, currRecordVersion, newCkpt)

	if err != nil {
		// We weren't able to atomically update the checkpoint record. This checkpoint record is essentially lost
		// since someone jumped ahead of us
		ckmgr.logger.Warnf("skipping checkpointing for vb=%v version %v since a more recent checkpoint has been completed",
			vbno, currRecordVersion)
		err = nil
	} else {
		ckmgr.markBpCkptStored(vbno)
	}
	// no error fall through. return nil here to keep checkpointing/replication going.
	// if there is a need to stop checkpointing/replication, it needs to be done in a separate return statement
	return
}

func (ckmgr *CheckpointManager) enhanceTgtTimestampWithCollection(vbno uint16, tgtManifestIds map[uint16]uint64, remoteTimestamp metadata.TargetTimestamp) (metadata.TargetTimestamp, error) {
	if !ckmgr.collectionEnabled() {
		return remoteTimestamp, nil
	}

	if remoteTimestamp.IsTraditional() {
		brokenMapping, tgtManifestId := ckmgr.retrieveTargetManifestIdAndBrokenMap(tgtManifestIds, vbno)

		if len(brokenMapping) == 0 {
			// Do not decorate with an empty brokenmap
			return remoteTimestamp, nil
		}

		tgtVbTimestamp, ok := remoteTimestamp.GetValue().(*metadata.TargetVBTimestamp)
		if !ok {
			return nil, fmt.Errorf("expecting remoteTimestamp to be *metadata.TargetVBTimestamp, but got %T", remoteTimestamp)
		}

		err := tgtVbTimestamp.SetBrokenMappingAsPartOfCreation(brokenMapping)
		if err != nil {
			return nil, err
		}

		tgtVbTimestamp.TargetManifest = tgtManifestId
		return tgtVbTimestamp, nil
	} else {
		tgtVbTimestampMap, ok := remoteTimestamp.GetValue().(metadata.GlobalTimestamp)
		if !ok {
			return nil, fmt.Errorf("vb %v expecting metadata.GlobalTimestamp, got %T", vbno, remoteTimestamp.GetValue())
		}

		if tgtVbTimestampMap == nil {
			return nil, fmt.Errorf("vb %v empty target timestamps for global ckpt", vbno)
		}

		errMap := make(base.ErrorMap)
		for tgtVb, gts := range tgtVbTimestampMap {
			brokenMapping, tgtManifestId := ckmgr.retrieveTargetManifestIdAndBrokenMap(tgtManifestIds, tgtVb)
			// Do tgtManifest regardless of brokenmap
			gts.TargetManifest = tgtManifestId
			if len(brokenMapping) == 0 {
				// Do not decorate with an empty brokenmap
				continue
			}
			// Do brokenmap
			err := gts.SetBrokenMappingAsPartOfCreation(brokenMapping)
			if err != nil {
				errMap[fmt.Sprintf("src:tgt vb %v:%v", vbno, tgtVb)] = err
			}
		}
		if len(errMap) > 0 {
			return nil, fmt.Errorf(base.FlattenErrorMap(errMap))
		}
		globalTs := metadata.GlobalTimestamp(tgtVbTimestampMap)
		return &globalTs, nil
	}
}

func (ckmgr *CheckpointManager) retrieveTargetManifestIdAndBrokenMap(tgtManifestIds map[uint16]uint64, vbno uint16) (metadata.CollectionNamespaceMapping, uint64) {
	tgtManifestId, ok := tgtManifestIds[vbno]
	if !ok {
		ckmgr.logger.Debugf("unable to find Target manifest ID for vb %v", vbno)
	}

	var brokenMapping metadata.CollectionNamespaceMapping
	ckmgr.cachedBrokenMap.lock.RLock()
	defer ckmgr.cachedBrokenMap.lock.RUnlock()

	// In the case of tgtManifestId is 0, we will need to use the "latest cache" to ensure that there is a uniform
	// view of the broken map, such as when pipelines are paused and resumed, and no data is written due to a complete
	// mismatch of collections mapping, and there is no target written throughSeqno, and yet for source VBs, they
	// are all considered "through" because they are part of the brokenmapping
	var useLatestCached bool = true
	if tgtManifestId > 0 && tgtManifestId < ckmgr.cachedBrokenMap.correspondingTargetManifest {
		// This is a very rare condition where the target VB has not yet gone forward
		if ckmgr.cachedBrokenMap.brokenMapHistories[tgtManifestId] == nil {
			ckmgr.logger.Warnf("vbno %v unable to find older tgtmanifestId %v from history", vbno, tgtManifestId)
			tgtManifestId = ckmgr.cachedBrokenMap.correspondingTargetManifest
			brokenMapping = ckmgr.cachedBrokenMap.brokenMap.Clone()
		} else {
			useLatestCached = false
			brokenMapping = ckmgr.cachedBrokenMap.brokenMapHistories[tgtManifestId].Clone()
		}
	}

	if useLatestCached {
		// It is possible that Router has already raised IgnoreEvent() to throughseqtrakcer service
		// If that's the case, the manifestID returned by it will not be correct to pair with the broken map
		// If the brokenMapping is present, then ensure that the checkpoint will store the corresponding
		// manifestID so future backfill will be created
		brokenMapping = ckmgr.cachedBrokenMap.brokenMap.Clone()
		tgtManifestId = ckmgr.cachedBrokenMap.correspondingTargetManifest
	}
	return brokenMapping, tgtManifestId
}

func (ckmgr *CheckpointManager) getSrcCollectionItems(vbno uint16, srcManifestIds map[uint16]uint64) (uint64, uint64) {
	if !ckmgr.collectionEnabled() {
		return 0, 0
	}

	srcManifestId, ok := srcManifestIds[vbno]
	if !ok {
		ckmgr.logger.Warnf("Unable to find Source manifest ID for vb %v", vbno)
	}

	backfillMgr := ckmgr.getBackfillMgr()
	lastBackfillSuccessfulId, err := backfillMgr.GetLastSuccessfulSourceManifestId(ckmgr.pipeline.Topic())
	if err != nil {
		ckmgr.logger.Warnf("Unable to get last successful source manifest Id - %v", err)
	}
	return srcManifestId, lastBackfillSuccessfulId
}

func (ckmgr *CheckpointManager) getThroughSeqno(vbno uint16, through_seqno_map map[uint16]uint64) (uint64, error) {
	var through_seqno uint64
	var ok bool
	through_seqno, ok = through_seqno_map[vbno]
	if !ok {
		return 0, fmt.Errorf("%v %v cannot find through seqno for vb %v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
	}
	return through_seqno, nil
}

func (ckmgr *CheckpointManager) getRemoteTimestamp(vbno uint16, high_seqno_and_vbuuid_map map[uint16][]uint64, curCkptTargetVBOpaque metadata.TargetVBOpaque) (metadata.TargetTimestamp, error) {
	if ckmgr.isVariableVBMode() {
		return ckmgr.getRemoteGlobalTimestamp(high_seqno_and_vbuuid_map, curCkptTargetVBOpaque)
	} else {
		return ckmgr.getRemoteSeqno(vbno, high_seqno_and_vbuuid_map, curCkptTargetVBOpaque)
	}
}

func (ckmgr *CheckpointManager) getRemoteSeqno(vbno uint16, high_seqno_and_vbuuid_map map[uint16][]uint64, curCkptTargetVBOpaque metadata.TargetVBOpaque) (metadata.TargetTimestamp, error) {
	high_seqno_and_vbuuid, ok := high_seqno_and_vbuuid_map[vbno]
	if !ok {
		return nil, fmt.Errorf("%v %v cannot find high seqno and vbuuid for vb %v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
	}

	remote_seqno := high_seqno_and_vbuuid[0]
	vbuuid := high_seqno_and_vbuuid[1]
	targetVBOpaque := &metadata.TargetVBUuid{vbuuid}
	if !curCkptTargetVBOpaque.IsSame(targetVBOpaque) {
		ckmgr.logger.Errorf("target vbuuid has changed for vb=%v. old=%v, new=%v", vbno, curCkptTargetVBOpaque, targetVBOpaque)
		return nil, targetVbuuidChangedError
	}
	remoteSeqno := &metadata.TargetVBTimestamp{
		Target_Seqno:     remote_seqno,
		Target_vb_opaque: targetVBOpaque,
	}
	return remoteSeqno, nil
}

func (ckmgr *CheckpointManager) raiseSuccessCkptForVbEvent(ckpt_record metadata.CheckpointRecord, vbno uint16) {
	//notify statisticsManager
	ckmgr.RaiseEvent(common.NewEvent(common.CheckpointDoneForVB, ckpt_record, ckmgr, nil, vbno))
}

func (ckmgr *CheckpointManager) persistCkptRecord(vbno uint16, ckpt_record *metadata.CheckpointRecord) (int, error) {
	ckmgr.logger.Debugf("Persist vb=%v ckpt_record=%v\n", vbno, ckpt_record)
	return ckmgr.checkpoints_svc.UpsertCheckpoints(ckmgr.pipeline.FullTopic(), ckmgr.pipeline.Specification().GetReplicationSpec().InternalId, vbno, ckpt_record)
}

func (*CheckpointManager) ListenerPipelineType() common.ListenerPipelineType {
	return common.ListenerNotShared
}

func (ckmgr *CheckpointManager) OnEvent(event *common.Event) {
	switch event.EventType {
	case common.StreamingStart:
		upr_event, ok := event.Data.(*mcc.UprEvent)
		if ok {
			flog := upr_event.FailoverLog
			vbno := upr_event.VBucket

			failoverlog_obj, ok1 := ckmgr.failoverlog_map[vbno]
			if ok1 {
				failoverlog_obj.lock.Lock()
				defer failoverlog_obj.lock.Unlock()

				failoverlog_obj.failoverlog = flog

				ckmgr.logger.Debugf("Got failover log for vb=%v\n", vbno)
			} else {
				err := fmt.Errorf("Received failoverlog on an unknown vb=%v\n", vbno)
				ckmgr.handleGeneralError(err)
			}
		}
	case common.SnapshotMarkerReceived:
		upr_event, ok := event.Data.(*mcc.UprEvent)
		ckmgr.logger.Debugf("Received snapshot vb=%v, start=%v, end=%v\n", upr_event.VBucket, upr_event.SnapstartSeq, upr_event.SnapendSeq)
		if ok {
			vbno := upr_event.VBucket

			snapshot_history_obj, ok1 := ckmgr.snapshot_history_map[vbno]
			if ok1 {
				// add current snapshot to snapshot history
				snapshot_history_obj.lock.Lock()
				defer snapshot_history_obj.lock.Unlock()
				cur_snapshot := &snapshot{start_seqno: upr_event.SnapstartSeq, end_seqno: upr_event.SnapendSeq}
				if len(snapshot_history_obj.snapshot_history) < base.MaxLengthSnapshotHistory {
					snapshot_history_obj.snapshot_history = append(snapshot_history_obj.snapshot_history, cur_snapshot)
				} else {
					// rotation is needed
					for i := 0; i < base.MaxLengthSnapshotHistory-1; i++ {
						snapshot_history_obj.snapshot_history[i] = snapshot_history_obj.snapshot_history[i+1]
					}
					snapshot_history_obj.snapshot_history[base.MaxLengthSnapshotHistory-1] = cur_snapshot
				}
			} else {
				err := fmt.Errorf("%v %v Received snapshot marker on an unknown vb=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
				ckmgr.handleGeneralError(err)
			}
		}
	case common.BrokenRoutingUpdateEvent:
		routingInfo, ok := event.Data.(parts.CollectionsRoutingInfo)
		if !ok {
			err := fmt.Errorf("%v %v Unable to handle BrokenRoutingUpdateEvent. Must not continue to perform checkpointing", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
			ckmgr.handleGeneralError(err)
			return
		}
		syncCh, ok := event.OtherInfos.(chan error)
		if !ok {
			err := fmt.Errorf("Invalid routing info response channel %v type", reflect.TypeOf(event.OtherInfos))
			ckmgr.handleGeneralError(err)
			return
		}
		// Each collection router will raise an update (from its perspective) event whenever:
		// 1. A new mapping is marked broken OR
		// 2. A new manifest has been used and some broken mappings are fixed
		//
		// It is not guaranteed that all the collections routers will have the same broken map view due to VB distribution
		// For example, if a single mutation goes through router A routes a mutation c1 -> c1, and that's the only broken mapping
		// All other mutations go through router B, C successfully (i.e. c2 -> c2 mapping)
		// Router A would raise brokenmap of c1 -> c1
		// While router B and C would raise a nil broken map, because none of it actually did any work for (c1 -> c1) mapping
		//
		// So, this is the consolidation point. It is always safer to soak up all the updates from all the routers into one
		// comprehensive brokenmap view (here in checkpoint manager)
		// Then figure out if there are any fixed mappings, that they are then subsequently removed from the consolidated view
		ckmgr.cachedBrokenMap.lock.RLock()
		if routingInfo.TargetManifestId < ckmgr.cachedBrokenMap.correspondingTargetManifest ||
			(routingInfo.TargetManifestId == ckmgr.cachedBrokenMap.correspondingTargetManifest &&
				ckmgr.cachedBrokenMap.brokenMap.IsSame(routingInfo.BrokenMap)) {
			// Don't accept "old" routing updates
			ckmgr.cachedBrokenMap.lock.RUnlock()
		} else {
			ckmgr.cachedBrokenMap.lock.RUnlock()
			ckmgr.cachedBrokenMap.lock.Lock()
			// Updates will occur and need to log the differences on UI
			olderMap := ckmgr.cachedBrokenMap.brokenMap.Clone()
			// No need to re-check because OnEvent() is serialized
			// First, absorb any new broken mappings (case 1 and 2)
			ckmgr.cachedBrokenMap.brokenMap.Consolidate(routingInfo.BrokenMap)

			// Consider three collection routers: A, B, and C.
			// - A and B initially had broken mappings: c1 -> c1 and c2 -> c2.
			// - C had a separate broken mapping: c3 -> c3.
			// - When c1, c2, and c3 are created on the target, the target manifest ID increments (e.g., to 4).
			// - Router A resolves its broken mappings (c1 -> c1, c2 -> c2) and triggers an event for the checkpoint manager and we update the cachedBrokenMap.brokenMap to c3->c3
			//   and cachedBrokenMap.correspondingTargetManifest to 4 here.
			// - Router B, upon resolving the same mappings (c1 -> c1, c2 -> c2), also triggers an event, but this becomes a no-op.
			// - When Router C resolves its broken mapping (c3 -> c3), it triggers an event for checkpoint manager
			//   and updates the cachedBrokenMap.brokenMap to []
			// - At this point, `ckmgr.cachedBrokenMap` is cleared, and `correspondingTargetManifest` remains 4.
			// - Note that we bump the ckmgr.cachedBrokenMap.correspondingTargetManifest only when the incoming TargetManifestID is greater
			// - If routingInfo.TargetManifestId == ckmgr.cachedBrokenMap.correspondingTargetManifest, then we just clear the ckmgr's broken map.
			//	 Note that we do not update the cachedBrokenMap.brokenMapHistories because it should not have entry corresponding to the `current` manifest
			if routingInfo.TargetManifestId > ckmgr.cachedBrokenMap.correspondingTargetManifest {
				oldManifestIdToRecord := ckmgr.cachedBrokenMap.correspondingTargetManifest
				// Newer version means potentially backfill maps that were fixed (case 2)
				// Remove those from the broken mapppings
				ckmgr.cachedBrokenMap.brokenMap = ckmgr.cachedBrokenMap.brokenMap.Delete(routingInfo.BackfillMap)
				ckmgr.cachedBrokenMap.correspondingTargetManifest = routingInfo.TargetManifestId
				// As part of bumping, save the pre-bumped manifest - brokenMap pair
				ckmgr.cachedBrokenMap.brokenMapHistories[oldManifestIdToRecord] = olderMap
			} else if routingInfo.TargetManifestId == ckmgr.cachedBrokenMap.correspondingTargetManifest {
				// remove the broken mappings
				ckmgr.cachedBrokenMap.brokenMap = ckmgr.cachedBrokenMap.brokenMap.Delete(routingInfo.BackfillMap)
			}

			newerMap := ckmgr.cachedBrokenMap.brokenMap.Clone()
			ckmgr.cachedBrokenMap.lock.Unlock()
			ckmgr.wait_grp.Add(2)
			// Can do this in the background - manifest updates usually don't happen too often
			// to warrant a serializer
			go ckmgr.logBrokenMapUpdatesToUI(olderMap, newerMap)
			// Just in case checkpoint operations may use this, ensure that this belongs
			go ckmgr.preUpsertBrokenMapTask(newerMap)
			go ckmgr.updateReplStatusBrokenMap()
		}
		syncCh <- nil
		close(syncCh)
	}
}

func (ckmgr *CheckpointManager) preUpsertBrokenMapTask(newerMap metadata.CollectionNamespaceMapping) {
	defer ckmgr.wait_grp.Done()
	ckmgr.checkpoints_svc.PreUpsertBrokenMapping(ckmgr.pipeline.FullTopic(), ckmgr.pipeline.Specification().GetReplicationSpec().InternalId, &newerMap)
}

func (ckmgr *CheckpointManager) logBrokenMapUpdatesToUI(olderMap, newerMap metadata.CollectionNamespaceMapping) {
	defer ckmgr.wait_grp.Done()
	var buffer bytes.Buffer
	var needToRaiseUI bool

	added, removed := olderMap.Diff(newerMap)

	if ckmgr.pipeline.Type() != common.MainPipeline {
		// BackfillPipeline should not log anything, since any of these events coming from backfill pipeline means they
		// are from xmem race conditions
		return
	}

	var migrationMode bool
	if len(added) > 0 || len(removed) > 0 {
		spec := ckmgr.pipeline.Specification().GetReplicationSpec()
		modes := spec.Settings.GetCollectionModes()
		migrationMode = modes.IsMigrationOn()
		ref, err := ckmgr.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, false /*refresh*/)
		if err != nil || ref == nil {
			ckmgr.logger.Warnf("Unable to write to UI log service the update.\nNewly broken: %v Repaired: %v\n",
				added, removed)
			return
		}
		buffer.WriteString(fmt.Sprintf("XDCR SourceBucket %v to Target Cluster %v Bucket %v\n",
			spec.SourceBucketName, ref.Name(), spec.TargetBucketName))
		ckmgr.backfillUILogMtx.Lock()
		if len(added) > 0 && ckmgr.backfillLastAdded.IsSame(added) {
			added = nil
		} else {
			ckmgr.backfillLastAdded = added
			if ckmgr.backfillLastRemoved != nil && added != nil {
				ckmgr.backfillLastRemoved = ckmgr.backfillLastRemoved.Delete(added)
			}
		}
		if len(removed) > 0 && ckmgr.backfillLastRemoved.IsSame(removed) {
			removed = nil
		} else {
			ckmgr.backfillLastRemoved = removed
			if ckmgr.backfillLastAdded != nil && removed != nil {
				ckmgr.backfillLastAdded = ckmgr.backfillLastAdded.Delete(removed)
			}
		}
		ckmgr.backfillUILogMtx.Unlock()
		needToRaiseUI = len(added) > 0 || len(removed) > 0
	}

	if len(added) > 0 {
		buffer.WriteString(base.BrokenMappingUIString)
		if migrationMode {
			buffer.WriteString(added.MigrateString())
		} else {
			buffer.WriteString(added.String())
		}
	}

	if len(removed) > 0 {
		buffer.WriteString("Following collection mappings are now repaired and replicating:\n")
		if migrationMode {
			buffer.WriteString(removed.MigrateString())
		} else {
			buffer.WriteString(removed.String())
		}
	}

	if needToRaiseUI {
		ckmgr.uiLogSvc.Write(buffer.String())
	}
}

func (ckmgr *CheckpointManager) getFailoverUUIDForSeqno(vbno uint16, snapEndSeqNo uint64) (uint64, error) {
	// We want to find a UUID that knows about snapEndSeqNo
	failoverlog_obj, ok1 := ckmgr.failoverlog_map[vbno]
	if ok1 {
		failoverlog_obj.lock.RLock()
		defer failoverlog_obj.lock.RUnlock()

		flog := failoverlog_obj.failoverlog
		if flog != nil {
			for _, entry := range *flog {
				failover_uuid := entry[0]
				starting_seqno := entry[1]
				if snapEndSeqNo >= starting_seqno {
					return failover_uuid, nil
				}
			}
		}
	} else {
		err := fmt.Errorf("%v Calling getFailoverUUIDForSeqno on an unknown vb=%v\n", ckmgr.pipeline.FullTopic(), vbno)
		ckmgr.handleGeneralError(err)
		return 0, err
	}
	return 0, fmt.Errorf("%v Failed to find vbuuid for vb=%v, snapEndSeqNo=%v\n", ckmgr.pipeline.FullTopic(), vbno, snapEndSeqNo)
}

// find the snapshot to which the checkpoint seqno belongs
func (ckmgr *CheckpointManager) getSnapshotForSeqno(vbno uint16, seqno uint64) (uint64, uint64, error) {
	snapshot_history_obj, ok1 := ckmgr.snapshot_history_map[vbno]
	var latestSnapshotStart uint64
	var latestSnapshotEnd uint64
	var earliestSnapshotStart uint64
	var earliestSnapshotEnd uint64
	if ok1 {
		snapshot_history_obj.lock.RLock()
		defer snapshot_history_obj.lock.RUnlock()
		for i := len(snapshot_history_obj.snapshot_history) - 1; i >= 0; i-- {
			cur_snapshot := snapshot_history_obj.snapshot_history[i]
			if i == len(snapshot_history_obj.snapshot_history)-1 {
				latestSnapshotStart = cur_snapshot.start_seqno
				latestSnapshotEnd = cur_snapshot.end_seqno
			}
			if i == 0 {
				earliestSnapshotStart = cur_snapshot.start_seqno
				earliestSnapshotEnd = cur_snapshot.end_seqno
			}

			if seqno >= cur_snapshot.start_seqno && seqno <= cur_snapshot.end_seqno {
				return cur_snapshot.start_seqno, cur_snapshot.end_seqno, nil
			}
		}
	} else {
		err := fmt.Errorf("%v Calling getSnapshotForSeqno on an unknown vb=%v\n", ckmgr.pipeline.FullTopic(), vbno)
		ckmgr.handleGeneralError(err)
		return 0, 0, err
	}

	// kv - https://github.com/couchbase/kv_engine/blob/master/docs/dcp/documentation/commands/stream-request.md
	// will require that ssStartSeqno <= seqnoToResume <= ssEndSeqno, so return the values accordingly for checkpointing
	return seqno, seqno, fmt.Errorf("%v Failed to find snapshot for vb=%v seqno=%v earliestSnapshot=[%v,%v] latestSnapshot=[%v,%v]",
		ckmgr.pipeline.FullTopic(), vbno, seqno, earliestSnapshotStart, earliestSnapshotEnd, latestSnapshotStart, latestSnapshotEnd)
}

func (ckmgr *CheckpointManager) getRollbackCtx() *globalCkptPrereplicateCacheCtx {
	ckmgr.rollbackCtxMtx.RLock()
	if ckmgr.rollbackCtx != nil {
		defer ckmgr.rollbackCtxMtx.RUnlock()
		return ckmgr.rollbackCtx
	}
	ckmgr.rollbackCtxMtx.RUnlock()

	ckmgr.rollbackCtxMtx.Lock()
	defer ckmgr.rollbackCtxMtx.Unlock()
	if ckmgr.rollbackCtx != nil {
		return ckmgr.rollbackCtx
	}

	ckmgr.rollbackCtx = ckmgr.newGlobalCkptPrereplicateCacheCtx(time.Duration(base.GlobalPreReplicateCacheExpireTimeSecs)*time.Second,
		time.Duration(base.GlobalPreReplicateCacheErrorExpireTimeSecs)*time.Second)
	return ckmgr.rollbackCtx
}

func (ckmgr *CheckpointManager) UpdateVBTimestamps(vbno uint16, rollbackseqno uint64) (*base.VBTimestamp, error) {
	ckmgr.logger.Infof("Received rollback from DCP stream vb=%v, rollbackseqno=%v\n", vbno, rollbackseqno)
	pipeline_startSeqnos_map, pipeline_startSeqnos_map_lock := GetStartSeqnos(ckmgr.pipeline, ckmgr.logger)

	if pipeline_startSeqnos_map == nil {
		return nil, fmt.Errorf("Error retrieving vb timestamp map for %v\n", ckmgr.pipeline.FullTopic())
	}
	pipeline_startSeqnos_map_lock.Lock()
	defer pipeline_startSeqnos_map_lock.Unlock()

	pipeline_start_seqno, ok := pipeline_startSeqnos_map[vbno]
	if !ok {
		return nil, fmt.Errorf("%v Invalid vbno=%v\n", ckmgr.pipeline.FullTopic(), vbno)
	}

	if ckmgr.pipeline.Type() == common.BackfillPipeline {
		ckmgr.clearBackfillStartingTsIfNeeded(vbno, rollbackseqno)
	}

	checkpointDoc, err := ckmgr.retrieveCkptDoc(vbno)
	if err == service_def.MetadataNotFoundErr {
		ckmgr.logger.Errorf("Unable to find ckpt doc from metakv for vbno: %v", vbno)
		err = nil
	}
	if err != nil {
		return nil, err
	}

	// when we re-compute vbtimestamp, we try earlier checkpoint records with seqno <= rollbackseqno
	max_seqno := rollbackseqno
	if max_seqno == 0 && pipeline_start_seqno.Seqno > 0 {
		// rollbackseqno = 0 may happen in two scenarios
		// 1. vbuuid associated with pipeline_start_seqno.Seqno cannot be found in the current failover log
		//    in this case rolling back to 0 may be avoided by trying earlier checkpoint records,
		//    which may contain a different vbuuid
		// 2. some seqno > pipeline_start_seqno.Seqno has been purged.
		//    in this case rolling back to 0 cannot be avoided by trying earlier checkpoint records
		// since we cannot differentiate between these two scenarios, we always try earlier checkpoint records
		// as a best effort to avoid rolling back to 0
		max_seqno = pipeline_start_seqno.Seqno - 1
	}

	ckmgr.logger.Infof("vb=%v, current_start_seqno=%v, max_seqno=%v\n", vbno, pipeline_start_seqno.Seqno, max_seqno)

	var rollbackCtx *globalCkptPrereplicateCacheCtx
	if ckmgr.isVariableVBMode() {
		// If one VB starts rollback, it'll affect many other VBs in the same timeframe. Get a single instance of rollback context
		rollbackCtx = ckmgr.getRollbackCtx()
	}
	//vbts, vbStats, brokenMappings, targetManifestId, lastSuccessfulBackfillMgrSrcManifestId, err := ckmgr.getDataFromCkpts(vbno, checkpointDoc, max_seqno, rollbackCtx)
	vbts, vbStats, lastSuccessfulBackfillMgrSrcManifestId, traditionalPair, globalPairs, err := ckmgr.getDataFromCkpts(vbno, checkpointDoc, max_seqno, rollbackCtx)
	if err != nil {
		return nil, err
	}

	pipeline_startSeqnos_map[vbno] = vbts

	ckmgr.restoreBrokenMappingManifests(traditionalPair, globalPairs, true /*isRollBack*/)

	//set the start seqno on through_seqno_tracker_svc
	ckmgr.through_seqno_tracker_svc.SetStartSeqno(vbno, vbts.Seqno, vbts.ManifestIDs)
	err = ckmgr.statsMgr.SetVBCountMetrics(vbno, vbStats)
	if err != nil {
		ckmgr.logger.Warnf("setting vbStat for vb %v returned error %v Stats: %v\n", vbno, err, vbStats)
	}

	if ckmgr.pipeline.Type() == common.MainPipeline {
		go ckmgr.getBackfillMgr().SetLastSuccessfulSourceManifestId(ckmgr.pipeline.Topic(), lastSuccessfulBackfillMgrSrcManifestId, true, ckmgr.finish_ch)
	}

	ckmgr.logger.Infof("Rolled back startSeqno to %v for vb=%v\n", vbts.Seqno, vbno)
	ckmgr.logger.Infof("Retry vbts=%v\n", vbts)

	return vbts, nil
}

// returns the vbts map and the associated lock on the map
// caller needs to lock the lock appropriatedly before using the map
func GetStartSeqnos(pipeline common.Pipeline, logger *log.CommonLogger) (map[uint16]*base.VBTimestamp, *sync.RWMutex) {
	if pipeline != nil {
		settings := pipeline.Settings()
		startSeqnos_obj_with_lock, ok := settings[base.VBTimestamps].(*base.ObjectWithLock)
		if ok {
			return startSeqnos_obj_with_lock.Object.(map[uint16]*base.VBTimestamp), startSeqnos_obj_with_lock.Lock
		} else {
			logger.Errorf("Didn't find 'VBTimesstamps' in settings. settings=%v\n", settings.CloneAndRedact())
		}
	} else {
		logger.Info("pipleine is nil")
	}
	return nil, nil
}

func (ckmgr *CheckpointManager) handleVBError(vbno uint16, err error) {
	additionalInfo := &base.VBErrorEventAdditional{vbno, err, base.VBErrorType_Target}
	ckmgr.RaiseEvent(common.NewEvent(common.VBErrorEncountered, nil, ckmgr, nil, additionalInfo))
}

func (ckmgr *CheckpointManager) getCurrentCkptWLock(vbno uint16) *checkpointRecordWithLock {
	return ckmgr.cur_ckpts[vbno]
}

func (ckgr *CheckpointManager) IsSharable() bool {
	return false
}

func (ckmgr *CheckpointManager) Detach(pipeline common.Pipeline) error {
	return base.ErrorNotSupported
}

func (ckmgr *CheckpointManager) updateReplStatusBrokenMap() {
	ckmgr.cachedBrokenMap.lock.RLock()
	brokenMapClone := ckmgr.cachedBrokenMap.brokenMap.Clone()
	ckmgr.cachedBrokenMap.lock.RUnlock()

	ckmgr.pipeline.SetBrokenMap(brokenMapClone)
}

func (ckmgr *CheckpointManager) NotifyBackfillMgrRollbackTo0(vbno uint16) {
	topic := ckmgr.pipeline.Topic()

	backfillMgrPipelineSvc := ckmgr.pipeline.RuntimeContext().Service(base.BACKFILL_MGR_SVC)

	settingsMap := make(map[string]interface{})
	settingsMap[base.NameKey] = topic
	settingsMap[metadata.CollectionsVBRollbackTo0Key] = int(vbno)
	backfillMgrPipelineSvc.UpdateSettings(settingsMap)
}

// This path is called once manifests changes occur, and backfill map may need to be cleaned up
// This potentially is needed if no I/O is ongoing, which means that router won't be cleaning it up
// Without this function, brokenMap is only updated if a doc flow through the collection router of the VB
// and the router raises brokenMap event. This function supplements that and can happen concurrently
func (ckmgr *CheckpointManager) CleanupInMemoryBrokenMap(diffPair *metadata.CollectionNamespaceMappingsDiffPair, srcManDelta []*metadata.CollectionsManifest, latestTgtManifestId uint64) {
	sourceChanged := len(srcManDelta) == 2 && srcManDelta[0] != nil && srcManDelta[1] != nil && srcManDelta[0].Uid() != srcManDelta[1].Uid()
	// This must be deferred first as defer calls are FILO and this needs a lock
	defer ckmgr.updateReplStatusBrokenMap()
	// This path only gets called when manifest changes... so it shouldn't be too often/expensive to clone
	ckmgr.cachedBrokenMap.lock.Lock()
	defer ckmgr.cachedBrokenMap.lock.Unlock()

	oldMap := ckmgr.cachedBrokenMap.brokenMap.Clone()
	oldTargetManifestId := ckmgr.cachedBrokenMap.correspondingTargetManifest

	if latestTgtManifestId > oldTargetManifestId {
		// bump the cached manifest
		ckmgr.cachedBrokenMap.correspondingTargetManifest = latestTgtManifestId
		// pair.Added means backfillMgr detected these and need to be backfilled, which means they're no longer broken
		ckmgr.cachedBrokenMap.brokenMap = ckmgr.cachedBrokenMap.brokenMap.Delete(diffPair.Added)
		// pair.Removed means these mappings no longer exist and are obsolete, and should be cleaned up too
		ckmgr.cachedBrokenMap.brokenMap = ckmgr.cachedBrokenMap.brokenMap.Delete(diffPair.Removed)
		// As part of bumping, save the pre-bumped manifest - brokenMap pair
		ckmgr.cachedBrokenMap.brokenMapHistories[oldTargetManifestId] = oldMap
	} else if oldTargetManifestId == latestTgtManifestId {
		// This can happen in 2 cases
		// 1. A router event updated the manifest+brokenMappings even before this function call
		// 2. Only source manifest changed. - In this case the below code will be a no-op

		// pair.Added means backfillMgr detected these and need to be backfilled, which means they're no longer broken
		ckmgr.cachedBrokenMap.brokenMap = ckmgr.cachedBrokenMap.brokenMap.Delete(diffPair.Added)
		// pair.Removed means these mappings no longer exist and are obsolete, and should be cleaned up too
		ckmgr.cachedBrokenMap.brokenMap = ckmgr.cachedBrokenMap.brokenMap.Delete(diffPair.Removed)
	}

	newerMap := ckmgr.cachedBrokenMap.brokenMap.Clone()

	if sourceChanged {
		_, _, removed, err := srcManDelta[1].Diff(srcManDelta[0])
		if err != nil {
			return
		}
		cachedIndex := ckmgr.cachedBrokenMap.brokenMap.CreateLookupIndex()
		var reusedNamespace base.CollectionNamespace
		for scopeName, scope := range removed {
			for collectionName, _ := range scope.Collections {
				reusedNamespace.ScopeName = scopeName
				reusedNamespace.CollectionName = collectionName

				srcPtr, _, _, exists := ckmgr.cachedBrokenMap.brokenMap.Get(&reusedNamespace, cachedIndex)
				if exists {
					delete(ckmgr.cachedBrokenMap.brokenMap, srcPtr)
				}
			}
		}
		newerMap = ckmgr.cachedBrokenMap.brokenMap.Clone()
	}

	if len(diffPair.Added) > 0 {
		// Only repaired should be logged
		// Things that are removed due to things being removed should not be logged
		ckmgr.wait_grp.Add(1)
		go ckmgr.logBrokenMapUpdatesToUI(oldMap, newerMap)
	}
}

// This API can be called from multiple places... i.e. either response from a request, or due to a peer node pushing
func (ckmgr *CheckpointManager) MergePeerNodesCkptInfo(genericResponse interface{}) error {
	nodesRespMap, ok := genericResponse.(peerToPeer.PeersVBMasterCheckRespMap)
	if ok {
		return ckmgr.mergeNodesToVBMasterCheckResp(nodesRespMap)
	}

	periodicPush, ok := genericResponse.(*peerToPeer.ReplicationPayload)
	if ok {
		return ckmgr.mergePeerNodesPeriodicPush(periodicPush)
	}

	return fmt.Errorf("mergePeerNodesCkptInfo unhandled type: %v", reflect.TypeOf(genericResponse))
}

type nodeVbCkptMap map[string]metadata.VBsCkptsDocMap

// Merging pre-requisites:
//  1. For each checkpoint, the VBUUID must exist in the src and target failover log. If not, ckpt is not valid
//     and is not worth saving
//  2. Once invalid ckpts are filtered out, they need to be sorted
//  3. Sort first by source side
func (ckmgr *CheckpointManager) mergeNodesToVBMasterCheckResp(respMap peerToPeer.PeersVBMasterCheckRespMap) error {
	if len(respMap) == 0 {
		ckmgr.logger.Infof("Nothing to merge")
		// nothing to merge
		return nil
	}

	var needToGetFailoverLogs bool
	combinedSrcManifests := make(metadata.ManifestsCache)
	combinedTgtManifests := make(metadata.ManifestsCache)
	var combinedBrokenMapping []*metadata.CollectionNsMappingsDoc
	combinedBrokenMappingSpecInternalId := make(map[string]string)
	var combinedGlobalInfoDocs []*metadata.GlobalInfoCompressedDoc

	warnMap := make(base.ErrorMap)
	nodeVbMainCkptsMap := make(nodeVbCkptMap)
	nodeVbBackfillCkptsMap := make(nodeVbCkptMap)
	for node, resp := range respMap {
		key := resp.SourceBucketName
		payloadMap, unlockFunc := resp.GetReponse()
		if payloadMap == nil {
			unlockFunc()
			warnMap[node] = base.ErrorNilPtr
			continue
		}
		payload, ok := (*payloadMap)[key]
		if !ok || payload == nil {
			unlockFunc()
			warnMap[node] = fmt.Errorf("Unable to find payload given bucket key %v", key)
			continue
		}

		oneNodeVbsCkptMap, err := payload.GetAllCheckpoints(common.MainPipeline)
		if err != nil {
			return err
		}
		if len(oneNodeVbsCkptMap) > 0 {
			needToGetFailoverLogs = true
			ckmgr.logger.Infof("Received peerToPeer checkpoint data from node %v replId %v opaque %v", node, resp.ReplicationSpecId, resp.GetOpaque())
			nodeVbMainCkptsMap[node] = oneNodeVbsCkptMap
		}
		oneNodeVbsBackfillCkptMap, err := payload.GetAllCheckpoints(common.BackfillPipeline)
		if err != nil {
			return err
		}
		if len(oneNodeVbsBackfillCkptMap) > 0 {
			needToGetFailoverLogs = true
			ckmgr.logger.Infof("Received peerToPeer backfill checkpoint data from node %v replId %v", node, resp.ReplicationSpecId)
			nodeVbBackfillCkptsMap[node] = oneNodeVbsBackfillCkptMap
		}

		srcManifests, tgtManifests := payload.GetAllManifests()
		combinedSrcManifests.LoadIfNotExists(srcManifests)
		combinedTgtManifests.LoadIfNotExists(tgtManifests)

		brokenMap := payload.GetBrokenMappingDoc()
		if brokenMap != nil && brokenMap.SpecInternalId != "" {
			combinedBrokenMapping = append(combinedBrokenMapping, brokenMap)
			combinedBrokenMappingSpecInternalId[node] = brokenMap.SpecInternalId
			ckmgr.logger.Infof("Received peerToPeer brokenMap data (%v) from node %v replId %v opaque %v", len(brokenMap.NsMappingRecords), node, resp.ReplicationSpecId, resp.GetOpaque())
		}

		if payload.GlobalInfoDoc != nil {
			combinedGlobalInfoDocs = append(combinedGlobalInfoDocs, payload.GlobalInfoDoc)
			ckmgr.logger.Infof("Received peerToPeer globalInfo deduped data (%v) from node %v replId %v opaque %v", len(payload.GlobalInfoDoc.NsMappingRecords), node, resp.ReplicationSpecId, resp.GetOpaque())
		}
		unlockFunc()
	}

	if len(warnMap) > 0 {
		ckmgr.logger.Warnf("Parsing peer nodes data had errors: %v", base.FlattenErrorMap(warnMap))
	}

	// If specIds are different, then something is mid flight
	brokenMapSpecInternalId, err := ckmgr.checkSpecInternalID(combinedBrokenMappingSpecInternalId)
	if err != nil {
		ckmgr.logger.Errorf("InternalID mismatch, combined SpecIDs: %v", combinedBrokenMappingSpecInternalId)
		return err
	}

	var srcFailoverLogs map[uint16]*mcc.FailoverLog
	var tgtFailoverLogs map[uint16]*mcc.FailoverLog
	if needToGetFailoverLogs {
		srcFailoverLogs, err = ckmgr.getOneTimeSrcFailoverLog()
		if err != nil {
			ckmgr.logger.Errorf("unable to get failoverlog from source %v", err)
			return err
		}
	}

	filteredMaps := filterInvalidCkptsBasedOnSourceFailover([]nodeVbCkptMap{nodeVbMainCkptsMap, nodeVbBackfillCkptsMap}, srcFailoverLogs)
	vbsThatNeedTargetFailoverlogs := findVbsThatNeedTargetFailoverLogs(filteredMaps)
	if len(vbsThatNeedTargetFailoverlogs) > 0 {
		tgtFailoverLogs, err = ckmgr.GetOneTimeTgtFailoverLogs(vbsThatNeedTargetFailoverlogs)
		if err != nil {
			ckmgr.logger.Warnf("unable to get failoverlog from target(s) %v - using pre_replicate", err)
			filteredMaps = ckmgr.filterInvalidCkptsBasedOnPreReplicate(filteredMaps)
		} else {
			filteredMaps = filterInvalidCkptsBasedOnTargetFailover(filteredMaps, tgtFailoverLogs, ckmgr.logger)
		}
	}

	filteredMaps = filterCkptsBasedOnPipelineReinitHash(filteredMaps, ckmgr.pipelineReinitHash)

	filteredMaps, combinedShaMap, err := filterCkptsWithoutValidBrokenmaps(filteredMaps, combinedBrokenMapping)
	if err != nil {
		// filteredMap may still be valid... continue
		ckmgr.logger.Warnf("filterCkptsWithoutValidBrokenMaps had errors: %v", err)
	}

	err, done := ckmgr.preStopGcCheck(filteredMaps)
	if done {
		ckmgr.logger.Errorf("Unable to continue merge checkpoint due to err %v", err)
		return err
	}

	combinedGlobalInfoShaMap := make(metadata.ShaToGlobalInfoMap)
	for _, oneGInfoShaDoc := range combinedGlobalInfoDocs {
		shaMap, err := oneGInfoShaDoc.ToShaMap()
		if err != nil {
			return err
		}
		combinedGlobalInfoShaMap.Load(shaMap)
	}

	getterFunc := func() *MergeCkptArgs {
		return &MergeCkptArgs{
			PipelinesCkptDocs:       filteredMaps,
			BrokenMappingShaMap:     combinedShaMap,
			GlobalInfoShaMap:        combinedGlobalInfoShaMap,
			BrokenMapSpecInternalId: brokenMapSpecInternalId,
			SrcManifests:            &combinedSrcManifests,
			TgtManifests:            &combinedTgtManifests,
			SrcFailoverLogs:         srcFailoverLogs,
			TgtFailoverLogs:         tgtFailoverLogs,
		}
	}

	err = ckmgr.stopTheWorldAndMergeCkpts(getterFunc)
	if err != nil {
		ckmgr.logger.Errorf("Unable to stop the world and merge checkpoint due to err %v", err)
		return err
	}
	return nil
}

func (ckmgr *CheckpointManager) preStopGcCheck(pipelinesCkptDocs []metadata.VBsCkptsDocMap) (error, bool) {
	if len(pipelinesCkptDocs) != int(common.PipelineTypeInvalidEnd) {
		return fmt.Errorf("Invalid number of pipelines: %v vs %v", len(pipelinesCkptDocs), int(common.PipelineTypeInvalidEnd)), true
	}

	if len(pipelinesCkptDocs[common.MainPipeline]) == 0 && len(pipelinesCkptDocs[common.BackfillPipeline]) == 0 {
		// Nothing to merge, don't waste time stopping the world
		return nil, true
	}
	return nil, false
}

func (ckmgr *CheckpointManager) stopTheWorldAndMergeCkpts(getter MergeCkptArgsGetter) error {
	stopFunc := ckmgr.utils.StartDiagStopwatch("ckmgr.stopTheWorldAndMergeCkpt", base.DiagStopTheWorldAndMergeCkptThreshold)
	defer stopFunc()
	// Before merging, disable checkpointing and wait until all checkpointing tasks are finished

	disableCkptStopTimer := ckmgr.utils.StartDiagStopwatch("ckmgr.stopTheWorldAndMergeCkpt.disableCkptAndWait", base.DiagCkptStopTheWorldThreshold)
	injWaitSec := int(atomic.LoadUint32(&ckmgr.xdcrDevInjectGcWaitSec))
	if injWaitSec > 0 {
		ckmgr.logger.Infof("XDCR Developer injection found and waiting for %v seconds for stopping the world...", injWaitSec)
		time.Sleep(time.Duration(injWaitSec) * time.Second)
	}

	ckmgr.checkpointAllowedHelper.disableCkptAndWait()
	disableCkptStopTimer()
	defer func() {
		// The MergeCheckpoints function can be invoked in the following scenarios:
		// 1. During VBMasterCheck at the time of pipeline start.
		// 2. As part of a periodic push operation.
		// Checkpointing operation should be allowed only if the SetVBTimestamp operation is done
		// i.e. for case 1 we should not allow checkpointing since SetVBTimestamp opertion wouldn't have been completed yet.
		if ckmgr.checkpointAllowedHelper.isSetVBTimestampOpDone() {
			ckmgr.checkpointAllowedHelper.setCheckpointAllowed()
		}
	}()

	// Once finished waiting, get all the accumulated information during this time that needs to be merged and do it in one shot
	ckptArgs := getter()
	if ckptArgs == nil {
		return nil
	}

	pipelinesCkptDocs := ckptArgs.PipelinesCkptDocs
	brokenMappingShaMap := ckptArgs.BrokenMappingShaMap
	globalInfoShaMap := ckptArgs.GlobalInfoShaMap
	brokenMapSpecInternalId := ckptArgs.BrokenMapSpecInternalId
	srcManifests := ckptArgs.SrcManifests
	tgtManifests := ckptArgs.TgtManifests
	srcFailoverLogs := ckptArgs.SrcFailoverLogs
	tgtFailoverLogs := ckptArgs.TgtFailoverLogs
	respChs := ckptArgs.PushRespChs

	err := ckmgr.mergeFinalCkpts(pipelinesCkptDocs, srcFailoverLogs, tgtFailoverLogs, brokenMappingShaMap, brokenMapSpecInternalId, globalInfoShaMap)
	if err != nil {
		ckmgr.logger.Errorf("megeFinalCkpts error %v", err)
		respToGcCh(respChs, err, ckmgr.finish_ch)
		return err
	}

	if srcManifests != nil || tgtManifests != nil {
		// Needs to persist at least some manifests
		nameOnlySpec := &metadata.ReplicationSpecification{Id: ckmgr.pipeline.Topic()}
		if srcManifests == nil {
			srcManifests = &metadata.ManifestsCache{}
		}
		if tgtManifests == nil {
			tgtManifests = &metadata.ManifestsCache{}
		}
		manifestMeasureStopFunc := ckmgr.utils.StartDiagStopwatch("ckmgr.PersistReceivedManifests", base.DiagInternalThreshold)
		err = ckmgr.collectionsManifestSvc.PersistReceivedManifests(nameOnlySpec, *srcManifests, *tgtManifests)
		manifestMeasureStopFunc()
		if err != nil {
			ckmgr.logger.Errorf("PersistReceivedManifests Error: %v", err)
			respToGcCh(respChs, err, ckmgr.finish_ch)
			return err
		}
	}

	if len(respChs) > 0 {
		// Periodic push do not go through pipeline restart, so need to register them for GC
		for i, ckpts := range pipelinesCkptDocs {
			if ckpts == nil {
				continue
			}
			genSpec := ckmgr.pipeline.Specification()
			registerTopic := common.ComposeFullTopic(genSpec.GetReplicationSpec().Id, common.PipelineType(i))
			for vbno, _ := range ckpts {
				gcErr := ckmgr.registerGCFunction(registerTopic, vbno)
				if gcErr != nil {
					ckmgr.logger.Warnf("Unable to register GC for vbno %v - err: %v", vbno, gcErr)
				}
			}
		}
	}

	respToGcCh(respChs, nil, ckmgr.finish_ch)
	return nil
}

func respToGcCh(respCh []chan error, err error, finCh chan bool) {
	select {
	case <-finCh:
		return
	default:
		if len(respCh) > 0 {
			for _, ch := range respCh {
				select {
				case <-finCh:
					return
				case ch <- err:
					// Done
				}
			}
		}
	}
}

func (ckmgr *CheckpointManager) checkSpecInternalID(combinedBrokenMappingSpecInternalId map[string]string) (string, error) {
	var brokenMapInternalId string
	internalIdCntMap := make(map[string]int)

	for _, checkId := range combinedBrokenMappingSpecInternalId {
		internalIdCntMap[checkId]++
	}

	if len(internalIdCntMap) == 0 {
		return "", nil
	} else if len(internalIdCntMap) == 1 {
		// No disagreements
		for k, _ := range internalIdCntMap {
			brokenMapInternalId = k
		}
	} else if len(internalIdCntMap) > 1 {
		// There are disagreements
		countOfCountMap := make(map[int]int)
		var maxCnt = -1
		var majorityId string
		// First check if there is no consensus
		for id, cnt := range internalIdCntMap {
			countOfCountMap[cnt]++
			if cnt > maxCnt {
				majorityId = id
				maxCnt = cnt
			}
		}
		if len(countOfCountMap) == 1 {
			// No majority
			return "", ckptSplitInternalSpecIds
		}
		brokenMapInternalId = majorityId
	}

	if brokenMapInternalId != "" {
		// Check to make sure brokenMap is valid
		genSpec := ckmgr.pipeline.Specification()
		if genSpec == nil {
			return brokenMapInternalId, base.ErrorNilPtr
		}
		spec := genSpec.GetReplicationSpec()
		if spec == nil {
			return brokenMapInternalId, base.ErrorNilPtr
		}
		if spec.InternalId != brokenMapInternalId {
			return brokenMapInternalId, fmt.Errorf("mismatch - brokenMapID is %v while spec's internal ID is %v", brokenMapInternalId, spec.InternalId)
		}
	}
	return brokenMapInternalId, nil
}

func filterCkptsWithoutValidBrokenmaps(filteredMaps []metadata.VBsCkptsDocMap, compressedBrokenMappings []*metadata.CollectionNsMappingsDoc) ([]metadata.VBsCkptsDocMap, metadata.ShaToCollectionNamespaceMap, error) {
	errMap := make(base.ErrorMap)
	var errCnt int
	combinedShaMap := make(metadata.ShaToCollectionNamespaceMap)

	for _, oneMap := range compressedBrokenMappings {
		oneShaMap, err := oneMap.ToShaMap()
		if err != nil {
			errMap[fmt.Sprintf("toShaMap error %v", errCnt)] = err
			errCnt++
			continue
		}
		for sha, actualMap := range oneShaMap {
			combinedShaMap[sha] = actualMap
		}
	}

	for _, filteredMap := range filteredMaps {
		for _, ckptDoc := range filteredMap {
			var replacementList metadata.CheckpointRecordsList
			for _, ckptRecord := range ckptDoc.Checkpoint_records {
				if ckptRecord == nil {
					continue
				}
				if ckptRecord.IsTraditional() {
					if ckptRecord.BrokenMappingSha256 == "" {
						// no brokenmap, ok to keep
						replacementList = append(replacementList, ckptRecord)
					} else {
						if brokenMapping, exists := combinedShaMap[ckptRecord.BrokenMappingSha256]; exists && brokenMapping != nil {
							err := ckptRecord.LoadBrokenMapping(combinedShaMap)
							if err != nil {
								errMap[fmt.Sprintf("loadBrokenMapping error %v", errCnt)] = err
								continue
							}
							// BrokenMapping is found, ok to keep
							replacementList = append(replacementList, ckptRecord)
						}
					}
				} else {
					// Global checkpoint has already loaded shas as part of the deserializing process
					replacementList = append(replacementList, ckptRecord)
				}
			}
			ckptDoc.Checkpoint_records = replacementList
		}
	}

	if len(errMap) > 0 {
		return filteredMaps, combinedShaMap, errors.New(base.FlattenErrorMap(errMap))
	}
	return filteredMaps, combinedShaMap, nil
}

func filterInvalidCkptsBasedOnTargetFailover(ckptsMaps []metadata.VBsCkptsDocMap, tgtFailoverLogs map[uint16]*mcc.FailoverLog, logger *log.CommonLogger) []metadata.VBsCkptsDocMap {
	mainPipelineMap := make(metadata.VBsCkptsDocMap)
	backfillPipelineMap := make(metadata.VBsCkptsDocMap)
	var combinedMap metadata.VBsCkptsDocMap

	for i, ckptsMap := range ckptsMaps {
		switch i {
		case int(common.MainPipeline):
			combinedMap = mainPipelineMap
		case int(common.BackfillPipeline):
			combinedMap = backfillPipelineMap
		default:
			panic(fmt.Sprintf("wrong integer %v", i))
		}
		if ckptsMap == nil {
			continue
		}
		for vbno, ckptDoc := range ckptsMap {
			failoverLog, found := tgtFailoverLogs[vbno]
			if !found || failoverLog == nil {
				continue
			}
			modifiedFailoverLog, err := prepareForLocalPreReplicate(failoverLog)
			if err != nil {
				// should never happen
				logger.Fatalf("failed to prepare failover log for localPreReplicate. vbno %v err=%v", vbno, err)
				continue
			}

			_, exists := combinedMap[vbno]
			if !exists {
				// It's ok for checkpointRecords to be emptyList
				combinedMap[vbno] = ckptDoc.CloneWithoutRecords()
			}

			// Validate that this record is valid
			for _, ckptRecord := range ckptDoc.Checkpoint_records {
				if ckptRecord == nil {
					continue
				}

				if IsCkptRecordValidBasedOnTgtFailover(ckptRecord, modifiedFailoverLog) {
					// Usable checkpoint based purely off of target bucket info
					combinedMap[vbno].Checkpoint_records = append(combinedMap[vbno].Checkpoint_records, ckptRecord)
					continue
				}
			}
		}
	}
	return []metadata.VBsCkptsDocMap{mainPipelineMap, backfillPipelineMap}
}

func filterInvalidCkptsBasedOnSourceFailover(nodeVbCkptMaps []nodeVbCkptMap, srcFailoverLogs map[uint16]*mcc.FailoverLog) []metadata.VBsCkptsDocMap {
	mainPipelineMap := make(metadata.VBsCkptsDocMap)
	backfillPipelineMap := make(metadata.VBsCkptsDocMap)
	var combinedMap metadata.VBsCkptsDocMap

	for i, ckptsMap := range nodeVbCkptMaps {
		switch i {
		case int(common.MainPipeline):
			combinedMap = mainPipelineMap
		case int(common.BackfillPipeline):
			combinedMap = backfillPipelineMap
		default:
			panic(fmt.Sprintf("wrong integer %v", i))
		}
		if ckptsMap == nil {
			continue
		}
		for _, vbCkpts := range ckptsMap {
			for vbno, ckptDoc := range vbCkpts {
				failoverLog, found := srcFailoverLogs[vbno]
				if !found || failoverLog == nil {
					continue
				}

				_, exists := combinedMap[vbno]
				if !exists {
					// It's ok for checkpointRecords to be emptyList
					combinedMap[vbno] = ckptDoc.CloneWithoutRecords()
				}

				// Validate that this record is valid
				for _, ckptRecord := range ckptDoc.Checkpoint_records {
					if ckptRecord == nil {
						continue
					}

					ckptRecordVbUuid := ckptRecord.Failover_uuid

					for _, vbUuidSeqnoPair := range *failoverLog {
						failoverVbUuid := vbUuidSeqnoPair[0]

						if ckptRecordVbUuid == failoverVbUuid {
							// Usable checkpoint based purely off of source bucket info
							combinedMap[vbno].Checkpoint_records = append(combinedMap[vbno].Checkpoint_records, ckptRecord)
							continue
						}
					}
				}
			}
		}
	}
	return []metadata.VBsCkptsDocMap{mainPipelineMap, backfillPipelineMap}
}

func filterCkptsBasedOnPipelineReinitHash(ckptsMaps []metadata.VBsCkptsDocMap, currentReinitHash string) []metadata.VBsCkptsDocMap {
	mainPipelineMap := make(metadata.VBsCkptsDocMap)
	backfillPipelineMap := make(metadata.VBsCkptsDocMap)
	var combinedMap metadata.VBsCkptsDocMap

	for i, ckptsMap := range ckptsMaps {
		switch i {
		case int(common.MainPipeline):
			combinedMap = mainPipelineMap
		case int(common.BackfillPipeline):
			combinedMap = backfillPipelineMap
		default:
			panic(fmt.Sprintf("wrong integer %v", i))
		}
		if ckptsMap == nil {
			continue
		}
		for vbno, ckptDoc := range ckptsMap {
			_, exists := combinedMap[vbno]
			if !exists {
				// It's ok for checkpointRecords to be emptyList
				combinedMap[vbno] = ckptDoc.CloneWithoutRecords()
			}

			// Validate that this record is valid
			for _, ckptRecord := range ckptDoc.Checkpoint_records {
				if ckptRecord == nil {
					continue
				}

				if ckptRecord.PipelineReinitHash != currentReinitHash {
					continue
				}

				combinedMap[vbno].Checkpoint_records = append(combinedMap[vbno].Checkpoint_records, ckptRecord)
			}
		}
	}
	return []metadata.VBsCkptsDocMap{mainPipelineMap, backfillPipelineMap}
}

func findVbsThatNeedTargetFailoverLogs(filteredMaps []metadata.VBsCkptsDocMap) []uint16 {
	dedupMap := make(map[uint16]bool)
	for _, filteredMap := range filteredMaps {
		if filteredMap == nil {
			continue
		}
		for vbno, doc := range filteredMap {
			if len(doc.Checkpoint_records) > doc.GetMaxCkptRecordsToKeep() {
				//We need to further filter down the checkpoints - may need filtering using target
				dedupMap[vbno] = true
			}
		}
	}
	var retVBList []uint16
	for vbno, _ := range dedupMap {
		retVBList = append(retVBList, vbno)
	}
	return retVBList
}

func (ckmgr *CheckpointManager) getOneTimeSrcFailoverLog() (map[uint16]*mcc.FailoverLog, error) {
	memcachedAddr, err := ckmgr.xdcr_topology_svc.MyMemcachedAddr()
	if err != nil {
		return nil, err
	}

	spec := ckmgr.pipeline.Specification().GetReplicationSpec()

	var vbsList []uint16
	srcsMap := ckmgr.pipeline.Sources()
	for _, srcNozzle := range srcsMap {
		oneList := srcNozzle.ResponsibleVBs()
		vbsList = append(vbsList, oneList...)
	}

	//GetMemcachedConnection(serverAddr, bucketName, userAgent string, keepAlivePeriod time.Duration, logger *log.CommonLogger) (mcc.ClientIface, error)
	client, err := ckmgr.utils.GetMemcachedConnection(memcachedAddr, spec.SourceBucketName, ckmgr.UserAgent, 0, ckmgr.logger)
	if err != nil {
		return nil, err
	}
	feed, err := client.NewUprFeedIface()
	if err != nil {
		return nil, err
	}
	err = feed.UprOpen(ckmgr.UserAgent, uint32(0), base.UprFeedBufferSize)
	if err != nil {
		return nil, err
	}
	defer feed.Close()
	return client.UprGetFailoverLog(vbsList)
}

// TODO - maybe need retry mechanism

// This merge function is called in two different paths: 1. Pull model vs 2. Push model (only on main pipeline's)
// When pulling, both main and backfill pipelines are stopped, and the main pipeline's checkpoint manager is responsible
// for merging both types of ckpts
// When pushing, either one or both are potentially running, but only backfill pipeline's ckptMgr may perform ckpt
// while this is happening
func (ckmgr *CheckpointManager) mergeFinalCkpts(filteredMaps []metadata.VBsCkptsDocMap,
	srcFailoverLogs, tgtFailoverLogs map[uint16]*mcc.FailoverLog,
	combinedShaMapFromPeers metadata.ShaToCollectionNamespaceMap, peerBrokenMapSpecInternalId string,
	combinedGInfoShaMapFromPeers metadata.ShaToGlobalInfoMap) error {

	genSpec := ckmgr.pipeline.Specification()
	if genSpec == nil {
		return base.ErrorNilPtr
	}
	spec := genSpec.GetReplicationSpec()
	if spec == nil {
		return base.ErrorNilPtr
	}

	errList := make([]error, len(filteredMaps))
	var waitGrp sync.WaitGroup
	for iRaw, filteredMapRaw := range filteredMaps {
		var ckptTopic string

		// Copy before passing to go-routines
		i := iRaw
		filteredMap := filteredMapRaw

		if len(combinedGInfoShaMapFromPeers) > 0 {
			for _, ckptDoc := range filteredMap {
				if ckptDoc == nil {
					continue
				}
				errMap := ckptDoc.LoadGlobalInfoFromShaMap(combinedGInfoShaMapFromPeers)
				if len(errMap) > 0 {
					return errors.New(base.FlattenErrorMap(errMap))
				}
			}
		}

		switch i {
		case int(common.MainPipeline):
			ckptTopic = ckmgr.pipeline.Topic()
		case int(common.BackfillPipeline):
			ckptTopic = common.ComposeFullTopic(ckmgr.pipeline.Topic(), common.BackfillPipeline)
		default:
			return base.ErrorNotSupported
		}

		waitGrp.Add(1)
		go func() {
			defer waitGrp.Done()
			getCkptDocsStopFunc := ckmgr.utils.StartDiagStopwatch(fmt.Sprintf("ckmgr.checkpointsDocs.%v", ckptTopic), base.DiagInternalThreshold)
			currDocs, err := ckmgr.checkpoints_svc.CheckpointsDocs(ckptTopic, true)
			getCkptDocsStopFunc()
			if err != nil {
				if err == service_def.MetadataNotFoundErr {
					// Use empty currDocs to merge incoming filteredMap
					err = nil
					currDocs = make(map[uint16]*metadata.CheckpointsDoc)
				} else {
					ckmgr.logger.Errorf("mergeFinalCkpts CheckpointsDocs err %v\n", err)
					errList[i] = err
					return
				}
			}

			combinePeerCkptDocsWithLocalCkptDoc(filteredMap, srcFailoverLogs, tgtFailoverLogs, currDocs, spec)
			err = ckmgr.mergeAndPersistBrokenMappingDocsAndCkpts(ckptTopic, combinedShaMapFromPeers, peerBrokenMapSpecInternalId, currDocs, combinedGInfoShaMapFromPeers)
			if err != nil {
				errList[i] = err
			}
		}()

	}
	waitGrp.Wait()

	var needToReturnErr bool
	var errStrs []string
	for _, checkErr := range errList {
		if checkErr != nil {
			needToReturnErr = true
			errStrs = append(errStrs, checkErr.Error())
		} else {
			errStrs = append(errStrs, "")
		}
	}

	if !needToReturnErr {
		return nil
	} else {
		return fmt.Errorf(strings.Join(errStrs, " "))
	}
}

func combinePeerCkptDocsWithLocalCkptDoc(filteredMap map[uint16]*metadata.CheckpointsDoc, srcFailoverLogs map[uint16]*mcc.FailoverLog, tgtFailoverLogs map[uint16]*mcc.FailoverLog, currDocs map[uint16]*metadata.CheckpointsDoc, spec *metadata.ReplicationSpecification) {
	// create empty docs for missing VBs
	for vb, ckptDoc := range filteredMap {
		if ckptDoc == nil || ckptDoc.Len() == 0 {
			continue
		}
		docs, exists := currDocs[vb]
		if !exists || docs == nil {
			currDocs[vb] = metadata.NewCheckpointsDoc(spec.InternalId)
		}
	}

	// This is where we combine with what checkpoints are currently stored (loaded via CheckpointsDocs() above)
	// with what is incoming and filtered, to consolidate into a full list of checkpoints
	for vb, ckptDoc := range currDocs {
		_, exists := filteredMap[vb]
		if !exists || ckptDoc == nil {
			continue
		}

		// First combine, then sort, then trim
		var srcFailoverLog *mcc.FailoverLog
		var tgtFailoverLog *mcc.FailoverLog

		if srcFailoverLogs != nil {
			srcFailoverLog = srcFailoverLogs[vb]
		}
		if tgtFailoverLogs != nil {
			tgtFailoverLog = tgtFailoverLogs[vb]
		}

		currRecordsToSort := ckptDoc.Checkpoint_records.PrepareSortStructure(srcFailoverLog, tgtFailoverLog)
		peersRecordsToSort := filteredMap[vb].Checkpoint_records.PrepareSortStructure(srcFailoverLog, tgtFailoverLog)

		var combinedRecords metadata.CheckpointSortRecordsList
		combinedRecords = append(combinedRecords, currRecordsToSort...)
		combinedRecords = append(combinedRecords, peersRecordsToSort...)

		sort.Sort(combinedRecords)

		// Trim to keep the top
		if len(combinedRecords) > ckptDoc.GetMaxCkptRecordsToKeep() {
			combinedRecords = combinedRecords[:ckptDoc.GetMaxCkptRecordsToKeep()]
		}

		ckptDoc.Checkpoint_records = combinedRecords.ToRegularList()
	}
}

func (ckmgr *CheckpointManager) mergeAndPersistBrokenMappingDocsAndCkpts(specId string,
	peersShaMap metadata.ShaToCollectionNamespaceMap, specInternalId string,
	ckptDocs map[uint16]*metadata.CheckpointsDoc, peerGInfoShaMap metadata.ShaToGlobalInfoMap) error {
	stopFunc := ckmgr.utils.StartDiagStopwatch("ckmgr.mergeAndPersistBrokenMappingDocsAndCkpts", base.DiagInternalThreshold)
	defer stopFunc()
	curNodeShaMap, curNodeMappingDoc, _, _, err := ckmgr.checkpoints_svc.LoadBrokenMappings(specId)
	if err != nil {
		ckmgr.logger.Errorf("mergeAndPersistBrokenMappingDocsAndCkpts LoadBrokenMappings err: %v", err)
		return err
	}

	curNodeGInfoMap, curNodeGInfoMappingDoc, _, _, err := ckmgr.checkpoints_svc.LoadGlobalInfoMapping(specId)
	if err != nil {
		ckmgr.logger.Errorf("mergeAndPersistBrokenMappingDocsAndCkpts LoadGlobalTimestamp err: %v", err)
		return err
	} else if curNodeGInfoMappingDoc == nil {
		err = fmt.Errorf("curNodeGtsMappingDoc returned is nil")
		return err
	}

	// merge peers into curNode
	for peerSha, peerMapping := range peersShaMap {
		_, exists := curNodeShaMap[peerSha]
		if !exists {
			curNodeShaMap[peerSha] = peerMapping
		}
	}
	if curNodeMappingDoc == nil {
		curNodeMappingDoc = &metadata.CollectionNsMappingsDoc{
			SpecInternalId: specInternalId,
		}
	}
	err = curNodeMappingDoc.LoadShaMap(curNodeShaMap)
	if err != nil {
		ckmgr.logger.Errorf("mergeAndPersistBrokenMappingDocsAndCkpts curNodeMappingDoc.LoadShaMap err: %v", err)
		return err
	}

	// Do the same for global timestamp
	for peerSha, peerGInfoMapping := range peerGInfoShaMap {
		_, exists := curNodeGInfoMap[peerSha]
		if !exists {
			curNodeGInfoMap[peerSha] = peerGInfoMapping
		}
	}
	curNodeGInfoMappingDoc = &metadata.GlobalInfoCompressedDoc{
		SpecInternalId: specInternalId,
	}
	err = curNodeGInfoMappingDoc.LoadShaMap(curNodeGInfoMap)
	if err != nil {
		ckmgr.logger.Errorf("mergeAndPersistBrokenMappingDocsAndCkpts curNodeGtsMappingDoc.LoadShaMap err: %v", err)
		return err
	}

	// TODO MB-63804: need to do global counters
	return ckmgr.checkpoints_svc.UpsertAndReloadCheckpointCompleteSet(specId, curNodeMappingDoc, ckptDocs, specInternalId, curNodeGInfoMappingDoc)
}

func (ckmgr *CheckpointManager) mergePeerNodesPeriodicPush(periodicPayload *peerToPeer.ReplicationPayload) error {
	if periodicPayload == nil {
		ckmgr.logger.Errorf("Nil periodicPayload")
		return base.ErrorNilPtr
	}

	// Before accepting peer nodes' payload, do our due diligence and find out the VBs that this node owns
	// In the case where a peer node and this node both think it owns the VB, be conservative and do not merge
	// the information
	vbList, genSpec, err := ckmgr.getLatestVbList()
	if err != nil {
		if !base.BypassUIErrorCodes(err.Error()) {
			ckmgr.logger.Errorf("error getting latest vblist: %v", err)
		}
		return err
	}

	if periodicPayload.SourceBucketName != genSpec.GetReplicationSpec().SourceBucketName {
		err := fmt.Errorf("Mismatch source bucket name - expecting %v got %v",
			genSpec.GetReplicationSpec().SourceBucketName, periodicPayload.SourceBucketName)
		ckmgr.logger.Errorf(err.Error())
		return err
	}

	filteredPayload := periodicPayload.GetSubsetBasedOnNonIntersectingVBs(vbList)
	if filteredPayload.IsEmpty() {
		if len(periodicPayload.GetPushVBs()) == 0 {
			ckmgr.logger.Warnf("Source payload for %v sent by peer is empty", periodicPayload.ReplicationSpecId)
			return nil
		} else {
			// Should not happen
			ckmgr.logger.Errorf(peerToPeer.ErrorCompleteVBOverlap.Error())
			return peerToPeer.ErrorCompleteVBOverlap
		}
	}

	payloadMap, unlockFunc := filteredPayload.GetPayloadWithReadLock()
	if payloadMap == nil {
		unlockFunc()
		ckmgr.logger.Errorf("Unable to get payload")
		return base.ErrorNilPtr
	}
	payload, ok := (*payloadMap)[filteredPayload.SourceBucketName]
	if !ok {
		unlockFunc()
		err := fmt.Errorf("Unable to find payload given bucket key %v", filteredPayload.SourceBucketName)
		ckmgr.logger.Errorf(err.Error())
		return err
	}

	var combinedBrokenMapping []*metadata.CollectionNsMappingsDoc
	mainCkptDocs, err := payload.GetAllCheckpoints(common.MainPipeline)
	if err != nil {
		return err
	}
	backfillCkptDocs, err := payload.GetAllCheckpoints(common.BackfillPipeline)
	if err != nil {
		return err
	}
	if mainCkptDocs == nil && backfillCkptDocs == nil {
		unlockFunc()
		err := fmt.Errorf("periodic push payload for %v has both nil main and backfill ckpt docs", periodicPayload.ReplicationSpecId)
		ckmgr.logger.Errorf(err.Error())
		return err
	}

	srcManifests, tgtManifests := payload.GetAllManifests()
	brokenMap := payload.GetBrokenMappingDoc()
	var brokenMapSpecInternalId string
	if brokenMap != nil {
		combinedBrokenMapping = append(combinedBrokenMapping, brokenMap)
		brokenMapSpecInternalId = brokenMap.SpecInternalId
	}
	unlockFunc()

	if brokenMapSpecInternalId != "" && brokenMapSpecInternalId != genSpec.GetReplicationSpec().InternalId {
		err := fmt.Errorf("mismatch - brokenMapID is %v while spec's internal ID is %v", brokenMapSpecInternalId,
			genSpec.GetReplicationSpec().InternalId)
		ckmgr.logger.Errorf(err.Error())
		return err
	}

	filteredMaps := filterCkptsBasedOnPipelineReinitHash([]metadata.VBsCkptsDocMap{mainCkptDocs, backfillCkptDocs}, ckmgr.pipelineReinitHash)

	pipelinesCkpts, shaMap, err := filterCkptsWithoutValidBrokenmaps(filteredMaps, combinedBrokenMapping)
	if err != nil {
		err := fmt.Errorf("filterCkptsWithoutValidBrokenMaps - %v", err)
		ckmgr.logger.Errorf(err.Error())
		return err
	}

	err, done := ckmgr.preStopGcCheck(pipelinesCkpts)
	if done {
		return err
	}

	respCh := make(chan error)
	err = ckmgr.requestPeriodicMerge(pipelinesCkpts, shaMap, brokenMapSpecInternalId, srcManifests, tgtManifests, respCh, payload.GlobalInfoDoc)
	if err != nil {
		return err
	}

	select {
	case retErr := <-respCh:
		return retErr
	case <-ckmgr.finish_ch:
		return fmt.Errorf("%v is stopped and unable to handle merge", ckmgr.Id())
	}
}

func (ckmgr *CheckpointManager) requestPeriodicMerge(pipelinesCkpts []metadata.VBsCkptsDocMap, shaMap metadata.ShaToCollectionNamespaceMap, brokenMapSpecInternalId string, srcManifests *metadata.ManifestsCache, tgtManifests *metadata.ManifestsCache, respCh chan error, globalInfo *metadata.GlobalInfoCompressedDoc) error {
	ckmgr.periodicPushDedupMtx.Lock()
	if ckmgr.periodicPushDedupArg == nil {
		// This means that there is no back pressure happening and this periodic push request is the first one to be merged
		// for this pipeline
		ckmgr.periodicPushDedupArg = &MergeCkptArgs{
			PipelinesCkptDocs:       pipelinesCkpts,
			BrokenMappingShaMap:     shaMap,
			BrokenMapSpecInternalId: brokenMapSpecInternalId,
			SrcManifests:            srcManifests,
			TgtManifests:            tgtManifests,
			SrcFailoverLogs:         nil,
			TgtFailoverLogs:         nil,
			PushRespChs:             []chan error{respCh},
		}

		if globalInfo != nil {
			globalInfoShaMap, err := globalInfo.ToShaMap()
			if err != nil {
				ckmgr.periodicPushDedupMtx.Unlock()
				return err
			}
			ckmgr.periodicPushDedupArg.GlobalInfoShaMap = globalInfoShaMap
		}

	} else {
		// This means that there is a push request already in place and haven't gotten to execute yet
		if brokenMapSpecInternalId != "" && ckmgr.periodicPushDedupArg.BrokenMapSpecInternalId != "" &&
			brokenMapSpecInternalId != ckmgr.periodicPushDedupArg.BrokenMapSpecInternalId {
			ckmgr.periodicPushDedupMtx.Unlock()
			return fmt.Errorf("BrokenMapSpecInternalId mismatch: %v vs %v", brokenMapSpecInternalId, ckmgr.periodicPushDedupArg.BrokenMapSpecInternalId)
		}
		// Since push requests are sent with payload that should only contain VB master information,
		// if multiple nodes push to this node at the same time, the following merges should take care of non-intersecting
		// information.
		// If the same VB master pushed before and is re-pushing again because this node is slow, then
		// the merge functions below should merge when necessary (i.e. shaMap) or replace when necessary (pipelinesCkptDocs)
		ckmgr.periodicPushDedupArg.PipelinesCkptDocs.Merge(pipelinesCkpts)
		ckmgr.periodicPushDedupArg.BrokenMappingShaMap.Merge(shaMap)
		ckmgr.periodicPushDedupArg.SrcManifests.LoadIfNotExists(srcManifests)
		ckmgr.periodicPushDedupArg.TgtManifests.LoadIfNotExists(tgtManifests)
		if globalInfo != nil {
			incomingShaMap, err := globalInfo.ToShaMap()
			if err != nil {
				ckmgr.periodicPushDedupMtx.Unlock()
				return err
			}
			if ckmgr.periodicPushDedupArg.GlobalInfoShaMap == nil {
				ckmgr.periodicPushDedupArg.GlobalInfoShaMap = incomingShaMap
			} else {
				ckmgr.periodicPushDedupArg.GlobalInfoShaMap.Load(incomingShaMap)
			}
		}
		ckmgr.periodicPushDedupArg.PushRespChs = append(ckmgr.periodicPushDedupArg.PushRespChs, respCh)
	}
	ckmgr.periodicPushDedupMtx.Unlock()

	select {
	case <-ckmgr.finish_ch:
		// the periodic pusher is probably not working anymore
		return fmt.Errorf("%v is stopping and unable to handle merge", ckmgr.Id())
	case ckmgr.periodicPushRequested <- true:
	// First one requested
	default:
		// already delivered
	}
	return nil
}

func (ckmgr *CheckpointManager) getLatestVbList() ([]uint16, metadata.GenericSpecification, error) {
	if ckmgr.pipeline == nil {
		return nil, nil, base.ErrorNilPipeline
	}
	stopFunc := ckmgr.utils.StartDiagStopwatch("ckmgr.getLatestVBList", base.DiagInternalThreshold)
	defer stopFunc()
	genSpec := ckmgr.pipeline.Specification()
	if genSpec == nil {
		return nil, nil, fmt.Errorf("Unable to find genSpec")
	}
	notificationCh, err := ckmgr.bucketTopologySvc.SubscribeToLocalBucketFeed(genSpec.GetReplicationSpec(), ckmgr.bucketTopologySubscriberId)
	if err != nil {
		return nil, nil, err
	}
	notification := <-notificationCh
	defer notification.Recycle()
	roMap := notification.GetSourceVBMapRO()
	vbList := roMap.GetSortedVBList()
	ckmgr.bucketTopologySvc.UnSubscribeLocalBucketFeed(genSpec.GetReplicationSpec(), ckmgr.bucketTopologySubscriberId)
	return vbList, genSpec, nil
}

func (ckmgr *CheckpointManager) getMaxSeqno(vbno uint16) uint64 {
	ckmgr.backfillTsMtx.RLock()
	defer ckmgr.backfillTsMtx.RUnlock()
	endTs, exists := ckmgr.backfillEndingTs[vbno]
	if !exists {
		return math.MaxUint64
	} else {
		return endTs.Seqno
	}
}

func (ckmgr *CheckpointManager) periodicBatchGetter() *MergeCkptArgs {
	ckmgr.periodicPushDedupMtx.Lock()
	defer ckmgr.periodicPushDedupMtx.Unlock()

	if ckmgr.periodicPushDedupArg == nil {
		return nil
	}

	// Do a copy to be able to free up the batch pointer
	returnedArg := &MergeCkptArgs{}
	*returnedArg = *ckmgr.periodicPushDedupArg
	// Clear the current batch so the next caller will start a new batch
	ckmgr.periodicPushDedupArg = nil
	return returnedArg
}

func (ckmgr *CheckpointManager) periodicMergerImpl() {
	for {
		select {
		case <-ckmgr.finish_ch:
			return
		case <-ckmgr.periodicPushRequested:
			// It is possible that backfill pipeline has checkpoints to be merged
			// When backfill pipeline checkpoints are being merged, it can throw off the
			// brokenMap shap ref counter embedded as part of the checkpoint service
			// If backfill pipeline were stopped, this would not be a problem, but we cannot always
			// stop backfill pipelines whenever a peer push comes, and main pipeline's ckptMgr has no way to contact
			// the backfill ckptmgr, and the backfill ckptmgr may be outdated if another new backfill pipeline is
			// constructed
			// Since 1) checkpoint service is shared between both main pipeline and backfill pipeline's
			// checkpoint manager, and 2) only the main pipeline checkpoint manager performs the merges,
			// the main checkpoint manager is responsible to tell the ckptService to not let the backfill
			// ckptMgr from decrementing the ref count while the merge is taking place
			backfillTopic := common.ComposeFullTopic(ckmgr.pipeline.Topic(), common.BackfillPipeline)
			if ckmgr.isVariableVBMode() {
				// Variable VB mode requires the use of global timestamp shas
				// This means that main pipeline should follow the process of disabling ref counting
				// to avoid situations where shas could be pre-emptively removed
				ckmgr.checkpoints_svc.DisableRefCntDecrement(ckmgr.pipeline.Topic())
			}
			ckmgr.checkpoints_svc.DisableRefCntDecrement(backfillTopic)

			// Once backfill ckptMgr cannot decrement the ref count, then do the merges, which ensures
			// that whatever is being merged (incl broken maps) cannot be deleted from underneath since
			// the decrementing ref-cnt for brokenMaps is disabled
			// Recall that each broken map can be referred to by many ckpts, and each ref is considered
			// a count and prevents the broken map entity from being cleaned up, and the ref decrement
			// call occurs during ckpt operation
			err := ckmgr.stopTheWorldAndMergeCkpts(ckmgr.periodicBatchGetter)
			if err != nil {
				ckmgr.logger.Errorf("%v stopTheWorldAndMerge - %v", ckmgr.Id(), err)
			}

			// Once the merge has finished, re-enable the decrementer
			// It is quite possible that the ref count could run high (i.e ref to a brokenmap seems to have
			// ckpts referring to it even though there is none)
			// But ckptService should have a global re-count every time a periodic merge occurs, which can then
			// do the clean up at the re-count time if counts are mismatched
			if ckmgr.isVariableVBMode() {
				ckmgr.checkpoints_svc.EnableRefCntDecrement(ckmgr.pipeline.Topic())
			}
			ckmgr.checkpoints_svc.EnableRefCntDecrement(backfillTopic)
		}
	}
}

func (ckmgr *CheckpointManager) getRemoteGlobalTimestamp(highSeqnoAndVBUuid metadata.GlobalTargetVbUuids, globalTargetOpaque metadata.TargetVBOpaque) (metadata.TargetTimestamp, error) {
	if highSeqnoAndVBUuid == nil {
		return nil, fmt.Errorf("%v:  getRemoteGlobalTimestamp:highSeqnoAndVBUuid", base.ErrorNilPtr.Error())
	}

	if globalTargetOpaque == nil {
		return nil, fmt.Errorf("%v:  getRemoteGlobalTimestamp:globalTargetOpaque", base.ErrorNilPtr.Error())
	}

	// first check if vbuuid has changed or not
	if !highSeqnoAndVBUuid.IsSame(globalTargetOpaque) {
		ckmgr.logger.Errorf("target vbuuid has changed: old=%v, new=%v", globalTargetOpaque.Value(), highSeqnoAndVBUuid)
		return nil, targetVbuuidChangedError
	}

	return highSeqnoAndVBUuid.ToGlobalTimestamp(), nil
}

// ClearBrokenmapHistory would remove unreferenced broken maps
// tgtManfiestIdsUsed is passed in to show manifest IDs that are still being referred
func (ckmgr *CheckpointManager) ClearBrokenmapHistory(tgtManifestIdsUsed map[uint16]uint64) {
	ckmgr.cachedBrokenMap.lock.Lock()
	defer ckmgr.cachedBrokenMap.lock.Unlock()

	tgtManifestIdUsedDedupMap := make(map[uint64]bool)
	for _, v := range tgtManifestIdsUsed {
		tgtManifestIdUsedDedupMap[v] = true
	}

	for manifestId, _ := range ckmgr.cachedBrokenMap.brokenMapHistories {
		if manifestId == ckmgr.cachedBrokenMap.correspondingTargetManifest {
			// This entry should not belong in the "history" as this entry is already captured by "current" broken map
			delete(ckmgr.cachedBrokenMap.brokenMapHistories, manifestId)
		} else if _, exists := tgtManifestIdUsedDedupMap[manifestId]; !exists {
			delete(ckmgr.cachedBrokenMap.brokenMapHistories, manifestId)
		}
	}
}

func (ckmgr *CheckpointManager) filterInvalidCkptsBasedOnPreReplicate(ckptsMaps []metadata.VBsCkptsDocMap) []metadata.VBsCkptsDocMap {
	preReplicateCtx := ckmgr.newGlobalCkptPrereplicateCacheCtx(time.Duration(base.GlobalPreReplicateCacheExpireTimeSecs)*time.Second,
		time.Duration(base.GlobalPreReplicateCacheErrorExpireTimeSecs)*time.Second)

	mainPipelineMap := make(metadata.VBsCkptsDocMap)
	backfillPipelineMap := make(metadata.VBsCkptsDocMap)
	var combinedMap metadata.VBsCkptsDocMap

	for i, oneCkptsMap := range ckptsMaps {
		switch i {
		case int(common.MainPipeline):
			combinedMap = mainPipelineMap
		case int(common.BackfillPipeline):
			combinedMap = backfillPipelineMap
		default:
			panic(fmt.Sprintf("wrong integer %v", i))
		}
		if oneCkptsMap == nil {
			continue
		}
		for vbno, ckptDoc := range oneCkptsMap {
			_, exists := combinedMap[vbno]
			if !exists {
				// It's ok for checkpointRecords to be emptyList
				combinedMap[vbno] = ckptDoc.CloneWithoutRecords()
			}
			for _, ckptRecord := range ckptDoc.Checkpoint_records {
				if ckptRecord == nil {
					continue
				}

				bMatch, err := ckmgr.IsCkptRecordValidBasedOnPreReplicate(ckptRecord, preReplicateCtx, vbno)
				if err != nil {
					continue
				}
				if bMatch {
					combinedMap[vbno].Checkpoint_records = append(combinedMap[vbno].Checkpoint_records, ckptRecord)
				}
			}
		}
	}
	return []metadata.VBsCkptsDocMap{mainPipelineMap, backfillPipelineMap}
}

// The failover log is a list of (vbuuid, start_seqno) pairs, where each entry
// marks the point a vBucket UUID (vbuuid) took ownership. For example:
// [(Y, 900), (X, 500), (W, 0)]. The most recent entry is Y.
//
// prepareForLocalPreReplicate function transforms this into (vbuuid, end_seqno)
// pairs to indicate the seqno at which each vbuuid's ownership ends:
// (Y, INT_MAX), (X, 900), (W, 500). INT_MAX is used for the currently active vbuuid.
//
// This transformation allows us to validate checkpoints. For example:
//   - Suppose a checkpoint contains the pair (W, 501). This should be rejected
//     because ownership for W ended at sequence number 500. Since X took over
//     from W at seqno 500, only mutations with seqno ≤ 500 are guaranteed to be present in X.
//   - A checkpoint with entry (X, 899) should be accepted since X's ownership ended
//     at 900, meaning all seqnos <= 900 are guaranteed to be present in Y.
func prepareForLocalPreReplicate(failoverLogPtr *mcc.FailoverLog) (*mcc.FailoverLog, error) {
	if failoverLogPtr == nil || len(*failoverLogPtr) == 0 {
		return nil, base.ErrorInvalidInput
	}

	failoverLog := *failoverLogPtr
	n := len(failoverLog)
	result := make(mcc.FailoverLog, n)

	for i := n - 1; i > 0; i-- {
		entry := [2]uint64{failoverLog[i][0], failoverLog[i-1][1]}
		result[i] = entry
	}

	lastEntry := [2]uint64{failoverLog[0][0], math.MaxUint64}
	result[0] = lastEntry

	return &result, nil
}

// its the callers responsibility to ensure that the parameters passed in are valid
func localPreReplicate(failoverLog *mcc.FailoverLog, commitOpaque [2]uint64) bool {
	commitUUID := commitOpaque[0]
	commitSeqno := commitOpaque[1]

	for _, entry := range *failoverLog {
		uuid := entry[0]
		endSeqno := entry[1]
		if commitUUID == uuid && commitSeqno <= endSeqno {
			return true
		}
	}
	return false
}

func IsCkptRecordValidBasedOnTgtFailover(ckptRecord *metadata.CheckpointRecord, failoverLog *mcc.FailoverLog) bool {
	if ckptRecord == nil {
		return false
	}

	if ckptRecord.IsTraditional() {
		ckptCommitOpaque := [2]uint64{
			ckptRecord.Target_vb_opaque.Value().(uint64),
			ckptRecord.Target_Seqno,
		}
		return localPreReplicate(failoverLog, ckptCommitOpaque)
	}

	for _, tgtTs := range ckptRecord.GlobalTimestamp {
		tgtCommitOpaque := [2]uint64{
			tgtTs.Target_vb_opaque.Value().(uint64),
			tgtTs.Target_Seqno,
		}
		if !localPreReplicate(failoverLog, tgtCommitOpaque) {
			return false
		}
	}
	return true
}

func (ckmgr *CheckpointManager) IsCkptRecordValidBasedOnPreReplicate(ckptRecord *metadata.CheckpointRecord, preReplicateCtx *globalCkptPrereplicateCacheCtx, vbno uint16) (bool, error) {
	if ckptRecord == nil {
		return false, nil
	}

	if ckptRecord.IsTraditional() {
		targetTimestamp := &service_def.RemoteVBReplicationStatus{
			VBOpaque: ckptRecord.Target_vb_opaque,
			VBSeqno:  ckptRecord.Target_Seqno,
			VBNo:     vbno,
		}
		bmatch, _, err := ckmgr.capi_svc.PreReplicate(ckmgr.remote_bucket, targetTimestamp, true)
		if err != nil {
			ckmgr.logger.Errorf("IsCkptRecordValidBasedOnPreReplicate targetTimeStamp (%v,%v,%v) error: %v",
				vbno, ckptRecord.Target_vb_opaque, ckptRecord.Target_Seqno, err)
		}
		return bmatch, err
	}
	for tgtno, tgtTs := range ckptRecord.GlobalTimestamp {
		targetTimestamp := &service_def.RemoteVBReplicationStatus{
			VBOpaque: tgtTs.Target_vb_opaque,
			VBSeqno:  tgtTs.Target_Seqno,
			VBNo:     tgtno,
		}
		bmatch, _, err := preReplicateCtx.preReplicate(ckmgr.remote_bucket, targetTimestamp)
		if err != nil {
			ckmgr.logger.Errorf("IsCkptRecordValidBasedOnPreReplicate tgtTs (%v,%v,%v) error: %v",
				tgtno, tgtTs.Target_vb_opaque, tgtTs.Target_Seqno, err)
			return false, err
		}
		if !bmatch {
			return false, nil
		}
	}
	return true, nil
}

func (ckmgr *CheckpointManager) filterBackfillCheckpointRecordsOnColIDs(vbno uint16, ckptRecord *metadata.CheckpointRecord) (skipCkptRec bool) {
	if ckmgr.pipeline.Type() != common.BackfillPipeline {
		return
	}

	ckmgr.backfillCollectionsMtx.RLock()
	defer ckmgr.backfillCollectionsMtx.RUnlock()
	currBackfillColIDs, ok := ckmgr.backfillCollections[vbno]
	if !ok || currBackfillColIDs == nil {
		// shouldn't happen, skip ckpt_record to be sure
		ckmgr.logger.Debugf("Backfill ColIDs was not initialised for vbno=%v, ok=%v", vbno, ok)
		skipCkptRec = true
		return
	}

	skipCkptRec = ckptRecord.FilterBasedOnBackfillCollections(currBackfillColIDs)
	return
}

// If ckmgr belongs a backfill pipeline, we will parse the top tasks, so that checkpointing is aware of
// which collections the backfill pipeline is running for.
func (ckmgr *CheckpointManager) setBackfillCollections() error {
	if ckmgr.pipeline.Type() != common.BackfillPipeline {
		return nil
	}

	settings := ckmgr.pipeline.Settings()

	vbTasksRaw, exists := settings[parts.DCP_VBTasksMap]
	if !exists || vbTasksRaw == nil {
		// odd for backfill pipeline
		ckmgr.logger.Warnf("Ckmgr didn't receive any backfill tasks")
		return nil
	}

	specificVBTasks := vbTasksRaw.(*metadata.VBTasksMapType)
	if specificVBTasks.IsNil() || specificVBTasks.Len() == 0 {
		// odd for backfill pipeline
		ckmgr.logger.Warnf("Ckmgr didn't see any backfill tasks")
		return nil
	}

	// Fetch the latest source manifest to check if the collections to be backfilled are still present on source bucket.
	replSpec := ckmgr.pipeline.Specification().GetReplicationSpec()
	latestSrcManifest, err := ckmgr.collectionsManifestSvc.GetSpecificSourceManifest(replSpec, math.MaxUint64)
	if err != nil {
		return err
	}

	ckmgr.backfillCollectionsMtx.Lock()
	defer ckmgr.backfillCollectionsMtx.Unlock()

	for _, vbno := range ckmgr.getMyVBs() {
		vbTasks, exists, unlockSpecificVBTasksMap := specificVBTasks.Get(vbno, false)
		if !exists || vbTasks == nil || vbTasks.Len() == 0 {
			unlockSpecificVBTasksMap()
			continue
		}

		// This step is expected to yield the same collection filter as got in dcp nozzle during starting streams.
		topTask, _, unlockVbTasks := vbTasks.GetRO(0)
		_, filter, toTaskErrs := topTask.ToDcpNozzleTask(latestSrcManifest)
		unlockVbTasks()
		unlockSpecificVBTasksMap()

		if toTaskErrs != nil {
			// it mostly means that the source manifest changed to not have some collections for the backfill task. In a steady
			// state, this tasks will be eventually be removed from the backfill spec and this error should fix itself after some
			// pipeline restarts.
			err := fmt.Errorf("ckmgr couldn't convert VBTask to DCP Nozzle Task %v", toTaskErrs)
			return err
		}

		// sort the list for checkpoint manager to search items in the list easily
		sort.Sort(base.Uint32List(filter.CollectionsList))
		ckmgr.backfillCollections[vbno] = append(ckmgr.backfillCollections[vbno], filter.CollectionsList...)
	}

	ckmgr.logger.Infof("Backfilling collections: %v", ckmgr.backfillCollections)
	return nil
}
