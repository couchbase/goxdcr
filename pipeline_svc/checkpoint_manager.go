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
	"github.com/couchbase/goxdcr/peerToPeer"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	component "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
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

	pipeline                   common.Pipeline
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

	target_username    string
	target_password    string
	target_bucket_name string

	target_cluster_ref *metadata.RemoteClusterReference

	// key = kv host name, value = ssl connection str
	ssl_con_str_map  map[string]string
	target_kv_vb_map base.KvVBMapType

	user_agent string

	// these fields are used for xmem replication only
	// memcached clients for retrieval of target bucket stats
	kv_mem_clients      map[string]mcc.ClientIface
	kv_mem_clients_lock sync.RWMutex

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

	initConnOnce sync.Once
	initConnDone chan bool
	initConnErr  error

	bucketTopologySvc service_def.BucketTopologySvc

	getHighSeqnoAndVBUuidFromTargetCh chan bool

	// Only used to bypass all the un-mockable RPC calls
	unitTest bool

	xdcrDevInjectGcWaitSec uint32

	periodicPushRequested chan bool

	periodicPushDedupMtx sync.RWMutex
	periodicPushDedupArg *MergeCkptArgs
	periodicMerger       periodicMergerType // can be replaced for unit test
}

type checkpointSyncHelper struct {
	checkpointAllowed bool
	ongoingOps        []bool // mark false when starting, true once done

	mtx sync.RWMutex
	cv  sync.Cond
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

func (h *checkpointSyncHelper) registerCkptOp(setVBTimestamp bool) (int, error) {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	if !setVBTimestamp && !h.checkpointAllowed {
		return -1, fmt.Errorf("cannot register ckptOp because checkpoint op is not currently allowed")
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

// Checkpoint Manager keeps track of one checkpointRecord per vbucket
type checkpointRecordWithLock struct {
	ckpt *metadata.CheckpointRecord
	lock *sync.RWMutex
	// Used to keep track internally of whether or not a write has occured while lock was released
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

func NewCheckpointManager(checkpoints_svc service_def.CheckpointsService, capi_svc service_def.CAPIService,
	remote_cluster_svc service_def.RemoteClusterSvc, rep_spec_svc service_def.ReplicationSpecSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	through_seqno_tracker_svc service_def.ThroughSeqnoTrackerSvc, active_vbs map[string][]uint16, target_username,
	target_password, target_bucket_name string, target_kv_vb_map base.KvVBMapType,
	target_cluster_ref *metadata.RemoteClusterReference, logger_ctx *log.LoggerContext, utilsIn utilities.UtilsIface,
	statsMgr service_def.StatsMgrIface, uiLogSvc service_def.UILogSvc,
	collectionsManifestSvc service_def.CollectionsManifestSvc, backfillReplSvc service_def.BackfillReplSvc,
	getBackfillMgr func() service_def.BackfillMgrIface,
	bucketTopologySvc service_def.BucketTopologySvc) (*CheckpointManager, error) {
	if checkpoints_svc == nil || capi_svc == nil || remote_cluster_svc == nil || rep_spec_svc == nil || xdcr_topology_svc == nil {
		return nil, errors.New("checkpoints_svc, capi_svc, remote_cluster_svc, rep_spec_svc, cluster_info_svc and xdcr_topology_svc can't be nil")
	}
	logger := log.NewLogger("CheckpointMgr", logger_ctx)

	ckmgr := &CheckpointManager{
		AbstractComponent:                 component.NewAbstractComponentWithLogger(CheckpointMgrId, logger),
		pipeline:                          nil,
		checkpoints_svc:                   checkpoints_svc,
		capi_svc:                          capi_svc,
		rep_spec_svc:                      rep_spec_svc,
		remote_cluster_svc:                remote_cluster_svc,
		xdcr_topology_svc:                 xdcr_topology_svc,
		through_seqno_tracker_svc:         through_seqno_tracker_svc,
		finish_ch:                         make(chan bool, 1),
		checkpoint_ticker_ch:              make(chan *time.Ticker, 1000),
		logger:                            logger,
		cur_ckpts:                         make(map[uint16]*checkpointRecordWithLock),
		active_vbs:                        active_vbs,
		target_username:                   target_username,
		target_password:                   target_password,
		target_bucket_name:                target_bucket_name,
		target_kv_vb_map:                  target_kv_vb_map,
		wait_grp:                          &sync.WaitGroup{},
		failoverlog_map:                   make(map[uint16]*failoverlogWithLock),
		snapshot_history_map:              make(map[uint16]*snapshotHistoryWithLock),
		kv_mem_clients:                    make(map[string]mcc.ClientIface),
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
		initConnDone:                      make(chan bool),
		getHighSeqnoAndVBUuidFromTargetCh: make(chan bool, 1),
		periodicPushRequested:             make(chan bool, 1),
	}

	// So that unit test can override this and test it
	ckmgr.periodicMerger = ckmgr.periodicMergerImpl
	return ckmgr, nil
}

func (ckmgr *CheckpointManager) Attach(pipeline common.Pipeline) error {

	ckmgr.logger.Infof("Attach checkpoint manager with %v pipeline %v\n", pipeline.Type().String(), pipeline.FullTopic())

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
	ckmgr.srcBucketName = ckmgr.pipeline.Specification().GetReplicationSpec().SourceBucketName

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
		router := dcp.Connector()
		router.RegisterComponentEventListener(common.BrokenRoutingUpdateEvent, ckmgr)
	}

	//register pipeline supervisor as ckmgr's error handler
	supervisor := ckmgr.pipeline.RuntimeContext().Service(base.PIPELINE_SUPERVISOR_SVC)
	if supervisor == nil {
		return errors.New("Pipeline supervisor has to exist")
	}
	err = ckmgr.RegisterComponentEventListener(common.ErrorEncountered, supervisor.(*PipelineSupervisor))
	if err != nil {
		return err
	}
	err = ckmgr.RegisterComponentEventListener(common.VBErrorEncountered, supervisor.(*PipelineSupervisor))
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
		defer ckmgr.wait_grp.Done()
		return ckmgr.initConnections()
	}
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
	ckmgr.logger.Infof("%v %v Checkpointing starts in %v sec", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), starting_time.Seconds())
	ckmgr.checkpoint_ticker_ch <- time.NewTicker(starting_time)
}

func (ckmgr *CheckpointManager) initialize() {
	listOfVbs := ckmgr.getMyVBs()
	for _, vbno := range listOfVbs {
		ckmgr.cur_ckpts[vbno] = &checkpointRecordWithLock{ckpt: &metadata.CheckpointRecord{}, lock: &sync.RWMutex{}}
		ckmgr.failoverlog_map[vbno] = &failoverlogWithLock{failoverlog: nil, lock: &sync.RWMutex{}}
		ckmgr.snapshot_history_map[vbno] = &snapshotHistoryWithLock{
			snapshot_history: make([]*snapshot, 0, base.MaxLengthSnapshotHistory),
		}
	}

	ckmgr.composeUserAgent()

	ckmgr.cachedBrokenMap.lock.Lock()
	ckmgr.cachedBrokenMap.brokenMap = make(metadata.CollectionNamespaceMapping)
	ckmgr.cachedBrokenMap.lock.Unlock()

	ckmgr.getHighSeqnoAndVBUuidFromTargetCh <- true
}

var ckmgrIterationId uint32

// compose user agent string for HELO command
func (ckmgr *CheckpointManager) composeUserAgent() {
	spec := ckmgr.pipeline.Specification().GetReplicationSpec()
	ckmgr.user_agent = base.ComposeUserAgentWithBucketNames("Goxdcr CkptMgr", spec.SourceBucketName, spec.TargetBucketName)
	ckmgr.bucketTopologySubscriberId = fmt.Sprintf("%v_%v_%v_%v", "ckptMgr", ckmgr.pipeline.Type().String(), ckmgr.pipeline.InstanceId(), base.GetIterationId(&ckmgrIterationId))
}

func (ckmgr *CheckpointManager) initConnections() error {
	ckmgr.initConnOnce.Do(func() {
		defer close(ckmgr.initConnDone)
		if ckmgr.target_cluster_ref.IsFullEncryption() {
			err := ckmgr.initSSLConStrMap()
			if err != nil {
				ckmgr.logger.Errorf("%v %v failed to initialize ssl connection string map, err=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), err)
				ckmgr.initConnErr = err
				return
			}
		}

		for server_addr, _ := range ckmgr.target_kv_vb_map {
			client, err := ckmgr.getNewMemcachedClient(server_addr, true /*initializing*/)
			if err != nil {
				ckmgr.logger.Errorf("%v %v failed to construct memcached client for %v, err=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), server_addr, err)
				ckmgr.initConnErr = err
				return
			}
			ckmgr.kv_mem_clients_lock.Lock()
			ckmgr.kv_mem_clients[server_addr] = client
			ckmgr.kv_mem_clients_lock.Unlock()
		}
		return
	})

	ckmgr.waitForInitConnDone()
	return ckmgr.initConnErr
}

func (ckmgr *CheckpointManager) waitForInitConnDone() {
	select {
	case <-ckmgr.initConnDone:
		return
	}
}

func (ckmgr *CheckpointManager) initSSLConStrMap() error {
	connStr, err := ckmgr.target_cluster_ref.MyConnectionStr()
	if err != nil {
		return err
	}

	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := ckmgr.target_cluster_ref.MyCredentials()
	if err != nil {
		return err
	}

	useExternal, err := ckmgr.remote_cluster_svc.ShouldUseAlternateAddress(ckmgr.target_cluster_ref)
	if err != nil {
		return err
	}

	ssl_port_map, err := ckmgr.utils.GetMemcachedSSLPortMap(connStr, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey,
		ckmgr.target_bucket_name, ckmgr.logger, useExternal)
	if err != nil {
		return err
	}

	ckmgr.ssl_con_str_map = make(map[string]string)
	for server_addr, _ := range ckmgr.target_kv_vb_map {
		ssl_port, ok := ssl_port_map[server_addr]
		if !ok {
			return fmt.Errorf("Can't get remote memcached ssl port for %v", server_addr)
		}
		host_name := base.GetHostName(server_addr)
		ssl_con_str := base.GetHostAddr(host_name, uint16(ssl_port))
		ckmgr.ssl_con_str_map[server_addr] = ssl_con_str
	}

	return nil
}

func (ckmgr *CheckpointManager) getNewMemcachedClient(server_addr string, initializing bool) (mcc.ClientIface, error) {
	if ckmgr.target_cluster_ref.IsFullEncryption() {
		_, _, _, certificate, san_in_certificate, client_certificate, client_key, err := ckmgr.target_cluster_ref.MyCredentials()
		if err != nil {
			return nil, err
		}
		ssl_con_str := ckmgr.ssl_con_str_map[server_addr]

		if !initializing {
			// if not initializing at replication startup time, retrieve up to date security settings
			latestTargetClusterRef, err := ckmgr.remote_cluster_svc.RemoteClusterByUuid(ckmgr.target_cluster_ref.Uuid(), false)
			if err != nil {
				return nil, err
			}
			connStr, err := latestTargetClusterRef.MyConnectionStr()
			if err != nil {
				return nil, err
			}
			// hostAddr not used in full encryption mode
			_, _, _, err = ckmgr.utils.GetSecuritySettingsAndDefaultPoolInfo("" /*hostAddr*/, connStr,
				ckmgr.target_username, ckmgr.target_password, certificate, client_certificate, client_key, false /*scramShaEnabled*/, ckmgr.logger)
			if err != nil {
				return nil, err
			}
		}
		return base.NewTLSConn(ssl_con_str, ckmgr.target_username, ckmgr.target_password, certificate, san_in_certificate, client_certificate, client_key, ckmgr.target_bucket_name, ckmgr.logger)
	} else {
		return ckmgr.utils.GetRemoteMemcachedConnection(server_addr, ckmgr.target_username, ckmgr.target_password,
			ckmgr.target_bucket_name, ckmgr.user_agent, !ckmgr.target_cluster_ref.IsEncryptionEnabled(), /*plain_auth*/
			base.KeepAlivePeriod, ckmgr.logger)
	}
}

func (ckmgr *CheckpointManager) closeConnections() {
	ckmgr.waitForInitConnDone()
	ckmgr.kv_mem_clients_lock.Lock()
	defer ckmgr.kv_mem_clients_lock.Unlock()
	for server_addr, client := range ckmgr.kv_mem_clients {
		err := client.Close()
		if err != nil {
			ckmgr.logger.Warnf("%v %v error from closing connection for %v is %v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), server_addr, err)
		}
	}
	ckmgr.kv_mem_clients = make(map[string]mcc.ClientIface)
}

// If periodicMode is false, there should not be any error returned
func (ckmgr *CheckpointManager) getHighSeqnoAndVBUuidFromTarget(fin_ch chan bool, periodicMode bool) (map[uint16][]uint64, error) {
	ckmgr.waitForInitConnDone()

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
	for serverAddr, vbnos := range ckmgr.target_kv_vb_map {
		waitGrp.Add(1)
		go ckmgr.getTargetKVStatsMapWithRetry(serverAddr, vbnos, fin_ch, serverClientStatsMap, &serverClientStatsMapMtx, &waitGrp)
	}
	waitGrp.Wait()

	// A map of vbucketID -> slice of 2 elements of 1)HighSeqNo and 2)VbUuid in that order
	high_seqno_and_vbuuid_map := make(map[uint16][]uint64)
	for serverAddr, vbnos := range ckmgr.target_kv_vb_map {
		if _, found := serverClientStatsMap[serverAddr]; !found {
			continue
		}
		ckmgr.utils.ParseHighSeqnoAndVBUuidFromStats(vbnos, serverClientStatsMap[serverAddr], high_seqno_and_vbuuid_map)
	}
	ckmgr.logger.Infof("high_seqno_and_vbuuid_map=%v\n", high_seqno_and_vbuuid_map)
	return high_seqno_and_vbuuid_map, nil
}

// checkpointing cannot be done without high seqno and vbuuid from target
// if retrieval of such stats fails, retry
func (ckmgr *CheckpointManager) getTargetKVStatsMapWithRetry(serverAddr string, vbnos []uint16, fin_ch chan bool,
	serverClientStatsMap map[string]base.StringStringMap, serverClientStatsMapMtx *sync.RWMutex, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	var stats_map map[string]string

	statMapOp := func(param interface{}) (interface{}, error) {
		var err error
		ckmgr.kv_mem_clients_lock.RLock()
		client, ok := ckmgr.kv_mem_clients[serverAddr]
		ckmgr.kv_mem_clients_lock.RUnlock()
		if !ok {
			// memcached connection may have been closed in previous retries. create a new one
			client, err = ckmgr.getNewMemcachedClient(serverAddr, false /*initializing*/)
			if err != nil {
				ckmgr.logger.Warnf("%v %v Retrieval of high seqno and vbuuid stats failed. serverAddr=%v, vbnos=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), serverAddr, vbnos)
				return nil, err
			} else {
				ckmgr.kv_mem_clients_lock.Lock()
				ckmgr.kv_mem_clients[serverAddr] = client
				ckmgr.kv_mem_clients_lock.Unlock()
			}
		}

		stats_map, err = client.StatsMap(base.VBUCKET_SEQNO_STAT_NAME)
		if err != nil {
			ckmgr.logger.Warnf("%v %v Error getting vbucket-seqno stats for serverAddr=%v. vbnos=%v, err=%v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), serverAddr, vbnos, err)
			clientCloseErr := client.Close()
			if clientCloseErr != nil {
				ckmgr.logger.Warnf("%v %v error from closing connection for %v is %v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), serverAddr, err)
			}
			ckmgr.kv_mem_clients_lock.Lock()
			delete(ckmgr.kv_mem_clients, serverAddr)
			ckmgr.kv_mem_clients_lock.Unlock()
		}
		return nil, err
	}

	opErr, _ := ckmgr.utils.ExponentialBackoffExecutorWithFinishSignal("StatsMapOnVBToSeqno", base.RemoteMcRetryWaitTime, base.MaxRemoteMcRetry,
		base.RemoteMcRetryFactor, statMapOp, nil, fin_ch)

	if opErr != nil {
		ckmgr.logger.Errorf("%v %v Retrieval of high seqno and vbuuid stats failed after %v retries. serverAddr=%v, vbnos=%v\n",
			ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), base.MaxRemoteMcRetry, serverAddr, vbnos)
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
	ckmgr.closeConnections()

	ckmgr.wait_grp.Wait()
	return nil
}
func (ckmgr *CheckpointManager) CheckpointBeforeStopWithWait(waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	ckmgr.CheckpointBeforeStop()
}
func (ckmgr *CheckpointManager) CheckpointBeforeStop() {
	if !ckmgr.isCheckpointAllowed() {
		ckmgr.logger.Errorf("%v %v has not been started (or has been disabled) - checkpointing is skipped", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
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
		panic(fmt.Sprintf("Unhandled type %v", ckmgr.pipeline.Type().String()))
	}

	if !specExists {
		// do not perform checkpoint if spec has been deleted
		ckmgr.logger.Infof("Skipping checkpointing for %v %v before stopping since replication spec has been deleted", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
		return
	}

	var opDoneIdx int
	err := fmt.Errorf("InitErr")
	registerCkptOpTimeStop := ckmgr.utils.StartDiagStopwatch(fmt.Sprintf("ckmgr.checkpointAllowedHelper.registerCkptOp(false) - %v", ckmgr.pipeline.FullTopic()), base.DiagInternalThreshold)
	for err != nil {
		opDoneIdx, err = ckmgr.checkpointAllowedHelper.registerCkptOp(false)
		time.Sleep(100 * time.Millisecond)
	}
	registerCkptOpTimeStop()
	defer ckmgr.checkpointAllowedHelper.markTaskDone(opDoneIdx)

	ckmgr.logger.Infof("Starting checkpointing for %v %v before stopping", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())

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
			ckmgr.logger.Infof("Checkpointing completed for %v %v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
			return
		case <-timeout_timer.C:
			close(close_ch)
			ckmgr.logger.Infof("Checkpointing timed out for %v %v after %v.", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), base.TimeoutCheckpointBeforeStop)
			// do not wait for ckmgr.PerformCkpt to complete, which could get stuck if call to target never comes back
			return
		}
	}
}

// get the lis of source vbuckets that this pipleline instance responsible.
// In current deployment - ReplicationManager coexist with source node, it means
// the list of buckets on that source node
func (ckmgr *CheckpointManager) getMyVBs() []uint16 {
	vbList := []uint16{}
	for _, vbs := range ckmgr.active_vbs {
		vbList = append(vbList, vbs...)
	}
	return vbList
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
	ckmgr.logger.Infof("%v %v Remote bucket %v supporting xdcrcheckpointing is %v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), ckmgr.remote_bucket, ckmgr.support_ckpt)
}

func (ckmgr *CheckpointManager) getCheckpointInterval() time.Duration {
	return time.Duration(atomic.LoadUint32(&ckmgr.ckpt_interval)) * time.Second
}

func (ckmgr *CheckpointManager) setCheckpointInterval(ckpt_interval int) {
	atomic.StoreUint32(&ckmgr.ckpt_interval, uint32(ckpt_interval))
	ckmgr.logger.Infof("%v %v set ckpt_interval to %v s\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), ckpt_interval)
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

// handle fatal error, raise error event to pipeline supervisor
func (ckmgr *CheckpointManager) handleGeneralError(err error) {
	ckmgr.logger.Error(err.Error())
	ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
}

func getDocsProcessedForReplication(topic string, vb_list []uint16, checkpoints_svc service_def.CheckpointsService,
	logger *log.CommonLogger) (uint64, error) {
	defer logger.Info("Done with GetDocsProcessedForReplication")
	logger.Infof("Start GetDocsProcessedForReplication for replication %v...", topic)

	logger.Debugf("Getting checkpoint for %v\n", topic)
	ckptDocs, err := checkpoints_svc.CheckpointsDocs(topic, false /*needBrokenMappings*/)
	logger.Debugf("Done getting checkpoint for %v\n", topic)
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
	opDoneIdx, err := ckmgr.checkpointAllowedHelper.registerCkptOp(true)
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
		ckmgr.checkpointAllowedHelper.setCheckpointAllowed()
	}()
	// Pipeline instances may overlap, so use instance ID instead
	ckmgr.logger.Infof("Set start seqnos for %v...", ckmgr.pipeline.InstanceId())

	listOfVbs := ckmgr.getMyVBs()

	// Get persisted checkpoints from metakv - each valid vbucket has a *metadata.CheckpointsDoc
	ckmgr.logger.Infof("Getting checkpoint for %v %v\n", ckmgr.pipeline.Type().String(), topic)
	ckptDocs, err := ckmgr.checkpoints_svc.CheckpointsDocs(topic, true /*needBrokenMapping*/)
	if err != nil {
		ckmgr.logger.Errorf("Getting checkpoint for %v had error %v", ckmgr.pipeline.FullTopic(), err)
		ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
		return err
	}
	ckmgr.loadCollectionMappingsFromCkptDocs(ckptDocs)
	ckmgr.logger.Infof("Found %v checkpoint documents for %v replication %v\n", len(ckptDocs), ckmgr.pipeline.Type().String(), topic)

	ckmgr.initBpMapIfNeeded(ckptDocs)

	deleted_vbnos := make([]uint16, 0)
	specInternalId := ckmgr.pipeline.Specification().GetReplicationSpec().InternalId

	genSpec := ckmgr.pipeline.Specification()
	spec := genSpec.GetReplicationSpec()
	if spec == nil {
		ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, fmt.Errorf("Unable to get spec - nil pointer")))
		return base.ErrorNilPtr
	}

	// Figure out if certain checkpoints need to be removed to force a complete resync due to external factors
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
				// if specInternalId does not match, replication spec has been deleted and recreated
				// the checkpoint doc is for the old replication spec and needs to be deleted
				// unlike the IsVbInList check above, it is critial for the checkpoint doc deletion to succeed here
				// restart pipeline if it fails
				err = ckmgr.checkpoints_svc.DelCheckpointsDoc(topic, vbno, ckmgr.pipeline.Specification().GetReplicationSpec().InternalId)
				if err != nil {
					ckmgr.logger.Errorf("Deleting checkpoint docs %v for vb %v had an err %v", topic, vbno, err)
					ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
					return err
				}
				deleted_vbnos = append(deleted_vbnos, vbno)
			}
		}
	}

	for _, deleted_vbno := range deleted_vbnos {
		delete(ckptDocs, deleted_vbno)
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

	for {
		end_index := start_index + workload
		if end_index > len(listOfVbs) {
			end_index = len(listOfVbs)
		}
		vbs_for_getter := listOfVbs[start_index:end_index]
		getter_wait_grp.Add(1)
		go ckmgr.startSeqnoGetter(getter_id, vbs_for_getter, ckptDocs, getter_wait_grp, err_ch, &lowestLastSuccessfulSourceManifestId, &vbtsStartedWithNon0)

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

	ckmgr.logger.Infof("Done with setting starting seqno for %v %v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
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

		ckmgr.logger.Infof("%v garbage collecting old checkpoint for vb %v", topic, vbno)
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
		ckmgr.logger.Errorf("Unable to register GC func for %v - %v due to %v", topic, vbno, err)
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
	var highestManifestId uint64
	ckmgr.cachedBrokenMap.lock.Lock()
	for _, ckptDoc := range ckptDocs {
		if ckptDoc == nil {
			continue
		}
		for _, record := range ckptDoc.Checkpoint_records {
			if record == nil {
				continue
			}
			brokenMappings := record.BrokenMappings()
			if brokenMappings == nil {
				continue
			}
			if record.TargetManifest > highestManifestId {
				// Simply set the new map
				ckmgr.cachedBrokenMap.brokenMap = *brokenMappings
				if ckmgr.cachedBrokenMap.brokenMap == nil {
					ckmgr.cachedBrokenMap.brokenMap = make(metadata.CollectionNamespaceMapping)
				}
				highestManifestId = record.TargetManifest
			} else if record.TargetManifest == highestManifestId {
				// Same version - consolidate it into a bigger fishing net
				ckmgr.cachedBrokenMap.brokenMap.Consolidate(*brokenMappings)
			}
		}
	}
	ckmgr.cachedBrokenMap.lock.Unlock()
	go ckmgr.updateReplStatusBrokenMap()
}

func (ckmgr *CheckpointManager) setTimestampForVB(vbno uint16, ts *base.VBTimestamp) error {
	ckmgr.logger.Infof("%v %v Set VBTimestamp: vb=%v, ts.Seqno=%v, ts.SourceManifestId=%v ts.TargetManifestId=%v\n",
		ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno, ts.Seqno, ts.ManifestIDs.SourceManifestId, ts.ManifestIDs.TargetManifestId)

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

// Restores broken mappings to multiple collection routers
func (ckmgr *CheckpointManager) restoreBrokenMappingManifestsToRouters(brokenMappingRO *metadata.CollectionNamespaceMapping, manifestId uint64, isRollBack bool) {
	if brokenMappingRO == nil || len(*brokenMappingRO) == 0 || manifestId == 0 {
		return
	}
	settings := make(map[string]interface{})
	var argsList []interface{}
	argsList = append(argsList, brokenMappingRO)
	argsList = append(argsList, manifestId)
	argsList = append(argsList, isRollBack)
	settings[metadata.BrokenMappingsUpdateKey] = argsList
	ckmgr.pipeline.UpdateSettings(settings)
}

func (ckmgr *CheckpointManager) startSeqnoGetter(getter_id int, listOfVbs []uint16, ckptDocs map[uint16]*metadata.CheckpointsDoc, waitGrp *sync.WaitGroup, err_ch chan interface{}, sharedLowestSuccessfulManifestId *uint64, vbtsStartedNon0 *uint32) {
	ckmgr.logger.Infof("%v %v StartSeqnoGetter %v is started to do _pre_prelicate for vbs %v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), getter_id, listOfVbs)
	defer waitGrp.Done()

	for _, vbno := range listOfVbs {
		seqnoMax := ckmgr.getMaxSeqno(vbno)
		vbts, vbStats, brokenMapping, targetManifestId, lastSuccessfulBackfillMgrSrcManifestId, err := ckmgr.getDataFromCkpts(vbno, ckptDocs[vbno], seqnoMax)
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
		ckmgr.restoreBrokenMappingManifestsToRouters(brokenMapping, targetManifestId, false /*isRollBack*/)
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

func (ckmgr *CheckpointManager) populateTargetVBOpaqueIfNeeded(vbno uint16) {
	obj, ok := ckmgr.cur_ckpts[vbno]
	if !ok {
		err := fmt.Errorf("%v %v Trying to check vbopaque on vb=%v which is not in MyVBList", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
		ckmgr.handleGeneralError(err)
		return
	}

	needToPopulate := true
	if obj != nil {
		obj.lock.RLock()
		if obj.ckpt != nil {
			needToPopulate = obj.ckpt.Target_vb_opaque == nil
		}
		obj.lock.RUnlock()
	}

	if !needToPopulate {
		return
	}

	_, curRemoteVBOpaque, err := ckmgr.capi_svc.PreReplicate(ckmgr.remote_bucket, service_def.NewEmptyRemoteVBReplicationStatus(vbno), ckmgr.support_ckpt)
	if err != nil {
		ckmgr.logger.Errorf("%v %v populateTargetVBOpaque(pre_replicate) failed for %v. err=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno, err)
		ckmgr.handleGeneralError(err)
		return
	}
	ckmgr.updateCurrentVBOpaque(vbno, curRemoteVBOpaque)
}

// Given a specific vbno and a list of checkpoints and a max possible seqno, return:
// valid VBTimestamp and corresponding VB-specific stats for statsMgr that was stored in the same ckpt doc
func (ckmgr *CheckpointManager) getDataFromCkpts(vbno uint16, ckptDoc *metadata.CheckpointsDoc, max_seqno uint64) (*base.VBTimestamp, service_def.VBCountMetricMap, *metadata.CollectionNamespaceMapping, uint64, uint64, error) {
	var agreedIndex int = -1

	ckptRecordsList := ckmgr.ckptRecordsWLock(ckptDoc, vbno)
	/**
	 * If we are fed a vbno and ckptDoc, then the above ckptRecordsWLock should feed us back a list
	 * that are not shared by anyone else so the locking here in this manner should be no problem.
	 * If we are not fed a legit ckptDoc, it means we're using an internal record, which should have
	 * only one record returned from the List, and locking a single element in this for loop should be fine.
	 */
	for index, ckptRecord := range ckptRecordsList {
		var remote_vb_status *service_def.RemoteVBReplicationStatus

		ckptRecord.lock.RLock()
		ckpt_record := ckptRecord.ckpt

		if ckpt_record == nil {
			ckptRecord.lock.RUnlock()
			continue
		}

		if ckpt_record.Seqno <= max_seqno {
			remote_vb_status = &service_def.RemoteVBReplicationStatus{VBOpaque: ckpt_record.Target_vb_opaque,
				VBSeqno: ckpt_record.Target_Seqno,
				VBNo:    vbno}
			if ckmgr.logger.GetLogLevel() >= log.LogLevelDebug {
				ckmgr.logger.Debugf("%v %v Remote bucket %v vbno %v checking to see if it could agreed on the checkpoint %v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), ckmgr.remote_bucket, vbno, ckpt_record)
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
				ckmgr.logger.Errorf("%v unable to find DCP source manifest ID %v, skipping a record...", ckmgr.Id(), sourceDCPManifestId)
				continue
			}
		}
		if sourceBackfillManifestId > 0 {
			_, err := ckmgr.collectionsManifestSvc.GetSpecificSourceManifest(spec, sourceBackfillManifestId)
			if err != nil {
				ckmgr.logger.Errorf("%v unable to find BackfillMgr source manifest ID %v, skipping a record...", ckmgr.Id(), sourceBackfillManifestId)
				continue
			}
		}

		if remote_vb_status != nil {
			bMatch := false
			bMatch, current_remoteVBOpaque, err := ckmgr.capi_svc.PreReplicate(ckmgr.remote_bucket, remote_vb_status, ckmgr.support_ckpt)

			if err != nil {
				if err == service_def.NoSupportForXDCRCheckpointingError {
					ckmgr.updateCurrentVBOpaque(vbno, nil)
					ckmgr.logger.Infof("%v %v Remote vbucket %v is on a old node which doesn't support checkpointing, update target_vb_uuid=0\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
					// no need to go through the remaining checkpoint records
					goto POPULATE
				} else {
					ckmgr.logger.Errorf("%v %v Pre_replicate failed for %v. err=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno, err)
					return nil, nil, nil, 0, 0, err
				}
			}

			ckmgr.updateCurrentVBOpaque(vbno, current_remoteVBOpaque)
			if ckmgr.logger.GetLogLevel() >= log.LogLevelDebug {
				ckmgr.logger.Debugf("%v %v Remote vbucket %v has a new opaque %v, update\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), current_remoteVBOpaque, vbno)
				ckmgr.logger.Debugf("%v %v Done with _pre_prelicate call for %v for vbno=%v, bMatch=%v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), remote_vb_status, vbno, bMatch)
			}

			if bMatch {
				if ckmgr.logger.GetLogLevel() >= log.LogLevelDebug {
					ckmgr.logger.Debugf("%v %v Remote bucket %v vbno %v agreed on the checkpoint above\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), ckmgr.remote_bucket, vbno)
				}
				if ckptDoc != nil {
					agreedIndex = index
				}
				goto POPULATE
			}
		} // end if remote_vb_status
	}
POPULATE:
	vbts, brokenMappings, targetManifestId, lastSuccessfulBackfillMgrSrcManifestId, err := ckmgr.populateDataFromCkptDoc(ckptDoc, agreedIndex, vbno)
	if err != nil {
		return vbts, nil, nil, 0, 0, err
	}
	vbStatMap := NewVBStatsMapFromCkpt(ckptDoc, agreedIndex)
	return vbts, vbStatMap, brokenMappings, targetManifestId, lastSuccessfulBackfillMgrSrcManifestId, err
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

	if ckptDoc != nil {
		// We're not going to use the checkpoint manager's internal record, so fake locks and return
		for _, aRecord := range ckptDoc.GetCheckpointRecords() {
			newLock := &sync.RWMutex{}
			recordWLock := &checkpointRecordWithLock{
				ckpt: aRecord,
				lock: newLock,
			}
			ret = append(ret, recordWLock)
		}
		ckmgr.logger.Infof("%v %v Found checkpoint doc for vb=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
	} else {
		// First time XDCR checkpoint is executing, thus why ckptDoc is nil
		ret = append(ret, ckmgr.getCurrentCkptWLock(vbno))
	}

	return ret
}

func (ckmgr *CheckpointManager) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	ckmgr.logger.Debugf("Updating settings on checkpoint manager for %v %v. settings=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), settings)
	// Before interval, check for idle brokenMap update
	diffPair, bmUpdateOk := settings[metadata.CkptMgrBrokenmapIdleUpdateDiffPair].(*metadata.CollectionNamespaceMappingsDiffPair)
	srcManifestsDelta, bmUpdateOk2 := settings[metadata.CkptMgrBrokenmapIdleUpdateSrcManDelta].([]*metadata.CollectionsManifest)
	if bmUpdateOk && bmUpdateOk2 {
		ckmgr.CleanupInMemoryBrokenMap(diffPair, srcManifestsDelta)
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
		ckmgr.logger.Infof("Skipped update of checkpoint interval for %v %v since it already has the value of %v.\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), checkpoint_interval)
		return nil
	}

	// update checkpoint interval
	ckmgr.setCheckpointInterval(checkpoint_interval)
	ckmgr.startRandomizedCheckpointingTicker()

	return nil
}

func (ckmgr *CheckpointManager) retrieveCkptDoc(vbno uint16) (*metadata.CheckpointsDoc, error) {
	ckmgr.logger.Infof("%v %v retrieve chkpt doc for vb=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
	return ckmgr.checkpoints_svc.CheckpointsDoc(ckmgr.pipeline.FullTopic(), vbno)
}

func (ckmgr *CheckpointManager) populateDataFromCkptDoc(ckptDoc *metadata.CheckpointsDoc, agreedIndex int, vbno uint16) (*base.VBTimestamp, *metadata.CollectionNamespaceMapping, uint64, uint64, error) {
	vbts := &base.VBTimestamp{Vbno: vbno}
	var brokenMapping *metadata.CollectionNamespaceMapping
	var targetManifestId uint64
	var backfillBypassAgreedIndex bool
	var lastSucccessfulBackfillMgrSrcManifestId uint64
	if agreedIndex > -1 {
		ckpt_records := ckptDoc.GetCheckpointRecords()
		if len(ckpt_records) < agreedIndex+1 {
			// should never happen
			ckmgr.logger.Warnf("%v %v could not find checkpoint record with agreedIndex=%v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), agreedIndex)
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
			vbts.ManifestIDs.TargetManifestId = ckpt_record.TargetManifest

			brokenMapping = ckpt_record.BrokenMappings()
			targetManifestId = ckpt_record.TargetManifest
			lastSucccessfulBackfillMgrSrcManifestId = ckpt_record.SourceManifestForBackfillMgr
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
		}
		// Usually, in the cases of Main pipelines, when there are no checkpoints (or no agreeable checkpoints)
		// the checkpoint manager will start at seqno 0, which is clean and guarantees no data loss
		// In the case of backfill pipelines, when there is no backfill checkpoint (or if there is no agreeable checkpoint)
		// the above will set the starting point accordingly
		// However, if there are backfill checkpoints and they all do not match because of
		//    checkpoint's startSeqno > backfillTaskEndSeqno
		// then the vbopaque will never be populated and will need to be populated to ensure that ckpt operations can take place
		ckmgr.populateTargetVBOpaqueIfNeeded(vbno)
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
			if brokenMapping != nil {
				obj.ckpt.LoadBrokenMapping(*brokenMapping)
			}
			obj.ckpt.TargetManifest = targetManifestId
		}
	} else {
		err := fmt.Errorf("%v %v Calling populateDataFromCkptDoc on vb=%v which is not in MyVBList", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
		ckmgr.handleGeneralError(err)
		return nil, nil, 0, 0, err
	}
	return vbts, brokenMapping, targetManifestId, lastSucccessfulBackfillMgrSrcManifestId, nil
}

func (ckmgr *CheckpointManager) checkpointing() {
	ckmgr.logger.Infof("%v %v checkpointing rountine started", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())

	defer ckmgr.logger.Infof("%v %v Exits checkpointing routine.", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
	defer ckmgr.wait_grp.Done()

	ticker := <-ckmgr.checkpoint_ticker_ch
	defer ticker.Stop()

	children_fin_ch := make(chan bool)
	children_wait_grp := &sync.WaitGroup{}

	first := true

	for {
		select {
		case new_ticker := <-ckmgr.checkpoint_ticker_ch:
			ckmgr.logger.Infof("%v %v Received new ticker due to changes to checkpoint interval setting", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())

			// wait for existing, if any, performCkpt routine to finish before setting new ticker
			close(children_fin_ch)
			children_wait_grp.Wait()
			children_fin_ch = make(chan bool)

			ticker.Stop()
			ticker = new_ticker
			first = true
		case <-ckmgr.finish_ch:
			ckmgr.logger.Infof("%v %v Received finish signal", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
			// wait for existing, if any, performCkpt routine to finish
			close(children_fin_ch)
			children_wait_grp.Wait()
			goto done
		case <-ticker.C:
			if !pipeline_utils.IsPipelineRunning(ckmgr.pipeline.State()) {
				//pipeline is no longer running, kill itself
				ckmgr.logger.Infof("%v %v Pipeline is no longer running, exit.", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
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
	ckmgr.cachedBrokenMap.lock.RLock()
	preUpsertMap := ckmgr.cachedBrokenMap.brokenMap.Clone()
	ckmgr.cachedBrokenMap.lock.RUnlock()
	err := ckmgr.checkpoints_svc.PreUpsertBrokenMapping(ckmgr.pipeline.FullTopic(), ckmgr.pipeline.Specification().GetReplicationSpec().InternalId, &preUpsertMap)
	if err != nil {
		ckmgr.logger.Warnf("Unable to pre-persist brokenMapping for %v: %v", ckmgr.pipeline.FullTopic(), err)
	}
}

func (ckmgr *CheckpointManager) CommitBrokenMappingUpdates() {
	// If router updated ckptmgr's brokenmap during the checkpointing process, this call will ensure that all the necessary brokenmaps are persisted
	err := ckmgr.checkpoints_svc.UpsertBrokenMapping(ckmgr.pipeline.FullTopic(), ckmgr.pipeline.Specification().GetReplicationSpec().InternalId)
	if err != nil {
		ckmgr.logger.Errorf("Unable to persist brokenMapping: %v", err)
	}
}

// public API. performs one checkpoint operation on request
func (ckmgr *CheckpointManager) PerformCkpt(fin_ch chan bool) {
	ckmgr.logger.Infof("Start one time checkpointing for replication %v %v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())

	var through_seqno_map map[uint16]uint64
	var high_seqno_and_vbuuid_map map[uint16][]uint64
	var srcManifestIds map[uint16]uint64
	var tgtManifestIds map[uint16]uint64
	var err error

	defer func() {
		if err == nil {
			ckmgr.logger.Infof("Done one time checkpointing for replication %v %v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
		} else {
			ckmgr.logger.Errorf("Skipped one time checkpointing for replication %v %v failed with error %v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), err)
		}
	}()

	// get through seqnos for all vbuckets in the pipeline
	through_seqno_map, srcManifestIds, tgtManifestIds = ckmgr.through_seqno_tracker_svc.GetThroughSeqnosAndManifestIds()
	// Let statsmgr know of the latest through seqno for it to prepare data for checkpointing
	ckmgr.statsMgr.HandleLatestThroughSeqnos(through_seqno_map)
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
	ckmgr.collectionsManifestSvc.PersistNeededManifests(ckmgr.pipeline.Specification().GetReplicationSpec())
	ckmgr.RaiseEvent(common.NewEvent(common.CheckpointDone, nil, ckmgr, nil, time.Duration(total_committing_time)*time.Nanosecond))
	ckmgr.checkpoints_svc.UpsertCheckpointsDone(ckmgr.pipeline.FullTopic())

}

// local API. supports periodical checkpoint operations
func (ckmgr *CheckpointManager) performCkpt(fin_ch chan bool, wait_grp *sync.WaitGroup) {
	defer wait_grp.Done()

	if !ckmgr.isCheckpointAllowed() {
		ckmgr.logger.Errorf("%v %v has not been started - checkpointing is skipped", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
		return
	}

	opDoneIdx, err := ckmgr.checkpointAllowedHelper.registerCkptOp(false)
	if err != nil {
		ckmgr.logger.Errorf("%v - %v (performCkpt) registerCkptOp failed - checkpointing is skipped", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
		return
	}
	defer ckmgr.checkpointAllowedHelper.markTaskDone(opDoneIdx)

	ckmgr.logger.Infof("Start checkpointing for replication %v %v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
	defer func() {
		if err == nil {
			ckmgr.logger.Infof("Done checkpointing for replication %v %v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
		} else {
			ckmgr.logger.Errorf("Skipped checkpointing for replication %v %v due to %v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), err)
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

	ckmgr.RaiseEvent(common.NewEvent(common.CheckpointDone, nil, ckmgr, nil, time.Duration(total_committing_time)*time.Nanosecond))
	ckmgr.checkpoints_svc.UpsertCheckpointsDone(ckmgr.pipeline.FullTopic())

}

func (ckmgr *CheckpointManager) gatherCkptData(fin_ch chan bool, through_seqno_map map[uint16]uint64, srcManifestIds map[uint16]uint64, tgtManifestIds map[uint16]uint64, high_seqno_and_vbuuid_map map[uint16][]uint64) (map[uint16]uint64, map[uint16]uint64, map[uint16]uint64, map[uint16][]uint64, error) {
	// get through seqnos for all vbuckets in the pipeline
	if ckmgr.collectionEnabled() {
		through_seqno_map, srcManifestIds, tgtManifestIds = ckmgr.through_seqno_tracker_svc.GetThroughSeqnosAndManifestIds()
	} else {
		through_seqno_map = ckmgr.through_seqno_tracker_svc.GetThroughSeqnos()
	}
	// Let statsmgr know of the latest through seqno for it to prepare data for checkpointing
	ckmgr.statsMgr.HandleLatestThroughSeqnos(through_seqno_map)
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
		ckmgr.logger.Errorf("%v %v checkpointing is disallowed - checkpointing is skipped", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
		return
	}

	opDoneIdx, err := ckmgr.checkpointAllowedHelper.registerCkptOp(false)
	if err != nil {
		ckmgr.logger.Errorf("%v - %v (performCkpt_internal) registerCkptOp failed - checkpointing is skipped", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
		return
	}
	defer ckmgr.checkpointAllowedHelper.markTaskDone(opDoneIdx)

	ckmgr.logger.Infof("Checkpointing for %v replication %v, vb_list=%v, time_to_wait=%v, interval_btwn_vb=%v sec\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vb_list, time_to_wait, interval_btwn_vb.Seconds())
	err_map := make(map[uint16]error)

	var totalSize int
	for index, vb := range vb_list {
		select {
		case <-fin_ch:
			ckmgr.logger.Infof("Aborting checkpointing routine for %v %v with vb list %v since received finish signal. index=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vb_list, index)
			return
		default:
			start_time_vb := time.Now()
			size, err := ckmgr.doCheckpoint(vb, through_seqno_map, high_seqno_and_vbuuid_map, srcManifestIds, tgtManifestIds)
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

	ckmgr.logger.Infof("Done checkpointing for %v replication %v with vb list %v totalSize=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vb_list, totalSize)
	if len(err_map) > 0 {
		ckmgr.logger.Infof("Errors encountered in checkpointing for %v replication %v: %v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), err_map)
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
		ckmgr.logger.Warnf("%v %v skipping checkpointing for vb=%v due to error persisting checkpoint record. err=%v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno, persistErr)
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
func (ckmgr *CheckpointManager) doCheckpoint(vbno uint16, through_seqno_map map[uint16]uint64, high_seqno_and_vbuuid_map map[uint16][]uint64, srcManifestIds, tgtManifestIds map[uint16]uint64) (size int, err error) {
	//locking the current ckpt record and notsent_seqno list for this vb, no update is allowed during the checkpointing
	ckmgr.logger.Debugf("%v %v Checkpointing for vb=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)

	ckpt_obj, ok := ckmgr.cur_ckpts[vbno]
	if !ok {
		err = fmt.Errorf("%v %v Trying to doCheckpoint on vb=%v which is not in MyVBList", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
		ckmgr.handleGeneralError(err)
		return
	}

	var curCkptTargetVBOpaque metadata.TargetVBOpaque
	// Get all necessary info before processing
	ckpt_obj.lock.RLock()
	// This is the version number that this iteration is working off of. Should the version number change
	// when updating the actual record, then this mean an concurrent operation has jumped ahead of us
	currRecordVersion := ckpt_obj.versionNum
	last_seqno := ckpt_obj.ckpt.Seqno
	curCkptTargetVBOpaque = ckpt_obj.ckpt.Target_vb_opaque
	ckpt_obj.lock.RUnlock()

	through_seqno, err := ckmgr.getThroughSeqno(vbno, through_seqno_map)
	if err != nil {
		ckmgr.logger.Warnf("%v %v skipping checkpointing for vb=%v. err=%v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno, err)
		err = nil
		return
	}

	// Item 1:
	// Update the one and only check point record that checkpoint_manager keeps track of in its "cur_ckpts" with the through_seq_no
	// that was found from "GetThroughSeqno", which has already done the work to single out the number to represent the latest state
	// from the source side perspective
	ckmgr.logger.Debugf("%v %v Seqno number used for checkpointing for vb %v is %v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno, through_seqno)

	if through_seqno < last_seqno {
		ckmgr.logger.Infof("%v %v Checkpoint seqno went backward, possibly due to rollback. vb=%v, old_seqno=%v, new_seqno=%v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno, last_seqno, through_seqno)
	}

	if through_seqno == last_seqno {
		ckmgr.logger.Debugf("%v %v No replication has happened in vb %v since replication start or last checkpoint. seqno=%v. Skip checkpointing\\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno, last_seqno)
		return
	}

	if curCkptTargetVBOpaque == nil {
		ckmgr.logger.Infof("%v %v remote bucket is an older node, no checkpointing should be done.", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
		return
	}

	remote_seqno, err := ckmgr.getRemoteSeqno(vbno, high_seqno_and_vbuuid_map, curCkptTargetVBOpaque)
	if err != nil {
		if err == targetVbuuidChangedError {
			// vb uuid on target has changed. rollback may be needed. return error to get pipeline restarted
			return
		} else {
			ckmgr.logger.Warnf("%v %v skipping checkpointing for vb=%v. err=%v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno, err)
			err = nil
			return
		}
	}

	// Go through the local snapshot records repository and figure out which snapshot contains this latest sequence number
	// Item: 5 and 6
	ckRecordDcpSnapSeqno, ckRecordDcpSnapEndSeqno, snapErr := ckmgr.getSnapshotForSeqno(vbno, through_seqno)
	if snapErr != nil {
		// if we cannot find snapshot for the checkpoint seqno, the checkpoint seqno is still usable in normal cases,
		// just that we may have to rollback to 0 when rollback is needed
		ckmgr.logger.Warnf("%v\n", snapErr.Error())
	}

	// if target vb opaque has not changed, persist checkpoint record
	ckptRecordTargetSeqno := remote_seqno
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
		ckmgr.logger.Warnf("%v unable to get %v metric from stats manager\n", service_def.DOCS_FILTERED_METRIC)
		return
	}
	filteredItems := vbCountMetrics[service_def.DOCS_FILTERED_METRIC]
	filterFailed := vbCountMetrics[service_def.DOCS_UNABLE_TO_FILTER_METRIC]

	// Collection-Related items
	srcManifestIdForDcp, srcManifestIdForBackfill, tgtManifestId, brokenMapping := ckmgr.getCollectionItemsIfEnabled(vbno, ok, srcManifestIds, tgtManifestIds)

	// Write-operation - feed the temporary variables and update them into the record and also write them to metakv
	newCkpt, err := metadata.NewCheckpointRecord(ckRecordFailoverUuid, through_seqno, ckRecordDcpSnapSeqno, ckRecordDcpSnapEndSeqno, ckptRecordTargetSeqno, uint64(filteredItems), uint64(filterFailed), srcManifestIdForDcp, srcManifestIdForBackfill, tgtManifestId, brokenMapping, uint64(time.Now().Unix()))
	if err != nil {
		ckmgr.logger.Errorf("Unable to create a checkpoint record due to - %v", err)
		err = nil
		return
	}

	size, err = ckpt_obj.updateAndPersist(ckmgr, vbno, currRecordVersion, newCkpt)

	if err != nil {
		// We weren't able to atomically update the checkpoint record. This checkpoint record is essentially lost
		// since someone jumped ahead of us
		ckmgr.logger.Warnf("%v %v skipping checkpointing for vb=%v version %v since a more recent checkpoint has been completed",
			ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno, currRecordVersion)
		err = nil
	} else {
		ckmgr.markBpCkptStored(vbno)
	}
	// no error fall through. return nil here to keep checkpointing/replication going.
	// if there is a need to stop checkpointing/replication, it needs to be done in a separate return statement
	return
}

func (ckmgr *CheckpointManager) getCollectionItemsIfEnabled(vbno uint16, ok bool, srcManifestIds map[uint16]uint64, tgtManifestIds map[uint16]uint64) (uint64, uint64, uint64, metadata.CollectionNamespaceMapping) {
	if !ckmgr.collectionEnabled() {
		return 0, 0, 0, metadata.CollectionNamespaceMapping{}
	}

	srcManifestId, ok := srcManifestIds[vbno]
	if !ok {
		ckmgr.logger.Warnf("Unable to find Source manifest ID for vb %v", vbno)
	}
	tgtManifestId, ok := tgtManifestIds[vbno]
	if !ok {
		ckmgr.logger.Warnf("Unable to find Target manifest ID for vb %v", vbno)
	}

	ckmgr.cachedBrokenMap.lock.RLock()
	brokenMapping := ckmgr.cachedBrokenMap.brokenMap.Clone()
	// It is possible that Router has already raised IgnoreEvent() to throughseqtrakcer service
	// If that's the case, the manifestID returned by it will not be correct to pair with the broken map
	// If the brokenMapping is present, then ensure that the checkpoint will store the corresponding
	// manifestID so future backfill will be created
	if len(ckmgr.cachedBrokenMap.brokenMap) > 0 && ckmgr.cachedBrokenMap.correspondingTargetManifest > 0 {
		tgtManifestId = ckmgr.cachedBrokenMap.correspondingTargetManifest
	}
	ckmgr.cachedBrokenMap.lock.RUnlock()

	backfillMgr := ckmgr.getBackfillMgr()
	lastBackfillSuccessfulId, err := backfillMgr.GetLastSuccessfulSourceManifestId(ckmgr.pipeline.Topic())
	if err != nil {
		ckmgr.logger.Warnf("Unable to get last successful source manifest Id for %v - %v", ckmgr.pipeline.Topic(), err)
	}
	return srcManifestId, lastBackfillSuccessfulId, tgtManifestId, brokenMapping
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

func (ckmgr *CheckpointManager) getRemoteSeqno(vbno uint16, high_seqno_and_vbuuid_map map[uint16][]uint64, curCkptTargetVBOpaque metadata.TargetVBOpaque) (uint64, error) {
	// non-capi mode, high_seqno and vbuuid on target have been retrieved through vbucket-seqno stats
	high_seqno_and_vbuuid, ok := high_seqno_and_vbuuid_map[vbno]
	if !ok {
		return 0, fmt.Errorf("%v %v cannot find high seqno and vbuuid for vb %v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
	}

	remote_seqno := high_seqno_and_vbuuid[0]
	vbuuid := high_seqno_and_vbuuid[1]
	targetVBOpaque := &metadata.TargetVBUuid{vbuuid}
	if !curCkptTargetVBOpaque.IsSame(targetVBOpaque) {
		ckmgr.logger.Errorf("%v %v target vbuuid has changed for vb=%v. old=%v, new=%v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno, curCkptTargetVBOpaque, targetVBOpaque)
		return 0, targetVbuuidChangedError
	}
	return remote_seqno, nil
}

func (ckmgr *CheckpointManager) raiseSuccessCkptForVbEvent(ckpt_record metadata.CheckpointRecord, vbno uint16) {
	//notify statisticsManager
	ckmgr.RaiseEvent(common.NewEvent(common.CheckpointDoneForVB, ckpt_record, ckmgr, nil, vbno))
}

func (ckmgr *CheckpointManager) persistCkptRecord(vbno uint16, ckpt_record *metadata.CheckpointRecord) (int, error) {
	ckmgr.logger.Debugf("Persist vb=%v ckpt_record=%v for %v %v\n", vbno, ckpt_record, ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
	return ckmgr.checkpoints_svc.UpsertCheckpoints(ckmgr.pipeline.FullTopic(), ckmgr.pipeline.Specification().GetReplicationSpec().InternalId, vbno, ckpt_record)
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

				ckmgr.logger.Debugf("%v %v Got failover log for vb=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
			} else {
				err := fmt.Errorf("%v %v Received failoverlog on an unknown vb=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
				ckmgr.handleGeneralError(err)
			}
		}
	case common.SnapshotMarkerReceived:
		upr_event, ok := event.Data.(*mcc.UprEvent)
		ckmgr.logger.Debugf("%v %v Received snapshot vb=%v, start=%v, end=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), upr_event.VBucket, upr_event.SnapstartSeq, upr_event.SnapendSeq)
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
			if routingInfo.TargetManifestId > ckmgr.cachedBrokenMap.correspondingTargetManifest {
				// Newer version means potentially backfill maps that were fixed (case 2)
				// Remove those from the broken mapppings
				ckmgr.cachedBrokenMap.brokenMap = ckmgr.cachedBrokenMap.brokenMap.Delete(routingInfo.BackfillMap)
				ckmgr.cachedBrokenMap.correspondingTargetManifest = routingInfo.TargetManifestId
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
		// TODO MB-38507 - Once UI portion is done and can parse replicationStatus with broken mapping, remove this log
		ckmgr.logger.Infof("Current broken mapping for pipeline %v: %v", ckmgr.pipeline.FullTopic(), newerMap.String())
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

func (ckmgr *CheckpointManager) UpdateVBTimestamps(vbno uint16, rollbackseqno uint64) (*base.VBTimestamp, error) {
	ckmgr.logger.Infof("%v Received rollback from DCP stream vb=%v, rollbackseqno=%v\n", ckmgr.pipeline.FullTopic(), vbno, rollbackseqno)
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
		ckmgr.logger.Errorf("Unable to find ckpt doc from metakv given pipeline topic: %v vbno: %v",
			ckmgr.pipeline.FullTopic(), vbno)
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

	ckmgr.logger.Infof("%v vb=%v, current_start_seqno=%v, max_seqno=%v\n", ckmgr.pipeline.FullTopic(), vbno, pipeline_start_seqno.Seqno, max_seqno)

	vbts, vbStats, brokenMappings, targetManifestId, lastsuccessfulBackfillMgrSrcManifestId, err := ckmgr.getDataFromCkpts(vbno, checkpointDoc, max_seqno)
	if err != nil {
		return nil, err
	}

	pipeline_startSeqnos_map[vbno] = vbts

	ckmgr.restoreBrokenMappingManifestsToRouters(brokenMappings, targetManifestId, true /*isRollBack*/)

	//set the start seqno on through_seqno_tracker_svc
	ckmgr.through_seqno_tracker_svc.SetStartSeqno(vbno, vbts.Seqno, vbts.ManifestIDs)
	err = ckmgr.statsMgr.SetVBCountMetrics(vbno, vbStats)
	if err != nil {
		ckmgr.logger.Warnf("%v setting vbStat for vb %v returned error %v Stats: %v\n", ckmgr.pipeline.FullTopic(), vbno, err, vbStats)
	}

	if ckmgr.pipeline.Type() == common.MainPipeline {
		go ckmgr.getBackfillMgr().SetLastSuccessfulSourceManifestId(ckmgr.pipeline.Topic(), lastsuccessfulBackfillMgrSrcManifestId, true, ckmgr.finish_ch)
	}

	ckmgr.logger.Infof("%v Rolled back startSeqno to %v for vb=%v\n", ckmgr.pipeline.FullTopic(), vbts.Seqno, vbno)
	ckmgr.logger.Infof("%v Retry vbts=%v\n", ckmgr.pipeline.FullTopic(), vbts)

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
			logger.Errorf("%v Didn't find 'VBTimesstamps' in settings. settings=%v\n", pipeline.FullTopic(), settings.CloneAndRedact())
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
func (ckmgr *CheckpointManager) CleanupInMemoryBrokenMap(diffPair *metadata.CollectionNamespaceMappingsDiffPair, srcManDelta []*metadata.CollectionsManifest) {
	sourceChanged := len(srcManDelta) == 2 && srcManDelta[0] != nil && srcManDelta[1] != nil
	// This must be deferred first as defer calls are FILO and this needs a lock
	defer ckmgr.updateReplStatusBrokenMap()
	// This path only gets called when manifest changes... so it shouldn't be too often/expensive to clone
	ckmgr.cachedBrokenMap.lock.Lock()
	defer ckmgr.cachedBrokenMap.lock.Unlock()

	oldMap := ckmgr.cachedBrokenMap.brokenMap.Clone()
	// pair.Added means backfillMgr detected these and need to be backfilled, which means they're no longer broken
	ckmgr.cachedBrokenMap.brokenMap = ckmgr.cachedBrokenMap.brokenMap.Delete(diffPair.Added)
	// pair.Removed means these mappings no longer exist and are obsolete, and should be cleaned up too
	ckmgr.cachedBrokenMap.brokenMap = ckmgr.cachedBrokenMap.brokenMap.Delete(diffPair.Removed)
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
	}
	newerMap = ckmgr.cachedBrokenMap.brokenMap.Clone()
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

	return fmt.Errorf("Unhandled type: %v", reflect.TypeOf(genericResponse))
}

type nodeVbCkptMap map[string]metadata.VBsCkptsDocMap

// Merging pre-requisites:
//  1. For each checkpoint, the VBUUID must exist in the src and target failover log. If not, ckpt is not valid
//     and is not worth saving
//  2. Once invalid ckpts are filtered out, they need to be sorted
//  3. Sort first by source side
func (ckmgr *CheckpointManager) mergeNodesToVBMasterCheckResp(respMap peerToPeer.PeersVBMasterCheckRespMap) error {
	if len(respMap) == 0 {
		// nothing to merge
		return nil
	}

	var needToGetFailoverLogs bool
	combinedSrcManifests := make(metadata.ManifestsCache)
	combinedTgtManifests := make(metadata.ManifestsCache)
	var combinedBrokenMapping []*metadata.CollectionNsMappingsDoc
	combinedBrokenMappingSpecInternalId := make(map[string]string)

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

		oneNodeVbsCkptMap := payload.GetAllCheckpoints(common.MainPipeline)
		if len(oneNodeVbsCkptMap) > 0 {
			for _, ckptDoc := range oneNodeVbsCkptMap {
				for _, ckptRecord := range ckptDoc.Checkpoint_records {
					if ckptRecord == nil {
						continue
					}
				}
			}
			needToGetFailoverLogs = true
			ckmgr.logger.Infof("Received peerToPeer checkpoint data from node %v replId %v", node, resp.ReplicationSpecId)
			nodeVbMainCkptsMap[node] = oneNodeVbsCkptMap
		}
		oneNodeVbsBackfillCkptMap := payload.GetAllCheckpoints(common.BackfillPipeline)
		if len(oneNodeVbsBackfillCkptMap) > 0 {
			for _, ckptDoc := range oneNodeVbsBackfillCkptMap {
				for _, ckptRecord := range ckptDoc.Checkpoint_records {
					if ckptRecord == nil {
						continue
					}
				}
			}
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
		}
		unlockFunc()
	}

	if len(warnMap) > 0 {
		ckmgr.logger.Warnf("Parsing peer nodes data had errors: %v", base.FlattenErrorMap(warnMap))
	}

	// If specIds are different, then something is mid flight
	brokenMapSpecInternalId, err := ckmgr.checkSpecInternalID(combinedBrokenMappingSpecInternalId)
	if err != nil {
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
		tgtFailoverLogs, err = ckmgr.getOneTimeTgtFailoverLogs(vbsThatNeedTargetFailoverlogs)
		if err != nil {
			ckmgr.logger.Errorf("unable to get failoverlog from target(s) %v", err)
			return err
		}
		filteredMaps = filterInvalidCkptsBasedOnTargetFailover(filteredMaps, tgtFailoverLogs)
	}
	filteredMaps, combinedShaMap, err := filterCkptsWithoutValidBrokenmaps(filteredMaps, combinedBrokenMapping)
	if err != nil {
		// filteredMap may still be valid... continue
		ckmgr.logger.Warnf("filterCkptsWithoutValidBrokenMaps had errors: %v", err)
	}

	err, done := ckmgr.preStopGcCheck(filteredMaps)
	if done {
		return err
	}

	getterFunc := func() *MergeCkptArgs {
		return &MergeCkptArgs{
			PipelinesCkptDocs:       filteredMaps,
			BrokenMappingShaMap:     combinedShaMap,
			BrokenMapSpecInternalId: brokenMapSpecInternalId,
			SrcManifests:            &combinedSrcManifests,
			TgtManifests:            &combinedTgtManifests,
			SrcFailoverLogs:         srcFailoverLogs,
			TgtFailoverLogs:         tgtFailoverLogs,
		}
	}

	err = ckmgr.stopTheWorldAndMergeCkpts(getterFunc)
	if err != nil {
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
	defer ckmgr.checkpointAllowedHelper.setCheckpointAllowed()

	// Once finished waiting, get all the accumulated information during this time that needs to be merged and do it in one shot
	ckptArgs := getter()
	if ckptArgs == nil {
		return nil
	}
	pipelinesCkptDocs := ckptArgs.PipelinesCkptDocs
	brokenMappingShaMap := ckptArgs.BrokenMappingShaMap
	brokenMapSpecInternalId := ckptArgs.BrokenMapSpecInternalId
	srcManifests := ckptArgs.SrcManifests
	tgtManifests := ckptArgs.TgtManifests
	srcFailoverLogs := ckptArgs.SrcFailoverLogs
	tgtFailoverLogs := ckptArgs.TgtFailoverLogs
	respChs := ckptArgs.PushRespChs

	err := ckmgr.mergeFinalCkpts(pipelinesCkptDocs, srcFailoverLogs, tgtFailoverLogs, brokenMappingShaMap, brokenMapSpecInternalId)
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
					ckmgr.logger.Warnf("%v - Unable to register GC for vbno %v - err: %v", registerTopic, vbno, gcErr)
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
				if ckptRecord.BrokenMappingSha256 == "" {
					// no brokenmap, ok to keep
					replacementList = append(replacementList, ckptRecord)
				} else {
					if brokenMapping, exists := combinedShaMap[ckptRecord.BrokenMappingSha256]; exists && brokenMapping != nil {
						err := ckptRecord.LoadBrokenMapping(*brokenMapping)
						if err != nil {
							errMap[fmt.Sprintf("loadBrokenMapping error %v", errCnt)] = err
							continue
						}
						// BrokenMapping is found, ok to keep
						replacementList = append(replacementList, ckptRecord)
					}
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

func filterInvalidCkptsBasedOnTargetFailover(ckptsMaps []metadata.VBsCkptsDocMap, tgtFailoverLogs map[uint16]*mcc.FailoverLog) []metadata.VBsCkptsDocMap {
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

				ckptRecordVbUuid := ckptRecord.Target_vb_opaque.Value().(uint64)

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

func findVbsThatNeedTargetFailoverLogs(filteredMaps []metadata.VBsCkptsDocMap) []uint16 {
	dedupMap := make(map[uint16]bool)
	for _, filteredMap := range filteredMaps {
		if filteredMap == nil {
			continue
		}
		for vbno, doc := range filteredMap {
			if len(doc.Checkpoint_records) > base.MaxCheckpointRecordsToKeep {
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
	client, err := ckmgr.utils.GetMemcachedConnection(memcachedAddr, spec.SourceBucketName, ckmgr.user_agent, 0, ckmgr.logger)
	if err != nil {
		return nil, err
	}
	feed, err := client.NewUprFeedIface()
	if err != nil {
		return nil, err
	}
	err = feed.UprOpen(ckmgr.user_agent, uint32(0), base.UprFeedBufferSize)
	if err != nil {
		return nil, err
	}
	defer feed.Close()
	return client.UprGetFailoverLog(vbsList)
}

// TODO - maybe need retry mechanism
func (ckmgr *CheckpointManager) getOneTimeTgtFailoverLogs(vbsList []uint16) (map[uint16]*mcc.FailoverLog, error) {
	err := ckmgr.initConnections()
	if err != nil {
		return nil, err
	}

	filteredKvVbMap := filterKvVbMap(ckmgr.target_kv_vb_map, vbsList)

	failoverLogsMap := make(map[string]map[uint16]*mcc.FailoverLog)
	var failoverLogsMapMtx sync.Mutex

	errMap := make(base.ErrorMap)
	var errMapMtx sync.Mutex

	var waitGrp sync.WaitGroup
	ckmgr.kv_mem_clients_lock.RLock()
	for kvTransient, mccClientTransient := range ckmgr.kv_mem_clients {
		mccClient := mccClientTransient
		kv := kvTransient
		// Get failoverlogs in parallel
		waitGrp.Add(1)
		go func() {
			defer waitGrp.Done()
			feed, err := mccClient.NewUprFeed()
			if err != nil {
				errMapMtx.Lock()
				errMap[kv] = err
				errMapMtx.Unlock()
				return
			}

			err = feed.UprOpen(ckmgr.user_agent, 0, base.UprFeedBufferSize)
			if err != nil {
				errMapMtx.Lock()
				errMap[kv] = err
				errMapMtx.Unlock()
				return
			}
			defer feed.Close()

			failoverLogs, err := mccClient.UprGetFailoverLog(filteredKvVbMap[kv])
			if err != nil {
				errMapMtx.Lock()
				errMap[kv] = err
				errMapMtx.Unlock()
				return
			}

			failoverLogsMapMtx.Lock()
			failoverLogsMap[kv] = failoverLogs
			failoverLogsMapMtx.Unlock()
		}()
	}
	ckmgr.kv_mem_clients_lock.RUnlock()
	waitGrp.Wait()

	if len(errMap) > 0 {
		ckmgr.logger.Errorf("err getting failoverlogs from target %v", errMap)
		return nil, fmt.Errorf(base.FlattenErrorMap(errMap))
	}

	// We shouldn't have conflicting VBs since each target KV should own non-intersecting VBs
	compiledMap := make(map[uint16]*mcc.FailoverLog)
	for _, failoverLogsPerVb := range failoverLogsMap {
		for vb, failoverLogs := range failoverLogsPerVb {
			compiledMap[vb] = failoverLogs
		}
	}

	return compiledMap, nil
}

// This merge function is called in two different paths: 1. Pull model vs 2. Push model (only on main pipeline's)
// When pulling, both main and backfill pipelines are stopped, and the main pipeline's checkpoint manager is responsible
// for merging both types of ckpts
// When pushing, either one or both are potentially running, but only backfill pipeline's ckptMgr may perform ckpt
// while this is happening
func (ckmgr *CheckpointManager) mergeFinalCkpts(filteredMaps []metadata.VBsCkptsDocMap, srcFailoverLogs, tgtFailoverLogs map[uint16]*mcc.FailoverLog, combinedShaMapFromPeers metadata.ShaToCollectionNamespaceMap, peerBrokenMapSpecInternalId string) error {

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
					ckmgr.logger.Errorf("mergeFinalCkpts %v CheckpointsDocs err %v\n", ckptTopic, err)
					errList[i] = err
					return
				}
			}

			combinePeerCkptDocsWithLocalCkptDoc(filteredMap, srcFailoverLogs, tgtFailoverLogs, currDocs, spec)
			err = ckmgr.mergeAndPersistBrokenMappingDocsAndCkpts(ckptTopic, combinedShaMapFromPeers, peerBrokenMapSpecInternalId, currDocs)
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
		if len(combinedRecords) > base.MaxCheckpointRecordsToKeep {
			combinedRecords = combinedRecords[:base.MaxCheckpointRecordsToKeep]
		}

		ckptDoc.Checkpoint_records = combinedRecords.ToRegularList()
	}
}

func (ckmgr *CheckpointManager) mergeAndPersistBrokenMappingDocsAndCkpts(specId string, peersShaMap metadata.ShaToCollectionNamespaceMap, specInternalId string, ckptDocs map[uint16]*metadata.CheckpointsDoc) error {
	stopFunc := ckmgr.utils.StartDiagStopwatch("ckmgr.mergeAndPersistBrokenMappingDocsAndCkpts", base.DiagInternalThreshold)
	defer stopFunc()
	curNodeShaMap, curNodeMappingDoc, _, _, err := ckmgr.checkpoints_svc.LoadBrokenMappings(specId)
	if err != nil {
		ckmgr.logger.Errorf("mergeAndPersistBrokenMappingDocsAndCkpts LoadBrokenMappings err: %v", err)
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
		ckmgr.logger.Errorf("mergeAndPersistBrokenMappingDocsAndCkpts LoadShaMap err: %v", err)
		return err
	}
	return ckmgr.checkpoints_svc.UpsertAndReloadCheckpointCompleteSet(specId, curNodeMappingDoc, ckptDocs, specInternalId)
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
		ckmgr.logger.Errorf("error getting latest vblist: %v", err)
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
	mainCkptDocs := payload.GetAllCheckpoints(common.MainPipeline)
	backfillCkptDocs := payload.GetAllCheckpoints(common.BackfillPipeline)
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

	pipelinesCkpts, shaMap, err := filterCkptsWithoutValidBrokenmaps([]metadata.VBsCkptsDocMap{mainCkptDocs, backfillCkptDocs}, combinedBrokenMapping)
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
	err = ckmgr.requestPeriodicMerge(pipelinesCkpts, shaMap, brokenMapSpecInternalId, srcManifests, tgtManifests, respCh)
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

func (ckmgr *CheckpointManager) requestPeriodicMerge(pipelinesCkpts []metadata.VBsCkptsDocMap, shaMap metadata.ShaToCollectionNamespaceMap, brokenMapSpecInternalId string, srcManifests *metadata.ManifestsCache, tgtManifests *metadata.ManifestsCache, respCh chan error) error {
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
			ckmgr.checkpoints_svc.EnableRefCntDecrement(backfillTopic)
		}
	}
}

func filterKvVbMap(kvVbMap base.KvVBMapType, vbsList []uint16) base.KvVBMapType {
	retMap := make(base.KvVBMapType)

	lookupIndex := kvVbMap.CompileLookupIndex()
	for _, vb := range vbsList {
		node := lookupIndex[vb]
		retMap[node] = append(retMap[node], vb)
	}
	return retMap
}
