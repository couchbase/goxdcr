// Copyright (c) 2013-2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package pipeline_svc

import (
	"bytes"
	"errors"
	"fmt"
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
	"math"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

const (
	XDCRCheckpointing string = "xdcrCheckpointing"
	CheckpointMgrId   string = "CheckpointMgr"
	TimeCommiting     string = "time_commiting"
	Vbno              string = "vbno"
)

var CHECKPOINT_INTERVAL = "checkpoint_interval"

var ckptRecordMismatch error = errors.New("Checkpoint Records internal version mismatch")
var targetVbuuidChangedError error = errors.New("target vbuuid has changed")

type CheckpointMgrSvc interface {
	common.PipelineService

	CheckpointsExist(topic string) (bool, error)
	DelSingleVBCheckpoint(topic string, vbno uint16) error
}

type CheckpointManager struct {
	*component.AbstractComponent

	pipeline common.Pipeline

	//checkpoints_svc handles CRUD operation of checkpoint docs in the metadata store
	checkpoints_svc service_def.CheckpointsService

	//capi_svc to do rest call to the remote cluster for checkpointing
	capi_svc service_def.CAPIService

	//remote cluster reference to refresh the remote bucket
	remote_cluster_svc service_def.RemoteClusterSvc

	//replication specification service
	rep_spec_svc service_def.ReplicationSpecSvc

	//cluster info service
	cluster_info_svc service_def.ClusterInfoSvc

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
	target_kv_vb_map map[string][]uint16

	// whether target cluster is elasticsearch
	isTargetES bool

	user_agent string

	// these fields are used for xmem replication only
	// memcached clients for retrieval of target bucket stats
	kv_mem_clients      map[string]mcc.ClientIface
	kv_mem_clients_lock sync.RWMutex

	target_cluster_version int
	utils                  utilities.UtilsIface
	statsMgr               service_def.StatsMgrIface

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
	backfillStartingTsMtx sync.RWMutex
	backfillStartingTs    map[uint16]*base.VBTimestamp

	// Before a pipeline starts, prevent checkpoints from being created
	// If a ckpt is created before old ones are loaded, it could lead to incorrect
	// resuming and potential data loss
	checkpointOpAllowed uint32

	collectionsManifestSvc service_def.CollectionsManifestSvc

	// When a pipeline starts up and if it's a backfill pipeline, and the backfill job is short
	// s.t. no ckpt is created, then it needs to have a way
	// to ensure that DelCheckpoint for a vbno is a no-op if this pipeline does not have ckpt
	// for such a VB so XDCR doesn't hit metakv un-necessarily, which is expensive
	isBackfillPipeline uint32
	bpVbMtx            sync.RWMutex
	bpVbHasCkpt        map[uint16]bool

	backfillReplSvc service_def.BackfillReplSvc
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

func NewCheckpointManager(checkpoints_svc service_def.CheckpointsService, capi_svc service_def.CAPIService,
	remote_cluster_svc service_def.RemoteClusterSvc, rep_spec_svc service_def.ReplicationSpecSvc, cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc, through_seqno_tracker_svc service_def.ThroughSeqnoTrackerSvc,
	active_vbs map[string][]uint16, target_username, target_password string, target_bucket_name string,
	target_kv_vb_map map[string][]uint16, target_cluster_ref *metadata.RemoteClusterReference,
	target_cluster_version int, isTargetES bool, logger_ctx *log.LoggerContext, utilsIn utilities.UtilsIface, statsMgr service_def.StatsMgrIface,
	uiLogSvc service_def.UILogSvc, collectionsManifestSvc service_def.CollectionsManifestSvc,
	backfillReplSvc service_def.BackfillReplSvc) (*CheckpointManager, error) {
	if checkpoints_svc == nil || capi_svc == nil || remote_cluster_svc == nil || rep_spec_svc == nil || cluster_info_svc == nil || xdcr_topology_svc == nil {
		return nil, errors.New("checkpoints_svc, capi_svc, remote_cluster_svc, rep_spec_svc, cluster_info_svc and xdcr_topology_svc can't be nil")
	}
	logger := log.NewLogger("CheckpointMgr", logger_ctx)

	return &CheckpointManager{
		AbstractComponent:         component.NewAbstractComponentWithLogger(CheckpointMgrId, logger),
		pipeline:                  nil,
		checkpoints_svc:           checkpoints_svc,
		capi_svc:                  capi_svc,
		rep_spec_svc:              rep_spec_svc,
		remote_cluster_svc:        remote_cluster_svc,
		cluster_info_svc:          cluster_info_svc,
		xdcr_topology_svc:         xdcr_topology_svc,
		through_seqno_tracker_svc: through_seqno_tracker_svc,
		finish_ch:                 make(chan bool, 1),
		checkpoint_ticker_ch:      make(chan *time.Ticker, 1000),
		logger:                    logger,
		cur_ckpts:                 make(map[uint16]*checkpointRecordWithLock),
		active_vbs:                active_vbs,
		target_username:           target_username,
		target_password:           target_password,
		target_bucket_name:        target_bucket_name,
		target_kv_vb_map:          target_kv_vb_map,
		wait_grp:                  &sync.WaitGroup{},
		failoverlog_map:           make(map[uint16]*failoverlogWithLock),
		snapshot_history_map:      make(map[uint16]*snapshotHistoryWithLock),
		kv_mem_clients:            make(map[string]mcc.ClientIface),
		target_cluster_ref:        target_cluster_ref,
		target_cluster_version:    target_cluster_version,
		isTargetES:                isTargetES,
		utils:                     utilsIn,
		statsMgr:                  statsMgr,
		uiLogSvc:                  uiLogSvc,
		collectionsManifestSvc:    collectionsManifestSvc,
		backfillStartingTs:        make(map[uint16]*base.VBTimestamp),
		backfillReplSvc:           backfillReplSvc,
	}, nil
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
	ckmgr.backfillStartingTsMtx.Lock()
	defer ckmgr.backfillStartingTsMtx.Unlock()

	vbTasks := spec.VBTasksMap
	for vb, tasks := range vbTasks {
		if tasks == nil || len(*tasks) == 0 {
			continue
		}
		ckmgr.backfillStartingTs[vb] = (*tasks)[0].Timestamps.StartingTimestamp
		atLeastOneValid = true
	}
	if !atLeastOneValid {
		return fmt.Errorf("Unable to find at least one valid task for backfill spec %v", spec.Id)
	}

	return nil
}

// When rolling back, if rollback is requesting a seqno that is earlier than the backfill task, then
// allow that to happen by removing the startingTs
// TODO MB-39584 - if rollback to 0 is requested by DCP, then ensure that all the backfill tasks are merged
func (ckmgr *CheckpointManager) clearBackfillStartingTsIfNeeded(vbno uint16, rollbackSeqno uint64) {
	var needToRemove bool
	ckmgr.backfillStartingTsMtx.RLock()
	startTs, exists := ckmgr.backfillStartingTs[vbno]
	if exists {
		if rollbackSeqno < startTs.Seqno {
			needToRemove = true
		}
	}
	ckmgr.backfillStartingTsMtx.RUnlock()

	if needToRemove {
		ckmgr.backfillStartingTsMtx.Lock()
		delete(ckmgr.backfillStartingTs, vbno)
		ckmgr.backfillStartingTsMtx.Unlock()
	}
}

func (ckmgr *CheckpointManager) populateRemoteBucketInfo(pipeline common.Pipeline) error {
	spec := pipeline.Specification().GetReplicationSpec()

	remote_bucket, err := service_def.NewRemoteBucketInfo(ckmgr.target_cluster_ref.Name(), spec.TargetBucketName, ckmgr.target_cluster_ref, ckmgr.remote_cluster_svc, ckmgr.cluster_info_svc, ckmgr.logger, ckmgr.utils)
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
	if !ckmgr.isTargetES {
		err = ckmgr.initConnections()
		if err != nil {
			return err
		}
	}

	//start checkpointing loop
	ckmgr.wait_grp.Add(1)
	go ckmgr.checkpointing()
	return nil
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
}

// compose user agent string for HELO command
func (ckmgr *CheckpointManager) composeUserAgent() {
	spec := ckmgr.pipeline.Specification().GetReplicationSpec()
	ckmgr.user_agent = base.ComposeUserAgentWithBucketNames("Goxdcr CkptMgr", spec.SourceBucketName, spec.TargetBucketName)
}

func (ckmgr *CheckpointManager) initConnections() error {
	var err error
	if ckmgr.target_cluster_ref.IsFullEncryption() {
		err = ckmgr.initSSLConStrMap()
		if err != nil {
			ckmgr.logger.Errorf("%v %v failed to initialize ssl connection string map, err=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), err)
			return err
		}
	}

	for server_addr, _ := range ckmgr.target_kv_vb_map {
		client, err := ckmgr.getNewMemcachedClient(server_addr, true /*initializing*/)
		if err != nil {
			ckmgr.logger.Errorf("%v %v failed to construct memcached client for %v, err=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), server_addr, err)
			return err
		}
		// no need to lock since this is done during initialization
		ckmgr.kv_mem_clients[server_addr] = client
	}

	return nil
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
			san_in_certificate, _, _, err = ckmgr.utils.GetSecuritySettingsAndDefaultPoolInfo("" /*hostAddr*/, connStr,
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

func (ckmgr *CheckpointManager) getHighSeqnoAndVBUuidFromTarget(fin_ch chan bool) map[uint16][]uint64 {
	ckmgr.kv_mem_clients_lock.Lock()
	defer ckmgr.kv_mem_clients_lock.Unlock()

	// A map of vbucketID -> slice of 2 elements of 1)HighSeqNo and 2)VbUuid in that order
	high_seqno_and_vbuuid_map := make(map[uint16][]uint64)
	for serverAddr, vbnos := range ckmgr.target_kv_vb_map {
		ckmgr.getHighSeqnoAndVBUuidForServerWithRetry(serverAddr, vbnos, high_seqno_and_vbuuid_map, fin_ch)
	}
	ckmgr.logger.Infof("high_seqno_and_vbuuid_map=%v\n", high_seqno_and_vbuuid_map)
	return high_seqno_and_vbuuid_map
}

// checkpointing cannot be done without high seqno and vbuuid from target
// if retrieval of such stats fails, retry
func (ckmgr *CheckpointManager) getHighSeqnoAndVBUuidForServerWithRetry(serverAddr string, vbnos []uint16, high_seqno_and_vbuuid_map map[uint16][]uint64, fin_ch chan bool) {
	var stats_map map[string]string

	statMapOp := func(param interface{}) (interface{}, error) {
		var err error
		client, ok := ckmgr.kv_mem_clients[serverAddr]
		if !ok {
			// memcached connection may have been closed in previous retries. create a new one
			client, err = ckmgr.getNewMemcachedClient(serverAddr, false /*initializing*/)
			if err != nil {
				ckmgr.logger.Warnf("%v %v Retrieval of high seqno and vbuuid stats failed. serverAddr=%v, vbnos=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), serverAddr, vbnos)
				return nil, err
			} else {
				ckmgr.kv_mem_clients[serverAddr] = client
			}
		}

		stats_map, err = client.StatsMap(base.VBUCKET_SEQNO_STAT_NAME)
		if err != nil {
			ckmgr.logger.Warnf("%v %v Error getting vbucket-seqno stats for serverAddr=%v. vbnos=%v, err=%v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), serverAddr, vbnos, err)
			clientCloseErr := client.Close()
			if clientCloseErr != nil {
				ckmgr.logger.Warnf("%v %v error from closing connection for %v is %v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), serverAddr, err)
			}
			delete(ckmgr.kv_mem_clients, serverAddr)
		}
		return nil, err
	}

	opErr, _ := ckmgr.utils.ExponentialBackoffExecutorWithFinishSignal("StatsMapOnVBToSeqno", base.RemoteMcRetryWaitTime, base.MaxRemoteMcRetry,
		base.RemoteMcRetryFactor, statMapOp, nil, fin_ch)

	if opErr != nil {
		ckmgr.logger.Errorf("%v %v Retrieval of high seqno and vbuuid stats failed after %v retries. serverAddr=%v, vbnos=%v\n",
			ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), base.MaxRemoteMcRetry, serverAddr, vbnos)
	} else {
		ckmgr.utils.ParseHighSeqnoAndVBUuidFromStats(vbnos, stats_map, high_seqno_and_vbuuid_map)
	}
}

func (ckmgr *CheckpointManager) Stop() error {
	//send signal to checkpoiting routine to exit
	close(ckmgr.finish_ch)

	//close the connections
	if !ckmgr.isTargetES {
		ckmgr.closeConnections()
	}

	ckmgr.wait_grp.Wait()
	return nil
}

func (ckmgr *CheckpointManager) CheckpointBeforeStop() {
	var specExists bool
	switch ckmgr.pipeline.Type() {
	case common.MainPipeline:
		spec, _ := ckmgr.rep_spec_svc.ReplicationSpec(ckmgr.pipeline.Topic())
		specExists = spec != nil
	case common.BackfillPipeline:
		backfillSpec, _ := ckmgr.backfillReplSvc.BackfillReplSpec(ckmgr.pipeline.Topic())
		specExists = backfillSpec != nil
		if specExists {
			if !backfillSpec.VBTasksMap.ContainsAtLeastOneTask() {
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

//get the lis of source vbuckets that this pipleline instance responsible.
//In current deployment - ReplicationManager coexist with source node, it means
//the list of buckets on that source node
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

func (ckmgr *CheckpointManager) updateCurrentVBOpaque(vbno uint16, vbOpaque metadata.TargetVBOpaque) error {
	obj, ok := ckmgr.cur_ckpts[vbno]
	if ok {
		obj.lock.Lock()
		defer obj.lock.Unlock()
		record := obj.ckpt
		record.Target_vb_opaque = vbOpaque
		return nil
	} else {
		err := fmt.Errorf("%v %v Trying to update vbopaque on vb=%v which is not in MyVBList", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
		ckmgr.handleGeneralError(err)
		return err
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
		} else {
			// otherwise, delete the checkpoint doc since it is no longer valid

			// ignore errors, which should have been logged
			checkpoints_svc.DelCheckpointsDoc(topic, vbno)
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
func (ckmgr *CheckpointManager) DelSingleVBCheckpoint(topic string, vbno uint16) error {
	ckmgr.bpVbMtx.RLock()
	hasCkpt := ckmgr.bpVbHasCkpt[vbno]
	ckmgr.bpVbMtx.RUnlock()

	if !hasCkpt {
		return nil
	}

	err := ckmgr.checkpoints_svc.DelCheckpointsDoc(topic, vbno)
	if err == service_def.MetadataNotFoundErr {
		err = nil
	}
	return err
}

/**
 * As part of starting the pipeline, a go routine is launched with this as the entrypoint to figure out the
 * VB timestamp, (see types.go's VBTimestamp struct) for each of the vbucket that this node is responsible for.
 * It does the followings:
 * 1. Gets the checkpoint docs from the checkpoint service (metakv)
 * 2. Removes any invalid ckpt docs.
 * 3. Figures out all VB opaques from remote cluster, and updates the local checkpoint records (with lock) to reflect that.
 * The timestamps are to be consumed by dcp nozzle to determine the start point of dcp stream/replication via settings map (UpdateSettings).
 */
func (ckmgr *CheckpointManager) SetVBTimestamps(topic string) error {
	defer ckmgr.logger.Infof("%v %v Done with SetVBTimestamps", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
	defer func() {
		atomic.StoreUint32(&ckmgr.checkpointOpAllowed, 1)
	}()
	ckmgr.logger.Infof("Set start seqnos for %v %v...", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())

	listOfVbs := ckmgr.getMyVBs()

	// Get persisted checkpoints from metakv - each valid vbucket has a *metadata.CheckpointsDoc
	ckmgr.logger.Infof("Getting checkpoint for %v\n", topic)
	ckptDocs, err := ckmgr.checkpoints_svc.CheckpointsDocs(topic, true /*needBrokenMapping*/)
	if err != nil {
		ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
		return err
	}
	ckmgr.loadBrokenMapFromCkptDocs(ckptDocs)
	ckmgr.logger.Infof("Found %v checkpoint documents for replication %v\n", len(ckptDocs), topic)

	ckmgr.initBpMapIfNeeded(ckptDocs)

	deleted_vbnos := make([]uint16, 0)
	target_support_xattr_now := base.IsClusterCompatible(ckmgr.target_cluster_version, base.VersionForRBACAndXattrSupport)
	specInternalId := ckmgr.pipeline.Specification().GetReplicationSpec().InternalId

	var brokenMappingModified bool
	// Figure out if certain checkpoints need to be removed to force a complete resync due to external factors
	for vbno, ckptDoc := range ckptDocs {
		if !base.IsVbInList(vbno, listOfVbs) {
			// if the vbno is no longer managed by the current checkpoint manager/pipeline,
			// the checkpoint doc is no longer valid and needs to be deleted
			// ignore errors, which should have been logged
			err = ckmgr.checkpoints_svc.DelCheckpointsDoc(topic, vbno)
			if err == nil {
				ckmgr.postDelCheckpointsDocWrapper(topic, ckptDoc, &brokenMappingModified)
			}
			deleted_vbnos = append(deleted_vbnos, vbno)
		} else if target_support_xattr_now {
			target_support_xattr_in_ckpt_doc := base.IsClusterCompatible(ckptDoc.TargetClusterVersion, base.VersionForRBACAndXattrSupport)
			if !target_support_xattr_in_ckpt_doc && ckptDoc.XattrSeqno > 0 {
				// if target did not support xattr when checkpoint records were created,
				// and target supports xattr now when pipeline is being restarted,
				// and the corresponding vbucket has seen xattr enabled mutations
				// we need to rollback to 0 for vbuckets that have seen xattr enabled mutations
				err = ckmgr.checkpoints_svc.DelCheckpointsDoc(topic, vbno)
				if err != nil {
					ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
					return err
				}
				ckmgr.postDelCheckpointsDocWrapper(topic, ckptDoc, &brokenMappingModified)
				deleted_vbnos = append(deleted_vbnos, vbno)
			}
		} else {
			if ckptDoc.SpecInternalId != specInternalId {
				// if specInternalId does not match, replication spec has been deleted and recreated
				// the checkpoint doc is for the old replication spec and needs to be deleted
				// unlike the IsVbInList check above, it is critial for the checkpoint doc deletion to succeed here
				// restart pipeline if it fails
				err = ckmgr.checkpoints_svc.DelCheckpointsDoc(topic, vbno)
				if err != nil {
					ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
					return err
				}
				ckmgr.postDelCheckpointsDocWrapper(topic, ckptDoc, &brokenMappingModified)
				deleted_vbnos = append(deleted_vbnos, vbno)
			}
		}
	}

	if brokenMappingModified {
		// For potential deleted checkpoints, ensure the brokenMapping is synced with metakv
		ckmgr.CommitBrokenMappingUpdates()
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

	for {
		end_index := start_index + workload
		if end_index > len(listOfVbs) {
			end_index = len(listOfVbs)
		}
		vbs_for_getter := listOfVbs[start_index:end_index]
		getter_wait_grp.Add(1)
		go ckmgr.startSeqnoGetter(getter_id, vbs_for_getter, ckptDocs, getter_wait_grp, err_ch)

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

	ckmgr.logger.Infof("Done with setting starting seqno for %v %v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())

	return nil
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

func (ckmgr *CheckpointManager) postDelCheckpointsDocWrapper(topic string, ckptDoc *metadata.CheckpointsDoc, brokenMappingModified *bool) {
	if ckptDoc == nil {
		return
	}

	modified, err := ckmgr.checkpoints_svc.PostDelCheckpointsDoc(topic, ckptDoc)
	if err != nil {
		ckmgr.logger.Warnf("Checkpoint service postDelCheckpointsDoc returned %v", err)
	}
	if modified {
		*brokenMappingModified = true
	}
}

func (ckmgr *CheckpointManager) loadBrokenMapFromCkptDocs(ckptDocs map[uint16]*metadata.CheckpointsDoc) {
	// When loading, just get the biggest brokenmap "fishing net" possible
	var highestManifestId uint64

	ckmgr.cachedBrokenMap.lock.Lock()
	defer ckmgr.cachedBrokenMap.lock.Unlock()

	for _, ckptDoc := range ckptDocs {
		if ckptDoc == nil {
			continue
		}
		for _, record := range ckptDoc.Checkpoint_records {
			if record == nil || record.BrokenMappings() == nil {
				continue
			}
			if record.TargetManifest > highestManifestId {
				// Simply set the new map
				ckmgr.cachedBrokenMap.brokenMap = *record.BrokenMappings()
				if ckmgr.cachedBrokenMap.brokenMap == nil {
					ckmgr.cachedBrokenMap.brokenMap = make(metadata.CollectionNamespaceMapping)
				}
			} else if record.TargetManifest == highestManifestId {
				// Same version - consolidate it into a bigger fishing net
				ckmgr.cachedBrokenMap.brokenMap.Consolidate(*record.BrokenMappings())
			}
		}
	}
}

func (ckmgr *CheckpointManager) setTimestampForVB(vbno uint16, ts *base.VBTimestamp) error {
	ckmgr.logger.Infof("%v %v Set VBTimestamp: vb=%v, ts.Seqno=%v, ts.SourceManifestId=%v ts.TargetManifestId=%v\n",
		ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno, ts.Seqno, ts.ManifestIDs.SourceManifestId, ts.ManifestIDs.TargetManifestId)

	//set the start seqno on through_seqno_tracker_svc
	ckmgr.through_seqno_tracker_svc.SetStartSeqno(vbno, ts.Seqno, ts.ManifestIDs.SourceManifestId)

	settings := make(map[string]interface{})
	ts_map := make(map[uint16]*base.VBTimestamp)
	ts_map[vbno] = ts
	settings[base.VBTimestamps] = ts_map
	//notify the settings change
	ckmgr.pipeline.UpdateSettings(settings)

	return nil
}

func (ckmgr *CheckpointManager) restoreBrokenMappingManifestsToRouter(brokenMapping *metadata.CollectionNamespaceMapping, manifestId uint64) {
	if brokenMapping == nil || len(*brokenMapping) == 0 || manifestId == 0 {
		return
	}
	settings := make(map[string]interface{})
	var pairList []interface{}
	pairList = append(pairList, brokenMapping)
	pairList = append(pairList, manifestId)
	settings[metadata.BrokenMappingsPair] = pairList
	ckmgr.pipeline.UpdateSettings(settings)
}

func (ckmgr *CheckpointManager) startSeqnoGetter(getter_id int, listOfVbs []uint16, ckptDocs map[uint16]*metadata.CheckpointsDoc,
	waitGrp *sync.WaitGroup, err_ch chan interface{}) {
	ckmgr.logger.Infof("%v %v StartSeqnoGetter %v is started to do _pre_prelicate for vbs %v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), getter_id, listOfVbs)
	defer waitGrp.Done()

	for _, vbno := range listOfVbs {
		// use math.MaxUint64 as max_seqno to make all checkpoint records eligible
		vbts, vbStats, brokenMapping, targetManifestId, err := ckmgr.getDataFromCkpts(vbno, ckptDocs[vbno], math.MaxUint64)
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
		ckmgr.restoreBrokenMappingManifestsToRouter(brokenMapping, targetManifestId)
		err = ckmgr.setTimestampForVB(vbno, vbts)
		if err != nil {
			err_info := []interface{}{vbno, err}
			err_ch <- err_info
			return
		}
	}
}

// Given a specific vbno and a list of checkpoints and a max possible seqno, return:
// valid VBTimestamp and corresponding VB-specific stats for statsMgr that was stored in the same ckpt doc
func (ckmgr *CheckpointManager) getDataFromCkpts(vbno uint16, ckptDoc *metadata.CheckpointsDoc, max_seqno uint64) (*base.VBTimestamp, service_def.VBCountMetricMap, *metadata.CollectionNamespaceMapping, uint64, error) {
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
		if ckpt_record != nil && ckpt_record.Seqno <= max_seqno {
			remote_vb_status = &service_def.RemoteVBReplicationStatus{VBOpaque: ckpt_record.Target_vb_opaque,
				VBSeqno: ckpt_record.Target_Seqno,
				VBNo:    vbno}
			ckmgr.logger.Debugf("%v %v Remote bucket %v vbno %v checking to see if it could agreed on the checkpoint %v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), ckmgr.remote_bucket, vbno, ckpt_record)
		}
		ckptRecord.lock.RUnlock()

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
					return nil, nil, nil, 0, err
				}
			}

			ckmgr.updateCurrentVBOpaque(vbno, current_remoteVBOpaque)
			ckmgr.logger.Debugf("%v %v Remote vbucket %v has a new opaque %v, update\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), current_remoteVBOpaque, vbno)
			ckmgr.logger.Debugf("%v %v Done with _pre_prelicate call for %v for vbno=%v, bMatch=%v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), remote_vb_status, vbno, bMatch)

			if bMatch {
				ckmgr.logger.Debugf("%v %v Remote bucket %v vbno %v agreed on the checkpoint above\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), ckmgr.remote_bucket, vbno)
				if ckptDoc != nil {
					agreedIndex = index
				}
				goto POPULATE
			}
		} // end if remote_vb_status
	}
POPULATE:
	vbts, brokenMappings, targetManifestId, err := ckmgr.populateDataFromCkptDoc(ckptDoc, agreedIndex, vbno)
	if err != nil {
		return vbts, nil, nil, 0, err
	}
	vbStatMap := NewVBStatsMapFromCkpt(ckptDoc, agreedIndex)
	return vbts, vbStatMap, brokenMappings, targetManifestId, err
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

func (ckmgr *CheckpointManager) populateDataFromCkptDoc(ckptDoc *metadata.CheckpointsDoc, agreedIndex int, vbno uint16) (*base.VBTimestamp, *metadata.CollectionNamespaceMapping, uint64, error) {
	vbts := &base.VBTimestamp{Vbno: vbno}
	var brokenMapping *metadata.CollectionNamespaceMapping
	var targetManifestId uint64
	var backfillBypassAgreedIndex bool
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

			vbts.ManifestIDs.SourceManifestId = ckpt_record.SourceManifest
			vbts.ManifestIDs.TargetManifestId = ckpt_record.TargetManifest

			brokenMapping = ckpt_record.BrokenMappings()
			targetManifestId = ckpt_record.TargetManifest
		}
	} else if ckmgr.pipeline.Type() == common.BackfillPipeline {
		// Check to see if there's a starting point to use instead of 0
		ckmgr.backfillStartingTsMtx.RLock()
		startingTs, exists := ckmgr.backfillStartingTs[vbno]
		ckmgr.backfillStartingTsMtx.RUnlock()
		if exists {
			backfillBypassAgreedIndex = true
			vbts.Vbuuid = startingTs.Vbuuid
			vbts.Seqno = startingTs.Seqno
			vbts.SnapshotStart = startingTs.SnapshotStart
			vbts.SnapshotEnd = startingTs.SnapshotEnd
			vbts.ManifestIDs = startingTs.ManifestIDs
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
			if brokenMapping != nil {
				obj.ckpt.LoadBrokenMapping(*brokenMapping)
			}
			obj.ckpt.TargetManifest = targetManifestId
		}
	} else {
		err := fmt.Errorf("%v %v Calling populateDataFromCkptDoc on vb=%v which is not in MyVBList", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
		ckmgr.handleGeneralError(err)
		return nil, nil, 0, err
	}
	return vbts, brokenMapping, targetManifestId, nil
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
		ckmgr.logger.Warnf("Unable to pre-persist brokenMapping: %v", err)
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
	ckmgr.logger.Infof("Start one time checkpointing for replication %v %v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
	defer ckmgr.logger.Infof("Done one time checkpointing for replication %v %v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())

	var through_seqno_map map[uint16]uint64
	var high_seqno_and_vbuuid_map map[uint16][]uint64
	var xattr_seqno_map map[uint16]uint64
	var srcManifestIds map[uint16]uint64
	var tgtManifestIds map[uint16]uint64
	if !ckmgr.isTargetES {
		// get through seqnos for all vbuckets in the pipeline
		through_seqno_map, srcManifestIds, tgtManifestIds = ckmgr.through_seqno_tracker_svc.GetThroughSeqnosAndManifestIds()
		// Let statsmgr know of the latest through seqno for it to prepare data for checkpointing
		ckmgr.statsMgr.HandleLatestThroughSeqnos(through_seqno_map)
		// get high seqno and vbuuid for all vbuckets in the pipeline
		high_seqno_and_vbuuid_map = ckmgr.getHighSeqnoAndVBUuidFromTarget(fin_ch)
		// get first seen xattr seqnos for all vbuckets in the pipeline
		xattr_seqno_map = pipeline_utils.GetXattrSeqnos(ckmgr.pipeline)
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
		go ckmgr.performCkpt_internal(vb_list_worker, fin_ch, worker_wait_grp, 0, through_seqno_map, high_seqno_and_vbuuid_map, xattr_seqno_map,
			&total_committing_time, srcManifestIds, tgtManifestIds)
	}

	//wait for all the getter done, then gather result
	worker_wait_grp.Wait()
	ckmgr.CommitBrokenMappingUpdates()
	ckmgr.collectionsManifestSvc.PersistNeededManifests(ckmgr.pipeline.Specification().GetReplicationSpec())
	ckmgr.RaiseEvent(common.NewEvent(common.CheckpointDone, nil, ckmgr, nil, time.Duration(total_committing_time)*time.Nanosecond))

}

// local API. supports periodical checkpoint operations
func (ckmgr *CheckpointManager) performCkpt(fin_ch chan bool, wait_grp *sync.WaitGroup) {
	ckmgr.logger.Infof("Start checkpointing for replication %v %v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
	defer ckmgr.logger.Infof("Done checkpointing for replication %v %v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
	// vbucketID -> ThroughSeqNumber
	var through_seqno_map map[uint16]uint64
	// vBucketID -> slice of 2 elements of 1)HighSeqNo and 2)VbUuid
	var high_seqno_and_vbuuid_map map[uint16][]uint64
	// map of vbucketID -> the seqno that corresponds to the first occurrence of xattribute
	var xattr_seqno_map map[uint16]uint64
	var srcManifestIds map[uint16]uint64
	var tgtManifestIds map[uint16]uint64
	if !ckmgr.isTargetES {
		// get through seqnos for all vbuckets in the pipeline
		through_seqno_map, srcManifestIds, tgtManifestIds = ckmgr.through_seqno_tracker_svc.GetThroughSeqnosAndManifestIds()
		// Let statsmgr know of the latest through seqno for it to prepare data for checkpointing
		ckmgr.statsMgr.HandleLatestThroughSeqnos(through_seqno_map)
		// get high seqno and vbuuid for all vbuckets in the pipeline
		high_seqno_and_vbuuid_map = ckmgr.getHighSeqnoAndVBUuidFromTarget(fin_ch)
		// get first seen xattr seqnos for all vbuckets in the pipeline
		xattr_seqno_map = pipeline_utils.GetXattrSeqnos(ckmgr.pipeline)
	}
	var total_committing_time int64

	ckmgr.PreCommitBrokenMapping()
	ckmgr.performCkpt_internal(ckmgr.getMyVBs(), fin_ch, wait_grp, ckmgr.getCheckpointInterval(), through_seqno_map, high_seqno_and_vbuuid_map,
		xattr_seqno_map, &total_committing_time, srcManifestIds, tgtManifestIds)
	ckmgr.CommitBrokenMappingUpdates()
	ckmgr.collectionsManifestSvc.PersistNeededManifests(ckmgr.pipeline.Specification().GetReplicationSpec())
	ckmgr.RaiseEvent(common.NewEvent(common.CheckpointDone, nil, ckmgr, nil, time.Duration(total_committing_time)*time.Nanosecond))

}

func (ckmgr *CheckpointManager) isCheckpointAllowed() bool {
	return atomic.LoadUint32(&ckmgr.checkpointOpAllowed) > 0
}

func (ckmgr *CheckpointManager) performCkpt_internal(vb_list []uint16, fin_ch <-chan bool, wait_grp *sync.WaitGroup, time_to_wait time.Duration,
	through_seqno_map map[uint16]uint64, high_seqno_and_vbuuid_map map[uint16][]uint64, xattr_seqno_map map[uint16]uint64, total_committing_time *int64,
	srcManifestIds, tgtManifestIds map[uint16]uint64) {

	defer wait_grp.Done()

	var interval_btwn_vb time.Duration
	if time_to_wait != 0 {
		interval_btwn_vb = time.Duration((time_to_wait.Seconds()/float64(len(vb_list)))*1000) * time.Millisecond
	}

	if !ckmgr.isCheckpointAllowed() {
		ckmgr.logger.Errorf("%v %v has not been started - checkpointing is skipped", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
		return
	}

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
			size, err := ckmgr.doCheckpoint(vb, through_seqno_map, high_seqno_and_vbuuid_map, xattr_seqno_map, srcManifestIds, tgtManifestIds)
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
func (ckptRecord *checkpointRecordWithLock) updateAndPersist(ckmgr *CheckpointManager, vbno uint16, versionNumberIn uint64,
	xattrSeqno uint64, incomingRecord *metadata.CheckpointRecord) (int, error) {

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
	persistSize, persistErr := ckmgr.persistCkptRecord(vbno, ckptRecord.ckpt, xattrSeqno)
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
func (ckmgr *CheckpointManager) doCheckpoint(vbno uint16, through_seqno_map map[uint16]uint64,
	high_seqno_and_vbuuid_map map[uint16][]uint64, xattr_seqno_map, srcManifestIds, tgtManifestIds map[uint16]uint64) (size int, err error) {
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

	// if target vb opaque has not changed, persist checkpoint record
	ckptRecordTargetSeqno := remote_seqno
	// Item 2:
	// Get the failover_UUID here
	ckRecordFailoverUuid, failoverUuidErr := ckmgr.getFailoverUUIDForSeqno(vbno, through_seqno)
	if failoverUuidErr != nil {
		// if we cannot find uuid for the checkpoint seqno, the checkpoint seqno is unusable
		// skip checkpointing of this vb
		// return nil so that we can continue to checkpoint the next vb
		ckmgr.logger.Warnf("%v\n", failoverUuidErr.Error())
		err = nil
		return
	}

	// Go through the local snapshot records repository and figure out which snapshot contains this latest sequence number
	// Item: 5 and 6
	ckRecordDcpSnapSeqno, ckRecordDcpSnapEndSeqno, snapErr := ckmgr.getSnapshotForSeqno(vbno, through_seqno)
	if snapErr != nil {
		// if we cannot find snapshot for the checkpoint seqno, the checkpoint seqno is still usable in normal cases,
		// just that we may have to rollback to 0 when rollback is needed
		// In rare cases, snapshot start/end seqno is needed only when DCP cannot simply use checkpoint seqno as the start seqno.
		// The logic is in ep-engine/src/failover-table.cc
		// log the problem and proceed
		ckmgr.logger.Warnf("%v\n", snapErr.Error())
	}

	xattr_seqno, err := ckmgr.getXattrSeqno(vbno, xattr_seqno_map, through_seqno)
	if err != nil {
		ckmgr.logger.Warnf("%v %v skipping checkpointing for vb=%v. err=%v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno, err)
		err = nil
		return
	}

	// Get stats that need to persist
	vbCountMetrics, err := ckmgr.statsMgr.GetVBCountMetrics(vbno)
	if err != nil {
		ckmgr.logger.Warnf("%v unable to get %v metric from stats manager\n", DOCS_FILTERED_METRIC)
		return
	}
	filteredItems := vbCountMetrics[DOCS_FILTERED_METRIC]
	filterFailed := vbCountMetrics[DOCS_UNABLE_TO_FILTER_METRIC]

	// Collection-Related items
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

	// Write-operation - feed the temporary variables and update them into the record and also write them to metakv
	newCkpt, err := metadata.NewCheckpointRecord(ckRecordFailoverUuid, through_seqno, ckRecordDcpSnapSeqno, ckRecordDcpSnapEndSeqno, ckptRecordTargetSeqno,
		uint64(filteredItems), uint64(filterFailed), srcManifestId, tgtManifestId, brokenMapping)
	if err != nil {
		ckmgr.logger.Errorf("Unable to create a checkpoint record due to - %v", err)
		err = nil
		return
	}

	size, err = ckpt_obj.updateAndPersist(ckmgr, vbno, currRecordVersion, xattr_seqno, newCkpt)

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

func (ckmgr *CheckpointManager) getThroughSeqno(vbno uint16, through_seqno_map map[uint16]uint64) (uint64, error) {
	var through_seqno uint64
	if !ckmgr.isTargetES {
		// non-capi mode, get from through_seqno_map
		var ok bool
		through_seqno, ok = through_seqno_map[vbno]
		if !ok {
			return 0, fmt.Errorf("%v %v cannot find through seqno for vb %v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
		}
	} else {
		// capi mode
		through_seqno = ckmgr.through_seqno_tracker_svc.GetThroughSeqno(vbno)
	}
	return through_seqno, nil
}

func (ckmgr *CheckpointManager) getRemoteSeqno(vbno uint16, high_seqno_and_vbuuid_map map[uint16][]uint64, curCkptTargetVBOpaque metadata.TargetVBOpaque) (uint64, error) {
	if !ckmgr.isTargetES {
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
	} else {
		// capi mode, get high_seqno and vbuuid on target by calling commitForCheckpoint
		remote_seqno, targetVBOpaque, commitForCheckpointErr := ckmgr.capi_svc.CommitForCheckpoint(ckmgr.remote_bucket, curCkptTargetVBOpaque, vbno)
		if commitForCheckpointErr != nil {
			if commitForCheckpointErr == service_def.VB_OPAQUE_MISMATCH_ERR {
				ckmgr.logger.Errorf("%v %v target vbuuid has changed for vb=%v. old=%v, new=%v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno, curCkptTargetVBOpaque, targetVBOpaque)
				return 0, targetVbuuidChangedError
			} else {
				return 0, fmt.Errorf("%v %v error contacting capi service when calling CommitForCheckpoint for vb %v. err=%v\n", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno, commitForCheckpointErr)
			}
		}
		return remote_seqno, nil
	}
}

func (ckmgr *CheckpointManager) getXattrSeqno(vbno uint16, xattr_seqno_map map[uint16]uint64, through_seqno uint64) (uint64, error) {
	if ckmgr.isTargetES {
		// xattr not supported by capi
		return 0, nil
	}

	xattr_seqno, ok := xattr_seqno_map[vbno]
	if !ok {
		return 0, fmt.Errorf("%v %v cannot find xattr seqno for vb %v", ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic(), vbno)
	}
	if xattr_seqno != 0 && xattr_seqno > through_seqno {
		// if xattr_seqno is larger than through_seqno, rollback is not needed
		// set xattr_seqno to 0 to avoid triggering unnecessary rollback
		// This case is possible if the mutation related to this sequence number has been sent to the target, and stored in the map
		// but the target has not yet responded, so that the through_seq_number service hasn't been able to process it yet.
		xattr_seqno = 0
	}

	return xattr_seqno, nil
}

func (ckmgr *CheckpointManager) raiseSuccessCkptForVbEvent(ckpt_record metadata.CheckpointRecord, vbno uint16) {
	//notify statisticsManager
	ckmgr.RaiseEvent(common.NewEvent(common.CheckpointDoneForVB, ckpt_record, ckmgr, nil, vbno))
}

func (ckmgr *CheckpointManager) persistCkptRecord(vbno uint16, ckpt_record *metadata.CheckpointRecord, xattr_seqno uint64) (int, error) {
	ckmgr.logger.Debugf("Persist vb=%v ckpt_record=%v for %v %v\n", vbno, ckpt_record, ckmgr.pipeline.Type().String(), ckmgr.pipeline.FullTopic())
	return ckmgr.checkpoints_svc.UpsertCheckpoints(ckmgr.pipeline.FullTopic(), ckmgr.pipeline.Specification().GetReplicationSpec().InternalId, vbno, ckpt_record, xattr_seqno, ckmgr.target_cluster_version)
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

	if len(added) > 0 || len(removed) > 0 {
		spec := ckmgr.pipeline.Specification().GetReplicationSpec()
		ref, err := ckmgr.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, false /*refresh*/)
		if err != nil || ref == nil {
			ckmgr.logger.Warnf("Unable to write to UI log service the update.\nNewly broken: %v Repaired: %v\n",
				added, removed)
			return
		}
		buffer.WriteString(fmt.Sprintf("XDCR SourceBucket %v to Target Cluster %v Bucket %v\n",
			spec.SourceBucketName, ref.Name(), spec.TargetBucketName))
		needToRaiseUI = true
	}

	if len(added) > 0 {
		buffer.WriteString("The followings are collection mappings that are newly broken:\n")
		buffer.WriteString(added.String())
	}

	if len(removed) > 0 {
		buffer.WriteString("The followings are collection mappings that are now repaired:\n")
		buffer.WriteString(removed.String())
	}

	if needToRaiseUI {
		ckmgr.uiLogSvc.Write(buffer.String())
		// TODO MB-38507 - Once UI portion is done and can parse replicationStatus with broken mapping, remove this log
		ckmgr.logger.Infof("Current broken mapping for pipeline %v: %v", ckmgr.pipeline.FullTopic(), newerMap.String())
	}
}

func (ckmgr *CheckpointManager) getFailoverUUIDForSeqno(vbno uint16, seqno uint64) (uint64, error) {
	failoverlog_obj, ok1 := ckmgr.failoverlog_map[vbno]
	if ok1 {
		failoverlog_obj.lock.RLock()
		defer failoverlog_obj.lock.RUnlock()

		flog := failoverlog_obj.failoverlog
		if flog != nil {
			for _, entry := range *flog {
				failover_uuid := entry[0]
				starting_seqno := entry[1]
				if seqno >= starting_seqno {
					return failover_uuid, nil
				}
			}
		}
	} else {
		err := fmt.Errorf("%v Calling getFailoverUUIDForSeqno on an unknown vb=%v\n", ckmgr.pipeline.FullTopic(), vbno)
		ckmgr.handleGeneralError(err)
		return 0, err
	}
	return 0, fmt.Errorf("%v Failed to find vbuuid for vb=%v, seqno=%v\n", ckmgr.pipeline.FullTopic(), vbno, seqno)
}

// find the snapshot to which the checkpoint seqno belongs
func (ckmgr *CheckpointManager) getSnapshotForSeqno(vbno uint16, seqno uint64) (uint64, uint64, error) {
	snapshot_history_obj, ok1 := ckmgr.snapshot_history_map[vbno]
	if ok1 {
		snapshot_history_obj.lock.RLock()
		defer snapshot_history_obj.lock.RUnlock()
		for i := len(snapshot_history_obj.snapshot_history) - 1; i >= 0; i-- {
			cur_snapshot := snapshot_history_obj.snapshot_history[i]
			if seqno >= cur_snapshot.start_seqno && seqno <= cur_snapshot.end_seqno {
				return cur_snapshot.start_seqno, cur_snapshot.end_seqno, nil
			}
		}
	} else {
		err := fmt.Errorf("%v Calling getSnapshotForSeqno on an unknown vb=%v\n", ckmgr.pipeline.FullTopic(), vbno)
		ckmgr.handleGeneralError(err)
		return 0, 0, err
	}
	return 0, 0, fmt.Errorf("%v Failed to find snapshot for vb=%v, seqno=%v\n", ckmgr.pipeline.FullTopic(), vbno, seqno)
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

	vbts, vbStats, brokenMappings, targetManifestId, err := ckmgr.getDataFromCkpts(vbno, checkpointDoc, max_seqno)
	if err != nil {
		return nil, err
	}

	pipeline_startSeqnos_map[vbno] = vbts

	ckmgr.restoreBrokenMappingManifestsToRouter(brokenMappings, targetManifestId)

	//set the start seqno on through_seqno_tracker_svc
	ckmgr.through_seqno_tracker_svc.SetStartSeqno(vbno, vbts.Seqno, vbts.ManifestIDs.SourceManifestId)
	err = ckmgr.statsMgr.SetVBCountMetrics(vbno, vbStats)
	if err != nil {
		ckmgr.logger.Warnf("%v setting vbStat for vb %v returned error %v Stats: %v\n", ckmgr.pipeline.FullTopic(), vbno, err, vbStats)
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
