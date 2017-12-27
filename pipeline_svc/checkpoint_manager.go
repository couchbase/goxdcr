// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package pipeline_svc

import (
	"errors"
	"fmt"
	"github.com/couchbase/go-couchbase"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	component "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"math"
	"math/rand"
	"sync"
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

	//the interval between checkpointing
	ckpt_interval time.Duration

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

	// whether replication is of capi type
	capi bool

	user_agent string

	// these fields are used for xmem replication only
	// memcached clients for retrieval of target bucket stats
	kv_mem_clients      map[string]mcc.ClientIface
	kv_mem_clients_lock sync.RWMutex

	target_cluster_version int
	utils                  utilities.UtilsIface
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

func NewCheckpointManager(checkpoints_svc service_def.CheckpointsService, capi_svc service_def.CAPIService,
	remote_cluster_svc service_def.RemoteClusterSvc, rep_spec_svc service_def.ReplicationSpecSvc, cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc, through_seqno_tracker_svc service_def.ThroughSeqnoTrackerSvc,
	active_vbs map[string][]uint16, target_username, target_password string, target_bucket_name string,
	target_kv_vb_map map[string][]uint16, target_cluster_ref *metadata.RemoteClusterReference,
	target_cluster_version int, logger_ctx *log.LoggerContext, utilsIn utilities.UtilsIface) (*CheckpointManager, error) {
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
		utils: utilsIn,
	}, nil
}

func getSSLConStrMap(target_kv_vb_map map[string][]uint16, ssl_port_map map[string]uint16) (map[string]string, error) {
	ssl_con_str_map := make(map[string]string)

	for server_addr, _ := range target_kv_vb_map {
		ssl_port, ok := ssl_port_map[server_addr]
		if !ok {
			return nil, fmt.Errorf("Can't get remote memcached ssl port for %v", server_addr)
		}
		ssl_con_str := base.GetHostAddr(server_addr, ssl_port)
		ssl_con_str_map[server_addr] = ssl_con_str
	}

	return ssl_con_str_map, nil
}

func (ckmgr *CheckpointManager) Attach(pipeline common.Pipeline) error {

	ckmgr.logger.Infof("Attach checkpoint manager with pipeline %v\n", pipeline.Topic())

	ckmgr.pipeline = pipeline

	//populate the remote bucket information at the time of attaching
	err := ckmgr.populateRemoteBucketInfo(pipeline)

	if err != nil {
		return err
	}

	dcp_parts := pipeline.Sources()
	for _, dcp := range dcp_parts {
		dcp.RegisterComponentEventListener(common.StreamingStart, ckmgr)
		dcp.RegisterComponentEventListener(common.SnapshotMarkerReceived, ckmgr)
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

func (ckmgr *CheckpointManager) populateRemoteBucketInfo(pipeline common.Pipeline) error {
	topic := pipeline.Topic()
	spec, err := ckmgr.rep_spec_svc.ReplicationSpec(topic)
	if err != nil {
		return err
	}
	remote_bucket, err := service_def.NewRemoteBucketInfo(ckmgr.target_cluster_ref.Name, spec.TargetBucketName, ckmgr.target_cluster_ref, ckmgr.remote_cluster_svc, ckmgr.cluster_info_svc, ckmgr.logger, ckmgr.utils)
	if err != nil {
		return err
	}
	ckmgr.remote_bucket = remote_bucket

	ckmgr.checkCkptCapability()

	return nil
}

func (ckmgr *CheckpointManager) Start(settings map[string]interface{}) error {
	if ckpt_interval, ok := settings[CHECKPOINT_INTERVAL].(int); ok {
		ckmgr.ckpt_interval = time.Duration(ckpt_interval) * time.Second
	} else {
		return errors.New(fmt.Sprintf("%v should be provided in settings", CHECKPOINT_INTERVAL))
	}

	ckmgr.logger.Infof("%v CheckpointManager starting with ckpt_interval=%v s\n", ckmgr.pipeline.Topic(), ckmgr.ckpt_interval.Seconds())

	ckmgr.startRandomizedCheckpointingTicker()

	//initialize connections
	if !ckmgr.capi {
		err := ckmgr.initConnections()
		if err != nil {
			return err
		}
	}

	//start checkpointing loop
	ckmgr.wait_grp.Add(1)
	go ckmgr.checkpointing()
	return nil
}

func (ckmgr *CheckpointManager) startRandomizedCheckpointingTicker() {
	//randomize the starting point so that the checkpoint manager for the same
	//replication on different node the starting point is randomized.
	starting_time := time.Duration(rand.Intn(5000))*time.Millisecond + ckmgr.ckpt_interval
	ckmgr.logger.Infof("%v Checkpointing starts in %v sec", ckmgr.pipeline.Topic(), starting_time.Seconds())
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

	ckmgr.capi = ckmgr.pipeline.Specification().Settings.IsCapi()
}

// compose user agent string for HELO command
func (ckmgr *CheckpointManager) composeUserAgent() {
	spec := ckmgr.pipeline.Specification()
	ckmgr.user_agent = base.ComposeUserAgentWithBucketNames("Goxdcr CkptMgr", spec.SourceBucketName, spec.TargetBucketName)
}

func (ckmgr *CheckpointManager) initConnections() error {
	var err error
	if ckmgr.target_cluster_ref.IsFullEncryption() {
		err = ckmgr.initSSLConStrMap()
		if err != nil {
			ckmgr.logger.Errorf("%v failed to initialize ssl connection string map, err=%v\n", ckmgr.pipeline.Topic(), err)
			return err
		}
	}

	for server_addr, _ := range ckmgr.target_kv_vb_map {
		client, err := ckmgr.getNewMemcachedClient(server_addr)
		if err != nil {
			ckmgr.logger.Errorf("%v failed to construct memcached client for %v, err=%v\n", ckmgr.pipeline.Topic(), server_addr, err)
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

	username, password, certificate, sanInCertificate, err := ckmgr.target_cluster_ref.MyCredentials()
	if err != nil {
		return err
	}

	ssl_port_map, err := ckmgr.utils.GetMemcachedSSLPortMap(connStr, username, password, certificate, sanInCertificate, ckmgr.target_bucket_name, ckmgr.logger)
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

func (ckmgr *CheckpointManager) getNewMemcachedClient(server_addr string) (mcc.ClientIface, error) {
	if ckmgr.target_cluster_ref.IsFullEncryption() {
		_, _, certificate, sanInCertificate, err := ckmgr.target_cluster_ref.MyCredentials()
		if err != nil {
			return nil, err
		}
		ssl_con_str := ckmgr.ssl_con_str_map[server_addr]
		return base.NewTLSConn(ssl_con_str, ckmgr.target_username, ckmgr.target_password, certificate, sanInCertificate, ckmgr.target_bucket_name, ckmgr.logger)
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
			ckmgr.logger.Warnf("%v error from closing connection for %v is %v\n", ckmgr.pipeline.Topic(), server_addr, err)
		}
	}
	ckmgr.kv_mem_clients = make(map[string]mcc.ClientIface)
}

func (ckmgr *CheckpointManager) getHighSeqnoAndVBUuidFromTarget() map[uint16][]uint64 {
	ckmgr.kv_mem_clients_lock.Lock()
	defer ckmgr.kv_mem_clients_lock.Unlock()

	// A map of vbucketID -> slice of 2 elements of 1)HighSeqNo and 2)VbUuid in that order
	high_seqno_and_vbuuid_map := make(map[uint16][]uint64)
	for serverAddr, vbnos := range ckmgr.target_kv_vb_map {
		ckmgr.getHighSeqnoAndVBUuidForServerWithRetry(serverAddr, vbnos, high_seqno_and_vbuuid_map)
	}
	ckmgr.logger.Infof("high_seqno_and_vbuuid_map=%v\n", high_seqno_and_vbuuid_map)
	return high_seqno_and_vbuuid_map
}

// checkpointing cannot be done without high seqno and vbuuid from target
// if retrieval of such stats fails, retry
func (ckmgr *CheckpointManager) getHighSeqnoAndVBUuidForServerWithRetry(serverAddr string, vbnos []uint16, high_seqno_and_vbuuid_map map[uint16][]uint64) {
	var stats_map map[string]string

	statMapOp := func() error {
		var err error
		client, ok := ckmgr.kv_mem_clients[serverAddr]
		if !ok {
			// memcached connection may have been closed in previous retries. create a new one
			client, err = ckmgr.getNewMemcachedClient(serverAddr)
			if err != nil {
				ckmgr.logger.Warnf("%v Retrieval of high seqno and vbuuid stats failed. serverAddr=%v, vbnos=%v\n", ckmgr.pipeline.Topic(), serverAddr, vbnos)
				return err
			} else {
				ckmgr.kv_mem_clients[serverAddr] = client
			}
		}

		stats_map, err = client.StatsMap(base.VBUCKET_SEQNO_STAT_NAME)
		if err != nil {
			ckmgr.logger.Warnf("%v Error getting vbucket-seqno stats for serverAddr=%v. vbnos=%v, err=%v", ckmgr.pipeline.Topic(), serverAddr, vbnos, err)
			clientCloseErr := client.Close()
			if clientCloseErr != nil {
				ckmgr.logger.Warnf("%v error from closing connection for %v is %v\n", ckmgr.pipeline.Topic(), serverAddr, err)
			}
			delete(ckmgr.kv_mem_clients, serverAddr)
		}
		return err
	}

	opErr := ckmgr.utils.ExponentialBackoffExecutor("StatsMapOnVBToSeqno", base.RemoteMcRetryWaitTime, base.MaxRemoteMcRetry,
		base.RemoteMcRetryFactor, statMapOp)

	if opErr != nil {
		ckmgr.logger.Errorf("%v Retrieval of high seqno and vbuuid stats failed after %v retries. serverAddr=%v, vbnos=%v\n",
			ckmgr.pipeline.Topic(), base.MaxRemoteMcRetry, serverAddr, vbnos)
	} else {
		ckmgr.utils.ParseHighSeqnoAndVBUuidFromStats(vbnos, stats_map, high_seqno_and_vbuuid_map)
	}
}

func (ckmgr *CheckpointManager) Stop() error {
	//send signal to checkpoiting routine to exit
	close(ckmgr.finish_ch)

	//close the connections
	if !ckmgr.capi {
		ckmgr.closeConnections()
	}

	ckmgr.wait_grp.Wait()
	return nil
}

func (ckmgr *CheckpointManager) CheckpointBeforeStop() {
	spec, _ := ckmgr.rep_spec_svc.ReplicationSpec(ckmgr.pipeline.Topic())
	if spec == nil {
		// do not perform checkpoint if spec has been deleted
		ckmgr.logger.Infof("Skipping checkpointing for pipeline %v before stopping since replication spec has been deleted", ckmgr.pipeline.Topic())
		return
	}

	ckmgr.logger.Infof("Starting checkpointing for pipeline %v before stopping", ckmgr.pipeline.Topic())

	timeout_timer := time.NewTimer(base.TimeoutCheckpointBeforeStop)
	defer timeout_timer.Stop()

	ret := make(chan bool, 1)
	close_ch := make(chan bool, 1)

	go func(finch chan bool) {
		ckmgr.PerformCkpt(close_ch)
		finch <- true
	}(ret)

	for {
		select {
		case <-ret:
			ckmgr.logger.Infof("Checkpointing for pipeline %v completed", ckmgr.pipeline.Topic())
			return
		case <-timeout_timer.C:
			close(close_ch)
			ckmgr.logger.Infof("Checkpointing for pipeline %v timed out after %v.", ckmgr.pipeline.Topic(), base.TimeoutCheckpointBeforeStop)
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
	ckmgr.logger.Infof("%v Remote bucket %v supporting xdcrcheckpointing is %v\n", ckmgr.pipeline.Topic(), ckmgr.remote_bucket, ckmgr.support_ckpt)
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
		err := fmt.Errorf("%v Trying to update vbopaque on vb=%v which is not in MyVBList", ckmgr.pipeline.Topic(), vbno)
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
	ckptDocs, err := checkpoints_svc.CheckpointsDocs(topic)
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
	defer ckmgr.logger.Infof("%v Done with SetVBTimestamps", ckmgr.pipeline.Topic())
	ckmgr.logger.Infof("Set start seqnos for pipeline %v...", ckmgr.pipeline.Topic())

	listOfVbs := ckmgr.getMyVBs()

	// Get persisted checkpoints from metakv - each valid vbucket has a *metadata.CheckpointsDoc
	ckmgr.logger.Infof("Getting checkpoint for %v\n", topic)
	ckptDocs, err := ckmgr.checkpoints_svc.CheckpointsDocs(topic)
	if err != nil {
		ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
		return err
	}
	ckmgr.logger.Infof("Found %v checkpoint documents for replication %v\n", len(ckptDocs), topic)

	deleted_vbnos := make([]uint16, 0)
	target_support_xattr_now := base.IsClusterCompatible(ckmgr.target_cluster_version, base.VersionForRBACAndXattrSupport)
	specInternalId := ckmgr.pipeline.Specification().InternalId

	// Figure out if certain checkpoints need to be removed to force a complete resync due to external factors
	for vbno, ckptDoc := range ckptDocs {
		if !base.IsVbInList(vbno, listOfVbs) {
			// if the vbno is no longer managed by the current checkpoint manager/pipeline,
			// the checkpoint doc is no longer valid and needs to be deleted
			// ignore errors, which should have been logged
			ckmgr.checkpoints_svc.DelCheckpointsDoc(topic, vbno)
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

	ckmgr.logger.Infof("Done with setting starting seqno for pipeline %v\n", ckmgr.pipeline.Topic())

	return nil
}

func (ckmgr *CheckpointManager) setTimestampForVB(vbno uint16, ts *base.VBTimestamp) error {
	ckmgr.logger.Infof("%v Set VBTimestamp: vb=%v, ts.Seqno=%v\n", ckmgr.pipeline.Topic(), vbno, ts.Seqno)
	ckmgr.logger.Debugf("%v vb=%v ts=%v\n", ckmgr.pipeline.Topic(), vbno, ts)
	defer ckmgr.logger.Debugf("%v Set VBTimestamp for vb=%v completed\n", ckmgr.pipeline.Topic(), vbno)

	//set the start seqno on through_seqno_tracker_svc
	ckmgr.through_seqno_tracker_svc.SetStartSeqno(vbno, ts.Seqno)

	settings := make(map[string]interface{})
	ts_map := make(map[uint16]*base.VBTimestamp)
	ts_map[vbno] = ts
	settings[base.VBTimestamps] = ts_map
	//notify the settings change
	ckmgr.pipeline.UpdateSettings(settings)

	return nil
}

func (ckmgr *CheckpointManager) startSeqnoGetter(getter_id int, listOfVbs []uint16, ckptDocs map[uint16]*metadata.CheckpointsDoc,
	waitGrp *sync.WaitGroup, err_ch chan interface{}) {
	ckmgr.logger.Infof("%v StartSeqnoGetter %v is started to do _pre_prelicate for vbs %v\n", ckmgr.pipeline.Topic(), getter_id, listOfVbs)
	defer waitGrp.Done()

	for _, vbno := range listOfVbs {
		// use math.MaxUint64 as max_seqno to make all checkpoint records eligible
		vbts, err := ckmgr.getVBTimestampForVB(vbno, ckptDocs[vbno], math.MaxUint64)
		if err != nil {
			err_info := []interface{}{vbno, err}
			err_ch <- err_info
			return
		}
		err = ckmgr.setTimestampForVB(vbno, vbts)
		if err != nil {
			err_info := []interface{}{vbno, err}
			err_ch <- err_info
			return
		}
	}
}

// get start seqno for a specific vb that is less than max_seqno
func (ckmgr *CheckpointManager) getVBTimestampForVB(vbno uint16, ckptDoc *metadata.CheckpointsDoc, max_seqno uint64) (*base.VBTimestamp, error) {
	var agreeedIndex int = -1

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
			ckmgr.logger.Debugf("%v Remote bucket %v vbno %v checking to see if it could agreed on the checkpoint %v\n", ckmgr.pipeline.Topic(), ckmgr.remote_bucket, vbno, ckpt_record)
		}
		ckptRecord.lock.RUnlock()

		if remote_vb_status != nil {
			bMatch := false
			bMatch, current_remoteVBOpaque, err := ckmgr.capi_svc.PreReplicate(ckmgr.remote_bucket, remote_vb_status, ckmgr.support_ckpt)

			if err == nil {
				ckmgr.updateCurrentVBOpaque(vbno, current_remoteVBOpaque)
				ckmgr.logger.Debugf("%v Remote vbucket %v has a new opaque %v, update\n", ckmgr.pipeline.Topic(), current_remoteVBOpaque, vbno)
				ckmgr.logger.Debugf("%v Done with _pre_prelicate call for %v for vbno=%v, bMatch=%v", ckmgr.pipeline.Topic(), remote_vb_status, vbno, bMatch)
			}

			if err != nil || bMatch {
				if bMatch {
					ckmgr.logger.Debugf("%v Remote bucket %v vbno %v agreed on the checkpoint above\n", ckmgr.pipeline.Topic(), ckmgr.remote_bucket, vbno)
					if ckptDoc != nil {
						agreeedIndex = index
					}
				} else if err == service_def.NoSupportForXDCRCheckpointingError {
					ckmgr.updateCurrentVBOpaque(vbno, nil)
					ckmgr.logger.Infof("%v Remote vbucket %v is on a old node which doesn't support checkpointing, update target_vb_uuid=0\n", ckmgr.pipeline.Topic(), vbno)
				} else {
					ckmgr.logger.Errorf("%v Pre_replicate failed for %v. err=%v\n", ckmgr.pipeline.Topic(), vbno, err)
					return nil, err
				}
				goto POPULATE
			}
		} // end if remote_vb_status
	}
POPULATE:
	return ckmgr.populateVBTimestamp(ckptDoc, agreeedIndex, vbno)
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
		ckmgr.logger.Infof("%v Found checkpoint doc for vb=%v\n", ckmgr.pipeline.Topic(), vbno)
	} else {
		// First time XDCR checkpoint is executing, thus why ckptDoc is nil
		ret = append(ret, ckmgr.getCurrentCkptWLock(vbno))
	}

	return ret
}

func (ckmgr *CheckpointManager) UpdateSettings(settings map[string]interface{}) error {
	ckmgr.logger.Debugf("Updating settings on checkpoint manager for pipeline %v. settings=%v\n", ckmgr.pipeline.Topic(), settings)
	checkpoint_interval, err := ckmgr.utils.GetIntSettingFromSettings(settings, CHECKPOINT_INTERVAL)
	if err != nil {
		return err
	}

	if checkpoint_interval < 0 {
		// checkpoint_interval not specified. no op
		return nil
	}

	if int(ckmgr.ckpt_interval.Seconds()) == checkpoint_interval {
		// no op if no real updates
		return nil
	}

	// update checkpoint interval
	ckmgr.ckpt_interval = time.Duration(checkpoint_interval) * time.Second
	ckmgr.logger.Infof("Updated checkpoint interval to %v for pipeline %v.\n", checkpoint_interval, ckmgr.pipeline.Topic())
	ckmgr.startRandomizedCheckpointingTicker()

	return nil
}

type failoverLogRetriever struct {
	listOfVbs        []uint16
	sourceBucket     *couchbase.Bucket
	cur_failover_log couchbase.FailoverLog
	logger           *log.CommonLogger
}

func newFailoverLogRetriever(listOfVbs []uint16, sourceBucket *couchbase.Bucket, logger *log.CommonLogger) *failoverLogRetriever {
	return &failoverLogRetriever{listOfVbs: listOfVbs,
		sourceBucket:     sourceBucket,
		cur_failover_log: nil,
		logger:           logger}
}

func (retriever *failoverLogRetriever) getFailiverLog() (err error) {
	retriever.cur_failover_log, err = retriever.sourceBucket.GetFailoverLogs(retriever.listOfVbs)
	if err != nil {
		//the err returned is too long
		err = errors.New(fmt.Sprintf("Failed to get FailoverLogs for %v", retriever.listOfVbs))
		return
	}
	retriever.logger.Debugf("sourceBucket=%v failoverlogMap=%v\n", retriever.sourceBucket, retriever.cur_failover_log)
	return nil
}

func (ckmgr *CheckpointManager) getFailoverLog(bucket *couchbase.Bucket, listOfVbs []uint16) (couchbase.FailoverLog, error) {
	//Get failover log can hang, timeout the executation if it takes too long.
	failoverLogRetriever := newFailoverLogRetriever(listOfVbs, bucket, ckmgr.logger)
	err := base.ExecWithTimeout(failoverLogRetriever.getFailiverLog, 20*time.Second, ckmgr.logger)

	if err != nil {
		return nil, errors.New("Failed to get failover log in 1 minute")
	}

	return failoverLogRetriever.cur_failover_log, nil
}

func (ckmgr *CheckpointManager) getSourceBucket() (*couchbase.Bucket, error) {
	bucketName := ckmgr.pipeline.Specification().SourceBucketName
	localConnStr, err := ckmgr.xdcr_topology_svc.MyConnectionStr()
	if err != nil {
		return nil, err
	}
	bucket, err := ckmgr.utils.LocalBucket(localConnStr, bucketName)
	if err != nil {
		return nil, err
	}
	ckmgr.logger.Infof("%v Got the bucket %v for ckmgr\n", ckmgr.pipeline.Topic(), bucketName)
	return bucket, nil
}

func (ckmgr *CheckpointManager) retrieveCkptDoc(vbno uint16) (*metadata.CheckpointsDoc, error) {
	ckmgr.logger.Infof("%v retrieve chkpt doc for vb=%v\n", ckmgr.pipeline.Topic(), vbno)
	return ckmgr.checkpoints_svc.CheckpointsDoc(ckmgr.pipeline.Topic(), vbno)
}

func (ckmgr *CheckpointManager) populateVBTimestamp(ckptDoc *metadata.CheckpointsDoc, agreedIndex int, vbno uint16) (*base.VBTimestamp, error) {
	vbts := &base.VBTimestamp{Vbno: vbno}
	if agreedIndex > -1 && ckptDoc != nil {
		ckpt_records := ckptDoc.GetCheckpointRecords()
		if len(ckpt_records) < agreedIndex+1 {
			// should never happen
			ckmgr.logger.Warnf("%v could not find checkpoint record with agreedIndex=%v", ckmgr.pipeline.Topic(), agreedIndex)
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
		}
	}

	//update current ckpt map
	obj, ok := ckmgr.cur_ckpts[vbno]
	if ok {
		obj.lock.Lock()
		defer obj.lock.Unlock()

		//populate the next ckpt (in cur_ckpts)'s information based on the previous checkpoint information if it exists
		if agreedIndex > -1 && ckptDoc != nil {
			obj.ckpt.Failover_uuid = vbts.Vbuuid
			obj.ckpt.Dcp_snapshot_seqno = vbts.SnapshotStart
			obj.ckpt.Dcp_snapshot_end_seqno = vbts.SnapshotEnd
			obj.ckpt.Seqno = vbts.Seqno
		}
	} else {
		err := fmt.Errorf("%v Calling populateVBTimestamp on vb=%v which is not in MyVBList", ckmgr.pipeline.Topic(), vbno)
		ckmgr.handleGeneralError(err)
		return nil, err
	}
	return vbts, nil
}

func (ckmgr *CheckpointManager) checkpointing() {
	ckmgr.logger.Infof("%v checkpointing rountine started", ckmgr.pipeline.Topic())

	defer ckmgr.logger.Infof("%v Exits checkpointing routine.", ckmgr.pipeline.Topic())
	defer ckmgr.wait_grp.Done()

	ticker := <-ckmgr.checkpoint_ticker_ch
	defer ticker.Stop()

	children_fin_ch := make(chan bool)
	children_wait_grp := &sync.WaitGroup{}

	first := true

	for {
		select {
		case new_ticker := <-ckmgr.checkpoint_ticker_ch:
			ckmgr.logger.Infof("%v Received new ticker due to changes to checkpoint interval setting", ckmgr.pipeline.Topic())

			// wait for existing, if any, performCkpt routine to finish before setting new ticker
			close(children_fin_ch)
			children_wait_grp.Wait()
			children_fin_ch = make(chan bool)

			ticker.Stop()
			ticker = new_ticker
			first = true
		case <-ckmgr.finish_ch:
			ckmgr.logger.Infof("%v Received finish signal", ckmgr.pipeline.Topic())
			// wait for existing, if any, performCkpt routine to finish
			close(children_fin_ch)
			children_wait_grp.Wait()
			return
		case <-ticker.C:
			if !pipeline_utils.IsPipelineRunning(ckmgr.pipeline.State()) {
				//pipeline is no longer running, kill itself
				ckmgr.logger.Infof("%v Pipeline is no longer running, exit.", ckmgr.pipeline.Topic())
				return
			}
			children_wait_grp.Add(1)
			go ckmgr.performCkpt(children_fin_ch, children_wait_grp)

			if first {
				// the beginning tick has a randomized time element to randomize start time
				// reset the second and onward ticks to remove the randomized time element
				ticker.Stop()
				ticker = time.NewTicker(ckmgr.ckpt_interval)
				first = false
			}
		}
	}

}

// public API. performs one checkpoint operation on request
func (ckmgr *CheckpointManager) PerformCkpt(fin_ch <-chan bool) {
	ckmgr.logger.Infof("Start one time checkpointing for replication %v\n", ckmgr.pipeline.Topic())
	defer ckmgr.logger.Infof("Done one time checkpointing for replication %v\n", ckmgr.pipeline.Topic())

	var through_seqno_map map[uint16]uint64
	var high_seqno_and_vbuuid_map map[uint16][]uint64
	var xattr_seqno_map map[uint16]uint64
	if !ckmgr.capi {
		// get through seqnos for all vbuckets in the pipeline
		through_seqno_map = ckmgr.through_seqno_tracker_svc.GetThroughSeqnos()
		// get high seqno and vbuuid for all vbuckets in the pipeline
		high_seqno_and_vbuuid_map = ckmgr.getHighSeqnoAndVBUuidFromTarget()
		// get first seen xattr seqnos for all vbuckets in the pipeline
		xattr_seqno_map = pipeline_utils.GetXattrSeqnos(ckmgr.pipeline)
	}

	//divide the workload to several getter and run the getter parallelly
	vb_list := ckmgr.getMyVBs()
	base.RandomizeUint16List(vb_list)
	number_of_vbs := len(vb_list)

	number_of_workers := 5
	if number_of_workers > number_of_vbs {
		number_of_workers = number_of_vbs
	}
	load_distribution := base.BalanceLoad(number_of_workers, number_of_vbs)

	worker_wait_grp := &sync.WaitGroup{}
	for i := 0; i < number_of_workers; i++ {
		vb_list_worker := make([]uint16, 0)
		for index := load_distribution[i][0]; index < load_distribution[i][1]; index++ {
			vb_list_worker = append(vb_list_worker, vb_list[index])
		}

		worker_wait_grp.Add(1)
		// do not wait between vbuckets
		go ckmgr.performCkpt_internal(vb_list_worker, fin_ch, worker_wait_grp, 0, through_seqno_map, high_seqno_and_vbuuid_map, xattr_seqno_map)
	}

	//wait for all the getter done, then gather result
	worker_wait_grp.Wait()
}

// local API. supports periodical checkpoint operations
func (ckmgr *CheckpointManager) performCkpt(fin_ch <-chan bool, wait_grp *sync.WaitGroup) {
	ckmgr.logger.Infof("Start checkpointing for replication %v\n", ckmgr.pipeline.Topic())
	defer ckmgr.logger.Infof("Done checkpointing for replication %v\n", ckmgr.pipeline.Topic())
	// vbucketID -> ThroughSeqNumber
	var through_seqno_map map[uint16]uint64
	// vBucketID -> slice of 2 elements of 1)HighSeqNo and 2)VbUuid
	var high_seqno_and_vbuuid_map map[uint16][]uint64
	// map of vbucketID -> the seqno that corresponds to the first occurrence of xattribute
	var xattr_seqno_map map[uint16]uint64
	if !ckmgr.capi {
		// get through seqnos for all vbuckets in the pipeline
		through_seqno_map = ckmgr.through_seqno_tracker_svc.GetThroughSeqnos()
		// get high seqno and vbuuid for all vbuckets in the pipeline
		high_seqno_and_vbuuid_map = ckmgr.getHighSeqnoAndVBUuidFromTarget()
		// get first seen xattr seqnos for all vbuckets in the pipeline
		xattr_seqno_map = pipeline_utils.GetXattrSeqnos(ckmgr.pipeline)
	}
	ckmgr.performCkpt_internal(ckmgr.getMyVBs(), fin_ch, wait_grp, ckmgr.ckpt_interval, through_seqno_map, high_seqno_and_vbuuid_map, xattr_seqno_map)
}

func (ckmgr *CheckpointManager) performCkpt_internal(vb_list []uint16, fin_ch <-chan bool, wait_grp *sync.WaitGroup, time_to_wait time.Duration,
	through_seqno_map map[uint16]uint64, high_seqno_and_vbuuid_map map[uint16][]uint64, xattr_seqno_map map[uint16]uint64) {

	defer wait_grp.Done()

	var interval_btwn_vb time.Duration
	if time_to_wait != 0 {
		interval_btwn_vb = time.Duration((time_to_wait.Seconds()/float64(len(vb_list)))*1000) * time.Millisecond
	}
	ckmgr.logger.Infof("Checkpointing for replication %v, vb_list=%v, time_to_wait=%v, interval_btwn_vb=%v sec\n", ckmgr.pipeline.Topic(), vb_list, time_to_wait, interval_btwn_vb.Seconds())
	err_map := make(map[uint16]error)
	var total_committing_time float64 = 0

	for index, vb := range vb_list {
		select {
		case <-fin_ch:
			ckmgr.logger.Infof("Aborting checkpointing routine for %v with vb list %v since received finish signal. index=%v\n", ckmgr.pipeline.Topic(), vb_list, index)
			return
		default:
			if pipeline_utils.IsPipelineStopping(ckmgr.pipeline.State()) {
				ckmgr.logger.Infof("Pipeline %v is already stopping/stopped, exit do_checkpointing for vb list %v. index=%v\n", ckmgr.pipeline.Topic(), vb_list, index)
				return
			}

			start_time_vb := time.Now()
			err := ckmgr.doCheckpoint(vb, through_seqno_map, high_seqno_and_vbuuid_map, xattr_seqno_map)
			committing_time_vb := time.Since(start_time_vb)
			total_committing_time += committing_time_vb.Seconds()
			if err != nil {
				ckmgr.handleVBError(vb, err)
				err_map[vb] = err
			}

			if interval_btwn_vb != 0 && index < len(vb_list)-1 {
				time.Sleep(interval_btwn_vb)
			}

		}
	}

	ckmgr.logger.Infof("Done checkpointing for replication %v with vb list %v\n", ckmgr.pipeline.Topic(), vb_list)
	if len(err_map) > 0 {
		ckmgr.logger.Infof("Errors encountered in checkpointing for replication %v: %v\n", ckmgr.pipeline.Topic(), err_map)
	}
	ckmgr.RaiseEvent(common.NewEvent(common.CheckpointDone, nil, ckmgr, nil, time.Duration(total_committing_time)*time.Second))
}

/**
 * Update this check point record only if the versionNumber matches, and increments the internal version number
 * If update is successful, then it will persist the record in the checkpoint service.
 * Returns an error if version number mismatches while updating.
 * If an error occurs during persistence, ignore and override the error and logs an error message.
 */
func (ckptRecord *checkpointRecordWithLock) updateAndPersist(ckmgr *CheckpointManager, vbno uint16, versionNumberIn uint64,
	xattrSeqno uint64, seqno uint64, targetSeqno uint64, failoverUuid uint64,
	dcpSsSeqno uint64, dcpSsEndSeqno uint64) error {

	if ckptRecord == nil {
		return errors.New("Nil ckptRecord")
	} else {
		ckptRecord.lock.Lock()
		defer ckptRecord.lock.Unlock()
		if ckptRecord.ckpt == nil {
			return errors.New("Nil ckpt")
		}
		if versionNumberIn != ckptRecord.versionNum {
			return ckptRecordMismatch
		}
		// Update the record
		ckptRecord.ckpt.Seqno = seqno
		ckptRecord.ckpt.Target_Seqno = targetSeqno
		ckptRecord.ckpt.Failover_uuid = failoverUuid
		ckptRecord.ckpt.Dcp_snapshot_seqno = dcpSsSeqno
		ckptRecord.ckpt.Dcp_snapshot_end_seqno = dcpSsEndSeqno
		ckptRecord.versionNum++

		// Persist the record
		persistErr := ckmgr.persistCkptRecord(vbno, ckptRecord.ckpt, xattrSeqno)
		if persistErr == nil {
			ckmgr.raiseSuccessCkptForVbEvent(*ckptRecord.ckpt, vbno)
		} else {
			ckmgr.logger.Warnf("%v skipping checkpointing for vb=%v due to error persisting checkpoint record. err=%v", ckmgr.pipeline.Topic(), vbno, persistErr)
		}
	}
	return nil
}

/**
 * Given the current information, we want to establish a single checkpoint entry and persist it. We need to populate the followings:
 * 1. The Source vbucket through sequence number
 * 2. Failover UUID of source side (i.e. VBucket UUID)
 * 3. Target Vbucket high sequence number
 * 4. Target VB Opaque (i.e. VBUuid in recent releases) - we don't touch this
 * 5. Snapshot start sequence number (correlating to item 1)
 * 6. Snapshot end sequence number (correlating to item 1)
 */
func (ckmgr *CheckpointManager) doCheckpoint(vbno uint16, through_seqno_map map[uint16]uint64,
	high_seqno_and_vbuuid_map map[uint16][]uint64, xattr_seqno_map map[uint16]uint64) (err error) {
	//locking the current ckpt record and notsent_seqno list for this vb, no update is allowed during the checkpointing
	ckmgr.logger.Debugf("%v Checkpointing for vb=%v\n", ckmgr.pipeline.Topic(), vbno)

	ckpt_obj, ok := ckmgr.cur_ckpts[vbno]
	if ok {
		var targetVBOpaque metadata.TargetVBOpaque
		var curCkptTargetVBOpaque metadata.TargetVBOpaque
		// Get all necessary info before processing
		ckpt_obj.lock.RLock()
		// This is the version number that this iteration is working off of. Should the version number change
		// when updating the actual record, then this mean an concurrent operation has jumped ahead of us
		currRecordVersion := ckpt_obj.versionNum
		last_seqno := ckpt_obj.ckpt.Seqno
		curCkptTargetVBOpaque = ckpt_obj.ckpt.Target_vb_opaque
		ckpt_obj.lock.RUnlock()

		var through_seqno uint64
		if !ckmgr.capi {
			// non-capi mode, all through_seqnos have been computed prior
			through_seqno, ok = through_seqno_map[vbno]
			if !ok {
				ckmgr.logger.Warnf("%v skipping checkpointing for vb=%v since cannot find through seqno", ckmgr.pipeline.Topic(), vbno)
				return nil
			}
		} else {
			// capi mode
			through_seqno = ckmgr.through_seqno_tracker_svc.GetThroughSeqno(vbno)
		}

		// Item 1:
		// Update the one and only check point record that checkpoint_manager keeps track of in its "cur_ckpts" with the through_seq_no
		// that was found from "GetThroughSeqno", which has already done the work to single out the number to represent the latest state
		// from the source side perspective
		ckmgr.logger.Debugf("%v Seqno number used for checkpointing for vb %v is %v\n", ckmgr.pipeline.Topic(), vbno, through_seqno)

		if through_seqno < last_seqno {
			ckmgr.logger.Infof("%v Checkpoint seqno went backward, possibly due to rollback. vb=%v, old_seqno=%v, new_seqno=%v", ckmgr.pipeline.Topic(), vbno, last_seqno, through_seqno)
		}

		if through_seqno == last_seqno {
			ckmgr.logger.Debugf("%v No replication has happened in vb %v since replication start or last checkpoint. seqno=%v. Skip checkpointing\\n", ckmgr.pipeline.Topic(), vbno, last_seqno)
			return nil
		}

		if curCkptTargetVBOpaque == nil {
			ckmgr.logger.Infof("%v remote bucket is an older node, no checkpointing should be done.", ckmgr.pipeline.Topic())
			return nil
		}

		// get remote_seqno and vbuuid from target
		// Items: 3 and 4
		var remote_seqno uint64
		var commitForCheckpointErr error
		if !ckmgr.capi {
			// non-capi mode, high_seqno and vbuuid on target have been retrieved through vbucket-seqno stats
			high_seqno_and_vbuuid, ok := high_seqno_and_vbuuid_map[vbno]
			if !ok {
				ckmgr.logger.Warnf("%v skipping checkpointing for vb=%v since cannot find high seqno and vbuuid", ckmgr.pipeline.Topic(), vbno)
				return nil
			}

			remote_seqno = high_seqno_and_vbuuid[0]
			vbuuid := high_seqno_and_vbuuid[1]
			targetVBOpaque = &metadata.TargetVBUuid{vbuuid}
		} else {
			// capi mode, get high_seqno and vbuuid on target by calling commitForCheckpoint
			remote_seqno, targetVBOpaque, commitForCheckpointErr = ckmgr.capi_svc.CommitForCheckpoint(ckmgr.remote_bucket, curCkptTargetVBOpaque, vbno)
			if commitForCheckpointErr != nil && commitForCheckpointErr != service_def.VB_OPAQUE_MISMATCH_ERR {
				// We should not continue checkpointing this vb if we had issues communicating with capi service
				ckmgr.logger.Warnf("Error contacting capi service when calling CommitForCheckpoint: %v. Skip persisting checkpoint for vb %v.\n", commitForCheckpointErr.Error(), vbno)
				return nil
			}
		}

		// 1. in capi mode, we can rely on commitForCheckpoint() for target vb opaque check
		///   - a VB_OPAQUE_MISMATCH_ERR would be returned if target vb opaque has changed
		// 2. in non-capi mode, we need to do the target vb opaque check ourselves
		if (ckmgr.capi && commitForCheckpointErr == nil) ||
			(!ckmgr.capi && curCkptTargetVBOpaque.IsSame(targetVBOpaque)) {
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
				return nil
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

			var xattr_seqno uint64
			if !ckmgr.capi {
				xattr_seqno, ok = xattr_seqno_map[vbno]
				if !ok {
					// leave xattr_seqno at 0
					ckmgr.logger.Warnf("%v skipping first seen xattr seqno check for vb=%v since cannot find xattr seqno", ckmgr.pipeline.Topic(), vbno)
				} else {
					if xattr_seqno != 0 && xattr_seqno > through_seqno {
						// if xattr_seqno is larger than through_seqno, rollback is not needed
						// set xattr_seqno to 0 to avoid triggering unnecessary rollback
						// This case is possible if the mutation related to this sequence number has been sent to the target, and stored in the map
						// but the target has not yet responded, so that the through_seq_number service hasn't been able to process it yet.
						xattr_seqno = 0
					}
				}
			}

			// Write-operation - feed the temporary variables and update them into the record and also write them to metakv
			err = ckpt_obj.updateAndPersist(ckmgr, vbno, currRecordVersion, xattr_seqno, through_seqno, ckptRecordTargetSeqno,
				ckRecordFailoverUuid, ckRecordDcpSnapSeqno, ckRecordDcpSnapEndSeqno)

			if err != nil {
				// We weren't able to atomically update the checkpoint record. This checkpoint record is essentially lost
				// since someone jumped ahead of us
				ckmgr.logger.Warnf("%v skipping checkpointing for vb=%v version %v since a more recent checkpoint has been completed",
					ckmgr.pipeline.Topic(), vbno, currRecordVersion)
			}
		} else {
			// vb uuid on target has changed. rollback may be needed. return error to get pipeline restarted
			errMsg := fmt.Sprintf("%v target vbuuid has changed for vb=%v. old=%v, new=%v", ckmgr.pipeline.Topic(), vbno, curCkptTargetVBOpaque, targetVBOpaque)
			ckmgr.logger.Error(errMsg)
			return errors.New(errMsg)
		}
	} else {
		err := fmt.Errorf("%v Trying to doCheckpoint on vb=%v which is not in MyVBList", ckmgr.pipeline.Topic(), vbno)
		ckmgr.handleGeneralError(err)
		return err
	}
	// no error fall through. return nil here to keep checkpointing/replication going.
	// if there is a need to stop checkpointing/replication, it needs to be done in a separate return statement
	return nil
}

func (ckmgr *CheckpointManager) raiseSuccessCkptForVbEvent(ckpt_record metadata.CheckpointRecord, vbno uint16) {
	//notify statisticsManager
	ckmgr.RaiseEvent(common.NewEvent(common.CheckpointDoneForVB, ckpt_record, ckmgr, nil, vbno))
}

func (ckmgr *CheckpointManager) persistCkptRecord(vbno uint16, ckpt_record *metadata.CheckpointRecord, xattr_seqno uint64) error {
	ckmgr.logger.Debugf("Persist vb=%v ckpt_record=%v for %v\n", vbno, ckpt_record, ckmgr.pipeline.Topic())
	return ckmgr.checkpoints_svc.UpsertCheckpoints(ckmgr.pipeline.Topic(), ckmgr.pipeline.Specification().InternalId, vbno, ckpt_record, xattr_seqno, ckmgr.target_cluster_version)
}

func (ckmgr *CheckpointManager) OnEvent(event *common.Event) {
	if event.EventType == common.StreamingStart {
		upr_event, ok := event.Data.(*mcc.UprEvent)
		if ok {
			flog := upr_event.FailoverLog
			vbno := upr_event.VBucket

			failoverlog_obj, ok1 := ckmgr.failoverlog_map[vbno]
			if ok1 {
				failoverlog_obj.lock.Lock()
				defer failoverlog_obj.lock.Unlock()

				failoverlog_obj.failoverlog = flog

				ckmgr.logger.Debugf("%v Got failover log for vb=%v\n", ckmgr.pipeline.Topic(), vbno)
			} else {
				err := fmt.Errorf("%v Received failoverlog on an unknown vb=%v\n", ckmgr.pipeline.Topic(), vbno)
				ckmgr.handleGeneralError(err)
			}
		}
	} else if event.EventType == common.SnapshotMarkerReceived {
		upr_event, ok := event.Data.(*mcc.UprEvent)
		ckmgr.logger.Debugf("%v Received snapshot vb=%v, start=%v, end=%v\n", ckmgr.pipeline.Topic(), upr_event.VBucket, upr_event.SnapstartSeq, upr_event.SnapendSeq)
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
				err := fmt.Errorf("%v  Received snapshot marker on an unknown vb=%v\n", ckmgr.pipeline.Topic(), vbno)
				ckmgr.handleGeneralError(err)
			}
		}
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
		err := fmt.Errorf("%v Calling getFailoverUUIDForSeqno on an unknown vb=%v\n", ckmgr.pipeline.Topic(), vbno)
		ckmgr.handleGeneralError(err)
		return 0, err
	}
	return 0, fmt.Errorf("%v Failed to find vbuuid for vb=%v, seqno=%v\n", ckmgr.pipeline.Topic(), vbno, seqno)
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
		err := fmt.Errorf("%v Calling getSnapshotForSeqno on an unknown vb=%v\n", ckmgr.pipeline.Topic(), vbno)
		ckmgr.handleGeneralError(err)
		return 0, 0, err
	}
	return 0, 0, fmt.Errorf("%v Failed to find snapshot for vb=%v, seqno=%v\n", ckmgr.pipeline.Topic(), vbno, seqno)
}

func (ckmgr *CheckpointManager) UpdateVBTimestamps(vbno uint16, rollbackseqno uint64) (*base.VBTimestamp, error) {
	ckmgr.logger.Infof("%v Received rollback from DCP stream vb=%v, rollbackseqno=%v\n", ckmgr.pipeline.Topic(), vbno, rollbackseqno)
	pipeline_startSeqnos_map, pipeline_startSeqnos_map_lock := GetStartSeqnos(ckmgr.pipeline, ckmgr.logger)

	if pipeline_startSeqnos_map == nil {
		return nil, fmt.Errorf("Error retrieving vb timestamp map for %v\n", ckmgr.pipeline.Topic())
	}
	pipeline_startSeqnos_map_lock.Lock()
	defer pipeline_startSeqnos_map_lock.Unlock()

	pipeline_start_seqno, ok := pipeline_startSeqnos_map[vbno]
	if !ok {
		return nil, fmt.Errorf("%v Invalid vbno=%v\n", ckmgr.pipeline.Topic(), vbno)
	}

	checkpointDoc, err := ckmgr.retrieveCkptDoc(vbno)
	if err == service_def.MetadataNotFoundErr {
		ckmgr.logger.Errorf("Unable to construct ckpt doc from metakv given pipeline topic: %v vbno: %v",
			ckmgr.pipeline.Topic(), vbno)
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

	ckmgr.logger.Infof("%v vb=%v, current_start_seqno=%v, max_seqno=%v\n", ckmgr.pipeline.Topic(), vbno, pipeline_start_seqno.Seqno, max_seqno)

	vbts, err := ckmgr.getVBTimestampForVB(vbno, checkpointDoc, max_seqno)
	if err != nil {
		return nil, err
	}

	pipeline_startSeqnos_map[vbno] = vbts

	//set the start seqno on through_seqno_tracker_svc
	ckmgr.through_seqno_tracker_svc.SetStartSeqno(vbno, vbts.Seqno)
	ckmgr.logger.Infof("%v Rolled back startSeqno to %v for vb=%v\n", ckmgr.pipeline.Topic(), vbts.Seqno, vbno)

	ckmgr.logger.Infof("%v Retry vbts=%v\n", ckmgr.pipeline.Topic(), vbts)

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
			logger.Errorf("%v Didn't find 'VBTimesstamps' in settings. settings=%v\n", pipeline.Topic(), settings)
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
