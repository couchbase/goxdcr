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
	"bytes"
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
	"github.com/couchbase/goxdcr/simple_utils"
	"github.com/couchbase/goxdcr/utils"
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

var mass_vb_check_interval = 60 * time.Second

var CHECKPOINT_INTERVAL = "checkpoint_interval"

// maximum number of snapshot markers to store for each vb
// once the maximum is reached, the oldest snapshot marker is dropped to make room for the new one
var MAX_SNAPSHOT_HISTORY_LENGTH = 200

// max retry for collection of target high seqno and vbuuid stats
var MaxRetryTargetStats = 6

// base wait time between retrys for target stats retrieval.
// it will become exponentially longer with each retry
var WaitBetweenRetryTargetStats = time.Second

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

	cur_ckpts            map[uint16]*checkpointRecordWithLock
	active_vbs           map[string][]uint16
	failoverlog_map      map[uint16]*failoverlogWithLock
	snapshot_history_map map[uint16]*snapshotHistoryWithLock

	logger *log.CommonLogger

	target_bucket_name     string
	target_bucket_password string
	target_kv_vb_map       map[string][]uint16

	// whether replication is of capi type
	capi bool

	user_agent string

	// memcached clients for retrieval of target bucket stats
	kv_mem_clients      map[string]*mcc.Client
	kv_mem_clients_lock sync.RWMutex
}

type checkpointRecordWithLock struct {
	ckpt *metadata.CheckpointRecord
	lock *sync.RWMutex
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
	active_vbs map[string][]uint16, target_bucket_name, target_bucket_password string, target_kv_vb_map map[string][]uint16,
	logger_ctx *log.LoggerContext) (*CheckpointManager, error) {
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
		target_bucket_name:        target_bucket_name,
		target_bucket_password:    target_bucket_password,
		target_kv_vb_map:          target_kv_vb_map,
		wait_grp:                  &sync.WaitGroup{},
		failoverlog_map:           make(map[uint16]*failoverlogWithLock),
		snapshot_history_map:      make(map[uint16]*snapshotHistoryWithLock),
		kv_mem_clients:            make(map[string]*mcc.Client)}, nil
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

	return err
}

func (ckmgr *CheckpointManager) populateRemoteBucketInfo(pipeline common.Pipeline) error {
	topic := pipeline.Topic()
	spec, err := ckmgr.rep_spec_svc.ReplicationSpec(topic)
	if err != nil {
		return err
	}
	remoteClusterRef, err := ckmgr.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, true)
	if err != nil {
		return err
	}
	remote_bucket, err := service_def.NewRemoteBucketInfo(remoteClusterRef.Name, spec.TargetBucketName, remoteClusterRef, ckmgr.remote_cluster_svc, ckmgr.cluster_info_svc, ckmgr.logger)
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
			snapshot_history: make([]*snapshot, 0, MAX_SNAPSHOT_HISTORY_LENGTH),
		}
	}

	ckmgr.composeUserAgent()

	ckmgr.capi = (ckmgr.pipeline.Specification().Settings.RepType == metadata.ReplicationTypeCapi)
}

// compose user agent string for HELO command
func (ckmgr *CheckpointManager) composeUserAgent() {
	var buffer bytes.Buffer
	buffer.WriteString("Goxdcr CkptMgr ")
	spec := ckmgr.pipeline.Specification()
	buffer.WriteString(" SourceBucket:" + spec.SourceBucketName)
	buffer.WriteString(" TargetBucket:" + spec.TargetBucketName)
	ckmgr.user_agent = buffer.String()
}

func (ckmgr *CheckpointManager) initConnections() error {
	for serverAddr, _ := range ckmgr.target_kv_vb_map {
		conn, err := utils.GetRemoteMemcachedConnection(serverAddr, ckmgr.target_bucket_name, ckmgr.target_bucket_password, ckmgr.user_agent, ckmgr.logger)
		if err != nil {
			return err
		}
		// no need to lock since this is done during initialization
		ckmgr.kv_mem_clients[serverAddr] = conn
	}

	return nil
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
	ckmgr.kv_mem_clients = make(map[string]*mcc.Client)
}

func (ckmgr *CheckpointManager) getHighSeqnoAndVBUuidFromTarget() map[uint16][]uint64 {
	ckmgr.kv_mem_clients_lock.Lock()
	defer ckmgr.kv_mem_clients_lock.Unlock()

	high_seqno_and_vbuuid_map := make(map[uint16][]uint64)
	for serverAddr, vbnos := range ckmgr.target_kv_vb_map {
		ckmgr.getHighSeqnoAndVBUuidForServerWithRetry(serverAddr, vbnos, high_seqno_and_vbuuid_map)
	}
	return high_seqno_and_vbuuid_map
}

// checkpointing cannot be done without high seqno and vbuuid from target
// if retrival of such stats fails, retry
func (ckmgr *CheckpointManager) getHighSeqnoAndVBUuidForServerWithRetry(serverAddr string, vbnos []uint16, high_seqno_and_vbuuid_map map[uint16][]uint64) {
	retry := 0
	err_count := 0
	// base wait time
	wait_time := WaitBetweenRetryTargetStats
	for {
		client := ckmgr.kv_mem_clients[serverAddr]
		if client != nil {
			stats_map, err := client.StatsMap(base.VBUCKET_SEQNO_STAT_NAME)
			if err == nil {
				utils.ParseHighSeqnoAndVBUuidFromStats(vbnos, stats_map, high_seqno_and_vbuuid_map)
				break
			} else {
				ckmgr.logger.Warnf("% Error getting vbucket-seqno stats for serverAddr=%v. vbnos=%v, err=%v", ckmgr.pipeline.Topic(), serverAddr, vbnos, err)

				err_count++
				if err_count >= base.MaxMemClientErrorCount {
					err = client.Close()
					if err != nil {
						ckmgr.logger.Warnf("%v error from closing connection for %v is %v\n", ckmgr.pipeline.Topic(), serverAddr, err)
					}

					new_client, err := utils.GetRemoteMemcachedConnection(serverAddr, ckmgr.target_bucket_name, ckmgr.target_bucket_password, ckmgr.user_agent, ckmgr.logger)
					if err != nil {
						ckmgr.logger.Warnf("%v error creating new connection to %v. err=%v\n", ckmgr.pipeline.Topic(), serverAddr, err)
					} else {
						ckmgr.kv_mem_clients[serverAddr] = new_client
						err_count = 0
					}
				}

				if retry >= MaxRetryTargetStats {
					ckmgr.logger.Warnf("%v Retrieval of high seqno and vbuuid stats failed after %v retries. serverAddr=%v, vbnos=%v\n", ckmgr.pipeline.Topic(), retry, serverAddr, vbnos)
					break
				}

				retry++

				time.Sleep(wait_time)
				// exponential backoff
				wait_time *= 2
			}
		} else {
			// just in case that client connections have been closed and released prior
			ckmgr.logger.Warnf("% cannot find memcached client for %v", ckmgr.pipeline.Topic(), serverAddr)
			break
		}
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
	ckmgr.logger.Infof("Starting checkpointing for pipeline %v before stopping", ckmgr.pipeline.Topic())

	timeout_ticker := time.NewTicker(base.TimeoutCheckpointBeforeStop)
	defer timeout_ticker.Stop()

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
		case <-timeout_ticker.C:
			close(close_ch)
			ckmgr.logger.Infof("Checkpointing for pipeline %v timed out after %v", ckmgr.pipeline.Topic(), base.TimeoutCheckpointBeforeStop)
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
		panic(fmt.Sprintf("Trying to update vbopaque on vb=%v which is not in MyVBList", vbno))
	}
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

		if simple_utils.IsVbInList(vbno, vb_list) {
			// if vbno is in vb_list, include its senqo in docs_processed computation

			// if checkpoint records exist, use the seqno in the first checkpoint record, which is the highest in all checkpoint records
			if ckptDoc != nil && ckptDoc.Checkpoint_records != nil && ckptDoc.Checkpoint_records[0] != nil {
				docsProcessed += ckptDoc.Checkpoint_records[0].Seqno
			}
		} else {
			// otherwise, delete the checkpoint doc since it is no longer valid

			// ignore errors, which should have been logged
			checkpoints_svc.DelCheckpointsDoc(topic, vbno)
		}
	}

	return docsProcessed, nil

}

func (ckmgr *CheckpointManager) SetVBTimestamps(topic string) error {
	defer ckmgr.logger.Infof("%v Done with SetVBTimestamps", ckmgr.pipeline.Topic())
	ckmgr.logger.Infof("Set start seqnos for pipeline %v...", ckmgr.pipeline.Topic())

	listOfVbs := ckmgr.getMyVBs()

	ckmgr.logger.Infof("Getting checkpoint for %v\n", topic)
	ckptDocs, err := ckmgr.checkpoints_svc.CheckpointsDocs(topic)
	if err != nil {
		ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
		return err
	}
	ckmgr.logger.Infof("Found %v checkpoint documents for replication %v\n", len(ckptDocs), topic)

	for vbno, _ := range ckptDocs {
		if !simple_utils.IsVbInList(vbno, listOfVbs) {
			// if the vbno is no longer managed by the current checkpoint manager/pipeline,
			// the checkpoint doc is no longer valid and needs to be deleted
			// ignore errors, which should have been logged
			ckmgr.checkpoints_svc.DelCheckpointsDoc(topic, vbno)
		}
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

	//wait for all the getter done, then gather result
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

	ckmgr.wait_grp.Add(1)
	go ckmgr.massCheckVBOpaquesJob()

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

	//do checkpointing only when the remote bucket supports xdcrcheckpointing
	//get the existing checkpoint records if they exist, otherwise return an empty ckpt record
	ckpt_list := ckmgr.ckptRecords(ckptDoc, vbno)
	for index, ckpt_record := range ckpt_list {
		if ckpt_record != nil && ckpt_record.Seqno <= max_seqno {
			remote_vb_status := &service_def.RemoteVBReplicationStatus{VBOpaque: ckpt_record.Target_vb_opaque,
				VBSeqno: ckpt_record.Target_Seqno,
				VBNo:    vbno}

			bMatch := false
			bMatch, current_remoteVBOpaque, err := ckmgr.capi_svc.PreReplicate(ckmgr.remote_bucket, remote_vb_status, ckmgr.support_ckpt)
			//remote vb topology changed
			//udpate the vb_uuid and try again
			if err == nil {
				ckmgr.updateCurrentVBOpaque(vbno, current_remoteVBOpaque)
				ckmgr.logger.Debugf("%v Remote vbucket %v has a new opaque %v, update\n", ckmgr.pipeline.Topic(), current_remoteVBOpaque, vbno)
				ckmgr.logger.Debugf("%v Done with _pre_prelicate call for %v for vbno=%v, bMatch=%v", ckmgr.pipeline.Topic(), remote_vb_status, vbno, bMatch)
			}

			if err != nil || bMatch {
				if bMatch {
					ckmgr.logger.Debugf("%v Remote bucket %v vbno %v agreed on the checkpoint %v\n", ckmgr.pipeline.Topic(), ckmgr.remote_bucket, vbno, ckpt_record)
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
		}
	}
POPULATE:
	return ckmgr.populateVBTimestamp(ckptDoc, agreeedIndex, vbno), nil
}

func (ckmgr *CheckpointManager) ckptRecords(ckptDoc *metadata.CheckpointsDoc, vbno uint16) []*metadata.CheckpointRecord {
	if ckptDoc != nil {
		ckmgr.logger.Infof("%v Found checkpoint doc for vb=%v\n", ckmgr.pipeline.Topic(), vbno)
		return ckptDoc.Checkpoint_records
	} else {
		ret := []*metadata.CheckpointRecord{}

		ret = append(ret, ckmgr.getCurrentCkpt(vbno))
		return ret
	}
}

func (ckmgr *CheckpointManager) UpdateSettings(settings map[string]interface{}) error {
	ckmgr.logger.Debugf("Updating settings on checkpoint manager for pipeline %v. settings=%v\n", ckmgr.pipeline.Topic(), settings)
	checkpoint_interval, err := utils.GetIntSettingFromSettings(settings, CHECKPOINT_INTERVAL)
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
	err := simple_utils.ExecWithTimeout(failoverLogRetriever.getFailiverLog, 20*time.Second, ckmgr.logger)
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
	bucket, err := utils.LocalBucket(localConnStr, bucketName)
	if err != nil {
		return nil, err
	}
	ckmgr.logger.Infof("%v Got the bucket %v for ckmgr\n", ckmgr.pipeline.Topic(), bucketName)
	return bucket, nil
}

func (ckmgr *CheckpointManager) getHighSeqno() (map[uint16]uint64, error) {

	bucket, err := ckmgr.getSourceBucket()
	if err != nil {
		return nil, err
	}
	defer bucket.Close()

	//GetStats(which string) map[string]map[string]string
	statsMap := bucket.GetStats(base.VBUCKET_SEQNO_STAT_NAME)

	vb_highseqno_map := make(map[uint16]uint64)
	for serverAddr, vbnos := range ckmgr.active_vbs {
		statsMapForServer, ok := statsMap[serverAddr]
		if !ok {
			return nil, errors.New(fmt.Sprintf("Failed to find highseqno stats in statsMap returned for server=%v", serverAddr))
		}
		utils.ParseHighSeqnoStat(vbnos, statsMapForServer, vb_highseqno_map)
	}

	return vb_highseqno_map, nil
}

func (ckmgr *CheckpointManager) retrieveCkptDoc(vbno uint16) (*metadata.CheckpointsDoc, error) {
	ckmgr.logger.Infof("%v retrieve chkpt doc for vb=%v\n", ckmgr.pipeline.Topic(), vbno)
	return ckmgr.checkpoints_svc.CheckpointsDoc(ckmgr.pipeline.Topic(), vbno)
}

func (ckmgr *CheckpointManager) populateVBTimestamp(ckptDoc *metadata.CheckpointsDoc, agreedIndex int, vbno uint16) *base.VBTimestamp {
	vbts := &base.VBTimestamp{Vbno: vbno}
	if agreedIndex > -1 && ckptDoc != nil {
		ckpt_record := ckptDoc.Checkpoint_records[agreedIndex]
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
		panic(fmt.Sprintf("Calling populateVBTimestamp on vb=%v which is not in MyVBList", vbno))
	}
	return vbts
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
	if !ckmgr.capi {
		// get through seqnos for all vbuckets in the pipeline
		through_seqno_map = ckmgr.through_seqno_tracker_svc.GetThroughSeqnos()
		// get high seqno and vbuuid for all vbuckets in the pipeline
		high_seqno_and_vbuuid_map = ckmgr.getHighSeqnoAndVBUuidFromTarget()
	}

	//divide the workload to several getter and run the getter parallelly
	vb_list := ckmgr.getMyVBs()
	simple_utils.RandomizeUint16List(vb_list)
	number_of_vbs := len(vb_list)

	number_of_workers := 5
	if number_of_workers > number_of_vbs {
		number_of_workers = number_of_vbs
	}
	load_distribution := simple_utils.BalanceLoad(number_of_workers, number_of_vbs)

	worker_wait_grp := &sync.WaitGroup{}
	for i := 0; i < number_of_workers; i++ {
		vb_list_worker := make([]uint16, 0)
		for index := load_distribution[i][0]; index < load_distribution[i][1]; index++ {
			vb_list_worker = append(vb_list_worker, vb_list[index])
		}

		worker_wait_grp.Add(1)
		// do not wait between vbuckets
		go ckmgr.performCkpt_internal(vb_list_worker, fin_ch, worker_wait_grp, 0, through_seqno_map, high_seqno_and_vbuuid_map)
	}

	//wait for all the getter done, then gather result
	worker_wait_grp.Wait()
}

// local API. supports periodical checkpoint operations
func (ckmgr *CheckpointManager) performCkpt(fin_ch <-chan bool, wait_grp *sync.WaitGroup) {
	ckmgr.logger.Infof("Start checkpointing for replication %v\n", ckmgr.pipeline.Topic())
	defer ckmgr.logger.Infof("Done checkpointing for replication %v\n", ckmgr.pipeline.Topic())
	var through_seqno_map map[uint16]uint64
	var high_seqno_and_vbuuid_map map[uint16][]uint64
	if !ckmgr.capi {
		// get through seqnos for all vbuckets in the pipeline
		through_seqno_map = ckmgr.through_seqno_tracker_svc.GetThroughSeqnos()
		// get high seqno and vbuuid for all vbuckets in the pipeline
		high_seqno_and_vbuuid_map = ckmgr.getHighSeqnoAndVBUuidFromTarget()
	}
	ckmgr.performCkpt_internal(ckmgr.getMyVBs(), fin_ch, wait_grp, ckmgr.ckpt_interval, through_seqno_map, high_seqno_and_vbuuid_map)
}

func (ckmgr *CheckpointManager) performCkpt_internal(vb_list []uint16, fin_ch <-chan bool, wait_grp *sync.WaitGroup, time_to_wait time.Duration,
	through_seqno_map map[uint16]uint64, high_seqno_and_vbuuid_map map[uint16][]uint64) {

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
			err := ckmgr.do_checkpoint(vb, through_seqno_map, high_seqno_and_vbuuid_map)
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

func (ckmgr *CheckpointManager) do_checkpoint(vbno uint16, through_seqno_map map[uint16]uint64, high_seqno_and_vbuuid_map map[uint16][]uint64) (err error) {
	//locking the current ckpt record and notsent_seqno list for this vb, no update is allowed during the checkpointing
	ckmgr.logger.Debugf("%v Checkpointing for vb=%v\n", ckmgr.pipeline.Topic(), vbno)

	ckpt_obj, ok := ckmgr.cur_ckpts[vbno]
	if ok {
		ckpt_obj.lock.Lock()
		defer ckpt_obj.lock.Unlock()

		ckpt_record := ckpt_obj.ckpt

		last_seqno := ckpt_record.Seqno

		// get through_seqno
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

		ckpt_record.Seqno = through_seqno
		ckmgr.logger.Debugf("%v Seqno number used for checkpointing for vb %v is %v\n", ckmgr.pipeline.Topic(), vbno, ckpt_record.Seqno)

		if ckpt_record.Seqno < last_seqno {
			ckmgr.logger.Infof("%v Checkpoint seqno went backward, possibly due to rollback. vb=%v, old_seqno=%v, new_seqno=%v", ckmgr.pipeline.Topic(), vbno, last_seqno, ckpt_record.Seqno)
		}

		if ckpt_record.Seqno == last_seqno {
			ckmgr.logger.Debugf("%v No replication has happened in vb %v since replication start or last checkpoint. seqno=%v. Skip checkpointing\\n", ckmgr.pipeline.Topic(), vbno, last_seqno)

			return nil
		}

		if ckpt_record.Target_vb_opaque == nil {
			ckmgr.logger.Infof("%v remote bucket is an older node, no checkpointing should be done.", ckmgr.pipeline.Topic())
			return nil
		}

		// get remote_seqno and vbuuid from target
		var remote_seqno uint64
		var targetVBOpaque metadata.TargetVBOpaque
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
			remote_seqno, targetVBOpaque, err = ckmgr.capi_svc.CommitForCheckpoint(ckmgr.remote_bucket, ckpt_record.Target_vb_opaque, vbno)
		}

		// targetVBOpaque may be nil when connecting to elastic search, skip target vb uuid check in this case
		if targetVBOpaque == nil || ckpt_record.Target_vb_opaque.IsSame(targetVBOpaque) {
			ckpt_record.Target_Seqno = remote_seqno
			ckpt_record.Failover_uuid, err = ckmgr.getFailoverUUIDForSeqno(vbno, ckpt_record.Seqno)
			if err != nil {
				// if we cannot find uuid for the checkpoint seqno, the checkpoint seqno is unusable
				// skip checkpointing of this vb
				// return nil so that we can continue to checkpoint the next vb
				ckmgr.logger.Warnf("%v\n", err.Error())
				return nil
			}

			ckpt_record.Dcp_snapshot_seqno, ckpt_record.Dcp_snapshot_end_seqno, err = ckmgr.getSnapshotForSeqno(vbno, ckpt_record.Seqno)
			if err != nil {
				// if we cannot find snapshot for the checkpoint seqno, the checkpoint seqno is still usable in normal cases,
				// just that we may have to rollback to 0 when rollback is needed
				// log the problem and proceed
				ckmgr.logger.Warnf("%v\n", err.Error())
			}

			err = ckmgr.persistCkptRecord(vbno, ckpt_record)
			if err == nil {
				ckmgr.raiseSuccessCkptForVbEvent(*ckpt_record, vbno)
			} else {
				ckmgr.logger.Warnf("%v skipping checkpointing for vb=%v due to error persisting checkpoint record. err=%v", ckmgr.pipeline.Topic(), vbno, err)
				err = nil
			}

		} else {
			// vb uuid on target has changed. rollback may be needed. return error to get pipeline restarted
			errMsg := fmt.Sprintf("%v target vbuuid has changed for vb=%v. old=%v, new=%v", ckmgr.pipeline.Topic(), vbno, ckpt_record.Target_vb_opaque, targetVBOpaque)
			ckmgr.logger.Error(errMsg)
			return errors.New(errMsg)
		}
		ckpt_record.Target_Seqno = 0
		ckpt_record.Failover_uuid = 0
	} else {
		panic(fmt.Sprintf("%v Trying to do_checkpoint on vb=%v which is not in MyVBList", ckmgr.pipeline.Topic(), vbno))

	}
	return
}

func (ckmgr *CheckpointManager) raiseSuccessCkptForVbEvent(ckpt_record metadata.CheckpointRecord, vbno uint16) {
	//notify statisticsManager
	ckmgr.RaiseEvent(common.NewEvent(common.CheckpointDoneForVB, ckpt_record, ckmgr, nil, vbno))
}

func (ckmgr *CheckpointManager) persistCkptRecord(vbno uint16, ckpt_record *metadata.CheckpointRecord) error {
	ckmgr.logger.Debugf("Persist vb=%v ckpt_record=%v for %v\n", vbno, ckpt_record, ckmgr.pipeline.Topic())
	return ckmgr.checkpoints_svc.UpsertCheckpoints(ckmgr.pipeline.Topic(), vbno, ckpt_record)
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
				panic(fmt.Sprintf("%v Received failoverlog on an unknown vb=%v\n", ckmgr.pipeline.Topic(), vbno))
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
				if len(snapshot_history_obj.snapshot_history) < MAX_SNAPSHOT_HISTORY_LENGTH {
					snapshot_history_obj.snapshot_history = append(snapshot_history_obj.snapshot_history, cur_snapshot)
				} else {
					// rotation is needed
					for i := 0; i < MAX_SNAPSHOT_HISTORY_LENGTH-1; i++ {
						snapshot_history_obj.snapshot_history[i] = snapshot_history_obj.snapshot_history[i+1]
					}
					snapshot_history_obj.snapshot_history[MAX_SNAPSHOT_HISTORY_LENGTH-1] = cur_snapshot
				}
			} else {
				panic(fmt.Sprintf("%v, Received snapshot marker on an unknown vb=%v\n", ckmgr.pipeline.Topic(), vbno))
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
		panic(fmt.Sprintf("%v Calling getFailoverUUIDForSeqno on an unknown vb=%v\n", ckmgr.pipeline.Topic(), vbno))

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
		panic(fmt.Sprintf("%v Calling getFailoverUUIDForSeqno on an unknown vb=%v\n", ckmgr.pipeline.Topic(), vbno))
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
	if rollbackseqno >= pipeline_start_seqno.Seqno {
		panic(fmt.Sprintf("%v rollbackseqno=%v, current_start_seqno=%v", ckmgr.pipeline.Topic(), rollbackseqno, pipeline_start_seqno.Seqno))
	}

	checkpointDoc, err := ckmgr.retrieveCkptDoc(vbno)
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

	ckmgr.logger.Infof("%v vb=%v, current_start_seqno=%v, max_seqno=%v\n", vbno, pipeline_start_seqno.Seqno, max_seqno)

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

// returns the vbts map and the associated lock on the map. 	861
// caller needs to lock the lock appropriatedly before using the map 	862
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

func (ckmgr *CheckpointManager) massCheckVBOpaquesJob() {
	defer ckmgr.logger.Infof("%v Exits massCheckVBOpaquesJob routine.", ckmgr.pipeline.Topic())
	defer ckmgr.wait_grp.Done()

	ticker := time.NewTicker(mass_vb_check_interval)
	defer ticker.Stop()
	for {
		select {
		case <-ckmgr.finish_ch:
			ckmgr.logger.Infof("%v Received finish signal", ckmgr.pipeline.Topic())
			return
		case <-ticker.C:
			if !pipeline_utils.IsPipelineRunning(ckmgr.pipeline.State()) {
				//pipeline is no longer running, kill itself
				ckmgr.logger.Infof("%v Pipeline is no longer running, exit.", ckmgr.pipeline.Topic())
				return
			}
			go ckmgr.massCheckVBOpaques()
		}
	}

}

func (ckmgr *CheckpointManager) massCheckVBOpaques() error {
	target_vb_vbuuid_map := make(map[uint16]metadata.TargetVBOpaque)
	//validate target bucket's vbucket uuid
	for vb, _ := range ckmgr.cur_ckpts {
		latest_ckpt_record := ckmgr.getCurrentCkpt(vb)
		if latest_ckpt_record.Target_vb_opaque != nil {
			target_vb_uuid := latest_ckpt_record.Target_vb_opaque
			target_vb_vbuuid_map[vb] = target_vb_uuid
		} else {
			ckmgr.logger.Infof("%v remote bucket is an older node, massCheckVBOpaque is not supported for vb=%v.", ckmgr.pipeline.Topic(), vb)
		}
	}

	if len(target_vb_vbuuid_map) > 0 {
		matching, mismatching, missing, err1 := ckmgr.capi_svc.MassValidateVBUUIDs(ckmgr.remote_bucket, target_vb_vbuuid_map)
		if err1 != nil {
			ckmgr.logger.Errorf("%v MassValidateVBUUID failed, err=%v", ckmgr.pipeline.Topic(), err1)
			return err1
		} else {
			if len(mismatching) > 0 {
				//error
				ckmgr.logger.Errorf("Target bucket for replication %v's topology has changed. mismatch=%v, missing=%v, matching=%v\n", ckmgr.pipeline.Topic(), mismatching, missing, matching)
				err := errors.New("Target bucket's topology has changed")
				ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
			} else if len(missing) > 0 {
				// missing vbuckets indicate that rebalance or failover may be going on on the target side
				for _, vbno := range missing {
					ckmgr.handleVBError(vbno, errors.New("MassValidateVBUUID failed since vbucket cannot be found on target"))
				}
			} else {
				ckmgr.logger.Infof("No target bucket topology change is detected for replication %v", ckmgr.pipeline.Topic())
			}
			return nil
		}
	}
	return nil
}

func (ckmgr *CheckpointManager) handleVBError(vbno uint16, err error) {
	additionalInfo := &base.VBErrorEventAdditional{vbno, err, base.VBErrorType_Target}
	ckmgr.RaiseEvent(common.NewEvent(common.VBErrorEncountered, nil, ckmgr, nil, additionalInfo))
}

func (ckmgr *CheckpointManager) getCurrentCkpt(vbno uint16) *metadata.CheckpointRecord {
	ckpt_obj, ok := ckmgr.cur_ckpts[vbno]
	if ok {
		ckpt_obj.lock.RLock()
		defer ckpt_obj.lock.RUnlock()
		return ckpt_obj.ckpt
	}
	return nil
}
