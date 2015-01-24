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
	"github.com/couchbase/gomemcached"
	base "github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	component "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
	"github.com/couchbaselabs/go-couchbase"
	"sync"
	"time"
)

const (
	XDCRCheckpointing string = "xdcrCheckpointing"
	CheckpointMgrId   string = "CheckpointMgr"
	TimeCommiting     string = "time_commiting"
	Vbno              string = "vbno"
)

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

	//the interval between checkpointing
	ckpt_interval time.Duration

	//the channel to communicate finish signal with statistic updater
	finish_ch chan bool
	wait_grp  *sync.WaitGroup

	//checkpointing ticker
	checkpoint_ticker *time.Ticker

	//remote bucket
	remote_bucket *service_def.RemoteBucketInfo

	backward_compat bool

	cur_ckpts        map[uint16]*metadata.CheckpointRecord
	active_vbs       map[string][]uint16
	vb_highseqno_map map[uint16]uint64

	logger          *log.CommonLogger
	cur_ckpts_locks map[uint16]*sync.RWMutex
}

func NewCheckpointManager(checkpoints_svc service_def.CheckpointsService, capi_svc service_def.CAPIService,
	remote_cluster_svc service_def.RemoteClusterSvc, rep_spec_svc service_def.ReplicationSpecSvc, cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc, active_vbs map[string][]uint16, logger_ctx *log.LoggerContext) (*CheckpointManager, error) {
	if checkpoints_svc == nil || capi_svc == nil || remote_cluster_svc == nil || rep_spec_svc == nil || cluster_info_svc == nil || xdcr_topology_svc == nil {
		return nil, errors.New("checkpoints_svc, capi_svc, remote_cluster_svc, rep_spec_svc, cluster_info_svc and xdcr_topology_svc can't be nil")
	}
	logger := log.NewLogger("CheckpointManager", logger_ctx)
	return &CheckpointManager{
		AbstractComponent:  component.NewAbstractComponentWithLogger(CheckpointMgrId, logger),
		pipeline:           nil,
		checkpoints_svc:    checkpoints_svc,
		capi_svc:           capi_svc,
		rep_spec_svc:       rep_spec_svc,
		remote_cluster_svc: remote_cluster_svc,
		cluster_info_svc:   cluster_info_svc,
		xdcr_topology_svc:  xdcr_topology_svc,
		finish_ch:          make(chan bool, 1),
		logger:             logger,
		cur_ckpts:          make(map[uint16]*metadata.CheckpointRecord),
		cur_ckpts_locks:    make(map[uint16]*sync.RWMutex),
		active_vbs:         active_vbs,
		wait_grp:           &sync.WaitGroup{},
		vb_highseqno_map:   make(map[uint16]uint64)}, nil
}

func (ckmgr *CheckpointManager) Attach(pipeline common.Pipeline) error {

	ckmgr.logger.Infof("Attach checkpoint manager with pipeline %v\n", pipeline.InstanceId())

	ckmgr.pipeline = pipeline

	//populate the remote bucket information at the time of attaching
	err := ckmgr.populateRemoteBucketInfo(pipeline)

	if err != nil {
		return err
	}

	xmem_parts := pipeline.Targets()
	for _, part := range xmem_parts {
		part.RegisterComponentEventListener(common.DataSent, ckmgr)
	}

	ckmgr.initCurrentCkptMap()

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
	remote_bucket, err := service_def.NewRemoteBucketInfo(remoteClusterRef.Name, spec.TargetBucketName, remoteClusterRef, ckmgr.remote_cluster_svc, ckmgr.logger)
	if err != nil {
		return err
	}
	ckmgr.remote_bucket = remote_bucket
	return nil
}

func (ckmgr *CheckpointManager) Start(settings map[string]interface{}) error {
	if ckpt_interval, ok := settings[metadata.CheckpointInterval].(int); ok {
		ckmgr.ckpt_interval = time.Duration(ckpt_interval) * time.Second
	} else {
		return errors.New(fmt.Sprintf("%v should be provided in settings", metadata.CheckpointInterval))
	}

	ckmgr.logger.Infof("CheckpointManager starting with ckpt_interval=%v s\n", ckmgr.ckpt_interval.Seconds())

	ckmgr.checkpoint_ticker = time.NewTicker(ckmgr.ckpt_interval)

	//register itself with pipeline supervisor
	supervisor := ckmgr.pipeline.RuntimeContext().Service(base.PIPELINE_SUPERVISOR_SVC)
	if supervisor == nil {
		return errors.New("Pipeline supervisor has to exist")
	}
	err := ckmgr.RegisterComponentEventListener(common.ErrorEncountered, supervisor.(*PipelineSupervisor))
	if err != nil {
		return err
	}

	//start checkpointing loop
	ckmgr.wait_grp.Add(1)
	go ckmgr.checkpointing()
	return nil
}

func (ckmgr *CheckpointManager) initCurrentCkptMap() {
	listOfVbs := ckmgr.getMyVBs()
	for _, vbno := range listOfVbs {
		ckmgr.cur_ckpts[vbno] = &metadata.CheckpointRecord{}
		ckmgr.cur_ckpts_locks[vbno] = &sync.RWMutex{}
	}
}

func (ckmgr *CheckpointManager) Stop() error {
	//send signal to checkpoiting routine to exit
	close(ckmgr.finish_ch)
	ckmgr.wait_grp.Wait()
	return nil
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

func (ckmgr *CheckpointManager) checkBackwardCompatability() {
	backward_compat := false
	bk_capabilities := ckmgr.remote_bucket.Capabilities
	for _, c := range bk_capabilities {
		if c == XDCRCheckpointing {
			backward_compat = true
			break
		}
	}
	ckmgr.backward_compat = backward_compat
	ckmgr.logger.Infof("Remote bucket %v supporting xdcrcheckpoing is %v\n", ckmgr.remote_bucket, ckmgr.backward_compat)
}

func (ckmgr *CheckpointManager) currentVBUUID(vbno uint16) uint64 {
	ckmgr.cur_ckpts_locks[vbno].RLock()
	ckmgr.cur_ckpts_locks[vbno].RUnlock()

	record := ckmgr.cur_ckpts[vbno]
	return record.Target_vb_uuid

}

func (ckmgr *CheckpointManager) updateCurrentVBUUID(vbno uint16, vbuuid uint64) {
	ckmgr.cur_ckpts_locks[vbno].Lock()
	ckmgr.cur_ckpts_locks[vbno].Unlock()
	record := ckmgr.cur_ckpts[vbno]
	record.Target_vb_uuid = vbuuid
	ckmgr.cur_ckpts[vbno] = record
}

func (ckmgr *CheckpointManager) VBTimestamps(topic string) (map[uint16]*base.VBTimestamp, error) {
	ckmgr.logger.Info("Getting VBTimestamps...")
	//refresh the remote bucket
	err := ckmgr.remote_bucket.Refresh(ckmgr.remote_cluster_svc)
	if err != nil {
		return nil, err
	}

	disableCkptBackwardsCompat := ckmgr.backward_compat

	ret := make(map[uint16]*base.VBTimestamp)
	listOfVbs := ckmgr.getMyVBs()
	ckptDocs, err := ckmgr.checkpoints_svc.CheckpointsDocs(topic)
	if err != nil {
		return nil, err
	}
	var failoverLogMap couchbase.FailoverLog
	var highseqnomap = make(map[uint16]uint64)
	ckmgr.logger.Debugf("Found %v checkpoit document for replication %v\n", len(ckptDocs), topic)
	if len(ckptDocs) > 0 {
		//populate failover uuid on cur_ckpts
		failoverLogMap, highseqnomap, err = ckmgr.getFailoverLogAndHighSeqno()
		if err != nil {
			return nil, err
		}
		ckmgr.populateFailoverUUIDs(failoverLogMap)
		ckmgr.logger.Info("Got failoverlog...")
	}

	//divide the workload to several getter and run the getter parallelly
	workload := 5
	start_index := 0

	getter_wait_grp := sync.WaitGroup{}
	errMap := make(map[uint16]error)
	getter_id := 0
	for {
		end_index := start_index + workload
		if end_index > len(listOfVbs) {
			end_index = len(listOfVbs)
		}
		vbs_for_getter := listOfVbs[start_index:end_index]
		getter_wait_grp.Add(1)
		go ckmgr.startSeqnoGetter(getter_id, vbs_for_getter, ckptDocs, disableCkptBackwardsCompat, ret, highseqnomap, &getter_wait_grp, errMap)

		start_index = end_index
		if start_index >= len(listOfVbs) {
			break
		}
		getter_id++
	}

	//wait for all the getter done, then gather result
	getter_wait_grp.Wait()
	if len(errMap) > 0 {
		return nil, errors.New(fmt.Sprintf("Failed to get starting seqno for pipeline %v", ckmgr.pipeline.InstanceId()))
	}
	ckmgr.logger.Infof("Done with getting starting seqno for pipeline %v\n", ckmgr.pipeline.InstanceId())
	return ret, nil
}

func (ckmgr *CheckpointManager) startSeqnoGetter(getter_id int, listOfVbs []uint16, ckptDocs map[uint16]*metadata.CheckpointsDoc,
	disableCkptBackwardsCompat bool, ret map[uint16]*base.VBTimestamp, highseqnomap map[uint16]uint64, waitGrp *sync.WaitGroup, errMap map[uint16]error) {
	ckmgr.logger.Debugf("StartSeqnoGetter %v is started to do _pre_prelicate for vb %v\n", getter_id, listOfVbs)
	defer func() {
		waitGrp.Done()
		ckmgr.logger.Debugf("StartSeqnoGetter %v is done\n", getter_id)
	}()

	for _, vbno := range listOfVbs {
		var agreeedIndex int = -1
		ckptDoc, _ := ckptDocs[vbno]

		//get the existing checkpoint records if they exist, otherwise return an empty ckpt record
		ckpt_list := ckmgr.ckptRecords(ckptDoc, vbno)
		for index, ckpt_record := range ckpt_list {
			if ckpt_record != nil {
				if len(errMap) > 0 {
					//there is already error
					return
				}

				//set the target vb uuid from the value returned
				var vb_uuid uint64
				vb_uuid = ckpt_record.Target_vb_uuid
				remote_vb_status := &service_def.RemoteVBReplicationStatus{VBUUID: vb_uuid,
					VBSeqno: ckpt_record.Commitopaque,
					VBNo:    vbno}

				ckmgr.logger.Debugf("Negotiate checkpoint record %v...\n", ckpt_record)
				var current_remoteVBUUID uint64 = 0
				bMatch := false
				var err error
				bMatch, current_remoteVBUUID, err = ckmgr.capi_svc.PreReplicate(ckmgr.remote_bucket, remote_vb_status, disableCkptBackwardsCompat)
				//remote vb topology changed
				//udpate the vb_uuid and try again
				if err == nil {
					ckmgr.updateCurrentVBUUID(vbno, current_remoteVBUUID)
					ckmgr.logger.Debugf("Remote vbucket %v has a new uuid %v, update\n", current_remoteVBUUID, vbno)
					ckmgr.logger.Debugf("Done with _pre_prelicate call for %v for vbno=%v, bMatch=%v", remote_vb_status, vbno, bMatch)
				}

				if err != nil || bMatch {
					if bMatch {
						ckmgr.logger.Debugf("Remote bucket %v vbno %v agreed on the checkpoint %v\n", ckmgr.remote_bucket, vbno, ckpt_record)
						if ckptDoc != nil {
							agreeedIndex = index
						}

					} else {
						//there is an error to do _pre_replicate
						//so the start seqno for this vb should be 0
						ckmgr.logger.Errorf("Pre_replicate failed. err=%v\n", err)
						errMap[vbno] = err

					}
					goto POPULATE
				}
			}
		}
	POPULATE:
		vbts := ckmgr.populateVBTimestamp(ckptDoc, agreeedIndex, vbno, highseqnomap)
		ret[vbno] = vbts
	}
}

func (ckmgr *CheckpointManager) ckptRecords(ckptDoc *metadata.CheckpointsDoc, vbno uint16) []*metadata.CheckpointRecord {
	if ckptDoc != nil {
		ckmgr.logger.Infof("Found checkpoint doc for vb=%v\n", vbno)
		return ckptDoc.Checkpoint_records
	} else {
		ret := []*metadata.CheckpointRecord{}
		ret = append(ret, ckmgr.cur_ckpts[vbno])
		return ret
	}
}

func (ckmgr *CheckpointManager) populateFailoverUUIDs(failoverlogMap couchbase.FailoverLog) error {
	//populate the FailoverUUID for cur_ckpt_record
	for vbno, record := range ckmgr.cur_ckpts {
		failoverlog := failoverlogMap[vbno]
		if len(failoverlog) < 1 {
			return errors.New(fmt.Sprintf("Got an empty failover log from DCP on %v", vbno))
		}
		recent_failover_uuid := failoverlog[0][0]
		// FailoverLog containing vvuid and sequnce number
		record.Failover_uuid = recent_failover_uuid

	}

	ckmgr.logger.Debugf("After populating failover uuid, cur_ckpts=%v\n", ckmgr.cur_ckpts)

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
	retriever.logger.Debugf("failoverlogMap=%v\n", retriever.cur_failover_log)
	return nil
}

func (ckmgr *CheckpointManager) getFailoverLogAndHighSeqno() (couchbase.FailoverLog, map[uint16]uint64, error) {
	topic := ckmgr.pipeline.Topic()
	spec, err := ckmgr.rep_spec_svc.ReplicationSpec(topic)
	if err != nil {
		return nil, nil, err
	}
	sourcBucketName := spec.SourceBucketName

	bucket, err := ckmgr.cluster_info_svc.GetBucket(ckmgr.xdcr_topology_svc, sourcBucketName)
	if err != nil {
		return nil, nil, err
	}
	defer bucket.Close()
	ckmgr.logger.Debugf("Got the bucket %v\n", sourcBucketName)

	//Get failover log can hang, timeout the executation if it takes too long.
	failoverLogRetriever := newFailoverLogRetriever(ckmgr.getMyVBs(), bucket, ckmgr.logger)
	err = utils.ExecWithTimeout(failoverLogRetriever.getFailiverLog, 1*time.Minute, ckmgr.logger)	
	if err != nil {
		return nil, nil, errors.New("Failed to get failover log in 1 minute")
	}
	
	//GetStats(which string) map[string]map[string]string
	statsMap := bucket.GetStats(base.VBUCKET_SEQNO_STAT_NAME)

	vb_highseqno_map := make(map[uint16]uint64)
	for serverAddr, vbnos := range ckmgr.active_vbs {
		statsMapForServer, ok := statsMap[serverAddr]
		if !ok {
			return nil, nil, errors.New(fmt.Sprintf("Failed to find highseqno stats in statsMap returned for server=%v", serverAddr))
		}
		utils.ParseHighSeqnoStat(vbnos, statsMapForServer, vb_highseqno_map)
	}
	return failoverLogRetriever.cur_failover_log, vb_highseqno_map, nil
}

func (ckmgr *CheckpointManager) retrieveCkptDoc(vbno uint16) (*metadata.CheckpointsDoc, error) {
	ckmgr.logger.Infof("retrieve chkpt doc for vb=%v\n", vbno)
	return ckmgr.checkpoints_svc.CheckpointsDoc(ckmgr.pipeline.Topic(), vbno)
}

func (ckmgr *CheckpointManager) populateVBTimestamp(ckptDoc *metadata.CheckpointsDoc, agreedIndex int, vbno uint16, highseqno_map map[uint16]uint64) *base.VBTimestamp {
	vbts := &base.VBTimestamp{Vbno: vbno}
	if agreedIndex > -1 && ckptDoc != nil {
		ckpt_record := ckptDoc.Checkpoint_records[agreedIndex]
		vbts.Vbuuid = ckpt_record.Failover_uuid
		vbts.Seqno = ckpt_record.Seqno
		vbts.SnapshotStart = ckpt_record.Dcp_snapshot_seqno
		vbts.SnapshotEnd = ckpt_record.Dcp_snapshot_end_seqno

		//validate and adjust vbts
		highseqno := highseqno_map[vbno]
		ckmgr.logger.Infof("vbno=%v, Seqno =%v, highseqno=%v", vbno, vbts.Seqno, highseqno)
		if vbts.Seqno > highseqno {
			vbts.Seqno = highseqno
		}

		//For all stream requests the snapshot start seqno must be less than or equal
		//to the start seqno and the start seqno must be less than or equal to the snapshot end seqno.
		if vbts.SnapshotStart > vbts.Seqno {
			vbts.SnapshotStart = vbts.Seqno
		}
		if vbts.Seqno > vbts.SnapshotEnd {
			vbts.SnapshotEnd = vbts.Seqno
		}

	}

	if vbts.Vbuuid == 0 {
		vbts.Vbuuid = ckmgr.cur_ckpts[vbno].Failover_uuid
	}

	//update current ckpt map
	ckmgr.cur_ckpts_locks[vbno].Lock()
	defer ckmgr.cur_ckpts_locks[vbno].Unlock()

	//populate the next ckpt (in cur_ckpts)'s information based on the previous checkpoint information if it exists
	if agreedIndex > -1 && ckptDoc != nil {
		ckmgr.cur_ckpts[vbno].Failover_uuid = vbts.Vbuuid
		ckmgr.cur_ckpts[vbno].Dcp_snapshot_seqno = vbts.SnapshotStart
		ckmgr.cur_ckpts[vbno].Dcp_snapshot_end_seqno = vbts.SnapshotEnd
	}
	//set the next ckpt's Seqno to 0 - the unset state
	ckmgr.cur_ckpts[vbno].Seqno = 0

	return vbts
}

func (ckmgr *CheckpointManager) checkpointing() {
	defer func() {
		ckmgr.wait_grp.Done()
		ckmgr.logger.Info("Exits checkpointing routine.")
	}()
	for {
		select {
		case <-ckmgr.finish_ch:
			ckmgr.logger.Info("Received finish signal")
			return
		case <-ckmgr.checkpoint_ticker.C:
			ckmgr.checkpoint_ticker.Stop()
			if ckmgr.pipeline.State() != common.Pipeline_Running {
				//pipeline is no longer running, kill itself
				ckmgr.logger.Info("Pipeline is no longer running, exit.")
				return
			}
			ckmgr.performCkpt(false)
			ckmgr.checkpoint_ticker = time.NewTicker(ckmgr.ckpt_interval)
		}
	}

}

func (ckmgr *CheckpointManager) performCkpt(skip_error bool) {
	//divide the work to several executor and execute them parallelly
	listOfVbs := ckmgr.getMyVBs()
	startTime := time.Now()
	workload := 5
	start_index := 0

	executor_wait_grp := &sync.WaitGroup{}
	errMap := make(map[uint16]error)
	executor_id := 0
	for {
		end_index := start_index + workload
		if end_index > len(listOfVbs) {
			end_index = len(listOfVbs)
		}
		vbs_for_executor := listOfVbs[start_index:end_index]
		executor_wait_grp.Add(1)
		go ckmgr.executeCkptTask(executor_id, vbs_for_executor, false, errMap, executor_wait_grp)
		start_index = end_index
		executor_id++
		if start_index >= len(listOfVbs) {
			break
		}
	}

	//wait for all executator is done and then gather result
	executor_wait_grp.Wait()
	success := len(errMap) == 0
	if success {
		otherInfo := make(map[string]interface{})
		otherInfo[TimeCommiting] = time.Since(startTime)
		ckmgr.RaiseEvent(common.CheckpointDone, nil, ckmgr, nil, otherInfo)
	} else {
		//error
		err := errors.New("Checkpointing failed")
		otherInfo := utils.WrapError(err)
		ckmgr.RaiseEvent(common.ErrorEncountered, nil, ckmgr, nil, otherInfo)
	}
	ckmgr.logger.Infof("Done with checkpointing for pipeline %v\n", ckmgr.pipeline.InstanceId())
}

func (ckmgr *CheckpointManager) executeCkptTask(executor_id int, listOfVbs []uint16, skip_error bool, errMap map[uint16]error, wait_grp *sync.WaitGroup) {
	ckmgr.logger.Debugf("Checkpoing executor %v is checkpointing for vbuckets %v", executor_id, listOfVbs)
	if wait_grp != nil {
		defer func() {
			wait_grp.Done()
			ckmgr.logger.Debugf("Checkpoing executor %v is done", executor_id)
		}()
	}

	for _, vbno := range listOfVbs {
		if len(errMap) > 0 {
			//there is already error reported by other executor
			return
		}
		err := ckmgr.do_checkpoint(vbno, skip_error)
		if err == nil {
			ckmgr.logger.Debugf("Checkpointing is done for vb=%v\n", vbno)

		} else {
			//break on the first error
			ckmgr.logger.Errorf("Failed to checkpointing for vb=%v, err=%v\n", vbno, err)
			errMap[vbno] = err
			return
		}
	}

}

func (ckmgr *CheckpointManager) do_checkpoint(vbno uint16, skip_error bool) (err error) {
	//locking the current ckpt record for this vb, no update is allowed during the checkpointing
	ckmgr.cur_ckpts_locks[vbno].Lock()
	defer ckmgr.cur_ckpts_locks[vbno].Unlock()

	ckpt_record := ckmgr.cur_ckpts[vbno]
	if ckpt_record.Seqno == 0 {
		ckmgr.logger.Debugf("No replication happened yet, skip checkpointing for vb=%v pipeline=%v\n", vbno, ckmgr.pipeline.InstanceId())
		return nil
	}

	remote_seqno, vb_uuid, err := ckmgr.capi_svc.CommitForCheckpoint(ckmgr.remote_bucket, ckpt_record.Target_vb_uuid, vbno)
	if err == nil {
		//succeed
		ckpt_record.Commitopaque = remote_seqno
		err = ckmgr.persistCkptRecord(vbno, ckpt_record)
		ckmgr.raiseSuccessCkptForVbEvent(*ckpt_record, vbno)

	} else {
		ckpt_record.Target_vb_uuid = vb_uuid
	}
	ckpt_record.Seqno = 0
	ckpt_record.Commitopaque = 0
	return
}

func (ckmgr *CheckpointManager) raiseSuccessCkptForVbEvent(ckpt_record metadata.CheckpointRecord, vbno uint16) {
	//notify statisticsManager
	otherInfo := make(map[string]interface{})
	otherInfo[Vbno] = vbno
	ckmgr.RaiseEvent(common.CheckpointDoneForVB, ckpt_record, ckmgr, nil, otherInfo)
}

func (ckmgr *CheckpointManager) persistCkptRecord(vbno uint16, ckpt_record *metadata.CheckpointRecord) error {
	ckmgr.logger.Debugf("Persist vb=%v ckpt_record=%v\n", vbno, ckpt_record)
	return ckmgr.checkpoints_svc.UpsertCheckpoints(ckmgr.pipeline.Topic(), vbno, ckpt_record)
}

func (ckmgr *CheckpointManager) OnEvent(eventType common.ComponentEventType,
	item interface{},
	component common.Component,
	derivedItems []interface{},
	otherInfos map[string]interface{}) {
	if eventType == common.DataSent {
		hiseqno, ok := otherInfos[parts.EVENT_ADDI_HISEQNO].(uint64)
		if ok {
			vbno := item.(*gomemcached.MCRequest).VBucket
			ckmgr.cur_ckpts_locks[vbno].Lock()
			defer ckmgr.cur_ckpts_locks[vbno].Unlock()
			ckmgr.cur_ckpts[vbno].Seqno = hiseqno
			ckmgr.logger.Debugf("ckmgr.cur_ckpts[%v].Seqno =%v\n", vbno, otherInfos[parts.EVENT_ADDI_SEQNO])
		}
	}
}

//TODO: erlang xdcr also registers current remote bucket and vb information in xdcr_stats
//Think about how incorporate this in statistics manager
//register_vb_stats(Id, Vb, CurrRemoteBucket, Target, RemoteVBOpaque) ->
//    R = #xdcr_vb_stats_sample{id_and_vb = {Id, Vb},
//                              pid = self(),
//                              httpdb = Target,
//                              bucket_uuid = CurrRemoteBucket#remote_bucket.uuid,
//                              remote_vbopaque = RemoteVBOpaque},
//    ets:insert(xdcr_stats, R).
