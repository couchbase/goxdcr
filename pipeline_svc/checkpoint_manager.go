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
	base "github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	component "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
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

	//checkpointing ticker
	checkpoint_ticker *time.Ticker

	//remote bucket
	remote_bucket *service_def.RemoteBucketInfo

	support_ckpt bool

	cur_ckpts         map[uint16]*metadata.CheckpointRecord
	active_vbs        map[string][]uint16
	vb_highseqno_map  map[uint16]uint64
	failoverlog_map   map[uint16]*mcc.FailoverLog
	failoverlog_locks map[uint16]*sync.RWMutex

	logger          *log.CommonLogger
	cur_ckpts_locks map[uint16]*sync.RWMutex
}

func NewCheckpointManager(checkpoints_svc service_def.CheckpointsService, capi_svc service_def.CAPIService,
	remote_cluster_svc service_def.RemoteClusterSvc, rep_spec_svc service_def.ReplicationSpecSvc, cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc, through_seqno_tracker_svc service_def.ThroughSeqnoTrackerSvc, active_vbs map[string][]uint16, logger_ctx *log.LoggerContext) (*CheckpointManager, error) {
	if checkpoints_svc == nil || capi_svc == nil || remote_cluster_svc == nil || rep_spec_svc == nil || cluster_info_svc == nil || xdcr_topology_svc == nil {
		return nil, errors.New("checkpoints_svc, capi_svc, remote_cluster_svc, rep_spec_svc, cluster_info_svc and xdcr_topology_svc can't be nil")
	}
	logger := log.NewLogger("CheckpointManager", logger_ctx)
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
		logger:                    logger,
		cur_ckpts:                 make(map[uint16]*metadata.CheckpointRecord),
		cur_ckpts_locks:           make(map[uint16]*sync.RWMutex),
		active_vbs:                active_vbs,
		wait_grp:                  &sync.WaitGroup{},
		failoverlog_map:           make(map[uint16]*mcc.FailoverLog),
		failoverlog_locks:         make(map[uint16]*sync.RWMutex),
		vb_highseqno_map:          make(map[uint16]uint64)}, nil
}

func (ckmgr *CheckpointManager) Attach(pipeline common.Pipeline) error {

	ckmgr.logger.Infof("Attach checkpoint manager with pipeline %v\n", pipeline.InstanceId())

	ckmgr.pipeline = pipeline

	//populate the remote bucket information at the time of attaching
	err := ckmgr.populateRemoteBucketInfo(pipeline)

	if err != nil {
		return err
	}

	dcp_parts := pipeline.Sources()
	for _, dcp := range dcp_parts {
		dcp.RegisterComponentEventListener(common.StreamingStart, ckmgr)
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
	remote_bucket, err := service_def.NewRemoteBucketInfo(remoteClusterRef.Name, spec.TargetBucketName, remoteClusterRef, ckmgr.remote_cluster_svc, ckmgr.logger)
	if err != nil {
		return err
	}
	ckmgr.remote_bucket = remote_bucket

	ckmgr.checkCkptCapability()

	return nil
}

func (ckmgr *CheckpointManager) Start(settings map[string]interface{}) error {
	if ckpt_interval, ok := settings[metadata.CheckpointInterval].(int); ok {
		ckmgr.ckpt_interval = time.Duration(ckpt_interval) * time.Second
	} else {
		return errors.New(fmt.Sprintf("%v should be provided in settings", metadata.CheckpointInterval))
	}

	ckmgr.logger.Infof("CheckpointManager starting with ckpt_interval=%v s\n", ckmgr.ckpt_interval.Seconds())

	//randomize the starting point so that the checkpoint manager for the same
	//replication on different node the starting point is randomized.
	starting_time := time.Duration(rand.Intn(5000))*time.Millisecond + ckmgr.ckpt_interval
	ckmgr.logger.Infof("Checkpointing starts in %v sec", starting_time.Seconds())
	ckmgr.checkpoint_ticker = time.NewTicker(starting_time)

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
	ckmgr.wait_grp.Add(1)
	go ckmgr.massCheckVBOpaquesJob()
	return nil
}

func (ckmgr *CheckpointManager) initialize() {
	listOfVbs := ckmgr.getMyVBs()
	for _, vbno := range listOfVbs {
		ckmgr.cur_ckpts[vbno] = &metadata.CheckpointRecord{}
		ckmgr.cur_ckpts_locks[vbno] = &sync.RWMutex{}
		ckmgr.failoverlog_map[vbno] = nil
		ckmgr.failoverlog_locks[vbno] = &sync.RWMutex{}
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
	ckmgr.logger.Infof("Remote bucket %v supporting xdcrcheckpoing is %v\n", ckmgr.remote_bucket, ckmgr.support_ckpt)
}

func (ckmgr *CheckpointManager) updateCurrentVBOpaque(vbno uint16, vbOpaque metadata.TargetVBOpaque) error {
	ckmgr.cur_ckpts_locks[vbno].Lock()
	defer ckmgr.cur_ckpts_locks[vbno].Unlock()
	record := ckmgr.cur_ckpts[vbno]
	record.Target_vb_opaque = vbOpaque
	ckmgr.cur_ckpts[vbno] = record
	return nil
}

func getDocsProcessedForReplication(topic string, checkpoints_svc service_def.CheckpointsService,
	logger *log.CommonLogger) (uint64, error) {
	defer logger.Info("Done with GetDocsProcessedForPausedReplication")
	logger.Infof("Start GetDocsProcessedForPausedReplication for replication %v...", topic)

	logger.Debugf("Getting checkpoint for %v\n", topic)
	ckptDocs, err := checkpoints_svc.CheckpointsDocs(topic)
	logger.Debugf("Done getting checkpoint for %v\n", topic)
	if err != nil {
		return 0, err
	}

	var docsProcessed uint64

	for _, ckptDoc := range ckptDocs {
		// if checkpoint records exist, use the seqno in the first checkpoint record, which is the highest in all checkpoint records
		if ckptDoc != nil && ckptDoc.Checkpoint_records != nil && ckptDoc.Checkpoint_records[0] != nil {
			docsProcessed += ckptDoc.Checkpoint_records[0].Seqno
		}
	}

	return docsProcessed, nil

}

func (ckmgr *CheckpointManager) SetVBTimestamps(topic string) error {
	defer ckmgr.logger.Info("Done with SetVBTimestamps")
	ckmgr.logger.Infof("Set start seqnos for pipeline %v...", ckmgr.pipeline.InstanceId())

	//refresh the remote bucket
	err := ckmgr.remote_bucket.Refresh(ckmgr.remote_cluster_svc)
	if err != nil {
		return err
	}

	support_ckpt := ckmgr.support_ckpt

	ret := make(map[uint16]*base.VBTimestamp)
	listOfVbs := ckmgr.getMyVBs()
	ckmgr.logger.Infof("Getting checkpoint for %v\n", topic)
	ckptDocs, err := ckmgr.checkpoints_svc.CheckpointsDocs(topic)
	ckmgr.logger.Infof("Done getting checkpoint for %v\n", topic)
	if err != nil {
		return err
	}

	ckmgr.logger.Debugf("Found %v checkpoit document for replication %v\n", len(ckptDocs), topic)

	highseqnomap, err := ckmgr.getHighSeqno()
	if err != nil {
		//failed to get highseqno from stats, so go ahead without highseqno validation
		highseqnomap = nil
	}

	//divide the workload to several getter and run the getter parallelly
	workload := 5
	start_index := 0

	getter_wait_grp := &sync.WaitGroup{}
	errMap := make(map[uint16]error)
	getter_id := 0
	ret_map_map := make(map[int]map[uint16]*base.VBTimestamp)
	for {
		end_index := start_index + workload
		if end_index > len(listOfVbs) {
			end_index = len(listOfVbs)
		}
		vbs_for_getter := listOfVbs[start_index:end_index]
		getter_wait_grp.Add(1)
		ret_map_map[getter_id] = make(map[uint16]*base.VBTimestamp)

		go ckmgr.startSeqnoGetter(getter_id, vbs_for_getter, ckptDocs, support_ckpt, ret_map_map[getter_id], highseqnomap, getter_wait_grp, errMap)

		start_index = end_index
		if start_index >= len(listOfVbs) {
			break
		}
		getter_id++
	}

	//wait for all the getter done, then gather result
	getter_wait_grp.Wait()
	if len(errMap) > 0 {
		return errors.New(fmt.Sprintf("Failed to get starting seqno for pipeline %v", ckmgr.pipeline.InstanceId()))
	}

	for _, ret_map := range ret_map_map {
		for vbno, vbts := range ret_map {
			ret[vbno] = vbts
		}
	}

	settings := ckmgr.pipeline.Settings()
	settings["VBTimestamps"] = ret

	start_seqno_map := make(map[uint16]uint64)
	for vbno, ts := range ret {
		start_seqno_map[vbno] = ts.Seqno
	}
	ckmgr.through_seqno_tracker_svc.SetStartSeqnos(start_seqno_map)

	ckmgr.logger.Infof("Done with setting starting seqno for pipeline %v\n", ckmgr.pipeline.InstanceId())
	return nil
}

func (ckmgr *CheckpointManager) startSeqnoGetter(getter_id int, listOfVbs []uint16, ckptDocs map[uint16]*metadata.CheckpointsDoc,
	disableCkptBackwardsCompat bool, ret map[uint16]*base.VBTimestamp, highseqnomap map[uint16]uint64, waitGrp *sync.WaitGroup, errMap map[uint16]error) {
	ckmgr.logger.Debugf("StartSeqnoGetter %v is started to do _pre_prelicate for vb %v, waitGrp=%v\n", getter_id, listOfVbs, *waitGrp)
	defer waitGrp.Done()

	for _, vbno := range listOfVbs {
		var agreeedIndex int = -1
		ckptDoc, _ := ckptDocs[vbno]

		//do checkpointing only when the remote bucket supports xdcrcheckpointing
		//get the existing checkpoint records if they exist, otherwise return an empty ckpt record
		ckpt_list := ckmgr.ckptRecords(ckptDoc, vbno)
		for index, ckpt_record := range ckpt_list {
			if ckpt_record != nil {
				if len(errMap) > 0 {
					//there is already error
					return
				}

				remote_vb_status := &service_def.RemoteVBReplicationStatus{VBOpaque: ckpt_record.Target_vb_opaque,
					VBSeqno: ckpt_record.Target_Seqno,
					VBNo:    vbno}

				ckmgr.logger.Debugf("Negotiate checkpoint record %v...\n", ckpt_record)
				bMatch := false
				bMatch, current_remoteVBOpaque, err := ckmgr.capi_svc.PreReplicate(ckmgr.remote_bucket, remote_vb_status, ckmgr.support_ckpt)
				//remote vb topology changed
				//udpate the vb_uuid and try again
				if err == nil {
					ckmgr.updateCurrentVBOpaque(vbno, current_remoteVBOpaque)
					ckmgr.logger.Debugf("Remote vbucket %v has a new opaque %v, update\n", current_remoteVBOpaque, vbno)
					ckmgr.logger.Debugf("Done with _pre_prelicate call for %v for vbno=%v, bMatch=%v", remote_vb_status, vbno, bMatch)
				}

				if err != nil || bMatch {
					if bMatch {
						ckmgr.logger.Debugf("Remote bucket %v vbno %v agreed on the checkpoint %v\n", ckmgr.remote_bucket, vbno, ckpt_record)
						if ckptDoc != nil {
							agreeedIndex = index
						}

					} else if err == service_def.NoSupportForXDCRCheckpointingError {
						ckmgr.updateCurrentVBOpaque(vbno, nil)
						ckmgr.logger.Infof("Remote vbucket %v is on a old node which doesn't support checkpointing, update target_vb_uuid=0\n", vbno)

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
		var highseqno_vb uint64 = 0
		if highseqnomap != nil {
			highseqno_vb = highseqnomap[vbno]
		}
		vbts := ckmgr.populateVBTimestamp(ckptDoc, agreeedIndex, vbno, highseqno_vb)
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

func (ckmgr *CheckpointManager) getFailoverLog(bucket *couchbase.Bucket, listOfVbs []uint16) (couchbase.FailoverLog, error) {
	//Get failover log can hang, timeout the executation if it takes too long.
	failoverLogRetriever := newFailoverLogRetriever(listOfVbs, bucket, ckmgr.logger)
	err := utils.ExecWithTimeout(failoverLogRetriever.getFailiverLog, 20*time.Second, ckmgr.logger)
	if err != nil {
		return nil, errors.New("Failed to get failover log in 1 minute")
	}

	return failoverLogRetriever.cur_failover_log, nil
}

func (ckmgr *CheckpointManager) getSourceBucket() (*couchbase.Bucket, error) {
	topic := ckmgr.pipeline.Topic()
	spec, err := ckmgr.rep_spec_svc.ReplicationSpec(topic)
	if err != nil {
		return nil, err
	}
	sourcBucketName := spec.SourceBucketName

	bucket, err := ckmgr.cluster_info_svc.GetBucket(ckmgr.xdcr_topology_svc, sourcBucketName)
	if err != nil {
		return nil, err
	}
	ckmgr.logger.Infof("Got the bucket %v\n", sourcBucketName)
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
	ckmgr.logger.Infof("retrieve chkpt doc for vb=%v\n", vbno)
	return ckmgr.checkpoints_svc.CheckpointsDoc(ckmgr.pipeline.Topic(), vbno)
}

func (ckmgr *CheckpointManager) populateVBTimestamp(ckptDoc *metadata.CheckpointsDoc, agreedIndex int, vbno uint16, highseqno uint64) *base.VBTimestamp {
	vbts := &base.VBTimestamp{Vbno: vbno}
	if agreedIndex > -1 && ckptDoc != nil {
		ckpt_record := ckptDoc.Checkpoint_records[agreedIndex]
		vbts.Vbuuid = ckpt_record.Failover_uuid
		vbts.Seqno = ckpt_record.Seqno
		vbts.SnapshotStart = ckpt_record.Dcp_snapshot_seqno
		vbts.SnapshotEnd = ckpt_record.Dcp_snapshot_end_seqno

		//validate and adjust vbts
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
	defer ckmgr.logger.Info("Exits checkpointing routine.")
	defer ckmgr.wait_grp.Done()
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
			go ckmgr.performCkpt()
			ckmgr.checkpoint_ticker = time.NewTicker(ckmgr.ckpt_interval)
		}
	}

}

func (ckmgr *CheckpointManager) performCkpt() {
	interval_btwn_vb := time.Duration((ckmgr.ckpt_interval.Seconds()/float64(len(ckmgr.getMyVBs())))*1000) * time.Millisecond
	ckmgr.logger.Infof("Checkpointing for replication %v, interval_btwn_vb=%v sec\n", ckmgr.pipeline.Topic(), interval_btwn_vb.Seconds())
	interval_ticker := time.NewTicker(1 * time.Millisecond)
	first_vb := true
	err_map := make(map[uint16]error)
	var total_committing_time float64 = 0
	for _, vb := range ckmgr.getMyVBs() {
		select {
		case <-interval_ticker.C:
			if first_vb {
				interval_ticker.Stop()
				interval_ticker = time.NewTicker(interval_btwn_vb)
				first_vb = false
			}

			if ckmgr.pipeline.State() != common.Pipeline_Running {
				//pipeline is no longer running, return
				ckmgr.logger.Info("Pipeline is no longer running, exit do_checkpointing")
				return
			}

			start_time_vb := time.Now()
			err := ckmgr.do_checkpoint(vb)
			committing_time_vb := time.Since(start_time_vb)
			total_committing_time += committing_time_vb.Seconds()
			if err != nil {
				err_map[vb] = err
			}

		}
	}

	if len(err_map) > 0 {
		//error
		ckmgr.logger.Errorf("Checkpointing failed for replication %v, err=%v\n", ckmgr.pipeline.Topic(), err_map)
		err := errors.New("Checkpointing failed")
		otherInfo := utils.WrapError(err)
		ckmgr.RaiseEvent(common.ErrorEncountered, nil, ckmgr, nil, otherInfo)
	} else {
		ckmgr.logger.Infof("Done checkpointing for replication %v\n", ckmgr.pipeline.Topic())
		otherInfo := make(map[string]interface{})
		otherInfo[TimeCommiting] = time.Duration(total_committing_time) * time.Second
		ckmgr.RaiseEvent(common.CheckpointDone, nil, ckmgr, nil, otherInfo)
	}

}

func (ckmgr *CheckpointManager) do_checkpoint(vbno uint16) (err error) {
	//locking the current ckpt record and notsent_seqno list for this vb, no update is allowed during the checkpointing
	ckmgr.logger.Debugf("Checkpointing for vb=%v\n", vbno)
	ckmgr.cur_ckpts_locks[vbno].Lock()
	defer ckmgr.cur_ckpts_locks[vbno].Unlock()

	ckpt_record := ckmgr.cur_ckpts[vbno]

	ckpt_record.Seqno = ckmgr.through_seqno_tracker_svc.GetThroughSeqno(vbno)
	ckmgr.logger.Debugf("Seqno number used for checkpointing for vb %v is %v\n", vbno, ckpt_record.Seqno)

	if ckpt_record.Seqno == 0 {
		ckmgr.logger.Debugf("No replication happened yet, skip checkpointing for vb=%v pipeline=%v\n", vbno, ckmgr.pipeline.InstanceId())
		return nil
	}

	if ckpt_record.Target_vb_opaque == nil {
		ckmgr.logger.Info("remote bucket is no an older node, no checkpointing should be done.")
		return nil
	}

	remote_seqno, vbOpaque, err := ckmgr.capi_svc.CommitForCheckpoint(ckmgr.remote_bucket, ckpt_record.Target_vb_opaque, vbno)
	if err == nil {
		//succeed
		ckpt_record.Target_Seqno = remote_seqno
		ckpt_record.Failover_uuid = ckmgr.getFailoverUUIDForSeqno(vbno, ckpt_record.Seqno)
		err = ckmgr.persistCkptRecord(vbno, ckpt_record)
		if err == nil {
			ckmgr.raiseSuccessCkptForVbEvent(*ckpt_record, vbno)
		}

	} else {
		ckpt_record.Target_vb_opaque = vbOpaque
	}
	ckpt_record.Seqno = 0
	ckpt_record.Target_Seqno = 0
	ckpt_record.Failover_uuid = 0
	return
}

func (ckmgr *CheckpointManager) raiseSuccessCkptForVbEvent(ckpt_record metadata.CheckpointRecord, vbno uint16) {
	//notify statisticsManager
	otherInfo := make(map[string]interface{})
	otherInfo[Vbno] = vbno
	ckmgr.RaiseEvent(common.CheckpointDoneForVB, ckpt_record, ckmgr, nil, otherInfo)
}

func (ckmgr *CheckpointManager) persistCkptRecord(vbno uint16, ckpt_record *metadata.CheckpointRecord) error {
	ckmgr.logger.Debugf("Persist vb=%v ckpt_record=%v for %v\n", vbno, ckpt_record, ckmgr.pipeline.Topic())
	return ckmgr.checkpoints_svc.UpsertCheckpoints(ckmgr.pipeline.Topic(), vbno, ckpt_record)
}

func (ckmgr *CheckpointManager) OnEvent(eventType common.ComponentEventType,
	item interface{},
	component common.Component,
	derivedItems []interface{},
	otherInfos map[string]interface{}) {
	if eventType == common.StreamingStart {
		event, ok := item.(*mcc.UprEvent)
		if ok {
			flog := event.FailoverLog
			vbno := event.VBucket
			ckmgr.failoverlog_locks[vbno].Lock()
			defer ckmgr.failoverlog_locks[vbno].Unlock()

			ckmgr.failoverlog_map[vbno] = flog
			ckmgr.logger.Infof("Got failover log for vb=%v\n", vbno)
		}
	}

}

func (ckmgr *CheckpointManager) getFailoverUUIDForSeqno(vbno uint16, seqno uint64) uint64 {
	ckmgr.failoverlog_locks[vbno].RLock()
	ckmgr.failoverlog_locks[vbno].RUnlock()

	flog := ckmgr.failoverlog_map[vbno]
	for _, entry := range *flog {
		failover_uuid := entry[0]
		starting_seqno := entry[1]
		if seqno > starting_seqno {
			return failover_uuid
		}
	}
	return 0
}

func (ckmgr *CheckpointManager) UpdateVBTimestamps(vbno uint16, rollbackseqno uint64) (*base.VBTimestamp, error) {
	ckmgr.logger.Infof("Received rollback from DCP stream vb=%v, rollbackseqno=%v\n", vbno, rollbackseqno)
	pipeline_startSeqnos_map := GetStartSeqnos(ckmgr.pipeline, ckmgr.logger)
	pipeline_start_seqno, ok := pipeline_startSeqnos_map[vbno]
	if !ok {
		return nil, fmt.Errorf("Invalid vbno=%v\n", vbno)
	}
	if rollbackseqno >= pipeline_start_seqno.Seqno {
		panic(fmt.Sprintf("rollbackseqno=%v, current_start_seqno=%v", rollbackseqno, pipeline_start_seqno.Seqno))
	}

	checkpointDoc, err := ckmgr.retrieveCkptDoc(vbno)
	if err != nil {
		return nil, err
	}

	foundIndex := -1
	for index, ckpt_record := range checkpointDoc.Checkpoint_records {
		if ckpt_record == nil {
			break
		}

		//found the first ckpt record whose Seqno <= rollbackseqno
		if ckpt_record.Seqno <= rollbackseqno {
			foundIndex = index
			break
		}

	}

	vbts := ckmgr.populateVBTimestamp(checkpointDoc, foundIndex, vbno, pipeline_start_seqno.Seqno)
	pipeline_startSeqnos_map[vbno] = vbts
	ckmgr.logger.Infof("Retry vbts=%v\n", vbts)

	return vbts, nil
}

func GetStartSeqnos(pipeline common.Pipeline, logger *log.CommonLogger) map[uint16]*base.VBTimestamp {
	if pipeline != nil {
		settings := pipeline.Settings()
		startSeqnos_map, ok := settings["VBTimestamps"].(map[uint16]*base.VBTimestamp)
		if ok {
			//			logger.Infof("The current start seqno for %v is %v\n", pipeline.Topic(), startSeqnos_map)
			return startSeqnos_map
		} else {
			logger.Infof("Didn't find 'VBTimesstamps' in settings. settings=%v\n", settings)
		}
	} else {
		logger.Infof("pipleine is nil")
	}

	//it is not in settings, return an empty map
	return make(map[uint16]*base.VBTimestamp)
}

func (ckmgr *CheckpointManager) massCheckVBOpaquesJob() {
	defer ckmgr.logger.Info("Exits massCheckVBOpaquesJob routine.")
	defer ckmgr.wait_grp.Done()

	ticker := time.NewTicker(60 * time.Second)
	for {
		select {
		case <-ckmgr.finish_ch:
			ckmgr.logger.Info("Received finish signal")
			return
		case <-ticker.C:
			ckmgr.checkpoint_ticker.Stop()
			if ckmgr.pipeline.State() != common.Pipeline_Running {
				//pipeline is no longer running, kill itself
				ckmgr.logger.Info("Pipeline is no longer running, exit.")
				return
			}
			go ckmgr.massCheckVBOpaues()
		}
	}

}

func (ckmgr *CheckpointManager) massCheckVBOpaues() error {
	//validate target bucket's vbucket uuid
	target_vb_vbuuid_map := make(map[uint16]metadata.TargetVBOpaque)
	for vb, latest_ckpt_record := range ckmgr.cur_ckpts {
		target_vb_uuid := latest_ckpt_record.Target_vb_opaque
		target_vb_vbuuid_map[vb] = target_vb_uuid
	}

	matching, mismatching, missing, err1 := ckmgr.capi_svc.MassValidateVBUUIDs(ckmgr.remote_bucket, target_vb_vbuuid_map)
	if err1 != nil {
		ckmgr.logger.Errorf("MassValidateVBUUID failed, err=%v", err1)
		return err1
	} else {
		if len(matching) != len(target_vb_vbuuid_map) {
			//error
			ckmgr.logger.Errorf("Target bucket for replication %v's topology has changed. mismatch=%v, missing=%v\n", ckmgr.pipeline.Topic(), mismatching, missing)
			err := errors.New("Target bucket's topology has changed")
			otherInfo := utils.WrapError(err)
			ckmgr.RaiseEvent(common.ErrorEncountered, nil, ckmgr, nil, otherInfo)

		} else {
			ckmgr.logger.Infof("No target bucket topology change is detected for replication %v", ckmgr.pipeline.Topic())
		}
		return nil
	}

}
