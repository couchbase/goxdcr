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
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/simple_utils"
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

var CHECKPOINT_INTERVAL = "checkpoint_interval"

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

	cur_ckpts        map[uint16]*checkpointRecordWithLock
	active_vbs       map[string][]uint16
	vb_highseqno_map map[uint16]uint64
	failoverlog_map  map[uint16]*failoverlogWithLock

	logger *log.CommonLogger
}

type checkpointRecordWithLock struct {
	ckpt *metadata.CheckpointRecord
	lock *sync.RWMutex
}

type failoverlogWithLock struct {
	failoverlog *mcc.FailoverLog
	lock        *sync.RWMutex
}

func NewCheckpointManager(checkpoints_svc service_def.CheckpointsService, capi_svc service_def.CAPIService,
	remote_cluster_svc service_def.RemoteClusterSvc, rep_spec_svc service_def.ReplicationSpecSvc, cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc, through_seqno_tracker_svc service_def.ThroughSeqnoTrackerSvc,
	active_vbs map[string][]uint16, logger_ctx *log.LoggerContext) (*CheckpointManager, error) {
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
		checkpoint_ticker_ch:      make(chan *time.Ticker, 1000),
		logger:                    logger,
		cur_ckpts:                 make(map[uint16]*checkpointRecordWithLock),
		active_vbs:                active_vbs,
		wait_grp:                  &sync.WaitGroup{},
		failoverlog_map:           make(map[uint16]*failoverlogWithLock),
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

	//register pipeline supervisor as ckmgr's error handler
	supervisor := ckmgr.pipeline.RuntimeContext().Service(base.PIPELINE_SUPERVISOR_SVC)
	if supervisor == nil {
		return errors.New("Pipeline supervisor has to exist")
	}
	err = ckmgr.RegisterComponentEventListener(common.ErrorEncountered, supervisor.(*PipelineSupervisor))
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

	ckmgr.logger.Infof("CheckpointManager starting with ckpt_interval=%v s\n", ckmgr.ckpt_interval.Seconds())

	ckmgr.startRandomizedCheckpointingTicker()

	//start checkpointing loop
	ckmgr.wait_grp.Add(1)
	go ckmgr.checkpointing()
	return nil
}

func (ckmgr *CheckpointManager) startRandomizedCheckpointingTicker() {
	//randomize the starting point so that the checkpoint manager for the same
	//replication on different node the starting point is randomized.
	starting_time := time.Duration(rand.Intn(5000))*time.Millisecond + ckmgr.ckpt_interval
	ckmgr.logger.Infof("Checkpointing starts in %v sec", starting_time.Seconds())
	ckmgr.checkpoint_ticker_ch <- time.NewTicker(starting_time)
}

func (ckmgr *CheckpointManager) initialize() {
	listOfVbs := ckmgr.getMyVBs()
	for _, vbno := range listOfVbs {
		ckmgr.cur_ckpts[vbno] = &checkpointRecordWithLock{ckpt: &metadata.CheckpointRecord{}, lock: &sync.RWMutex{}}
		ckmgr.failoverlog_map[vbno] = &failoverlogWithLock{failoverlog: nil, lock: &sync.RWMutex{}}
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
	ckmgr.logger.Infof("Remote bucket %v supporting xdcrcheckpointing is %v\n", ckmgr.remote_bucket, ckmgr.support_ckpt)
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
	defer ckmgr.logger.Info("Done with SetVBTimestamps")
	ckmgr.logger.Infof("Set start seqnos for pipeline %v...", ckmgr.pipeline.InstanceId())

	//refresh the remote bucket
	err := ckmgr.remote_bucket.Refresh(ckmgr.remote_cluster_svc)
	if err != nil {
		ckmgr.logger.Errorf("Received error when trying to set VBTimestamps: %v\n", err)
		ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
		return err
	}

	support_ckpt := ckmgr.support_ckpt

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

	highseqnomap, err := ckmgr.getHighSeqno()
	if err != nil {
		//failed to get highseqno from stats, so go ahead without highseqno validation
		highseqnomap = nil
	}

	//divide the workload to several getter and run the getter parallelly
	workload := 100
	start_index := 0

	getter_wait_grp := &sync.WaitGroup{}
	err_ch := make(chan error, len(listOfVbs))
	getter_id := 0
	for {
		end_index := start_index + workload
		if end_index > len(listOfVbs) {
			end_index = len(listOfVbs)
		}
		vbs_for_getter := listOfVbs[start_index:end_index]
		getter_wait_grp.Add(1)
		go ckmgr.startSeqnoGetter(getter_id, vbs_for_getter, ckptDocs, support_ckpt, highseqnomap, getter_wait_grp, err_ch)

		start_index = end_index
		if start_index >= len(listOfVbs) {
			break
		}
		getter_id++
	}

	//wait for all the getter done, then gather result
	getter_wait_grp.Wait()
	if len(err_ch) > 0 {
		err = errors.New(fmt.Sprintf("Failed to get starting seqno for pipeline %v", ckmgr.pipeline.InstanceId()))
		ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
		return err
	}

	ckmgr.logger.Infof("Done with setting starting seqno for pipeline %v\n", ckmgr.pipeline.InstanceId())

	ckmgr.wait_grp.Add(1)
	go ckmgr.massCheckVBOpaquesJob()

	return nil
}

func (ckmgr *CheckpointManager) setTimestampForVB(vbno uint16, ts *base.VBTimestamp) error {
	ckmgr.logger.Debugf("%v Set VBTimestamp: vb=%v, ts.Seqno=%v\n", ckmgr.pipeline.Topic(), vbno, ts.Seqno)
	ckmgr.logger.Debugf("%v vb=%v ts=%v\n", ckmgr.pipeline.Topic(), vbno, ts)
	defer ckmgr.logger.Debugf("%v Set VBTimestamp for vb=%v completed\n", ckmgr.pipeline.Topic(), vbno)

	//set the start seqno on through_seqno_tracker_svc
	ckmgr.through_seqno_tracker_svc.SetStartSeqno(vbno, ts.Seqno)
	ckmgr.logger.Debugf("%v Set startSeqno to %v for vb=%v\n", ckmgr.pipeline.Topic(), ts.Seqno, vbno)

	settings := make(map[string]interface{})
	ts_map := make(map[uint16]*base.VBTimestamp)
	ts_map[vbno] = ts
	settings[base.VBTimestamps] = ts_map
	//notify the settings change
	ckmgr.pipeline.UpdateSettings(settings)

	return nil
}

func (ckmgr *CheckpointManager) startSeqnoGetter(getter_id int, listOfVbs []uint16, ckptDocs map[uint16]*metadata.CheckpointsDoc,
	disableCkptBackwardsCompat bool, highseqnomap map[uint16]uint64, waitGrp *sync.WaitGroup, err_ch chan error) {
	ckmgr.logger.Infof("StartSeqnoGetter %v is started to do _pre_prelicate for vb %v\n", getter_id, listOfVbs)
	defer waitGrp.Done()

	for _, vbno := range listOfVbs {
		var agreeedIndex int = -1
		ckptDoc, _ := ckptDocs[vbno]

		//do checkpointing only when the remote bucket supports xdcrcheckpointing
		//get the existing checkpoint records if they exist, otherwise return an empty ckpt record
		ckpt_list := ckmgr.ckptRecords(ckptDoc, vbno)
		for index, ckpt_record := range ckpt_list {
			if ckpt_record != nil {
				if len(err_ch) > 0 {
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
						ckmgr.logger.Errorf("Pre_replicate failed for %v. err=%v\n", vbno, err)
						err_ch <- err

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
		err1 := ckmgr.setTimestampForVB(vbno, vbts)
		if err1 != nil {
			err_ch <- err1
		}
	}
}

func (ckmgr *CheckpointManager) ckptRecords(ckptDoc *metadata.CheckpointsDoc, vbno uint16) []*metadata.CheckpointRecord {
	if ckptDoc != nil {
		ckmgr.logger.Infof("Found checkpoint doc for vb=%v\n", vbno)
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
	retriever.logger.Debugf("failoverlogMap=%v\n", retriever.cur_failover_log)
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
	bucket_name := ckmgr.pipeline.Specification().SourceBucketName
	bucket, err := ckmgr.cluster_info_svc.GetBucket(ckmgr.xdcr_topology_svc, bucket_name)
	if err != nil {
		return nil, err
	}
	ckmgr.logger.Infof("Got the bucket %v for ckmgr\n", bucket_name)
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
	obj, ok := ckmgr.cur_ckpts[vbno]
	if ok {
		obj.lock.Lock()
		defer obj.lock.Unlock()

		//populate the next ckpt (in cur_ckpts)'s information based on the previous checkpoint information if it exists
		if agreedIndex > -1 && ckptDoc != nil {
			obj.ckpt.Failover_uuid = vbts.Vbuuid
			obj.ckpt.Dcp_snapshot_seqno = vbts.SnapshotStart
			obj.ckpt.Dcp_snapshot_end_seqno = vbts.SnapshotEnd
		}
		//set the next ckpt's Seqno to 0 - the unset state
		obj.ckpt.Seqno = 0
	} else {
		panic(fmt.Sprintf("Calling populateVBTimestamp on vb=%v which is not in MyVBList", vbno))
	}
	return vbts
}

func (ckmgr *CheckpointManager) checkpointing() {
	ckmgr.logger.Info("checkpointing rountine started")

	defer ckmgr.logger.Info("Exits checkpointing routine.")
	defer ckmgr.wait_grp.Done()

	ticker := <-ckmgr.checkpoint_ticker_ch

	children_fin_ch := make(chan bool)
	children_wait_grp := &sync.WaitGroup{}

	first := true

	for {
		select {
		case new_ticker := <-ckmgr.checkpoint_ticker_ch:
			ckmgr.logger.Info("Received new ticker due to changes to checkpoint interval setting")

			// wait for existing, if any, performCkpt routine to finish before setting new ticker
			close(children_fin_ch)
			children_wait_grp.Wait()
			children_fin_ch = make(chan bool)

			ticker.Stop()
			ticker = new_ticker
			first = true
		case <-ckmgr.finish_ch:
			ckmgr.logger.Info("Received finish signal")
			return
		case <-ticker.C:
			if ckmgr.pipeline.State() != common.Pipeline_Running {
				//pipeline is no longer running, kill itself
				ckmgr.logger.Info("Pipeline is no longer running, exit.")
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

func (ckmgr *CheckpointManager) performCkpt(fin_ch chan bool, wait_grp *sync.WaitGroup) {
	defer wait_grp.Done()

	interval_btwn_vb := time.Duration((ckmgr.ckpt_interval.Seconds()/float64(len(ckmgr.getMyVBs())))*1000) * time.Millisecond
	ckmgr.logger.Infof("Checkpointing for replication %v, interval_btwn_vb=%v sec\n", ckmgr.pipeline.Topic(), interval_btwn_vb.Seconds())
	err_map := make(map[uint16]error)
	var total_committing_time float64 = 0
	listOfVBs := ckmgr.getMyVBs()

	for index, vb := range listOfVBs {
		select {
		case <-fin_ch:
			ckmgr.logger.Info("Aborting checkpointing routine since received finish signal for checkpointing")
			return
		default:
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

			if index < len(listOfVBs)-1 {
				time.Sleep(interval_btwn_vb)
			}

		}
	}

	if len(err_map) > 0 {
		//error
		ckmgr.logger.Errorf("Checkpointing failed for replication %v, err=%v\n", ckmgr.pipeline.Topic(), err_map)
		err := errors.New("Checkpointing failed")
		ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))
	} else {
		ckmgr.logger.Infof("Done checkpointing for replication %v\n", ckmgr.pipeline.Topic())
		ckmgr.RaiseEvent(common.NewEvent(common.CheckpointDone, nil, ckmgr, nil, time.Duration(total_committing_time)*time.Second))
	}

}

func (ckmgr *CheckpointManager) do_checkpoint(vbno uint16) (err error) {
	//locking the current ckpt record and notsent_seqno list for this vb, no update is allowed during the checkpointing
	ckmgr.logger.Debugf("Checkpointing for vb=%v\n", vbno)

	ckpt_obj, ok := ckmgr.cur_ckpts[vbno]
	if ok {
		ckpt_obj.lock.Lock()
		defer ckpt_obj.lock.Unlock()

		ckpt_record := ckpt_obj.ckpt

		ckpt_record.Seqno = ckmgr.through_seqno_tracker_svc.GetThroughSeqno(vbno)
		ckmgr.logger.Debugf("Seqno number used for checkpointing for vb %v is %v\n", vbno, ckpt_record.Seqno)

		if ckpt_record.Seqno == 0 {
			ckmgr.logger.Debugf("No replication happened yet, skip checkpointing for vb=%v pipeline=%v\n", vbno, ckmgr.pipeline.InstanceId())
			return nil
		}

		if ckpt_record.Target_vb_opaque == nil {
			ckmgr.logger.Info("remote bucket is an older node, no checkpointing should be done.")
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
			if vbOpaque != nil {
				ckpt_record.Target_vb_opaque = vbOpaque
			}
		}
		ckpt_record.Seqno = 0
		ckpt_record.Target_Seqno = 0
		ckpt_record.Failover_uuid = 0
	} else {
		panic(fmt.Sprintf("Trying to do_checkpoint on vb=%v which is not in MyVBList", vbno))

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
				ckmgr.logger.Tracef("Got failover log for vb=%v\n", vbno)
			} else {
				panic(fmt.Sprintf("Received failoverlog on an unknown vb=%v\n", vbno))
			}
		}
	}

}

func (ckmgr *CheckpointManager) getFailoverUUIDForSeqno(vbno uint16, seqno uint64) uint64 {
	failoverlog_obj, ok1 := ckmgr.failoverlog_map[vbno]
	if ok1 {
		failoverlog_obj.lock.RLock()
		defer failoverlog_obj.lock.RUnlock()

		flog := failoverlog_obj.failoverlog
		if flog != nil {
			for _, entry := range *flog {
				failover_uuid := entry[0]
				starting_seqno := entry[1]
				if seqno > starting_seqno {
					return failover_uuid
				}
			}
		}
	} else {
		panic(fmt.Sprintf("Calling getFailoverUUIDForSeqno on an unknown vb=%v\n", vbno))

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

	//set the start seqno on through_seqno_tracker_svc
	ckmgr.through_seqno_tracker_svc.SetStartSeqno(vbno, vbts.Seqno)
	ckmgr.logger.Infof("%v Rolled back startSeqno to %v for vb=%v\n", ckmgr.pipeline.Topic(), vbts.Seqno, vbno)

	ckmgr.logger.Infof("Retry vbts=%v\n", vbts)

	return vbts, nil
}

func GetStartSeqnos(pipeline common.Pipeline, logger *log.CommonLogger) map[uint16]*base.VBTimestamp {
	if pipeline != nil {
		settings := pipeline.Settings()
		startSeqnos_map, ok := settings[base.VBTimestamps].(map[uint16]*base.VBTimestamp)
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

	target_vb_vbuuid_map := make(map[uint16]metadata.TargetVBOpaque)

	ticker := time.NewTicker(60 * time.Second)
	for {
		select {
		case <-ckmgr.finish_ch:
			ckmgr.logger.Info("Received finish signal")
			return
		case <-ticker.C:
			if ckmgr.pipeline.State() != common.Pipeline_Running {
				//pipeline is no longer running, kill itself
				ckmgr.logger.Info("Pipeline is no longer running, exit.")
				return
			}
			go ckmgr.massCheckVBOpaques(target_vb_vbuuid_map)
		}
	}

}

func (ckmgr *CheckpointManager) massCheckVBOpaques(target_vb_vbuuid_map map[uint16]metadata.TargetVBOpaque) error {
	//validate target bucket's vbucket uuid
	for vb, _ := range ckmgr.cur_ckpts {
		latest_ckpt_record := ckmgr.getCurrentCkpt(vb)
		if latest_ckpt_record.Target_vb_opaque != nil {
			target_vb_uuid := latest_ckpt_record.Target_vb_opaque
			target_vb_vbuuid_map[vb] = target_vb_uuid
		} else {
			ckmgr.logger.Infof("remote bucket is an older node, massCheckVBOpaque is not supported for vb=%v.", vb)
		}
	}

	if len(target_vb_vbuuid_map) > 0 {
		matching, mismatching, missing, err1 := ckmgr.capi_svc.MassValidateVBUUIDs(ckmgr.remote_bucket, target_vb_vbuuid_map)
		if err1 != nil {
			ckmgr.logger.Errorf("MassValidateVBUUID failed, err=%v", err1)
			return err1
		} else {
			if len(matching) != len(target_vb_vbuuid_map) && (len(mismatching) > 0 || len(missing) > 0) {
				//error
				ckmgr.logger.Errorf("Target bucket for replication %v's topology has changed. mismatch=%v, missing=%v, mataching\n", ckmgr.pipeline.Topic(), mismatching, missing)
				err := errors.New("Target bucket's topology has changed")
				ckmgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, ckmgr, nil, err))

			} else {
				ckmgr.logger.Infof("No target bucket topology change is detected for replication %v", ckmgr.pipeline.Topic())
			}
			return nil
		}
	}
	return nil
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
