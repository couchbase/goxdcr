// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package pipeline_manager

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"sync"
	"sync/atomic"
	"time"
)

var ReplicationSpecNotActive error = errors.New("Replication specification not found or no longer active")
var ReplicationStatusNotFound error = errors.New("Replication Status not found")

var default_failure_restart_interval = 10

type func_report_fixed func(topic string)

type PipelineManager struct {
	pipeline_factory   common.PipelineFactory
	repl_spec_svc      service_def.ReplicationSpecSvc
	xdcr_topology_svc  service_def.XDCRCompTopologySvc
	remote_cluster_svc service_def.RemoteClusterSvc
	cluster_info_svc   service_def.ClusterInfoSvc
	checkpoint_svc     service_def.CheckpointsService
	uilog_svc          service_def.UILogSvc
	once               sync.Once
	logger             *log.CommonLogger
	child_waitGrp      *sync.WaitGroup
	// used for making sure there's only one ReplicationStatus creation at one time
	repStatusCrLock sync.RWMutex
	utils           utilities.UtilsIface
}

type Pipeline_mgr_iface interface {
	OnExit() error
	StopAllUpdaters()
	StopPipeline(topic string) error
	StopPipelineInner(rep_status *pipeline.ReplicationStatus) error
	StartPipeline(topic string) error
	Update(topic string, cur_err error) error
	ReplicationStatusMap() map[string]*pipeline.ReplicationStatus
	ReplicationStatus(topic string) (*pipeline.ReplicationStatus, error)
	AllReplicationsForBucket(bucket string) []string
	AllReplications() []string
	AllReplicationsForTargetCluster(targetClusterUuid string) []string
	RemoveReplicationStatus(topic string) error
	AllReplicationSpecsForTargetCluster(targetClusterUuid string) map[string]*metadata.ReplicationSpecification
	// get a replicationStatus for the given topic
	// returns: ptr to the replicationStatus
	// Will return nil if the replicationStatus does not exist, and a new one will be created
	GetOrCreateReplicationStatus(topic string, cur_err error) (*pipeline.ReplicationStatus, error)
	GetLastUpdateResult(topic string) bool // whether or not the last update was successful
	GetRemoteClusterSvc() service_def.RemoteClusterSvc
	GetClusterInfoSvc() service_def.ClusterInfoSvc
	GetLogSvc() service_def.UILogSvc
	GetReplSpecSvc() service_def.ReplicationSpecSvc
	GetXDCRTopologySvc() service_def.XDCRCompTopologySvc
}

type PipelineUpdaterIface interface {
	run()
	update() bool
	reportStatus()
	raiseXattrWarningIfNeeded(p common.Pipeline)
	checkReplicationActiveness() (err error)
	stop()
	clearError()
	allowableErrorCodes(err error) bool // returns true if the error code is not considered a failure for update
	refreshPipelineManually()
	refreshPipelineDueToErr(err error)
	scheduleFutureRefresh()
	cancelFutureRefresh() bool // returns true if timer was cancelled successfully, false if fired
	sendUpdateNow()
	sendUpdateErr(err error)
	setLastUpdateSuccess()
	setLastUpdateFailure(err error)
	getLastResult() bool // whether or not the last update was successful
	isScheduledTimerNil() bool
}

// Global ptr, should slowly get rid of refences to this global
var pipeline_mgr *PipelineManager

func NewPipelineManager(factory common.PipelineFactory, repl_spec_svc service_def.ReplicationSpecSvc, xdcr_topology_svc service_def.XDCRCompTopologySvc,
	remote_cluster_svc service_def.RemoteClusterSvc, cluster_info_svc service_def.ClusterInfoSvc, checkpoint_svc service_def.CheckpointsService,
	uilog_svc service_def.UILogSvc, logger_context *log.LoggerContext, utilsIn utilities.UtilsIface) *PipelineManager {

	pipelineMgrRetVar := &PipelineManager{
		pipeline_factory:   factory,
		repl_spec_svc:      repl_spec_svc,
		xdcr_topology_svc:  xdcr_topology_svc,
		remote_cluster_svc: remote_cluster_svc,
		checkpoint_svc:     checkpoint_svc,
		logger:             log.NewLogger("PipelineMgr", logger_context),
		cluster_info_svc:   cluster_info_svc,
		uilog_svc:          uilog_svc,
		child_waitGrp:      &sync.WaitGroup{},
		utils:              utilsIn,
	}
	pipelineMgrRetVar.logger.Info("Pipeline Manager is constucted")

	//initialize the expvar storage for replication status
	pipeline.RootStorage()

	if pipeline_mgr == nil {
		pipeline_mgr = pipelineMgrRetVar
	}

	return pipelineMgrRetVar
}

// TO be deprecated, use interface method below
func ReplicationStatus(topic string) (*pipeline.ReplicationStatus, error) {
	return pipeline_mgr.ReplicationStatus(topic)
}

// Use this one - able to be mocked
func (pipelineMgr *PipelineManager) ReplicationStatus(topic string) (*pipeline.ReplicationStatus, error) {
	obj, err := pipelineMgr.repl_spec_svc.GetDerivedObj(topic)
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, pipelineMgr.utils.ReplicationStatusNotFoundError(topic)
	}
	return obj.(*pipeline.ReplicationStatus), nil
}

func NewMCRequestObj(topic string) (*base.WrappedMCRequest, error) {
	rep_status, err := ReplicationStatus(topic)
	if err != nil {
		return nil, err
	}
	return rep_status.ObjectPool().Get(), nil
}

func RecycleMCRequestObj(topic string, obj *base.WrappedMCRequest) {
	rep_status, _ := ReplicationStatus(topic)
	if rep_status != nil {
		rep_status.ObjectPool().Put(obj)
	}
}

func Update(topic string, cur_err error) error {
	return pipeline_mgr.Update(topic, cur_err)
}

func (pipelineMgr *PipelineManager) RemoveReplicationStatus(topic string) error {
	// TODO - we should really cleanUp all the other non-user places that call GetOrCreateReplication
	// After that is done, we should lock repStatusCrLock here so removal/creation are serialized
	rs, err := pipelineMgr.ReplicationStatus(topic)
	if err != nil {
		return err
	}

	if rs != nil {
		updaterObj := rs.Updater()

		pipelineMgr.logger.Infof("Stopping pipeline %v since the replication spec has been deleted\n", topic)
		if updaterObj == nil {
			pipelineMgr.logger.Errorf("Updater object is nil, may be leaking an updater somewhere\n", topic)
		} else {
			updater := updaterObj.(*PipelineUpdater)
			updater.stop()
		}
	}

	return nil
}

//This doesn't include the replication status of just deleted replication spec in the map
// TODO - This should be deprecated. We should really move towards a object-oriented method
// of calling ReplicationStatusMap
func ReplicationStatusMap() map[string]*pipeline.ReplicationStatus {
	return pipeline_mgr.ReplicationStatusMap()
}

// This should really be the method to be used. This is considered part of the interface
// and can be easily unit tested
func (pipelineMgr *PipelineManager) ReplicationStatusMap() map[string]*pipeline.ReplicationStatus {
	ret := make(map[string]*pipeline.ReplicationStatus)
	specId_list, err := pipelineMgr.repl_spec_svc.AllReplicationSpecIds()
	if err == nil {
		for _, specId := range specId_list {
			rep_status, _ := pipelineMgr.GetOrCreateReplicationStatus(specId, nil)
			ret[specId] = rep_status
		}
	}
	return ret
}

func LogStatusSummary() {
	rep_status_map := pipeline_mgr.ReplicationStatusMap()
	if len(rep_status_map) > 0 {
		pipeline_mgr.logger.Infof("Replication Status = %v\n", rep_status_map)
	}
}

func (pipelineMgr *PipelineManager) AllReplicationsForBucket(bucket string) []string {
	var repIds []string
	for _, key := range pipelineMgr.topics() {
		// there should not be any errors since topics() should return valid replication ids
		match, _ := metadata.IsReplicationIdForSourceBucket(key, bucket)
		if match {
			repIds = append(repIds, key)
		}
	}
	return repIds
}

func (pipelineMgr *PipelineManager) AllReplicationSpecsForTargetCluster(targetClusterUuid string) map[string]*metadata.ReplicationSpecification {
	ret := make(map[string]*metadata.ReplicationSpecification)
	for topic, rep_status := range pipelineMgr.ReplicationStatusMap() {
		if rep_status.Spec().TargetClusterUUID == targetClusterUuid {
			ret[topic] = rep_status.Spec()
		}
	}

	return ret
}

func (pipelineMgr *PipelineManager) AllReplicationsForTargetCluster(targetClusterUuid string) []string {
	ret := make([]string, 0)
	specs := pipelineMgr.AllReplicationSpecsForTargetCluster(targetClusterUuid)

	for topic, _ := range specs {
		ret = append(ret, topic)
	}

	return ret
}

func (pipelineMgr *PipelineManager) AllReplications() []string {
	return pipelineMgr.topics()
}

func IsPipelineRunning(topic string) bool {
	rep_status, _ := ReplicationStatus(topic)
	if rep_status != nil {
		return (rep_status.RuntimeStatus(true) == pipeline.Replicating)
	} else {
		return false
	}
}

func (pipelineMgr *PipelineManager) CheckPipelines() {
	rep_status_map := pipelineMgr.ReplicationStatusMap()
	for _, rep_status := range rep_status_map {
		//validate replication spec
		spec := rep_status.Spec()
		if spec != nil {
			pipelineMgr.logger.Infof("checkpipeline spec=%v, uuid=%v", spec, spec.SourceBucketUUID)
			pipelineMgr.repl_spec_svc.ValidateAndGC(spec)
		}
	}
	LogStatusSummary()
}

func RuntimeCtx(topic string) common.PipelineRuntimeContext {
	return pipeline_mgr.runtimeCtx(topic)
}

func (pipelineMgr *PipelineManager) OnExit() error {
	//send finish signal to all updater to stop the pipelines and exit
	pipelineMgr.StopAllUpdaters()

	pipelineMgr.logger.Infof("Sent finish signal to all running repairer")
	pipelineMgr.child_waitGrp.Wait()

	return nil

}

func (pipelineMgr *PipelineManager) StopAllUpdaters() {
	rep_status_map := pipelineMgr.ReplicationStatusMap()
	if rep_status_map == nil {
		return
	}
	for _, rep_status := range rep_status_map {
		updater := rep_status.Updater()
		if updater != nil {
			updater.(*PipelineUpdater).stop()
		}
	}
}

func (pipelineMgr *PipelineManager) StartPipeline(topic string) error {
	var err error
	pipelineMgr.logger.Infof("Starting the pipeline %s\n", topic)

	rep_status, _ := pipelineMgr.ReplicationStatus(topic)
	if rep_status == nil || (rep_status != nil && rep_status.RuntimeStatus(true) != pipeline.Replicating) {
		// validate the pipeline before starting it
		err = pipelineMgr.validatePipeline(topic)
		if err != nil {
			return err
		}

		if rep_status == nil {
			// This should not be nil as updater should be the only one calling this and
			// it would have created a rep_status way before here
			return errors.New("Error: Replication Status is missing when starting pipeline.")
		}

		rep_status.RecordProgress("Start pipeline construction")

		p, err := pipelineMgr.pipeline_factory.NewPipeline(topic, rep_status.RecordProgress)
		if err != nil {
			pipelineMgr.logger.Errorf("Failed to construct a new pipeline with topic %v: %s", topic, err.Error())
			return err
		}

		rep_status.RecordProgress("Pipeline is constructed")
		rep_status.SetPipeline(p)

		pipelineMgr.logger.Infof("Pipeline %v is constructed. Starting it.", p.InstanceId())
		p.SetProgressRecorder(rep_status.RecordProgress)
		err = p.Start(rep_status.SettingsMap())
		if err != nil {
			pipelineMgr.logger.Error("Failed to start the pipeline")
			return err
		}

		return nil
	} else {
		//the pipeline is already running
		pipelineMgr.logger.Infof("The pipeline asked to be started, %v, is already running", topic)
		return err
	}
	return err
}

// validate that a pipeline has valid configuration and can be started before starting it
func (pipelineMgr *PipelineManager) validatePipeline(topic string) error {
	pipelineMgr.logger.Infof("Validating pipeline %v\n", topic)

	spec, err := pipelineMgr.repl_spec_svc.ReplicationSpec(topic)
	if err != nil {
		pipelineMgr.logger.Errorf("Failed to get replication specification for pipeline %v, err=%v\n", topic, err)
		return err
	}

	targetClusterRef, err := pipelineMgr.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, false)
	if err != nil {
		pipelineMgr.logger.Errorf("Error getting remote cluster with uuid=%v for pipeline %v, err=%v\n", spec.TargetClusterUUID, topic, err)
		return err
	}

	err = pipelineMgr.remote_cluster_svc.ValidateRemoteCluster(targetClusterRef)
	if err != nil {
		pipelineMgr.logger.Errorf("Error validating remote cluster with uuid %v for pipeline %v. err=%v\n", spec.TargetClusterUUID, topic, err)
		return err
	}

	return nil
}

func (pipelineMgr *PipelineManager) getPipelineFromMap(topic string) common.Pipeline {
	rep_status, _ := ReplicationStatus(topic)
	if rep_status != nil {
		return rep_status.Pipeline()
	}
	return nil
}

func (pipelineMgr *PipelineManager) removePipelineFromReplicationStatus(p common.Pipeline) error {
	//	return nil
	if p != nil {
		rep_status, _ := ReplicationStatus(p.Topic())
		if rep_status != nil {
			rep_status.SetPipeline(nil)
		} else {
			return fmt.Errorf("Replication %v hasn't been registered with PipelineManager yet", p.Topic())

		}
	}
	return nil

}

func (pipelineMgr *PipelineManager) StopPipeline(topic string) error {
	rep_status, err := pipelineMgr.ReplicationStatus(topic)
	if err != nil {
		return err
	}
	return pipelineMgr.StopPipelineInner(rep_status)
}

func (pipelineMgr *PipelineManager) StopPipelineInner(rep_status *pipeline.ReplicationStatus) error {
	if rep_status == nil {
		return fmt.Errorf("Invalid parameter value rep_status=nil")
	}

	replId := rep_status.RepId()

	pipelineMgr.logger.Infof("Trying to stop the pipeline %s", replId)
	var err error

	p := rep_status.Pipeline()

	if p != nil {
		state := p.State()
		if state == common.Pipeline_Running || state == common.Pipeline_Starting || state == common.Pipeline_Error {
			err = p.Stop()
			if err != nil {
				pipelineMgr.logger.Errorf("Received error when stopping pipeline %v - %v\n", replId, err)
				//pipeline failed to stopped gracefully in time. ignore the error.
				//the parts of the pipeline will eventually commit suicide.
			} else {
				pipelineMgr.logger.Infof("Pipeline %v has been stopped\n", replId)
			}
			pipelineMgr.removePipelineFromReplicationStatus(p)
			pipelineMgr.logger.Infof("Replication Status=%v\n", rep_status)
		} else {
			pipelineMgr.logger.Infof("Pipeline %v is not in the right state to be stopped. state=%v\n", replId, state)
		}
	} else {
		pipelineMgr.logger.Infof("Pipeline %v is not running\n", replId)
	}

	// if replication spec has been deleted
	// or deleted and recreated, which is signaled by change in spec internal id
	// perform clean up
	spec, _ := pipelineMgr.repl_spec_svc.ReplicationSpec(replId)
	if spec == nil || (rep_status.SpecInternalId != "" && rep_status.SpecInternalId != spec.InternalId) {
		if spec == nil {
			pipelineMgr.logger.Infof("%v Cleaning up replication status since repl spec has been deleted.\n", replId)
		} else {
			pipelineMgr.logger.Infof("%v Cleaning up replication status since repl spec has been deleted and recreated. oldSpecInternalId=%v, newSpecInternalId=%v\n", replId, rep_status.SpecInternalId, spec.InternalId)
		}

		pipelineMgr.checkpoint_svc.DelCheckpointsDocs(replId)

		rep_status.ResetStorage()
		pipelineMgr.repl_spec_svc.SetDerivedObj(replId, nil)

		//close the connection pool for the replication
		pools := base.ConnPoolMgr().FindPoolNamesByPrefix(replId)
		for _, poolName := range pools {
			base.ConnPoolMgr().RemovePool(poolName)
		}
	}

	return err
}

func (pipelineMgr *PipelineManager) runtimeCtx(topic string) common.PipelineRuntimeContext {
	pipeline := pipelineMgr.pipeline(topic)
	if pipeline != nil {
		return pipeline.RuntimeContext()
	}

	return nil
}

func (pipelineMgr *PipelineManager) pipeline(topic string) common.Pipeline {
	pipeline := pipelineMgr.getPipelineFromMap(topic)
	return pipeline
}

func (pipelineMgr *PipelineManager) liveTopics() []string {
	rep_status_map := pipelineMgr.ReplicationStatusMap()
	topics := make([]string, 0, len(rep_status_map))
	for topic, rep_status := range rep_status_map {
		if rep_status.RuntimeStatus(true) == pipeline.Replicating {
			topics = append(topics, topic)
		}
	}
	return topics
}

func (pipelineMgr *PipelineManager) topics() []string {
	rep_status_map := pipelineMgr.ReplicationStatusMap()
	topics := make([]string, 0, len(rep_status_map))
	for topic, _ := range rep_status_map {
		topics = append(topics, topic)
	}

	return topics
}

func (pipelineMgr *PipelineManager) livePipelines() map[string]common.Pipeline {
	ret := make(map[string]common.Pipeline)
	rep_status_map := pipelineMgr.ReplicationStatusMap()
	for topic, rep_status := range rep_status_map {
		if rep_status.RuntimeStatus(true) == pipeline.Replicating {
			ret[topic] = rep_status.Pipeline()
		}
	}

	return ret
}

// Should be only called by GetOrCreateReplicationStatus
func (pipelineMgr *PipelineManager) launchUpdater(topic string, cur_err error, rep_status *pipeline.ReplicationStatus) error {
	settingsMap := rep_status.SettingsMap()
	retry_interval_obj := settingsMap[metadata.FailureRestartInterval]
	// retry_interval_obj may be null in abnormal scenarios, e.g., when replication spec has been deleted
	// default retry_interval to 10 seconds in such cases
	retry_interval := default_failure_restart_interval
	if retry_interval_obj != nil {
		retry_interval = settingsMap[metadata.FailureRestartInterval].(int)
	}

	updater := newPipelineUpdater(topic, retry_interval, pipelineMgr.child_waitGrp, cur_err, rep_status, pipelineMgr.logger, pipelineMgr)

	// SetUpdater should not fail
	err := rep_status.SetUpdater(updater)
	if err != nil {
		return err
	}

	pipelineMgr.child_waitGrp.Add(1)
	go updater.run()
	pipelineMgr.logger.Infof("Pipeline updater %v is lauched with retry_interval=%v\n", topic, retry_interval)

	return nil
}

func (pipelineMgr *PipelineManager) GetOrCreateReplicationStatus(topic string, cur_err error) (*pipeline.ReplicationStatus, error) {
	var repStatus *pipeline.ReplicationStatus
	/**
	 * The repl_spec_svc holds a map of topics, but it's possible that there's a race during
	 * creation. As a result, if a go routine is unable to find a specific ReplicationStatus, then
	 * it needs to wait in line for creation, checking each time to make sure that no other go routines
	 * before it has not created the topic of what it's trying to create.
	 */
	pipelineMgr.repStatusCrLock.RLock()
	repStatus, _ = pipelineMgr.ReplicationStatus(topic)
	pipelineMgr.repStatusCrLock.RUnlock()
	if repStatus != nil {
		return repStatus, nil
	} else {
		var retErr error
		//	actual portion of creating replicationStatus and updater
		pipelineMgr.repStatusCrLock.Lock()
		defer pipelineMgr.repStatusCrLock.Unlock()
		// Potentially, another go routine could have jumped ahead of us
		repStatus, retErr = pipelineMgr.ReplicationStatus(topic)
		if repStatus == nil {
			// actual portion of creating replicationStatus and updater - we really don't have anything
			repStatus = pipeline.NewReplicationStatus(topic, pipelineMgr.repl_spec_svc.ReplicationSpec, pipelineMgr.logger)
			pipelineMgr.repl_spec_svc.SetDerivedObj(topic, repStatus)
			pipelineMgr.logger.Infof("ReplicationStatus is created and set with %v\n", topic)
			if repStatus.Updater() != nil {
				panic("non-nill updater with a new rep_status")
			}
			retErr = pipelineMgr.launchUpdater(topic, cur_err, repStatus)
		}
		return repStatus, retErr
	}
}

func (pipelineMgr *PipelineManager) Update(topic string, cur_err error) error {

	rep_status, retErr := pipelineMgr.GetOrCreateReplicationStatus(topic, cur_err)
	if rep_status == nil {
		combinedRetErr := errors.New(ReplicationStatusNotFound.Error() + " Cause: " + retErr.Error())
		return combinedRetErr
	} else if retErr != nil {
		return retErr
	}

	updaterObj := rep_status.Updater()
	if updaterObj == nil {
		errorStr := fmt.Sprint("Failed to get updater from replication status in topic %v in update()", topic)
		return errors.New(errorStr)
	} else {
		updater, ok := updaterObj.(*PipelineUpdater)
		if updater == nil || !ok {
			errorStr := fmt.Sprint("Unable to cast updaterObj as type PipelineUpdater in topic %v", topic)
			return errors.New(errorStr)
		}

		// if update is not initiated by error
		// trigger updater to update immediately, not to wait for the retry interval
		if cur_err != nil {
			rep_status.AddError(cur_err)
			updater.refreshPipelineDueToErr(cur_err)
		} else {
			updater.refreshPipelineManually()
		}
	}
	return nil
}

// Use this only for unit test
func (pipelineMgr *PipelineManager) GetLastUpdateResult(topic string) bool {
	repStatus, err := pipelineMgr.ReplicationStatus(topic)
	if err != nil || repStatus == nil || repStatus.Updater() == nil {
		return false
	} else {
		updater := repStatus.Updater().(*PipelineUpdater)
		return updater.getLastResult()
	}
}

func (pipelineMgr *PipelineManager) GetRemoteClusterSvc() service_def.RemoteClusterSvc {
	return pipelineMgr.remote_cluster_svc
}

func (pipelineMgr *PipelineManager) GetClusterInfoSvc() service_def.ClusterInfoSvc {
	return pipelineMgr.cluster_info_svc
}

func (pipelineMgr *PipelineManager) GetLogSvc() service_def.UILogSvc {
	return pipelineMgr.uilog_svc
}

func (pipelineMgr *PipelineManager) GetReplSpecSvc() service_def.ReplicationSpecSvc {
	return pipelineMgr.repl_spec_svc
}

func (pipelineMgr *PipelineManager) GetXDCRTopologySvc() service_def.XDCRCompTopologySvc {
	return pipelineMgr.xdcr_topology_svc
}

var updaterStateErrorStr = "Can't move update state from %v to %v"

const (
	pipelineUpdaterErrInjNil         int32 = iota
	pipelineUpdaterErrInjOfflineFail int32 = iota
)

//pipelineRepairer is responsible to repair a failing pipeline
//it will retry after the retry_interval
type PipelineUpdater struct {
	//the name of the pipeline to be repaired
	pipeline_name string
	//the interval to wait after the failure for next retry
	retry_interval time.Duration
	//finish channel
	fin_ch chan bool
	//update-now channel
	update_now_ch chan bool
	// update-error channel
	update_err_ch chan error
	// channel indicating whether updater is really done
	done_ch chan bool
	//the current error
	current_error error

	// passed in from and managed by pipeline_manager
	waitGrp *sync.WaitGroup

	// Pipeline Manager that created this updater
	pipelineMgr Pipeline_mgr_iface

	// should modify it via scheduleFutureRefresh() and cancelFutureRefresh()
	// Null if nothing is being scheduled
	scheduledTimerLock sync.RWMutex
	scheduledTimer     *time.Timer

	// boolean to store whether or not the last update was successful
	lastSuccessful bool

	rep_status          *pipeline.ReplicationStatus
	logger              *log.CommonLogger
	pipelineUpdaterLock sync.RWMutex

	// A counter for keeping track how many times this updater has executed with allowable error codes
	runCounter uint64

	// Unit test only - should be in milliseconds
	testCustomScheduleTime time.Duration
	testInjectionError     int32
}

func newPipelineUpdater(pipeline_name string, retry_interval int, waitGrp *sync.WaitGroup, cur_err error, rep_status_in *pipeline.ReplicationStatus, logger *log.CommonLogger, pipelineMgr_in *PipelineManager) *PipelineUpdater {
	if rep_status_in == nil {
		panic("nil ReplicationStatus")
	}
	repairer := &PipelineUpdater{pipeline_name: pipeline_name,
		retry_interval: time.Duration(retry_interval) * time.Second,
		fin_ch:         make(chan bool, 1),
		done_ch:        make(chan bool, 1),
		update_now_ch:  make(chan bool, 1),
		update_err_ch:  make(chan error, 1),
		waitGrp:        waitGrp,
		rep_status:     rep_status_in,
		logger:         logger,
		pipelineMgr:    pipelineMgr_in,
		lastSuccessful: true,
		current_error:  cur_err}

	return repairer
}

func (r *PipelineUpdater) getLastResult() bool {
	r.pipelineUpdaterLock.RLock()
	defer r.pipelineUpdaterLock.RUnlock()
	return r.lastSuccessful
}

//start the repairer
func (r *PipelineUpdater) run() {
	defer r.waitGrp.Done()
	defer close(r.done_ch)

	var retErr error

	for {
		select {
		case <-r.fin_ch:
			r.logger.Infof("Quit updating pipeline %v after updating a total of %v times\n", r.pipeline_name, atomic.LoadUint64(&r.runCounter))
			r.logger.Infof("Replication %v's status is to be closed, shutting down\n", r.pipeline_name)
			r.pipelineMgr.StopPipelineInner(r.rep_status)
			// Reset runCounter to 0 for unit test
			atomic.StoreUint64(&r.runCounter, 0)
			return
		case <-r.update_now_ch:
			r.logger.Infof("Replication %v's status is changed, update now\n", r.pipeline_name)
			retErr = r.update()
			r.cancelFutureRefresh()
			if retErr != nil {
				r.logger.Infof("Replication %v update experienced an error: %v. Scheduling a redo.\n", r.pipeline_name, retErr)
				r.update_err_ch <- retErr
			}
		case retErr = <-r.update_err_ch:
			var updateAgain bool
			if r.getLastResult() {
				r.logger.Infof("Replication %v's status experienced changes or errors (%v), updating now\n", r.pipeline_name, retErr)
				// Last time update succeeded, so this error then triggers an immediate update
				r.current_error = retErr
				if r.update() != nil {
					updateAgain = true
				}
			} else {
				r.logger.Infof("Replication %v's status experienced changes or errors (%v). However, last update resulted in failure, so will reschedule a future update\n", r.pipeline_name, retErr)
				// Last time update failed, so we should wait for stablization
				updateAgain = true
			}
			if updateAgain {
				r.scheduleFutureRefresh()
			}
		}
	}
}

func (r *PipelineUpdater) allowableErrorCodes(err error) bool {
	if err == nil ||
		err == ReplicationSpecNotActive ||
		err == service_def.MetadataNotFoundErr {
		return true
	}
	return false
}

//update the pipeline
func (r *PipelineUpdater) update() error {
	// Used externally
	var retVal error
	// Used internally
	var err error
	var p common.Pipeline

	if r.current_error == nil {
		r.logger.Infof("Try to start/restart Pipeline %v. \n", r.pipeline_name)
	} else {
		r.logger.Infof("Try to fix Pipeline %v. Current error=%v \n", r.pipeline_name, r.current_error)
	}

	r.logger.Infof("Try to stop pipeline %v\n", r.pipeline_name)
	err = r.pipelineMgr.StopPipelineInner(r.rep_status)

	// unit test error injection
	if atomic.LoadInt32(&r.testInjectionError) == pipelineUpdaterErrInjOfflineFail {
		r.logger.Infof("Error injected... this should only happen during unit test")
		err = errors.New("Injected Offline failure")
	}
	// end unit test error injection

	if err != nil {
		// If pipeline fails to stop, everything within it should "commit suicide" eventually
		goto RE
	}

	// For unit test, perhaps sleep to induce some scheduling
	time.Sleep(time.Duration(100) * time.Nanosecond)

	err = r.checkReplicationActiveness()
	if err != nil {
		goto RE
	}

	err = r.pipelineMgr.StartPipeline(r.pipeline_name)

RE:
	if err == nil {
		r.logger.Infof("Replication %v has been updated. Back to business\n", r.pipeline_name)
	} else if err == ReplicationSpecNotActive {
		r.logger.Infof("Replication %v has been paused. no need to update\n", r.pipeline_name)
	} else if err == service_def.MetadataNotFoundErr {
		r.logger.Infof("Replication %v has been deleted. no need to update\n", r.pipeline_name)
	} else {
		r.logger.Errorf("Failed to update pipeline %v, err=%v\n", r.pipeline_name, err)
	}

	if r.allowableErrorCodes(err) {
		r.logger.Infof("Pipeline %v has been updated successfully\n", r.pipeline_name)
		r.setLastUpdateSuccess()
		if err == nil {
			r.raiseXattrWarningIfNeeded(p)
		}
		if r.rep_status != nil {
			r.rep_status.ClearErrors()
		}
		r.clearError()
		retVal = nil
		atomic.AddUint64(&r.runCounter, 1)
	} else {
		r.setLastUpdateFailure(err)
		r.logger.Errorf("Update of pipeline %v failed with error=%v\n", r.pipeline_name, err)
		retVal = err
	}
	r.reportStatus()

	return retVal
}

func (r *PipelineUpdater) reportStatus() {
	if r.rep_status != nil {
		r.rep_status.AddError(r.current_error)
	}
}

// raise warning on UI console when
// 1. replication is not of capi type
// 2. replication is not recovering from error
// 3. target cluster does not support xattr
// 4. current node is the master for vbucket 0 - this is needed to ensure that warning is shown on UI only once, instead of once per source node
func (r *PipelineUpdater) raiseXattrWarningIfNeeded(p common.Pipeline) {
	if r.current_error == nil {
		if p == nil {
			r.logger.Warnf("Skipping xattr warning check since pipeline %v has not been started\n", r.pipeline_name)
			return
		}

		spec := p.Specification()
		if spec == nil {
			r.logger.Warnf("Skipping xattr warning check since cannot find replication spec for pipeline %v\n", r.pipeline_name)
			return
		}

		if spec.Settings.IsCapi() {
			return
		}

		targetClusterRef, err := r.pipelineMgr.GetRemoteClusterSvc().RemoteClusterByUuid(spec.TargetClusterUUID, false)
		if err != nil {
			r.logger.Warnf("Skipping xattr warning check since received error getting remote cluster with uuid=%v for pipeline %v, err=%v\n", spec.TargetClusterUUID, spec.Id, err)
			return
		}

		hasXattrSupport, err := r.pipelineMgr.GetClusterInfoSvc().IsClusterCompatible(targetClusterRef, base.VersionForRBACAndXattrSupport)
		if err != nil {
			r.logger.Warnf("Skipping xattr warning check since received error checking target cluster version. target cluster=%v, pipeline=%v, err=%v\n", spec.TargetClusterUUID, spec.Id, err)
			return
		}
		if !hasXattrSupport {
			errMsg := fmt.Sprintf("Replication from source bucket '%v' to target bucket '%v' on cluster '%v' has been started. Note - Target cluster is older than 5.0.0, hence some of the new feature enhancements such as \"Extended Attributes (XATTR)\" are not supported, which might result in loss of XATTR data. If this is not acceptable, please pause the replication, upgrade cluster '%v' to 5.0.0, and restart replication.", spec.SourceBucketName, spec.TargetBucketName, targetClusterRef.Name, targetClusterRef.Name)
			r.logger.Warn(errMsg)

			sourceVbList := pipeline_utils.GetSourceVBListPerPipeline(p)
			containsVb0 := false
			for _, vbno := range sourceVbList {
				if vbno == 0 {
					containsVb0 = true
					break
				}
			}

			if containsVb0 {
				// write warning to UI
				r.pipelineMgr.GetLogSvc().Write(errMsg)
			}
		}
	}
}

func (r *PipelineUpdater) checkReplicationActiveness() (err error) {
	spec, err := r.pipelineMgr.GetReplSpecSvc().ReplicationSpec(r.pipeline_name)
	if err != nil || spec == nil || !spec.Settings.Active {
		err = ReplicationSpecNotActive
	} else {
		r.logger.Debugf("Pipeline %v is not paused or deleted\n", r.pipeline_name)
	}
	return
}

//It should be called only once.
func (r *PipelineUpdater) stop() {
	defer func() {
		if e := recover(); e != nil {
			r.logger.Infof("Updater for pipeline is already stopped\n", r.pipeline_name)
		}
	}()

	close(r.fin_ch)

	// wait for updater to really stop
	<-r.done_ch
}

// This method will update the replicationStatus immediately
func (r *PipelineUpdater) refreshPipelineManually() {
	r.sendUpdateNow()
}

func (r *PipelineUpdater) refreshPipelineDueToErr(err error) {
	r.sendUpdateErr(err)
}

// Lock must be held
func (r *PipelineUpdater) sendUpdateNow() {
	select {
	case r.update_now_ch <- true:
	default:
		r.logger.Infof("Update-now message is already delivered for %v\n", r.pipeline_name)
	}
	r.logger.Infof("Replication status is updated now, current status=%v\n", r.rep_status)
}

func (r *PipelineUpdater) sendUpdateErr(err error) {
	select {
	case r.update_err_ch <- err:
	default:
		r.logger.Infof("Update-err message is already delivered for %v\n", r.pipeline_name)
	}
	r.logger.Infof("Replication status is updated with error %v, current status=%v\n", err, r.rep_status)
}

func (r *PipelineUpdater) isScheduledTimerNil() bool {
	r.scheduledTimerLock.RLock()
	defer r.scheduledTimerLock.RUnlock()
	return r.scheduledTimer == nil
}

func (r *PipelineUpdater) scheduleFutureRefresh() {
	var scheduledTimeMs time.Duration = r.retry_interval
	r.scheduledTimerLock.Lock()
	defer r.scheduledTimerLock.Unlock()

	if r.testCustomScheduleTime > 0 {
		scheduledTimeMs = r.testCustomScheduleTime
		r.logger.Infof("Running unit test with custom sleep time of: %v\n", scheduledTimeMs)
	}

	if r.scheduledTimer == nil {
		r.logger.Infof("Pipeline updater scheduled to update in %v\n", scheduledTimeMs)
		r.scheduledTimer = time.AfterFunc(scheduledTimeMs, func() {
			r.logger.Infof("Pipeline updater scheduled time is up. Executing pipeline updater\n")
			r.sendUpdateNow()
		})
	}
}

func (r *PipelineUpdater) setLastUpdateSuccess() {
	r.pipelineUpdaterLock.Lock()
	defer r.pipelineUpdaterLock.Unlock()
	r.lastSuccessful = true
}

func (r *PipelineUpdater) setLastUpdateFailure(err error) {
	r.pipelineUpdaterLock.Lock()
	defer r.pipelineUpdaterLock.Unlock()
	r.lastSuccessful = false
	r.current_error = err
}

// Stopped == true means that we stopped it in time
// If stopped is false it means that the timer fired during the window of when we're trying to check
func (r *PipelineUpdater) cancelFutureRefresh() bool {
	var stopped bool
	r.scheduledTimerLock.Lock()
	defer r.scheduledTimerLock.Unlock()
	if r.scheduledTimer != nil {
		stopped = r.scheduledTimer.Stop()
		r.scheduledTimer = nil
	} else {
		// If no timer, pretend we stopped it in time
		stopped = true
	}
	return stopped
}

func (r *PipelineUpdater) clearError() {
	r.pipelineUpdaterLock.Lock()
	defer r.pipelineUpdaterLock.Unlock()

	r.current_error = nil
}

func getRequestPoolSize(rep_status *pipeline.ReplicationStatus, numOfTargetNozzles int) int {
	return rep_status.Spec().Settings.BatchCount * 52 * numOfTargetNozzles
}
