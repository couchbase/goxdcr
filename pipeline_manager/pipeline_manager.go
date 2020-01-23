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
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

var ReplicationSpecNotActive error = errors.New("Replication specification not found or no longer active")
var ReplicationStatusNotFound error = errors.New("Replication Status not found")
var UpdaterStoppedError error = errors.New("Updater already stopped")

var default_failure_restart_interval = 10

// Maximum entries in an error map or array
const MaxErrorDataEntries = 50

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
	serializer         *PipelineOpSerializer
	utils              utilities.UtilsIface
}

type Pipeline_mgr_iface interface {
	// External APIs
	InitiateRepStatus(pipelineName string) error
	ReInitStreams(pipelineName string) error
	UpdatePipeline(pipelineName string, cur_err error) error
	DeletePipeline(pipelineName string) error
	CheckPipelines()

	// Internal APIs
	OnExit() error
	StopAllUpdaters()
	StopPipeline(rep_status pipeline.ReplicationStatusIface) base.ErrorMap
	StartPipeline(topic string) base.ErrorMap
	Update(topic string, cur_err error) error
	ReplicationStatusMap() map[string]*pipeline.ReplicationStatus
	ReplicationStatus(topic string) (*pipeline.ReplicationStatus, error)
	AllReplicationsForBucket(bucket string) []string
	AllReplications() []string
	AllReplicationsForTargetCluster(targetClusterUuid string) []string
	CleanupPipeline(topic string) error
	RemoveReplicationStatus(topic string) error
	RemoveReplicationCheckpoints(topic string) error
	AllReplicationSpecsForTargetCluster(targetClusterUuid string) map[string]*metadata.ReplicationSpecification
	GetOrCreateReplicationStatus(topic string, cur_err error) (*pipeline.ReplicationStatus, error)
	GetLastUpdateResult(topic string) bool // whether or not the last update was successful
	GetRemoteClusterSvc() service_def.RemoteClusterSvc
	GetClusterInfoSvc() service_def.ClusterInfoSvc
	GetLogSvc() service_def.UILogSvc
	GetReplSpecSvc() service_def.ReplicationSpecSvc
	GetXDCRTopologySvc() service_def.XDCRCompTopologySvc
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
		utils:              utilsIn,
	}
	pipelineMgrRetVar.logger.Info("Pipeline Manager is constructed")

	//initialize the expvar storage for replication status
	pipeline.RootStorage()

	// initialize serializer - this is the front-end of all user requests
	pipelineMgrRetVar.serializer = NewPipelineOpSerializer(pipelineMgrRetVar, pipelineMgrRetVar.logger)

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
	pipeline_mgr.serializer.Update(topic, cur_err)
	return nil
}

// Should be called only from serializer to prevent race
func (pipelineMgr *PipelineManager) RemoveReplicationStatus(topic string) error {
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

func (pipelineMgr *PipelineManager) RemoveReplicationCheckpoints(topic string) error {
	rep_status, err := pipelineMgr.ReplicationStatus(topic)
	if err != nil {
		return err
	}
	replId := rep_status.RepId()

	err = pipelineMgr.checkpoint_svc.DelCheckpointsDocs(replId)
	if err != nil {
		pipelineMgr.logger.Errorf("Unable to delete checkpoints for pipeline %v\n", topic)
	}
	return err
}

// External APIs
//This doesn't include the replication status of just deleted replication spec in the map
// TODO - This should be deprecated. We should really move towards a object-oriented method
// of calling ReplicationStatusMap
func ReplicationStatusMap() map[string]*pipeline.ReplicationStatus {
	return pipeline_mgr.ReplicationStatusMap()
}

func (pipelineMgr *PipelineManager) UpdatePipeline(topic string, cur_err error) error {
	return pipelineMgr.serializer.Update(topic, cur_err)
}

func (pipelineMgr *PipelineManager) DeletePipeline(pipelineName string) error {
	return pipelineMgr.serializer.Delete(pipelineName)
}

func (pipelineMgr *PipelineManager) InitiateRepStatus(pipelineName string) error {
	return pipelineMgr.serializer.Init(pipelineName)
}

// This should really be the method to be used. This is considered part of the interface
// and can be easily unit tested
func (pipelineMgr *PipelineManager) ReplicationStatusMap() map[string]*pipeline.ReplicationStatus {
	ret := make(map[string]*pipeline.ReplicationStatus)
	specId_list, err := pipelineMgr.repl_spec_svc.AllReplicationSpecIds()
	if err == nil {
		for _, specId := range specId_list {
			repStatus, _ := pipelineMgr.ReplicationStatus(specId)
			if repStatus != nil {
				ret[specId] = repStatus
			}
		}
	}
	return ret
}

func LogStatusSummary() {
	pipeline_mgr.logger.Infof("Replication Status = %v\n", pipeline_mgr.ReplicationStatusMap())
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
		spec := rep_status.Spec()
		if spec.TargetClusterUUID == targetClusterUuid {
			ret[topic] = spec
		}
	}

	return ret
}

func (pipelineMgr *PipelineManager) AllReplicationsForTargetCluster(targetClusterUuid string) []string {
	ret := make([]string, 0)
	for topic, rep_status := range pipelineMgr.ReplicationStatusMap() {
		if rep_status.Spec().TargetClusterUUID == targetClusterUuid {
			ret = append(ret, topic)
		}
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
	pipelineMgr.validateAndGCSpecs()
	pipelineMgr.checkRemoteClusterSvcForChangedConfigs()
	LogStatusSummary()
}

func (pipelineMgr *PipelineManager) validateAndGCSpecs() {
	rep_status_map := pipelineMgr.ReplicationStatusMap()
	for _, rep_status := range rep_status_map {
		//validate replication spec
		spec := rep_status.Spec()
		if spec != nil {
			pipelineMgr.logger.Infof("checking pipeline spec=%v, source bucket uuid=%v", spec, spec.SourceBucketUUID)
			pipelineMgr.repl_spec_svc.ValidateAndGC(spec)
		}
	}
}

func (pipelineMgr *PipelineManager) checkRemoteClusterSvcForChangedConfigs() {
	refList, err := pipelineMgr.remote_cluster_svc.GetRefListForRestartAndClearState()
	if err != nil {
		pipelineMgr.logger.Warnf("Checkpipeline got %v when asking remote cluster reference for config changes that require pipeline restarts", err)
	}
	for _, ref := range refList {
		replSpecsList, err := pipelineMgr.repl_spec_svc.AllReplicationSpecsWithRemote(ref)
		if err != nil {
			pipelineMgr.logger.Warnf("Unable to retrieve specs for remote cluster %v ... user should manually restart pipelines replicating to this remote cluster", ref.Id())
			continue
		}
		for _, spec := range replSpecsList {
			pipelineMgr.UpdatePipeline(spec.Id, base.ErrorPipelineRestartDueToClusterConfigChange)
		}
	}
}

func (pipelineMgr *PipelineManager) OnExit() error {
	//send finish signal to all updater to stop the pipelines and exit
	pipelineMgr.StopSerializer()
	pipelineMgr.StopAllUpdaters()

	pipelineMgr.logger.Infof("All running repairers stopped")
	return nil

}

func (pipelineMgr *PipelineManager) StopAllUpdaters() {
	rep_status_map := pipelineMgr.ReplicationStatusMap()
	for _, rep_status := range rep_status_map {
		updater := rep_status.Updater()
		if updater != nil {
			updater.(*PipelineUpdater).stop()
		}
	}
}

func (pipelineMgr *PipelineManager) StopSerializer() {
	if pipelineMgr.serializer != nil {
		pipelineMgr.serializer.Stop()
	}
}

func (pipelineMgr *PipelineManager) StartPipeline(topic string) base.ErrorMap {
	var err error
	errMap := make(base.ErrorMap)
	pipelineMgr.logger.Infof("Starting the pipeline %s\n", topic)

	rep_status, _ := pipelineMgr.ReplicationStatus(topic)
	if rep_status == nil {
		// This should not be nil as updater should be the only one calling this and
		// it would have created a rep_status way before here
		errMap[fmt.Sprintf("pipelineMgr.ReplicationStatus(%v)", topic)] = errors.New("Error: Replication Status is missing when starting pipeline.")
		return errMap
	}

	if rep_status.RuntimeStatus(true) == pipeline.Replicating {
		//the pipeline is already running
		pipelineMgr.logger.Infof("The pipeline asked to be started, %v, is already running", topic)
		return errMap
	}

	// validate the pipeline before starting it
	err = pipelineMgr.validatePipeline(topic)
	if err != nil {
		errMap[fmt.Sprintf("pipelineMgr.validatePipeline(%v)", topic)] = err
		return errMap
	}

	rep_status.RecordProgress("Start pipeline construction")

	p, err := pipelineMgr.pipeline_factory.NewPipeline(topic, rep_status.RecordProgress)
	if err != nil {
		errMap[fmt.Sprintf("pipelineMgr.pipeline_factory.NewPipeline(%v)", topic)] = err
		return errMap
	}

	rep_status.RecordProgress("Pipeline is constructed")
	rep_status.SetPipeline(p)

	pipelineMgr.logger.Infof("Pipeline %v is constructed. Starting it.", p.InstanceId())
	p.SetProgressRecorder(rep_status.RecordProgress)
	errMap = p.Start(rep_status.SettingsMap())
	if len(errMap) > 0 {
		pipelineMgr.logger.Errorf("Failed to start the pipeline %v", p.InstanceId())
	}
	return errMap
}

// validate that a pipeline has valid configuration and can be started before starting it
func (pipelineMgr *PipelineManager) validatePipeline(topic string) error {
	pipelineMgr.logger.Infof("Validating pipeline %v\n", topic)

	spec, err := pipelineMgr.repl_spec_svc.ReplicationSpec(topic)
	if err != nil {
		pipelineMgr.logger.Errorf("Failed to get replication specification for pipeline %v, err=%v\n", topic, err)
		return err
	}

	// refresh remote cluster reference when retrieving it, hence making sure that all fields,
	// especially the security settings like sanInCertificate, are up to date
	targetClusterRef, err := pipelineMgr.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, true)
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

// Called internally from updater only
func (pipelineMgr *PipelineManager) StopPipeline(rep_status pipeline.ReplicationStatusIface) base.ErrorMap {
	var errMap base.ErrorMap = make(base.ErrorMap)

	if rep_status == nil {
		errMap["pipelineMgr.StopPipeline"] = errors.New("Invalid parameter value rep_status=nil")
		return errMap
	}

	replId := rep_status.RepId()

	pipelineMgr.logger.Infof("Trying to stop the pipeline %s", replId)

	p := rep_status.Pipeline()

	if p != nil {
		state := p.State()
		if state == common.Pipeline_Running || state == common.Pipeline_Starting || state == common.Pipeline_Error {
			errMap = p.Stop()
			if len(errMap) > 0 {
				pipelineMgr.logger.Errorf("Received error(s) when stopping pipeline %v - %v\n", replId, base.FlattenErrorMap(errMap))
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
	if spec == nil || (rep_status.GetSpecInternalId() != "" && rep_status.GetSpecInternalId() != spec.InternalId) {
		if spec == nil {
			pipelineMgr.logger.Infof("%v Cleaning up replication status since repl spec has been deleted.\n", replId)
		} else {
			pipelineMgr.logger.Infof("%v Cleaning up replication status since repl spec has been deleted and recreated. oldSpecInternalId=%v, newSpecInternalId=%v\n", replId, rep_status.GetSpecInternalId(), spec.InternalId)
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

	return errMap
}

// Stops pipeline and cleans all checkpoints with it, results in a restream
// Add retry mechanism here because failure will not be ideal
// Should be called only from serializer
func (pipelineMgr *PipelineManager) CleanupPipeline(topic string) error {
	var rep_status *pipeline.ReplicationStatus
	var err error
	defer pipelineMgr.logger.Infof("%v CleanupPipeline including checkpoints removal finished (err = %v)", topic, err)
	getOp := func() error {
		rep_status, err = pipelineMgr.GetOrCreateReplicationStatus(topic, nil)
		return err
	}
	err = pipelineMgr.utils.ExponentialBackoffExecutor(fmt.Sprintf("GetOrCreateReplicationStatus %v", topic), base.PipelineSerializerRetryWaitTime, base.PipelineSerializerMaxRetry, base.PipelineSerializerRetryFactor, getOp)

	if err != nil {
		return err
	}

	updater := rep_status.Updater().(*PipelineUpdater)
	replId := rep_status.RepId()

	// Stop the updater to stop the pipeline so it will not handle any more jobs before we remove the checkpoints
	updater.stop()

	retryOp := func() error {
		return pipelineMgr.checkpoint_svc.DelCheckpointsDocs(replId)
	}
	err = pipelineMgr.utils.ExponentialBackoffExecutor(fmt.Sprintf("DelCheckpointsDocs %v", topic), base.PipelineSerializerRetryWaitTime, base.PipelineSerializerMaxRetry, base.PipelineSerializerRetryFactor, retryOp)
	if err != nil {
		pipelineMgr.logger.Warnf("Removing checkpoint resulting in error: %v\n")
	}

	// regardless of err above, we should restart updater
	err = pipelineMgr.launchUpdater(topic, nil, rep_status)

	return err
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
	retry_interval_obj := settingsMap[metadata.FailureRestartIntervalKey]
	// retry_interval_obj may be null in abnormal scenarios, e.g., when replication spec has been deleted
	// default retry_interval to 10 seconds in such cases
	retry_interval := default_failure_restart_interval
	if retry_interval_obj != nil {
		retry_interval = settingsMap[metadata.FailureRestartIntervalKey].(int)
	}

	updater := newPipelineUpdater(topic, retry_interval, cur_err, rep_status, pipelineMgr.logger, pipelineMgr)

	// SetUpdater should not fail
	err := rep_status.SetUpdater(updater)
	if err != nil {
		return err
	}

	go updater.run()
	pipelineMgr.logger.Infof("Pipeline updater %v is launched with retry_interval=%v\n", topic, retry_interval)

	return nil
}

// Should be used internally from serializer only, or only from an internal update op
func (pipelineMgr *PipelineManager) GetOrCreateReplicationStatus(topic string, cur_err error) (*pipeline.ReplicationStatus, error) {
	var repStatus *pipeline.ReplicationStatus
	repStatus, _ = pipelineMgr.ReplicationStatus(topic)
	if repStatus != nil {
		return repStatus, nil
	} else {
		var retErr error
		repStatus = pipeline.NewReplicationStatus(topic, pipelineMgr.repl_spec_svc.ReplicationSpec, pipelineMgr.logger)
		pipelineMgr.repl_spec_svc.SetDerivedObj(topic, repStatus)
		pipelineMgr.logger.Infof("ReplicationStatus is created and set with %v\n", topic)
		if repStatus.Updater() != nil {
			panic("non-nill updater with a new rep_status")
		}
		retErr = pipelineMgr.launchUpdater(topic, cur_err, repStatus)
		return repStatus, retErr
	}
}

// Should be called internally from serializer's Update()
func (pipelineMgr *PipelineManager) Update(topic string, cur_err error) error {
	// Since this Update call should be serialized, and no one else should with this pipeline name
	// should be calling GetOrCreate, it's ok to call GetOrCreate directly
	rep_status, retErr := pipelineMgr.GetOrCreateReplicationStatus(topic, cur_err)
	if rep_status == nil {
		combinedRetErr := errors.New(ReplicationStatusNotFound.Error() + " Cause: " + retErr.Error())
		return combinedRetErr
	} else if retErr != nil {
		return retErr
	}

	updaterObj := rep_status.Updater()
	if updaterObj == nil {
		errorStr := fmt.Sprintf("Failed to get updater from replication status in topic %v in update()", topic)
		return errors.New(errorStr)
	} else {
		updater, ok := updaterObj.(*PipelineUpdater)
		if updater == nil || !ok {
			errorStr := fmt.Sprintf("Unable to cast updaterObj as type PipelineUpdater in topic %v", topic)
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

func (pipelineMgr *PipelineManager) ReInitStreams(pipelineName string) error {
	return pipelineMgr.serializer.ReInit(pipelineName)
}

// Bunch of getters
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

// unit test injection flags
const (
	pipelineUpdaterErrInjNil         int32 = iota
	pipelineUpdaterErrInjOfflineFail int32 = iota
)

// Masks for the disabledFeatures Flag
const (
	disabledCompression uint8 = 1
)

// This context records the last replication setting's revision. If it has changed from the last recorded time,
// it means that the replication setting has been updated and the disabled Features should be discarded
// Since only the updater (which is a singleton per replication) calls is in a single routine, no need for synchronization
type replSettingsRevContext struct {
	rep_status          pipeline.ReplicationStatusIface
	recordedRevision    interface{}
	recordedRevIsActive bool
}

func (h *replSettingsRevContext) Clear() {
	h.recordedRevIsActive = false
	h.recordedRevision = nil
}

func (h *replSettingsRevContext) GetCurrentSettingsRevision() interface{} {
	if h.rep_status != nil {
		spec := h.rep_status.Spec()
		if spec != nil {
			return spec.Settings.Revision
		}
	}
	return nil
}

// Sets the revision copy and also turns on a flag
func (h *replSettingsRevContext) Record() {
	h.recordedRevision = h.GetCurrentSettingsRevision()
	h.recordedRevIsActive = true
}

// Returns true if a recorded revision is active and the replSettings has changed since the last recorded revision
func (h *replSettingsRevContext) HasChanged() bool {
	if h.recordedRevIsActive {
		revToCheck := h.GetCurrentSettingsRevision()
		return !(reflect.DeepEqual(revToCheck, h.recordedRevision))
	}
	return false
}

type DisableCompressionReason int

const (
	DCNotDisabled  DisableCompressionReason = iota
	DCIncompatible DisableCompressionReason = iota
	DCDecompress   DisableCompressionReason = iota
	DCInvalid      DisableCompressionReason = iota
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
	// update-error-map channel
	update_err_map_ch chan base.ErrorMap
	// channel indicating whether updater is really done
	done_ch chan bool
	//the current errors
	currentErrors *pmErrMapType
	// temporary map to catch any other errors while currentErrors are being processed
	// Note - lock ordering not to be violated: 1. overflowErrors 2. currentErrors
	overflowErrors *pmErrMapType

	// Pipeline Manager that created this updater
	pipelineMgr Pipeline_mgr_iface

	// should modify it via scheduleFutureRefresh() and cancelFutureRefresh()
	// Null if nothing is being scheduled
	scheduledTimerLock sync.RWMutex
	scheduledTimer     *time.Timer

	// boolean to store whether or not the last update was successful
	lastSuccessful bool

	rep_status          pipeline.ReplicationStatusIface
	logger              *log.CommonLogger
	pipelineUpdaterLock sync.RWMutex

	// A counter for keeping track how many times this updater has executed with allowable error codes
	runCounter uint64

	// Disabled Features - with a monitor context to go along with it
	disabledFeatures          uint8
	replSpecSettingsHelper    *replSettingsRevContext
	disabledCompressionReason DisableCompressionReason

	// Unit test only - should be in milliseconds - should be only read by unit tests
	testCustomScheduleTime      time.Duration
	testInjectionError          int32
	testCurrentDisabledFeatures uint8

	stopped     bool
	stoppedLock sync.Mutex
}

type pmErrMapType struct {
	errMap    base.ErrorMap
	curErrMtx sync.RWMutex
	logger    *log.CommonLogger
}

func (em *pmErrMapType) IsEmpty() bool {
	em.curErrMtx.RLock()
	defer em.curErrMtx.RUnlock()
	return len(em.errMap) == 0
}

func (em *pmErrMapType) Size() int {
	em.curErrMtx.RLock()
	defer em.curErrMtx.RUnlock()
	return len(em.errMap)
}

func (em *pmErrMapType) Clear() {
	em.curErrMtx.Lock()
	defer em.curErrMtx.Unlock()
	em.errMap = make(base.ErrorMap)
}

func (em *pmErrMapType) String() string {
	em.curErrMtx.RLock()
	defer em.curErrMtx.RUnlock()
	return base.FlattenErrorMap(em.errMap)
}

func (em *pmErrMapType) LoadErrMap(newMap base.ErrorMap) {
	em.curErrMtx.Lock()
	defer em.curErrMtx.Unlock()
	em.errMap = base.DeepCopyErrorMap(newMap)
}

func (em *pmErrMapType) ConcatenateErrors(incomingMap base.ErrorMap) {
	em.curErrMtx.Lock()
	defer em.curErrMtx.Unlock()
	base.ConcatenateErrors(em.errMap, incomingMap, MaxErrorDataEntries, em.logger)
}

// The behavior is to not overwrite keys - since traditionally Pipeline Manager's error handling
// is to register the first type of error and ignore the subsequent onces
func (em *pmErrMapType) AddError(source string, err error) error {
	if len(source) == 0 {
		return errors.New("Empty string")
	}
	em.curErrMtx.Lock()
	defer em.curErrMtx.Unlock()
	if _, ok := em.errMap[source]; !ok {
		if len(em.errMap) < MaxErrorDataEntries {
			em.errMap[source] = err
		} else {
			em.logger.Warnf("Failed to add error to error map since the error map is full. error discarded = %v", err)
		}
	}
	return nil
}

func (em *pmErrMapType) ContainsError(checkErr error, exactMatch bool) bool {
	em.curErrMtx.RLock()
	defer em.curErrMtx.RUnlock()
	return base.CheckErrorMapForError(em.errMap, checkErr, exactMatch)
}

func newPipelineUpdater(pipeline_name string, retry_interval int, cur_err error, rep_status_in pipeline.ReplicationStatusIface, logger *log.CommonLogger, pipelineMgr_in *PipelineManager) *PipelineUpdater {
	if rep_status_in == nil {
		panic("nil ReplicationStatus")
	}
	repairer := &PipelineUpdater{pipeline_name: pipeline_name,
		retry_interval:         time.Duration(retry_interval) * time.Second,
		fin_ch:                 make(chan bool, 1),
		done_ch:                make(chan bool, 1),
		update_now_ch:          make(chan bool, 1),
		update_err_map_ch:      make(chan base.ErrorMap, 1),
		rep_status:             rep_status_in,
		logger:                 logger,
		pipelineMgr:            pipelineMgr_in,
		lastSuccessful:         true,
		currentErrors:          &pmErrMapType{errMap: make(base.ErrorMap), logger: logger},
		overflowErrors:         &pmErrMapType{errMap: make(base.ErrorMap), logger: logger},
		replSpecSettingsHelper: &replSettingsRevContext{rep_status: rep_status_in},
	}

	if cur_err != nil {
		repairer.currentErrors.AddError("newPipelineUpdater", cur_err)
	}
	return repairer
}

func (r *PipelineUpdater) getLastResult() bool {
	r.pipelineUpdaterLock.RLock()
	defer r.pipelineUpdaterLock.RUnlock()
	return r.lastSuccessful
}

//start the repairer
func (r *PipelineUpdater) run() {
	defer close(r.done_ch)

	var retErr error
	var retErrMap base.ErrorMap

	for {
		select {
		case <-r.fin_ch:
			r.logger.Infof("Quit updating pipeline %v after updating a total of %v times\n", r.pipeline_name, atomic.LoadUint64(&r.runCounter))
			r.logger.Infof("Replication %v's status is to be closed, shutting down\n", r.pipeline_name)
			r.pipelineMgr.StopPipeline(r.rep_status)
			// Reset runCounter to 0 for unit test
			atomic.StoreUint64(&r.runCounter, 0)
			return
		case <-r.update_now_ch:
			r.logger.Infof("Replication %v's status is changed, update now\n", r.pipeline_name)
			retErrMap = r.update()
			r.cancelFutureRefresh()
			if len(retErrMap) > 0 {
				r.logger.Infof("Replication %v update experienced error(s): %v. Scheduling a redo.\n", r.pipeline_name, base.FlattenErrorMap(retErrMap))
				r.sendUpdateErrMap(retErrMap)
			}
		case retErrMap = <-r.update_err_map_ch:
			var updateAgain bool
			r.currentErrors.ConcatenateErrors(retErrMap)
			if r.getLastResult() {
				// Last time update succeeded, so this error then triggers an immediate update
				r.logger.Infof("Replication %v's status experienced changes or errors (%v), updating now\n", r.pipeline_name, base.FlattenErrorMap(retErrMap))
				retErrMap = r.update()
				r.cancelFutureRefresh()
				if len(retErrMap) > 0 {
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

func allErrorsAreAllowed(errMap base.ErrorMap) bool {
	for _, v := range errMap {
		if !allowableErrorCodes(v) {
			return false
		}
	}
	return true
}

func allowableErrorCodes(err error) bool {
	if err == nil ||
		err == ReplicationSpecNotActive ||
		err == service_def.MetadataNotFoundErr {
		return true
	}
	return false
}

// If supported, one by one disable the features that are allowed to be disabled so that
// the pipeline can eventually start. These temporary changes are cleared when clearErrors() is called later
func (r *PipelineUpdater) checkAndDisableProblematicFeatures() {
	// First make sure that no one has changed replicationSettings from underneath us
	if r.replSpecSettingsHelper.HasChanged() {
		r.replSpecSettingsHelper.Clear()
	} else if r.currentErrors != nil {
		if r.currentErrors.ContainsError(base.ErrorCompressionNotSupported, true /*exactMatch*/) {
			r.disableCompression(base.ErrorCompressionNotSupported)
		} else if r.currentErrors.ContainsError(base.ErrorCompressionUnableToInflate, false /*exactMatch*/) {
			r.disableCompression(base.ErrorCompressionUnableToInflate)
		}
	}
}

func (r *PipelineUpdater) disableCompression(reason error) {
	if r.rep_status == nil {
		r.logger.Errorf(fmt.Sprintf("Unable to disable compression for pipeline %v - rep_status is nil", r.pipeline_name))
		return
	}

	r.logger.Infof("Temporarily disabling compression for pipeline: %v", r.pipeline_name)
	settings := make(map[string]interface{})
	settings[base.CompressionTypeKey] = (int)(base.CompressionTypeForceUncompress)
	r.rep_status.SetCustomSettings(settings)

	switch reason {
	case base.ErrorCompressionNotSupported:
		r.disabledCompressionReason = DCIncompatible
	case base.ErrorCompressionUnableToInflate:
		r.disabledCompressionReason = DCDecompress
	default:
		r.disabledCompressionReason = DCInvalid
	}

	r.disabledFeatures |= disabledCompression
	// For unit test only
	r.testCurrentDisabledFeatures = r.disabledFeatures
}

func (r *PipelineUpdater) resetDisabledFeatures() {
	if r.disabledFeatures > 0 {
		r.logger.Infof("Restoring compression for pipeline: %v the next time it restarts", r.pipeline_name)
		r.rep_status.ClearTemporaryCustomSettings()
		r.disabledFeatures = 0
		r.disabledCompressionReason = DCNotDisabled
	}
}

//update the pipeline
func (r *PipelineUpdater) update() base.ErrorMap {
	var err error
	var errMap base.ErrorMap
	var checkReplicationActivenessKey string = "r.CheckReplicationActiveness"

	r.pickupOverflowErrors()

	if r.currentErrors.IsEmpty() {
		r.logger.Infof("Try to start/restart Pipeline %v. \n", r.pipeline_name)
	} else {
		r.logger.Infof("Try to fix Pipeline %v. Current error(s)=%v \n", r.pipeline_name, r.currentErrors.String())
		r.checkAndDisableProblematicFeatures()
	}

	// Store the current revision of the actual replicationSpec's setting. If things changed from underneath,
	// before we kick-start another update, we'll know at the start of our next update
	r.replSpecSettingsHelper.Record()

	r.logger.Infof("Try to stop pipeline %v\n", r.pipeline_name)
	errMap = r.pipelineMgr.StopPipeline(r.rep_status)

	// unit test error injection
	if atomic.LoadInt32(&r.testInjectionError) == pipelineUpdaterErrInjOfflineFail {
		r.logger.Infof("Error injected... this should only happen during unit test")
		// make a new map so we don't modify the mock PM's errMap reference above
		injectedErrMap := make(base.ErrorMap)
		injectedErrMap["InjectedError"] = errors.New("Injected Offline failure")
		errMap = injectedErrMap
	}
	// end unit test error injection

	if len(errMap) > 0 {
		// If pipeline fails to stop, everything within it should "commit suicide" eventually
		goto RE
	}

	// For unit test, perhaps sleep to induce some scheduling
	time.Sleep(time.Duration(100) * time.Nanosecond)

	err = r.checkReplicationActiveness()
	if err != nil {
		errMap[checkReplicationActivenessKey] = err
		goto RE
	}

	errMap = r.pipelineMgr.StartPipeline(r.pipeline_name)

RE:
	if len(errMap) == 0 {
		r.logger.Infof("Replication %v has been updated. Back to business\n", r.pipeline_name)
	} else if base.CheckErrorMapForError(errMap, ReplicationSpecNotActive, true /*exactMatch*/) {
		r.logger.Infof("Replication %v has been paused. no need to update\n", r.pipeline_name)
	} else if base.CheckErrorMapForError(errMap, service_def.MetadataNotFoundErr, true /*exactMatch */) {
		r.logger.Infof("Replication %v has been deleted. no need to update\n", r.pipeline_name)
	} else {
		r.logger.Errorf("Failed to update pipeline %v, err=%v\n", r.pipeline_name, base.FlattenErrorMap(errMap))
	}

	if allErrorsAreAllowed(errMap) {
		r.logger.Infof("Pipeline %v has been updated successfully\n", r.pipeline_name)
		r.setLastUpdateSuccess()
		if len(errMap) == 0 {
			r.raiseWarningsIfNeeded()
		}
		if r.rep_status != nil {
			r.rep_status.ClearErrors()
		}
		r.clearErrors()
		errMap = make(base.ErrorMap) // clear errMap
		atomic.AddUint64(&r.runCounter, 1)
		r.resetDisabledFeatures() // also clears custom settings
	} else {
		r.setLastUpdateFailure(errMap)
		r.logger.Errorf("Update of pipeline %v failed with errors=%v\n", r.pipeline_name, base.FlattenErrorMap(errMap))
	}
	r.reportStatus()

	return errMap
}

// pick up overflow errors that were not captured by the channel
func (r *PipelineUpdater) pickupOverflowErrors() {
	r.overflowErrors.curErrMtx.Lock()
	defer r.overflowErrors.curErrMtx.Unlock()
	// If current errors map is too full, then these overflow errors may be missed
	r.currentErrors.ConcatenateErrors(r.overflowErrors.errMap)
	r.overflowErrors.errMap = make(base.ErrorMap)
}

func (r *PipelineUpdater) reportStatus() {
	if r.rep_status != nil {
		r.currentErrors.curErrMtx.RLock()
		defer r.currentErrors.curErrMtx.RUnlock()
		r.rep_status.AddErrorsFromMap(r.currentErrors.errMap)
	}
}

func (r *PipelineUpdater) raiseWarningsIfNeeded() {
	r.raiseXattrWarningIfNeeded()
	r.raiseCompressionWarningIfNeeded()
	r.raiseRemoteClusterRestartReasonsIfNeeded()
}

func (r *PipelineUpdater) raiseRemoteClusterRestartReasonsIfNeeded() {
	if !r.currentErrors.ContainsError(base.ErrorPipelineRestartDueToClusterConfigChange, true /*exactMatch*/) {
		return
	}
	spec := r.rep_status.Spec()
	if spec == nil {
		return
	}
	if spec.Settings.IsCapi() {
		return
	}
	targetClusterRef, err := r.pipelineMgr.GetRemoteClusterSvc().RemoteClusterByUuid(spec.TargetClusterUUID, false)
	if err != nil || targetClusterRef == nil {
		return
	}
	errMsg := fmt.Sprintf("Replication from source bucket '%v' to target bucket '%v' on cluster '%v' has been restarted due to remote-cluster configuration changes\n", spec.SourceBucketName, spec.TargetBucketName, targetClusterRef.Name())
	r.logger.Warnf(errMsg)
	r.pipelineMgr.GetLogSvc().Write(errMsg)
}

// raise warning on UI console when
// 1. replication is not of capi type
// 2. replication is not recovering from error
// 3. target cluster does not support xattr
// 4. current node is the master for vbucket 0 - this is needed to ensure that warning is shown on UI only once, instead of once per source node
func (r *PipelineUpdater) raiseXattrWarningIfNeeded() {
	p := r.rep_status.Pipeline()
	if p == nil {
		r.logger.Warnf("Skipping xattr warning check since pipeline %v has not been started\n", r.pipeline_name)
		return
	}
	spec := r.rep_status.Spec()
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
		errMsg := fmt.Sprintf("Replication from source bucket '%v' to target bucket '%v' on cluster '%v' has been started. Note - Target cluster is older than 5.0.0, hence some of the new feature enhancements such as \"Extended Attributes (XATTR)\" are not supported, which might result in loss of XATTR data. If this is not acceptable, please pause the replication, upgrade cluster '%v' to 5.0.0, and restart replication.", spec.SourceBucketName, spec.TargetBucketName, targetClusterRef.Name(), targetClusterRef.Name())
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

func (r *PipelineUpdater) raiseCompressionWarningIfNeeded() {
	if r.disabledFeatures > 0 && (r.disabledFeatures&disabledCompression > 0) {
		spec := r.rep_status.Spec()
		var errMsg string
		switch r.disabledCompressionReason {
		case DCIncompatible:
			errMsg = fmt.Sprintf("Replication from source bucket '%v' to target bucket '%v' has been started without compression enabled due to errors that occurred during clusters handshaking. This may result in more data bandwidth usage. Please ensure that both the source and target clusters are at or above version %v.%v, and restart the replication.",
				spec.SourceBucketName, spec.TargetBucketName, base.VersionForCompressionSupport[0], base.VersionForCompressionSupport[1])
		case DCDecompress:
			errMsg = fmt.Sprintf("Replication from source bucket '%v' to target bucket '%v' has been started without compression enabled due to errors that occurred while attempting to decompress data for filtering. This may result in more data bandwidth usage. Please check the XDCR error logs for more information.",
				spec.SourceBucketName, spec.TargetBucketName)
		default:
			errMsg = fmt.Sprintf("Replication from source bucket '%v' to target bucket '%v' has been started without compression enabled due to unknown errors. This may result in more data bandwidth usage.",
				spec.SourceBucketName, spec.TargetBucketName)
		}
		r.logger.Warn(errMsg)
		r.pipelineMgr.GetLogSvc().Write(errMsg)
	}
}

func (r *PipelineUpdater) checkReplicationActiveness() (err error) {
	spec, err := r.pipelineMgr.GetReplSpecSvc().ReplicationSpec(r.pipeline_name)
	if err != nil || spec == nil || !spec.Settings.Active {
		err = ReplicationSpecNotActive
	}
	return
}

func (r *PipelineUpdater) stop() {
	err := r.setStopped()
	if err != nil {
		return
	}

	close(r.fin_ch)

	// wait for updater to really stop
	<-r.done_ch
}

func (r *PipelineUpdater) setStopped() error {
	r.stoppedLock.Lock()
	defer r.stoppedLock.Unlock()
	if r.stopped {
		return UpdaterStoppedError
	}
	r.stopped = true
	return nil
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
	oneErrorInMap := make(base.ErrorMap)
	oneErrorInMap["r.update_err_ch"] = err
	r.sendUpdateErrMap(oneErrorInMap)
}

func (r *PipelineUpdater) sendUpdateErrMap(errMap base.ErrorMap) {
	select {
	case r.update_err_map_ch <- errMap:
	default:
		r.overflowErrors.ConcatenateErrors(errMap)
		r.logger.Infof("Updater is currently running. Errors are captured in overflow map, and may be picked up if the current pipeline restart fails")
	}
	r.logger.Infof("Replication status is updated with error(s) %v, current status=%v\n", base.FlattenErrorMap(errMap), r.rep_status)
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

func (r *PipelineUpdater) setLastUpdateFailure(errs base.ErrorMap) {
	r.pipelineUpdaterLock.Lock()
	defer r.pipelineUpdaterLock.Unlock()
	r.lastSuccessful = false
	r.currentErrors.LoadErrMap(errs)
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

func (r *PipelineUpdater) clearErrors() {
	r.pipelineUpdaterLock.Lock()
	defer r.pipelineUpdaterLock.Unlock()

	r.currentErrors.Clear()
	r.replSpecSettingsHelper.Clear()
	r.overflowErrors.Clear()
}

func getRequestPoolSize(rep_status *pipeline.ReplicationStatus, numOfTargetNozzles int) int {
	return rep_status.Spec().Settings.BatchCount * 52 * numOfTargetNozzles
}
