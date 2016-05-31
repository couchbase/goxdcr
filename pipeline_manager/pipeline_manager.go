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
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
	"sync"
	"time"
)

var ReplicationSpecNotActive error = errors.New("Replication specification not found or no longer active")
var ReplicationSpecNotFound error = errors.New("Replication specification not found")

type func_report_fixed func(topic string)

type pipelineManager struct {
	pipeline_factory   common.PipelineFactory
	repl_spec_svc      service_def.ReplicationSpecSvc
	xdcr_topology_svc  service_def.XDCRCompTopologySvc
	remote_cluster_svc service_def.RemoteClusterSvc
	once               sync.Once
	logger             *log.CommonLogger
	child_waitGrp      *sync.WaitGroup
}

var pipeline_mgr pipelineManager

func PipelineManager(factory common.PipelineFactory, repl_spec_svc service_def.ReplicationSpecSvc, xdcr_topology_svc service_def.XDCRCompTopologySvc,
	remote_cluster_svc service_def.RemoteClusterSvc, logger_context *log.LoggerContext) {
	pipeline_mgr.once.Do(func() {
		pipeline_mgr.pipeline_factory = factory
		pipeline_mgr.repl_spec_svc = repl_spec_svc
		pipeline_mgr.xdcr_topology_svc = xdcr_topology_svc
		pipeline_mgr.remote_cluster_svc = remote_cluster_svc
		pipeline_mgr.logger = log.NewLogger("PipelineManager", logger_context)
		pipeline_mgr.logger.Info("Pipeline Manager is constucted")
		pipeline_mgr.child_waitGrp = &sync.WaitGroup{}

		//initialize the expvar storage for replication status
		pipeline.RootStorage()
	})
}

func StartPipeline(topic string) (common.Pipeline, error) {
	p, err := pipeline_mgr.startPipeline(topic)
	return p, err
}

func StopPipeline(topic string) error {
	rep_status, err := ReplicationStatus(topic)
	if err != nil {
		return err
	}
	return pipeline_mgr.stopPipeline(rep_status)
}

func OnExit() error {
	return pipeline_mgr.onExit()
}

func ReplicationStatus(topic string) (*pipeline.ReplicationStatus, error) {
	obj, err := pipeline_mgr.repl_spec_svc.GetDerviedObj(topic)
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, utils.ReplicationStatusNotFoundError(topic)
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

func InitReplicationStatusForReplication(specId string) *pipeline.ReplicationStatus {
	rs := pipeline.NewReplicationStatus(specId, pipeline_mgr.repl_spec_svc.ReplicationSpec, pipeline_mgr.logger)
	pipeline_mgr.repl_spec_svc.SetDerivedObj(specId, rs)
	return rs
}

func Update(topic string, cur_err error) error {
	return pipeline_mgr.update(topic, cur_err)
}

func RemoveReplicationStatus(topic string) error {
	rs, err := ReplicationStatus(topic)
	if err != nil {
		return err
	}

	//ask the updater on this topic if any to stop
	stopUpdater(topic)
	rs.ResetStorage()
	rs.SetUpdater(nil)

	// stop replication
	pipeline_mgr.logger.Infof("Stopping pipeline %v since the replication spec has been deleted\n", topic)
	err = pipeline_mgr.stopPipeline(rs)
	if err != nil {
		pipeline_mgr.logger.Infof("Stopping pipeline %v failed with err = %v\n", topic, err)
	}

	pipeline_mgr.repl_spec_svc.SetDerivedObj(topic, nil)

	return nil
}

func stopUpdater(topic string) {
	rs, err := ReplicationStatus(topic)
	if rs != nil {
		updaterObj := rs.Updater()
		if updaterObj != nil {
			updaterObj.(*pipelineUpdater).stop()
		}
	} else {
		pipeline_mgr.logger.Infof("Skipping stopping updater for %v because of error retrieving replication status. err=%v", topic, err)
	}
}

//This doesn't include the replication status of just deleted replication spec in the map
func ReplicationStatusMap() map[string]*pipeline.ReplicationStatus {
	ret := make(map[string]*pipeline.ReplicationStatus)
	specId_list, err := pipeline_mgr.repl_spec_svc.AllReplicationSpecIds()
	if err == nil {
		for _, specId := range specId_list {
			rep_status, _ := ReplicationStatus(specId)
			if rep_status == nil {
				//create the replication status
				pipeline_mgr.logger.Infof("rep_status for topic %v is nil. Initialize it\n", specId)
				rep_status = InitReplicationStatusForReplication(specId)
			}
			ret[specId] = rep_status
		}
	}
	return ret
}

func LogStatusSummary() {
	rep_status_map := ReplicationStatusMap()
	if len(rep_status_map) > 0 {
		pipeline_mgr.logger.Infof("Replication Status = %v\n", rep_status_map)
	}
}

func AllReplicationsForBucket(bucket string) []string {
	var repIds []string
	for _, key := range pipeline_mgr.topics() {
		// there should not be any errors since topics() should return valid replication ids
		match, _ := metadata.IsReplicationIdForSourceBucket(key, bucket)
		if match {
			repIds = append(repIds, key)
		}
	}
	return repIds
}

func AllReplicationSpecsForTargetCluster(targetClusterUuid string) map[string]*metadata.ReplicationSpecification {
	ret := make(map[string]*metadata.ReplicationSpecification)
	for topic, rep_status := range ReplicationStatusMap() {
		if rep_status.Spec().TargetClusterUUID == targetClusterUuid {
			ret[topic] = rep_status.Spec()
		}
	}

	return ret
}

func AllReplicationsForTargetCluster(targetClusterUuid string) []string {
	ret := make([]string, 0)
	specs := AllReplicationSpecsForTargetCluster(targetClusterUuid)

	for topic, _ := range specs {
		ret = append(ret, topic)
	}

	return ret
}

func AllReplications() []string {
	return pipeline_mgr.topics()
}

func IsPipelineRunning(topic string) bool {
	rep_status, _ := ReplicationStatus(topic)
	if rep_status != nil {
		return (rep_status.RuntimeStatus(true) == pipeline.Replicating)
	} else {
		return false
	}
}

func CheckPipelines() {
	rep_status_map := ReplicationStatusMap()
	for specId, rep_status := range rep_status_map {
		//validate replication spec
		if rep_status.Spec() != nil {
			pipeline_mgr.repl_spec_svc.ValidateAndGC(rep_status.Spec())
		}
		if rep_status.RuntimeStatus(true) == pipeline.Pending {
			if rep_status.Updater() == nil {
				pipeline_mgr.logger.Infof("Pipeline %v is broken, but not yet attended, launch updater", specId)
				pipeline_mgr.launchUpdater(specId, nil, rep_status)
			}
		}
	}
	LogStatusSummary()
}

func RuntimeCtx(topic string) common.PipelineRuntimeContext {
	return pipeline_mgr.runtimeCtx(topic)
}

func (pipelineMgr *pipelineManager) onExit() error {
	// stop running pipelines
	for _, topic := range pipelineMgr.liveTopics() {
		StopPipeline(topic)
	}

	//send finish signal to all updater
	pipelineMgr.stopAllUpdaters()

	pipelineMgr.logger.Infof("Sent finish signal to all running repairer")
	pipelineMgr.child_waitGrp.Wait()

	return nil

}

func (pipelineMgr *pipelineManager) stopAllUpdaters() {
	rep_status_map := ReplicationStatusMap()
	for _, rep_status := range rep_status_map {
		updater := rep_status.Updater()
		if updater != nil {
			updater.(*pipelineUpdater).stop()
		}
	}
}

func (pipelineMgr *pipelineManager) startPipeline(topic string) (common.Pipeline, error) {
	var err error
	pipelineMgr.logger.Infof("Starting the pipeline %s\n", topic)

	rep_status, _ := ReplicationStatus(topic)
	if rep_status == nil || (rep_status != nil && rep_status.RuntimeStatus(true) != pipeline.Replicating) {
		// validate the pipeline before starting it
		err = pipelineMgr.validatePipeline(topic)
		if err != nil {
			return nil, err
		}

		if rep_status == nil {
			rep_status = pipeline.NewReplicationStatus(topic, pipelineMgr.repl_spec_svc.ReplicationSpec, pipelineMgr.logger)
			pipelineMgr.repl_spec_svc.SetDerivedObj(topic, rep_status)
		}

		rep_status.RecordProgress("Start pipeline construction")

		p, err := pipelineMgr.pipeline_factory.NewPipeline(topic, rep_status.RecordProgress)
		if err != nil {
			pipelineMgr.logger.Errorf("Failed to construct a new pipeline with topic %v: %s", topic, err.Error())
			return p, err
		}

		rep_status.RecordProgress("Pipeline is constructed")
		rep_status.SetPipeline(p)

		pipelineMgr.logger.Infof("Pipeline %v is constructed. Starting it.", p.InstanceId())
		p.SetProgressRecorder(rep_status.RecordProgress)
		err = p.Start(rep_status.SettingsMap())
		if err != nil {
			pipelineMgr.logger.Error("Failed to start the pipeline")
			return p, err
		}

		return p, nil
	} else {
		//the pipeline is already running
		pipelineMgr.logger.Infof("The pipeline asked to be started, %v, is already running", topic)
		return rep_status.Pipeline(), err
	}
	return nil, err
}

// validate that a pipeline has valid configuration and can be started before starting it
func (pipelineMgr *pipelineManager) validatePipeline(topic string) error {
	pipelineMgr.logger.Infof("Validating pipeline %v\n", topic)

	spec, err := pipelineMgr.repl_spec_svc.ReplicationSpec(topic)
	if err != nil {
		pipelineMgr.logger.Errorf("Failed to get replication specification for pipeline %v, err=%v\n", topic, err)
		return err
	}

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

func (pipelineMgr *pipelineManager) getPipelineFromMap(topic string) common.Pipeline {
	rep_status, _ := ReplicationStatus(topic)
	if rep_status != nil {
		return rep_status.Pipeline()
	}
	return nil
}

func (pipelineMgr *pipelineManager) removePipelineFromReplicationStatus(p common.Pipeline) error {
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

func (pipelineMgr *pipelineManager) stopPipeline(rep_status *pipeline.ReplicationStatus) error {
	if rep_status == nil {
		return fmt.Errorf("Invalid parameter value rep_status=nil")
	}

	pipelineMgr.logger.Infof("Trying to stop the pipeline %s", rep_status.RepId())
	var err error

	p := rep_status.Pipeline()

	if p != nil {
		state := p.State()
		if state == common.Pipeline_Running || state == common.Pipeline_Starting || state == common.Pipeline_Error {
			err = p.Stop()
			if err != nil {
				pipelineMgr.logger.Errorf("Received error when stopping pipeline %v - %v\n", rep_status.RepId(), err)
				//pipeline failed to stopped gracefully in time. ignore the error.
				//the parts of the pipeline will eventually commit suicide.
			} else {
				pipelineMgr.logger.Infof("Pipeline %v has been stopped\n", rep_status.RepId())
			}
			pipelineMgr.removePipelineFromReplicationStatus(p)
			pipelineMgr.logger.Infof("Replication Status=%v\n", rep_status)
		} else {
			pipelineMgr.logger.Infof("Pipeline %v is not in the right state to be stopped. state=%v\n", rep_status.RepId(), state)
		}
	} else {
		pipelineMgr.logger.Infof("Pipeline %v is not running\n", rep_status.RepId())
	}
	return err
}

func (pipelineMgr *pipelineManager) runtimeCtx(topic string) common.PipelineRuntimeContext {
	pipeline := pipelineMgr.pipeline(topic)
	if pipeline != nil {
		return pipeline.RuntimeContext()
	}

	return nil
}

func (pipelineMgr *pipelineManager) pipeline(topic string) common.Pipeline {
	pipeline := pipelineMgr.getPipelineFromMap(topic)
	return pipeline
}

func (pipelineMgr *pipelineManager) liveTopics() []string {
	rep_status_map := ReplicationStatusMap()
	topics := make([]string, 0, len(rep_status_map))
	for topic, rep_status := range rep_status_map {
		if rep_status.RuntimeStatus(true) == pipeline.Replicating {
			topics = append(topics, topic)
		}
	}
	return topics
}

func (pipelineMgr *pipelineManager) topics() []string {
	rep_status_map := ReplicationStatusMap()
	topics := make([]string, 0, len(rep_status_map))
	for topic, _ := range rep_status_map {
		topics = append(topics, topic)
	}

	return topics
}

func (pipelineMgr *pipelineManager) livePipelines() map[string]common.Pipeline {
	ret := make(map[string]common.Pipeline)
	rep_status_map := ReplicationStatusMap()
	for topic, rep_status := range rep_status_map {
		if rep_status.RuntimeStatus(true) == pipeline.Replicating {
			ret[topic] = rep_status.Pipeline()
		}
	}

	return ret
}

func (pipelineMgr *pipelineManager) reportFixed(topic string, r *pipelineUpdater) error {
	rep_status, _ := ReplicationStatus(topic)
	if rep_status != nil {
		err := r.updateState(Updater_Done)
		if err != nil {
			return err
		}
		rep_status.SetUpdater(nil)
	} else {
		pipelineMgr.logger.Infof("reportFixed skipped since replication status for %v no longer exists", topic)
	}

	return nil
}

func (pipelineMgr *pipelineManager) launchUpdater(topic string, cur_err error, rep_status *pipeline.ReplicationStatus) error {
	isKV, err := pipelineMgr.xdcr_topology_svc.IsKVNode()
	if err == nil && !isKV {
		pipelineMgr.logger.Infof("This node is not a KV node, would not act on replication spec %s's update\n", topic)
		return nil
	}
	settingsMap := rep_status.SettingsMap()
	retry_interval_obj := settingsMap[metadata.FailureRestartInterval]
	var retry_interval int = 0
	if retry_interval_obj != nil {
		retry_interval = settingsMap[metadata.FailureRestartInterval].(int)
	}

	updater, err := newPipelineUpdater(topic, retry_interval, pipelineMgr.child_waitGrp, cur_err, rep_status, pipelineMgr.logger)
	if err != nil {
		pipelineMgr.logger.Error(err.Error())
		return err
	}

	//SetUpdater could fail if another go routine has already started an updater. do not run updater in this case
	err = rep_status.SetUpdater(updater)
	if err != nil {
		pipelineMgr.logger.Error(err.Error())
		return err
	}

	pipelineMgr.child_waitGrp.Add(1)
	go updater.start()
	pipelineMgr.logger.Infof("Pipeline updater %v is lauched with retry_interval=%v\n", topic, retry_interval)
	return nil
}

func (pipelineMgr *pipelineManager) update(topic string, cur_err error) error {
	rep_status, _ := ReplicationStatus(topic)
	if rep_status == nil {
		rep_status = pipeline.NewReplicationStatus(topic, pipelineMgr.repl_spec_svc.ReplicationSpec, pipelineMgr.logger)
		pipelineMgr.repl_spec_svc.SetDerivedObj(topic, rep_status)
		pipelineMgr.logger.Infof("ReplicationStatus is created and set with %v\n", topic)
	}
	updaterObj := rep_status.Updater()
	if updaterObj == nil {
		return pipelineMgr.launchUpdater(topic, cur_err, rep_status)
	} else {
		updater := updaterObj.(*pipelineUpdater)
		if cur_err == nil {
			//update is not initiated by error, update the replication status
			//trigger updater to update immediately, not to wait for the retry interval
			updater.refreshReplicationStatus(rep_status, true)
		} else {
			rep_status.AddError(cur_err)
			updater.refreshReplicationStatus(rep_status, false)
		}
		pipelineMgr.logger.Infof("There is already an updater launched for the replication %v, no-op", topic)
	}
	return nil

}

type pipelineUpdaterState int

const (
	Updater_Initialized = iota
	Updater_Running     = iota
	Updater_Done        = iota
)

var updaterStateErrorStr = "Can't move update state from %v to %v"

//pipelineRepairer is responsible to repair a failing pipeline
//it will retry after the retry_interval
type pipelineUpdater struct {
	//the name of the pipeline to be repaired
	pipeline_name string
	//the interval to wait after the failure for next retry
	retry_interval time.Duration
	//the number of retries
	num_of_retries uint64
	//finish channel
	fin_ch chan bool
	//update-now channel
	update_now_ch chan bool
	// channel indicating whether updater is really done
	done_ch chan bool
	//the current error
	current_error error

	// passed in from and managed by pipeline_manager
	waitGrp *sync.WaitGroup

	rep_status *pipeline.ReplicationStatus
	logger     *log.CommonLogger
	state_lock sync.RWMutex
	state      pipelineUpdaterState
}

func newPipelineUpdater(pipeline_name string, retry_interval int, waitGrp *sync.WaitGroup, cur_err error, rep_status *pipeline.ReplicationStatus, logger *log.CommonLogger) (*pipelineUpdater, error) {
	if retry_interval < 0 {
		return nil, fmt.Errorf("Invalid retry interval %v", retry_interval)
	}

	if rep_status == nil {
		panic("nil ReplicationStatus")
	}
	repairer := &pipelineUpdater{pipeline_name: pipeline_name,
		retry_interval: time.Duration(retry_interval) * time.Second,
		num_of_retries: 0,
		fin_ch:         make(chan bool, 1),
		done_ch:        make(chan bool, 1),
		waitGrp:        waitGrp,
		rep_status:     rep_status,
		logger:         logger,
		state:          Updater_Initialized,
		current_error:  cur_err}

	return repairer, nil
}

//start the repairer
func (r *pipelineUpdater) start() {
	defer r.waitGrp.Done()
	defer close(r.done_ch)

	if r.current_error == nil {
		//the update is not initiated from a failure case, so don't wait, update now
		if r.update() {
			return
		}
	} else {
		r.reportStatus()
	}

	ticker := time.NewTicker(r.retry_interval)
	defer ticker.Stop()
	for {
		r.updateState(Updater_Running)
		select {
		case <-r.fin_ch:
			r.logger.Infof("Quit updating pipeline %v\n", r.pipeline_name)
			return
		case <-r.update_now_ch:
			r.logger.Infof("Replication %v's status is changed, update now\n", r.pipeline_name)
			if r.update() {
				return
			} else {
				r.num_of_retries++
			}
		case <-ticker.C:
			if r.update() {
				return
			} else {
				r.num_of_retries++
			}
		}
	}
}

//update the pipeline
func (r *pipelineUpdater) update() bool {
	r.logger.Infof("Try to fix Pipeline %v. Current error=%v \n", r.pipeline_name, r.current_error)

	err := r.updateState(Updater_Running)
	if err != nil {
		// the only scenario where err can be returned is when updater is already in "Done" state
		r.logger.Infof("Skipping update since updater is already done")
		return true
	}

	r.logger.Infof("Try to stop pipeline %v\n", r.pipeline_name)
	err = pipeline_mgr.stopPipeline(r.rep_status)
	if err != nil {
		goto RE
	}

	err = r.checkReplicationActiveness()
	if err != nil {
		goto RE
	}

	_, err = pipeline_mgr.startPipeline(r.pipeline_name)
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

	if err == nil || err == ReplicationSpecNotActive || err == service_def.MetadataNotFoundErr {
		r.logger.Infof("Pipeline %v has been updated successfully\n", r.pipeline_name)
		if err1 := pipeline_mgr.reportFixed(r.pipeline_name, r); err1 == nil {
			r.rep_status.ClearErrors()
			r.current_error = nil
			return true
		} else {
			r.logger.Errorf("Update of pipeline %v failed with error=%v\n", r.pipeline_name, err1)
			r.current_error = err1
		}
	} else {
		r.logger.Errorf("Update of pipeline %v failed with error=%v\n", r.pipeline_name, err)
		r.current_error = err
	}
	r.reportStatus()

	return false
}

func (r *pipelineUpdater) reportStatus() {
	r.rep_status.AddError(r.current_error)
}

func (r *pipelineUpdater) checkReplicationActiveness() (err error) {
	spec, err := pipeline_mgr.repl_spec_svc.ReplicationSpec(r.pipeline_name)
	if err != nil || spec == nil || !spec.Settings.Active {
		err = ReplicationSpecNotActive
	} else {
		r.logger.Debugf("Pipeline %v is not paused or deleted\n", r.pipeline_name)
	}
	return
}

//It should be called only once.
func (r *pipelineUpdater) stop() {
	defer func() {
		if e := recover(); e != nil {
			r.logger.Infof("Updater for pipeline is already stopped\n", r.pipeline_name)
		}
	}()

	close(r.fin_ch)

	// wait for updater to really stop
	<-r.done_ch
}

func (r *pipelineUpdater) refreshReplicationStatus(rep_status *pipeline.ReplicationStatus, updateNow bool) {
	r.state_lock.Lock()
	defer r.state_lock.Unlock()

	r.rep_status = rep_status
	r.state = Updater_Initialized
	if updateNow {
		select {
		case r.update_now_ch <- true:
		default:
			r.logger.Infof("Update-now message is already delivered for %v", r.pipeline_name)
		}
	}
	r.logger.Infof("Replication status is updated, current status=%v\n", rep_status)
}

func (r *pipelineUpdater) updateState(new_state pipelineUpdaterState) error {
	r.state_lock.Lock()
	defer r.state_lock.Unlock()

	switch r.state {
	case Updater_Initialized:
		if new_state != Updater_Running {
			r.logger.Infof("Updater %v can't move to %v from %v\n", r.pipeline_name, new_state, r.state)
			return fmt.Errorf(updaterStateErrorStr, Updater_Initialized, new_state)
		}
	case Updater_Done:
		r.logger.Infof("Updater %v can't move to %v from %v\n", r.pipeline_name, new_state, r.state)
		return fmt.Errorf(updaterStateErrorStr, Updater_Done, new_state)

	}

	r.logger.Infof("Updater %v moved to %v from %v\n", r.pipeline_name, new_state, r.state)
	r.state = new_state

	return nil
}

func getRequestPoolSize(rep_status *pipeline.ReplicationStatus, numOfTargetNozzles int) int {
	return rep_status.Spec().Settings.BatchCount * 52 * numOfTargetNozzles
}
