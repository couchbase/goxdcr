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
	common "github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/service_def"
	"sync"
	"time"
)

var ReplicationSpecNotActive error = errors.New("Replication specification not found or no longer active")

type func_report_fixed func(topic string)

type pipelineManager struct {
	pipeline_factory common.PipelineFactory
	repl_spec_svc    service_def.ReplicationSpecSvc
	pipelines_map    map[string]*pipeline.ReplicationStatus

	once sync.Once

	mapLock sync.RWMutex
	logger  *log.CommonLogger

	//lock to pipeline_pending_for_repair map
	repair_map_lock *sync.RWMutex
	//keep track of the pipeline in repair
	pipeline_pending_for_update map[string]*pipelineUpdater
	child_waitGrp               *sync.WaitGroup
}

var pipeline_mgr pipelineManager

func PipelineManager(factory common.PipelineFactory, repl_spec_svc service_def.ReplicationSpecSvc, logger_context *log.LoggerContext) {
	pipeline_mgr.once.Do(func() {
		pipeline_mgr.pipeline_factory = factory
		pipeline_mgr.repl_spec_svc = repl_spec_svc
		pipeline_mgr.pipelines_map = make(map[string]*pipeline.ReplicationStatus)
		pipeline_mgr.logger = log.NewLogger("PipelineManager", logger_context)
		pipeline_mgr.logger.Info("Pipeline Manager is constucted")
		pipeline_mgr.child_waitGrp = &sync.WaitGroup{}
		pipeline_mgr.pipeline_pending_for_update = make(map[string]*pipelineUpdater)
		pipeline_mgr.repair_map_lock = &sync.RWMutex{}
	})
}

func StartPipeline(topic string, settings map[string]interface{}) (common.Pipeline, error) {
	p, err := pipeline_mgr.startPipeline(topic, settings)
	return p, err
}

func StopPipeline(topic string) error {
	return pipeline_mgr.stopPipeline(topic)
}

func Pipeline(topic string) common.Pipeline {
	return pipeline_mgr.pipeline(topic)
}

func OnExit() error {
	return pipeline_mgr.onExit()
}

func Update(topic string, cur_err error, settings map[string]interface{}) error {
	return pipeline_mgr.update(topic, cur_err, settings)
}

func ReplicationStatus(topic string) *pipeline.ReplicationStatus {
	return pipeline_mgr.pipelines_map[topic]
}

func RemoveReplicationStatus(topic string) error {
	pipeline_mgr.mapLock.Lock()
	defer pipeline_mgr.mapLock.Unlock()

	delete(pipeline_mgr.pipelines_map, topic)

	pipeline.ResetRootStorage()
	for _, rs := range pipeline_mgr.pipelines_map {
		rs.Publish()
	}
	return nil
}

func LogStatusSummary() {
	pipeline_mgr.logger.Infof("Replication Status = %v\n", pipeline_mgr.pipelines_map)
}

func ActiveReplications(bucket string) []string {
	var repIds []string
	for _, key := range pipeline_mgr.liveTopics() {
		if metadata.IsReplicationIdForSourceBucket(key, bucket) {
			repIds = append(repIds, key)
		}
	}
	return repIds
}

func AllActiveReplications() []string {
	return pipeline_mgr.liveTopics()
}

func IsPipelineRunning(topic string) bool {
	if ReplicationStatus(topic) != nil {
		return (ReplicationStatus(topic).RuntimeStatus() == pipeline.Replicating)
	} else {
		return false
	}
}
func RuntimeCtx(topic string) common.PipelineRuntimeContext {
	return pipeline_mgr.runtimeCtx(topic)
}

func (pipelineMgr *pipelineManager) onExit() error {
	// stop running pipelines
	for _, topic := range pipelineMgr.liveTopics() {
		pipelineMgr.stopPipeline(topic)
	}

	//send finish signal to all repairer
	for _, repairer := range pipelineMgr.pipeline_pending_for_update {
		close(repairer.fin_ch)
	}

	pipelineMgr.logger.Infof("Sent finish signal to all running repairer")
	pipelineMgr.child_waitGrp.Wait()

	return nil

}

func (pipelineMgr *pipelineManager) startPipeline(topic string, settings map[string]interface{}) (common.Pipeline, error) {
	var err error
	pipelineMgr.logger.Infof("Starting the pipeline %s with settings = %v\n", topic, settings)

	if rep_status, ok := pipelineMgr.pipelines_map[topic]; !ok || rep_status.RuntimeStatus() != pipeline.Replicating {

		err := pipelineMgr.updateReplicationStatus(topic, settings)
		if err != nil {
			return nil, err
		}

		rep_status.RecordProgress("Start pipeline construction")
		p, err := pipelineMgr.pipeline_factory.NewPipeline(topic, rep_status.RecordProgress)
		if err != nil {
			pipelineMgr.logger.Errorf("Failed to construct a new pipeline: %s", err.Error())
			return p, err
		}

		rep_status.RecordProgress("Pipeline is constructed")
		err = pipelineMgr.addPipelineToReplicationStatus(p)
		if err != nil {
			return p, err
		}

		pipelineMgr.logger.Infof("Pipeline %v is constructed, start it", p.InstanceId())
		err = p.Start(settings)
		if err != nil {
			pipelineMgr.logger.Error("Failed to start the pipeline")
			return p, err
		}

		return p, nil
	} else {
		//the pipeline is already running
		pipelineMgr.logger.Info("The pipeline asked to be started is already running")
		return rep_status.Pipeline(), err
	}
	return nil, err
}

func (pipelineMgr *pipelineManager) updateReplicationStatus(topic string, settings map[string]interface{}) error {
	pipelineMgr.mapLock.Lock()
	defer pipelineMgr.mapLock.Unlock()

	replication_spec, err := pipelineMgr.repl_spec_svc.ReplicationSpec(topic)
	if err != nil {
		return err
	}

	rep_status, ok := pipelineMgr.pipelines_map[topic]
	if ok {
		rep_status.SetSpec(replication_spec)
		rep_status.PutSettings(settings)
	} else {
		pipelineMgr.pipelines_map[topic] = pipeline.NewReplicationStatus(replication_spec, settings, pipelineMgr.logger)
	}

	return nil
}

func (pipelineMgr *pipelineManager) addPipelineToReplicationStatus(p common.Pipeline) error {
	pipelineMgr.mapLock.Lock()
	defer pipelineMgr.mapLock.Unlock()

	rep_status, ok := pipelineMgr.pipelines_map[p.Topic()]
	if ok {
		rep_status.SetPipeline(p)
		p.SetProgressRecorder(rep_status.RecordProgress)
		pipelineMgr.logger.Infof("addPipelineToMap. pipelines=%v\n", pipelineMgr.pipelines_map)
	} else {
		return fmt.Errorf("replication %v hasn't been registered with PipelineManager yet", p.Topic())
	}
	return nil
}

func (pipelineMgr *pipelineManager) getPipelineFromMap(topic string) common.Pipeline {
	pipelineMgr.mapLock.RLock()
	defer pipelineMgr.mapLock.RUnlock()

	rep_status, ok := pipelineMgr.pipelines_map[topic]
	if ok {
		return rep_status.Pipeline()
	}
	return nil
}

func (pipelineMgr *pipelineManager) removePipelineFromReplicationStatus(p common.Pipeline) error {
	pipelineMgr.mapLock.Lock()
	defer pipelineMgr.mapLock.Unlock()

	rep_status, ok := pipelineMgr.pipelines_map[p.Topic()]
	if ok {
		rep_status.SetPipeline(nil)
		pipelineMgr.logger.Infof("addPipelineToMap. pipelines=%v\n", pipelineMgr.pipelines_map)
	} else {
		return fmt.Errorf("replication %v hasn't been registered with PipelineManager yet", p.Topic())
	}
	return nil
}

func (pipelineMgr *pipelineManager) stopPipeline(topic string) error {
	pipelineMgr.logger.Infof("Try to stop the pipeline %s", topic)
	var err error
	if rep_status, ok := pipelineMgr.pipelines_map[topic]; ok && rep_status.RuntimeStatus() == pipeline.Replicating {
		p := rep_status.Pipeline()
		err = p.Stop()
		if err != nil {
			pipelineMgr.logger.Errorf("Failed to stop pipeline %v - %v\n", topic, err)
			//pipeline failed to stopped gracefully in time. ignore the error.
			//the parts tof the pipeline will eventually commit suicide.
		}
		pipelineMgr.removePipelineFromReplicationStatus(p)
		pipelineMgr.updateReplicationStatus(topic, nil)

		pipelineMgr.logger.Infof("Pipeline is stopped")
	} else {
		//The named pipeline is not active
		pipelineMgr.logger.Infof("The pipeline asked to be stopped is not running.")
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
	topics := make([]string, 0, len(pipelineMgr.pipelines_map))
	for k, _ := range pipelineMgr.pipelines_map {
		topics = append(topics, k)
	}
	return topics
}

func (pipelineMgr *pipelineManager) livePipelines() map[string]common.Pipeline {
	ret := make(map[string]common.Pipeline)
	for topic, rep_status := range pipelineMgr.pipelines_map {
		if rep_status.RuntimeStatus() == pipeline.Replicating {
			ret[topic] = rep_status.Pipeline()
		}
	}

	return ret
}

func (pipelineMgr *pipelineManager) reportFixed(topic string) {
	pipelineMgr.repair_map_lock.Lock()
	defer pipelineMgr.repair_map_lock.Unlock()
	delete(pipelineMgr.pipeline_pending_for_update, topic)
}

func (pipelineMgr *pipelineManager) update(topic string, cur_err error, settings map[string]interface{}) error {
	pipelineMgr.repair_map_lock.Lock()
	defer pipelineMgr.repair_map_lock.Unlock()

	if _, ok := pipelineMgr.pipeline_pending_for_update[topic]; !ok {

		var settingsMap map[string]interface{} = settings
		if settingsMap == nil {
			spec, err := pipelineMgr.repl_spec_svc.ReplicationSpec(topic)
			if err != nil {
				return err
			}

			s := spec.Settings
			settingsMap = s.ToMap()
		}

		err := pipelineMgr.updateReplicationStatus(topic, settingsMap)
		if err != nil {
			return err
		}
		rep_status := pipelineMgr.pipelines_map[topic]

		retry_interval := settingsMap[metadata.FailureRestartInterval].(int)

		repairer, err := newPipelineUpdater(topic, retry_interval, pipelineMgr.child_waitGrp, cur_err, rep_status, pipelineMgr.logger)
		if err != nil {
			return err
		}
		pipelineMgr.child_waitGrp.Add(1)
		pipelineMgr.pipeline_pending_for_update[topic] = repairer
		go repairer.start()
		pipelineMgr.logger.Infof("Repairer to fix pipeline %v is lauched with retry_interval=%v\n", topic, retry_interval)
	} else {
		pipelineMgr.logger.Infof("There is already an repairer launched for the replication, no-op")
	}
	return nil
}

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
	//the current error
	current_error error

	waitGrp *sync.WaitGroup

	rep_status *pipeline.ReplicationStatus
	logger     *log.CommonLogger
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
		waitGrp:        waitGrp,
		rep_status:     rep_status,
		logger:         logger,
		current_error:  cur_err}

	return repairer, nil
}

//start the repairer
func (r *pipelineUpdater) start() {
	defer r.waitGrp.Done()

	if r.current_error == nil {
		//the update is not initiated from a failure case, so don't wait, update now
		if r.update() {
			return
		}
	} else {
		r.reportStatus()
	}

	ticker := time.NewTicker(r.retry_interval)
	for {
		select {
		case <-ticker.C:
			if r.update() {
				return
			} else {
				r.num_of_retries++
			}
		case <-r.fin_ch:
			r.logger.Infof("Quit updating pipeline %v\n", r.pipeline_name)
			return
		}
	}
}

//update the pipeline
func (r *pipelineUpdater) update() bool {
	r.logger.Infof("Try to fix Pipeline %v \n", r.pipeline_name)

	r.logger.Infof("Try to stop pipeline %v\n", r.pipeline_name)
	err := pipeline_mgr.stopPipeline(r.pipeline_name)
	if err != nil {
		goto RE
	}

	err = r.checkReplicationActiveness()
	if err != nil {
		goto RE
	}

	_, err = pipeline_mgr.startPipeline(r.pipeline_name, r.rep_status.Settings())
RE:
	if err == nil {
		r.logger.Infof("Replication %v is updated, back to business\n", r.pipeline_name)
	} else {
		r.logger.Errorf("Failed to update pipeline %v, err=%v\n", r.pipeline_name, err)
	}

	r.current_error = err

	if r.current_error == nil || r.current_error == ReplicationSpecNotActive {
		r.logger.Infof("Pipeline %v is updated\n", r.pipeline_name)
		pipeline_mgr.reportFixed(r.pipeline_name)
		return true
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
	}
	r.logger.Debugf("Pipeline %v is not paused or deleted\n", r.pipeline_name)
	return
}
