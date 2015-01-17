// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// replication manager.

package replication_manager

import (
	"bufio"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/cbauth"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/factory"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/pipeline_manager"
	"github.com/couchbase/goxdcr/pipeline_svc"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/supervisor"
	"github.com/couchbase/goxdcr/utils"
	"github.com/couchbaselabs/go-couchbase"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"
)

var logger_rm *log.CommonLogger = log.NewLogger("ReplicationManager", log.DefaultLoggerContext)
var ReplicationSpecNotActive error = errors.New("Replication specification not found or no longer active")

/************************************
/* struct ReplicationManager
*************************************/
type replicationManager struct {
	// supervises the livesness of adminport and pipelineMasterSupervisor
	supervisor.GenericSupervisor
	// supervises the liveness of all pipeline supervisors
	pipelineMasterSupervisor *supervisor.GenericSupervisor

	//replication specification service handle
	repl_spec_svc service_def.ReplicationSpecSvc
	//remote cluster service handle
	remote_cluster_svc service_def.RemoteClusterSvc
	//cluster info service handle
	cluster_info_svc service_def.ClusterInfoSvc
	//xdcr topology service handle
	xdcr_topology_svc service_def.XDCRCompTopologySvc
	//replication settings service handle
	replication_settings_svc service_def.ReplicationSettingsSvc
	//checkpoint service handle
	checkpoint_svc service_def.CheckpointsService
	//capi service handle
	capi_svc service_def.CAPIService
	once     sync.Once

	//finish channel for adminport
	adminport_finch chan bool

	//lock to pipeline_pending_for_repair map
	repair_map_lock *sync.RWMutex
	//keep track of the pipeline in repair
	pipeline_pending_for_repair map[string]*pipelineRepairer

	repl_spec_callback_cancel_ch chan struct{}

	children_waitgrp *sync.WaitGroup
}

//singleton
var replication_mgr replicationManager

//pipelineRepairer is responsible to repair a failing pipeline
//it will retry after the retry_interval
type pipelineRepairer struct {
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

	expvar_map *expvar.Map

	waitGrp *sync.WaitGroup
}

func newPipelineRepairer(pipeline_name string, retry_interval int, waitGrp *sync.WaitGroup, cur_err error) (*pipelineRepairer, error) {
	if retry_interval < 0 {
		return nil, errors.New(fmt.Sprintf("Invalid retry interval %v", retry_interval))
	}

	repairer := &pipelineRepairer{pipeline_name: pipeline_name,
		retry_interval: time.Duration(retry_interval) * time.Second,
		num_of_retries: 0,
		fin_ch:         make(chan bool, 1),
		waitGrp:        waitGrp,
		current_error:  cur_err}

	repairer.expvar_map = getOrInitExpvarMap(pipeline_name)
	return repairer, nil
}

func getOrInitExpvarMap(pipeline_name string) *expvar.Map {
	var ret_map *expvar.Map
	expvar_map := expvar.Get(pipeline_name)
	if expvar_map == nil {
		ret_map = expvar.NewMap(pipeline_name)
	} else {
		ret_map = expvar_map.(*expvar.Map)
	}

	statusVar := new(expvar.String)
	statusVar.Set(base.Pending)
	ret_map.Set("Status", statusVar)

	errArr := ret_map.Get(base.ErrorsStatsKey)
	if errArr == nil {
		errArr := pipelineErrorArray{}
		ret_map.Set(base.ErrorsStatsKey, errArr)
	}

	return ret_map
}

//start the repairer
func (r *pipelineRepairer) start() {
	defer r.waitGrp.Done()

	r.reportStatus()

	ticker := time.NewTicker(r.retry_interval)
	for {
		select {
		case <-ticker.C:
			r.current_error = r.repair()
			if r.current_error == nil {
				logger_rm.Infof("Pipeline %v is fixed\n", r.pipeline_name)
				replication_mgr.reportFixed(r.pipeline_name)
				return
			} else if r.current_error == ReplicationSpecNotActive {
				logger_rm.Infof("Stop repairing - replication %v is no longer active\n", r.pipeline_name)
				return
			} else {
				logger_rm.Errorf("Reparing pipeline failed. error=%v\n", r.current_error)
				r.num_of_retries++
			}

			r.reportStatus()
		case <-r.fin_ch:
			logger_rm.Infof("Quit repairing pipeline %v\n", r.pipeline_name)
			return

		}
	}
}

//repair the pipeline
func (r *pipelineRepairer) repair() (err error) {
	err = nil
	logger_rm.Infof("Try to fix Pipeline %v \n", r.pipeline_name)
	if checkPipelineOnFile(r.pipeline_name) {
		logger_rm.Infof("Try to stop pipeline %v\n", r.pipeline_name)
		err = stopPipeline(r.pipeline_name)
	}
	if err != nil {
		goto RE
	}

	err = r.checkPipelineActiveness()
	if err != nil {
		goto RE
	}

	err = startPipeline(r.pipeline_name)
RE:
	if err == nil {
		logger_rm.Infof("Replication %v is fixed, back to business\n", r.pipeline_name)
	} else {
		logger_rm.Errorf("Failed to fix pipeline %v, err=%v\n", r.pipeline_name, err)
		//TODO: propagate the error to ns_server

	}
	return

}

func (r *pipelineRepairer) reportStatus() {
	//update the error array
	existing_errArr := r.expvar_map.Get(base.ErrorsStatsKey).(pipelineErrorArray)
	new_errArr := pipelineErrorArray{}

	new_errArr[0] = r.current_error.Error()
	for i := 1; i < 10; i++ {
		new_errArr[i] = existing_errArr[i-1]
	}

	logger_rm.Infof("new_errArr=%v, r.current_error=%v\n", new_errArr, r.current_error)
	r.expvar_map.Set(base.ErrorsStatsKey, new_errArr)
}

type pipelineErrorArray [10]string

func (errArray pipelineErrorArray) String() string {
	bytes, err := json.Marshal(errArray)
	if err != nil {
		return ""
	}

	return string(bytes)
}

func (r *pipelineRepairer) checkPipelineActiveness() (err error) {
	spec, err := ReplicationSpecService().ReplicationSpec(r.pipeline_name)
	if err != nil || spec == nil || !spec.Settings.Active {
		err = ReplicationSpecNotActive
	}
	logger_rm.Debugf("Pipeline %v is not paused or deleted\n", r.pipeline_name)
	return
}
func StartReplicationManagerForConversion(
	repl_spec_svc service_def.ReplicationSpecSvc,
	remote_cluster_svc service_def.RemoteClusterSvc) {
	// TODO implement
	/*
		replication_mgr.once.Do(func() {
			// initializes replication manager
			replication_mgr.init(sourceKVHost, sourceKVPort, isEnterprise, repl_spec_svc, remote_cluster_svc, nil, nil, nil)
		})*/
}

func StartReplicationManager(sourceKVHost string, xdcrRestPort uint16,
	repl_spec_svc service_def.ReplicationSpecSvc,
	remote_cluster_svc service_def.RemoteClusterSvc,
	cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	replication_settings_svc service_def.ReplicationSettingsSvc,
	checkpoints_svc service_def.CheckpointsService,
	capi_svc service_def.CAPIService) {

	replication_mgr.once.Do(func() {
		// initializes replication manager
		replication_mgr.init(repl_spec_svc, remote_cluster_svc, cluster_info_svc, xdcr_topology_svc, replication_settings_svc, checkpoints_svc, capi_svc)

		// start pipeline master supervisor
		// TODO should we make heart beat settings configurable?
		replication_mgr.pipelineMasterSupervisor.Start(nil)
		logger_rm.Info("Master supervisor has started")

		// start adminport
		adminport := NewAdminport(sourceKVHost, xdcrRestPort, replication_mgr.adminport_finch)
		go adminport.Start()
		logger_rm.Info("Admin port has been launched")

		// add adminport as children of replication manager supervisor
		replication_mgr.GenericSupervisor.AddChild(adminport)

		// start replication manager supervisor
		// TODO should we make heart beat settings configurable?
		replication_mgr.GenericSupervisor.Start(nil)

		// start listening to repl spec changed events
		// this will start replications for active replication specs
		err := repl_spec_svc.StartSpecChangedCallBack(specChangedCallback, replication_mgr.repl_spec_callback_cancel_ch, replication_mgr.children_waitgrp)
		if err != nil {
			logger_rm.Errorf("Failed to start spec changed call back, err=%v", err)
			stop()
		}

		// ns_server shutdown protocol: poll stdin and exit upon reciept of EOF
		go pollStdin()

	})

}

func (rm *replicationManager) init(
	repl_spec_svc service_def.ReplicationSpecSvc,
	remote_cluster_svc service_def.RemoteClusterSvc,
	cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	replication_settings_svc service_def.ReplicationSettingsSvc,
	checkpoint_svc service_def.CheckpointsService,
	capi_svc service_def.CAPIService) {

	rm.GenericSupervisor = *supervisor.NewGenericSupervisor(base.ReplicationManagerSupervisorId, log.DefaultLoggerContext, rm, nil)
	rm.pipelineMasterSupervisor = supervisor.NewGenericSupervisor(base.PipelineMasterSupervisorId, log.DefaultLoggerContext, rm, &rm.GenericSupervisor)
	rm.repl_spec_svc = repl_spec_svc
	rm.remote_cluster_svc = remote_cluster_svc
	rm.cluster_info_svc = cluster_info_svc
	rm.xdcr_topology_svc = xdcr_topology_svc
	rm.replication_settings_svc = replication_settings_svc
	rm.checkpoint_svc = checkpoint_svc
	rm.capi_svc = capi_svc
	rm.adminport_finch = make(chan bool, 1)
	fac := factory.NewXDCRFactory(repl_spec_svc, remote_cluster_svc, cluster_info_svc, xdcr_topology_svc, checkpoint_svc, capi_svc, log.DefaultLoggerContext, log.DefaultLoggerContext, rm, rm.pipelineMasterSupervisor)

	pipeline_manager.PipelineManager(fac, log.DefaultLoggerContext)

	rm.pipeline_pending_for_repair = make(map[string]*pipelineRepairer)
	rm.repair_map_lock = &sync.RWMutex{}
	rm.children_waitgrp = &sync.WaitGroup{}

	rm.repl_spec_callback_cancel_ch = make(chan struct{}, 1)

	// use xdcr transport when talking to go-couchbase, which sets authentication info
	// on requests on local cluster
	couchbase.HTTPClient.Transport = cbauth.WrapHTTPTransport(couchbase.HTTPTransport)

	logger_rm.Info("Replication manager is initialized")

}

func (rm *replicationManager) reportFixed(topic string) {
	rm.repair_map_lock.Lock()
	defer rm.repair_map_lock.Unlock()
	delete(rm.pipeline_pending_for_repair, topic)
}

func ReplicationSpecService() service_def.ReplicationSpecSvc {
	return replication_mgr.repl_spec_svc
}

func RemoteClusterService() service_def.RemoteClusterSvc {
	return replication_mgr.remote_cluster_svc
}

func ClusterInfoService() service_def.ClusterInfoSvc {
	return replication_mgr.cluster_info_svc
}

func XDCRCompTopologyService() service_def.XDCRCompTopologySvc {
	return replication_mgr.xdcr_topology_svc
}

func ReplicationSettingsService() service_def.ReplicationSettingsSvc {
	return replication_mgr.replication_settings_svc
}

func CheckpointService() service_def.CheckpointsService {
	return replication_mgr.checkpoint_svc
}

//CreateReplication create the replication specification in metadata store
//and start the replication pipeline
func CreateReplication(sourceBucket, targetCluster, targetBucket string, settings map[string]interface{}) (string, map[string]error, error) {
	logger_rm.Infof("Creating replication - sourceBucket=%s, targetCluster=%s, targetBucket=%s, settings=%v\n",
		sourceBucket, targetCluster, targetBucket, settings)

	targetClusterRef, err := RemoteClusterService().RemoteClusterByRefName(targetCluster)
	if err != nil {
		return "", nil, err
	}

	var spec *metadata.ReplicationSpecification
	spec, errorsMap, err := replication_mgr.createAndPersistReplicationSpec(sourceBucket, targetClusterRef.Uuid, targetBucket, settings)
	if err != nil {
		logger_rm.Errorf("%v\n", err)
		return "", nil, err
	} else if len(errorsMap) != 0 {
		return "", errorsMap, nil
	}

	logger_rm.Infof("Pipeline %s is created\n", spec.Id)

	return spec.Id, nil, nil
}

//DeleteReplication stops the running replication of given replicationId and
//delete the replication specification from the metadata store
func DeleteReplication(topic string) error {
	logger_rm.Infof("Deleting replication %s\n", topic)

	// delete replication spec
	err := ReplicationSpecService().DelReplicationSpec(topic)
	if err == nil {
		logger_rm.Infof("Replication specification %s is deleted\n", topic)
	} else {
		logger_rm.Errorf("%v\n", err)
		return err
	}

	//delete all checkpoint docs in an async fashion
	replication_mgr.checkpoint_svc.DelCheckpointsDocs(topic)

	logger_rm.Infof("Pipeline %s is deleted\n", topic)

	return nil
}

//start the replication for the given replicationId
func startPipelineWithRetry(topic string) error {
	err := startPipeline(topic)
	if err != nil {

		err = replication_mgr.lauchRepairer(topic, err)
	}
	return err
}

func PipelineMasterSupervisor() *supervisor.GenericSupervisor {
	return replication_mgr.pipelineMasterSupervisor
}

//start pipeline in synchronous fashion
func startPipeline(topic string) error {
	spec, err := ReplicationSpecService().ReplicationSpec(topic)
	if err != nil {
		return err
	}

	settings := spec.Settings
	settingsMap := settings.ToMap()

	_, err = pipeline_manager.StartPipeline(topic, settingsMap)

	logger_rm.Infof("Started pipeline - err = %v\n", err)
	return err
}

//stop the running replication with the given replicationId
//in synchronous fashion
func stopPipeline(topic string) error {
	return pipeline_manager.StopPipeline(topic)
}

//update the default replication settings
func UpdateDefaultReplicationSettings(settings map[string]interface{}) (map[string]error, error) {
	defaultSettings, err := ReplicationSettingsService().GetDefaultReplicationSettings()
	if err != nil {
		return nil, err
	}

	errorMap, changed := defaultSettings.UpdateSettingsFromMap(settings)
	if len(errorMap) != 0 {
		return errorMap, nil
	}

	if changed {
		logger_rm.Infof("Updated default replication settings\n")
		return nil, ReplicationSettingsService().SetDefaultReplicationSettings(defaultSettings)
	} else {
		logger_rm.Infof("Did not update default replication settings since there are no real changes")
		return nil, nil
	}
}

//update the per-replication settings
func UpdateReplicationSettings(topic string, settings map[string]interface{}) (map[string]error, error) {
	// read replication spec with the specified replication id
	replSpec, err := ReplicationSpecService().ReplicationSpec(topic)
	if err != nil {
		return nil, err
	}
	
	oldFilterExpression := replSpec.Settings.FilterExpression
	
	// update replication spec with input settings
	errorMap, changed := replSpec.Settings.UpdateSettingsFromMap(settings)
	
	// enforce that filter expression cannot be changed
	newFilterExpression, ok := settings[FilterExpression]
	if ok {
		if newFilterExpression != oldFilterExpression {
			errorMap[FilterExpression] = errors.New("Filter expression cannot be changed after the replication is created")
		}
	}

	if len(errorMap) != 0 {
		return errorMap, nil
	}

	if changed {
		logger_rm.Infof("Updated replication settings for replication %v\n", topic)
		return nil, ReplicationSpecService().SetReplicationSpec(replSpec)
	} else {
		logger_rm.Infof("Did not update replication settings for replication %v since there are no real changes", topic)
		return nil, nil
	}
}

// delete all replications for the specified bucket
func DeleteAllReplications(bucket string) error {
	repIds, err := ReplicationSpecService().ActiveReplicationSpecIdsForBucket(bucket)
	if err != nil {
		return err
	}
	logger_rm.Infof("repIds to be deleted = %v\n", repIds)

	failedRepMap := make(map[string]string)

	for _, repId := range repIds {
		err = DeleteReplication(repId)
		if err != nil {
			failedRepMap[repId] = err.Error()
		}
	}

	if len(failedRepMap) == 0 {
		return nil
	} else {
		errMsg := fmt.Sprintf("Error deleting replications. %v", failedRepMap)
		logger_rm.Errorf(errMsg)
		return errors.New(errMsg)
	}
}

// get statistics for all running replications
//% returns a list of replication stats for the bucket. the format for each
//% item in the list is:
//% {ReplicationDocId,           & the settings doc id for this replication
//%    [{changes_left, Integer}, % amount of work remaining
//%     {docs_checked, Integer}, % total number of docs checked on target, survives restarts
//%     {docs_written, Integer}, % total number of docs written to target, survives restarts
//%     ...
//%    ]
//% }

func GetStatistics(bucket string) (*expvar.Map, error) {
	repIds, err := ReplicationSpecService().ActiveReplicationSpecIdsForBucket(bucket)
	if err != nil {
		return nil, err
	}
	logger_rm.Infof("repId=%v\n", repIds)
	stats := new(expvar.Map).Init()
	for _, repId := range repIds {
		statsForPipeline := pipeline_svc.GetStatisticsForPipeline(repId)
		logger_rm.Infof("statsForPipeline=%v\n", statsForPipeline)
		if err == nil {
			stats.Set(repId, statsForPipeline)
		}
	}
	logger_rm.Infof("stats=%v\n", stats)

	return stats, nil
}

//create and persist the replication specification
func (rm *replicationManager) createAndPersistReplicationSpec(sourceBucket, targetClusterUUID, targetBucket string, settings map[string]interface{}) (*metadata.ReplicationSpecification, map[string]error, error) {
	logger_rm.Infof("Creating replication spec - sourceBucket=%s, targetClusterUUID=%s, targetBucket=%s, settings=%v\n",
		sourceBucket, targetClusterUUID, targetBucket, settings)

	spec := metadata.NewReplicationSpecification(sourceBucket, targetClusterUUID, targetBucket)
	replSettings, err := ReplicationSettingsService().GetDefaultReplicationSettings()
	if err != nil {
		return nil, nil, err
	}
	errorMap, _ := replSettings.UpdateSettingsFromMap(settings)
	
	// check if the same replication already exists
	replicationId := metadata.ReplicationId(sourceBucket, targetClusterUUID, targetBucket)
	if err := validatePipelineExists(replicationId, "starting", false); err != nil {
		// for backward compatibility with old xdcr
		errorMap["_"] = errors.New("Replication to the same remote cluster and bucket already exists") 
	}
	
	if len(errorMap) != 0 {
		return nil, errorMap, nil
	}
	
	spec.Settings = replSettings

	//persist it
	replication_mgr.repl_spec_svc.AddReplicationSpec(spec)
	logger_rm.Debugf("replication specification %s is created and persisted\n", spec.Id)
	return spec, nil, nil
}

// get info of all running replications
func GetReplicationInfos() ([]base.ReplicationInfo, error) {
	replInfos := make([]base.ReplicationInfo, 0)

	replSpecs, err := ReplicationSpecService().ActiveReplicationSpecs()
	if err != nil {
		return nil, err
	}

	for _, replSpec := range replSpecs {
		replInfo := base.ReplicationInfo{}
		replInfo.Id = replSpec.Id

		// set stats map
		expvarMap := pipeline_svc.GetStatisticsForPipeline(replSpec.Id)
		if expvarMap != nil {
			replInfo.StatsMap = utils.GetMapFromExpvarMap(expvarMap)
		}

		// set error list
		replInfo.ErrorList = make([]base.ErrorInfo, 0)
		errorArr := getErrorsForPipeline(replSpec.Id)
		if errorArr != nil {
			for _, errMsg := range errorArr {
				// TODO add timestamp to error list in expvar
				errInfo := base.ErrorInfo{time.Now(), errMsg}
				replInfo.ErrorList = append(replInfo.ErrorList, errInfo)
			}
		}

		replInfos = append(replInfos, replInfo)
	}
	return replInfos, nil
}

// errors of a pipeline which may or may not be running
func getErrorsForPipeline(topic string) *pipelineErrorArray {
	expvarVar := expvar.Get(topic)
	if expvarVar != nil {
		expvarMap := expvarVar.(*expvar.Map)
		errorVar := expvarMap.Get(base.ErrorsStatsKey)
		if errorVar != nil {
			errorArr := errorVar.(pipelineErrorArray)
			return &errorArr
		} else {
			return nil
		}
	} else {
		return nil
	}
}

func Pipeline(topic string) common.Pipeline {
	return pipeline_manager.Pipeline(topic)
}

//error handler
func (rm *replicationManager) OnError(s common.Supervisor, errMap map[string]error) {
	logger_rm.Infof("Supervisor %v of type %v reported errors %v\n", s.Id(), reflect.TypeOf(s), errMap)

	if s.Id() == base.ReplicationManagerSupervisorId {
		// the errors came from the replication manager supervisor because adminport or pipeline master supervisor is not longer alive.
		// there is nothing we can do except to abort xdcr. ns_server will restart xdcr while later
		stop()
	} else if s.Id() == base.PipelineMasterSupervisorId {
		// the errors came from the pipeline master supervisor because some pipeline supervisors are not longer alive.
		for childId, err1 := range errMap {
			child, _ := s.Child(childId)
			// child could be null if the pipeline has been stopped so far. //TODO should we restart it here?
			if child != nil {
				pipeline, err := getPipelineFromPipelineSupevisor(child.(common.Supervisor))
				if err == nil {
					// try to fix the pipeline
					rm.lauchRepairer(pipeline.Topic(), err1)
				}
			}
		}
	} else {
		// the errors came from a pipeline supervisor because some parts are not longer alive.
		pipeline, err := getPipelineFromPipelineSupevisor(s)
		if err == nil {
			// try to fix the pipeline
			rm.lauchRepairer(pipeline.Topic(), errors.New(fmt.Sprintf("%v", errMap)))
		}
	}
}

//lauch the repairer for a pipeline
//in asynchronous fashion
func (rm *replicationManager) lauchRepairer(topic string, cur_err error) error {
	rm.repair_map_lock.Lock()
	defer rm.repair_map_lock.Unlock()

	if _, ok := rm.pipeline_pending_for_repair[topic]; !ok {
		spec, err := ReplicationSpecService().ReplicationSpec(topic)
		if err != nil {
			return err
		}

		settings := spec.Settings
		settingsMap := settings.ToMap()
		retry_interval := settingsMap[metadata.FailureRestartInterval].(int)

	repairer, err := newPipelineRepairer(topic, retry_interval, rm.children_waitgrp, cur_err)
	if err != nil {
		return err
	}
	rm.children_waitgrp.Add(1)
		rm.pipeline_pending_for_repair[topic] = repairer
		go repairer.start()
		logger_rm.Infof("Repairer to fix pipeline %v is lauched with retry_interval=%v\n", topic, retry_interval)
	} else {
		logger_rm.Infof("There is already an repairer launched for the replication, no-op")
	}
	return nil
}

func getPipelineFromPipelineSupevisor(s common.Supervisor) (common.Pipeline, error) {
	supervisorId := s.Id()
	if strings.HasPrefix(supervisorId, base.PipelineSupervisorIdPrefix) {
		pipelineId := supervisorId[len(base.PipelineSupervisorIdPrefix):]
		pipeline := pipeline_manager.Pipeline(pipelineId)
		if pipeline != nil {
			return pipeline, nil
		} else {
			// should never get here
			return nil, errors.New(fmt.Sprintf("Internal error. Pipeline, %v, is not found", pipelineId))
		}
	} else {
		// should never get here
		return nil, errors.New(fmt.Sprintf("Internal error. Supervisor, %v, is not a pipeline supervisor.", supervisorId))
	}
}

// start all replications with active replication spec
func (rm *replicationManager) startReplications() error {
	logger_rm.Infof("Replication manager init - starting existing replications")

	specs, err := replication_mgr.repl_spec_svc.ActiveReplicationSpecs()
	if err != nil {
		logger_rm.Errorf("Error retrieving active replication specs, err=%v\n", err)
		return err
	}
	logger_rm.Infof("Active replication specs=%v\n", specs)

	for _, spec := range specs {
		if spec.Settings.Active {
			startPipelineWithRetry(spec.Id)
		}
	}

	return nil
}

// check if a specified pipeline is on file
func checkPipelineOnFile(topic string) bool {
	var isOnFile = (pipeline_manager.Pipeline(topic) != nil)
	if !isOnFile {
		logger_rm.Debug("Ignore the error report, as the error is reported on a pipeline that is not on file")
	}
	return isOnFile
}

func SetPipelineLogLevel(topic string, levelStr string) error {
	pipeline := pipeline_manager.Pipeline(topic)

	//update the setting
	spec, err := ReplicationSpecService().ReplicationSpec(topic)
	if err != nil && spec == nil {
		return errors.New(fmt.Sprintf("Failed to lookup replication specification %v, err=%v", topic, err))
	}

	settings := spec.Settings
	err = settings.SetLogLevel(levelStr)
	if err != nil {
		return err
	}

	if pipeline != nil {
		if pipeline.RuntimeContext() == nil {
			return errors.New("Pipeline doesn't have the runtime context")
		}
		if pipeline.RuntimeContext().Service(base.PIPELINE_SUPERVISOR_SVC) == nil {
			return errors.New("Pipeline doesn't have the PipelineSupervisor registered")
		}
		supervisor := pipeline.RuntimeContext().Service(base.PIPELINE_SUPERVISOR_SVC).(*pipeline_svc.PipelineSupervisor)

		if supervisor != nil {
			err := supervisor.SetPipelineLogLevel(levelStr)
			if err == nil {
				logger_rm.Infof("Changed log level to %v for pipeline %v\n", levelStr, topic)
			}
			return err
		}
	}
	return nil
}

func validatePipelineExists(topic, action string, exist bool) error {
	_, err := replication_mgr.repl_spec_svc.ReplicationSpec(topic)
	pipelineExist := (err == nil)
	if pipelineExist != exist {
		state := "already exists"
		if exist {
			state = "does not exist"
		}
		return errors.New(fmt.Sprintf("Error %v replication with id, %v, since it %v.\n", action, topic, state))
	}
	return nil
}

func validatePipelineState(topic, action string, active bool) error {
	pipelineActive := (pipeline_manager.Pipeline(topic) != nil)
	if pipelineActive != active {
		state := "already"
		if active {
			state = "not"
		}
		return errors.New(fmt.Sprintf("Warning: Cannot %v replication with id, %v, since it is %v actively running.\n", action, topic, state))
	}
	return nil
}

// ns_server shutdown protocol: poll stdin and exit upon reciept of EOF
func pollStdin() {
	reader := bufio.NewReader(os.Stdin)
	logger_rm.Infof("pollEOF: About to start stdin polling")
	for {
		ch, err := reader.ReadByte()
		logger_rm.Infof("received byte %v\n", ch)
		if err == io.EOF {
			logger_rm.Infof("Received EOF; Exiting...")
			stop()
		}
		if err != nil {
			logger_rm.Errorf("Unexpected error polling stdin: %v\n", err)
			os.Exit(1)
		}
		if ch == '\n' || ch == '\r' {
			logger_rm.Infof("Received EOL; Exiting...")
			stop()
		}
	}
}

func stop() {
	// kill adminport to stop receiving new requests
	close(replication_mgr.adminport_finch)

	// TODO checkpointing

	// stop running pipelines
	for _, topic := range pipeline_manager.Topics() {
		pipeline_manager.StopPipeline(topic)
	}

	// stop listening to spec changed events
	replication_mgr.repl_spec_callback_cancel_ch <- struct{}{}
	logger_rm.Infof("Sent cancel signal to spec change observer")

	//send finish signal to all repairer
	for _, repairer := range replication_mgr.pipeline_pending_for_repair {
		repairer.fin_ch <- true
	}
	logger_rm.Infof("Sent finish signal to all running repairer")


	//clean up the connection pool
	base.ConnPoolMgr().Close()
	
	replication_mgr.children_waitgrp.Wait()

	logger_rm.Infof("Replication manager exists")
	// exit main routine
	os.Exit(0)
}

// Implement callback function for replication spec service
func specChangedCallback(changedSpecId string, changedSpec *metadata.ReplicationSpecification) error {
	logger_rm.Debugf("specChangedCallback called on id = %v\n", changedSpecId)

	topic := changedSpecId
	pipelineRunning := (pipeline_manager.Pipeline(topic) != nil)

	if changedSpec == nil {
		// replication spec has been deleted

		// stop replication if it is running
		if pipelineRunning {
			logger_rm.Infof("Stopping pipeline %v since the replication spec has been deleted\n", topic)
			return stopPipeline(topic)
		} else {
			return nil
		}
	}

	specActive := changedSpec.Settings.Active

	if pipelineRunning && specActive {
		// some replication settings have been changed.

		oldSettings := pipeline_manager.Pipeline(topic).Specification().Settings

		// if some critical settings have been changed, stop, reconstruct, and restart pipeline
		if needToReconstructPipeline(oldSettings, changedSpec.Settings) {
			logger_rm.Infof("Restarting pipeline %v since the changes to replication spec are critical\n", topic)

			err := stopPipeline(topic)
			if err != nil {
				logger_rm.Errorf("Failed to stop pipeline %v, err=%v\n", topic, err)
				return err
			}
			return startPipelineWithRetry(topic)
		} else {
			// otherwise, perform live update to pipeline
			err := liveUpdatePipeline(topic, oldSettings, changedSpec.Settings)
			if err != nil {
				logger_rm.Errorf("Failed to perform live update on pipeline %v, err=%v\n", topic, err)
				return err
			} else {

				logger_rm.Infof("Kept pipeline %v running since the changes to replication spec are not critical\n", topic)
				return nil
			}
		}

	} else if pipelineRunning && !specActive {
		//stop replication
		logger_rm.Infof("Stopping pipeline %v since the replication spec has been changed to inactive\n", topic)

		return stopPipeline(topic)
	} else if !pipelineRunning && specActive {
		// start replication
		logger_rm.Infof("Starting pipeline %v since the replication spec has been changed to active\n", topic)

		return startPipelineWithRetry(topic)
	} else {
		// this is the case where pipeline is not running and spec is not active.
		// nothing needs to be done
		return nil
	}
}

// whether there are critical changes to the replication spec that require pipeline reconstruction
func needToReconstructPipeline(oldSettings *metadata.ReplicationSettings, newSettings *metadata.ReplicationSettings) bool {

	// the following require reconstuction of pipeline
	repTypeChanged := !(oldSettings.RepType == newSettings.RepType)
	sourceNozzlePerNodeChanged := !(oldSettings.SourceNozzlePerNode == newSettings.SourceNozzlePerNode)
	targetNozzlePerNodeChanged := !(oldSettings.TargetNozzlePerNode == newSettings.TargetNozzlePerNode)

	// the following may qualify for live update in the future. The corresponding
	// parts need to expose APIs to change the settings on the fly
	batchCountChanged := (oldSettings.BatchCount != newSettings.BatchCount)
	batchSizeChanged := (oldSettings.BatchSize != newSettings.BatchSize)
	statsIntervalChanged := (oldSettings.StatsInterval != newSettings.StatsInterval)

	return repTypeChanged || sourceNozzlePerNodeChanged || targetNozzlePerNodeChanged ||
		batchCountChanged || batchSizeChanged || statsIntervalChanged

}

func liveUpdatePipeline(topic string, oldSettings *metadata.ReplicationSettings, newSettings *metadata.ReplicationSettings) error {
	logger_rm.Infof("Performing live update on pipeline %v \n", topic)
	logLevelChanged := (oldSettings.LogLevel != newSettings.LogLevel)

	if logLevelChanged {
		err := SetPipelineLogLevel(topic, newSettings.LogLevel.String())
		if err != nil {
			return err
		}
	}

	// the following do not require any actions
	// FailureRestartInterval

	// TODO other live updates

	// TODO need to decide what to do with the following after they are implemented.
	// CheckpointInterval, OptimisticReplicationThreshold, MaxExpectedReplicationLag, TimeoutPercentageCap

	return nil
}
