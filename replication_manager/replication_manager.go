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
	"errors"
	"expvar"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/factory"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/pipeline_manager"
	"github.com/couchbase/goxdcr/pipeline_svc"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/supervisor"
	"github.com/couchbase/goxdcr/utils"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"
)

var logger_rm *log.CommonLogger = log.NewLogger("ReplicationManager", log.DefaultLoggerContext)

var GoXDCROptions struct {
	SourceKVAdminPort    uint64 //source kv admin port
	XdcrRestPort         uint64 // port number of XDCR rest server
	SslProxyUpstreamPort uint64
	IsEnterprise         bool // whether couchbase is of enterprise edition
	IsConvert            bool // whether xdcr is running in conversion/upgrade mode

	// logging related parameters
	LogFileDir          string
	MaxLogFileSize      uint64
	MaxNumberOfLogFiles uint64
}

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
	//audit service handle
	audit_svc service_def.AuditSvc

	once sync.Once

	//finish channel for adminport
	adminport_finch chan bool

	repl_spec_callback_cancel_ch chan struct{}

	running bool

	children_waitgrp *sync.WaitGroup

	status_logger_finch chan bool
}

//singleton
var replication_mgr replicationManager

func StartReplicationManager(sourceKVHost string, xdcrRestPort uint16,
	repl_spec_svc service_def.ReplicationSpecSvc,
	remote_cluster_svc service_def.RemoteClusterSvc,
	cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	replication_settings_svc service_def.ReplicationSettingsSvc,
	checkpoints_svc service_def.CheckpointsService,
	capi_svc service_def.CAPIService,
	audit_svc service_def.AuditSvc) {

	replication_mgr.once.Do(func() {
		// ns_server shutdown protocol: poll stdin and exit upon reciept of EOF
		go pollStdin()

		// initializes replication manager
		replication_mgr.init(repl_spec_svc, remote_cluster_svc, cluster_info_svc, xdcr_topology_svc, replication_settings_svc, checkpoints_svc, capi_svc, audit_svc)

		// start pipeline master supervisor
		// TODO should we make heart beat settings configurable?
		replication_mgr.pipelineMasterSupervisor.Start(nil)
		logger_rm.Info("Master supervisor has started")

		// start replication manager supervisor
		// TODO should we make heart beat settings configurable?
		replication_mgr.GenericSupervisor.Start(nil)

		// start replications with active spec and set ReplicationStatus for paused replications
		replication_mgr.initReplications()

		replication_mgr.running = true

		replication_mgr.status_logger_finch = make(chan bool, 1)
		go replication_mgr.checkReplicationStatus(replication_mgr.status_logger_finch)

		// start adminport
		adminport := NewAdminport(sourceKVHost, xdcrRestPort, replication_mgr.adminport_finch)
		go adminport.Start()
		logger_rm.Info("Admin port has been launched")
		// add adminport as children of replication manager supervisor
		replication_mgr.GenericSupervisor.AddChild(adminport)

	})

}

func (rm *replicationManager) initReplications() {
	for i := 0; i < service_def.MaxNumOfRetries; i++ {
		// set ReplicationStatus for paused replications so that they will show up in task list
		specs, err := rm.repl_spec_svc.AllReplicationSpecs()
		if err != nil {
			logger_rm.Errorf("Failed to get all replication specs, err=%v, num_of_retry=%v\n", err, i)

		} else {

			for _, spec := range specs {
				if !spec.Settings.Active {
					pipeline_manager.SetReplicationStatusForPausedReplication(spec)
				}
			}

			// start listening to repl spec changed events
			// this will start replications with active replication specs
			err = rm.repl_spec_svc.StartSpecChangedCallBack(specChangedCallback, specChangeObservationFailureCallBack, replication_mgr.repl_spec_callback_cancel_ch, replication_mgr.children_waitgrp)
			if err != nil {
				logger_rm.Errorf("Failed to start spec changed call back, err=%v, num_of_retry=%v\n", err, i)
			} else {
				return
			}
		}
	}
	logger_rm.Errorf("Can't initReplications after %v retries. Exit replicaiton manager", service_def.MaxNumOfRetries)
	exitProcess(false)
}

func (rm *replicationManager) checkReplicationStatus(fin_chan chan bool) {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-fin_chan:
			return
		case <-ticker.C:
			pipeline_manager.CheckPipelines()
		}
	}
}

func (rm *replicationManager) init(
	repl_spec_svc service_def.ReplicationSpecSvc,
	remote_cluster_svc service_def.RemoteClusterSvc,
	cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	replication_settings_svc service_def.ReplicationSettingsSvc,
	checkpoint_svc service_def.CheckpointsService,
	capi_svc service_def.CAPIService,
	audit_svc service_def.AuditSvc) {

	rm.GenericSupervisor = *supervisor.NewGenericSupervisor(base.ReplicationManagerSupervisorId, log.DefaultLoggerContext, rm, nil)
	rm.pipelineMasterSupervisor = supervisor.NewGenericSupervisor(base.PipelineMasterSupervisorId, log.DefaultLoggerContext, rm, &rm.GenericSupervisor)
	rm.repl_spec_svc = repl_spec_svc
	rm.remote_cluster_svc = remote_cluster_svc
	rm.cluster_info_svc = cluster_info_svc
	rm.xdcr_topology_svc = xdcr_topology_svc
	rm.replication_settings_svc = replication_settings_svc
	rm.checkpoint_svc = checkpoint_svc
	rm.capi_svc = capi_svc
	rm.audit_svc = audit_svc
	rm.adminport_finch = make(chan bool, 1)
	rm.children_waitgrp = &sync.WaitGroup{}
	fac := factory.NewXDCRFactory(repl_spec_svc, remote_cluster_svc, cluster_info_svc, xdcr_topology_svc, checkpoint_svc, capi_svc, log.DefaultLoggerContext, log.DefaultLoggerContext, rm, rm.pipelineMasterSupervisor)

	pipeline_manager.PipelineManager(fac, rm.repl_spec_svc, log.DefaultLoggerContext)

	rm.repl_spec_callback_cancel_ch = make(chan struct{}, 1)

	logger_rm.Info("Replication manager is initialized")

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

func AuditService() service_def.AuditSvc {
	return replication_mgr.audit_svc
}

//CreateReplication create the replication specification in metadata store
//and start the replication pipeline
func CreateReplication(justValidate bool, sourceBucket, targetCluster, targetBucket string, settings map[string]interface{}, realUserId *base.RealUserId) (string, map[string]error, error) {
	logger_rm.Infof("Creating replication - justValidate=%v, sourceBucket=%s, targetCluster=%s, targetBucket=%s, settings=%v\n",
		justValidate, sourceBucket, targetCluster, targetBucket, settings)

	var spec *metadata.ReplicationSpecification
	spec, errorsMap, err := replication_mgr.createAndPersistReplicationSpec(justValidate, sourceBucket, targetCluster, targetBucket, settings)
	if err != nil {
		logger_rm.Errorf("%v\n", err)
		return "", nil, err
	} else if len(errorsMap) != 0 {
		return "", errorsMap, nil
	}

	if justValidate {
		return spec.Id, nil, nil
	}

	//trigger specChangedCallback explicitly asynchronously
	go specChangedCallback(spec.Id, spec)

	go writeCreateReplicationEvent(spec, realUserId)

	logger_rm.Infof("Replication specification %s is created\n", spec.Id)

	return spec.Id, nil, nil
}

//DeleteReplication stops the running replication of given replicationId and
//delete the replication specification from the metadata store
func DeleteReplication(topic string, realUserId *base.RealUserId) error {
	logger_rm.Infof("Deleting replication %s\n", topic)

	// delete replication spec
	spec, err := ReplicationSpecService().DelReplicationSpec(topic)
	if err == nil {
		logger_rm.Infof("Replication specification %s is deleted\n", topic)
	} else {
		logger_rm.Errorf("%v\n", err)
		return err
	}

	//trigger specChangedCallback explicitly asynchronously
	go specChangedCallback(topic, nil)

	go writeGenericReplicationEvent(base.CancelReplicationEventId, spec, realUserId)

	logger_rm.Infof("Pipeline %s is deleted\n", topic)

	return nil
}

//start the replication for the given replicationId
func startPipelineWithRetry(topic string) error {
	_, err := pipeline_manager.StartPipeline(topic)
	if err != nil {

		err = pipeline_manager.Update(topic, err)
	}
	return err
}

func PipelineMasterSupervisor() *supervisor.GenericSupervisor {
	return replication_mgr.pipelineMasterSupervisor
}

//update the default replication settings
func UpdateDefaultReplicationSettings(settings map[string]interface{}, realUserId *base.RealUserId) (map[string]error, error) {
	defaultSettings, err := ReplicationSettingsService().GetDefaultReplicationSettings()
	if err != nil {
		return nil, err
	}

	changedSettingsMap, errorMap := defaultSettings.UpdateSettingsFromMap(settings)
	if len(errorMap) != 0 {
		return errorMap, nil
	}

	if len(changedSettingsMap) != 0 {
		err = ReplicationSettingsService().SetDefaultReplicationSettings(defaultSettings)
		if err != nil {
			return nil, err
		}
		logger_rm.Infof("Updated default replication settings\n")

		go writeUpdateDefaultReplicationSettingsEvent(&changedSettingsMap, realUserId)

	} else {
		logger_rm.Infof("Did not update default replication settings since there are no real changes")
	}

	return nil, nil
}

//update the per-replication settings
func UpdateReplicationSettings(topic string, settings map[string]interface{}, realUserId *base.RealUserId) (map[string]error, error) {
	logger_rm.Infof("Update replication settings for %v, settings=%v\n", topic, settings)
	// read replication spec with the specified replication id
	replSpec, err := ReplicationSpecService().ReplicationSpec(topic)
	if err != nil {
		return nil, err
	}

	oldFilterExpression := replSpec.Settings.FilterExpression

	// update replication spec with input settings
	changedSettingsMap, errorMap := replSpec.Settings.UpdateSettingsFromMap(settings)

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

	if len(changedSettingsMap) != 0 {
		err = ReplicationSpecService().SetReplicationSpec(replSpec)
		if err != nil {
			return nil, err
		}
		logger_rm.Infof("Updated replication settings for replication %v\n", topic)

		//trigger specChangedCallback explicitly asynchronously
		go specChangedCallback(topic, replSpec)

		go writeUpdateReplicationSettingsEvent(replSpec, &changedSettingsMap, realUserId)

		// if the active flag has been changed, log Pause/ResumeReplication event
		active, ok := changedSettingsMap[metadata.Active]
		if ok {
			if active.(bool) {
				go writeGenericReplicationEvent(base.ResumeReplicationEventId, replSpec, realUserId)
			} else {
				go writeGenericReplicationEvent(base.PauseReplicationEventId, replSpec, realUserId)
			}
		}
		logger_rm.Infof("Done with replication settings auditing for replication %v\n", topic)

	} else {
		logger_rm.Infof("Did not update replication settings for replication %v since there are no real changes", topic)
	}

	return nil, nil
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
	repIds := pipeline_manager.AllReplicationsForBucket(bucket)
	logger_rm.Debugf("repIds=%v\n", repIds)

	stats := new(expvar.Map).Init()
	for _, repId := range repIds {
		statsForPipeline := pipeline_svc.GetStatisticsForPipeline(repId)
		if statsForPipeline != nil {
			stats.Set(repId, statsForPipeline)
		}
	}
	logger_rm.Debugf("stats=%v\n", stats)

	return stats, nil
}

//create and persist the replication specification
func (rm *replicationManager) createAndPersistReplicationSpec(justValidate bool, sourceBucket, targetCluster, targetBucket string, settings map[string]interface{}) (*metadata.ReplicationSpecification, map[string]error, error) {
	logger_rm.Infof("Creating replication spec - justValidate=%v, sourceBucket=%s, targetCluster=%s, targetBucket=%s, settings=%v\n",
		justValidate, sourceBucket, targetCluster, targetBucket, settings)

	// validate that everything is alright with the replication configuration before actually creating it
	errorMap := replication_mgr.repl_spec_svc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket)
	if len(errorMap) > 0 {
		return nil, errorMap, nil
	}

	//this will not fail because of the validation above
	targetClusterRef, err := rm.remote_cluster_svc.RemoteClusterByRefName(targetCluster, false)
	if err != nil {
		return nil, nil, err
	}

	spec, err := rm.repl_spec_svc.ConstructNewReplicationSpec(sourceBucket, targetClusterRef.Uuid, targetBucket)
	if err != nil {
		return nil, nil, err
	}

	replSettings, err := ReplicationSettingsService().GetDefaultReplicationSettings()
	if err != nil {
		return nil, nil, err
	}
	_, errorMap = replSettings.UpdateSettingsFromMap(settings)
	if len(errorMap) != 0 {
		return nil, errorMap, nil
	}
	spec.Settings = replSettings

	if justValidate {
		return spec, nil, nil
	}

	//persist it
	err = replication_mgr.repl_spec_svc.AddReplicationSpec(spec)
	if err == nil {
		logger_rm.Infof("Success adding replication specification %s\n", spec.Id)
		return spec, nil, nil
	} else {
		logger_rm.Errorf("Error adding replication specification %s. err=%v\n", spec.Id, err)
		return spec, nil, err
	}
}

// get info of all running replications
func GetReplicationInfos() ([]base.ReplicationInfo, error) {
	replInfos := make([]base.ReplicationInfo, 0)

	replIds := pipeline_manager.AllReplications()

	for _, replId := range replIds {
		replInfo := base.ReplicationInfo{}
		replInfo.Id = replId
		replInfo.StatsMap = make(map[string]interface{})
		replInfo.ErrorList = make([]base.ErrorInfo, 0)

		rep_status := pipeline_manager.ReplicationStatus(replId)
		if rep_status != nil {
			// set stats map
			expvarMap := pipeline_svc.GetStatisticsForPipeline(replId)
			if expvarMap != nil {
				replInfo.StatsMap = utils.GetMapFromExpvarMap(expvarMap)
				validateStatsMap(replInfo.StatsMap)
			}

			// set error list
			errs := rep_status.Errors()
			for _, pipeline_error := range errs {
				errInfo := base.ErrorInfo{pipeline_error.Timestamp.UnixNano(), pipeline_error.ErrMsg}
				replInfo.ErrorList = append(replInfo.ErrorList, errInfo)
			}
		}

		// set maxVBReps stats to 0 when replication has never been run or has been paused to ensure that ns_server gets the correct replication status
		if rep_status == nil || rep_status.RuntimeStatus() == pipeline.Paused {
			replInfo.StatsMap[base.MaxVBReps] = 0
		}

		replInfos = append(replInfos, replInfo)
	}
	return replInfos, nil
}

func validateStatsMap(statsMap map[string]interface{}) {
	missingStats := make([]string, 0)
	if _, ok := statsMap[pipeline_svc.CHANGES_LEFT_METRIC]; !ok {
		missingStats = append(missingStats, pipeline_svc.CHANGES_LEFT_METRIC)
	}
	if len(missingStats) > 0 {
		logger_rm.Errorf("Stats missing when constructing replication infos: %v", missingStats)
	}
}

//error handler
func (rm *replicationManager) OnError(s common.Supervisor, errMap map[string]error) {
	logger_rm.Infof("Supervisor %v of type %v reported errors %v\n", s.Id(), reflect.TypeOf(s), errMap)

	if s.Id() == base.ReplicationManagerSupervisorId {
		// the errors came from the replication manager supervisor because adminport or pipeline master supervisor is not longer alive.
		// there is nothing we can do except to abort xdcr. ns_server will restart xdcr while later
		exitProcess(false)
	} else if s.Id() == base.PipelineMasterSupervisorId {
		// the errors came from the pipeline master supervisor because some pipeline supervisors are not longer alive.
		for childId, err1 := range errMap {
			child, _ := s.Child(childId)
			// child could be null if the pipeline has been stopped so far. //TODO should we restart it here?
			if child != nil {
				pipeline, err := getPipelineFromPipelineSupevisor(child.(common.Supervisor))
				if err == nil {
					// try to fix the pipeline
					pipeline_manager.Update(pipeline.Topic(), err1)
				}
			}
		}
	} else {
		// the errors came from a pipeline supervisor because some parts are not longer alive.
		pipeline, err := getPipelineFromPipelineSupevisor(s)
		if err == nil {
			// try to fix the pipeline
			pipeline_manager.Update(pipeline.Topic(), errors.New(fmt.Sprintf("%v", errMap)))
		}
	}
}

//lauch the repairer for a pipeline
//in asynchronous fashion

func getPipelineFromPipelineSupevisor(s common.Supervisor) (common.Pipeline, error) {
	supervisorId := s.Id()
	if strings.HasPrefix(supervisorId, base.PipelineSupervisorIdPrefix) {
		pipelineId := supervisorId[len(base.PipelineSupervisorIdPrefix):]
		pipeline := pipeline_manager.ReplicationStatus(pipelineId).Pipeline()
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

func SetPipelineLogLevel(topic string, levelStr string) error {
	pipeline := pipeline_manager.ReplicationStatus(topic).Pipeline()

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

// ns_server shutdown protocol: poll stdin and exit upon reciept of EOF
func pollStdin() {
	reader := bufio.NewReader(os.Stdin)
	logger_rm.Infof("pollEOF: About to start stdin polling")
	for {
		ch, err := reader.ReadByte()
		logger_rm.Infof("received byte %v\n", ch)
		if err == io.EOF {
			logger_rm.Infof("Received EOF; Exiting...")
			exitProcess(false)
		}
		if err != nil {
			logger_rm.Errorf("Unexpected error polling stdin: %v\n", err)
			exitProcess(true)
		}
		if ch == '\n' || ch == '\r' {
			logger_rm.Infof("Received EOL; Exiting...")
			exitProcess(false)
		}
	}
}

//gracefull stop
func cleanup() {
	if replication_mgr.running {

		replication_mgr.running = false

		//stop the generic supervisor
		replication_mgr.Stop()

		//stop pipelineMasterSupervisor
		replication_mgr.pipelineMasterSupervisor.Stop()

		// stop listening to spec changed events
		replication_mgr.repl_spec_callback_cancel_ch <- struct{}{}
		logger_rm.Infof("Sent cancel signal to spec change observer")

		// kill adminport to stop receiving new requests
		close(replication_mgr.adminport_finch)

		utils.ExecWithTimeout(pipeline_manager.OnExit, 1*time.Second, logger_rm)

		close(replication_mgr.status_logger_finch)

		logger_rm.Infof("Replication manager exists")
	} else {
		logger_rm.Info("Replication manager is already in the processof stopping, no-op on this stop request")
	}
}

//crash
func exitProcess(byForce bool) {
	logger_rm.Info("Replication manager is exiting...")
	//clean up the connection pool
	defer base.ConnPoolMgr().Close()

	//clean up the tcp connection pool
	defer base.TCPConnPoolMgr().Close()

	if !byForce {
		cleanup()
	}

	os.Exit(0)
	logger_rm.Info("Replication manager exited")

}

// Implement callback function for replication spec service
func specChangedCallback(changedSpecId string, changedSpec *metadata.ReplicationSpecification) error {
	logger_rm.Infof("specChangedCallback called on id = %v\n", changedSpecId)

	topic := changedSpecId

	if changedSpec == nil {
		// replication spec has been deleted

		// stop replication if it is running
		pipelineRunning := pipeline_manager.IsPipelineRunning(topic)
		if pipelineRunning {
			logger_rm.Infof("Stopping pipeline %v since the replication spec has been deleted\n", topic)
			err := pipeline_manager.StopPipeline(topic)
			if err != nil {
				logger_rm.Infof("Stopping pipeline %v failed with err = %v, leave it alone\n", topic, err)
			}
		}
		pipeline_manager.RemoveReplicationStatus(topic)

		//delete all checkpoint docs in an async fashion
		replication_mgr.checkpoint_svc.DelCheckpointsDocs(topic)

		//close the connection pool for the replication
		pools := base.ConnPoolMgr().FindPoolNamesByPrefix(topic)
		for _, poolName := range pools {
			base.ConnPoolMgr().RemovePool(poolName)
		}
		return nil
	}

	specActive := changedSpec.Settings.Active
	//if the replication doesn't exit, it is treated the same as it exits, but it is paused
	specActive_old := false
	var oldSettings *metadata.ReplicationSettings = nil
	if pipeline_manager.ReplicationStatus(topic) != nil {
		oldSettings = pipeline_manager.ReplicationStatus(topic).Settings()
		specActive_old = oldSettings.Active
	}

	if specActive_old && specActive {
		// if some critical settings have been changed, stop, reconstruct, and restart pipeline
		if needToReconstructPipeline(oldSettings, changedSpec.Settings) {
			logger_rm.Infof("Restarting pipeline %v since the changes to replication spec are critical\n", topic)
			return pipeline_manager.Update(topic, nil)

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

	} else if specActive_old && !specActive {
		//stop replication
		logger_rm.Infof("Stopping pipeline %v since the replication spec has been changed to inactive\n", topic)

		return pipeline_manager.Update(topic, nil)

	} else if !specActive_old && specActive {
		// start replication
		logger_rm.Infof("Starting pipeline %v since the replication spec has been changed to active\n", topic)

		return pipeline_manager.Update(topic, nil)

	} else {
		// this is the case where pipeline is not running and spec is not active.
		// nothing needs to be done
		return nil
	}
}

// Callback function for replication spec observation failed event
func specChangeObservationFailureCallBack(err error) {
	// the only thing we can do is to abort
	logger_rm.Infof("metakv.RunObserveChildren failed, err=%v. Restart it\n", err)
	if err == nil && !replication_mgr.running {
		//callback is cannceled and replication_mgr is exiting.
		//no-op
		return
	}
	replication_mgr.initReplications()
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

func writeGenericReplicationEvent(eventId uint32, spec *metadata.ReplicationSpecification, realUserId *base.RealUserId) {
	event, err := constructGenericReplicationEvent(spec, realUserId)
	if err == nil {
		err = AuditService().Write(eventId, event)
	}

	logAuditErrors(err)
}

func writeCreateReplicationEvent(spec *metadata.ReplicationSpecification, realUserId *base.RealUserId) {
	genericReplicationEvent, err := constructGenericReplicationEvent(spec, realUserId)
	if err == nil {
		createReplicationEvent := &base.CreateReplicationEvent{
			GenericReplicationEvent: *genericReplicationEvent,
			FilterExpression:        spec.Settings.FilterExpression}

		err = AuditService().Write(base.CreateReplicationEventId, createReplicationEvent)
	}

	logAuditErrors(err)
}

func writeUpdateDefaultReplicationSettingsEvent(changedSettingsMap *map[string]interface{}, realUserId *base.RealUserId) {
	event, err := constructUpdateDefaultReplicationSettingsEvent(changedSettingsMap, realUserId)
	if err == nil {
		err = AuditService().Write(base.UpdateDefaultReplicationSettingsEventId, event)
	}
	logAuditErrors(err)
}

func writeUpdateReplicationSettingsEvent(spec *metadata.ReplicationSpecification, changedSettingsMap *map[string]interface{}, realUserId *base.RealUserId) {
	replicationSpecificFields, err := constructReplicationSpecificFieldsFromSpec(spec)
	if err == nil {
		updateDefaultReplicationSettingsEvent, err := constructUpdateDefaultReplicationSettingsEvent(changedSettingsMap, realUserId)
		if err == nil {
			updateReplicationSettingsEvent := &base.UpdateReplicationSettingsEvent{
				ReplicationSpecificFields:             *replicationSpecificFields,
				UpdateDefaultReplicationSettingsEvent: *updateDefaultReplicationSettingsEvent}
			err = AuditService().Write(base.UpdateReplicationSettingsEventId, updateReplicationSettingsEvent)
		}
	}
	logAuditErrors(err)
}

func constructGenericReplicationFields(realUserId *base.RealUserId) (*base.GenericReplicationFields, error) {
	localClusterName, err := XDCRCompTopologyService().MyHostAddr()
	if err != nil {
		return nil, err
	}

	return &base.GenericReplicationFields{
		GenericFields:    base.GenericFields{log.FormatTimeWithMilliSecondPrecision(time.Now()), *realUserId},
		LocalClusterName: localClusterName}, nil
}

func constructReplicationSpecificFieldsFromSpec(spec *metadata.ReplicationSpecification) (*base.ReplicationSpecificFields, error) {
	remoteClusterName := RemoteClusterService().GetRemoteClusterNameFromClusterUuid(spec.TargetClusterUUID)

	return &base.ReplicationSpecificFields{
		SourceBucketName:  spec.SourceBucketName,
		RemoteClusterName: remoteClusterName,
		TargetBucketName:  spec.TargetBucketName}, nil
}

func constructGenericReplicationEvent(spec *metadata.ReplicationSpecification, realUserId *base.RealUserId) (*base.GenericReplicationEvent, error) {
	genericReplicationFields, err := constructGenericReplicationFields(realUserId)
	if err != nil {
		return nil, err
	}

	replicationSpecificFields, err := constructReplicationSpecificFieldsFromSpec(spec)
	if err != nil {
		return nil, err
	}

	return &base.GenericReplicationEvent{
		GenericReplicationFields:  *genericReplicationFields,
		ReplicationSpecificFields: *replicationSpecificFields}, nil
}

func constructUpdateDefaultReplicationSettingsEvent(changedSettingsMap *map[string]interface{}, realUserId *base.RealUserId) (*base.UpdateDefaultReplicationSettingsEvent, error) {
	logger_rm.Info("Start constructUpdateDefaultReplicationSettingsEvent....")
	genericReplicationFields, err := constructGenericReplicationFields(realUserId)
	if err != nil {
		return nil, err
	}

	// convert keys in changedSettingsMap from internal metadata keys to external facing rest api keys
	convertedSettingsMap := make(map[string]interface{})
	for key, value := range *changedSettingsMap {
		if key == metadata.Active {
			convertedSettingsMap[SettingsKeyToRestKeyMap[key]] = !(value.(bool))
		} else {
			convertedSettingsMap[SettingsKeyToRestKeyMap[key]] = value
		}
	}
	logger_rm.Info("Done constructUpdateDefaultReplicationSettingsEvent....")

	return &base.UpdateDefaultReplicationSettingsEvent{
		GenericReplicationFields: *genericReplicationFields,
		UpdatedSettings:          convertedSettingsMap}, nil
}

func logAuditErrors(err error) {
	if err != nil {
		err = utils.NewEnhancedError(base.ErrorWritingAudit, err)
		logger_rm.Errorf(err.Error())
	}
}
