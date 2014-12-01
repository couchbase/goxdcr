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
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/pipeline_manager"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/factory"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/pipeline_svc"
	"github.com/couchbase/goxdcr/supervisor"
	"sync"
	"reflect"
	"strings"
	"os"
	"io"
	"bufio"
)

var logger_rm *log.CommonLogger = log.NewLogger("ReplicationManager", log.DefaultLoggerContext)

/************************************
/* struct ReplicationManager
*************************************/
type replicationManager struct {
	supervisor.GenericSupervisor  // supervises the livesness of adminport and pipelineMasterSupervisor
	pipelineMasterSupervisor  *supervisor.GenericSupervisor  // supervises the liveness of all pipeline supervisors
	
	sourceKVHost      string //source kv host name
	sourceKVPort      int //source kv admin port
	
	repl_spec_svc            service_def.ReplicationSpecSvc
	remote_cluster_svc       service_def.RemoteClusterSvc
	cluster_info_svc         service_def.ClusterInfoSvc
	xdcr_topology_svc        service_def.XDCRCompTopologySvc
	replication_settings_svc service_def.ReplicationSettingsSvc
	once                     sync.Once
	
	adminport_finch  chan bool //finish channel for adminport
}

var replication_mgr replicationManager

func StartReplicationManager(sourceKVHost string, 
	sourceKVPort int, 
	repl_spec_svc service_def.ReplicationSpecSvc,
	remote_cluster_svc service_def.RemoteClusterSvc,
	cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	replication_settings_svc service_def.ReplicationSettingsSvc) {
	
	replication_mgr.once.Do(func() {
		// initializes replication manager
		replication_mgr.init(sourceKVHost, sourceKVPort, repl_spec_svc, remote_cluster_svc, cluster_info_svc, xdcr_topology_svc, replication_settings_svc)
				
		// start pipeline master supervisor
		// TODO should we make heart beat settings configurable?
		replication_mgr.pipelineMasterSupervisor.Start(nil)
		
		// start adminport
		adminport := NewAdminport(sourceKVHost, replication_mgr.adminport_finch)	
		go adminport.Start()
		
		// add pipeline master supervisor and adminport as children of replication manager supervisor
		replication_mgr.GenericSupervisor.AddChild(replication_mgr.pipelineMasterSupervisor)
		replication_mgr.GenericSupervisor.AddChild(adminport)
		
		// start replication manager supervisor
		// TODO should we make heart beat settings configurable?
		replication_mgr.GenericSupervisor.Start(nil)
		
		// start replications in existing replication specs
		go replication_mgr.startReplications()
		
		// ns_server shutdown protocol: poll stdin and exit upon reciept of EOF
		go pollStdin()
	})
	
}

func (rm *replicationManager) init(sourceKVHost string, 
	sourceKVPort int,
	repl_spec_svc service_def.ReplicationSpecSvc,
	remote_cluster_svc service_def.RemoteClusterSvc,
	cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	replication_settings_svc service_def.ReplicationSettingsSvc) {

	rm.GenericSupervisor = *supervisor.NewGenericSupervisor(base.ReplicationManagerSupervisorId, log.DefaultLoggerContext, rm)
	rm.pipelineMasterSupervisor = supervisor.NewGenericSupervisor(base.PipelineMasterSupervisorId, log.DefaultLoggerContext, rm)
	rm.sourceKVHost = sourceKVHost
	rm.sourceKVPort = sourceKVPort
	rm.repl_spec_svc = repl_spec_svc
	rm.remote_cluster_svc = remote_cluster_svc
	rm.cluster_info_svc = cluster_info_svc
	rm.xdcr_topology_svc = xdcr_topology_svc
	rm.replication_settings_svc = replication_settings_svc
	rm.adminport_finch = make(chan bool)
	fac := factory.NewXDCRFactory(repl_spec_svc, cluster_info_svc, xdcr_topology_svc, log.DefaultLoggerContext, log.DefaultLoggerContext, rm)

	pipeline_manager.PipelineManager(fac, log.DefaultLoggerContext)

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

func CreateReplication(sourceClusterUUID, sourceBucket, targetClusterUUID, targetBucket, filterName string, settings map[string]interface{}, createReplSpec bool) (string, error) {
	logger_rm.Infof("Creating replication - sourceCluterUUID=%s, sourceBucket=%s, targetClusterUUID=%s, targetBucket=%s, filterName=%s, settings=%v, createReplSpec=%v\n", sourceClusterUUID,
	                sourceBucket, targetClusterUUID, targetBucket, filterName, settings, createReplSpec)

	var topic string
	if createReplSpec {
		spec, err := replication_mgr.createAndPersistReplicationSpec(sourceClusterUUID, sourceBucket, targetClusterUUID, targetBucket, filterName, settings)
		if err != nil {
			logger_rm.Errorf("%v\n", err)
			return "", err
		}
		topic = spec.Id
	} else {
		topic = metadata.ReplicationId(sourceClusterUUID, sourceBucket, targetClusterUUID, targetBucket, filterName)
	}
	
	go StartPipeline(topic, settings)
	logger_rm.Infof("Pipeline %s is created and started\n", topic)

	return topic, nil
}

func PauseReplication(topic string, updateReplSpec bool, sync bool) error {
	defer func () {
		if r := recover(); r != nil {
			logger_rm.Errorf("PauseReplication on pipeline %v panic: %v\n", topic, r)
		}
	}()
	
	logger_rm.Infof("Pausing replication %s\n", topic)
	
	if updateReplSpec {
		// update replication spec
		if err := validatePipelineExists(topic, "pausing", true); err != nil {
			return err
		}
		
		err := UpdateReplicationSpec(topic, false, "pause")
		if err == nil {
			logger_rm.Debugf("Replication specification %s is updated\n", topic)
		} else {
			logger_rm.Errorf("%v\n", err)
			return err
		}
	}

	if sync {
		err := StopPipeline(topic)
		logger_rm.Infof("Pipeline %s has been paused\n", topic)
		return err
	} else {
		go StopPipeline(topic)
		logger_rm.Infof("Pipeline %s is being paused\n", topic)
		return nil
	}
}

func ResumeReplication(topic string, updateReplSpec bool, sync bool) error {
	logger_rm.Infof("Resuming replication %s\n", topic)
	
	if updateReplSpec {
		// update replication spec
		if err := validatePipelineExists(topic, "resuming", true); err != nil {
			return err
		}
		
		err := UpdateReplicationSpec(topic, true, "resume")
		if err == nil {
			logger_rm.Debugf("Replication specification %s is updated\n", topic)
		} else {
			logger_rm.Errorf("%v\n", err)
			return err
		}
	}

	spec, err := ReplicationSpecService().ReplicationSpec(topic)
	if err != nil {
		return err
	}
	
	settings := spec.Settings
	settingsMap := settings.ToMap()
	if sync {
		err := StartPipeline(topic, settingsMap)
		logger_rm.Infof("Pipeline %s has been resumed\n", topic)
		return err
	} else {
		go StartPipeline(topic, settingsMap)
		logger_rm.Infof("Pipeline %s is being resumed\n", topic)
		return nil
	}
}

func DeleteReplication(topic string, deleteReplSpec bool) error {
	logger_rm.Infof("Deleting replication %s\n", topic)
	
	if deleteReplSpec {
		// delete replication spec
		if err := validatePipelineExists(topic, "deleting", true); err != nil {
			return err
		}

		err := ReplicationSpecService().DelReplicationSpec(topic)
		if err == nil {
			logger_rm.Debugf("Replication specification %s is deleted\n", topic)
		} else {
			logger_rm.Errorf("%v\n", err)
			return err
		}
	}
	
	go StopPipeline(topic)

	logger_rm.Infof("Pipeline %s is deleted\n", topic)

	return nil
}

func StartPipeline(topic string, settings map[string]interface{}) error {
	pipeline, err := pipeline_manager.StartPipeline(topic, settings)
	if err == nil {
		//add pipeline supervisor to the children of replication manager supervisor
		replication_mgr.GenericSupervisor.AddChild(pipeline.RuntimeContext().Service(base.PIPELINE_SUPERVISOR_SVC).(*pipeline_svc.PipelineSupervisor))
	}
	return nil
}

func StopPipeline(topic string) error {
	//remove pipeline supervisor from the children of replication manager supervisor
	pipeline := pipeline_manager.Pipeline(topic)
	if pipeline != nil {
		replication_mgr.GenericSupervisor.RemoveChild(pipeline.RuntimeContext().Service(base.PIPELINE_SUPERVISOR_SVC).(*pipeline_svc.PipelineSupervisor).Id())
	}
		
	return pipeline_manager.StopPipeline(topic)
}

func HandleChangesToReplicationSettings(topic string, settings map[string]interface{}) error {
	// read replication spec with the specified replication id
	replSpec, err := ReplicationSpecService().ReplicationSpec(topic)
	if err != nil {
		return err
	}

	// update replication spec with input settings
	replSpec.Settings.UpdateSettingsFromMap(settings)
	err = ReplicationSpecService().SetReplicationSpec(replSpec)
	
	// TODO implement additional logic, e.g.,
	// 1. reconstruct pipeline when source/targetNozzlePerNode is changed
	// 2. pause pipeline when active is changed from true to false
	// 3. restart pipeline when criteral settings are changed
	return err
}

// get statistics for all running replications
func GetStatistics() (map[string]interface{}, error) {
	// TODO implement
	return nil, nil
}

func (rm *replicationManager) createAndPersistReplicationSpec(sourceClusterUUID, sourceBucket, targetClusterUUID, targetBucket, filterName string, settings map[string]interface{}) (*metadata.ReplicationSpecification, error) {
	logger_rm.Infof("Creating replication spec - sourceCluterUUID=%s, sourceBucket=%s, targetClusterUUID=%s, targetBucket=%s, filterName=%s, settings=%v\n", sourceClusterUUID,
		sourceBucket, targetClusterUUID, targetBucket, filterName, settings)
		
	// check if the same replication already exists
	replicationId := metadata.ReplicationId(sourceClusterUUID, sourceBucket, targetClusterUUID, targetBucket, filterName)
	if err := validatePipelineExists(replicationId, "starting", false); err != nil {
		return nil, err
	}
	
	spec := metadata.NewReplicationSpecification(sourceClusterUUID, sourceBucket, targetClusterUUID, targetBucket, filterName)
	s, err := metadata.SettingsFromMap(settings)
	if err == nil {
		spec.Settings = s

		//persist it
		replication_mgr.repl_spec_svc.AddReplicationSpec(spec)
		logger_rm.Debugf("replication specification %s is created and persisted\n", spec.Id)
		return spec, nil
	} else {
		return nil, err
	}
}

//update the replication specification's "active" setting
func UpdateReplicationSpec(topic string, active bool, action string) error {	
	spec, err := replication_mgr.repl_spec_svc.ReplicationSpec(topic)
	if err != nil {
		logger_rm.Errorf("%v\n", err)
		return err
	}

	settings := spec.Settings
	if settings.Active == active {
		state := "not"
		if active {
			state = "already"
		}
		return errors.New(fmt.Sprintf("Invalid operation. Cannot %v replication with id, %v, since it is %v actively running.\n", action, topic, state))
	}
	settings.Active = active
	err = replication_mgr.repl_spec_svc.SetReplicationSpec(spec)
	logger_rm.Debugf("Replication specification %s is set to active=%v\n", topic, active)
	return err
}

func (rm *replicationManager) OnError(s common.Supervisor, errors map[string]error) {
	logger_rm.Infof("Supervisor %v of type %v reported errors %v\n", s.Id(), reflect.TypeOf(s), errors)
	
	if s.Id() == base.ReplicationManagerSupervisorId {
		// the errors came from the replication manager supervisor because adminport or pipeline master supervisor is not longer alive.	
		// there is nothing we can do except to abort xdcr. ns_server will restart xdcr while later
		stop()
	} else if s.Id() == base.PipelineMasterSupervisorId {
		// the errors came from the pipeline master supervisor because some pipeline supervisors are not longer alive.	
		for childId, _ := range errors {
			child, _ := s.Child(childId)
			// child could be null if the pipeline has been stopped so far. //TODO should we restart it here?
			if child != nil {
				pipeline, err := getPipelineFromPipelineSupevisor(child.(common.Supervisor))
				if err == nil {
					// try to fix the pipeline
					go fixPipeline(pipeline)
				}
			}
		}
	} else {
		// the errors came from a pipeline supervisor because some parts are not longer alive.
			pipeline, err := getPipelineFromPipelineSupevisor(s)
			if err == nil {
				// try to fix the pipeline
				go fixPipeline(pipeline)
			}
	}
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
func (rm *replicationManager) startReplications() {
	logger_rm.Infof("Replication manager init - starting existing replications")
	
	specs, err := replication_mgr.repl_spec_svc.ActiveReplicationSpecs()
	if err != nil {
		logger_rm.Errorf("Error retrieving active replication specs")
		return
	}
	
	for _, spec := range specs {
		go StartPipeline(spec.Id, spec.Settings.ToMap())
	}
}

func fixPipeline(pipeline common.Pipeline) error {
	if checkPipelineOnFile(pipeline) {
		//pause and then resume the replication without updating the replication spec
		err := PauseReplication(pipeline.Topic(), false/*updateReplSpec*/, true/*sync*/)
		if err == nil {
			err = ResumeReplication(pipeline.Topic(), false/*updateReplSpec*/, true/*sync*/)
		}
		if err == nil {
			logger_rm.Infof("Pipeline %v is fixed, back to business\n", pipeline.Topic())
		}else {
			logger_rm.Errorf("Failed to fix pipeline %v, err=%v\n", pipeline.Topic(), err)
			//panic("Failed to fix pipeline")
				//TODO: propagate the error to ns_server

				//TODO: schedule the retry
		}
		return err
	} 
	return nil
}

// check if a specified pipeline is on file
func checkPipelineOnFile(pipeline common.Pipeline) bool {
	var isOnFile = (pipeline_manager.Pipeline(pipeline.Topic()) == pipeline)
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

	// exit main routine
	os.Exit(0)
}
