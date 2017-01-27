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
	"bytes"
	"errors"
	"fmt"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/simple_utils"
	"github.com/couchbase/goxdcr/supervisor"
	"github.com/couchbase/goxdcr/utils"
	"reflect"
	"sync"
	"time"
)

//configuration settings
const (
	PIPELINE_LOG_LEVEL         = "pipeline_loglevel"
	default_pipeline_log_level = log.LogLevelInfo
)

const (
	CMD_CHANGE_LOG_LEVEL int = 2
)

const (
	health_check_interval      = 120 * time.Second
	default_max_dcp_miss_count = 3
	// memcached client will be reset if it encounters consecutive errors
	max_mem_client_error_count = 3
)

var pipeline_supervisor_setting_defs base.SettingDefinitions = base.SettingDefinitions{supervisor.HEARTBEAT_TIMEOUT: base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	PIPELINE_LOG_LEVEL:            base.NewSettingDef(reflect.TypeOf((*log.LogLevel)(nil)), false),
	supervisor.HEARTBEAT_INTERVAL: base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false)}

type PipelineSupervisor struct {
	*supervisor.GenericSupervisor
	pipeline         common.Pipeline
	errors_seen      map[string]error
	errors_seen_lock *sync.RWMutex

	cluster_info_svc  service_def.ClusterInfoSvc
	xdcr_topology_svc service_def.XDCRCompTopologySvc

	// memcached clients for dcp health check
	kv_mem_clients map[string]*mcc.Client
	// stores error count of memcached clients
	kv_mem_client_error_count map[string]int
	kv_mem_clients_lock       *sync.Mutex

	user_agent string
}

func NewPipelineSupervisor(id string, logger_ctx *log.LoggerContext, failure_handler common.SupervisorFailureHandler,
	parentSupervisor *supervisor.GenericSupervisor, cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc) *PipelineSupervisor {
	supervisor := supervisor.NewGenericSupervisor(id, logger_ctx, failure_handler, parentSupervisor)
	pipelineSupervisor := &PipelineSupervisor{GenericSupervisor: supervisor,
		errors_seen:               make(map[string]error),
		errors_seen_lock:          &sync.RWMutex{},
		cluster_info_svc:          cluster_info_svc,
		xdcr_topology_svc:         xdcr_topology_svc,
		kv_mem_clients:            make(map[string]*mcc.Client),
		kv_mem_client_error_count: make(map[string]int),
		kv_mem_clients_lock:       &sync.Mutex{}}
	return pipelineSupervisor
}

func (pipelineSupervisor *PipelineSupervisor) Pipeline() common.Pipeline {
	return pipelineSupervisor.pipeline
}

func (pipelineSupervisor *PipelineSupervisor) Attach(p common.Pipeline) error {
	pipelineSupervisor.Logger().Infof("Attaching pipeline %v to supervior service %v\n", p.InstanceId(), pipelineSupervisor.Id())

	pipelineSupervisor.pipeline = p

	pipelineSupervisor.composeUserAgent()

	partsMap := pipeline.GetAllParts(p)

	for _, part := range partsMap {
		// the assumption here is that all XDCR parts are Supervisable
		pipelineSupervisor.AddChild(part.(common.Supervisable))

		//register itself with all parts' ErrorEncountered event
		part.RegisterComponentEventListener(common.ErrorEncountered, pipelineSupervisor)
		part.RegisterComponentEventListener(common.VBErrorEncountered, pipelineSupervisor)
		pipelineSupervisor.Logger().Debugf("Registering ErrorEncountered event on part %v\n", part.Id())
	}

	//register itself with all connectors' ErrorEncountered event
	connectorsMap := pipeline.GetAllConnectors(p)

	for _, connector := range connectorsMap {
		connector.RegisterComponentEventListener(common.ErrorEncountered, pipelineSupervisor)
		connector.RegisterComponentEventListener(common.VBErrorEncountered, pipelineSupervisor)
		pipelineSupervisor.Logger().Debugf("Registering ErrorEncountered event on connector %v\n", connector.Id())
	}

	return nil
}

func (pipelineSupervisor *PipelineSupervisor) Start(settings map[string]interface{}) error {
	// when doing health check, we want to wait long enough to ensure that we see bad stats in at least two different stats collection intervals
	// before we declare the pipeline to be broken
	var max_dcp_miss_count int
	stats_update_interval := StatsUpdateInterval(settings)
	number_of_waits_to_ensure_stats_update := int(stats_update_interval.Nanoseconds()/health_check_interval.Nanoseconds()) + 1
	if number_of_waits_to_ensure_stats_update < default_max_dcp_miss_count {
		max_dcp_miss_count = default_max_dcp_miss_count
	} else {
		max_dcp_miss_count = number_of_waits_to_ensure_stats_update
	}

	for _, dcp_nozzle := range pipelineSupervisor.pipeline.Sources() {
		dcp_nozzle.(*parts.DcpNozzle).SetMaxMissCount(max_dcp_miss_count)
	}

	// do the generic supervisor start stuff
	err := pipelineSupervisor.GenericSupervisor.Start(settings)
	if err != nil {
		return err
	}

	// start an additional go routine for pipeline health monitoring
	pipelineSupervisor.GenericSupervisor.ChidrenWaitGroup().Add(1)
	go pipelineSupervisor.monitorPipelineHealth()

	return err
}

func (pipelineSupervisor *PipelineSupervisor) Stop() error {
	// do the generic supervisor stop stuff
	err := pipelineSupervisor.GenericSupervisor.Stop()
	if err != nil {
		return err
	}

	//close the connections
	pipelineSupervisor.closeConnections()

	return nil
}

func (pipelineSupervisor *PipelineSupervisor) closeConnections() {
	pipelineSupervisor.kv_mem_clients_lock.Lock()
	defer pipelineSupervisor.kv_mem_clients_lock.Unlock()
	for serverAddr, client := range pipelineSupervisor.kv_mem_clients {
		err := client.Close()
		if err != nil {
			pipelineSupervisor.Logger().Infof("error from closing connection for %v is %v\n", serverAddr, err)
		}
	}
	pipelineSupervisor.kv_mem_clients = make(map[string]*mcc.Client)
}

func (pipelineSupervisor *PipelineSupervisor) monitorPipelineHealth() error {
	pipelineSupervisor.Logger().Info("monitorPipelineHealth started")

	defer pipelineSupervisor.GenericSupervisor.ChidrenWaitGroup().Done()

	health_check_ticker := time.NewTicker(health_check_interval)
	defer health_check_ticker.Stop()

	fin_ch := pipelineSupervisor.GenericSupervisor.FinishChannel()

	for {
		select {
		case <-fin_ch:
			pipelineSupervisor.Logger().Infof("monitorPipelineHealth routine is exiting because parent supervisor %v has been stopped\n", pipelineSupervisor.Id())
			return nil
		case <-health_check_ticker.C:
			err := simple_utils.ExecWithTimeout(pipelineSupervisor.checkPipelineHealth, 1000*time.Millisecond, pipelineSupervisor.Logger())
			if err != nil {
				if _, ok := err.(*simple_utils.ExecutionTimeoutError); ok {
					// ignore timeout error and continue
					pipelineSupervisor.Logger().Infof("Received timeout error when checking pipeline health. topic=%v\n", pipelineSupervisor.pipeline.Topic())
				} else {
					return err
				}
			}
		}
	}
	return nil
}

func (pipelineSupervisor *PipelineSupervisor) OnEvent(event *common.Event) {
	if event.EventType == common.ErrorEncountered {
		if pipelineSupervisor.pipeline.State() != common.Pipeline_Error {
			if event.OtherInfos != nil {
				pipelineSupervisor.setError(event.Component.Id(), event.OtherInfos.(error))
			}
			pipelineSupervisor.declarePipelineBroken()
		} else {
			pipelineSupervisor.errors_seen_lock.RLock()
			defer pipelineSupervisor.errors_seen_lock.RUnlock()
			pipelineSupervisor.Logger().Infof("%v Received error report : %v, but error is ignored. pipeline_state=%v\n", pipelineSupervisor.pipeline.Topic(), pipelineSupervisor.errors_seen, pipelineSupervisor.pipeline.State())

		}

	} else if event.EventType == common.VBErrorEncountered {
		additionalInfo := event.OtherInfos.(*base.VBErrorEventAdditional)
		vbno := additionalInfo.Vbno
		err := additionalInfo.Error
		errType := additionalInfo.ErrorType
		pipelineSupervisor.Logger().Debugf("%v Received error report on vb %v. err=%v, err type=%v\n", pipelineSupervisor.pipeline.Topic(), vbno, err, errType)
		settings := make(map[string]interface{})
		if errType == base.VBErrorType_Source {
			settings[base.ProblematicVBSource] = map[uint16]error{vbno: err}
		} else {
			settings[base.ProblematicVBTarget] = map[uint16]error{vbno: err}
		}

		// ignore vb errors and just mark the vbs as problematic for now.
		// at the next topology check time, we will decide whether the problematic vbs are caused by topology
		// changes and will restart pipeline if they are not
		pipelineSupervisor.pipeline.UpdateSettings(settings)
	} else {
		pipelineSupervisor.Logger().Errorf("Pipeline supervisor didn't register to recieve event %v for component %v", event.EventType, event.Component.Id())
	}
}

func (pipelineSupervisor *PipelineSupervisor) init(settings map[string]interface{}) error {
	//initialize settings
	err := utils.ValidateSettings(pipeline_supervisor_setting_defs, settings, pipelineSupervisor.Logger())
	if err != nil {
		pipelineSupervisor.Logger().Errorf("The setting for Pipeline supervisor %v is not valid. err=%v", pipelineSupervisor.Id(), err)
		return err
	}

	pipelineSupervisor.Init(settings)

	if val, ok := settings[PIPELINE_LOG_LEVEL]; ok {
		pipelineSupervisor.LoggerContext().SetLogLevel(val.(log.LogLevel))
	}

	return nil
}

func (pipelineSupervisor *PipelineSupervisor) UpdateSettings(settings map[string]interface{}) error {
	pipelineSupervisor.Logger().Debugf("Updating settings on pipelineSupervisor %v. settings=%v\n", pipelineSupervisor.Id(), settings)
	logLevelObj := utils.GetSettingFromSettings(settings, PIPELINE_LOG_LEVEL)

	if logLevelObj == nil {
		// logLevel not specified. no op
		return nil
	}

	logLevel, ok := logLevelObj.(log.LogLevel)
	if !ok {
		return fmt.Errorf("Log level %v is of wrong type", logLevelObj)
	}
	pipelineSupervisor.LoggerContext().SetLogLevel(logLevel)
	return nil
}

func (pipelineSupervisor *PipelineSupervisor) ReportFailure(errors map[string]error) {
	pipelineSupervisor.StopHeartBeatTicker()
	//report the failure to decision maker
	pipelineSupervisor.GenericSupervisor.ReportFailure(errors)
}

func (pipelineSupervisor *PipelineSupervisor) declarePipelineBroken() {
	pipelineSupervisor.errors_seen_lock.RLock()
	defer pipelineSupervisor.errors_seen_lock.RUnlock()
	err := pipelineSupervisor.pipeline.SetState(common.Pipeline_Error)
	if err == nil {
		pipelineSupervisor.Logger().Errorf("Received error report : %v", pipelineSupervisor.errors_seen)
		pipelineSupervisor.ReportFailure(pipelineSupervisor.errors_seen)
		pipelineSupervisor.pipeline.ReportProgress(fmt.Sprintf("Received error report : %v", pipelineSupervisor.errors_seen))

	} else {
		pipelineSupervisor.Logger().Infof("Received error report : %v, but error is ignored. pipeline_state=%v\n", pipelineSupervisor.errors_seen, pipelineSupervisor.pipeline.State())

	}
}

// check if any runtime stats indicates that pipeline is broken
func (pipelineSupervisor *PipelineSupervisor) checkPipelineHealth() error {
	if !pipeline_utils.IsPipelineRunning(pipelineSupervisor.pipeline.State()) {
		//the pipeline is no longer running, kill myself
		message := "Pipeline is no longer running, exit."
		pipelineSupervisor.Logger().Info(message)
		return errors.New(message)
	}

	dcp_stats, err := pipelineSupervisor.getDcpStats()
	if err != nil {
		pipelineSupervisor.Logger().Error("Failed to get dcp stats. Skipping dcp health check.")
		return nil
	}

	for _, dcp_nozzle := range pipelineSupervisor.pipeline.Sources() {
		err = dcp_nozzle.(*parts.DcpNozzle).CheckStuckness(dcp_stats)
		if err != nil {
			//declare pipeline broken
			pipelineSupervisor.setError(dcp_nozzle.Id(), err)
			pipelineSupervisor.declarePipelineBroken()
			return err
		}
	}

	return nil
}

// compose user agent string for HELO command
func (pipelineSupervisor *PipelineSupervisor) composeUserAgent() {
	var buffer bytes.Buffer
	buffer.WriteString("Goxdcr PipelineSupervisor ")
	spec := pipelineSupervisor.pipeline.Specification()
	buffer.WriteString(" SourceBucket:" + spec.SourceBucketName)
	buffer.WriteString(" TargetBucket:" + spec.TargetBucketName)
	pipelineSupervisor.user_agent = buffer.String()
}

func (pipelineSupervisor *PipelineSupervisor) getDcpStats() (map[string]map[string]string, error) {
	// need to lock since it is possible, even though unlikely, that getDcpStats() may be called multiple times at the same time
	pipelineSupervisor.kv_mem_clients_lock.Lock()
	defer pipelineSupervisor.kv_mem_clients_lock.Unlock()

	dcp_stats := make(map[string]map[string]string)

	bucketName := pipelineSupervisor.pipeline.Specification().SourceBucketName
	nodes, err := pipelineSupervisor.xdcr_topology_svc.MyKVNodes()
	if err != nil {
		pipelineSupervisor.Logger().Errorf("Error retrieving kv nodes for pipeline %v. Skipping dcp stats check. err=%v", pipelineSupervisor.pipeline.Topic(), err)
		return nil, err
	}

	for _, serverAddr := range nodes {
		client, err := utils.GetMemcachedClient(serverAddr, bucketName, pipelineSupervisor.kv_mem_clients, pipelineSupervisor.user_agent, pipelineSupervisor.Logger())
		if err != nil {
			return nil, err
		}

		stats_map, err := client.StatsMap(base.DCP_STAT_NAME)
		if err != nil {
			pipelineSupervisor.Logger().Infof("Error getting dcp stats for kv %v. err=%v", serverAddr, err)
			// increment the error count of the client. retire the client if it has failed too many times
			err_count, ok := pipelineSupervisor.kv_mem_client_error_count[serverAddr]
			if !ok {
				err_count = 1
			} else {
				err_count++
			}
			if err_count > max_mem_client_error_count {
				err = client.Close()
				if err != nil {
					pipelineSupervisor.Logger().Infof("error from closing connection for %v is %v\n", serverAddr, err)
				}
				delete(pipelineSupervisor.kv_mem_clients, serverAddr)
				pipelineSupervisor.kv_mem_client_error_count[serverAddr] = 0
			} else {
				pipelineSupervisor.kv_mem_client_error_count[serverAddr] = err_count
			}
			return nil, err
		} else {
			dcp_stats[serverAddr] = stats_map
		}
	}

	return dcp_stats, nil
}

func (pipelineSupervisor *PipelineSupervisor) setError(partId string, err error) {
	pipelineSupervisor.errors_seen_lock.Lock()
	defer pipelineSupervisor.errors_seen_lock.Unlock()
	pipelineSupervisor.errors_seen[partId] = err
}
