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
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/supervisor"
	"github.com/couchbase/goxdcr/utils"
	"github.com/couchbase/goxdcr/parts"
	"reflect"
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
	default_health_check_interval = 15 * time.Second
	default_max_dcp_miss_count    = 3
)

var pipeline_supervisor_setting_defs base.SettingDefinitions = base.SettingDefinitions{supervisor.HEARTBEAT_TIMEOUT: base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	PIPELINE_LOG_LEVEL:            base.NewSettingDef(reflect.TypeOf((*log.LogLevel)(nil)), false),
	supervisor.HEARTBEAT_INTERVAL: base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false)}

type PipelineSupervisor struct {
	*supervisor.GenericSupervisor
	pipeline    common.Pipeline
	errors_seen map[string]error

	cluster_info_svc  service_def.ClusterInfoSvc
	xdcr_topology_svc service_def.XDCRCompTopologySvc
}

func NewPipelineSupervisor(id string, logger_ctx *log.LoggerContext, failure_handler common.SupervisorFailureHandler,
	parentSupervisor *supervisor.GenericSupervisor, cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc) *PipelineSupervisor {
	supervisor := supervisor.NewGenericSupervisor(id, logger_ctx, failure_handler, parentSupervisor)
	pipelineSupervisor := &PipelineSupervisor{GenericSupervisor: supervisor,
		errors_seen:       make(map[string]error),
		cluster_info_svc:  cluster_info_svc,
		xdcr_topology_svc: xdcr_topology_svc}
	return pipelineSupervisor
}

func (pipelineSupervisor *PipelineSupervisor) Pipeline() common.Pipeline {
	return pipelineSupervisor.pipeline
}

func (pipelineSupervisor *PipelineSupervisor) Attach(p common.Pipeline) error {
	pipelineSupervisor.Logger().Infof("Attaching pipeline %v to supervior service %v\n", p.InstanceId(), pipelineSupervisor.Id())

	pipelineSupervisor.pipeline = p

	partsMap := pipeline.GetAllParts(p)

	for _, part := range partsMap {
		// the assumption here is that all XDCR parts are Supervisable
		pipelineSupervisor.AddChild(part.(common.Supervisable))

		//register itself with all parts' ErrorEncountered event
		part.RegisterComponentEventListener(common.ErrorEncountered, pipelineSupervisor)
		pipelineSupervisor.Logger().Debugf("Registering ErrorEncountered event on part %v\n", part.Id())
	}

	//register itself with all connectors' ErrorEncountered event
	connectorsMap := pipeline.GetAllConnectors(p)

	for _, connector := range connectorsMap {
		connector.RegisterComponentEventListener(common.ErrorEncountered, pipelineSupervisor)
		pipelineSupervisor.Logger().Debugf("Registering ErrorEncountered event on connector %v\n", connector.Id())
	}

	return nil
}

func (pipelineSupervisor *PipelineSupervisor) Start(settings map[string]interface{}) error {
	// when doing health check, we want to wait long enough to ensure that we see bad stats in at least two different stats collection intervals
	// before we declare the pipeline to be broken
	var max_dcp_miss_count int
	stats_update_interval := StatsUpdateInterval(settings)
	number_of_waits_to_ensure_stats_update := int(stats_update_interval.Nanoseconds()/default_health_check_interval.Nanoseconds()) + 1
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

func (pipelineSupervisor *PipelineSupervisor) monitorPipelineHealth() error {
	pipelineSupervisor.Logger().Info("monitorPipelineHealth started")

	defer pipelineSupervisor.GenericSupervisor.ChidrenWaitGroup().Done()

	health_check_ticker := time.NewTicker(default_health_check_interval)

	fin_ch := pipelineSupervisor.GenericSupervisor.FinishChannel()

	for {
		select {
		case <-fin_ch:
			pipelineSupervisor.Logger().Infof("monitorPipelineHealth routine is exiting because parent supervisor %v has been stopped\n", pipelineSupervisor.Id())
			return nil
		case <-health_check_ticker.C:
			err := pipelineSupervisor.checkPipelineHealth()
			if err != nil {
				return nil
			}
		}
	}
	return nil
}

func (pipelineSupervisor *PipelineSupervisor) OnEvent(eventType common.ComponentEventType,
	item interface{},
	component common.Component,
	derivedItems []interface{},
	otherInfos map[string]interface{}) {
	if eventType == common.ErrorEncountered {
		if pipelineSupervisor.pipeline.State() != common.Pipeline_Error {
			if otherInfos["error"] != nil {
				pipelineSupervisor.errors_seen[component.Id()] = otherInfos["error"].(error)
			}
			pipelineSupervisor.declarePipelineBroken()
		} else {
			pipelineSupervisor.Logger().Infof("Received error report : %v, but error is ignored. pipeline_state=%v\n", pipelineSupervisor.errors_seen, pipelineSupervisor.pipeline.State())

		}

	} else {
		pipelineSupervisor.Logger().Errorf("Pipeline supervisor didn't register to recieve event %v for component %v", eventType, component.Id())
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
		pipelineSupervisor.LoggerContext().Log_level = val.(log.LogLevel)
	}

	return nil
}

func (pipelineSupervisor *PipelineSupervisor) UpdateSettings(settings map[string]interface{}) error {
	pipelineSupervisor.Logger().Debugf("Updating settings on pipelineSupervisor %v. settings=%v\n", pipelineSupervisor.Id(), settings)
	logLevelObj := utils.GetSettingFromSettings(settings, PIPELINE_LOG_LEVEL)

	if logLevelObj == 0 {
		// logLevel not specified. no op
		return nil
	}

	logLevel, ok := logLevelObj.(log.LogLevel)
	if !ok {
		return fmt.Errorf("Log level %v is of wrong type", logLevelObj)
	}
	pipelineSupervisor.LoggerContext().Log_level = logLevel
	return nil
}

func (pipelineSupervisor *PipelineSupervisor) ReportFailure(errors map[string]error) {
	pipelineSupervisor.StopHeartBeatTicker()
	//report the failure to decision maker
	pipelineSupervisor.GenericSupervisor.ReportFailure(errors)
}

func (pipelineSupervisor *PipelineSupervisor) declarePipelineBroken() {
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
	if pipelineSupervisor.pipeline.State() != common.Pipeline_Running && pipelineSupervisor.pipeline.State() != common.Pipeline_Starting {
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
		err = dcp_nozzle.(*parts.DcpNozzle).CheckDcpHealth(dcp_stats)
		if err != nil {
			//declare pipeline broken
			pipelineSupervisor.errors_seen[dcp_nozzle.Id()] = err
			pipelineSupervisor.declarePipelineBroken()
			return err
		}
	}

	return nil
}

func (pipelineSupervisor *PipelineSupervisor) getDcpStats() (map[string]map[string]string, error) {
	bucket_name := pipelineSupervisor.pipeline.Specification().SourceBucketName
	bucket, err := pipelineSupervisor.cluster_info_svc.GetBucket(pipelineSupervisor.xdcr_topology_svc, bucket_name)
	if err != nil {
		return nil, err
	}
	defer bucket.Close()

	return bucket.GetStats(base.DCP_STAT_NAME), nil
}
