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
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	generic_p "github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/supervisor"
	"github.com/couchbase/goxdcr/utils"
	"reflect"
	//	"sync"
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

var pipeline_supervisor_setting_defs base.SettingDefinitions = base.SettingDefinitions{supervisor.HEARTBEAT_TIMEOUT: base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	PIPELINE_LOG_LEVEL:            base.NewSettingDef(reflect.TypeOf((*log.LogLevel)(nil)), false),
	supervisor.HEARTBEAT_INTERVAL: base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false)}

type PipelineSupervisor struct {
	*supervisor.GenericSupervisor
	pipeline    common.Pipeline
	errors_seen map[string]error
}

func NewPipelineSupervisor(id string, logger_ctx *log.LoggerContext, failure_handler common.SupervisorFailureHandler, parentSupervisor *supervisor.GenericSupervisor) *PipelineSupervisor {
	supervisor := supervisor.NewGenericSupervisor(id, logger_ctx, failure_handler, parentSupervisor)
	pipelineSupervisor := &PipelineSupervisor{GenericSupervisor: supervisor,
		errors_seen: make(map[string]error)}
	return pipelineSupervisor
}

func (pipelineSupervisor *PipelineSupervisor) Pipeline() common.Pipeline {
	return pipelineSupervisor.pipeline
}

func (pipelineSupervisor *PipelineSupervisor) Attach(p common.Pipeline) error {
	pipelineSupervisor.Logger().Infof("Attaching pipeline %v to supervior service %v\n", p.InstanceId(), pipelineSupervisor.Id())

	pipelineSupervisor.pipeline = p

	partsMap := generic_p.GetAllParts(p)

	for _, part := range partsMap {
		// the assumption here is that all XDCR parts are Supervisable
		pipelineSupervisor.AddChild(part.(common.Supervisable))

		//register itself with all parts' ErrorEncountered event
		part.RegisterComponentEventListener(common.ErrorEncountered, pipelineSupervisor)
		pipelineSupervisor.Logger().Infof("Registering ErrorEncountered event on part %v\n", part.Id())
	}

	//register itself with all connectors' ErrorEncountered event
	connectorsMap := generic_p.GetAllConnectors(p)

	for _, connector := range connectorsMap {
		connector.RegisterComponentEventListener(common.ErrorEncountered, pipelineSupervisor)
		pipelineSupervisor.Logger().Infof("Registering ErrorEncountered event on connector %v\n", connector.Id())
	}

	return nil
}

func (pipelineSupervisor *PipelineSupervisor) OnEvent(eventType common.ComponentEventType,
	item interface{},
	component common.Component,
	derivedItems []interface{},
	otherInfos map[string]interface{}) {
	if eventType == common.ErrorEncountered {
		pipelineSupervisor.errors_seen[component.Id()] = otherInfos["error"].(error)
		pipelineSupervisor.declarePipelineBroken()

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

func (pipelineSupervisor *PipelineSupervisor) SetPipelineLogLevel(log_level_str string) error {
	level, err := log.LogLevelFromStr(log_level_str)
	if err != nil {
		return err
	}
	pipelineSupervisor.LoggerContext().Log_level = level
	return nil
}

func (pipelineSupervisor *PipelineSupervisor) ReportFailure(errors map[string]error) {
	pipelineSupervisor.StopHeartBeatTicker()
	//report the failure to decision maker
	pipelineSupervisor.GenericSupervisor.ReportFailure(errors)
}

func (pipelineSupervisor *PipelineSupervisor) declarePipelineBroken() {
	pipelineSupervisor.Logger().Errorf("Received error report : %v, declare pipeline broken", pipelineSupervisor.errors_seen)
	err := pipelineSupervisor.pipeline.SetState(common.Pipeline_Error)
	if err == nil {
		pipelineSupervisor.ReportFailure(pipelineSupervisor.errors_seen)

	}
}
