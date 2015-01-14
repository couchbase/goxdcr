// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package pipeline

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	log "github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/utils"
	"sync"
	"time"
)

//the function can construct part specific settings for the pipeline
type PartsSettingsConstructor func(pipeline common.Pipeline, part common.Part, pipeline_settings map[string]interface{}, ts map[uint16]*base.VBTimestamp) (map[string]interface{}, error)

type StartingSeqnoConstructor func(pipeline common.Pipeline) (map[uint16]*base.VBTimestamp, error)

//GenericPipeline is the generic implementation of a data processing pipeline
//
//The assumption here is all the processing steps are self-connected, so
//once incoming nozzle is open, it will propel the data through all
//processing steps.
type GenericPipeline struct {

	//name of the pipeline
	topic string

	//incoming nozzles of the pipeline
	sources map[string]common.Nozzle

	//outgoing nozzles of the pipeline
	targets map[string]common.Nozzle

	//runtime context of the pipeline
	context common.PipelineRuntimeContext

	//	//communication channel with PipelineManager
	//	reqch chan []interface{}

	//the lock to serialize the request to start\stop the pipeline
	stateLock sync.Mutex

	partSetting_constructor PartsSettingsConstructor

	startingSeqno_constructor StartingSeqnoConstructor

	//the map that contains the references to all parts used in the pipeline
	//it only populated when GetAllParts called the first time
	partsMap map[string]common.Part

	//the map that contains the references to all connectors used in the pipeline
	//it only populated when GetAllConnectors called the first time
	connectorsMap map[string]common.Connector

	logger *log.CommonLogger

	spec              *metadata.ReplicationSpecification
	settings_at_start map[string]interface{}

	state common.PipelineState

	instance_id int
}

//Get the runtime context of this pipeline
func (genericPipeline *GenericPipeline) RuntimeContext() common.PipelineRuntimeContext {
	return genericPipeline.context
}

func (genericPipeline *GenericPipeline) SetRuntimeContext(ctx common.PipelineRuntimeContext) {
	genericPipeline.context = ctx
}

func (genericPipeline *GenericPipeline) startPart(part common.Part, settings map[string]interface{}, ts map[uint16]*base.VBTimestamp) error {
	var err error = nil

	//start downstreams
	if part.Connector() != nil {
		downstreamParts := part.Connector().DownStreams()
		for _, p := range downstreamParts {
			err = genericPipeline.startPart(p, settings, ts)
			if err != nil {
				return err
			}
		}
	}

	partSettings := settings
	if genericPipeline.partSetting_constructor != nil {
		genericPipeline.logger.Debugf("Calling part setting constructor\n")
		partSettings, err = genericPipeline.partSetting_constructor(genericPipeline, part, settings, ts)
		if err != nil {
			return err
		}

	}

	if !part.IsStarted() {
		err = part.Start(partSettings)
	} else {
		genericPipeline.logger.Debugf("Part is already started\n")
	}

	return err
}

//Start starts the pipeline
//
//settings - a map of parameter to start the pipeline. it can contain initialization paramters
//			 for each processing steps and for runtime context of the pipeline.
func (genericPipeline *GenericPipeline) Start(settings map[string]interface{}) error {
	if genericPipeline.state != common.Pipeline_Stopped {
		return errors.New(fmt.Sprintf("Can't start the pipeline, the state is wrong. The current state is %v\n", genericPipeline.state))
	}

	genericPipeline.logger.Debugf("Try to start the pipeline with settings = %s", fmt.Sprint(settings))
	var err error

	genericPipeline.stateLock.Lock()
	defer genericPipeline.stateLock.Unlock()

	//get starting vb timestamp
	ts, err := genericPipeline.startingSeqno_constructor(genericPipeline)
	if err != nil {
		return err
	}

	settings["VBTimestamps"] = ts
	genericPipeline.logger.Debugf("Pipeline %v's starting seqno is %v\n", genericPipeline.InstanceId(), ts)

	//start all the processing steps of the Pipeline
	//start the incoming nozzle which would start the downstream steps
	//subsequently
	for _, source := range genericPipeline.sources {
		err = genericPipeline.startPart(source, settings, ts)
		if err != nil {
			return err
		}
		genericPipeline.logger.Debugf("Incoming nozzle %s is started", source.Id())
	}
	genericPipeline.logger.Info("All parts has been started")

	genericPipeline.logger.Debug("Try to start the runtime context")
	//start the runtime
	err = genericPipeline.context.Start(settings)
	if err != nil {
		return err
	}
	genericPipeline.logger.Debug("The runtime context is started")

	//open targets
	for _, target := range genericPipeline.targets {
		err = target.Open()
		if err != nil {
			genericPipeline.logger.Errorf("Failed to open outgoing nozzle %s", target.Id())
			return err
		}
	}
	genericPipeline.logger.Debug("All outgoing nozzles have been opened")

	//open source
	for _, source := range genericPipeline.sources {
		err = source.Open()
		if err != nil {
			genericPipeline.logger.Errorf("Failed to open incoming nozzle %s", source.Id())
			return err
		}
	}
	genericPipeline.logger.Debug("All incoming nozzles have been opened")

	genericPipeline.logger.Infof("-----------Pipeline %s is started----------", genericPipeline.InstanceId())

	genericPipeline.settings_at_start = settings

	genericPipeline.state = common.Pipeline_Running

	return err
}

func (genericPipeline *GenericPipeline) stopPart(part common.Part) error {
	var err error = nil
	genericPipeline.logger.Infof("Trying to stop part %v for %v\n", part.Id(), genericPipeline.InstanceId())
	if genericPipeline.canStop(part) {
		if !part.IsStarted() {
			genericPipeline.logger.Debugf("part %v is already stopped\n", part.Id())
			return nil
		}
		err = utils.ExecWithTimeout(part.Stop, 600*time.Millisecond, genericPipeline.logger)
		if err == nil {
			genericPipeline.logger.Infof("part %v is stopped\n", part.Id())
			if part.Connector() != nil {
				downstreamParts := part.Connector().DownStreams()
				for _, p := range downstreamParts {
					err = genericPipeline.stopPart(p)
					if err != nil {
						return err
					}
				}
			}
		} else {
			genericPipeline.logger.Errorf("Failed to stop part %v, err=%v\n", part.Id(), err)
		}
	}
	return err
}

//part can't be stopped if one of its upstreams is still running
func (genericPipeline *GenericPipeline) canStop(part common.Part) bool {
	parents := genericPipeline.findUpstreams(part)
	genericPipeline.logger.Debugf("part %v's parents=%v\n", part.Id(), parents)
	for _, parent := range parents {
		if parent.IsStarted() {
			genericPipeline.logger.Debugf("Part %v can't stop, parent %v is still running\n", part.Id(), parent.Id())
			return false
		}
	}
	genericPipeline.logger.Debugf("Part %v can stop\n", part.Id())
	return true
}

func (genericPipeline *GenericPipeline) findUpstreams(part common.Part) []common.Part {
	upstreams := []common.Part{}

	//searching down each source nozzle
	for _, source := range genericPipeline.sources {
		searchResults := genericPipeline.searchUpStreamsWithStartingPoint(part, source)
		if searchResults != nil && len(searchResults) > 0 {
			upstreams = append(upstreams, searchResults...)
		}
	}
	return upstreams
}

func (genericPipeline *GenericPipeline) searchUpStreamsWithStartingPoint(target_part common.Part, starting_part common.Part) []common.Part {
	upstreams := []common.Part{}

	if genericPipeline.isUpstreamTo(target_part, starting_part) {
		//add it to upstreams
		upstreams = append(upstreams, starting_part)
	} else {
		if starting_part.Connector() != nil {
			downstreams := starting_part.Connector().DownStreams()
			for _, downstream := range downstreams {
				result := genericPipeline.searchUpStreamsWithStartingPoint(target_part, downstream)
				if result != nil && len(result) > 0 {
					upstreams = append(upstreams, result...)
				}
			}
		}
	}
	return upstreams
}

func (genericPipeline *GenericPipeline) isUpstreamTo(target_part common.Part, part common.Part) bool {
	connector := part.Connector()
	if connector != nil {
		downstreams := connector.DownStreams()
		if downstreams != nil {
			for _, downstream := range downstreams {
				if downstream.Id() == target_part.Id() {
					return true
				}
			}
		}
	}
	return false
}

//Stop stops the pipeline
//it can result the pipeline in either "Stopped" if the operation is successful or "Pending" otherwise
func (genericPipeline *GenericPipeline) Stop() error {
	if genericPipeline.State() == common.Pipeline_Stopped {
		//pipeline is already stopped, no-op
		return nil
	}

	genericPipeline.stateLock.Lock()
	defer genericPipeline.stateLock.Unlock()

	//first move the pipeline state to pending to signal this pipeline is in shutting-down phase
	genericPipeline.state = common.Pipeline_Pending

	genericPipeline.logger.Infof("stoppping pipeline %v\n", genericPipeline.InstanceId())
	var err error

	// stop services before stopping parts to avoid spurious errors from services
	err = genericPipeline.context.Stop()
	if err != nil {
		return err
	}

	//close the sources
	for _, source := range genericPipeline.sources {
		err = source.Close()
		if err != nil {
			return err
		}
	}
	genericPipeline.logger.Debug("Incoming nozzles are closed, preparing to stop.")

	//stop the sources
	//source nozzle would notify the stop intention to its downsteam steps
	for _, source := range genericPipeline.sources {
		err = genericPipeline.stopPart(source)
		if err != nil {
			return err
		}
	}

	//stop runtime context only if all the processing steps in the pipeline
	//has been stopped.
	finchan := make(chan bool, 1)
	go genericPipeline.waitToStop(finchan)
	<-finchan

	genericPipeline.state = common.Pipeline_Stopped
	genericPipeline.logger.Infof("Pipeline %v is stopped\n", genericPipeline.InstanceId(), )
	return err

}

func (genericPipeline *GenericPipeline) Sources() map[string]common.Nozzle {
	return genericPipeline.sources
}

func (genericPipeline *GenericPipeline) Targets() map[string]common.Nozzle {
	return genericPipeline.targets
}

func (genericPipeline *GenericPipeline) Topic() string {
	return genericPipeline.topic
}

func (genericPipeline *GenericPipeline) waitToStop(finchan chan bool) {
	done := true
	for {
		for _, target := range genericPipeline.targets {
			if target.IsStarted() {
				genericPipeline.logger.Debugf("outgoing nozzle %s is still running", target.Id())
				done = false
			}
		}
		if done {
			break
		}
	}
	finchan <- true
}

func NewGenericPipeline(t string,
	sources map[string]common.Nozzle,
	targets map[string]common.Nozzle,
	spec *metadata.ReplicationSpecification) *GenericPipeline {
	pipeline := &GenericPipeline{topic: t,
		sources: sources,
		targets: targets,
		spec:    spec,
		logger:  log.NewLogger("GenericPipeline", nil),
		instance_id: time.Now().Nanosecond(),
		state:   common.Pipeline_Stopped}
	return pipeline
}

func NewPipelineWithSettingConstructor(t string,
	sources map[string]common.Nozzle,
	targets map[string]common.Nozzle,
	spec *metadata.ReplicationSpecification,
	partsSettingsConstructor PartsSettingsConstructor,
	startingSeqnoConstructor StartingSeqnoConstructor,
	logger_context *log.LoggerContext) *GenericPipeline {
	pipeline := &GenericPipeline{topic: t,
		sources: sources,
		targets: targets,
		spec:    spec,
		partSetting_constructor:   partsSettingsConstructor,
		startingSeqno_constructor: startingSeqnoConstructor,
		logger: log.NewLogger("GenericPipeline", logger_context),
		instance_id: time.Now().Nanosecond(),
		state:  common.Pipeline_Stopped}
	pipeline.logger.Debugf("Pipeline %s is initialized with a part setting constructor %v", t, partsSettingsConstructor)

	return pipeline
}

func GetAllParts(p common.Pipeline) map[string]common.Part {
	if p.(*GenericPipeline).partsMap == nil {
		p.(*GenericPipeline).partsMap = make(map[string]common.Part)
		sources := p.Sources()
		for _, source := range sources {
			addPartToMap(source, p.(*GenericPipeline).partsMap)
		}
	}
	return p.(*GenericPipeline).partsMap
}

func addPartToMap(part common.Part, partsMap map[string]common.Part) {
	if _, ok := partsMap[part.Id()]; !ok {
		// process the part if it has not been processed yet to avoid infinite loop
		partsMap[part.Id()] = part

		connector := part.Connector()
		if connector != nil {
			for _, downStreamPart := range connector.DownStreams() {
				addPartToMap(downStreamPart, partsMap)
			}
		}
	}
}

func GetAllConnectors(p common.Pipeline) map[string]common.Connector {
	if p.(*GenericPipeline).connectorsMap == nil {
		p.(*GenericPipeline).connectorsMap = make(map[string]common.Connector)
		sources := p.Sources()
		for _, source := range sources {
			connector := source.Connector()
			if connector != nil {
				addConnectorToMap(connector, p.(*GenericPipeline).connectorsMap)
			}
		}
	}
	return p.(*GenericPipeline).connectorsMap
}

func addConnectorToMap(connector common.Connector, connectorsMap map[string]common.Connector) {
	if _, ok := connectorsMap[connector.Id()]; !ok {
		// process the connector if it has not been processed yet to avoid infinite loop
		connectorsMap[connector.Id()] = connector

		for _, downStreamPart := range connector.DownStreams() {
			downStreamConnector := downStreamPart.Connector()
			if downStreamConnector != nil {
				addConnectorToMap(downStreamConnector, connectorsMap)
			}
		}
	}
}

func (genericPipeline *GenericPipeline) Specification() *metadata.ReplicationSpecification {
	return genericPipeline.spec
}

func (genericPipeline *GenericPipeline) Settings() map[string]interface{} {
	return genericPipeline.settings_at_start
}

func (genericPipeline *GenericPipeline) State() common.PipelineState {
	return genericPipeline.state
}

func (genericPipeline *GenericPipeline) InstanceId() string {
	return fmt.Sprintf("%v-%v", genericPipeline.topic, genericPipeline.instance_id)
}

func (genericPipeline *GenericPipeline) String() string {
	return genericPipeline.InstanceId()
}
//enforcer for GenericPipeline to implement Pipeline
var _ common.Pipeline = (*GenericPipeline)(nil)
