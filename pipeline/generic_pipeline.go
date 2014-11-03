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
	"fmt"
	common "github.com/Xiaomei-Zhang/goxdcr/common"
	log "github.com/Xiaomei-Zhang/goxdcr/log"
	"sync"
)

//the function can construct part specific settings for the pipeline
type PartsSettingsConstructor func(pipeline common.Pipeline, part common.Part, pipeline_settings map[string]interface{}) (map[string]interface{}, error)

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

	//if the pipeline is active running
	isActive bool

	//the lock to serialize the request to start\stop the pipeline
	stateLock sync.Mutex

	partSetting_constructor PartsSettingsConstructor

	//the map that contains the reference of all parts used in the pipeline
	//it only populated when GetAllParts called the first time
	partsMap map[string]common.Part

	logger *log.CommonLogger
}

//Get the runtime context of this pipeline
func (genericPipeline *GenericPipeline) RuntimeContext() common.PipelineRuntimeContext {
	return genericPipeline.context
}

func (genericPipeline *GenericPipeline) SetRuntimeContext(ctx common.PipelineRuntimeContext) {
	genericPipeline.context = ctx
}

func (genericPipeline *GenericPipeline) startPart(part common.Part, settings map[string]interface{}) error {
	var err error = nil

	//start downstreams
	if part.Connector() != nil {
		downstreamParts := part.Connector().DownStreams()
		for _, p := range downstreamParts {
			err = genericPipeline.startPart(p, settings)
			if err != nil {
				return err
			}
		}
	}

	partSettings := settings
	if genericPipeline.partSetting_constructor != nil {
		genericPipeline.logger.Debugf("Calling part setting constructor\n")
		partSettings, err = genericPipeline.partSetting_constructor(genericPipeline, part, settings)
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
	genericPipeline.logger.Debugf("Try to start the pipeline with settings = %s", fmt.Sprint(settings))
	var err error

	genericPipeline.stateLock.Lock()
	defer genericPipeline.stateLock.Unlock()

	//start all the processing steps of the Pipeline
	//start the incoming nozzle which would start the downstream steps
	//subsequently
	for _, source := range genericPipeline.sources {
		err = genericPipeline.startPart(source, settings)
		if err != nil {
			return err
		}
		genericPipeline.logger.Debugf("Incoming nozzle %s is started", source.Id())
	}
	genericPipeline.logger.Info("All parts has been started")

	genericPipeline.logger.Debug("Try to start the runtime context")
	//start the runtime
	err = genericPipeline.context.Start(settings)
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

	//set its state to be active
	genericPipeline.isActive = true

	genericPipeline.logger.Infof("-----------Pipeline %s is started----------", genericPipeline.Topic())

	return err
}

func (genericPipeline *GenericPipeline) stopPart(part common.Part) error {
	var err error = nil
	genericPipeline.logger.Infof("Trying to stop part %v\n", part.Id())
	if genericPipeline.canStop(part) {
		if !part.IsStarted() {
			genericPipeline.logger.Debugf("part %v is already stopped\n", part.Id())
			return nil
		}
		err = part.Stop()
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
func (genericPipeline *GenericPipeline) Stop() error {
	genericPipeline.logger.Infof("stoppping pipeline %v\n", genericPipeline.Topic())
	var err error

//	genericPipeline.stateLock.Lock()
//	defer genericPipeline.stateLock.Unlock()

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

	err = genericPipeline.context.Stop()

	genericPipeline.isActive = false

	genericPipeline.logger.Infof("Pipeline %v is stopped\n", genericPipeline.Topic())
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
				genericPipeline.logger.Infof("outgoing nozzle %s is still running", target.Id())
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
	targets map[string]common.Nozzle) *GenericPipeline {
	pipeline := &GenericPipeline{topic: t,
		sources:  sources,
		targets:  targets,
		isActive: false,
		logger:   log.NewLogger("GenericPipeline", nil)}
	return pipeline
}

func NewPipelineWithSettingConstructor(t string,
	sources map[string]common.Nozzle,
	targets map[string]common.Nozzle,
	partsSettingsConstructor PartsSettingsConstructor,
	logger_context *log.LoggerContext) *GenericPipeline {
	pipeline := &GenericPipeline{topic: t,
		sources:                 sources,
		targets:                 targets,
		isActive:                false,
		partSetting_constructor: partsSettingsConstructor,
		logger:                  log.NewLogger("GenericPipeline", logger_context)}
	pipeline.logger.Debugf("Pipeline %s is initialized with a part setting constructor %v", t, partsSettingsConstructor)

	return pipeline
}

func GetAllParts(p common.Pipeline) map[string]common.Part {
	if p.(*GenericPipeline).partsMap == nil {
		p.(*GenericPipeline).partsMap = make(map[string]common.Part)
		sources := p.Sources()
		for key, source := range sources {
			p.(*GenericPipeline).partsMap[key] = source

			addDownStreams(source, p.(*GenericPipeline).partsMap)
		}
	}
	return p.(*GenericPipeline).partsMap
}

func addDownStreams(p common.Part, partsMap map[string]common.Part) {
	connector := p.Connector()
	if connector != nil {
		downstreams := connector.DownStreams()
		for key, part := range downstreams {
			partsMap[key] = part

			addDownStreams(part, partsMap)
		}
	}
}

//enforcer for GenericPipeline to implement Pipeline
var _ common.Pipeline = (*GenericPipeline)(nil)
