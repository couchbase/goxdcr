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
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/simple_utils"
	"sync"
	"time"
)

var ErrorKey = "Error"

//the function constructs start settings for parts of the pipeline
type PartsSettingsConstructor func(pipeline common.Pipeline, part common.Part, pipeline_settings map[string]interface{},
	ts map[uint16]*base.VBTimestamp, targetClusterref *metadata.RemoteClusterReference, ssl_port_map map[string]uint16,
	isSSLOverMem bool) (map[string]interface{}, error)

//the function constructs start settings for parts of the pipeline
type SSLPortMapConstructor func(targetClusterRef *metadata.RemoteClusterReference, spec *metadata.ReplicationSpecification) (map[string]uint16, bool, error)

//the function constructs update settings for parts of the pipeline
type PartsUpdateSettingsConstructor func(pipeline common.Pipeline, part common.Part, pipeline_settings map[string]interface{}) (map[string]interface{}, error)

type StartingSeqnoConstructor func(pipeline common.Pipeline) error

type RemoteClsuterRefRetriever func(remoteClusterUUID string, refresh bool) (*metadata.RemoteClusterReference, error)

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

	partSetting_constructor       PartsSettingsConstructor
	sslPortMapConstructor         SSLPortMapConstructor
	partUpdateSetting_constructor PartsUpdateSettingsConstructor

	startingSeqno_constructor StartingSeqnoConstructor

	remoteClusterRef_retriever RemoteClsuterRefRetriever

	//the map that contains the references to all parts used in the pipeline
	//it only populated when GetAllParts called the first time
	partsMap map[string]common.Part

	//the map that contains the references to all connectors used in the pipeline
	//it only populated when GetAllConnectors called the first time
	connectorsMap map[string]common.Connector

	//the map that contains the references to all async event listeners used in the pipeline
	//it only populated when GetAllAsyncComponentEventListeners is called the first time
	asyncEventListenerMap map[string]common.AsyncComponentEventListener

	logger *log.CommonLogger

	spec          *metadata.ReplicationSpecification
	settings      map[string]interface{}
	settings_lock *sync.RWMutex

	state common.PipelineState

	instance_id       int
	progress_recorder common.PipelineProgressRecorder
}

type partError struct {
	partId string
	err    error
}

//Get the runtime context of this pipeline
func (genericPipeline *GenericPipeline) RuntimeContext() common.PipelineRuntimeContext {
	return genericPipeline.context
}

func (genericPipeline *GenericPipeline) SetRuntimeContext(ctx common.PipelineRuntimeContext) {
	genericPipeline.context = ctx
}

func (genericPipeline *GenericPipeline) startPart(part common.Part, settings map[string]interface{}, ts map[uint16]*base.VBTimestamp,
	targetClusterRef *metadata.RemoteClusterReference, ssl_port_map map[string]uint16, isSSLOverMem bool, err_ch chan partError) {

	var err error = nil

	//start downstreams
	if part.Connector() != nil {
		downstreamParts := part.Connector().DownStreams()
		waitGrp := &sync.WaitGroup{}
		for _, p := range downstreamParts {
			waitGrp.Add(1)
			go func(waitGrp *sync.WaitGroup, err_ch chan partError, p common.Part, settings map[string]interface{}, ts map[uint16]*base.VBTimestamp) {
				defer waitGrp.Done()
				if p.State() == common.Part_Initial {
					genericPipeline.startPart(p, settings, ts, targetClusterRef, ssl_port_map, isSSLOverMem, err_ch)
				}
			}(waitGrp, err_ch, p, settings, ts)
		}

		waitGrp.Wait()

		if len(err_ch) > 0 {
			return
		}
	}

	partSettings := settings
	if genericPipeline.partSetting_constructor != nil {
		genericPipeline.logger.Debugf("Calling part setting constructor\n")
		partSettings, err = genericPipeline.partSetting_constructor(genericPipeline, part, settings, ts, targetClusterRef, ssl_port_map, isSSLOverMem)
		if err != nil {
			err_ch <- partError{part.Id(), err}
			return
		}

	}

	err = part.Start(partSettings)
	if err != nil && err.Error() != parts.PartAlreadyStartedError.Error() {
		err_ch <- partError{part.Id(), err}
	}
}

//Start starts the pipeline
//
//settings - a map of parameter to start the pipeline. it can contain initialization paramters
//			 for each processing steps and for runtime context of the pipeline.
func (genericPipeline *GenericPipeline) Start(settings map[string]interface{}) error {
	genericPipeline.logger.Infof("Starting pipeline %s\n %s \n settings = %s\n", genericPipeline.InstanceId(), genericPipeline.Layout(), fmt.Sprint(settings))
	var err error

	defer func(genericPipeline *GenericPipeline, err error) {
		if err != nil {
			genericPipeline.ReportProgress(fmt.Sprintf("Pipeline failed to start, err=%v", err))
		}
	}(genericPipeline, err)

	err = genericPipeline.SetState(common.Pipeline_Starting)
	if err != nil {
		return err
	}

	settings[base.VBTimestamps] = make(map[uint16]*base.VBTimestamp)
	settings[base.ProblematicVBs] = make(map[uint16]error)
	genericPipeline.settings = settings

	//get starting vb timestamp
	go genericPipeline.startingSeqno_constructor(genericPipeline)

	targetClusterRef, err := genericPipeline.remoteClusterRef_retriever(genericPipeline.spec.TargetClusterUUID, true)
	if err != nil {
		genericPipeline.logger.Errorf("Error getting remote cluster with uuid=%v, err=%v\n", genericPipeline.spec.TargetClusterUUID, err)
		return err
	}

	// start async event listeners
	for _, async_event_listener := range GetAllAsyncComponentEventListeners(genericPipeline) {
		async_event_listener.Start()
	}

	//start the runtime
	err = genericPipeline.context.Start(genericPipeline.settings)
	if err != nil {
		return err
	}
	genericPipeline.logger.Debug("The runtime context is started")
	genericPipeline.ReportProgress("The runtime context is started")

	var ssl_port_map map[string]uint16
	var isSSLOverMem bool
	if genericPipeline.sslPortMapConstructor != nil {
		ssl_port_map, isSSLOverMem, err = genericPipeline.sslPortMapConstructor(targetClusterRef, genericPipeline.spec)
		if err != nil {
			return err
		}
	}

	//start all the processing steps of the Pipeline
	//start the incoming nozzle which would start the downstream steps
	//subsequently
	err_ch := make(chan partError, 1000)
	for _, source := range genericPipeline.sources {
		waitGrp := &sync.WaitGroup{}
		waitGrp.Add(1)
		go func(err_ch chan partError, source common.Nozzle, settings map[string]interface{}, waitGrp *sync.WaitGroup) {
			defer waitGrp.Done()
			ts := settings["VBTimestamps"].(map[uint16]*base.VBTimestamp)

			genericPipeline.startPart(source, settings, ts, targetClusterRef, ssl_port_map, isSSLOverMem, err_ch)
			if len(err_ch) == 0 {
				genericPipeline.logger.Infof("Incoming nozzle %s is started", source.Id())
			}
			return

		}(err_ch, source, genericPipeline.settings, waitGrp)

		waitGrp.Wait()
		if len(err_ch) != 0 {
			errMsg := formatErrMsg(err_ch)
			return fmt.Errorf("Pipeline %v failed to start, err=%v\n", genericPipeline.Topic(), errMsg)
		}
	}
	genericPipeline.logger.Info("All parts has been started")
	genericPipeline.ReportProgress("All parts has been started")

	//open targets
	for _, target := range genericPipeline.targets {
		err = target.Open()
		if err != nil {
			genericPipeline.logger.Errorf("Failed to open outgoing nozzle %s", target.Id())
			return err
		}
	}
	genericPipeline.logger.Debug("All outgoing nozzles have been opened")
	genericPipeline.ReportProgress("All outgoing nozzles have been opened")

	//open source
	for _, source := range genericPipeline.sources {

		err = source.Open()
		if err != nil {
			genericPipeline.logger.Errorf("Failed to open incoming nozzle %s", source.Id())
			return err
		}
	}
	genericPipeline.logger.Debug("All incoming nozzles have been opened")
	genericPipeline.ReportProgress("All incoming nozzles have been openedL¬")

	err = genericPipeline.SetState(common.Pipeline_Running)
	if err == nil {
		genericPipeline.logger.Infof("-----------Pipeline %s is started----------", genericPipeline.InstanceId())
		genericPipeline.ReportProgress("Pipeline is running")

	} else {
		err = fmt.Errorf("Pipeline %s failed to start", genericPipeline.Topic())
	}

	return err
}

func formatErrMsg(err_ch chan partError) map[string]error {
	errMap := make(map[string]error)
	for {
		select {
		case part_err := <-err_ch:
			errMap[part_err.partId] = part_err.err
		default:
			return errMap
		}
	}
	return errMap
}

func (genericPipeline *GenericPipeline) stopPart(part common.Part) error {
	var err error = nil
	genericPipeline.logger.Infof("Trying to stop part %v for %v\n", part.Id(), genericPipeline.InstanceId())
	err = simple_utils.ExecWithTimeout(part.Stop, 1000*time.Millisecond, genericPipeline.logger)
	if err == nil {
		genericPipeline.logger.Infof("part %v is stopped\n", part.Id())
	} else {
		genericPipeline.logger.Infof("Failed to stop part %v, err=%v, let it alone to die\n", part.Id(), err)
	}
	return err
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

	genericPipeline.logger.Infof("Stopping pipeline %v\n", genericPipeline.InstanceId())
	var err error

	err = genericPipeline.SetState(common.Pipeline_Stopping)
	if err != nil {
		return err
	}

	// stop services before stopping parts to avoid spurious errors from services
	err = genericPipeline.context.Stop()
	if err != nil {
		return err
	}
	genericPipeline.ReportProgress("Runtime context is stopped")

	//close the sources
	for _, source := range genericPipeline.sources {
		err = source.Close()
		if err != nil {
			return err
		}
	}
	genericPipeline.logger.Debug("Incoming nozzles are closed, preparing to stop.")

	partsMap := GetAllParts(genericPipeline)
	for _, part := range partsMap {
		go func(part common.Part) {
			err = genericPipeline.stopPart(part)
			if err != nil {
				genericPipeline.logger.Infof("Source nozzle %v failed to stop in time, left it alone to die.", part.Id())
			}

		}(part)
	}

	// stop async event listeners
	for _, asyncEventListener := range GetAllAsyncComponentEventListeners(genericPipeline) {
		go func(asyncEventListener common.AsyncComponentEventListener) {
			asyncEventListener.Stop()
		}(asyncEventListener)
	}

	genericPipeline.ReportProgress("Pipeline is stopped")

	err = genericPipeline.SetState(common.Pipeline_Stopped)
	genericPipeline.logger.Infof("Pipeline %v is stopped\n", genericPipeline.InstanceId())
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

func NewGenericPipeline(t string,
	sources map[string]common.Nozzle,
	targets map[string]common.Nozzle,
	spec *metadata.ReplicationSpecification,
	uilog_svc service_def.UILogSvc) *GenericPipeline {
	pipeline := &GenericPipeline{topic: t,
		sources:       sources,
		targets:       targets,
		spec:          spec,
		logger:        log.NewLogger("GenericPipeline", nil),
		instance_id:   time.Now().Nanosecond(),
		state:         common.Pipeline_Initial,
		settings_lock: &sync.RWMutex{}}
	return pipeline
}

func NewPipelineWithSettingConstructor(t string,
	sources map[string]common.Nozzle,
	targets map[string]common.Nozzle,
	spec *metadata.ReplicationSpecification,
	partsSettingsConstructor PartsSettingsConstructor,
	sslPortMapConstructor SSLPortMapConstructor,
	partsUpdateSettingsConstructor PartsUpdateSettingsConstructor,
	startingSeqnoConstructor StartingSeqnoConstructor,
	remoteClusterRefRetriever RemoteClsuterRefRetriever,
	logger_context *log.LoggerContext) *GenericPipeline {
	pipeline := &GenericPipeline{topic: t,
		sources: sources,
		targets: targets,
		spec:    spec,
		partSetting_constructor:       partsSettingsConstructor,
		sslPortMapConstructor:         sslPortMapConstructor,
		partUpdateSetting_constructor: partsUpdateSettingsConstructor,
		startingSeqno_constructor:     startingSeqnoConstructor,
		remoteClusterRef_retriever:    remoteClusterRefRetriever,
		logger:        log.NewLogger("GenericPipeline", logger_context),
		instance_id:   time.Now().Nanosecond(),
		state:         common.Pipeline_Initial,
		settings_lock: &sync.RWMutex{}}
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
	if part != nil && partsMap != nil {
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
}

func GetAllAsyncComponentEventListeners(p common.Pipeline) map[string]common.AsyncComponentEventListener {
	genericPipeline := p.(*GenericPipeline)
	if genericPipeline.asyncEventListenerMap == nil {
		genericPipeline.asyncEventListenerMap = make(map[string]common.AsyncComponentEventListener)
		partsMap := make(map[string]common.Part)
		// add async listeners on parts and connectors to listeners map
		for _, source := range genericPipeline.Sources() {
			addAsyncListenersToMap(source, genericPipeline.asyncEventListenerMap, partsMap)
		}
	}

	return genericPipeline.asyncEventListenerMap
}

func addAsyncListenersToMap(part common.Part, listenersMap map[string]common.AsyncComponentEventListener, partsMap map[string]common.Part) {
	if part != nil {
		// use partsMap to check if the part has been processed before to avoid infinite loop
		if _, ok := partsMap[part.Id()]; !ok {
			partsMap[part.Id()] = part

			mergeListenersMap(listenersMap, part.AsyncComponentEventListeners())

			connector := part.Connector()
			if connector != nil {
				mergeListenersMap(listenersMap, connector.AsyncComponentEventListeners())
				for _, downStreamPart := range connector.DownStreams() {
					addAsyncListenersToMap(downStreamPart, listenersMap, partsMap)
				}
			}
		}
	}
}

// add all entries in listenersMap2 to listenersMap
func mergeListenersMap(listenersMap map[string]common.AsyncComponentEventListener, listenersMap2 map[string]common.AsyncComponentEventListener) {
	for id, listener := range listenersMap2 {
		if _, ok := listenersMap[id]; !ok {
			listenersMap[id] = listener
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
	genericPipeline.settings_lock.RLock()
	defer genericPipeline.settings_lock.RUnlock()
	return genericPipeline.settings
}

func (genericPipeline *GenericPipeline) State() common.PipelineState {
	if genericPipeline != nil {
		return genericPipeline.state
	} else {
		return common.Pipeline_Stopped
	}
}

func (genericPipeline *GenericPipeline) InstanceId() string {
	return fmt.Sprintf("%v-%v", genericPipeline.topic, genericPipeline.instance_id)
}

func (genericPipeline *GenericPipeline) String() string {
	return genericPipeline.InstanceId()
}

func (genericPipeline *GenericPipeline) Layout() string {
	header := fmt.Sprintf("------------%s---------------", genericPipeline.Topic())
	footer := "-----------------------------"
	content := ""
	for _, sourceNozzle := range genericPipeline.Sources() {
		dcpSection := fmt.Sprintf("\t%s:{vbList=%v}\n", sourceNozzle.Id(), sourceNozzle.(*parts.DcpNozzle).GetVBList())
		router := sourceNozzle.Connector().(*parts.Router)
		routerSection := fmt.Sprintf("\t\t%s :{\nroutingMap=%v}\n", router.Id(), router.RoutingMapByDownstreams())
		downstreamParts := router.DownStreams()
		targetNozzleSection := ""
		for partId, _ := range downstreamParts {
			targetNozzleSection = targetNozzleSection + fmt.Sprintf("\t\t\t%s\n", partId)
		}

		content = content + fmt.Sprintf("%s%s%s\n", dcpSection, routerSection, targetNozzleSection)
	}
	return fmt.Sprintf("%s\n%s\n%s\n", header, content, footer)
}

func (genericPipeline *GenericPipeline) SetState(state common.PipelineState) error {
	genericPipeline.stateLock.Lock()
	defer genericPipeline.stateLock.Unlock()

	//validate the state transition
	switch genericPipeline.State() {
	case common.Pipeline_Initial:
		if state != common.Pipeline_Starting && state != common.Pipeline_Stopping {
			return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, genericPipeline.InstanceId(), "Initial", "Starting, Stopping"))
		}
	case common.Pipeline_Starting:
		if state != common.Pipeline_Running && state != common.Pipeline_Stopping && state != common.Pipeline_Error {
			return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, genericPipeline.InstanceId(), "Starting", "Running, Stopping, Error"))
		}
	case common.Pipeline_Running:
		if state != common.Pipeline_Stopping && state != common.Pipeline_Error {
			return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, genericPipeline.InstanceId(), "Running", "Stopping, Error"))
		}
	case common.Pipeline_Stopping:
		if state != common.Pipeline_Stopped {
			return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, genericPipeline.InstanceId(), "Stopping", "Stopped"))
		}
	case common.Pipeline_Stopped:
		return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, genericPipeline.InstanceId(), "Stopped", ""))
	case common.Pipeline_Error:
		if state != common.Pipeline_Stopping {
			return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, genericPipeline.InstanceId(), "Error", "Stopping"))
		}
	}

	genericPipeline.state = state

	return nil
}

func (genericPipeline *GenericPipeline) ReportProgress(progress string) {
	if genericPipeline.progress_recorder != nil {
		genericPipeline.progress_recorder(progress)
	}
}

func (genericPipeline *GenericPipeline) SetProgressRecorder(recorder common.PipelineProgressRecorder) {
	genericPipeline.progress_recorder = recorder
}

func (genericPipeline *GenericPipeline) UpdateSettings(settings map[string]interface{}) error {
	if len(settings) == 0 {
		return nil
	}

	// update settings in pipeline itself
	genericPipeline.updatePipelineSettings(settings)

	// update settings on parts and services in pipeline
	if genericPipeline.partSetting_constructor != nil {
		genericPipeline.logger.Debugf("Calling part update setting constructor with settings=%v\n", settings)
		for _, part := range GetAllParts(genericPipeline) {
			partSettings, err := genericPipeline.partUpdateSetting_constructor(genericPipeline, part, settings)
			if err != nil {
				return err
			}
			err = part.UpdateSettings(partSettings)
			if err != nil {
				return err
			}
		}
	}

	if genericPipeline.context != nil {
		genericPipeline.logger.Debugf("Calling update setting constructor on runtime context with settings=%v\n", settings)
		return genericPipeline.context.UpdateSettings(settings)
	}

	return nil

}

func (genericPipeline *GenericPipeline) updatePipelineSettings(settings map[string]interface{}) {
	genericPipeline.settings_lock.Lock()
	defer genericPipeline.settings_lock.Unlock()
	genericPipeline.updateTimestampsSetting(settings)
	genericPipeline.updateProblematicVBsSetting(settings)
}

func (genericPipeline *GenericPipeline) updateTimestampsSetting(settings map[string]interface{}) {
	ts_obj := settings[base.VBTimestamps]
	if ts_obj != nil {
		ts_map := ts_obj.(map[uint16]*base.VBTimestamp)
		existing_ts_map := genericPipeline.settings[base.VBTimestamps].(map[uint16]*base.VBTimestamp)
		for vbno, ts := range ts_map {
			existing_ts_map[vbno] = ts
		}
	}
}

func (genericPipeline *GenericPipeline) updateProblematicVBsSetting(settings map[string]interface{}) {
	vb_err_map_obj := settings[base.ProblematicVBs]
	if vb_err_map_obj != nil {
		vb_err_map := vb_err_map_obj.(map[uint16]error)
		existing_vb_err_map := genericPipeline.settings[base.ProblematicVBs].(map[uint16]error)
		for vbno, err := range vb_err_map {
			existing_vb_err_map[vbno] = err
		}
	}
}

//enforcer for GenericPipeline to implement Pipeline
var _ common.Pipeline = (*GenericPipeline)(nil)
