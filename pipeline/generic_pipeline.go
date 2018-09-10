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
	"sync"
	"time"
)

var ErrorKey = "Error"

// In certain scenarios, e.g., incorrect bucket password, a large number of parts
// may return error when starting. limit the number of errors we track and log
// to avoid overly long log entries
var MaxNumberOfErrorsToTrack = 15

//the function constructs start settings for parts of the pipeline
type PartsSettingsConstructor func(pipeline common.Pipeline, part common.Part, pipeline_settings metadata.ReplicationSettingsMap,
	targetClusterref *metadata.RemoteClusterReference, ssl_port_map map[string]uint16) (metadata.ReplicationSettingsMap, error)

//the function constructs start settings for parts of the pipeline
type SSLPortMapConstructor func(targetClusterRef *metadata.RemoteClusterReference, spec *metadata.ReplicationSpecification) (map[string]uint16, error)

//the function constructs update settings for parts of the pipeline
type PartsUpdateSettingsConstructor func(pipeline common.Pipeline, part common.Part, pipeline_settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error)

type StartingSeqnoConstructor func(pipeline common.Pipeline) error

type CheckpointFunc func(pipeline common.Pipeline) error

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
	stateLock sync.RWMutex

	partSetting_constructor       PartsSettingsConstructor
	sslPortMapConstructor         SSLPortMapConstructor
	partUpdateSetting_constructor PartsUpdateSettingsConstructor

	startingSeqno_constructor StartingSeqnoConstructor

	checkpoint_func CheckpointFunc

	//the map that contains the references to all parts used in the pipeline
	//it only populated when GetAllParts called the first time
	// PartsMap for now is a map of sources
	partsMap map[string]common.Part

	//the map that contains the references to all connectors used in the pipeline
	//it only populated when GetAllConnectors called the first time
	// ConnectorsMap for now is a map of routers
	connectorsMap map[string]common.Connector

	//the map that contains the references to all async event listeners used in the pipeline
	//it only populated when GetAllAsyncComponentEventListeners is called the first time
	asyncEventListenerMap map[string]common.AsyncComponentEventListener

	logger *log.CommonLogger

	spec          *metadata.ReplicationSpecification
	settings      metadata.ReplicationSettingsMap
	settings_lock *sync.RWMutex

	targetClusterRef *metadata.RemoteClusterReference

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

func (genericPipeline *GenericPipeline) startPartsWithTimeout(ssl_port_map map[string]uint16) error {
	err_ch := make(chan partError, 1000)
	startPartsFunc := func() error {
		for _, source := range genericPipeline.sources {
			waitGrp := &sync.WaitGroup{}
			waitGrp.Add(1)
			go func(err_ch chan partError, source common.Nozzle, settings metadata.ReplicationSettingsMap, waitGrp *sync.WaitGroup, ssl_port_map map[string]uint16) {
				defer waitGrp.Done()
				genericPipeline.startPart(source, settings, genericPipeline.targetClusterRef, ssl_port_map, err_ch)
				if len(err_ch) == 0 {
					genericPipeline.logger.Infof("%v Incoming nozzle %s has been started", genericPipeline.InstanceId(), source.Id())
				}
				return

			}(err_ch, source, genericPipeline.settings, waitGrp, ssl_port_map)

			waitGrp.Wait()
			if len(err_ch) != 0 {
				errMap := formatErrMsg(err_ch)
				err := fmt.Errorf("Pipeline %v failed to start, err=%v\n", genericPipeline.Topic(), errMap)
				genericPipeline.logger.Errorf("%v", err)
				return err
			}
		}
		return nil
	}

	// put a timeout around part part starting to avoid being stuck
	err := base.ExecWithTimeout(startPartsFunc, base.TimeoutPartsStart, genericPipeline.logger)
	if err != nil {
		genericPipeline.logger.Errorf("%v error starting pipeline parts. err=%v", genericPipeline.InstanceId(), err)
	} else {
		genericPipeline.logger.Infof("%v pipeline parts have started successfully", genericPipeline.InstanceId())
	}
	return err
}

// Starts the downstream parts recursively, and eventually the part itself
// In a more specific use case, for now, it starts the nozzles and routers (out-going nozzles first, then source nozzles)
func (genericPipeline *GenericPipeline) startPart(part common.Part, settings metadata.ReplicationSettingsMap,
	targetClusterRef *metadata.RemoteClusterReference, ssl_port_map map[string]uint16, err_ch chan partError) {

	var err error = nil

	// start downstreams, such as CAPI or XMEM nozzles, before we start the actual part (i.e. DCP nozzle)
	if part.Connector() != nil {
		downstreamParts := part.Connector().DownStreams()
		waitGrp := &sync.WaitGroup{}
		for _, p := range downstreamParts {
			waitGrp.Add(1)
			go func(waitGrp *sync.WaitGroup, err_ch chan partError, p common.Part, settings metadata.ReplicationSettingsMap) {
				defer waitGrp.Done()
				if p.State() == common.Part_Initial {
					genericPipeline.startPart(p, settings, genericPipeline.targetClusterRef, ssl_port_map, err_ch)
				}
			}(waitGrp, err_ch, p, settings)
		}

		waitGrp.Wait()

		if len(err_ch) > 0 {
			return
		}
	}

	partSettings := settings
	if genericPipeline.partSetting_constructor != nil {
		genericPipeline.logger.Debugf("%v calling part setting constructor\n", genericPipeline.InstanceId())

		// partSetting_contructor currently is only: xdcrf.ConstructUpdateSettingsForPart
		partSettings, err = genericPipeline.partSetting_constructor(genericPipeline, part, settings, genericPipeline.targetClusterRef, ssl_port_map)
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
func (genericPipeline *GenericPipeline) Start(settings metadata.ReplicationSettingsMap) base.ErrorMap {
	genericPipeline.logger.Infof("Starting pipeline %s\n %s \n settings = %v\n", genericPipeline.InstanceId(), genericPipeline.Layout(), settings.CloneAndRedact())
	var errMap base.ErrorMap = make(base.ErrorMap)
	var err error

	defer func(genericPipeline *GenericPipeline, errMap map[string]error) {
		if len(errMap) > 0 {
			genericPipeline.logger.Errorf("%v failed to start, err=%v", genericPipeline.InstanceId(), errMap)
			genericPipeline.ReportProgress(fmt.Sprintf("Pipeline failed to start, err=%v", errMap))
		}
	}(genericPipeline, errMap)

	err = genericPipeline.SetState(common.Pipeline_Starting)
	if err != nil {
		errMap["genericPipeline.SetState.Pipeline_Starting"] = err
		return errMap
	}

	settings[base.VBTimestamps] = &base.ObjectWithLock{make(map[uint16]*base.VBTimestamp), &sync.RWMutex{}}
	settings[base.ProblematicVBSource] = &base.ObjectWithLock{make(map[uint16]error), &sync.RWMutex{}}
	settings[base.ProblematicVBTarget] = &base.ObjectWithLock{make(map[uint16]error), &sync.RWMutex{}}

	genericPipeline.settings = settings

	//get starting vb timestamp
	go genericPipeline.startingSeqno_constructor(genericPipeline)

	// start async event listeners
	for _, async_event_listener := range GetAllAsyncComponentEventListeners(genericPipeline) {
		async_event_listener.Start()
	}

	//start the runtime
	err = genericPipeline.context.Start(genericPipeline.settings)
	if err != nil {
		errMap["genericPipeline.context.Start"] = err
		return errMap
	}
	genericPipeline.logger.Debugf("%v The runtime context has been started", genericPipeline.InstanceId())
	genericPipeline.ReportProgress("The runtime context has been started")

	var ssl_port_map map[string]uint16
	if genericPipeline.sslPortMapConstructor != nil {
		ssl_port_map, err = genericPipeline.sslPortMapConstructor(genericPipeline.targetClusterRef, genericPipeline.spec)
		if err != nil {
			errMap["genericPipeline.sslPortMapConstructor"] = err
			return errMap
		}
	}

	//start all the processing steps of the Pipeline
	//start the incoming nozzle which would start the downstream steps subsequently
	err = genericPipeline.startPartsWithTimeout(ssl_port_map)
	if err != nil {
		errMap["genericPipeline.startParts"] = err
		return errMap
	}

	genericPipeline.logger.Infof("%v All parts have been started", genericPipeline.Topic())
	genericPipeline.ReportProgress("All parts have been started")

	//open targets
	for _, target := range genericPipeline.targets {
		err = target.Open()
		if err != nil {
			genericPipeline.logger.Errorf("%v failed to open outgoing nozzle %s. err=%v", genericPipeline.InstanceId(), target.Id(), err)
			errMap["genericPipeline.outgoingNozzle.Open"] = err
			return errMap
		}
	}
	genericPipeline.logger.Debugf("%v All outgoing nozzles have been opened", genericPipeline.Topic())
	genericPipeline.ReportProgress("All outgoing nozzles have been opened")

	//open source
	for _, source := range genericPipeline.sources {

		err = source.Open()
		if err != nil {
			genericPipeline.logger.Errorf("%v failed to open incoming nozzle %s. err=%v", genericPipeline.InstanceId(), source.Id(), err)
			errMap["genericPipeline.sourceNozzle.Open"] = err
			return errMap
		}
	}
	genericPipeline.logger.Debugf("%v All incoming nozzles have been opened", genericPipeline.Topic())
	genericPipeline.ReportProgress("All incoming nozzles have been opened")

	err = genericPipeline.SetState(common.Pipeline_Running)
	if err == nil {
		genericPipeline.logger.Infof("-----------Pipeline %s has been started----------", genericPipeline.InstanceId())
		genericPipeline.ReportProgress("Pipeline is running")
	} else {
		err = fmt.Errorf("Pipeline %s failed to start", genericPipeline.Topic())
		errMap["genericPipeline.SetState.Pipeline_running"] = err
	}

	return errMap
}

func formatErrMsg(err_ch chan partError) map[string]error {
	errMap := make(map[string]error)
	for {
		select {
		case part_err := <-err_ch:
			errMap[part_err.partId] = part_err.err
			if len(errMap) >= MaxNumberOfErrorsToTrack {
				return errMap
			}
		default:
			return errMap
		}
	}
	return errMap
}

func (genericPipeline *GenericPipeline) stopPartsWithTimeout() error {
	// put a timeout around part stopping to avoid being stuck
	err := base.ExecWithTimeout(genericPipeline.stopParts, base.TimeoutPartsStop, genericPipeline.logger)
	if err != nil {
		genericPipeline.logger.Warnf("%v error stopping pipeline parts. err=%v", genericPipeline.InstanceId(), err)
	} else {
		genericPipeline.logger.Infof("%v pipeline parts have stopped successfully", genericPipeline.InstanceId())
	}
	return err
}

// stopParts() never returns non-nil error
// it has error in sigurature just to meet the requirement of ExecWithTimeout.
func (genericPipeline *GenericPipeline) stopParts() error {
	partsMap := GetAllParts(genericPipeline)
	wait_grp := &sync.WaitGroup{}
	for _, part := range partsMap {
		wait_grp.Add(1)
		// stop parts in parallel to ensure that all parts get their turns
		go genericPipeline.stopPart(part, wait_grp)
	}

	wait_grp.Wait()
	return nil
}

func (genericPipeline *GenericPipeline) stopPart(part common.Part, wait_grp *sync.WaitGroup) {
	defer wait_grp.Done()
	genericPipeline.logger.Infof("%v trying to stop part %v\n", genericPipeline.InstanceId(), part.Id())
	err := part.Stop()
	if err != nil {
		genericPipeline.logger.Warnf("%v failed to stop part %v, err=%v, let it alone to die\n", genericPipeline.InstanceId(), part.Id(), err)
	}
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
func (genericPipeline *GenericPipeline) Stop() base.ErrorMap {
	var errMap base.ErrorMap = make(base.ErrorMap)

	genericPipeline.logger.Infof("Stopping pipeline %v\n", genericPipeline.InstanceId())
	var err error

	// perform checkpoint first before stopping
	genericPipeline.checkpoint_func(genericPipeline)

	err = genericPipeline.SetState(common.Pipeline_Stopping)
	if err != nil {
		errMap["genericPipeline.SetState.Pipeline_Stopping"] = err
		return errMap
	}

	// stop async event listeners so that RaiseEvent() would not block
	for _, asyncEventListener := range GetAllAsyncComponentEventListeners(genericPipeline) {
		asyncEventListener.Stop()
	}
	genericPipeline.logger.Infof("%v Async listeners have been stopped", genericPipeline.InstanceId())
	genericPipeline.ReportProgress("Async listeners have been stopped")

	// stop services before stopping parts to avoid spurious errors from services
	genericPipeline.context.Stop()
	genericPipeline.ReportProgress("Runtime context has been stopped")

	//close the sources
	for _, source := range genericPipeline.sources {
		err = source.Close()
		if err != nil {
			genericPipeline.logger.Warnf("%v failed to close source %v. err=%v", genericPipeline.InstanceId(), source.Id(), err)
			errMap[fmt.Sprintf("genericPipeline.%v.Close", source.Id())] = err
		}
	}
	genericPipeline.logger.Infof("%v source nozzles have been closed", genericPipeline.InstanceId())
	genericPipeline.ReportProgress("Source nozzles have been closed")

	genericPipeline.stopPartsWithTimeout()
	genericPipeline.ReportProgress("Pipeline has been stopped")

	err = genericPipeline.SetState(common.Pipeline_Stopped)
	if err != nil {
		errMap["genericPipeline.SetState.Pipeline_Stopped"] = err
	}

	genericPipeline.logger.Infof("Pipeline %v has been stopped\n", genericPipeline.InstanceId())
	return errMap
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

// Used for testing only
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
	pipeline.initialize()
	return pipeline
}

func NewPipelineWithSettingConstructor(t string,
	sources map[string]common.Nozzle,
	targets map[string]common.Nozzle,
	spec *metadata.ReplicationSpecification,
	targetClusterRef *metadata.RemoteClusterReference,
	partsSettingsConstructor PartsSettingsConstructor,
	sslPortMapConstructor SSLPortMapConstructor,
	partsUpdateSettingsConstructor PartsUpdateSettingsConstructor,
	startingSeqnoConstructor StartingSeqnoConstructor,
	checkpoint_func CheckpointFunc,
	logger_context *log.LoggerContext) *GenericPipeline {
	pipeline := &GenericPipeline{topic: t,
		sources:                       sources,
		targets:                       targets,
		spec:                          spec,
		targetClusterRef:              targetClusterRef,
		partSetting_constructor:       partsSettingsConstructor,
		sslPortMapConstructor:         sslPortMapConstructor,
		partUpdateSetting_constructor: partsUpdateSettingsConstructor,
		startingSeqno_constructor:     startingSeqnoConstructor,
		checkpoint_func:               checkpoint_func,
		logger:                        log.NewLogger("GenericPipeline", logger_context),
		instance_id:                   time.Now().Nanosecond(),
		state:                         common.Pipeline_Initial,
		settings_lock:                 &sync.RWMutex{}}
	// NOTE: Calling initialize here as part of constructor
	pipeline.initialize()
	pipeline.logger.Debugf("Pipeline %s has been initialized with a part setting constructor %v", t, partsSettingsConstructor)

	return pipeline
}

// intialize all maps
// the maps will not be modified at pipeline runtime, hence there is no chance of concurrent read and write to the maps
func (genericPipeline *GenericPipeline) initialize() {
	sources := genericPipeline.Sources()

	genericPipeline.partsMap = make(map[string]common.Part)
	for _, source := range sources {
		addPartToMap(source, genericPipeline.partsMap)
	}

	genericPipeline.connectorsMap = make(map[string]common.Connector)
	for _, source := range sources {
		connector := source.Connector()
		if connector != nil {
			addConnectorToMap(connector, genericPipeline.connectorsMap)
		}
	}
}

/**
 * Given a hiearchical structure of parts and its downstream parts, recursively flatten
 * all the parts in the heiarchy into a flat partsMap
 */
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

/**
 * Given a hiearchical structure of connectors, recursively flatten all the connectors
 * into a flat connectormap
 */
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

// there is no lock on genericPipeline.asyncEventListenerMap and care must be taken to avoid concurrency issues
// As of now, this method is called by xdcr_factory right after pipeline is constructed and async listeners are registered on parts
// This is the only time when genericPipeline.asyncEventListenerMap is initialized/modified, when concurrent access is not possible.
// Hence there are no real concurrency issues.
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

// Modifies and adds to "listenersMap"
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

func GetAllParts(p common.Pipeline) map[string]common.Part {
	return p.(*GenericPipeline).partsMap
}

func GetAllConnectors(p common.Pipeline) map[string]common.Connector {
	return p.(*GenericPipeline).connectorsMap
}

func (genericPipeline *GenericPipeline) Specification() *metadata.ReplicationSpecification {
	return genericPipeline.spec
}

func (genericPipeline *GenericPipeline) Settings() metadata.ReplicationSettingsMap {
	genericPipeline.settings_lock.RLock()
	defer genericPipeline.settings_lock.RUnlock()
	return genericPipeline.settings
}

func (genericPipeline *GenericPipeline) State() common.PipelineState {
	if genericPipeline != nil {
		return genericPipeline.GetState()
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

func (genericPipeline *GenericPipeline) GetState() common.PipelineState {
	genericPipeline.stateLock.RLock()
	defer genericPipeline.stateLock.RUnlock()

	return genericPipeline.state
}

func (genericPipeline *GenericPipeline) SetState(state common.PipelineState) error {
	genericPipeline.stateLock.Lock()
	defer genericPipeline.stateLock.Unlock()

	//validate the state transition
	switch genericPipeline.state {
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

func (genericPipeline *GenericPipeline) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	var redactOnceSync sync.Once
	var redactedSettings metadata.ReplicationSettingsMap
	redactOnce := func() {
		redactOnceSync.Do(func() {
			redactedSettings = settings.CloneAndRedact()
		})
	}

	if genericPipeline.logger.GetLogLevel() >= log.LogLevelDebug {
		redactOnce()
		genericPipeline.logger.Debugf("%v update settings called with settings=%v\n", genericPipeline.InstanceId(), redactedSettings)
	}

	if len(settings) == 0 {
		return nil
	}

	// update settings in pipeline itself
	genericPipeline.updatePipelineSettings(settings)

	// update settings on parts and services in pipeline
	if genericPipeline.partSetting_constructor != nil {
		if genericPipeline.logger.GetLogLevel() >= log.LogLevelDebug {
			redactOnce()
			genericPipeline.logger.Debugf("%v calling part update setting constructor with settings=%v\n", genericPipeline.InstanceId(), redactedSettings)
		}
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
		if genericPipeline.logger.GetLogLevel() >= log.LogLevelDebug {
			redactOnce()
			genericPipeline.logger.Debugf("%v calling update setting constructor on runtime context with settings=%v\n", genericPipeline.InstanceId(), redactedSettings)
		}
		return genericPipeline.context.UpdateSettings(settings)
	}

	return nil

}

func (genericPipeline *GenericPipeline) updatePipelineSettings(settings metadata.ReplicationSettingsMap) {
	genericPipeline.settings_lock.RLock()
	defer genericPipeline.settings_lock.RUnlock()
	genericPipeline.updateTimestampsSetting(settings)
	genericPipeline.updateProblematicVBSettings(settings)
}

func (genericPipeline *GenericPipeline) updateTimestampsSetting(settings metadata.ReplicationSettingsMap) {
	ts_obj := settings[base.VBTimestamps]
	if ts_obj != nil {
		ts_map := ts_obj.(map[uint16]*base.VBTimestamp)
		existing_ts_obj := genericPipeline.settings[base.VBTimestamps].(*base.ObjectWithLock)
		existing_ts_obj.Lock.Lock()
		defer existing_ts_obj.Lock.Unlock()
		existing_ts_map := existing_ts_obj.Object.(map[uint16]*base.VBTimestamp)
		for vbno, ts := range ts_map {
			existing_ts_map[vbno] = ts
		}
	}
}

func (genericPipeline *GenericPipeline) updateProblematicVBSettings(settings metadata.ReplicationSettingsMap) {
	genericPipeline.updateProblematicVBSetting(settings, base.ProblematicVBSource)
	genericPipeline.updateProblematicVBSetting(settings, base.ProblematicVBTarget)
}

func (genericPipeline *GenericPipeline) updateProblematicVBSetting(settings metadata.ReplicationSettingsMap, settings_key string) {
	vb_err_map_obj := settings[settings_key]
	if vb_err_map_obj != nil {
		vb_err_map := vb_err_map_obj.(map[uint16]error)
		existing_vb_err_map_obj := genericPipeline.settings[settings_key].(*base.ObjectWithLock)
		// when target topology changes, we delay the restart of pipeline to allow checkpointing
		// to be done before restart. As a result, a large number of not_my_vbucket errors may be seen
		// and reported by different xmem nozzles before pipeline is restarted. If each error requires
		// write lock of existing_vb_err_map_obj, it will cause contention and delay.
		// With the isUpdateNeeded check, we acquire write lock on and update existing_vb_err_map_obj
		// only when the first not_my_vbucket error is received for a vb. Subsequent not_my_vbucket errors
		// on the same vb require only read lock on existing_vb_err_map_obj. Processing of not_my_vbucket
		// errors from different xmem nozzles will not block one another.
		// the probem is less severe in source topology change case, since dcp streams on moved out vbuckets
		// will be closed and no new mutations in the moved out vbuckets will pass through pipeline.
		// the same mechanism won't hurt and can still be used there.
		if isUpdateNeeded(existing_vb_err_map_obj, vb_err_map) {
			existing_vb_err_map_obj.Lock.Lock()
			defer existing_vb_err_map_obj.Lock.Unlock()
			existing_vb_err_map := existing_vb_err_map_obj.Object.(map[uint16]error)
			for vbno, err := range vb_err_map {
				if _, ok := existing_vb_err_map[vbno]; !ok {
					existing_vb_err_map[vbno] = err
				}
			}
		}
	}
}

// check if the vbs in vb_err_map already exists in existing_vb_err_map_obj
func isUpdateNeeded(existing_vb_err_map_obj *base.ObjectWithLock, vb_err_map map[uint16]error) bool {
	existing_vb_err_map_obj.Lock.RLock()
	defer existing_vb_err_map_obj.Lock.RUnlock()
	existing_vb_err_map := existing_vb_err_map_obj.Object.(map[uint16]error)
	for vbno, _ := range vb_err_map {
		if _, ok := existing_vb_err_map[vbno]; !ok {
			return true
		}
	}
	return false
}

//enforcer for GenericPipeline to implement Pipeline
var _ common.Pipeline = (*GenericPipeline)(nil)
