// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package pipeline

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/peerToPeer"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
)

var ErrorKey = "Error"

const PipelineContextStart = "genericPipeline.context.Start"
const PipelinePartStart = "genericPipeline.startPartsWithTimeout"
const MergeCkptFuncKey = "genericPipeline.mergeCkptFunc"

// In certain scenarios, e.g., incorrect bucket password, a large number of parts
// may return error when starting. limit the number of errors we track and log
// to avoid overly long log entries
var MaxNumberOfErrorsToTrack = 15

// the function constructs start settings for parts of the pipeline
type PartsSettingsConstructor func(pipeline common.Pipeline, part common.Part, pipeline_settings metadata.ReplicationSettingsMap,
	targetClusterref *metadata.RemoteClusterReference, ssl_port_map map[string]uint16) (metadata.ReplicationSettingsMap, error)

type ConnectorSettingsConstructor func(pipeline common.Pipeline, connector common.Connector, pipeline_settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error)

// the function constructs start settings for parts of the pipeline
type SSLPortMapConstructor func(targetClusterRef *metadata.RemoteClusterReference, spec *metadata.ReplicationSpecification) (map[string]uint16, error)

// the function constructs update settings for parts of the pipeline
type PartsUpdateSettingsConstructor func(pipeline common.Pipeline, part common.Part, pipeline_settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error)

// the function constructs update settings for connectors of the pipeline
type ConnectorsUpdateSettingsConstructor func(pipeline common.Pipeline, connector common.Connector, pipeline_settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error)

type StartingSeqnoConstructor func(pipeline common.Pipeline) error

type CheckpointFunc func(pipeline common.Pipeline) error

// Returns the VBMasterCheck response and results, which will contain checkpoint information
type VBMasterCheckFunc func(common.Pipeline) (map[string]*peerToPeer.VBMasterCheckResp, error)

type MergeVBMasterRespCkptsFunc func(common.Pipeline, peerToPeer.PeersVBMasterCheckRespMap) error

type PrometheusPipelineStatusCb []func()

// GenericPipeline is the generic implementation of a data processing pipeline
//
// The assumption here is all the processing steps are self-connected, so
// once incoming nozzle is open, it will propel the data through all
// processing steps.
type GenericPipeline struct {
	//name of the pipeline
	topic     string
	fullTopic string

	// type of pipeline
	pipelineType common.PipelineType

	//incoming nozzles of the pipeline
	sources map[string]common.Nozzle

	//outgoing nozzles of the pipeline
	targets map[string]common.Nozzle

	//runtime context of the pipeline
	context common.PipelineRuntimeContext

	bucketTopologySvc service_def.BucketTopologySvc

	//	//communication channel with PipelineManager
	//	reqch chan []interface{}

	//the lock to serialize the request to start\stop the pipeline
	stateLock sync.RWMutex

	partSetting_constructor            PartsSettingsConstructor
	connectorSetting_constructor       ConnectorSettingsConstructor
	sslPortMapConstructor              SSLPortMapConstructor
	partUpdateSetting_constructor      PartsUpdateSettingsConstructor
	connectorUpdateSetting_constructor ConnectorsUpdateSettingsConstructor

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

	spec          metadata.GenericSpecification
	settings      metadata.ReplicationSettingsMap
	settings_lock *sync.RWMutex

	targetClusterRef *metadata.RemoteClusterReference

	state common.PipelineState

	instance_id       int
	progress_recorder common.PipelineProgressRecorder

	brokenMapMtx sync.RWMutex
	brokenMap    metadata.CollectionNamespaceMapping

	vbMasterCheckFunc VBMasterCheckFunc
	mergeCkptFunc     MergeVBMasterRespCkptsFunc

	utils                   utilities.UtilsIface
	p2pVbMasterCheckTimeout time.Duration

	topologyMtx               sync.Mutex
	sourceTopologyProgressMsg string
	targetTopologyProgressMsg string

	statusCallbacks PrometheusPipelineStatusCb
}

// Get the runtime context of this pipeline
func (genericPipeline *GenericPipeline) RuntimeContext() common.PipelineRuntimeContext {
	return genericPipeline.context
}

func (genericPipeline *GenericPipeline) SetRuntimeContext(ctx common.PipelineRuntimeContext) {
	genericPipeline.context = ctx
}

func (genericPipeline *GenericPipeline) startPartsWithTimeout(ssl_port_map map[string]uint16, errMap base.ErrorMap) {
	err_ch := make(chan base.ComponentError, 1000)
	startPartsFunc := func() error {
		for _, source := range genericPipeline.sources {
			waitGrp := &sync.WaitGroup{}
			waitGrp.Add(1)
			go func(err_ch chan base.ComponentError, source common.Nozzle, settings metadata.ReplicationSettingsMap, waitGrp *sync.WaitGroup, ssl_port_map map[string]uint16) {
				defer waitGrp.Done()
				genericPipeline.startPart(source, settings, genericPipeline.targetClusterRef, ssl_port_map, err_ch)
				if len(err_ch) == 0 {
					genericPipeline.logger.Infof("%v Incoming nozzle %s has been started", genericPipeline.InstanceId(), source.Id())
				}
				return

			}(err_ch, source, genericPipeline.settings, waitGrp, ssl_port_map)

			waitGrp.Wait()
			if len(err_ch) != 0 {
				partErrs := base.FormatErrMsgWithUpperLimit(err_ch, MaxNumberOfErrorsToTrack)
				err := fmt.Errorf("Pipeline %v failed to start, err=%v\n", genericPipeline.FullTopic(), base.FlattenErrorMap(partErrs))
				genericPipeline.logger.Errorf("%v", err)
				// This func is run serially so no need for lock - if changes in the future, need lock
				errMap.AddErrors(partErrs)
				return nil
			}
		}
		return nil
	}

	// put a timeout around part part starting to avoid being stuck
	err := base.ExecWithTimeout(startPartsFunc, base.TimeoutPartsStart, genericPipeline.logger)
	if err != nil {
		genericPipeline.logger.Errorf("%v timed out when starting pipeline parts. err=%v", genericPipeline.InstanceId(), err)
		errMap[PipelinePartStart] = err
	} else if len(errMap) > 0 {
		genericPipeline.logger.Errorf("%v error starting pipeline parts. errs=%v", genericPipeline.InstanceId(), base.FlattenErrorMap(errMap))
	} else {
		genericPipeline.logger.Infof("%v pipeline parts have started successfully", genericPipeline.InstanceId())
	}
}

// Starts the downstream parts recursively, and eventually the part itself
// In a more specific use case, for now, it starts the nozzles and routers (out-going nozzles first, then source nozzles)
func (genericPipeline *GenericPipeline) startPart(part common.Part, settings metadata.ReplicationSettingsMap,
	targetClusterRef *metadata.RemoteClusterReference, ssl_port_map map[string]uint16, err_ch chan base.ComponentError) {

	var err error = nil

	// start downstreams, such as CAPI or XMEM nozzles, before we start the actual part (i.e. DCP nozzle)
	if part.Connector() != nil {
		if part.Connector().IsStartable() {
			err = part.Connector().Start()
			if err != nil {
				err_ch <- base.ComponentError{part.Connector().Id(), err}
			}
		}

		downstreamParts := part.Connector().DownStreams()
		waitGrp := &sync.WaitGroup{}
		for _, p := range downstreamParts {
			waitGrp.Add(1)
			go func(waitGrp *sync.WaitGroup, err_ch chan base.ComponentError, p common.Part, settings metadata.ReplicationSettingsMap) {
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
			err_ch <- base.ComponentError{part.Id(), err}
			return
		}
	}

	err = part.Start(partSettings)
	if err != nil && err.Error() != parts.PartAlreadyStartedError.Error() {
		err_ch <- base.ComponentError{part.Id(), err}
	}
}

var genericPipelineIteration uint32

// Start starts the pipeline
//
// settings - a map of parameter to start the pipeline. it can contain initialization paramters
//
//	for each processing steps and for runtime context of the pipeline.
func (genericPipeline *GenericPipeline) Start(settings metadata.ReplicationSettingsMap) base.ErrorMap {
	genericPipeline.logger.Infof("Starting %v %s\n%s \n%s \nsettings = %v\n", genericPipeline.Type().String(), genericPipeline.InstanceId(), genericPipeline.Layout(), genericPipeline.Summary(), settings.CloneAndRedact())
	var errMap base.ErrorMap = make(base.ErrorMap)
	var err error

	defer func(genericPipeline *GenericPipeline, errMap map[string]error) {
		if len(errMap) > 0 {
			genericPipeline.logger.Errorf("%v failed to start, err=%v", genericPipeline.InstanceId(), errMap)
			genericPipeline.ReportProgress(fmt.Sprintf("Pipeline failed to start, err=%v", errMap))
			genericPipeline.statusCallbacks[base.PipelineStatusError]()
		} else {
			// Only set to running if there is no error starting the pipeline
			genericPipeline.statusCallbacks[base.PipelineStatusRunning]()
		}
	}(genericPipeline, errMap)

	err = genericPipeline.SetState(common.Pipeline_Starting)
	if err != nil {
		errMap["genericPipeline.SetState.Pipeline_Starting"] = err
		return errMap
	}
	genPipelineId := "GenericPipeline" + base.GetIterationId(&genericPipelineIteration)
	spec := genericPipeline.Specification().GetReplicationSpec()
	notificationCh, err := genericPipeline.bucketTopologySvc.SubscribeToLocalBucketFeed(spec, genPipelineId)
	if err != nil {
		errMap["genericPipeline.SetState.Pipeline_Starting"] = err
		return errMap
	}
	defer genericPipeline.bucketTopologySvc.UnSubscribeLocalBucketFeed(spec, genPipelineId)
	latestNotification := <-notificationCh
	defer latestNotification.Recycle()
	hlvEnable := latestNotification.GetEnableCrossClusterVersioning()
	settings[base.EnableCrossClusterVersioningKey] = hlvEnable
	pruningWindow := latestNotification.GetVersionPruningWindowHrs()
	settings[base.VersionPruningWindowHrsKey] = pruningWindow
	if hlvEnable {
		maxCas := latestNotification.GetVbucketsMaxCas()
		settings[base.VbucketsMaxCasKey] = maxCas
	}

	settings[base.VBTimestamps] = &base.ObjectWithLock{make(map[uint16]*base.VBTimestamp), &sync.RWMutex{}}
	settings[base.ProblematicVBSource] = &base.ObjectWithLock{make(map[uint16]error), &sync.RWMutex{}}
	settings[base.ProblematicVBTarget] = &base.ObjectWithLock{make(map[uint16]error), &sync.RWMutex{}}

	genericPipeline.settings = settings

	// Before starting vb timestamp, need to ensure VBMaster
	vbMasterCheckConfig, ok := settings[base.PreReplicateVBMasterCheckKey]
	if (!ok || ok && vbMasterCheckConfig.(bool) == true) && genericPipeline.Type() == common.MainPipeline {
		genericPipeline.p2pVbMasterCheckTimeout = metadata.GetP2PTimeoutFromSettings(settings)
		genericPipeline.logger.Infof("%v - Performing PeerToPeer communication and metadata merging with timeout of %v",
			genericPipeline.FullTopic(), genericPipeline.p2pVbMasterCheckTimeout)
		p2pErrMap := make(base.ErrorMap)
		genericPipeline.runP2PProtocol(&p2pErrMap)
		if len(p2pErrMap) > 0 {
			// Any P2P pull error should be ignored for now and continue
			genericPipeline.logger.Warnf("P2P PreReplicate Ckpt Pull and merge had errors but will continue to replicate: %v", p2pErrMap)
		}
		genericPipeline.ReportProgress(common.ProgressP2PDoneMerge)
	}

	//get starting vb timestamp
	go genericPipeline.startingSeqno_constructor(genericPipeline)

	// start async event listeners
	for _, async_event_listener := range GetAllAsyncComponentEventListeners(genericPipeline) {
		async_event_listener.Start()
	}

	//start the runtime
	err = genericPipeline.context.Start(genericPipeline.settings)
	if err != nil {
		errMap[PipelineContextStart] = err
		return errMap
	}
	genericPipeline.logger.Debugf("%v %v", genericPipeline.InstanceId(), common.ProgressRuntimeCtxStarted)
	genericPipeline.ReportProgress(common.ProgressRuntimeCtxStarted)

	var ssl_port_map map[string]uint16
	if genericPipeline.sslPortMapConstructor != nil {
		ssl_port_map, err = genericPipeline.sslPortMapConstructor(genericPipeline.targetClusterRef, genericPipeline.spec.GetReplicationSpec())
		if err != nil {
			errMap["genericPipeline.sslPortMapConstructor"] = err
			return errMap
		}
	}

	// apply settings to connectors in pipeline
	if genericPipeline.connectorSetting_constructor != nil {
		for _, connector := range GetAllConnectors(genericPipeline) {
			connectorSettings, err := genericPipeline.connectorSetting_constructor(genericPipeline, connector, settings)
			if err != nil {
				errMap["genericPipeline.connectorSettingConstructor"] = err
				return errMap
			}
			err = connector.UpdateSettings(connectorSettings)
			if err != nil {
				errMap["genericPipeline.UpdateSettingsToConnector"] = err
				return errMap
			}
		}
	}

	//start all the processing steps of the Pipeline
	//start the incoming nozzle which would start the downstream steps subsequently
	genericPipeline.startPartsWithTimeout(ssl_port_map, errMap)
	if len(errMap) > 0 {
		return errMap
	}

	genericPipeline.logger.Infof("%v", common.ProgressPartsStarted)
	genericPipeline.ReportProgress(common.ProgressPartsStarted)

	//open targets
	for _, target := range genericPipeline.targets {
		err = target.Open()
		if err != nil {
			genericPipeline.logger.Errorf("%v %v failed to open outgoing nozzle %s. err=%v", genericPipeline.Type(), genericPipeline.InstanceId(), target.Id(), err)
			errMap["genericPipeline.outgoingNozzle.Open"] = err
			return errMap
		}
	}
	genericPipeline.logger.Debugf("%v", common.ProgressOutNozzleOpened)
	genericPipeline.ReportProgress(common.ProgressOutNozzleOpened)

	//open source
	for _, source := range genericPipeline.sources {

		err = source.Open()
		if err != nil {
			genericPipeline.logger.Errorf("%v %v failed to open incoming nozzle %s. err=%v", genericPipeline.Type(), genericPipeline.InstanceId(), source.Id(), err)
			errMap["genericPipeline.sourceNozzle.Open"] = err
			return errMap
		}
	}
	genericPipeline.logger.Debugf("%v", common.ProgressInNozzleOpened)
	genericPipeline.ReportProgress(common.ProgressInNozzleOpened)

	err = genericPipeline.SetState(common.Pipeline_Running)
	if err == nil {
		genericPipeline.logger.Infof("----------- %v %s has been started----------", genericPipeline.Type(), genericPipeline.InstanceId())
		genericPipeline.ReportProgress(common.ProgressPipelineRunning)
	} else {
		err = fmt.Errorf("Pipeline %s when setting state to Pipeline_Running: %v", genericPipeline.FullTopic(), err)
		errMap["genericPipeline.SetState.Pipeline_running"] = err
	}

	return errMap
}

func (genericPipeline *GenericPipeline) runP2PProtocol(errMapPtr *base.ErrorMap) {
	errMap := *errMapPtr

	genericPipeline.ReportProgress(common.ProgressP2PComm)
	stopRpcMeasurement := genericPipeline.utils.StartDiagStopwatch(fmt.Sprintf("%v_vbMasterCheckFunc", genericPipeline.FullTopic()), genericPipeline.p2pVbMasterCheckTimeout)
	resp, vbMasterCheckErr := genericPipeline.vbMasterCheckFunc(genericPipeline)
	if vbMasterCheckErr != nil {
		if vbMasterCheckErr == base.ErrorOpInterrupted || vbMasterCheckErr == base.ErrorNoBackfillNeeded {
			// Pipeline paused or repl deleted
			stopRpcMeasurement()
			return
		}

		errMap["genericPipeline.vbMasterCheckFunc"] = vbMasterCheckErr
		// Even if vbmaster has issues, the data being sent should still be stored and then forwarded to others
		// to prevent data loss
	}
	stopRpcMeasurement()

	// resp is potentially nil if checkFunc failed above
	genericPipeline.ReportProgress(common.ProgressP2PMerge)
	if resp != nil {
		stopMergeMeasurement := genericPipeline.utils.StartDiagStopwatch(fmt.Sprintf("%v_vbMasterMergeFunc", genericPipeline.FullTopic()), base.DiagCkptMergeThreshold)
		mergeCkptErr := genericPipeline.mergeCkptFunc(genericPipeline, resp)
		// If error returned is ErrorNoBackfillNeeded, then it's considered not an error
		if mergeCkptErr != nil && mergeCkptErr != base.ErrorNoBackfillNeeded {
			errMap[MergeCkptFuncKey] = mergeCkptErr
		}
		stopMergeMeasurement()
	}
	return
}

func (genericPipeline *GenericPipeline) stopConnectorsWithTimeout() base.ErrorMap {
	errMap := make(base.ErrorMap)
	connectorsMap := GetAllConnectors(genericPipeline)

	err_ch := make(chan base.ComponentError, len(connectorsMap))

	stopConnectorsFunc := func() error {
		wait_grp := &sync.WaitGroup{}
		for _, connector := range connectorsMap {
			wait_grp.Add(1)
			go genericPipeline.stopConnector(connector, wait_grp, err_ch)
		}

		wait_grp.Wait()
		if len(err_ch) != 0 {
			errMap = base.FormatErrMsgWithUpperLimit(err_ch, MaxNumberOfErrorsToTrack)
		}
		return nil
	}

	// put a timeout around part stopping to avoid being stuck
	err := base.ExecWithTimeout(stopConnectorsFunc, base.TimeoutConnectorsStop, genericPipeline.logger)
	if err != nil {
		// if err is not nill, it is possible that stopPartsFunc is still running and may still access errMap
		// return errMap1 instead of errMap to avoid race conditions
		genericPipeline.logger.Warnf("%v error stopping pipeline connector.", genericPipeline.InstanceId(), err)
		errMap1 := make(base.ErrorMap)
		errMap1["genericPipeline.StopConnectors"] = err
		return errMap1
	}

	genericPipeline.logger.Infof("%v pipeline connectors have stopped successfully", genericPipeline.InstanceId())
	return errMap
}

func (genericPipeline *GenericPipeline) stopPartsWithTimeout() base.ErrorMap {
	errMap := make(base.ErrorMap)

	partsMap := GetAllParts(genericPipeline)
	err_ch := make(chan base.ComponentError, len(partsMap))

	stopPartsFunc := func() error {
		wait_grp := &sync.WaitGroup{}
		for _, part := range partsMap {
			wait_grp.Add(1)
			// stop parts in parallel to ensure that all parts get their turns
			go genericPipeline.stopPart(part, wait_grp, err_ch)
		}

		wait_grp.Wait()
		if len(err_ch) != 0 {
			errMap = base.FormatErrMsgWithUpperLimit(err_ch, MaxNumberOfErrorsToTrack)
		}
		return nil
	}

	// put a timeout around part stopping to avoid being stuck
	err := base.ExecWithTimeout(stopPartsFunc, base.TimeoutPartsStop, genericPipeline.logger)
	if err != nil {
		// if err is not nill, it is possible that stopPartsFunc is still running and may still access errMap
		// return errMap1 instead of errMap to avoid race conditions
		genericPipeline.logger.Warnf("%v error stopping pipeline parts %v", genericPipeline.InstanceId(), err)
		errMap1 := make(base.ErrorMap)
		errMap1["genericPipeline.StopParts"] = err
		return errMap1
	}

	genericPipeline.logger.Infof("%v pipeline parts have stopped successfully", genericPipeline.InstanceId())
	return errMap
}

func (genericPipeline *GenericPipeline) stopPart(part common.Part, wait_grp *sync.WaitGroup, err_ch chan base.ComponentError) {
	defer wait_grp.Done()
	genericPipeline.logger.Infof("%v trying to stop part %v\n", genericPipeline.InstanceId(), part.Id())
	err := part.Stop()
	if err != nil {
		genericPipeline.logger.Warnf("%v failed to stop part %v, err=%v, let it alone to die\n", genericPipeline.InstanceId(), part.Id(), err)
		err_ch <- base.ComponentError{part.Id(), err}
	}
}

func (genericPipeline *GenericPipeline) stopConnector(connector common.Connector, wait_grp *sync.WaitGroup, err_ch chan base.ComponentError) {
	defer wait_grp.Done()
	genericPipeline.logger.Infof("%v trying to stop connector %v\n", genericPipeline.InstanceId(), connector.Id())
	err := connector.Stop()
	if err != nil {
		genericPipeline.logger.Warnf("%v failed to stop connector %v, err=%v, let it alone to die\n", genericPipeline.InstanceId(), connector.Id(), err)
		err_ch <- base.ComponentError{connector.Id(), err}
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

// Stop stops the pipeline
// it can result the pipeline in either "Stopped" if the operation is successful or "Pending" otherwise
func (genericPipeline *GenericPipeline) Stop() base.ErrorMap {
	var errMap base.ErrorMap = make(base.ErrorMap)

	genericPipeline.logger.Infof("Stopping %v %v\n", genericPipeline.Type().String(), genericPipeline.InstanceId())
	var err error

	err = genericPipeline.SetState(common.Pipeline_Stopping)
	if err != nil {
		errMap["genericPipeline.SetState.Pipeline_Stopping"] = err
		return errMap
	}

	if genericPipeline.spec != nil && genericPipeline.spec.GetReplicationSpec() != nil {
		if !genericPipeline.spec.GetReplicationSpec().Settings.Active {
			// Only mark the pipeline paused if the user requested it (or auto paused)
			// Otherwise, any stop() could be called during error phase and we want to keep
			// the pipeline in error phase
			genericPipeline.statusCallbacks[base.PipelineStatusPaused]()
		}
	}

	// perform checkpoint first before stopping
	genericPipeline.checkpoint_func(genericPipeline)

	// stop async event listeners so that RaiseEvent() would not block
	for _, asyncEventListener := range GetAllAsyncComponentEventListeners(genericPipeline) {
		asyncEventListener.Stop()
	}
	genericPipeline.logger.Infof("%v %v Async listeners have been stopped", genericPipeline.Type(), genericPipeline.InstanceId())
	genericPipeline.ReportProgress(common.ProgressAsyncStopped)

	// stop services before stopping parts to avoid spurious errors from services
	contextErrMap := genericPipeline.context.Stop()
	base.ConcatenateErrors(errMap, contextErrMap, MaxNumberOfErrorsToTrack, genericPipeline.logger)
	genericPipeline.ReportProgress(common.ProgressRuntimeCtxStopped)

	//close the sources
	for _, source := range genericPipeline.sources {
		err = source.Close()
		if err != nil {
			genericPipeline.logger.Warnf("%v %v failed to close source %v. err=%v", genericPipeline.Type(), genericPipeline.InstanceId(), source.Id(), err)
			errMap[fmt.Sprintf("genericPipeline.%v.Close", source.Id())] = err
		}
	}
	genericPipeline.logger.Infof("%v %v %v", genericPipeline.Type(), genericPipeline.InstanceId(), common.ProgressSrcNozzleClose)
	genericPipeline.ReportProgress(common.ProgressSrcNozzleClose)

	partsErrMap := genericPipeline.stopPartsWithTimeout()
	base.ConcatenateErrors(errMap, partsErrMap, MaxNumberOfErrorsToTrack, genericPipeline.logger)

	connectorsErrMap := genericPipeline.stopConnectorsWithTimeout()
	base.ConcatenateErrors(errMap, connectorsErrMap, MaxNumberOfErrorsToTrack, genericPipeline.logger)

	genericPipeline.ReportProgress(common.ProgressPipelineStopped)

	err = genericPipeline.SetState(common.Pipeline_Stopped)
	if err != nil {
		errMap["genericPipeline.SetState.Pipeline_Stopped"] = err
	}

	genericPipeline.logger.Infof("%v %v has been stopped\n errMap=%v", genericPipeline.Type(), genericPipeline.InstanceId(), errMap)
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

func (genericPipeline *GenericPipeline) FullTopic() string {
	return genericPipeline.fullTopic
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

func NewPipelineWithSettingConstructor(t string, pipelineType common.PipelineType, sources map[string]common.Nozzle, targets map[string]common.Nozzle, spec metadata.GenericSpecification, targetClusterRef *metadata.RemoteClusterReference, partsSettingsConstructor PartsSettingsConstructor, connectorSettingsConstructor ConnectorSettingsConstructor, sslPortMapConstructor SSLPortMapConstructor, partsUpdateSettingsConstructor PartsUpdateSettingsConstructor, connectorUpdateSetting_constructor ConnectorsUpdateSettingsConstructor, startingSeqnoConstructor StartingSeqnoConstructor, checkpoint_func CheckpointFunc, logger_context *log.LoggerContext, vbMasterCheckFunc VBMasterCheckFunc, mergeCkptFunc MergeVBMasterRespCkptsFunc, bucketTopologySvc service_def.BucketTopologySvc, utils utilities.UtilsIface, prometheusStatusCbConstructor func(pipeline common.Pipeline) PrometheusPipelineStatusCb) *GenericPipeline {
	pipeline := &GenericPipeline{topic: t,
		sources:                            sources,
		targets:                            targets,
		spec:                               spec,
		bucketTopologySvc:                  bucketTopologySvc,
		targetClusterRef:                   targetClusterRef,
		partSetting_constructor:            partsSettingsConstructor,
		connectorSetting_constructor:       connectorSettingsConstructor,
		sslPortMapConstructor:              sslPortMapConstructor,
		partUpdateSetting_constructor:      partsUpdateSettingsConstructor,
		connectorUpdateSetting_constructor: connectorUpdateSetting_constructor,
		startingSeqno_constructor:          startingSeqnoConstructor,
		checkpoint_func:                    checkpoint_func,
		logger:                             log.NewLogger("GenericPipeline", logger_context),
		instance_id:                        time.Now().Nanosecond(),
		state:                              common.Pipeline_Initial,
		settings_lock:                      &sync.RWMutex{},
		pipelineType:                       pipelineType,
		vbMasterCheckFunc:                  vbMasterCheckFunc,
		mergeCkptFunc:                      mergeCkptFunc,
		utils:                              utils,
	}
	// NOTE: Calling initialize here as part of constructor
	pipeline.statusCallbacks = prometheusStatusCbConstructor(pipeline)
	pipeline.initialize()
	pipeline.logger.Debugf("Pipeline %s has been initialized with a part setting constructor %v", t, partsSettingsConstructor)

	return pipeline
}

// intialize all maps
// the maps will not be modified at pipeline runtime, hence there is no chance of concurrent read and write to the maps
func (genericPipeline *GenericPipeline) initialize() {
	genericPipeline.initFullTopic()

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

func (genericPipeline *GenericPipeline) initFullTopic() {
	genericPipeline.fullTopic = common.ComposeFullTopic(genericPipeline.topic, genericPipeline.Type())
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
	asyncListenerMap := p.GetAsyncListenerMap()
	if asyncListenerMap == nil {
		asyncListenerMap = make(map[string]common.AsyncComponentEventListener)
		partsMap := make(map[string]common.Part)
		// add async listeners on parts and connectors to listeners map
		for _, source := range p.Sources() {
			addAsyncListenersToMap(source, asyncListenerMap, partsMap)
		}
		p.SetAsyncListenerMap(asyncListenerMap)
	}
	return asyncListenerMap
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

func (genericPipeline *GenericPipeline) Specification() metadata.GenericSpecification {
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
	return fmt.Sprintf("%v-%v", genericPipeline.fullTopic, genericPipeline.instance_id)
}

func (genericPipeline *GenericPipeline) String() string {
	return genericPipeline.InstanceId()
}

func (genericPipeline *GenericPipeline) Layout() string {
	header := fmt.Sprintf("------------%s---------------", genericPipeline.Topic())
	footer := "-----------------------------"
	content := ""
	for _, sourceNozzle := range genericPipeline.Sources() {
		vbsList := sourceNozzle.ResponsibleVBs()
		dcpSection := fmt.Sprintf("\t%s:{vbList=%v}\n", sourceNozzle.Id(), vbsList)
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

func (genericPipeline *GenericPipeline) Summary() string {
	spec := genericPipeline.spec.GetReplicationSpec()
	var totalVBs []uint16
	for _, sourceNozzle := range genericPipeline.Sources() {
		vbsList := sourceNozzle.ResponsibleVBs()
		totalVBs = append(totalVBs, vbsList...)
	}
	return fmt.Sprintf("Remote Cluster: %v SourceBucket: %v TargetBucket: %v SortedVBs: %v\n",
		spec.TargetClusterUUID, spec.SourceBucketName, spec.TargetBucketName, base.SortUint16List(totalVBs))
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
		genericPipeline.logger.Infof("%v update settings called with settings=%v\n", genericPipeline.InstanceId(), redactedSettings)
	}

	if len(settings) == 0 {
		return nil
	}

	// update settings in pipeline itself
	genericPipeline.updatePipelineSettings(settings)

	// update settings on parts and services in pipeline
	if genericPipeline.partUpdateSetting_constructor != nil {
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

	// update settings on connectors in pipeline
	if genericPipeline.connectorUpdateSetting_constructor != nil {
		if genericPipeline.logger.GetLogLevel() >= log.LogLevelDebug {
			redactOnce()
			genericPipeline.logger.Debugf("%v calling connector update setting constructor with settings=%v\n", genericPipeline.InstanceId(), redactedSettings)
		}
		for _, connector := range GetAllConnectors(genericPipeline) {
			connectorSettings, err := genericPipeline.connectorUpdateSetting_constructor(genericPipeline, connector, settings)
			if err != nil {
				return err
			}
			err = connector.UpdateSettings(connectorSettings)
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
	genericPipeline.updateTopologyProgress(settings)
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

// Should avoid concurrent use
func (genericPipeline *GenericPipeline) GetAsyncListenerMap() map[string]common.AsyncComponentEventListener {
	return genericPipeline.asyncEventListenerMap
}

func (genericPipeline *GenericPipeline) SetAsyncListenerMap(asyncListenerMap map[string]common.AsyncComponentEventListener) {
	genericPipeline.asyncEventListenerMap = asyncListenerMap
}

func (genericPipeline *GenericPipeline) Type() common.PipelineType {
	return genericPipeline.pipelineType
}

// The map itself is not cloned, so to prevent concurrent modification, the caller must call the unlock func once done
func (genericPipeline *GenericPipeline) GetBrokenMapRO() (metadata.CollectionNamespaceMapping, func()) {
	genericPipeline.brokenMapMtx.RLock()
	unlockFunc := func() {
		genericPipeline.brokenMapMtx.RUnlock()
	}
	return genericPipeline.brokenMap, unlockFunc
}

func (genericPipeline *GenericPipeline) SetBrokenMap(brokenMap metadata.CollectionNamespaceMapping) {
	genericPipeline.brokenMapMtx.Lock()
	defer genericPipeline.brokenMapMtx.Unlock()
	genericPipeline.brokenMap = brokenMap
}

func (genericPipeline *GenericPipeline) updateTopologyProgress(settings metadata.ReplicationSettingsMap) {
	srcMsg, srcOK := settings[metadata.SourceTopologyChangeStatusKey].(string)
	if srcOK {
		genericPipeline.topologyMtx.Lock()
		genericPipeline.sourceTopologyProgressMsg = srcMsg
		genericPipeline.topologyMtx.Unlock()
	}

	tgtMsg, tgtOK := settings[metadata.TargetTopologyChangeStatusKey].(string)
	if tgtOK {
		genericPipeline.topologyMtx.Lock()
		genericPipeline.targetTopologyProgressMsg = tgtMsg
		genericPipeline.topologyMtx.Unlock()
	}
}

func (genericPipeline *GenericPipeline) GetRebalanceProgress() (string, string) {
	genericPipeline.topologyMtx.Lock()
	defer genericPipeline.topologyMtx.Unlock()

	return genericPipeline.sourceTopologyProgressMsg, genericPipeline.targetTopologyProgressMsg
}

// enforcer for GenericPipeline to implement Pipeline
var _ common.Pipeline = (*GenericPipeline)(nil)
