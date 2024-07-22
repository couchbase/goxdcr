// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package pipeline_svc

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/supervisor"
	utilities "github.com/couchbase/goxdcr/utils"
)

// configuration settings
const (
	PIPELINE_LOG_LEVEL         = "pipeline_loglevel"
	default_pipeline_log_level = log.LogLevelInfo
)

const (
	filterErrCheckAndPrintInterval = 5 * time.Second
	maxFilterErrorsPerInterval     = 20
)

var pipeline_supervisor_setting_defs base.SettingDefinitions = base.SettingDefinitions{supervisor.HEARTBEAT_TIMEOUT: base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	PIPELINE_LOG_LEVEL:            base.NewSettingDef(reflect.TypeOf((*log.LogLevel)(nil)), false),
	supervisor.HEARTBEAT_INTERVAL: base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false)}

type PipelineSupervisorSvc interface {
	common.PipelineService
	common.ComponentEventListener
}

type PipelineSupervisor struct {
	*supervisor.GenericSupervisor
	pipeline         common.Pipeline
	errors_seen      map[string]error
	errors_seen_lock *sync.RWMutex

	xdcr_topology_svc service_def.XDCRCompTopologySvc
	remoteClusterSvc  service_def.RemoteClusterSvc
	bucketTopologySvc service_def.BucketTopologySvc

	user_agent string

	utils utilities.UtilsIface

	// Filtering Errors should be regulated for printing otherwise to prevent log flooding
	filterErrCh chan error
}

func NewPipelineSupervisor(id string, logger_ctx *log.LoggerContext, failure_handler common.SupervisorFailureHandler, xdcr_topology_svc service_def.XDCRCompTopologySvc, utilsIn utilities.UtilsIface, remoteClusterSvc service_def.RemoteClusterSvc, bucketTopologySvc service_def.BucketTopologySvc) *PipelineSupervisor {
	supervisor := supervisor.NewGenericSupervisor(id, logger_ctx, failure_handler, nil, utilsIn)
	pipelineSupervisor := &PipelineSupervisor{GenericSupervisor: supervisor,
		errors_seen:       make(map[string]error),
		errors_seen_lock:  &sync.RWMutex{},
		xdcr_topology_svc: xdcr_topology_svc,
		utils:             utilsIn,
		filterErrCh:       make(chan error, maxFilterErrorsPerInterval),
		remoteClusterSvc:  remoteClusterSvc,
		bucketTopologySvc: bucketTopologySvc,
	}
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
		connector.RegisterComponentEventListener(common.DataUnableToFilter, pipelineSupervisor)
		pipelineSupervisor.Logger().Debugf("Registering ErrorEncountered event on connector %v\n", connector.Id())
	}

	return nil
}

func (pipelineSupervisor *PipelineSupervisor) Start(settings metadata.ReplicationSettingsMap) error {
	// do the generic supervisor start stuff
	err := pipelineSupervisor.GenericSupervisor.Start(settings)
	if err != nil {
		return err
	}

	pipelineSupervisor.GenericSupervisor.ChidrenWaitGroup().Add(1)
	go pipelineSupervisor.checkAndLogFilterErrors()
	return err
}

func (pipelineSupervisor *PipelineSupervisor) Stop() error {
	// do the generic supervisor stop stuff
	err := pipelineSupervisor.GenericSupervisor.Stop()
	if err != nil {
		pipelineSupervisor.Logger().Warnf("%v received error when stopping, %v\n", pipelineSupervisor.Id(), err)
	}

	return nil
}

func (pipelineSupervisor *PipelineSupervisor) logFilterError(err error, desc string) {
	combinedErr := fmt.Errorf("Filter error: %v - %v", err, desc)
	select {
	case pipelineSupervisor.filterErrCh <- combinedErr:
		// Error added to channel
	default:
		// Error channel is full. Can't add anymore. Have to drop
	}
}

func (pipelineSupervisor *PipelineSupervisor) checkAndLogFilterErrors() {
	pipelineSupervisor.Logger().Infof("%v checkAndLogFilterErrors started", pipelineSupervisor.Id())

	defer pipelineSupervisor.GenericSupervisor.ChidrenWaitGroup().Done()

	filterCheckTicker := time.NewTicker(filterErrCheckAndPrintInterval)
	defer filterCheckTicker.Stop()

	fin_ch := pipelineSupervisor.GenericSupervisor.FinishChannel()

	for {
		select {
		case <-fin_ch:
			pipelineSupervisor.Logger().Infof("checkAndLogFilterErrors routine is exiting because parent supervisor %v has been stopped\n", pipelineSupervisor.Id())
			return
		case <-filterCheckTicker.C:
			var msgsPrinted int
			var errMsgs []string
			select {
			case errMsg := <-pipelineSupervisor.filterErrCh:
				msgsPrinted++
				if msgsPrinted > maxFilterErrorsPerInterval {
					break
				}
				errMsgs = append(errMsgs, errMsg.Error())
			default:
				// No error msgs
				break
			}
			if len(errMsgs) > 0 {
				pipelineSupervisor.Logger().Warnf("Last %v filtering errors: %v", msgsPrinted, strings.Join(errMsgs, ", "))
			}
		}
	}
	return
}

func (pipelineSupervisor *PipelineSupervisor) OnEvent(event *common.Event) {
	var err error
	switch event.EventType {
	case common.ErrorEncountered:
		partId := event.Component.Id()
		if event.OtherInfos != nil {
			err = event.OtherInfos.(error)
		} else {
			// should not get here
			err = errors.New("Unknown")
			pipelineSupervisor.Logger().Warnf("Received nil error from part %v\n", partId)
		}
		pipelineSupervisor.setError(partId, err)
	case common.VBErrorEncountered:
		additionalInfo := event.OtherInfos.(*base.VBErrorEventAdditional)
		vbno := additionalInfo.Vbno
		err = additionalInfo.Error
		errType := additionalInfo.ErrorType
		pipelineSupervisor.Logger().Debugf("Received error report on vb %v. err=%v, err type=%v\n", vbno, err, errType)
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
	case common.DataUnableToFilter:
		err = event.DerivedData[0].(error)
		pipelineSupervisor.logFilterError(err, string(event.DerivedData[1].(string)))
		if base.FilterErrorIsRecoverable(err) {
			// Raise error so pipeline will restart and take recovery actions
			pipelineSupervisor.setError(event.Component.Id(), err)
		} else if pipelineSupervisor.Logger().GetLogLevel() >= log.LogLevelDebug {
			uprEvent := event.Data.(*mcc.UprEvent)
			if uprEvent == nil {
				return
			}
			uprDumpBytes, err := json.Marshal(*uprEvent)
			if err == nil {
				pipelineSupervisor.Logger().Debugf("Failed filtering uprEvent dump\n%v%v%v\n", base.UdTagBegin, string(uprDumpBytes), base.UdTagEnd)
			}
		}
	default:
		pipelineSupervisor.Logger().Errorf("%v Pipeline supervisor didn't register to recieve event %v for component %v", pipelineSupervisor.Id(), event.EventType, event.Component.Id())
	}
}

func (pipelineSupervisor *PipelineSupervisor) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	pipelineSupervisor.Logger().Debugf("Updating settings on pipelineSupervisor %v. settings=%v\n", pipelineSupervisor.Id(), settings)
	logLevelObj := pipelineSupervisor.utils.GetSettingFromSettings(settings, PIPELINE_LOG_LEVEL)

	if logLevelObj != nil {
		logLevel, ok := logLevelObj.(log.LogLevel)
		if !ok {
			return fmt.Errorf("Log level %v is of wrong type %v", logLevelObj, reflect.TypeOf(logLevelObj))
		}
		pipelineSupervisor.LoggerContext().SetLogLevel(logLevel)
		pipelineSupervisor.Logger().Infof("%v Updated log level to %v\n", pipelineSupervisor.Id(), logLevel)
	}

	return nil
}

func (pipelineSupervisor *PipelineSupervisor) ReportFailure(errors map[string]error) {
	pipelineSupervisor.StopHeartBeatTicker()
	//report the failure to decision maker
	pipelineSupervisor.GenericSupervisor.ReportFailure(errors)
}

func (pipelineSupervisor *PipelineSupervisor) setError(partId string, err error) {
	pipelineSupervisor.errors_seen_lock.Lock()
	defer pipelineSupervisor.errors_seen_lock.Unlock()

	err1 := pipelineSupervisor.pipeline.SetState(common.Pipeline_Error)
	if err1 == nil {
		pipelineSupervisor.errors_seen[partId] = err
		pipelineSupervisor.Logger().Errorf("%v Received error report : %v\n. errors_seen=%v\n", pipelineSupervisor.Id(), err, pipelineSupervisor.errors_seen)
		pipelineSupervisor.ReportFailure(pipelineSupervisor.errors_seen)
		pipelineSupervisor.pipeline.ReportProgress(fmt.Sprintf("Received error report : %v", err))

	} else {
		pipelineSupervisor.Logger().Infof("%v Received error report : %v, but error is ignored. pipeline_state=%v\n errors_seen=%v\n", pipelineSupervisor.Id(), err, pipelineSupervisor.pipeline.State(), pipelineSupervisor.errors_seen)
	}
}

func (pipelineSupervisor *PipelineSupervisor) IsSharable() bool {
	return false
}

func (pipelineSupervisor *PipelineSupervisor) Detach(pipeline common.Pipeline) error {
	return base.ErrorNotSupported
}
