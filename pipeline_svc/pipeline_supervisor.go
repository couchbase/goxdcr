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
	"encoding/json"
	"errors"
	"fmt"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/supervisor"
	utilities "github.com/couchbase/goxdcr/utils"
	"reflect"
	"strings"
	"sync"
	"time"
)

//configuration settings
const (
	PIPELINE_LOG_LEVEL         = "pipeline_loglevel"
	default_pipeline_log_level = log.LogLevelInfo
)

const (
	health_check_interval          = 120 * time.Second
	default_max_dcp_miss_count     = 10
	filterErrCheckAndPrintInterval = 5 * time.Second
	maxFilterErrorsPerInterval     = 20
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
	kv_mem_clients      map[string]mcc.ClientIface
	kv_mem_clients_lock *sync.Mutex

	user_agent string

	utils utilities.UtilsIface

	// Filtering Errors should be regulated for printing otherwise to prevent log flooding
	filterErrCh chan error
}

func NewPipelineSupervisor(id string, logger_ctx *log.LoggerContext, failure_handler common.SupervisorFailureHandler,
	cluster_info_svc service_def.ClusterInfoSvc, xdcr_topology_svc service_def.XDCRCompTopologySvc,
	utilsIn utilities.UtilsIface) *PipelineSupervisor {
	supervisor := supervisor.NewGenericSupervisor(id, logger_ctx, failure_handler, nil, utilsIn)
	pipelineSupervisor := &PipelineSupervisor{GenericSupervisor: supervisor,
		errors_seen:         make(map[string]error),
		errors_seen_lock:    &sync.RWMutex{},
		cluster_info_svc:    cluster_info_svc,
		xdcr_topology_svc:   xdcr_topology_svc,
		kv_mem_clients:      make(map[string]mcc.ClientIface),
		kv_mem_clients_lock: &sync.Mutex{},
		utils:               utilsIn,
		filterErrCh:         make(chan error, maxFilterErrorsPerInterval),
	}
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
	pipelineSupervisor.setMaxDcpMissCount(settings)

	// do the generic supervisor start stuff
	err := pipelineSupervisor.GenericSupervisor.Start(settings)
	if err != nil {
		return err
	}

	// start an additional go routine for pipeline health monitoring
	pipelineSupervisor.GenericSupervisor.ChidrenWaitGroup().Add(2)
	go pipelineSupervisor.monitorPipelineHealth()
	go pipelineSupervisor.checkAndLogFilterErrors()

	return err
}

func (pipelineSupervisor *PipelineSupervisor) setMaxDcpMissCount(settings metadata.ReplicationSettingsMap) {
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

	pipelineSupervisor.Logger().Infof("%v updating max_dcp_miss_count to %v\n", pipelineSupervisor.Id(), max_dcp_miss_count)

	for _, dcp_nozzle := range pipelineSupervisor.pipeline.Sources() {
		dcp_nozzle.(*parts.DcpNozzle).SetMaxMissCount(max_dcp_miss_count)
	}
}

func (pipelineSupervisor *PipelineSupervisor) Stop() error {
	// do the generic supervisor stop stuff
	err := pipelineSupervisor.GenericSupervisor.Stop()
	if err != nil {
		pipelineSupervisor.Logger().Warnf("%v received error when stopping, %v\n", pipelineSupervisor.Id(), err)
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
			pipelineSupervisor.Logger().Warnf("%v error from closing connection for %v is %v\n", pipelineSupervisor.Id(), serverAddr, err)
		}
	}
	pipelineSupervisor.kv_mem_clients = make(map[string]mcc.ClientIface)
}

func (pipelineSupervisor *PipelineSupervisor) monitorPipelineHealth() error {
	pipelineSupervisor.Logger().Infof("%v monitorPipelineHealth started", pipelineSupervisor.Id())

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
			err := base.ExecWithTimeout(pipelineSupervisor.checkPipelineHealth, 1000*time.Millisecond, pipelineSupervisor.Logger())
			if err != nil {
				if err == base.ExecutionTimeoutError {
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
			pipelineSupervisor.Logger().Warnf("%v Received nil error from part %v\n", pipelineSupervisor.pipeline.Topic(), partId)
		}
		pipelineSupervisor.setError(partId, err)
	case common.VBErrorEncountered:
		additionalInfo := event.OtherInfos.(*base.VBErrorEventAdditional)
		vbno := additionalInfo.Vbno
		err = additionalInfo.Error
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
	case common.DataUnableToFilter:
		err = event.DerivedData[0].(error)
		pipelineSupervisor.logFilterError(err, string(event.DerivedData[1].(string)))
		if base.FilterErrorIsRecoverable(err) {
			// Raise error so pipeline will restart and take recovery actions
			pipelineSupervisor.setError(event.Component.Id(), err)
		} else if pipelineSupervisor.Logger().GetLogLevel() >= log.LogLevelDebug {
			uprEvent := event.Data.(*mcc.UprEvent)
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

	updateIntervalObj := pipelineSupervisor.utils.GetSettingFromSettings(settings, PUBLISH_INTERVAL)
	if updateIntervalObj != nil {
		pipelineSupervisor.setMaxDcpMissCount(settings)
	}

	return nil
}

func (pipelineSupervisor *PipelineSupervisor) ReportFailure(errors map[string]error) {
	pipelineSupervisor.StopHeartBeatTicker()
	//report the failure to decision maker
	pipelineSupervisor.GenericSupervisor.ReportFailure(errors)
}

// check if any runtime stats indicates that pipeline is broken
func (pipelineSupervisor *PipelineSupervisor) checkPipelineHealth() error {
	if !pipeline_utils.IsPipelineRunning(pipelineSupervisor.pipeline.State()) {
		//the pipeline is no longer running, kill myself
		message := "Pipeline is no longer running, exit."
		pipelineSupervisor.Logger().Infof("%v %v", pipelineSupervisor.Id(), message)
		return errors.New(message)
	}

	dcp_stats, err := pipelineSupervisor.getDcpStats()
	if err != nil {
		pipelineSupervisor.Logger().Errorf("%v Failed to get dcp stats. Skipping dcp health check.", pipelineSupervisor.Id())
		return nil
	}

	for _, dcp_nozzle := range pipelineSupervisor.pipeline.Sources() {
		err = dcp_nozzle.(*parts.DcpNozzle).CheckStuckness(dcp_stats)
		if err != nil {
			pipelineSupervisor.setError(dcp_nozzle.Id(), err)
			return err
		}
	}

	return nil
}

// compose user agent string for HELO command
func (pipelineSupervisor *PipelineSupervisor) composeUserAgent() {
	spec := pipelineSupervisor.pipeline.Specification()
	pipelineSupervisor.user_agent = base.ComposeUserAgentWithBucketNames("Goxdcr PipelineSupervisor", spec.SourceBucketName, spec.TargetBucketName)
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
		client, err := pipelineSupervisor.utils.GetMemcachedClient(serverAddr, bucketName, pipelineSupervisor.kv_mem_clients, pipelineSupervisor.user_agent, base.KeepAlivePeriod, pipelineSupervisor.Logger())
		if err != nil {
			return nil, err
		}

		stats_map, err := client.StatsMap(base.DCP_STAT_NAME)
		if err != nil {
			pipelineSupervisor.Logger().Warnf("%v Error getting dcp stats for kv %v. err=%v", pipelineSupervisor.Id(), serverAddr, err)
			err1 := client.Close()
			if err1 != nil {
				pipelineSupervisor.Logger().Warnf("%v error from closing connection for %v is %v\n", pipelineSupervisor.Id(), serverAddr, err1)
			}
			delete(pipelineSupervisor.kv_mem_clients, serverAddr)
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
