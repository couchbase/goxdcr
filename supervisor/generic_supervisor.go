// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package supervisor

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/gen_server"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	utilities "github.com/couchbase/goxdcr/v8/utils"
)

// Generic implementation of the Supervisor interface

// configuration settings
const (
	// interval of sending heart beat signals to children
	HEARTBEAT_INTERVAL = "heartbeat_interval"
	// child is considered to have missed a heart beat if it did not respond within this timeout period
	HEARTBEAT_TIMEOUT = "heartbeat_timeout"
	// interval to check heart beat responses from children
	HEARTBEAT_RESP_CHECK_INTERVAL = "heartbeat_resp_resp_check_interval"
	// child is considered to be broken if it had missed this number of heart beats consecutively
	MISSED_HEARTBEAT_THRESHOLD = "missed_heartbeat_threshold"

	default_heartbeat_interval            time.Duration = 3000 * time.Millisecond
	default_heartbeat_resp_check_interval time.Duration = 500 * time.Millisecond
	default_heartbeat_timeout             time.Duration = 4000 * time.Millisecond

	// adminport could miss heart beat because it is actively processing rest requests.
	// since we use a http read timeout of 180 seconds, the time threshold for heartbeat miss should be larger than 180 seconds.
	// setting it to 600 seconds to be conservative, since false positive will get goxdcr process restarted.
	// hence the heartbeat miss count threshold is 600/3 = 200
	default_missed_heartbeat_threshold = 200
)

var supervisor_setting_defs base.SettingDefinitions = base.SettingDefinitions{HEARTBEAT_TIMEOUT: base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	HEARTBEAT_INTERVAL:         base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	MISSED_HEARTBEAT_THRESHOLD: base.NewSettingDef(reflect.TypeOf((*uint16)(nil)), false)}

type heartbeatRespStatus int

const (
	skip            heartbeatRespStatus = iota
	notYetResponded heartbeatRespStatus = iota
	respondedOk     heartbeatRespStatus = iota
	respondedNotOk  heartbeatRespStatus = iota
)

type GenericSupervisor struct {
	id string
	gen_server.GenServer
	children                      map[string]common.Supervisable
	children_lock                 sync.RWMutex
	loggerContext                 *log.LoggerContext
	heartbeat_timeout             time.Duration
	heartbeat_interval            time.Duration
	heartbeat_resp_check_interval time.Duration
	missed_heartbeat_threshold    uint16
	// key - child Id; value - number of consecutive heart beat misses
	childrenBeatMissedMap map[string]uint16
	heartbeat_ticker      *time.Ticker
	failure_handler       common.SupervisorFailureHandler
	finch                 chan bool
	childrenWaitGrp       sync.WaitGroup
	err_ch                chan bool
	parent_supervisor     *GenericSupervisor
	utils                 utilities.UtilsIface
}

func NewGenericSupervisor(id string, logger_ctx *log.LoggerContext, failure_handler common.SupervisorFailureHandler, parent_supervisor *GenericSupervisor, utilsIn utilities.UtilsIface) *GenericSupervisor {
	server := gen_server.NewGenServer(nil,
		nil, nil, logger_ctx, "GenericSupervisor", utilsIn)
	supervisor := &GenericSupervisor{id: id,
		GenServer:                     server,
		children:                      make(map[string]common.Supervisable, 0),
		loggerContext:                 logger_ctx,
		heartbeat_timeout:             default_heartbeat_timeout,
		heartbeat_interval:            default_heartbeat_interval,
		heartbeat_resp_check_interval: default_heartbeat_resp_check_interval,
		missed_heartbeat_threshold:    default_missed_heartbeat_threshold,
		childrenBeatMissedMap:         make(map[string]uint16, 0),
		failure_handler:               failure_handler,
		finch:                         make(chan bool, 1),
		childrenWaitGrp:               sync.WaitGroup{},
		err_ch:                        make(chan bool, 1),
		parent_supervisor:             parent_supervisor,
		utils:                         utilsIn,
	}

	if parent_supervisor != nil {
		parent_supervisor.AddChild(supervisor)
	}
	return supervisor
}

func (supervisor *GenericSupervisor) Id() string {
	return supervisor.id
}

func (supervisor *GenericSupervisor) LoggerContext() *log.LoggerContext {
	return supervisor.loggerContext
}

func (supervisor *GenericSupervisor) AddChild(child common.Supervisable) error {
	supervisor.Logger().Infof("Adding child %v to supervisor %v\n", child.Id(), supervisor.Id())

	supervisor.children_lock.Lock()
	defer supervisor.children_lock.Unlock()
	supervisor.children[child.Id()] = child
	supervisor.childrenBeatMissedMap[child.Id()] = 0
	return nil
}

func (supervisor *GenericSupervisor) RemoveChild(childId string) error {
	return supervisor.removeChild_internal(childId, true)
}

func (supervisor *GenericSupervisor) removeChild_internal(childId string, lock bool) error {
	supervisor.Logger().Infof("Removing child %v from supervisor %v\n", childId, supervisor.Id())
	if lock {
		supervisor.children_lock.Lock()
		defer supervisor.children_lock.Unlock()
	}
	// TODO should we return error when childId does not exist?
	delete(supervisor.children, childId)
	delete(supervisor.childrenBeatMissedMap, childId)
	return nil
}

func (supervisor *GenericSupervisor) Child(childId string) (common.Supervisable, error) {
	supervisor.children_lock.RLock()
	defer supervisor.children_lock.RUnlock()
	if child, ok := supervisor.children[childId]; ok {
		return child, nil
	} else {
		return nil, errors.New(fmt.Sprintf("Cannot find child %v of supervisor %v\n", childId, supervisor.Id()))
	}
}

func (supervisor *GenericSupervisor) Start(settings metadata.ReplicationSettingsMap) error {
	supervisor.Logger().Infof("Starting supervisor %v.\n", supervisor.Id())

	err := supervisor.Init(settings)
	if err == nil {
		//start heartbeat ticker
		supervisor.heartbeat_ticker = time.NewTicker(supervisor.heartbeat_interval)

		supervisor.childrenWaitGrp.Add(1)
		go supervisor.supervising()

		//start the sever looping
		supervisor.GenServer.Start_server()

		supervisor.Logger().Infof("Started supervisor %v.\n", supervisor.Id())
	} else {
		supervisor.Logger().Errorf("Failed to start supervisor %v. error=%v\n", supervisor.Id(), err)
	}
	return err
}

func (supervisor *GenericSupervisor) Stop() error {
	supervisor.Logger().Infof("Stopping supervisor %v.\n", supervisor.Id())

	// stop supervising routine
	close(supervisor.finch)

	// stop gen_server
	err := supervisor.Stop_server()

	if supervisor.heartbeat_ticker != nil {
		supervisor.heartbeat_ticker.Stop()
	}

	supervisor.Logger().Debug("Wait for children goroutines to exit")
	supervisor.childrenWaitGrp.Wait()

	supervisor.Logger().Infof("Stopped supervisor %v.\n", supervisor.Id())

	if supervisor.parent_supervisor != nil {
		supervisor.parent_supervisor.RemoveChild(supervisor.Id())
	}
	return err
}

func (supervisor *GenericSupervisor) supervising() error {
	defer supervisor.childrenWaitGrp.Done()

	//heart beat
	count := 0
	waitGrp := &sync.WaitGroup{}
loop:
	for {
		count++
		select {
		case <-supervisor.finch:
			break loop
		case <-supervisor.heartbeat_ticker.C:
			supervisor.Logger().Debugf("heart beat tick from super %v\n", supervisor.Id())
			//wait until the previous heartbeat response are received or timed-out to send a new heartbeat
			waitGrp.Wait()
			supervisor.sendHeartBeats(waitGrp)
		}
	}

	supervisor.Logger().Infof("Supervisor %v exited\n", supervisor.Id())
	return nil
}

func (supervisor *GenericSupervisor) sendHeartBeats(waitGrp *sync.WaitGroup) {
	supervisor.Logger().Debugf("Sending heart beat msg from supervisor %v\n", supervisor.Id())

	supervisor.children_lock.RLock()
	defer supervisor.children_lock.RUnlock()

	if len(supervisor.children) > 0 {
		heartbeat_report := make(map[string]heartbeatRespStatus)
		heartbeat_resp_chs := make(map[string]chan []interface{})
		for childId, child := range supervisor.children {
			if child.IsReadyForHeartBeat() {
				respch := make(chan []interface{}, 1)
				supervisor.Logger().Debugf("heart beat sent to child %v from super %v\n", childId, supervisor.Id())
				err := child.HeartBeat_async(respch, time.Now())
				if err != nil {
					supervisor.Logger().Infof("Send heartbeat failed for %v, err=%v\n", childId, err)
					heartbeat_report[childId] = respondedNotOk
				} else {
					heartbeat_resp_chs[childId] = respch
					heartbeat_report[childId] = notYetResponded
				}
			}
		}
		if len(heartbeat_resp_chs) > 0 {
			waitGrp.Add(1)
			go supervisor.waitForResponse(heartbeat_report, heartbeat_resp_chs, supervisor.finch, waitGrp)
		} else {
			supervisor.Logger().Debugf("No response to be waited.")
		}
	}
	return
}

func (supervisor *GenericSupervisor) Init(settings metadata.ReplicationSettingsMap) error {
	//initialize settings
	err := supervisor.utils.ValidateSettings(supervisor_setting_defs, settings, supervisor.Logger())
	if err != nil {
		supervisor.Logger().Errorf("The setting for supervisor %v is not valid. err=%v", supervisor.Id(), err)
		return err
	}

	if val, ok := settings[HEARTBEAT_INTERVAL]; ok {
		supervisor.heartbeat_interval = val.(time.Duration)
	}
	if val, ok := settings[HEARTBEAT_TIMEOUT]; ok {
		supervisor.heartbeat_timeout = val.(time.Duration)
	}
	if val, ok := settings[HEARTBEAT_RESP_CHECK_INTERVAL]; ok {
		supervisor.heartbeat_resp_check_interval = val.(time.Duration)
	}

	return nil
}

func (supervisor *GenericSupervisor) waitForResponse(heartbeat_report map[string]heartbeatRespStatus, heartbeat_resp_chs map[string]chan []interface{}, finch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	defer supervisor.Logger().Debugf("Exiting waitForResponse from supervisor %v\n", supervisor.Id())

	//start a timer
	ping_time := time.Now()
	heartbeat_timeout_ch := time.After(supervisor.heartbeat_timeout)
	heartbeat_resp_check_ticker := time.NewTicker(supervisor.heartbeat_resp_check_interval)
	defer heartbeat_resp_check_ticker.Stop()
	responded_count := 0

	for {
		select {
		case <-finch:
			supervisor.Logger().Infof("Wait routine is exiting because parent supervisor %v has been stopped\n", supervisor.Id())
			//the supervisor is stopping
			return
		case <-heartbeat_timeout_ch:
			//time is up
			supervisor.Logger().Errorf("Heartbeat timeout in supervisor %v! not_yet_resp_count=%v\n", supervisor.Id(), len(heartbeat_report)-responded_count)
			goto REPORT
		case <-heartbeat_resp_check_ticker.C:
			for childId, status := range heartbeat_report {
				if status == notYetResponded {
					select {
					case <-heartbeat_resp_chs[childId]:
						responded_count++
						supervisor.Logger().Debugf("Child %v has responded to the heartbeat ping sent at %v to supervisor %v\n", childId, ping_time, supervisor.Id())
						heartbeat_report[childId] = respondedOk
					default:
					}
				}
			}
			if responded_count == len(heartbeat_resp_chs) {
				goto REPORT
			}
		}
	}

	//process the result
REPORT:
	supervisor.processReport(heartbeat_report)
}

func (supervisor *GenericSupervisor) processReport(heartbeat_report map[string]heartbeatRespStatus) {
	supervisor.Logger().Debugf("***********ProcessReport for supervisor %v*************\n", supervisor.Id())
	supervisor.Logger().Debugf("len(heartbeat_report)=%v\n", len(heartbeat_report))

	supervisor.children_lock.Lock()
	defer supervisor.children_lock.Unlock()

	brokenChildren := make(map[string]error)
	for childId, status := range heartbeat_report {
		supervisor.Logger().Debugf("childId=%v, status=%v\n", childId, status)

		if status == respondedNotOk || status == notYetResponded {
			var missedCount uint16
			// missedCount would be zero when child is not yet in the map, which would be the correct value
			missedCount, _ = supervisor.childrenBeatMissedMap[childId]
			missedCount++
			supervisor.Logger().Infof("Child %v of supervisor %v missed %v consecutive heart beats\n", childId, supervisor.Id(), missedCount)
			supervisor.childrenBeatMissedMap[childId] = missedCount
			if missedCount > supervisor.missed_heartbeat_threshold {
				// report the child as broken if it exceeded the beat_missed_threshold
				brokenChildren[childId] = base.ErrorNotResponding
				supervisor.removeChild_internal(childId, false)
			}
		} else {
			// reset missed count to 0 when child responds
			supervisor.childrenBeatMissedMap[childId] = 0
		}
	}

	if len(brokenChildren) > 0 {
		supervisor.Logger().Errorf("%v has exceeded heartbeat_missed_threshold", brokenChildren)
		supervisor.ReportFailure(brokenChildren)
	}
}

func (supervisor *GenericSupervisor) ReportFailure(errors map[string]error) {
	//report the failure to decision maker
	supervisor.failure_handler.OnError(supervisor, errors)
}

func (supervisor *GenericSupervisor) StopHeartBeatTicker() {
	if supervisor.heartbeat_ticker != nil {
		supervisor.heartbeat_ticker.Stop()
	}
}

func (supervisor *GenericSupervisor) IsReadyForHeartBeat() bool {
	return supervisor.IsStarted()
}

func (supervisor *GenericSupervisor) ChidrenWaitGroup() *sync.WaitGroup {
	return &supervisor.childrenWaitGrp
}

func (supervisor *GenericSupervisor) FinishChannel() chan bool {
	return supervisor.finch
}
