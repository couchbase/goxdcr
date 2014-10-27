package pipeline_svc

import (
	"errors"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
	generic_p "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/gen_server"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/parts"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/utils"
	"reflect"
	//	"sync"
	"time"
)

//configuration settings
const (
	HEARTBEAT_INTERVAL     = "heartbeat_interval"
	PART_HEARTBEAT_TIMEOUT = "heartbeat_timeout"
	PIPELINE_LOG_LEVEL     = "pipeline_loglevel"

	default_heartbeat_interval     time.Duration = 400 * time.Millisecond
	default_part_heartbeat_timeout time.Duration = 400 * time.Millisecond
	default_pipeline_log_level                   = log.LogLevelInfo
)

const (
	CMD_CHANGE_LOG_LEVEL int = 2
)

var supervisor_setting_defs base.SettingDefinitions = base.SettingDefinitions{PART_HEARTBEAT_TIMEOUT: base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	PIPELINE_LOG_LEVEL: base.NewSettingDef(reflect.TypeOf((*log.LogLevel)(nil)), false),
	HEARTBEAT_INTERVAL: base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false)}

type heartbeatRespStatus int

const (
	skip            heartbeatRespStatus = iota
	notYetResponded heartbeatRespStatus = iota
	respondedOk     heartbeatRespStatus = iota
	respondedNotOk  heartbeatRespStatus = iota
)

type PipelineSupervisor struct {
	gen_server.GenServer
	pipeline               common.Pipeline
	pipeline_loggerContext *log.LoggerContext
	part_heartbeat_timeout time.Duration
	heartbeat_interval     time.Duration
	heartbeat_ticker       *time.Ticker
	failure_handler        base.PipelineFailureHandler
	finch                  chan bool
	//	children_waitGrp       sync.WaitGroup
	pipeline_err_ch chan bool
	resp_waiter_chs []chan bool
}

func NewPipelineSupervisor(logger_ctx *log.LoggerContext, failure_handler base.PipelineFailureHandler) *PipelineSupervisor {
	var behavior_callback_func gen_server.Behavior_Callback_Func

	server := gen_server.NewGenServer(nil,
		&behavior_callback_func, nil, nil, logger_ctx, "PipelineSupervisor")
	supervisor := &PipelineSupervisor{GenServer: server,
		pipeline:               nil,
		pipeline_loggerContext: logger_ctx,
		part_heartbeat_timeout: default_part_heartbeat_timeout,
		heartbeat_interval:     default_heartbeat_interval,
		failure_handler:        failure_handler,
		finch:                  make(chan bool, 1),
		//		children_waitGrp:       sync.WaitGroup{},
		pipeline_err_ch: make(chan bool, 1),
		resp_waiter_chs: []chan bool{}}

	behavior_callback_func = supervisor.supervising
	return supervisor
}

func (supervisor *PipelineSupervisor) Attach(p common.Pipeline) error {
	supervisor.pipeline = p

	//register itself with all parts' ErrorEncountered event
	partsMap := generic_p.GetAllParts(p)

	for _, part := range partsMap {
		part.RegisterComponentEventListener(common.ErrorEncountered, supervisor)
	}
	return nil
}

func (supervisor *PipelineSupervisor) Start(settings map[string]interface{}) error {
	err := supervisor.init(settings)
	if err == nil {
		//start heartbeat ticker
		supervisor.heartbeat_ticker = time.NewTicker(supervisor.heartbeat_interval)

		//start the sever looping
		supervisor.GenServer.Start_server()
	} else {
		supervisor.Logger().Errorf("Failed to start PipelineSupervisor. error=%v\n", err)
	}
	return err
}

func (supervisor *PipelineSupervisor) Stop() error {

	err := supervisor.Stop_server()

	close(supervisor.finch)

	supervisor.Logger().Debug("Wait for children goroutines to exit")
	//	supervisor.children_waitGrp.Wait()
	supervisor.heartbeat_ticker.Stop()
	supervisor.Logger().Debug("Supervisor exits....")
	return err
}

func (supervisor *PipelineSupervisor) OnEvent(eventType common.ComponentEventType,
	item interface{},
	component common.Component,
	derivedItems []interface{},
	otherInfos map[string]interface{}) {
	if eventType == common.ErrorEncountered {
		partsError := make(map[string]error)
		partsError[component.Id()] = otherInfos["error"].(error)
		supervisor.reportFailure(partsError)
	} else {
		supervisor.Logger().Errorf("Pipeline supervisor didn't register to recieve event %v for component %v", eventType, component.Id())
	}
}

func (supervisor *PipelineSupervisor) supervising() error {
	//heart beat
	select {
	case <-supervisor.finch:
		return nil
	case <-supervisor.heartbeat_ticker.C:
		select {
		case supervisor.pipeline_err_ch <- true:

			supervisor.Logger().Info("Time to send heart beat msg")
			partsMap := generic_p.GetAllParts(supervisor.pipeline)
			heartbeat_report := make(map[string]heartbeatRespStatus)
			heartbeat_resp_chs := make(map[string]chan []interface{})
			for partId, part := range partsMap {
				respch := make(chan []interface{})
				xdcrPart, ok := part.(parts.XDCRPart)
				if ok {
					err := xdcrPart.HeartBeat_async(respch)
					heartbeat_resp_chs[partId] = respch
					if err != nil {
						heartbeat_report[partId] = skip
					} else {
						heartbeat_report[partId] = notYetResponded
					}
				}
			}
			//		supervisor.children_waitGrp.Add(1)
			fin_ch := make(chan bool, 1)
			supervisor.resp_waiter_chs = append(supervisor.resp_waiter_chs, fin_ch)
			go supervisor.waitForResponse(heartbeat_report, heartbeat_resp_chs, fin_ch)
		default:
		}
	default:
	}
	return nil
}

func (supervisor *PipelineSupervisor) init(settings map[string]interface{}) error {
	//initialize settings
	err := utils.ValidateSettings(supervisor_setting_defs, settings, supervisor.Logger())
	if err != nil {
		supervisor.Logger().Errorf("The setting for Pipeline supervisor is not valid. err=%v", err)
		return err
	}

	if val, ok := settings[HEARTBEAT_INTERVAL]; ok {
		supervisor.heartbeat_interval = val.(time.Duration)
	}
	if val, ok := settings[PART_HEARTBEAT_TIMEOUT]; ok {
		supervisor.part_heartbeat_timeout = val.(time.Duration)
	}
	if val, ok := settings[PIPELINE_LOG_LEVEL]; ok {
		supervisor.pipeline_loggerContext.Log_level = val.(log.LogLevel)
	}

	return nil
}

func (supervisor *PipelineSupervisor) waitForResponse(heartbeat_report map[string]heartbeatRespStatus, heartbeat_resp_chs map[string]chan []interface{}, finch chan bool) {
	defer func() {
		<-supervisor.pipeline_err_ch
		supervisor.Logger().Debug("Exiting waitForResponse....")
	}()

	//start a timer
	ping_time := time.Now()
	heartbeat_timeout_ch := time.After(supervisor.part_heartbeat_timeout)
	not_yet_resp_count := 0

	for {
		select {
		case <-finch:
			//the supervisor is stopping
			return
		case <-heartbeat_timeout_ch:
			//time is up
			supervisor.Logger().Debugf("Heartbeat timeout! not_yet_resp_count=%v\n", not_yet_resp_count)
			goto REPORT
		default:
			not_yet_resp_count := 0
			for partId, status := range heartbeat_report {
				if status == notYetResponded {
					not_yet_resp_count++
					select {
					case <-heartbeat_resp_chs[partId]:
						supervisor.Logger().Debugf("Part %v has responded the heartbeat ping sent at %v\n", partId, ping_time)
						heartbeat_report[partId] = respondedOk
					default:
					}
				}
			}

			if not_yet_resp_count == 0 {
				goto REPORT
			}
		}
	}

	//process the result
REPORT:
	supervisor.processReport(heartbeat_report)
}

func (supervisor *PipelineSupervisor) processReport(heartbeat_report map[string]heartbeatRespStatus) {
	supervisor.Logger().Debugf("***********ProcessReport*************")
	supervisor.Logger().Debugf("len(heartbeat_report)=%v\n", len(heartbeat_report))
	brokenParts := make(map[string]error)
	for partId, status := range heartbeat_report {
		supervisor.Logger().Debugf("PartId=%v, status=%v\n", partId, status)
		if status == respondedNotOk || status == notYetResponded {
			supervisor.Logger().Infof("Part %v is not responding\n", partId)
			brokenParts[partId] = errors.New("Not responding")
		}
	}

	if len(brokenParts) > 0 {
		supervisor.reportFailure(brokenParts)
	}
}

func (supervisor *PipelineSupervisor) reportFailure(partsError map[string]error) {
	//report the failure to decision maker
	if supervisor.heartbeat_ticker != nil {
		supervisor.heartbeat_ticker.Stop()
	}
	supervisor.notifyWaitersToFinish()
	supervisor.failure_handler.OnError(supervisor.pipeline, partsError)
}

func (supervisor *PipelineSupervisor) SetPipelineLogLevel(log_level_str string) error {
	level, err := log.LogLevelFromStr(log_level_str)
	if err != nil {
		return err
	}
	supervisor.pipeline_loggerContext.Log_level = level
	return nil
}

func (supervisor *PipelineSupervisor) notifyWaitersToFinish() {
	for _, ctrl_ch := range supervisor.resp_waiter_chs {
		select {
		case ctrl_ch <- true:
		default:
		}
	}

}
