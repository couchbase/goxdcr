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
	"sync"
	"time"
)

//configuration settings
const (
	HEARTBEAT_INTERVAL     = "heartbeat_interval"
	PART_HEARTBEAT_TIMEOUT = "heartbeat_timeout"
	PIPELINE_LOG_LEVEL     = "pipeline_loglevel"

	default_heartbeat_interval     time.Duration = 100 * time.Millisecond
	default_part_heartbeat_timeout time.Duration = 100 * time.Millisecond
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
	not_yet_responded heartbeatRespStatus = iota
	responded_ok      heartbeatRespStatus = iota
	responded_not_ok  heartbeatRespStatus = iota
)

type PipelineSupervisor struct {
	gen_server.GenServer
	pipeline               common.Pipeline
	pipeline_loggerContext *log.LoggerContext
	part_heartbeat_timeout time.Duration
	heartbeat_interval     time.Duration
	heartbeat_ticker       <-chan time.Time
	failure_handler        base.PipelineFailureHandler
	finch                  chan bool
	children_waitGrp       sync.WaitGroup
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
		children_waitGrp:       sync.WaitGroup{}}

	behavior_callback_func = supervisor.supervising
	return supervisor
}

func (supervisor *PipelineSupervisor) Attach(p common.Pipeline) error {
	supervisor.pipeline = p

	//register itself with all parts' ErrorEncountered event
	partsMap := generic_p.GetAllParts(p)

	for _, part := range partsMap {
		part.RegisterPartEventListener(common.ErrorEncountered, supervisor)
	}
	return nil
}

func (supervisor *PipelineSupervisor) Start(settings map[string]interface{}) error {
	err := supervisor.init(settings)
	if err != nil {
		//start heartbeat ticker
		supervisor.heartbeat_ticker = time.Tick(supervisor.heartbeat_interval)

		//start the sever looping
		supervisor.GenServer.Start_server()
	} else {
		supervisor.Logger().Errorf("Failed to start PipelineSupervisor. error=%v\n", err)
	}
	return err
}

func (supervisor *PipelineSupervisor) Stop() error {
	err := supervisor.Stop_server()

	supervisor.finch <- true

	supervisor.Logger().Debug("Wait for children goroutines to exit")
	supervisor.children_waitGrp.Wait()
	return err
}

func (supervisor *PipelineSupervisor) OnEvent(eventType common.PartEventType,
	item interface{},
	part common.Part,
	derivedItems []interface{},
	otherInfos map[string]interface{}) {
	if eventType == common.ErrorEncountered {
		partsError := make(map[string]error)
		partsError[part.Id()] = otherInfos["error"].(error)
		supervisor.reportFailure(partsError)
	} else {
		supervisor.Logger().Errorf("Pipeline supervisor didn't register to recieve event %v for part %v", eventType, part.Id())
	}
}

func (supervisor *PipelineSupervisor) supervising() error {
	//heart beat
	for _ = range supervisor.heartbeat_ticker {
		supervisor.Logger().Info("Time to send heart beat msg")
		partsMap := generic_p.GetAllParts(supervisor.pipeline)
		heartbeat_report := make(map[string]heartbeatRespStatus)
		heartbeat_resp_chs := make(map[string]chan []interface{})
		for partId, part := range partsMap {
			respch := make(chan []interface{})
			err := part.(parts.XDCRPart).HeartBeat_async(respch)
			heartbeat_resp_chs[partId] = respch
			if err != nil {
				heartbeat_report[partId] = responded_not_ok
			} else {
				heartbeat_report[partId] = not_yet_responded
			}
		}
		supervisor.children_waitGrp.Add(1)
		go supervisor.waitForResponse(heartbeat_report, heartbeat_resp_chs)
	}
	return nil
}

func (supervisor *PipelineSupervisor) init(settings map[string]interface{}) (err error) {
	//initialize settings
	err = utils.ValidateSettings(supervisor_setting_defs, settings, supervisor.Logger())
	if err != nil {
		supervisor.Logger().Errorf("The setting for Pipeline supervisor is not valid. err=%v", err)
		return
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

	return
}

func (supervisor *PipelineSupervisor) waitForResponse(heartbeat_report map[string]heartbeatRespStatus, heartbeat_resp_chs map[string]chan []interface{}) {
	//start a timer
	ping_time := time.Now()
	heartbeat_timeout_ch := time.After(supervisor.part_heartbeat_timeout)

	for {
		select {
		case <-supervisor.finch:
			//the supervisor is stopping
			goto RE
		case <-heartbeat_timeout_ch:
			//time is up
			break
		default:
			for partId, status := range heartbeat_report {
				if status == not_yet_responded {
					select {
					case <-heartbeat_resp_chs[partId]:
						supervisor.Logger().Debugf("Part %v has responded the heartbeat ping sent at %v\n", partId, ping_time)
						heartbeat_report[partId] = responded_ok

					}
				}
			}
		}
	}

	//process the result
	supervisor.processReport(heartbeat_report)
RE:
	supervisor.children_waitGrp.Done()
}

func (supervisor *PipelineSupervisor) processReport(heartbeat_report map[string]heartbeatRespStatus) {
	brokenParts := make(map[string]error)
	for partId, status := range heartbeat_report {
		if status == responded_not_ok {
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
	supervisor.failure_handler.OnError(supervisor.pipeline.Topic(), partsError)
}

func (supervisor *PipelineSupervisor) SetPipelineLogLevel(log_level_str string) error {
	level, err := log.LogLevelFromStr(log_level_str)
	if err != nil {
		return err
	}
	supervisor.pipeline_loggerContext.Log_level = level
	return nil
}
