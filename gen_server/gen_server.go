package gen_server

import (
	"errors"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
	utils "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/utils"
	"reflect"
	"sync"
)

const (
	cmdStop      = 0
	cmdHeartBeat = 1
)

//var logger *log.CommonLogger
//
//func init() {
//	logger = log.NewLogger("GenServer", log.LogLevelDebug)
//}

type Msg_Callback_Func func(msg []interface{}) error
type Behavior_Callback_Func func() error
type Exit_Callback_Func func()
type Error_Handler_Func func(err error)

type GenServer struct {
	//msg channel
	msgChan chan []interface{}

	//heartbeat channel
	heartBeatChan chan []interface{}

	msg_callback      *Msg_Callback_Func
	behavior_callback *Behavior_Callback_Func
	exit_callback     *Exit_Callback_Func
	error_handler     *Error_Handler_Func

	isStarted bool
	startLock sync.RWMutex
	logger    *log.CommonLogger
}

func NewGenServer(msg_callback *Msg_Callback_Func,
	behavior_callback *Behavior_Callback_Func,
	exit_callback *Exit_Callback_Func,
	error_handler *Error_Handler_Func,
	logger_context *log.LoggerContext,
	module string) GenServer {
	return GenServer{msgChan: make(chan []interface{}, 1),
		heartBeatChan:     make(chan []interface{}),
		msg_callback:      msg_callback,
		behavior_callback: behavior_callback,
		exit_callback:     exit_callback,
		error_handler:     error_handler,
		isStarted:         false,
		startLock:         sync.RWMutex{},
		logger:            log.NewLogger(module, logger_context)}
}

func (s *GenServer) Start_server() (err error) {
	s.startLock.Lock()
	defer s.startLock.Unlock()
	defer utils.RecoverPanic(&err)

	go s.run()
	s.isStarted = true
	return err
}

func (s *GenServer) run() {
loop:
	for {
		select {
		case heartBeatReq := <-s.heartBeatChan:
			if err1, respch_heartbeat := s.decodeCmd(cmdHeartBeat, heartBeatReq); err1 == nil {
				respch_heartbeat <- []interface{}{true}
			}

		case msg := <-s.msgChan:
			if err2, respch := s.decodeCmd(cmdStop, msg); err2 == nil {
				s.logger.Debugf("server is stopped\n")
				close(s.msgChan)
				respch <- []interface{}{true}
				break loop
			} else {
				if (*s.msg_callback) != nil {
					err := (*s.msg_callback)(msg)
					if err != nil {
						//report error
						s.reportError(err)
					}
				} else {
					s.logger.Debugf("No msg_callback for %s\n", reflect.TypeOf(s).Name())
				}
			}
		default:
			if (*s.behavior_callback) != nil {
				err := (*s.behavior_callback)()
				if err != nil {
					//report error
					s.reportError(err)
				}
			} else {
				s.logger.Debugf("No behavior_callback for %s\n", reflect.TypeOf(s).Name())
			}

		}
	}

	if (*s.exit_callback) != nil {
		(*s.exit_callback)()
		//probably no need to report error during exitting.
	} else {
		s.logger.Debugf("No exit_callback for %s\n", reflect.TypeOf(s).Name())
	}
}

func (s *GenServer) decodeCmd(command int, msg []interface{}) (error, chan []interface{}) {
	if len(msg) != 2 {
		return errors.New("Failed to decode command"), nil
	} else {
		cmd := msg[0].(int)
		respch := msg[1].(chan []interface{})
		if cmd == command {
			return nil, respch
		} else {
			return errors.New("Failed to decode command"), nil
		}

	}
}

func (s *GenServer) IsStarted() bool {
	s.startLock.RLock()
	defer s.startLock.RUnlock()

	return s.isStarted
}

func (s *GenServer) Stop_server() error {
	s.startLock.Lock()
	defer s.startLock.Unlock()

	if s.isStarted {

		respChan := make(chan []interface{})
		s.msgChan <- []interface{}{cmdStop, respChan}

		response := <-respChan
		succeed := response[0].(bool)

		if succeed {
			s.isStarted = false
			s.logger.Debug("Stopped")
			return nil
		} else {
			error_msg := response[1].(string)
			s.logger.Debug("Failed to stop")
			return errors.New(error_msg)
		}
	}
	return nil
}

func (s *GenServer) HeartBeat_sync() bool {
	respchan := make(chan []interface{})
	s.heartBeatChan <- []interface{}{cmdHeartBeat, respchan}

	response := <-respchan
	if response != nil {
		return response[0].(bool)
	}
	return false
}

func (s *GenServer) HeartBeat_async(respchan chan []interface{}) error {
	select {
	case s.heartBeatChan <- []interface{}{cmdHeartBeat, respchan}:
		return nil
	default:
		s.logger.Info("Last heart beat msg has not been processed")
		return errors.New("Last heart beat msg has not been processed")
	}
}

func (s *GenServer) Logger() *log.CommonLogger {
	return s.logger
}

func (s *GenServer) reportError(err error) {
	if s.error_handler != nil {
		(*s.error_handler)(err)
	} else {
		//no error handler is registered, log the error
		s.Logger().Errorf("unhandled err=%v\n", err)
	}
}

func (s *GenServer) SendMsg_async (msg []interface{}) {
	s.msgChan <- msg
}