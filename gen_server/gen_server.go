// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package gen_server

import (
	"errors"
	"github.com/couchbase/goxdcr/log"
	utilities "github.com/couchbase/goxdcr/utils"
	"reflect"
	"sync"
	"time"
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
type Exit_Callback_Func func()
type Error_Handler_Func func(err error)

type GenServer struct {
	//msg channel
	msgChan chan []interface{}

	//heartbeat channel
	heartBeatChan chan []interface{}

	msg_callback  *Msg_Callback_Func
	exit_callback *Exit_Callback_Func
	error_handler *Error_Handler_Func

	isStarted      bool
	isStarted_lock sync.RWMutex
	logger         *log.CommonLogger
	utils          utilities.UtilsIface
}

func NewGenServer(msg_callback *Msg_Callback_Func,
	exit_callback *Exit_Callback_Func,
	error_handler *Error_Handler_Func,
	logger_context *log.LoggerContext,
	module string,
	utilsIn utilities.UtilsIface) GenServer {
	return GenServer{
		msgChan:        make(chan []interface{}, 1),
		heartBeatChan:  make(chan []interface{}, 1),
		msg_callback:   msg_callback,
		exit_callback:  exit_callback,
		error_handler:  error_handler,
		isStarted:      false,
		isStarted_lock: sync.RWMutex{},
		logger:         log.NewLogger(module, logger_context),
		utils: utilsIn,
	}
}

func (s *GenServer) Start_server() (err error) {
	defer s.utils.RecoverPanic(&err)
	go s.run()
	s.SetStarted(true)
	return err
}

func (s *GenServer) run() {
	// resp ch used when exiting the routine
	var exitRespCh chan []interface{}
loop:
	for {
		select {
		case heartBeatReq := <-s.heartBeatChan:
			if err1, respch_heartbeat, timestamp := s.decodeCmd(cmdHeartBeat, heartBeatReq); err1 == nil {
				select {
				case respch_heartbeat <- []interface{}{true}:
					s.logger.Debugf("responded to heart beat sent at %v\n", timestamp)
				default:
				}
			} else {
				s.logger.Errorf("Error decoding heartbeat cmd, err=%v\n", err1)
			}

		case msg := <-s.msgChan:
			if err2, respch, timestamp := s.decodeCmd(cmdStop, msg); err2 == nil {
				s.logger.Infof("server is stopped per request sent at %v\n", timestamp)
				close(s.msgChan)
				exitRespCh = respch
				break loop
			} else {
				if (*s.msg_callback) != nil {
					err := (*s.msg_callback)(msg)
					if err != nil {
						//report error
						s.reportError(err)
					}
				}
			}

		}
	}

	if s.exit_callback != nil && (*s.exit_callback) != nil {
		(*s.exit_callback)()
		//probably no need to report error during exitting.
	} else {
		s.logger.Debugf("No exit_callback for %s\n", reflect.TypeOf(s).Name())
	}

	if exitRespCh != nil {
		exitRespCh <- []interface{}{true}
	}
}

func (s *GenServer) decodeCmd(command int, msg []interface{}) (error, chan []interface{}, time.Time) {
	if len(msg) != 3 {
		return errors.New("Failed to decode command"), nil, time.Now()
	} else {
		cmd := msg[0].(int)
		respch := msg[1].(chan []interface{})
		timestamp := msg[2].(time.Time)
		if cmd == command {
			return nil, respch, timestamp
		} else {
			return errors.New("Failed to decode command"), nil, time.Now()
		}

	}
}

func (s *GenServer) IsStarted() bool {
	s.isStarted_lock.RLock()
	defer s.isStarted_lock.RUnlock()
	return s.isStarted
}

func (s *GenServer) SetStarted(isStart bool) {
	s.isStarted_lock.Lock()
	defer s.isStarted_lock.Unlock()
	s.isStarted = isStart
}

func (s *GenServer) Stop_server() error {
	if s.IsStarted() {

		respChan := make(chan []interface{})
		s.msgChan <- []interface{}{cmdStop, respChan, time.Now()}

		response := <-respChan
		succeed := response[0].(bool)

		if succeed {
			//s.isStarted = false
			s.SetStarted(false)
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

func (s *GenServer) HeartBeat_async(respchan chan []interface{}, timestamp time.Time) error {
	select {
	case s.heartBeatChan <- []interface{}{cmdHeartBeat, respchan, timestamp}:
		s.logger.Debug("heart beat async called")
		return nil
	default:
		s.logger.Debugf("Last heart beat msg has not been processed, len(heartBeatChan)=%v", len(s.heartBeatChan))
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

func (s *GenServer) SendMsg_async(msg []interface{}) {
	s.msgChan <- msg
}
