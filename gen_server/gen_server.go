package gen_server

import (
	"errors"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/util"
	utils "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/utils"
	"reflect"
	"sync"
)

const (
cmdStop = 0
cmdHeartBeat = 1
)

var logger *log.CommonLogger

func init() {
	logger = log.NewLogger("GenServer", log.LogLevelDebug)
}

type Msg_Callback_Func func(msg []interface{}) bool
type Behavior_Callback_Func func()
type Exit_Callback_Func func()

type GenServer struct {
	//msg channel
	msgChan chan []interface{}

	//heartbeat channel
	heartBeatChan chan []interface{}

	msg_callback      *Msg_Callback_Func
	behavior_callback *Behavior_Callback_Func
	exit_callback     *Exit_Callback_Func

	isStarted bool
	startLock sync.RWMutex
}

func NewGenServer(msg_callback *Msg_Callback_Func,
	behavior_callback *Behavior_Callback_Func,
	exit_callback *Exit_Callback_Func) GenServer {
	return GenServer{make(chan []interface{}, 1), make(chan []interface{}), msg_callback, behavior_callback, exit_callback, false, sync.RWMutex{}}
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
				logger.Debugf("server is stopped\n")
				close(s.msgChan)
				respch <- []interface{}{true}
				break loop
			} else {
				if (*s.msg_callback) != nil {
					(*s.msg_callback)(msg)
				} else {
					logger.Debugf("No msg_callback for %s\n", reflect.TypeOf(s).Name())
				}
			}
		default:
			if (*s.behavior_callback) != nil {
				(*s.behavior_callback)()
			} else {
				logger.Debugf("No behavior_callback for %s\n", reflect.TypeOf(s).Name())
			}

		}
	}

	if (*s.exit_callback) != nil {
		(*s.exit_callback)()
	} else {
		logger.Debugf("No exit_callback for %s\n", reflect.TypeOf(s).Name())
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

	respChan := make(chan []interface{})
	s.msgChan <- []interface{}{cmdStop, respChan}

	response := <-respChan
	succeed := response[0].(bool)

	if succeed {
		s.isStarted = false
		logger.Debug("Stopped")
		return nil
	} else {
		error_msg := response[1].(string)
		logger.Debug("Failed to stop")
		return errors.New(error_msg)
	}

}

func (s *GenServer) HeartBeat() bool {
	respchan := make(chan []interface{})
	s.heartBeatChan <- []interface{}{cmdHeartBeat, respchan}

	response := <-respchan
	if response != nil {
		return response[0].(bool)
	}
	return false
}
