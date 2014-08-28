package base

import (
	"errors"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/util"
	"sync"
)

var logger *log.CommonLogger

func init() {
	logger = log.NewLogger("GenServer", log.LogLevelInfo)
}

type Gen_Server_Msg_Callback_Func func(msg []interface{}) bool
type Gen_Server_Extend_Callback_Func func()
type Gen_Server_Exit_Callback_Func func()

type GenServer struct {
	//msg channel
	msgChan chan []interface{}

	//heartbeat channel
	heartBeatChan chan []interface{}

	msg_callback    Gen_Server_Msg_Callback_Func
	extend_callback Gen_Server_Extend_Callback_Func
	exit_callback   Gen_Server_Exit_Callback_Func

	isStarted bool
	startLock sync.RWMutex
}

func (s *GenServer) Start_server() error{
	s.startLock.Lock()
	defer s.startLock.Unlock()

	go s.run()
	s.isStarted = true
	return nil
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
				close(s.msgChan)
				respch <- []interface{}{true}
				break loop
			} else {
				s.msg_callback(msg)
			}
		default:
			s.extend_callback()

		}
	}
	s.exit_callback()
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

func (s *GenServer) Stop_server() error{
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

func (s *GenServer) HeartBeat() bool{
	respchan := make (chan []interface{})
	s.heartBeatChan <- []interface{}{cmdHeartBeat, respchan}
	
	response := <-respchan
	if response != nil {
		return response[0].(bool)
	}
	return false
}
