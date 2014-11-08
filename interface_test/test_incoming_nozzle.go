// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package interface_test

import (
	"errors"
	"fmt"
	common "github.com/couchbase/goxdcr/common"
	part "github.com/couchbase/goxdcr/part"
	"github.com/couchbase/goxdcr/log"
	"reflect"
	"sync"
)

var lt = log.NewLogger("testIncomingNozzle", log.DefaultLoggerContext)

type testIncomingNozzle struct {
	part.AbstractPart
	//settings
	start_int int

	//communication channel
	communicationChan chan []interface{}

	isOpen    bool
	isStarted bool

	//the lock to serialize the request to open\close the nozzle
	openLock sync.RWMutex
	startLock sync.RWMutex

	waitGrp sync.WaitGroup
}

func newInComingNozzle(id string) *testIncomingNozzle {
	nozzle := &testIncomingNozzle{start_int :0, 
	communicationChan: nil, 
	isOpen: false, 
	isStarted: false, 
	openLock: sync.RWMutex{}, 
	startLock: sync.RWMutex{}, 
	waitGrp: sync.WaitGroup{}}
	funcw := nozzle.IsStarted
	funce := (part.IsStarted_Callback_Func)(funcw)
	nozzle.AbstractPart = part.NewAbstractPartWithLogger(id, &funce, log.NewLogger("testIncomingNozzle", log.DefaultLoggerContext))
	return nozzle
}

func (p *testIncomingNozzle) Start(settings map[string]interface{}) error {
	p.startLock.Lock()
	defer p.startLock.Unlock()

	p.init(settings)
	go p.run(p.communicationChan)
	p.isStarted = true
	return nil
}

func (p *testIncomingNozzle) init(settings map[string]interface{}) error {
	p.communicationChan = make(chan []interface{})

	start, ok := settings["start_int"]
	if ok {
		if reflect.TypeOf(start).Name() != "int" {
			return errors.New("Wrong parameter. Expects int as the value for 'start_int'")
		}
		p.start_int = start.(int)
	}
	return nil
}

func (p *testIncomingNozzle) monitor(msgChan chan []interface{}, ctlChan chan bool) {
	logger.Debug("monitor is started.....")
loop:
	for {
		select {
		case <-ctlChan:
			break loop
		default:
			if len(msgChan) > 0 {
				logger.Debugf("Monitor***channel size is %d", len(msgChan))
			}

		}
	}
	logger.Info("Monitor routine stopped !!!")
}

func (p *testIncomingNozzle) run(msgChan chan []interface{}) {
	lt.Infof("run with msgChan = %s", fmt.Sprint(msgChan))
	counter := 0
	incCounter := 1
	data := p.start_int
loop:
	for {
		select {
		case msg := <-msgChan:
			lt.Infof("Received msg=%s", fmt.Sprint(msg))
			cmd := msg[0].(int)
			respch := msg[1].(chan []interface{})
			switch cmd {
			case cmdStop:
				lt.Info("Received Stop request")
				close(msgChan)
				respch <- []interface{}{true}
				break loop
			case cmdHeartBeat:
				respch <- []interface{}{true}
			}
		default:
			if p.IsOpen() && counter == 1000000 {
				//async
				p.waitGrp.Add(1)
				go p.pumpData(data + incCounter)
				counter = 0
				incCounter++
			}

		}
		counter++
	}
}

func (p *testIncomingNozzle) pumpData(data int) {
	//raise DataReceived event
	if data < 1000 {

		//raise DataReceived event
		p.RaiseEvent(common.DataReceived, data, p, nil, nil)

		lt.Debugf("Pump data: %d", data)
		p.Connector().Forward(data)
	}

	p.waitGrp.Done()
}

func (p *testIncomingNozzle) Stop() error {
	p.startLock.Lock()
	defer p.startLock.Unlock()

	lt.Infof("Try to stop part %s", p.Id())
	err := p.stopSelf(p.communicationChan)
	if err != nil {
		return err
	}
	lt.Infof("Part %s is stopped", p.Id())

	return err
}

func (p *testIncomingNozzle) stopSelf(msgChan chan []interface{}) error {

	respChan := make(chan []interface{})
	msgChan <- []interface{}{cmdStop, respChan}

	logger.Info("Waiting....")
	ctlChan := make(chan bool)
	go p.monitor(msgChan, ctlChan)

	response := <-respChan
	succeed := response[0].(bool)

	//wait all the data-pumping routines have finished
	p.waitGrp.Wait()

	if succeed {
		p.isStarted = false
		lt.Debugf("Part %s is stopped", p.Id())
		return nil
	} else {
		error_msg := response[1].(string)
		lt.Debugf("Failed to stop part %s", p.Id())
		return errors.New(error_msg)
	}
}

//Data can be passed to the downstream
func (p *testIncomingNozzle) Open() error {
	p.openLock.Lock()
	defer p.openLock.Unlock()

	p.isOpen = true
	return nil
}

//Close closes the Nozzle
//
//Data can get to this nozzle, but would not be passed to the downstream
func (p *testIncomingNozzle) Close() error {
	p.openLock.Lock()
	defer p.openLock.Unlock()

	p.isOpen = false

	lt.Debugf("Nozzle %s is closed", p.Id())
	return nil
}

//IsOpen returns true if the nozzle is open; returns false if the nozzle is closed
func (p *testIncomingNozzle) IsOpen() bool {
	p.openLock.RLock()
	defer p.openLock.RUnlock()

	return p.isOpen
}

//IsStarted returns true if the nozzle is started; otherwise returns false
func (p *testIncomingNozzle) IsStarted() bool {
	p.startLock.RLock()
	defer p.startLock.RUnlock()

	return p.isStarted
}

func (p *testIncomingNozzle) Receive(data interface{}) error {
	return errors.New("Incoming nozzle doesn't have upstream")
}
