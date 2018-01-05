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
	"github.com/couchbase/goxdcr/log"
	part "github.com/couchbase/goxdcr/part"
	"sync"
)

var logger_outnozzle = log.NewLogger("testOutgoingNozzle", log.DefaultLoggerContext)

type testOutgoingNozzle struct {
	part.AbstractPart

	dataChan chan interface{}
	//communication channel
	communicationChan chan []interface{}

	isOpen    bool
	isStarted bool

	//the lock to serialize the request to open\close the nozzle
	stateLock sync.Mutex

	waitGrp sync.WaitGroup
}

func newOutgoingNozzle(id string) *testOutgoingNozzle {
	nozzle := &testOutgoingNozzle{dataChan: nil,
		communicationChan: nil,
		isOpen:            false,
		isStarted:         false,
		stateLock:         sync.Mutex{},
		waitGrp:           sync.WaitGroup{}}
	funcw := nozzle.IsStarted
	funce := (part.IsStarted_Callback_Func)(funcw)
	nozzle.AbstractPart = part.NewAbstractPartWithLogger(id, &funce, log.NewLogger("testOutgoingNozzle", log.DefaultLoggerContext))
	return nozzle
}

func (p *testOutgoingNozzle) Start(settings metadata.ReplicationSettingsMap) error {
	logger_outnozzle.Debugf("Try to start part %s", p.Id())
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	p.init(settings)
	go p.run()
	p.isStarted = true
	return nil
}

func (p *testOutgoingNozzle) init(settings metadata.ReplicationSettingsMap) error {
	p.dataChan = make(chan interface{})
	p.communicationChan = make(chan []interface{})
	return nil
}

func (p *testOutgoingNozzle) run() {
	logger_outnozzle.Debugf("Part %s is running", p.Id())
loop:
	for {
		select {
		case msg := <-p.communicationChan:
			cmd := msg[0].(int)
			logger_outnozzle.Debugf("Received cmd=%d", cmd)
			respch := msg[1].(chan []interface{})
			switch cmd {
			case cmdStop:
				logger_outnozzle.Info("Received Stop request")
				close(p.communicationChan)
				respch <- []interface{}{true}
				break loop
			case cmdHeartBeat:
				respch <- []interface{}{true}
			}
		case data := <-p.dataChan:
			if p.IsOpen() {
				//async
				p.waitGrp.Add(1)
				go p.printData(data)
			}
		}
	}

}

func (p *testOutgoingNozzle) printData(data interface{}) {
	logger_outnozzle.Debugf("Send out data %d", data.(int))
	fmt.Printf("data: %d\n", data.(int))
	p.RaiseEvent(common.DataSent, data, p, nil, nil)

	p.waitGrp.Done()
}

func (p *testOutgoingNozzle) Stop() error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	err := p.stopSelf()
	if err != nil {
		return err
	}
	logger_outnozzle.Debugf("Part %s is stopped", p.Id())

	return err
}

func (p *testOutgoingNozzle) stopSelf() error {
	respChan := make(chan []interface{})
	p.communicationChan <- []interface{}{cmdStop, respChan}
	response := <-respChan
	succeed := response[0].(bool)

	//wait for all spawned data-process goroutines to finish
	p.waitGrp.Wait()

	if succeed {
		p.isStarted = false
		logger_outnozzle.Infof("Part %s is stopped", p.Id())
		return nil
	} else {
		error_msg := response[1].(string)
		logger_outnozzle.Errorf("Failed to stop part %s", p.Id())
		return errors.New(error_msg)
	}
}

//Data can be passed to the downstream
func (p *testOutgoingNozzle) Open() error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	p.isOpen = true
	return nil
}

//Close closes the Nozzle
//
//Data can get to this nozzle, but would not be passed to the downstream
func (p *testOutgoingNozzle) Close() error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	p.isOpen = false
	return nil
}

//IsOpen returns true if the nozzle is open; returns false if the nozzle is closed
func (p *testOutgoingNozzle) IsOpen() bool {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	return p.isOpen
}

//IsStarted returns true if the nozzle is started; otherwise returns false
func (p *testOutgoingNozzle) IsStarted() bool {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	return p.isStarted
}

func (p *testOutgoingNozzle) Receive(data interface{}) error {
	logger_outnozzle.Debug("Data reached, try to send it to data channale")
	if p.dataChan == nil {
		return errors.New("The Part is not running, not ready to process data")
	}
	p.dataChan <- data
	logger_outnozzle.Debug("data Received")

	//raise DataReceived event
	p.RaiseEvent(common.DataReceived, data, p, nil, nil)

	return nil
}
