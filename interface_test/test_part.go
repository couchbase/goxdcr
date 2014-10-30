package interface_test

import (
	"errors"
	common "github.com/Xiaomei-Zhang/goxdcr/common"
	part "github.com/Xiaomei-Zhang/goxdcr/part"
	"github.com/Xiaomei-Zhang/goxdcr/log"
	"reflect"
	"sync"
)

var logger_part = log.NewLogger ("testPart", log.DefaultLoggerContext)

//constants
var cmdStop = 0
var cmdHeartBeat = 1

type testPart struct {
	part.AbstractPart
	//settings
	increase_amount int

	dataChan chan interface{}
	//communication channel
	communicationChan chan []interface{}

	isStarted bool

	//the lock to serialize the request to start\stop the nozzle
	stateLock sync.Mutex

	waitGrp sync.WaitGroup
}

func newTestPart(id string) *testPart {
	p := &testPart{increase_amount: 0, 
	dataChan: nil, 
	communicationChan: nil, 
	isStarted: false, 
	stateLock: sync.Mutex{}, 
	waitGrp: sync.WaitGroup{}}
	funcw := p.IsStarted
	funce := (part.IsStarted_Callback_Func)(funcw)
	p.AbstractPart = part.NewAbstractPartWithLogger(id, &funce, log.NewLogger("testPart", log.DefaultLoggerContext))
	return p
}

func (p *testPart) Start(settings map[string]interface{}) error {
	logger_part.Debugf("Try to start part %s", p.Id())
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	p.init(settings)
	go p.run()
	p.isStarted = true
	return nil
}

func (p *testPart) init(settings map[string]interface{}) error {
	p.dataChan = make(chan interface{}, 1)
	p.communicationChan = make(chan []interface{}, 1)

	amt, ok := settings["increase_amount"]
	if ok {
		if reflect.TypeOf(amt).Name() != "int" {
			return errors.New("Wrong parameter. Expects int as the value for 'increase_amount'")
		}
		p.increase_amount = amt.(int)
	}

	logger_part.Debugf("Part %s is initialized\n", p.Id())
	return nil
}

func (p *testPart) run() {
	logger_part.Infof("Part %s starts running", p.Id())
loop:
	for {
		select {
		case msg := <-p.communicationChan:
			cmd := msg[0].(int)
			logger_part.Debugf("Received cmd=%d", cmd)
			respch := msg[1].(chan []interface{})
			switch cmd {
			case cmdStop:
				logger_part.Info("Received Stop request")
				close(p.communicationChan)
				close(p.dataChan)
				respch <- []interface{}{true}
				break loop
			case cmdHeartBeat:
				respch <- []interface{}{true}
			}
		case data := <-p.dataChan:
			//async
			p.waitGrp.Add(1)
			go p.process(data)
		}
	}

}

func (p *testPart) process(data interface{}) {
	newData := data.(int) + p.increase_amount

	//raise DataProcessed event
	p.RaiseEvent(common.DataProcessed, data, p, nil, nil)

	p.Connector().Forward(newData)

	p.waitGrp.Done()
}

func (p *testPart) Stop() error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	err := p.stopSelf()
	if err != nil {
		return err
	}
	logger_part.Debugf("Part %s is stopped", p.Id())

	return err
}

func (p *testPart) stopSelf() error {
	respChan := make(chan []interface{})
	p.communicationChan <- []interface{}{cmdStop, respChan}
	response := <-respChan
	succeed := response[0].(bool)

	//wait for all the data processing go routine to finsh
	p.waitGrp.Wait()

	if succeed {
		p.isStarted = false
		logger_part.Debugf("Part %s is stopped", p.Id())
		return nil
	} else {
		error_msg := response[1].(string)
		logger_part.Debugf("Failed to stop part %s", p.Id())
		return errors.New(error_msg)
	}
}

func (p *testPart) Receive(data interface{}) error {
	logger_part.Debug("Data reached, try to send it to data channale")
	if p.dataChan == nil || !p.IsStarted() {
		return errors.New("The Part is not running, not ready to process data")
	}
	p.dataChan <- data
	logger_part.Debug("data Received")

	//raise DataReceived event
	p.RaiseEvent(common.DataReceived, data, p, nil, nil)

	return nil
}

//IsStarted returns true if the nozzle is started; otherwise returns false
func (p *testPart) IsStarted() bool {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	return p.isStarted
}
