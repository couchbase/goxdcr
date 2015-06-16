// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// implementation of AsyncComponentEventListener
package Component

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/simple_utils"
	"time"
)

type AsyncComponentEventListenerImpl struct {
	id           string
	topic        string
	event_chan   chan *common.Event
	fin_ch       chan bool
	done_ch      chan bool
	isStarted    bool
	logger       *log.CommonLogger
	processEvent common.ProcessEvent
}

func NewAsyncComponentEventListenerImpl(id, topic string, processEvent common.ProcessEvent,
	logger *log.CommonLogger, event_chan_length int) *AsyncComponentEventListenerImpl {
	return &AsyncComponentEventListenerImpl{
		id:           id,
		topic:        topic,
		event_chan:   make(chan *common.Event, event_chan_length),
		fin_ch:       make(chan bool, 1),
		done_ch:      make(chan bool, 1),
		logger:       logger,
		processEvent: processEvent,
	}
}

func NewDefaultAsyncComponentEventListenerImpl(id, topic string, processEvent common.ProcessEvent,
	logger *log.CommonLogger) *AsyncComponentEventListenerImpl {
	return NewAsyncComponentEventListenerImpl(id, topic, processEvent, logger, base.EventChanLength)
}

func (l *AsyncComponentEventListenerImpl) OnEvent(event *common.Event) {
	l.event_chan <- event
}

func (l *AsyncComponentEventListenerImpl) Start() error {
	if !l.isStarted {
		go l.start()
		l.isStarted = true
		l.logger.Infof("%v started processing events\n", l.id)
	}

	return nil
}

func (l *AsyncComponentEventListenerImpl) start() {
	fin_ch := l.fin_ch
	for {
		select {
		case <-fin_ch:
			l.done_ch <- true
			return
		case event := <-l.event_chan:
			err := l.processEvent(event)
			if err != nil {
				l.logger.Errorf("%v Error processing events. err = %v\n", l.Id(), err)
			}
		}
	}
}

func (l *AsyncComponentEventListenerImpl) Stop() error {
	err := simple_utils.ExecWithTimeout(l.stop, 500*time.Millisecond, l.logger)
	if err == nil {
		l.logger.Infof("%v stopped processing events\n", l.id)
	} else {
		l.logger.Infof("%v failed to stop processing events. Leaving it alone to die. err=%v\n", l.id, err)
	}
	return err
}

func (l *AsyncComponentEventListenerImpl) stop() error {
	if !l.isStarted {
		return nil
	}

	close(l.fin_ch)
	<-l.done_ch
	l.isStarted = false

	return nil
}

func (l *AsyncComponentEventListenerImpl) Id() string {
	return l.id
}
