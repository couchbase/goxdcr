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
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/simple_utils"
	"time"
)

type AsyncComponentEventListenerImpl struct {
	id         string
	topic      string
	event_chan chan *common.Event
	fin_ch     chan bool
	done_ch    chan bool
	isStarted  bool
	logger     *log.CommonLogger
	handlers   map[string]common.AsyncComponentEventHandler
}

func NewAsyncComponentEventListenerImpl(id, topic string, logger_context *log.LoggerContext,
	event_chan_length int) *AsyncComponentEventListenerImpl {
	return &AsyncComponentEventListenerImpl{
		id:         id,
		topic:      topic,
		event_chan: make(chan *common.Event, event_chan_length),
		fin_ch:     make(chan bool, 1),
		done_ch:    make(chan bool, 1),
		handlers:   make(map[string]common.AsyncComponentEventHandler),
		logger:     log.NewLogger("AsyncListener", logger_context),
	}
}

func NewDefaultAsyncComponentEventListenerImpl(id, topic string,
	logger_context *log.LoggerContext) *AsyncComponentEventListenerImpl {
	return NewAsyncComponentEventListenerImpl(id, topic, logger_context, base.EventChanSize)
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
			for _, handler := range l.handlers {
				err := handler.ProcessEvent(event)
				if err != nil {
					l.logger.Errorf("%v Error processing event %v. err = %v\n", handler.Id(), event, err)
				}
			}
		}
	}
}

func (l *AsyncComponentEventListenerImpl) Stop() error {
	l.logger.Infof("%v stopping processing events\n", l.id)
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

func (l *AsyncComponentEventListenerImpl) StatusSummary() string {
	return fmt.Sprintf("%v chan size =%v ", l.id, len(l.event_chan))
}

func (l *AsyncComponentEventListenerImpl) RegisterComponentEventHandler(handler common.AsyncComponentEventHandler) {
	if handler != nil {
		l.handlers[handler.Id()] = handler
		l.logger.Debugf("Registering handler %v on listener %v\n", handler.Id(), l.id)
	}
}
