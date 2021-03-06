// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

// implementation of AsyncComponentEventListener
package Component

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
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
	select {
	case l.event_chan <- event:
	case <-l.fin_ch:
	}
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
	err := base.ExecWithTimeout(l.stop, 500*time.Millisecond, l.logger)
	if err == nil {
		l.logger.Infof("%v stopped processing events\n", l.id)
	} else {
		l.logger.Warnf("%v failed to stop processing events. Leaving it alone to die. err=%v\n", l.id, err)
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

func (l *AsyncComponentEventListenerImpl) PrintStatusSummary() {
	event_chan_size := len(l.event_chan)
	if event_chan_size > base.ThresholdForEventChanSizeLogging {
		l.logger.Infof("%v chan size =%v ", l.id, event_chan_size)
	}
}

func (l *AsyncComponentEventListenerImpl) RegisterComponentEventHandler(handler common.AsyncComponentEventHandler) {
	if handler != nil {
		// no need to lock since this is done during pipeline construction, which is way before any event can be generated and handlers accessed
		l.handlers[handler.Id()] = handler
		l.logger.Debugf("Registering handler %v on listener %v\n", handler.Id(), l.id)
	}
}
