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
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
)

type AsyncComponentEventListenerImpl struct {
	id           string
	topic        string
	event_chan   chan *common.Event
	fin_ch       chan bool
	done_ch      chan bool
	isStarted    uint32
	stopOnce     sync.Once
	logger       *log.CommonLogger
	handlers     map[string]common.AsyncComponentEventHandler
	pipelineType common.ListenerPipelineType
}

func NewAsyncComponentEventListenerImpl(id, fullTopic string, logger_context *log.LoggerContext,
	event_chan_length int) *AsyncComponentEventListenerImpl {

	topic, pipelineType := common.DecomposeFullTopic(fullTopic)
	al := &AsyncComponentEventListenerImpl{
		id:         id,
		topic:      topic,
		event_chan: make(chan *common.Event, event_chan_length),
		fin_ch:     make(chan bool, 1),
		done_ch:    make(chan bool, 1),
		handlers:   make(map[string]common.AsyncComponentEventHandler),
		logger:     log.NewLogger("AsyncListener", logger_context),
	}

	switch pipelineType {
	case common.MainPipeline:
		al.pipelineType = common.ListenerOfMainPipeline
	case common.BackfillPipeline:
		al.pipelineType = common.ListenerOfBackfillPipeline
	default:
		al.pipelineType = common.ListenerNotShared
	}

	return al
}

func NewDefaultAsyncComponentEventListenerImpl(id, fullTopic string, logger_context *log.LoggerContext, componentEventsChanSize int) *AsyncComponentEventListenerImpl {
	return NewAsyncComponentEventListenerImpl(id, fullTopic, logger_context, componentEventsChanSize)
}

func (l *AsyncComponentEventListenerImpl) isClosed() bool {
	select {
	case <-l.fin_ch:
		return true
	default:
		return false
	}
}

func (l *AsyncComponentEventListenerImpl) OnEvent(event *common.Event) {
	if l.isClosed() {
		handleFin(event)
		return
	}

	select {
	case <-l.fin_ch:
		handleFin(event)
	case l.event_chan <- event:
	}
}

func handleFin(event *common.Event) {
	if event == nil || !event.EventType.IsSynchronousEvent() {
		return
	}

	// If the event is synchronous, unblock the caller by closing the blocking channel.
	// Make sure we only close an unclosed channel.

	syncErrCh, ok1 := event.OtherInfos.(chan error)
	if ok1 {
		var closeable bool
		select {
		case _, ok := <-syncErrCh:
			if ok {
				closeable = true
			}
		default:
			closeable = true
		}

		if closeable {
			close(syncErrCh)
		}

		return
	}

	syncBoolCh, ok2 := event.OtherInfos.(chan bool)
	if ok2 {
		var closeable bool
		select {
		case _, ok := <-syncBoolCh:
			if ok {
				closeable = true
			}
		default:
			closeable = true
		}

		if closeable {
			close(syncBoolCh)
		}

		return
	}
}

func (b *AsyncComponentEventListenerImpl) ListenerPipelineType() common.ListenerPipelineType {
	return b.pipelineType
}

func (l *AsyncComponentEventListenerImpl) Start() error {
	if atomic.CompareAndSwapUint32(&l.isStarted, 0, 1) {
		go l.run()
		l.logger.Infof("%v started processing events", l.id)
	} else {
		l.logger.Infof("%v already started processing events", l.id)
	}
	return nil
}

func (l *AsyncComponentEventListenerImpl) run() {
	for {
		select {
		case <-l.fin_ch:
			l.done_ch <- true
			return
		case event := <-l.event_chan:
			for _, handler := range l.handlers {
				err := handler.ProcessEvent(event)
				if err != nil {
					l.logger.Errorf("%v Error processing event %v. err = %v", handler.Id(), event, err)
				}
			}
		}
	}
}

func (l *AsyncComponentEventListenerImpl) Stop() error {
	var err error
	var triedToStop bool
	l.stopOnce.Do(func() {
		triedToStop = true
		l.logger.Infof("%v stopping processing events", l.id)
		err = base.ExecWithTimeout(l.stop, 500*time.Millisecond, l.logger)
		if err == nil {
			l.logger.Infof("%v stopped processing events", l.id)
		} else {
			l.logger.Warnf("%v failed to stop processing events. Leaving it alone to die. err=%v", l.id, err)
		}
	})

	if !triedToStop {
		l.logger.Infof("%v already stopped processing events", l.id)
	}

	return err
}

func (l *AsyncComponentEventListenerImpl) stop() error {
	// Regardless of if Start() was called before or after Stop(), close this
	// If run() happens later, it'll read the closed channel and stop instead of hang around forever
	close(l.fin_ch)

	if atomic.LoadUint32(&l.isStarted) == 1 {
		// Start was called before Stop, so do a wait
		// Otherwise, if Start runs after this routine is finished, not a big deal and don't wait
		<-l.done_ch
	} else {
		l.logger.Infof("%v hadn't started processing events", l.id)
	}

	return nil
}

func (l *AsyncComponentEventListenerImpl) Id() string {
	return l.id
}

func (l *AsyncComponentEventListenerImpl) PrintStatusSummary() {
	eventChanLen, eventChanCap := len(l.event_chan), cap(l.event_chan)

	if eventChanLen > (eventChanCap*base.ThresholdPercentForEventChanSizeLogging)/100 {
		l.logger.Infof("%v chan size = %v ", l.id, eventChanLen)
	}
}

func (l *AsyncComponentEventListenerImpl) RegisterComponentEventHandler(handler common.AsyncComponentEventHandler) {
	if handler != nil {
		// no need to lock since this is done during pipeline construction, which is way before any event can be generated and handlers accessed
		l.handlers[handler.Id()] = handler
		l.logger.Debugf("Registering handler %v on listener %v", handler.Id(), l.id)
	}
}
