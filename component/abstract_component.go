// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package Component

import (
	"errors"
	"fmt"
	"sync"

	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
)

type AbstractComponent struct {
	id              string
	pipeline        common.Pipeline
	event_listeners map[common.ComponentEventType][]common.ComponentEventListener
	evtListenersMtx sync.RWMutex
	logger          *log.CommonLogger
}

func NewAbstractComponentWithLogger(id string, logger *log.CommonLogger) *AbstractComponent {
	return &AbstractComponent{
		id:              id,
		event_listeners: make(map[common.ComponentEventType][]common.ComponentEventListener),
		logger:          logger,
	}
}

func NewAbstractComponent(id string) *AbstractComponent {
	return NewAbstractComponentWithLogger(id, log.NewLogger("AbstractComp", log.DefaultLoggerContext))
}

func (c *AbstractComponent) Id() string {
	return c.id
}

func (c *AbstractComponent) RegisterComponentEventListener(eventType common.ComponentEventType, listener common.ComponentEventListener) error {
	c.evtListenersMtx.Lock()
	defer c.evtListenersMtx.Unlock()
	listenerList := c.event_listeners[eventType]
	if listenerList == nil {
		listenerList = make([]common.ComponentEventListener, 0, 2)
	}

	listenerList = append(listenerList, listener)
	c.event_listeners[eventType] = listenerList

	c.logger.Debugf("listener %v is registered on event %v for Component %v", listener, eventType, c.Id())
	return nil
}

// Warning - if too many RaiseEvent() is happening and is blocked because listener's channel is full
// then this UnRegisterComponentEventListener will be blocked as well, and pipeline stop will show timeout
// Only a few components use this call for now. If the components in the future use a lot of RaiseEvent and require
// this Unregister call, then those components need to be careful to prevent deadlock
func (c *AbstractComponent) UnRegisterComponentEventListener(eventType common.ComponentEventType, listener common.ComponentEventListener) error {
	c.evtListenersMtx.Lock()
	defer c.evtListenersMtx.Unlock()
	listenerList := c.event_listeners[eventType]
	var index int = -1

	for i, l := range listenerList {
		if l == listener {
			index = i
			c.logger.Debugf("listener's index is " + fmt.Sprint(i))
			break
		}
	}

	if index >= 0 {
		listenerList = append(listenerList[:index], listenerList[index+1:]...)
		c.event_listeners[eventType] = listenerList
	} else {
		return errors.New("UnRegisterComponentEventListener failed: can't find listener " + fmt.Sprint(listener))
	}
	return nil
}

func (c *AbstractComponent) RaiseEvent(event *common.Event) {
	c.evtListenersMtx.RLock()
	defer c.evtListenersMtx.RUnlock()
	listenerList := c.event_listeners[event.EventType]

	for _, listener := range listenerList {
		if listener != nil {
			listener.OnEvent(event)
		}
	}
}

func (c *AbstractComponent) Logger() *log.CommonLogger {
	return c.logger
}

func (c *AbstractComponent) AsyncComponentEventListeners() map[string]common.AsyncComponentEventListener {
	listenerMap := make(map[string]common.AsyncComponentEventListener)
	c.evtListenersMtx.RLock()
	defer c.evtListenersMtx.RUnlock()
	for _, listeners := range c.event_listeners {
		for _, listener := range listeners {
			if asyncListener, ok := listener.(common.AsyncComponentEventListener); ok {
				if _, ok = listenerMap[asyncListener.Id()]; !ok {
					listenerMap[asyncListener.Id()] = asyncListener
				}
			}
		}
	}
	return listenerMap
}
