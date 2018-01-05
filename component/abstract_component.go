// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package Component

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
)

type AbstractComponent struct {
	id              string
	pipeline        common.Pipeline
	event_listeners map[common.ComponentEventType][]common.ComponentEventListener
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

	listenerList := c.event_listeners[eventType]
	if listenerList == nil {
		listenerList = make([]common.ComponentEventListener, 0, 2)
	}

	listenerList = append(listenerList, listener)
	c.event_listeners[eventType] = listenerList

	c.logger.Debugf("listener %v is registered on event %v for Component %v", listener, eventType, c.Id())
	return nil
}

func (c *AbstractComponent) UnRegisterComponentEventListener(eventType common.ComponentEventType, listener common.ComponentEventListener) error {

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
