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
	common "github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
)

type AbstractComponent struct {
	id        string
	event_listeners map[common.ComponentEventType][]common.ComponentEventListener
	logger    *log.CommonLogger
}

func NewAbstractComponentWithLogger(id string, logger *log.CommonLogger) *AbstractComponent {
	return &AbstractComponent{
		id:                 id,
		event_listeners:    make(map[common.ComponentEventType][]common.ComponentEventListener),
		logger:             logger,
	}
}

func NewAbstractComponent(id string) *AbstractComponent {
	return NewAbstractComponentWithLogger(id, log.NewLogger("AbstractComponent", log.DefaultLoggerContext))
}

func (c *AbstractComponent) Id() string {
	return c.id
}

func (c *AbstractComponent) RegisterComponentEventListener(eventType common.ComponentEventType, listener common.ComponentEventListener) error {

	listenerList := c.event_listeners[eventType]
	if listenerList == nil {
		listenerList = make([]common.ComponentEventListener, 0, 15)
	}

	listenerList = append(listenerList, listener)
	c.event_listeners[eventType] = listenerList
	c.logger.Debugf("listener %s is registered on event %s for Component %s", fmt.Sprint(listener), fmt.Sprint(eventType), c.Id())
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

func (c *AbstractComponent) RaiseEvent(eventType common.ComponentEventType, data interface{}, component common.Component, derivedData []interface{}, otherInfos map[string]interface{}) {

	c.logger.Debugf("Raise event %d for component %s\n", eventType, component.Id())
	listenerList := c.event_listeners[eventType]

	for _, listener := range listenerList {
		if listener != nil {
			//			logger.LogDebug("", "", fmt.Sprintf("calling listener %s on event %s on part %s", fmt.Sprint(listener), fmt.Sprint(eventType), part.Id()))
			listener.OnEvent(eventType, data, component, derivedData, otherInfos)
		}
	}
}


func (c *AbstractComponent) Logger() *log.CommonLogger {
	return c.logger
}
