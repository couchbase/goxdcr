// Copyright (c) 2013-2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package Component

import (
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
)

// AbstractComponent are considered legacy type of components used by pipelines that instantiate first
// then run without modification. Thus, the event listeners are static and do not require locking
type AbstractComponent struct {
	id              string
	pipeline        common.Pipeline
	event_listeners EventListenersMap
	logger          *log.CommonLogger
}

func NewAbstractComponentWithLogger(id string, logger *log.CommonLogger) *AbstractComponent {
	return &AbstractComponent{
		id:              id,
		event_listeners: make(EventListenersMap),
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
	c.event_listeners.registerListerNoLock(eventType, listener)

	c.logger.Debugf("listener %v is registered on event %v for Component %v", listener, eventType, c.Id())
	return nil
}

func (c *AbstractComponent) UnRegisterComponentEventListener(eventType common.ComponentEventType, listener common.ComponentEventListener) error {
	return c.event_listeners.unregisterEventListenerNoLock(eventType, listener)
}

func (c *AbstractComponent) RaiseEvent(event *common.Event) {
	c.event_listeners.raiseEvent(event)
}

func (c *AbstractComponent) Logger() *log.CommonLogger {
	return c.logger
}

func (c *AbstractComponent) AsyncComponentEventListeners() map[string]common.AsyncComponentEventListener {
	listenerMap := make(map[string]common.AsyncComponentEventListener)
	c.event_listeners.exportToMap(listenerMap)
	return listenerMap
}
