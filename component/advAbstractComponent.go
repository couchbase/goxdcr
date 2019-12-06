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
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"sync"
)

// AdvAbstractComponent are considered newer type of components used by pipelines that instantiate first
// then run with potential modification. Thus, rwmutex are needed
type AdvAbstractComponent struct {
	id     string
	logger *log.CommonLogger

	pipelines      map[string]common.Pipeline
	eventListeners map[string]EventListenersMap
	mutex          sync.RWMutex
}

func NewAdvAbstractComponentWithLogger(id string, logger *log.CommonLogger) *AdvAbstractComponent {
	return &AdvAbstractComponent{
		id:             id,
		pipelines:      make(map[string]common.Pipeline),
		eventListeners: make(map[string]EventListenersMap),
		logger:         logger,
	}
}

func (c *AdvAbstractComponent) Id() string {
	return c.id
}

func (c *AdvAbstractComponent) RegisterComponentEventListener(eventType common.ComponentEventType, listener common.ComponentEventListener) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, v := range c.eventListeners {
		v.registerListerNoLock(eventType, listener)
	}

	return nil
}

func (c *AdvAbstractComponent) UnRegisterComponentEventListener(eventType common.ComponentEventType, listener common.ComponentEventListener) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	errMap := make(base.ErrorMap)

	for k, v := range c.eventListeners {
		err := v.unregisterEventListenerNoLock(eventType, listener)
		if err != nil {
			errMap[k] = err
		}
	}

	if len(errMap) > 0 {
		return fmt.Errorf(base.FlattenErrorMap(errMap))
	} else {
		return nil
	}
}

func (c *AdvAbstractComponent) RaiseEvent(event *common.Event) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	for _, listener := range c.eventListeners {
		listener.raiseEvent(event)
	}
}

func (c *AdvAbstractComponent) AsyncComponentEventListeners() map[string]common.AsyncComponentEventListener {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	listenersMap := make(map[string]common.AsyncComponentEventListener)
	for _, listener := range c.eventListeners {
		listener.exportToMap(listenersMap)
	}

	return listenersMap
}

func (c *AdvAbstractComponent) Logger() *log.CommonLogger {
	return c.logger
}
