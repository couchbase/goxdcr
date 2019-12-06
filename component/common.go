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
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
)

type EventListenersMap map[common.ComponentEventType][]common.ComponentEventListener

func (e *EventListenersMap) registerListerNoLock(eventType common.ComponentEventType, listener common.ComponentEventListener) {
	if e == nil || *e == nil {
		return
	}

	listenerList := (*e)[eventType]
	if listenerList == nil {
		listenerList = make([]common.ComponentEventListener, 0, 2)
	}

	listenerList = append(listenerList, listener)
	(*e)[eventType] = listenerList
}

func (e *EventListenersMap) unregisterEventListenerNoLock(eventType common.ComponentEventType, listener common.ComponentEventListener) error {
	if e == nil || *e == nil {
		return base.ErrorInvalidInput
	}

	listenerList := (*e)[eventType]
	var index int = -1

	for i, l := range listenerList {
		if l == listener {
			index = i
			break
		}
	}

	if index >= 0 {
		listenerList = append(listenerList[:index], listenerList[index+1:]...)
		(*e)[eventType] = listenerList
	} else {
		return errors.New("UnRegisterComponentEventListener failed: can't find listener " + fmt.Sprint(listener))
	}
	return nil
}

func (e *EventListenersMap) raiseEvent(event *common.Event) {
	if e == nil || *e == nil {
		return
	}

	for _, listener := range (*e)[event.EventType] {
		if listener != nil {
			listener.OnEvent(event)
		}
	}
}

func (e *EventListenersMap) exportToMap(listenerMap map[string]common.AsyncComponentEventListener) {
	if e == nil || *e == nil {
		return
	}

	for _, listeners := range *e {
		for _, listener := range listeners {
			if asyncListener, ok := listener.(common.AsyncComponentEventListener); ok {
				if _, ok = listenerMap[asyncListener.Id()]; !ok {
					listenerMap[asyncListener.Id()] = asyncListener
				}
			}
		}
	}
}
