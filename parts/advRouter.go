// Copyright (c) 2013-2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package parts

import (
	"errors"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/metadata"
	"sync"
)

// Advanced router is a conglomerate of multiple regular routers
type AdvRouter struct {
	common.Connector
	id string

	routersList map[string]*Router
	mtx         sync.RWMutex
}

func NewAdvRouter(id string) *AdvRouter {
	return &AdvRouter{
		routersList: make(map[string]*Router),
		id:          id,
	}
}

func (a *AdvRouter) AddRouter(router *Router) {
	if router == nil {
		return
	}

	a.mtx.Lock()
	defer a.mtx.Unlock()
	a.routersList[router.Id()] = router
}

func (a *AdvRouter) RoutingMapByDownstreams() map[string][]uint16 {
	outMap := make(map[string][]uint16)

	a.mtx.RLock()
	defer a.mtx.RUnlock()

	for _, router := range a.routersList {
		downstreamMap := router.RoutingMapByDownstreams()
		for k, v := range downstreamMap {
			outMap[k] = v
		}
	}

	return outMap
}

// Component implementations
func (a *AdvRouter) Id() string {
	return a.id
}

func (a *AdvRouter) RegisterComponentEventListener(eventType common.ComponentEventType, listener common.ComponentEventListener) error {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	errMap := make(base.ErrorMap)
	for routerName, router := range a.routersList {
		err := router.RegisterComponentEventListener(eventType, listener)
		if err != nil {
			errMap[routerName] = err
		}
	}

	if len(errMap) > 0 {
		return errors.New(base.FlattenErrorMap(errMap))
	} else {
		return nil
	}
}

func (a *AdvRouter) UnRegisterComponentEventListener(eventType common.ComponentEventType, listener common.ComponentEventListener) error {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	errMap := make(base.ErrorMap)
	for routerName, router := range a.routersList {
		err := router.UnRegisterComponentEventListener(eventType, listener)
		if err != nil {
			errMap[routerName] = err
		}
	}

	if len(errMap) > 0 {
		return errors.New(base.FlattenErrorMap(errMap))
	} else {
		return nil
	}
}

func (a *AdvRouter) AsyncComponentEventListeners() map[string]common.AsyncComponentEventListener {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	totalMap := make(map[string]common.AsyncComponentEventListener)
	for _, router := range a.routersList {
		outMap := router.AsyncComponentEventListeners()
		for k, v := range outMap {
			totalMap[k] = v
		}
	}
	return totalMap
}

func (a *AdvRouter) RaiseEvent(event *common.Event) {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	for _, router := range a.routersList {
		router.RaiseEvent(event)
	}
}

// Connector implementations
func (a *AdvRouter) Forward(data interface{}) error {
	var errMap base.ErrorMap
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	for routerName, router := range a.routersList {
		err := router.Forward(data)
		if err != nil {
			if errMap == nil {
				errMap = make(base.ErrorMap)
			}
			errMap[routerName] = err
		}
	}

	if len(errMap) > 0 {
		return errors.New(base.FlattenErrorMap(errMap))
	} else {
		return nil
	}
}

func (a *AdvRouter) DownStreams() map[string]common.Part {
	totalMap := make(map[string]common.Part)

	a.mtx.RLock()
	defer a.mtx.RUnlock()

	for _, router := range a.routersList {
		outMap := router.DownStreams()
		for k, v := range outMap {
			totalMap[k] = v
		}
	}

	return totalMap
}

func (a *AdvRouter) AddDownStream(partId string, part common.Part) error {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	errMap := make(base.ErrorMap)
	for routerName, router := range a.routersList {
		err := router.AddDownStream(partId, part)
		if err != nil {
			errMap[routerName] = err
		}
	}

	if len(errMap) > 0 {
		return errors.New(base.FlattenErrorMap(errMap))
	} else {
		return nil
	}
}

func (a *AdvRouter) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	var errMap base.ErrorMap
	for routerName, router := range a.routersList {
		err := router.UpdateSettings(settings)
		if err != nil {
			errMap[routerName] = err
		}
	}

	if len(errMap) > 0 {
		return errors.New(base.FlattenErrorMap(errMap))
	} else {
		return nil
	}
}
