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
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"sync"
	"sync/atomic"
)

// Advanced router is a conglomerate of multiple regular routers
type AdvRouter struct {
	common.AdvConnector
	id string

	routersOpen    map[string]*uint32
	routersList    map[string]*Router
	topicRouterMap map[string]*Router
	mtx            sync.RWMutex

	logger    *log.CommonLogger
	debugOnce sync.Once
}

func NewAdvRouter(id string, logger *log.CommonLogger) *AdvRouter {
	return &AdvRouter{
		logger:         logger,
		routersList:    make(map[string]*Router),
		routersOpen:    make(map[string]*uint32),
		topicRouterMap: make(map[string]*Router),
		id:             id,
	}
}

func (a *AdvRouter) Open(topic string) {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	router, found := a.topicRouterMap[topic]
	if !found {
		panic(fmt.Sprintf("Adv router not found for topic %v", topic))
	}
	atomic.StoreUint32(a.routersOpen[router.Id()], 1)
	a.logger.Infof("Advanced router %v opened %v", a.Id(), router.Id())
}

func (a *AdvRouter) Close(topic string) {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	router, found := a.topicRouterMap[topic]
	if !found {
		panic(fmt.Sprintf("Adv router not found for topic %v", topic))
	}
	atomic.StoreUint32(a.routersOpen[router.Id()], 0)
	a.logger.Infof("Advanced router %v closed %v", a.Id(), router.Id())
}

func (a *AdvRouter) RemoveRouter(topic string, router *Router) {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	delete(a.topicRouterMap, topic)
	delete(a.routersList, router.Id())
	delete(a.routersOpen, router.Id())
}

func (a *AdvRouter) AddRouter(topic string, router *Router) {
	if router == nil {
		return
	}

	a.mtx.Lock()
	defer a.mtx.Unlock()
	_, exists := a.routersList[router.Id()]
	a.logger.Infof("NEIL DEBUG advRouter %v added router: %v alreadyExists? %v", a.Id(), router.Id(), exists)
	a.routersList[router.Id()] = router
	a.topicRouterMap[topic] = router

	var state uint32
	a.routersOpen[router.Id()] = &state
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
	//	a.mtx.RLock()
	//	defer a.mtx.RUnlock()
	//
	//	errMap := make(base.ErrorMap)
	//	for routerName, router := range a.routersList {
	//		err := router.RegisterComponentEventListener(eventType, listener)
	//		if err != nil {
	//			errMap[routerName] = err
	//		}
	//	}
	//
	//	if len(errMap) > 0 {
	//		return errors.New(base.FlattenErrorMap(errMap))
	//	} else {
	//		return nil
	//	}
	return base.ErrorNotImplemented
}

func (a *AdvRouter) UnRegisterComponentEventListener(eventType common.ComponentEventType, listener common.ComponentEventListener) error {
	//	a.mtx.RLock()
	//	defer a.mtx.RUnlock()
	//
	//	errMap := make(base.ErrorMap)
	//	for routerName, router := range a.routersList {
	//		err := router.UnRegisterComponentEventListener(eventType, listener)
	//		if err != nil {
	//			errMap[routerName] = err
	//		}
	//	}
	//
	//	if len(errMap) > 0 {
	//		return errors.New(base.FlattenErrorMap(errMap))
	//	} else {
	//		return nil
	//	}
	return base.ErrorNotImplemented
}

func (a *AdvRouter) RegisterSpecificComponentEventListener(topic string, eventType common.ComponentEventType, listener common.ComponentEventListener) error {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	router, found := a.topicRouterMap[topic]
	if !found {
		return base.ErrorNotFound
	}

	return router.RegisterComponentEventListener(eventType, listener)
}

func (a *AdvRouter) UnRegisterSpecificComponentEventListener(topic string, eventType common.ComponentEventType, listener common.ComponentEventListener) error {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	router, found := a.topicRouterMap[topic]
	if !found {
		return base.ErrorNotFound
	}

	return router.UnRegisterComponentEventListener(eventType, listener)
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

func (a *AdvRouter) SpecificAsyncComponentEventListeners(topic string) map[string]common.AsyncComponentEventListener {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	router, ok := a.topicRouterMap[topic]
	if !ok {
		return nil
	}
	return router.AsyncComponentEventListeners()
}

func (a *AdvRouter) RaiseEvent(event *common.Event) {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	for _, router := range a.routersList {
		open := atomic.LoadUint32(a.routersOpen[router.Id()]) != 0
		if open {
			router.RaiseEvent(event)
		}
	}
}

// Connector implementations
func (a *AdvRouter) Forward(data interface{}) error {
	var errMap base.ErrorMap
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	var debugString string
	for routerName, router := range a.routersList {
		open := atomic.LoadUint32(a.routersOpen[routerName]) != 0
		if !open {
			return nil
		}
		err := router.Forward(data)
		debugString = fmt.Sprintf("%v%v%v", debugString, " forwardedTo:", routerName)
		if err != nil {
			if errMap == nil {
				errMap = make(base.ErrorMap)
			}
			errMap[routerName] = err
		}
	}

	a.debugOnce.Do(func() {
		a.logger.Infof("NEIL DEBUG advRouter %v", debugString)
	})

	if len(errMap) > 0 {
		return errors.New(base.FlattenErrorMap(errMap))
	} else {
		return nil
	}
}

func (a *AdvRouter) SpecificDownStreams(topic string) map[string]common.Part {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	router, ok := a.topicRouterMap[topic]
	if !ok {
		return nil
	}
	return router.DownStreams()
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
