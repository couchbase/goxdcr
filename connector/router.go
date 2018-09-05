// Copyright (c) 2013-2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package connector

import (
	"errors"
	common "github.com/couchbase/goxdcr/common"
	component "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"sync"
)

// Router routes data to downstream parts

var ErrorInvalidRouterConfig = errors.New("Invalid Router configuration. Downstream parts and/or routing call back function are not defined.")
var ErrorInvalidRoutingResult = errors.New("Invalid results from routing algorithm.")

// call back function implementing the routing alrogithm
// @Param - data to be routed
// @Return - a map of partId to data to the routed to that part
type Routing_Callback_Func func(data interface{}) (map[string]interface{}, error)

/**
 * This is an inner router struct that XDCR router (parts/router) will inherit
 */
type Router struct {
	*component.AbstractComponent
	downStreamParts  map[string]common.Part // partId -> Part
	routing_callback *Routing_Callback_Func

	stateLock sync.RWMutex
}

func NewRouter(id string, downStreamParts map[string]common.Part,
	routing_callback *Routing_Callback_Func,
	logger_context *log.LoggerContext, logger_module string) *Router {
	router := &Router{
		AbstractComponent: component.NewAbstractComponentWithLogger(id, log.NewLogger(logger_module, logger_context)),
		downStreamParts:   downStreamParts,
		routing_callback:  routing_callback,
	}
	return router
}

func (router *Router) Forward(data interface{}) error {
	router.stateLock.RLock()
	defer router.stateLock.RUnlock()

	if len(router.downStreamParts) == 0 || *router.routing_callback == nil {
		return ErrorInvalidRouterConfig
	}

	routedData, err := (*router.routing_callback)(data)
	if err == nil {
		for partId, partData := range routedData {
			part := router.downStreamParts[partId]
			if part != nil {
				err = part.Receive(partData)
				if err != nil {
					break
				}
			} else {
				return ErrorInvalidRoutingResult
			}
		}
	}
	return err
}

func (router *Router) DownStreams() map[string]common.Part {
	router.stateLock.RLock()
	defer router.stateLock.RUnlock()

	return router.downStreamParts
}

func (router *Router) AddDownStream(partId string, part common.Part) error {
	router.stateLock.Lock()
	defer router.stateLock.Unlock()
	if part != nil {
		router.downStreamParts[partId] = part
	}
	return nil
}

// set or replace routing call back function.
// this may be allowed when router is still running
// Not used
func (router *Router) SetRoutingCallBackFunc(routing_callback *Routing_Callback_Func) {
	router.stateLock.Lock()
	defer router.stateLock.Unlock()

	router.routing_callback = routing_callback
}
