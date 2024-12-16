// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package connector

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/couchbase/goxdcr/v8/base"
	common "github.com/couchbase/goxdcr/v8/common"
	component "github.com/couchbase/goxdcr/v8/component"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	utilities "github.com/couchbase/goxdcr/v8/utils"
)

// Router routes data to downstream parts

var ErrorInvalidRouterConfig = errors.New("Invalid Router configuration. Downstream parts and/or routing call back function are not defined.")
var ErrorInvalidRoutingResult = errors.New("Invalid results from routing algorithm.")

// call back function implementing the routing alrogithm
// @Param - data to be routed
// @Return - a map of partId to data to the routed to that part
type Routing_Callback_Func func(data interface{}) (map[string]interface{}, error)

type CollectionsRerouteFunc func(data interface{}, partId string, preData *base.WrappedUprEvent) error

/**
 * This is an inner router struct that XDCR router (parts/router) will inherit
 */
type Router struct {
	*component.AbstractComponent
	downStreamParts  map[string]common.Part // partId -> Part
	routing_callback *Routing_Callback_Func

	startable uint32
	startFunc func() error
	stopFunc  func() error

	stateLock sync.RWMutex

	// If collections mapping temporarily failed, this function will be called
	// to try to re-forward it to the right downstream part
	collectionsRerouteCb CollectionsRerouteFunc

	upstreamObjRecycler utilities.RecycleObjFunc

	routingMapStringer func() map[string][]uint16
}

func (router *Router) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	// Nothing to do
	return nil
}

func NewRouter(id string, downStreamParts map[string]common.Part,
	routing_callback *Routing_Callback_Func,
	logger_context *log.LoggerContext, logger_module string,
	startable bool, startFunc, stopFunc func() error, collectionsRerouteCb CollectionsRerouteFunc,
	upstreamObjRecycler utilities.RecycleObjFunc,
	routingMapStringer func() map[string][]uint16) *Router {
	router := &Router{
		AbstractComponent:    component.NewAbstractComponentWithLogger(id, log.NewLogger(logger_module, logger_context)),
		downStreamParts:      downStreamParts,
		routing_callback:     routing_callback,
		startFunc:            startFunc,
		stopFunc:             stopFunc,
		collectionsRerouteCb: collectionsRerouteCb,
		upstreamObjRecycler:  upstreamObjRecycler,
		routingMapStringer:   routingMapStringer,
	}
	if startable {
		atomic.StoreUint32(&router.startable, 1)
	}
	return router
}

func (router *Router) SetStartable(startable bool) {
	if startable {
		atomic.StoreUint32(&router.startable, 1)
	} else {
		if atomic.CompareAndSwapUint32(&router.startable, 1, 0) && router.stopFunc != nil {
			router.Logger().Warnf("Setting Unstartable for router %v... stopping collectionsRouter", router.Id())
			router.stopFunc()
		}
	}
}

func (router *Router) IsStartable() bool {
	return atomic.LoadUint32(&router.startable) != 0
}

func (router *Router) Start() error {
	if !router.IsStartable() {
		return base.ErrorInvalidOperation
	}
	return router.startFunc()
}

func (router *Router) Stop() error {
	if router.stopFunc != nil {
		return router.stopFunc()
	} else {
		return nil
	}
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
	} else if err == base.ErrorRequestAlreadyIgnored || err == base.ErrorIgnoreRequest {
		err = nil
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

func (router *Router) GetUpstreamObjRecycler() func(interface{}) {
	return router.upstreamObjRecycler
}

func (r *Router) GetLayoutString(part common.Part) string {
	if r.routingMapStringer != nil {
		return fmt.Sprintf("\t\t%s :{\nroutingMap=%v}\n", r.Id(), r.routingMapStringer())
	}
	return ""
}

func (r *Router) RegisterUpstreamPart(part common.Part) error {
	// No op
	return nil
}
