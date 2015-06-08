// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package parts

import (
	"encoding/binary"
	"errors"
	"fmt"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	connector "github.com/couchbase/goxdcr/connector"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/utils"
	"regexp"
	"time"
)

var ErrorInvalidDataForRouter = errors.New("Input data to Router is invalid.")
var ErrorNoDownStreamNodesForRouter = errors.New("No downstream nodes have been defined for the Router.")
var ErrorNoRoutingMapForRouter = errors.New("No routingMap has been defined for Router.")
var ErrorInvalidRoutingMapForRouter = errors.New("routingMap in Router is invalid.")

type ReqCreator func(id string) *base.WrappedMCRequest

// XDCR Router does two things:
// 1. converts UprEvent to MCRequest
// 2. routes MCRequest to downstream parts
type Router struct {
	id string
	*connector.Router
	filterRegexp *regexp.Regexp    // filter expression
	routingMap   map[uint16]string // pvbno -> partId. This defines the loading balancing strategy of which vbnos would be routed to which part
	//Debug only, need to be rolled into statistics and monitoring
	counter     map[string]int
	req_creator ReqCreator
	topic       string
}

func NewRouter(id string, topic string, filterExpression string,
	downStreamParts map[string]common.Part,
	routingMap map[uint16]string,
	logger_context *log.LoggerContext, req_creator ReqCreator) (*Router, error) {
	// compile filter expression
	var filterRegexp *regexp.Regexp
	var err error
	if len(filterExpression) > 0 {
		filterRegexp, err = regexp.Compile(filterExpression)
		if err != nil {
			return nil, err
		}
	}
	router := &Router{
		id:           id,
		filterRegexp: filterRegexp,
		routingMap:   routingMap,
		counter:      make(map[string]int),
		topic:        topic,
		req_creator:  req_creator}

	var routingFunc connector.Routing_Callback_Func = router.route
	router.Router = connector.NewRouter(id, downStreamParts, &routingFunc, logger_context, "XDCRRouter")

	//initialize counter
	for partId, _ := range downStreamParts {
		router.counter[partId] = 0
	}

	router.Logger().Infof("Router created with %d downstream parts \n", len(downStreamParts))
	return router, nil
}

func (router *Router) ComposeMCRequest(event *mcc.UprEvent) *base.WrappedMCRequest {
	wrapped_req := router.newWrappedMCRequest()

	req := wrapped_req.Req
	req.Cas = event.Cas
	req.Opaque = 0
	req.VBucket = event.VBucket
	req.Key = event.Key
	req.Body = event.Value
	if req.Extras == nil{
		req.Extras = make([]byte, 24)
	}
	//opCode
	req.Opcode = event.Opcode

	//extra
	if event.Opcode == mc.UPR_MUTATION || event.Opcode == mc.UPR_DELETION ||
		event.Opcode == mc.UPR_EXPIRATION {
		//    <<Flg:32, Exp:32, SeqNo:64, CASPart:64, 0:32>>.
		binary.BigEndian.PutUint32(req.Extras[0:4], event.Flags)
		binary.BigEndian.PutUint32(req.Extras[4:8], event.Expiry)
		binary.BigEndian.PutUint64(req.Extras[8:16], event.RevSeqno)
		binary.BigEndian.PutUint64(req.Extras[16:24], event.Cas)
	} else if event.Opcode == mc.UPR_SNAPSHOT {
		binary.BigEndian.PutUint64(req.Extras[0:8], event.Seqno)
		binary.BigEndian.PutUint64(req.Extras[8:16], event.SnapstartSeq)
		binary.BigEndian.PutUint64(req.Extras[16:24], event.SnapendSeq)
		binary.BigEndian.PutUint32(req.Extras[24:28], event.SnapshotType)
	}

	wrapped_req.Seqno = event.Seqno
	wrapped_req.Start_time = time.Now()
	return wrapped_req
}

// Implementation of the routing algorithm
// Currently doing static dispatching based on vbucket number.
func (router *Router) route(data interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	// only *mc.UprEvent type data is accepted
	uprEvent, ok := data.(*mcc.UprEvent)
	if !ok {
		return nil, ErrorInvalidDataForRouter
	}

	if router.routingMap == nil {
		return nil, ErrorNoRoutingMapForRouter
	}

	// use vbMap to determine which downstream part to route the request
	partId, ok := router.routingMap[uprEvent.VBucket]
	if !ok {
		return nil, ErrorInvalidRoutingMapForRouter
	}

	router.Logger().Debugf("Data with key=%v, vbno=%d, opCode=%v is routed to downstream part %s", string(uprEvent.Key), uprEvent.VBucket, uprEvent.Opcode, partId)

	// filter data if filter expession has been defined
	if router.filterRegexp != nil {
		if !utils.RegexpMatch(router.filterRegexp, uprEvent.Key) {
			// if data does not match filter expression, drop it. return empty result
			router.RaiseEvent(common.NewEvent(common.DataFiltered, uprEvent, router, nil, nil))
			router.Logger().Debugf("Data with key=%v, vbno=%d, opCode=%v has been filtered out", string(uprEvent.Key), uprEvent.VBucket, uprEvent.Opcode)
			return result, nil
		}
	}
	result[partId] = router.ComposeMCRequest(uprEvent)
	router.counter[partId] = router.counter[partId] + 1
	return result, nil
}

func (router *Router) SetRoutingMap(routingMap map[uint16]string) {
	router.routingMap = routingMap
	router.Logger().Debugf("Set vbMap %v in Router", routingMap)
}

func (router *Router) RoutingMap() map[uint16]string {
	return router.routingMap
}

func (router *Router) RoutingMapByDownstreams() map[string][]uint16 {
	ret := make(map[string][]uint16)
	for vbno, partId := range router.routingMap {
		vblist, ok := ret[partId]
		if !ok {
			vblist = []uint16{}
			ret[partId] = vblist
		}

		vblist = append(vblist, vbno)
		ret[partId] = vblist
	}
	return ret
}
func (router *Router) StatusSummary() string {
	return fmt.Sprintf("Rounter %v = %v", router.id, router.counter)

}

func (router *Router) newWrappedMCRequest() *base.WrappedMCRequest {
	if router.req_creator != nil {
		return router.req_creator(router.topic)
	} else {
		return &base.WrappedMCRequest{Seqno: 0,
			Req: &mc.MCRequest{},
		}
	}
}
