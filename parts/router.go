// Copyright (c) 2018 Couchbase, Inc.
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
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"github.com/golang/snappy"
	"reflect"
	"sync/atomic"
	"time"
)

var ErrorInvalidDataForRouter = errors.New("Input data to Router is invalid.")
var ErrorNoDownStreamNodesForRouter = errors.New("No downstream nodes have been defined for the Router.")
var ErrorNoRoutingMapForRouter = errors.New("No routingMap has been defined for Router.")
var ErrorInvalidRoutingMapForRouter = errors.New("routingMap in Router is invalid.")

var IsHighReplicationKey = "IsHighReplication"
var NeedToThrottleKey = "NeedToThrottle"
var FilterExpDelKey = base.FilterExpDelKey

// enum for whether router needs to be throttled
const (
	NoNeedToThrottle int32 = 0
	NeedToThrottle   int32 = 1
)

type ReqCreator func(id string) (*base.WrappedMCRequest, error)

type FilterExpDelAtomicType struct {
	val uint32
}

func (f *FilterExpDelAtomicType) Set(value base.FilterExpDelType) {
	atomic.StoreUint32(&f.val, uint32(value))
}

func (f *FilterExpDelAtomicType) Get() base.FilterExpDelType {
	return base.FilterExpDelType(atomic.LoadUint32(&f.val))
}

// XDCR Router does two things:
// 1. converts UprEvent(DCP) to MCRequest (MemCached)
// 2. routes MCRequest to downstream parts
type Router struct {
	id string
	*connector.Router
	filter      *Filter
	routingMap  map[uint16]string // pvbno -> partId. This defines the loading balancing strategy of which vbnos would be routed to which part
	req_creator ReqCreator
	topic       string
	// whether lww conflict resolution mode has been enabled
	sourceCRMode base.ConflictResolutionMode
	utils        utilities.UtilsIface
	expDelMode   FilterExpDelAtomicType

	throughputThrottlerSvc service_def.ThroughputThrottlerSvc
	// whether the current replication is a high priority replication
	// when Priority or Ongoing setting is changed, this field will be updated through UpdateSettings() call
	isHighReplication *base.AtomicBooleanType

	// Instead of wasting memory, use these datapools
	dcpObjRecycler utilities.RecycleObjFunc
	dataPool       utilities.DataPoolIface
	mcRequestPool  *base.MCRequestPool
}

/**
 * Note
 * A router (for now) is created per source nozzle.
 * Input:
 * 1. downStreamParts - a map of <targetNozzleID> -> <TargetNozzle>.
 * 		The map only includes the targets that this source (router) is responsible for replicating.
 * 2. routingMap == vbNozzleMap, which is a map of <vbucketID> -> <targetNozzleID>
 * 3+ Rest should be relatively obv
 */
func NewRouter(id string, spec *metadata.ReplicationSpecification,
	downStreamParts map[string]common.Part,
	routingMap map[uint16]string,
	sourceCRMode base.ConflictResolutionMode,
	logger_context *log.LoggerContext, req_creator ReqCreator,
	utilsIn utilities.UtilsIface,
	throughputThrottlerSvc service_def.ThroughputThrottlerSvc,
	isHighReplication bool,
	filterExpDelType base.FilterExpDelType,
	dcpObjRecycler utilities.RecycleObjFunc) (*Router, error) {
	var filter *Filter
	var err error
	var filterPrintMsg string = "<nil>"

	topic := spec.Id
	filterExpression, ok := spec.Settings.Values[metadata.FilterExpressionKey].(string)
	if !ok {
		// No filterExpression
		filterExpression = ""
	}

	expDelMode := spec.Settings.GetExpDelMode()
	filter, err = NewFilter(id, filterExpression, utilsIn, expDelMode.IsSkipReplicateUncommittedTxnSet())
	if err != nil {
		return nil, err
	}
	if len(filterExpression) > 0 {
		filterPrintMsg = fmt.Sprintf("%v%v%v", base.UdTagBegin, filter.filterExpressionInternal, base.UdTagEnd)
	}

	router := &Router{
		id:                     id,
		filter:                 filter,
		routingMap:             routingMap,
		topic:                  topic,
		sourceCRMode:           sourceCRMode,
		req_creator:            req_creator,
		utils:                  utilsIn,
		isHighReplication:      base.NewAtomicBooleanType(isHighReplication),
		throughputThrottlerSvc: throughputThrottlerSvc,
		dataPool:               utilities.NewDataPool(),
		mcRequestPool:          base.NewMCRequestPool(spec.Id, nil /*logger*/),
		dcpObjRecycler:         dcpObjRecycler,
	}

	router.expDelMode.Set(filterExpDelType)

	// routingFunc is the main intelligence of the router's functionality
	var routingFunc connector.Routing_Callback_Func = router.route
	router.Router = connector.NewRouter(id, downStreamParts, &routingFunc, logger_context, "XDCRRouter", router.recycleDataObj)

	router.Logger().Infof("%v created with %d downstream parts isHighReplication=%v and filter=%v\n", router.id, len(downStreamParts), isHighReplication, filterPrintMsg)
	return router, nil
}

func (router *Router) ComposeMCRequest(wrappedEvent *base.WrappedUprEvent) (*base.WrappedMCRequest, error) {
	wrapped_req, err := router.newWrappedMCRequest()
	if err != nil {
		return nil, err
	}

	event := wrappedEvent.UprEvent

	req := wrapped_req.Req
	req.Cas = event.Cas
	req.Opaque = 0
	req.VBucket = event.VBucket
	req.Key = event.Key

	if wrappedEvent.Flags.ShouldUseDecompressedValue() {
		// The decompresedValue will get recycled before this mcRequest is passed down to Xmem
		// Copy the data to another recycled slice
		recycledSlice, err := router.dataPool.GetByteSlice(uint64(len(wrappedEvent.DecompressedValue)))
		if err != nil {
			return nil, err
		}
		wrapped_req.SlicesToBeReleasedMtx.Lock()
		if wrapped_req.SlicesToBeReleasedByRouter == nil {
			wrapped_req.SlicesToBeReleasedByRouter = make([][]byte, 0, 1)
		}
		wrapped_req.SlicesToBeReleasedByRouter = append(wrapped_req.SlicesToBeReleasedByRouter, recycledSlice)
		wrapped_req.SlicesToBeReleasedMtx.Unlock()
		if wrappedEvent.UprEvent.IsSnappyDataType() {
			// snappy.Encode performs trim on recycledSlice when it returns
			recycledSlice = snappy.Encode(recycledSlice, wrappedEvent.DecompressedValue)
		} else {
			copy(recycledSlice, wrappedEvent.DecompressedValue)
			// Recycled slice may have extra garbage at the end. Trim it manually
			recycledSlice = recycledSlice[0:len(wrappedEvent.DecompressedValue)]
		}
		req.Body = recycledSlice
	} else {
		req.Body = event.Value
	}
	//opCode
	req.Opcode = event.Opcode
	req.DataType = event.DataType

	//extra
	if event.Opcode == mc.UPR_MUTATION || event.Opcode == mc.UPR_DELETION ||
		event.Opcode == mc.UPR_EXPIRATION {

		extrasSize := 24
		if router.sourceCRMode == base.CRMode_LWW || event.Opcode == mc.UPR_EXPIRATION {
			extrasSize = 28
		}
		if len(req.Extras) != extrasSize {
			req.Extras = make([]byte, extrasSize)
		}

		//    <<Flg:32, Exp:32, SeqNo:64, CASPart:64, Options:32>>.
		binary.BigEndian.PutUint32(req.Extras[0:4], event.Flags)
		binary.BigEndian.PutUint32(req.Extras[4:8], event.Expiry)
		binary.BigEndian.PutUint64(req.Extras[8:16], event.RevSeqno)
		binary.BigEndian.PutUint64(req.Extras[16:24], event.Cas)

		var options uint32
		if router.sourceCRMode == base.CRMode_LWW {
			// if source bucket is of lww type, add FORCE_ACCEPT_WITH_META_OPS options for memcached
			options |= base.FORCE_ACCEPT_WITH_META_OPS
		}
		if event.Opcode == mc.UPR_EXPIRATION {
			options |= base.IS_EXPIRATION
		}
		if options > 0 {
			binary.BigEndian.PutUint32(req.Extras[24:28], options)
		}

	} else if event.Opcode == mc.UPR_SNAPSHOT {
		if len(req.Extras) != 28 {
			req.Extras = make([]byte, 28)
		}
		binary.BigEndian.PutUint64(req.Extras[0:8], event.Seqno)
		binary.BigEndian.PutUint64(req.Extras[8:16], event.SnapstartSeq)
		binary.BigEndian.PutUint64(req.Extras[16:24], event.SnapendSeq)
		binary.BigEndian.PutUint32(req.Extras[24:28], event.SnapshotType)
	}

	wrapped_req.Seqno = event.Seqno
	wrapped_req.Start_time = time.Now()
	wrapped_req.ConstructUniqueKey()

	return wrapped_req, nil
}

// Implementation of the routing algorithm
// Currently doing static dispatching based on vbucket number.
func (router *Router) route(data interface{}) (map[string]interface{}, error) {
	router.throttle()

	result := make(map[string]interface{})

	wrappedUpr, ok := data.(*base.WrappedUprEvent)
	if !ok {
		return nil, ErrorInvalidDataForRouter
	}

	// only *mc.UprEvent type data is accepted
	uprEvent := wrappedUpr.UprEvent

	if router.routingMap == nil {
		return nil, ErrorNoRoutingMapForRouter
	}

	// use vbMap to determine which downstream part to route the request
	partId, ok := router.routingMap[uprEvent.VBucket]
	if !ok {
		return nil, ErrorInvalidRoutingMapForRouter
	}

	shouldContinue := router.ProcessExpDelTTL(uprEvent)
	if !shouldContinue {
		router.RaiseEvent(common.NewEvent(common.DataFiltered, uprEvent, router, nil, nil))
		return result, nil
	}

	needToReplicate, err, errDesc, failedDpCnt := router.filter.FilterUprEvent(wrappedUpr)
	if failedDpCnt > 0 {
		router.RaiseEvent(common.NewEvent(common.DataPoolGetFail, failedDpCnt, router, nil, nil))
	}
	if router.Logger().GetLogLevel() >= log.LogLevelDebug && errDesc != "" {
		router.Logger().Debugf("Matcher doc %v%v%v matched: %v with error: %v and additional info: %v",
			base.UdTagBegin, string(uprEvent.Key), base.UdTagEnd, needToReplicate, err, errDesc)
	}
	if !needToReplicate || err != nil {
		if err != nil {
			// Let pipeline supervisor do the logging
			router.RaiseEvent(common.NewEvent(common.DataUnableToFilter, uprEvent, router, []interface{}{err, errDesc}, nil))
		} else {
			// if data does not need to be replicated, drop it. return empty result
			router.RaiseEvent(common.NewEvent(common.DataFiltered, uprEvent, router, nil, nil))
		}
		// Let supervisor set the err instead of the router, to minimize pipeline interruption
		return result, nil
	}

	mcRequest, err := router.ComposeMCRequest(wrappedUpr)

	defer router.dcpObjRecycler(wrappedUpr)

	if err != nil {
		return nil, router.utils.NewEnhancedError("Error creating new memcached request.", err)
	}
	result[partId] = mcRequest
	return result, nil
}

func (router *Router) throttle() {
	// this statement before the for loop is to ensure that
	// we do not incur the overhead of collecting start time
	// and raising event when throttling does not happen
	if router.throughputThrottlerSvc.CanSend(router.isHighReplication.Get()) {
		return
	}

	start_time := time.Now()
	for {
		if router.throughputThrottlerSvc.CanSend(router.isHighReplication.Get()) {
			break
		} else {
			router.throughputThrottlerSvc.Wait()
		}
	}

	router.RaiseEvent(common.NewEvent(common.DataThroughputThrottled, nil, router, nil, time.Since(start_time)))

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

func (router *Router) updateExpDelMode(expDelModeObj interface{}) error {
	expDelMode, ok := expDelModeObj.(base.FilterExpDelType)
	if !ok {
		err := fmt.Errorf("%v invalid data type for expDelMode. value = %v\n", router.id, expDelMode)
		router.Logger().Warn(err.Error())
		return err
	}

	router.Logger().Infof("Router %v's Deletion/Expiration filter method: %v\n", router.id, expDelMode.LogString())

	router.expDelMode.Set(expDelMode)
	router.filter.SetShouldSkipUncommittedTxn(expDelMode.IsSkipReplicateUncommittedTxnSet())
	return nil
}

func (router *Router) updateHighRepl(isHighReplicationObj interface{}) error {
	isHighReplication, ok := isHighReplicationObj.(bool)
	if !ok {
		err := fmt.Errorf("%v invalid data type for isHighReplication. value = %v\n", router.id, isHighReplicationObj)
		router.Logger().Warn(err.Error())
		return err
	}

	router.Logger().Infof("%v changing isHighReplication to %v\n", router.id, isHighReplication)
	router.isHighReplication.Set(isHighReplication)
	return nil
}

func (router *Router) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	errMap := make(base.ErrorMap)

	isHighReplicationObj, ok := settings[IsHighReplicationKey]
	if ok {
		err := router.updateHighRepl(isHighReplicationObj)
		if err != nil {
			errMap["UpdatingIsHighReplication"] = err
		}
	}

	expDelModeObj, ok := settings[metadata.FilterExpDelKey]
	if ok {
		err := router.updateExpDelMode(expDelModeObj)
		if err != nil {
			errMap["UpdatingFilterExpDel"] = err
		}
	}

	if len(errMap) > 0 {
		return fmt.Errorf("Router %v UpdateSettings error(s): %v", router.id, base.FlattenErrorMap(errMap))
	} else {
		return nil
	}
}

func (router *Router) newWrappedMCRequest() (*base.WrappedMCRequest, error) {
	newReq := router.mcRequestPool.Get()
	newReq.Req = &mc.MCRequest{}
	return newReq, nil
}

// Returns bool indicating if the data should continue to be sent
func (router *Router) ProcessExpDelTTL(uprEvent *mcc.UprEvent) bool {
	expDelMode := router.expDelMode.Get()
	if expDelMode == base.FilterExpDelNone {
		return true
	}

	if expDelMode&base.FilterExpDelSkipDeletes > 0 {
		if uprEvent.Opcode == mc.UPR_DELETION {
			return false
		}
	}

	if expDelMode&base.FilterExpDelSkipExpiration > 0 {
		if uprEvent.Opcode == mc.UPR_EXPIRATION {
			return false
		}
	}

	if expDelMode&base.FilterExpDelStripExpiration > 0 {
		uprEvent.Expiry = uint32(0)
		router.RaiseEvent(common.NewEvent(common.ExpiryFieldStripped, uprEvent, router, nil, nil))
	}

	return true
}

func (router *Router) recycleDataObj(obj interface{}) {
	switch obj.(type) {
	case *base.WrappedUprEvent:
		if router.dcpObjRecycler != nil {
			router.dcpObjRecycler(obj)
		}
	case *base.WrappedMCRequest:
		if router.mcRequestPool != nil {
			req := obj.(*base.WrappedMCRequest)
			req.SlicesToBeReleasedMtx.Lock()
			if len(req.SlicesToBeReleasedByRouter) > 0 {
				for _, slice := range req.SlicesToBeReleasedByRouter {
					router.dataPool.PutByteSlice(slice)
				}
			}
			req.SlicesToBeReleasedByRouter = nil
			req.SlicesToBeReleasedMtx.Unlock()
			router.mcRequestPool.Put(obj.(*base.WrappedMCRequest))
		}
	default:
		panic(fmt.Sprintf("Coding bug type is %v", reflect.TypeOf(obj)))
	}
}
