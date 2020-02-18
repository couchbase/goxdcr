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
	"math"
	"reflect"
	"sync"
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
var ForceCollectionDisableKey = "ForceCollectionDisable"

// A function used by the router to raise routing updates to other services that need to know
type CollectionsRoutingUpdater func(CollectionsRoutingInfo)

// When data cannot be sent because mapping is broken, this function is used to raise the event
type IgnoreDataEventer func(*base.WrappedMCRequest)

/**
 * The RoutingInfo is used to communicate between router and checkpoint manager
 * When collection routing routes are broken, they are saved alongside the checkpoints
 * so when replication resumes from the checkpoint, the "exact state" that was saved
 * will be loaded back into the router... and the router then can figure out which
 * routes will need to be backfilled if the target manifest has moved forward
 */
type CollectionsRoutingInfo struct {
	BrokenMap   metadata.CollectionNamespaceMapping
	BackfillMap metadata.CollectionNamespaceMapping
	// The target manifest that the above routing information is based on
	TargetManifestId uint64
}

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

type RouterIface interface {
	Start() error
	Stop() error
	Route(data interface{}) (map[string]interface{}, error)
	RouteCollection(*base.WrappedMCRequest, string) error
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

	// Each router is responsible for routing incoming DCP packet to a corresponding XMEM
	// When collections are used, it is responsible for mapping to the target collection
	collectionsRouting CollectionsRoutingMap
}

/**
 * Each router currently sits behind each DCP nozzle
 * Requests are piped down from DCP nozzle to the router, and to one or more XMEM nozzles
 * IF collections are used, then each *CollectionRouter* will sit *in front* of each XMEM nozzles
 * Each XMEM nozzle talks to one single target node, and because target manifests are distributed
 * in an eventually consistent method, it is possible that one target node may see a newer manifest
 * being destributed than the other.
 * In the same way, each CollectionsRouter will experience different perspective of the target cluster
 * through different target nodes, but always will be in an eventually consistent behavior.
 *
 * Here is how the collection router handles the different situations:
 *
 * 1. Lagging target nodes when a new manifest is created
 * When a new manifest is created on the target, some nodes may get it quicker than the others.
 * If RemoteClusterAgent pulled the laggy node last, it may not have the latest manifest
 * The CollectionsRouter has a retry mechanism where it will buffer up and retry for this case
 *
 * 2. Broken Mapping
 * When a CollectionsRouter determines that a mapping is no longer valid after the retries (#1)
 * then it will declare the mapping broken. When a mapping is marked broken, further requests
 * that follow this map will simply be ignored and we will save the look-up and retry times
 *  It will do 2 things in strict order (and other svcs must follow):
 *    a. Note the broken mappings
 *    b. Raise events to mark failed requests as "Ignored" so pipeline so that the pipeline continues
 * (a) is needed to make sure that:	the broken mappings are saved and restored so things can be
 *     efficient when resuming.  (i.e. heavy disk backfill may occur and
 *     there wont' be subjected to detecting broken mapping penalty from #1
 * (b) is needed to obviously ensure that pipelines do not get stuck
 *
 * 3. Backfills Needed (TODO - Backfill Milestone)
 * Backfills need to be created when a mapped target collection is created. There are two ways to detect
 * it - 1. data path, and 2. static path (when no data is flowing)
 * CollectionsRouter is considered the data path detection, and will raise backfill if it is needed
 * The static path will be taken care of by Collections Manifest Service
 */
type CollectionsRouter struct {
	collectionsManifestSvc service_def.CollectionsManifestSvc
	spec                   *metadata.ReplicationSpecification
	finCh                  chan bool
	started                uint32
	logger                 *log.CommonLogger
	waitGrp                sync.WaitGroup

	// Requests that failed collection mapping routing will be put into this buffer
	// and every retryInterval, all the data in this buffer will be retried
	retryBufferCh    chan interface{}
	retryInterval    time.Duration
	retryForwardFunc func(interface{}) error

	// No lock needed as this is going to be called only by the mother-Router
	// which is run on a single go-routine
	lastKnownManifest *metadata.CollectionsManifest

	// dynamic
	re_retryQueueInitialized uint32
	tempRetryCh              chan interface{}

	/*
	 * When retry fails, these members record the needed information
	 */
	// Collectively, these are the mappings that this collectionsRouter has seen broken
	brokenMapMtx   sync.RWMutex
	brokenMapping  metadata.CollectionNamespaceMapping
	routingUpdater CollectionsRoutingUpdater
	ignoreDataFunc IgnoreDataEventer

	fatalErrorFunc func(error)
}

func NewCollectionsRouter(colManifestSvc service_def.CollectionsManifestSvc,
	spec *metadata.ReplicationSpecification,
	logger *log.CommonLogger,
	retryForwardFunc func(interface{}) error,
	retryInterval time.Duration,
	retryBufferCap int,
	routingUpdater CollectionsRoutingUpdater,
	ignoreDataFunc IgnoreDataEventer,
	fatalErrorFunc func(error)) *CollectionsRouter {
	return &CollectionsRouter{
		collectionsManifestSvc: colManifestSvc,
		spec:                   spec,
		finCh:                  make(chan bool),
		logger:                 logger,
		retryForwardFunc:       retryForwardFunc,
		retryBufferCh:          make(chan interface{}, retryBufferCap),
		tempRetryCh:            make(chan interface{}, retryBufferCap),
		retryInterval:          retryInterval,
		brokenMapping:          make(metadata.CollectionNamespaceMapping),
		routingUpdater:         routingUpdater,
		ignoreDataFunc:         ignoreDataFunc,
		fatalErrorFunc:         fatalErrorFunc,
	}
}

func (c *CollectionsRouter) Start() error {
	atomic.StoreUint32(&c.started, 1)
	c.waitGrp.Add(1)
	go c.runRetry()
	return nil
}

func (c *CollectionsRouter) Stop() error {
	if atomic.CompareAndSwapUint32(&c.started, 1, 0) {
		close(c.finCh)
	}
	c.waitGrp.Wait()
	return nil
}

func (c *CollectionsRouter) IsRunning() bool {
	return atomic.LoadUint32(&c.started) != 0
}

// No-Concurrent call
func (c *CollectionsRouter) RouteReqToLatestTargetManifest(namespace *base.CollectionNamespace) (colId, manifestId uint64, err error) {
	if !c.IsRunning() {
		err = PartStoppedError
		return
	}

	latestManifest, err := c.collectionsManifestSvc.GetSpecificTargetManifest(c.spec, math.MaxUint64)
	if err != nil {
		return
	}
	if latestManifest == nil {
		err = base.ErrorInvalidInput
		return
	}

	err = c.handleNewManifestChanges(latestManifest)
	if err != nil {
		return
	}

	manifestId = latestManifest.Uid()

	colId, err = latestManifest.GetCollectionId(namespace.ScopeName, namespace.CollectionName)
	if err == base.ErrorNotFound {

		c.brokenMapMtx.RLock()
		_, _, exists := c.brokenMapping.Get(namespace)
		c.brokenMapMtx.RUnlock()

		if exists {
			err = base.ErrorIgnoreRequest
		} else {
			err = fmt.Errorf("Target manifest version %v does not include scope %v collection %v", manifestId, namespace.ScopeName, namespace.CollectionName)
		}
	}
	return
}

func (c *CollectionsRouter) handleNewManifestChanges(latestManifest *metadata.CollectionsManifest) (err error) {
	if c.lastKnownManifest == nil {
		c.lastKnownManifest = latestManifest
		return
	}

	if latestManifest.Uid() == c.lastKnownManifest.Uid() {
		return
	}

	added, _, _, err := latestManifest.Diff(c.lastKnownManifest)
	if err != nil {
		err = fmt.Errorf("Unable to diff manifests between %v and %v: %v", c.lastKnownManifest.Uid(), latestManifest.Uid(), err)
		return
	}

	var lockUpgraded bool
	c.brokenMapMtx.RLock()
	if len(added) > 0 && len(c.brokenMapping) > 0 {
		c.brokenMapMtx.RUnlock()
		// These are new target collections - see if the broken ones can be backfilled
		fixedMapping := c.brokenMapping.GetSubsetBasedOnAddedTargets(added)

		if len(fixedMapping) > 0 {
			var routingInfo CollectionsRoutingInfo
			c.brokenMapMtx.Lock()
			lockUpgraded = true
			c.brokenMapping = c.brokenMapping.Delete(fixedMapping)
			routingInfo.BrokenMap = c.brokenMapping.Clone()
			routingInfo.BackfillMap = fixedMapping.Clone()
			routingInfo.TargetManifestId = latestManifest.Uid()
			c.logger.Debugf("Based on target manifestID %v These mappings need backfilling: %v\n", latestManifest.Uid(), fixedMapping.String())
			c.logger.Debugf("Based on target manifestID %v These mappings are broken: %v\n", latestManifest.Uid(), routingInfo.BrokenMap.String())
			c.brokenMapMtx.Unlock()
			c.routingUpdater(routingInfo)
		}

	}
	if !lockUpgraded {
		c.brokenMapMtx.RUnlock()
	}

	c.lastKnownManifest = latestManifest
	return
}

// This will attempt to retry the mutation by queuing it back in the queue if possible
// Note - will block if full
func (c *CollectionsRouter) retryRequest(req interface{}) error {
	select {
	case <-c.finCh:
		return PartStoppedError
	case c.retryBufferCh <- req:
		return nil
	default:
		return c.emergencyProcessOneReq(req)
	}
}

// Should only be run by runRetry goroutine
func (c *CollectionsRouter) addToTemporaryRetryQueue(req interface{}) error {
	wrappedMcr, ok := req.(*base.WrappedMCRequest)
	if !ok {
		panic(fmt.Sprintf("coding error type is: %v", reflect.TypeOf((req))))
	}
	if wrappedMcr.ColInfo.RoutingRetryCnt >= base.MaxCollectionsRoutingRetry {
		c.recordUnroutableRequest(req)
		// Record UnroutableRequest means this req is ignored
		return nil
	} else {
		wrappedMcr.ColInfo.RoutingRetryCnt++
	}

	select {
	case <-c.finCh:
		return PartStoppedError
	case c.tempRetryCh <- req:
		return nil
	default:
		// buffer is full
		return c.emergencyProcessOneReqFromTemporaryQueue(req)
	}
}

// Should only be run by runRetry
func (c *CollectionsRouter) moveItemsFromTempRetryQueue() error {
	for {
		select {
		case <-c.finCh:
			return PartStoppedError
		case req := <-c.tempRetryCh:
			select {
			case <-c.finCh:
				return PartStoppedError
			case c.retryBufferCh <- req:
				// happy path
			default:
				// retryBuffer full
				c.emergencyProcessOneReqFromTemporaryQueue(req)
			}
		default:
			// All has been processed
			return nil
		}
	}
}

// Run when temporary queue is full
// Its job is to pop off one item from retryBufferCh for immediate processing,
// pop one off from temporary queue and put it into retryBuffer
// and then put the incomingReq into the temporary queue
func (c *CollectionsRouter) emergencyProcessOneReqFromTemporaryQueue(incomingReq interface{}) error {
	for {
		select {
		case <-c.finCh:
			return PartStoppedError
		case tempReq := <-c.tempRetryCh:
			c.tempRetryCh <- incomingReq
			select {
			case <-c.finCh:
				return PartStoppedError
			case processReq := <-c.retryBufferCh:
				c.retryBufferCh <- tempReq
				// Flush the retryBufferCh
				err := c.retryForwardFunc(processReq)
				if err != nil && err != base.ErrorIgnoreRequest {
					c.recordUnroutableRequest(processReq)
				}
				return nil
			default:
				// retryBufferCh had nothing to take out
				select {
				case <-c.finCh:
					return PartStoppedError
				case c.retryBufferCh <- tempReq:
					return nil
				default:
					// For really odd emergency - just process the tempReq out of order
					err := c.retryForwardFunc(tempReq)
					if err != nil && err != base.ErrorIgnoreRequest {
						c.recordUnroutableRequest(tempReq)
					}
					return nil
				}
			}
		default:
			// Nothing to move
			return nil
		}
	}
}

// Essentially, the #1 request waiting to be retried will immediately be called to retry
// And the Maxnumber of retries no longer applies to it. Any failed error will immediately
// be marked as a broken route
func (c *CollectionsRouter) emergencyProcessOneReq(incomingReq interface{}) error {
	for {
		select {
		case <-c.finCh:
			return PartStoppedError
		case req := <-c.retryBufferCh:
			c.retryBufferCh <- incomingReq
			// Flush the retryBufferCh
			err := c.retryForwardFunc(req)
			if err != nil && err != base.ErrorIgnoreRequest {
				c.recordUnroutableRequest(req)
			}
			return nil
		default:
			// Nothing to move
			return nil
		}
	}
}

// This handler is responsible for
func (c *CollectionsRouter) runRetry() {
	defer c.waitGrp.Done()
	manifestRefreshTicker := time.NewTicker(c.retryInterval)

	for {
		select {
		case <-c.finCh:
			manifestRefreshTicker.Stop()
			return
		case <-manifestRefreshTicker.C:
		timerLoop:
			for {
				select {
				case <-c.finCh:
					manifestRefreshTicker.Stop()
					return
				case requestToBeRetried := <-c.retryBufferCh:
					err := c.retryForwardFunc(requestToBeRetried)
					if err != nil {
						err = c.addToTemporaryRetryQueue(requestToBeRetried)
						if err != nil {
							// Just for safety
							c.fatalErrorFunc(err)
						}
					}
				default:
					// Either there is no requests to retry, or need to finalize the
					// requests for next cycle, this function will handle it
					c.moveItemsFromTempRetryQueue()
					break timerLoop
				}
			}
		default:
			// Do nothing
		}
	}
}

// When a request is considered unroutable, then it needs to be marked to be
// bypassed. Until it is marked, the throughseqNo will remain static, so data loss is safe
// The process must be that the broken mapping is noted before it can be marked bypassed
func (c *CollectionsRouter) recordUnroutableRequest(req interface{}) {
	wrappedMcr, ok := req.(*base.WrappedMCRequest)
	if !ok {
		panic("coding bug")
	}

	c.brokenMapMtx.Lock()

	// At this point only implicit routing is supported
	alreadyExists := c.brokenMapping.AddSingleMapping(wrappedMcr.ColNamespace /*src*/, wrappedMcr.ColNamespace /*target*/)
	if !alreadyExists && c.logger.GetLogLevel() > log.LogLevelDebug {
		c.logger.Debugf("Based on target manifest %v brokenmap: %v", wrappedMcr.ColInfo.ManifestId, c.brokenMapping.String())
	}
	var routingInfo CollectionsRoutingInfo
	routingInfo.BrokenMap = c.brokenMapping.Clone()
	routingInfo.TargetManifestId = wrappedMcr.ColInfo.ManifestId
	c.brokenMapMtx.Unlock()
	c.routingUpdater(routingInfo)

	// Once a routing broken event is raised, then allow the data to be ignored
	c.ignoreDataFunc(wrappedMcr)
}

// Key - xmem nozzle ID
type CollectionsRoutingMap map[string]*CollectionsRouter

func (c CollectionsRoutingMap) Init(downStreamParts map[string]common.Part,
	colManifestSvc service_def.CollectionsManifestSvc,
	spec *metadata.ReplicationSpecification,
	logger *log.CommonLogger,
	routingUpdater CollectionsRoutingUpdater,
	ignoreDataFunc IgnoreDataEventer,
	fatalErrorFunc func(error),
	retryForwardFunc connector.CollectionsRerouteFunc) error {

	retryInterval := time.Duration(base.ManifestRefreshTgtInterval) * time.Second
	retryBufferCap := base.UprFeedDataChanLength * spec.Settings.SourceNozzlePerNode / spec.Settings.TargetNozzlePerNode

	for partId, _ := range downStreamParts {
		_, exists := c[partId]
		if !exists {
			// This is equivalent inside to connector.Router's Forward()
			wrappedRetryFunc := func(req interface{}) error {
				return retryForwardFunc(req, partId)
			}
			c[partId] = NewCollectionsRouter(colManifestSvc, spec, logger, wrappedRetryFunc, retryInterval,
				retryBufferCap, routingUpdater, ignoreDataFunc, fatalErrorFunc)
		}
	}

	return nil
}

func (c CollectionsRoutingMap) StartAll() error {
	return c.startOrStopAll(true /*start*/)
}

func (c CollectionsRoutingMap) StopAll() error {
	return c.startOrStopAll(false /*start*/)
}

func (c CollectionsRoutingMap) startOrStopAll(start bool) error {
	var errMap base.ErrorMap

	for partId, collectionsRouter := range c {
		var err error
		if start {
			err = collectionsRouter.Start()
		} else {
			err = collectionsRouter.Stop()
		}
		if err != nil {
			if len(errMap) == 0 {
				errMap = make(base.ErrorMap)
			}
			errMap[partId] = err
		}
	}

	if len(errMap) > 0 {
		return fmt.Errorf(base.FlattenErrorMap(errMap))
	} else {
		return nil
	}
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
	collectionsManifestSvc service_def.CollectionsManifestSvc) (*Router, error) {

	topic := spec.Id
	filterExpression, ok := spec.Settings.Values[metadata.FilterExpressionKey].(string)
	if !ok {
		// No filterExpression
		filterExpression = ""
	}

	var filter *Filter
	var err error
	var filterPrintMsg string = "<nil>"

	filter, err = NewFilter(id, filterExpression, utilsIn)
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
		collectionsRouting:     make(CollectionsRoutingMap),
	}

	router.expDelMode.Set(filterExpDelType)

	startable := true
	// CAPI mode doesn't have any need to start connector
	if spec.Settings.IsCapi() {
		// Note startable can be changed by UpdateSettings() during pipeline startup
		startable = false
	}

	// routingFunc is the main intelligence of the router's functionality
	var routingFunc connector.Routing_Callback_Func = router.Route
	router.Router = connector.NewRouter(id, downStreamParts, &routingFunc, logger_context, "XDCRRouter",
		startable, router.Start, router.Stop, (connector.CollectionsRerouteFunc)(router.RouteCollection))

	routingUpdater := func(info CollectionsRoutingInfo) {
		// TODO: MB-38021 - next step is to handle these events in lock-step
		routingEvent := common.NewEvent(common.RoutingUpdateEvent, info, router, nil, nil)
		router.RaiseEvent(routingEvent)
	}

	ignoreRequestFunc := func(req *base.WrappedMCRequest) {
		ignoreEvent := common.NewEvent(common.DataNotReplicated, req, router, nil, nil)
		router.RaiseEvent(ignoreEvent)
	}

	fatalErrorFunc := func(err error) {
		router.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, router, nil, err))
	}

	router.collectionsRouting.Init(downStreamParts, collectionsManifestSvc, spec, router.Logger(),
		CollectionsRoutingUpdater(routingUpdater), IgnoreDataEventer(ignoreRequestFunc), fatalErrorFunc,
		router.Router.RetryCollectionsForward)

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
	req.Body = event.Value
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

	// TODO - this means implicit routing
	wrapped_req.ColNamespace = wrappedEvent.ColNamespace

	if wrapped_req.ColInfo == nil {
		wrapped_req.ColInfo = &base.TargetCollectionInfo{}
	}

	return wrapped_req, nil
}

// These Start() and Stop() operations are not subjected to wait timeout executors
// So they should not block and the operations underneath should be innocuous if not
// stopped in time
func (router *Router) Start() error {
	router.Logger().Infof("Router %v started", router.Id())
	return router.collectionsRouting.StartAll()
}

func (router *Router) Stop() error {
	router.Logger().Infof("Router %v stopped", router.Id())
	return router.collectionsRouting.StopAll()
}

// Implementation of the routing algorithm
// Currently doing static dispatching based on vbucket number.
func (router *Router) Route(data interface{}) (map[string]interface{}, error) {
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

	// filter data if filter expession has been defined
	if router.filter != nil {
		needToReplicate, err, errDesc, failedDpCnt := router.filter.FilterUprEvent(uprEvent)
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
	}

	mcRequest, err := router.ComposeMCRequest(wrappedUpr)
	if err != nil {
		return nil, router.utils.NewEnhancedError("Error creating new memcached request.", err)
	}
	if mcRequest == nil {
		return nil, fmt.Errorf("Unable to create new mcRequest")
	}

	err = router.RouteCollection(mcRequest, partId)
	if err != nil {
		return result, err
	}

	result[partId] = mcRequest

	return result, nil
}

func (router *Router) RouteCollection(data interface{}, partId string) error {
	if router.collectionsRouting == nil {
		// collections is not being used
		return nil
	}

	mcRequest, ok := data.(*base.WrappedMCRequest)
	if !ok {
		panic(fmt.Sprintf("Coding bug: type: %v", reflect.TypeOf(mcRequest)))
	}

	colId, manifestId, err := router.collectionsRouting[partId].RouteReqToLatestTargetManifest(mcRequest.ColNamespace)
	mcRequest.ColInfo.ManifestId = manifestId
	if err != nil {
		if err == base.ErrorIgnoreRequest {
			if router.Logger().GetLogLevel() >= log.LogLevelDebug {
				router.Logger().Debugf("Request map is already marked broken for %v. Ready to be ignored", string(mcRequest.Req.Key))
			}
			router.collectionsRouting[partId].ignoreDataFunc(mcRequest)
			err = nil
		} else {
			err = router.collectionsRouting[partId].retryRequest(mcRequest)
			if err == nil && router.Logger().GetLogLevel() >= log.LogLevelDebug {
				router.Logger().Debugf("Request %v with namespace %v:%v could not be mapped. Is in retry queue",
					string(mcRequest.Req.Key), mcRequest.ColNamespace.ScopeName, mcRequest.ColNamespace.CollectionName)
			}
		}
		return err
	}

	leb128Cid, err := base.NewUleb128(colId)
	if err != nil {
		err = fmt.Errorf("LEB encoding for collection ID %v error: %v", colId, err)
		return err
	}

	// TODO - MB-37983
	// Embed collection ID in the beginning of the key
	mcRequest.Req.Key = append([]byte(leb128Cid), mcRequest.Req.Key...)
	// Technically, RE-construct
	mcRequest.ConstructUniqueKey()
	return nil
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

	_, ok = settings[ForceCollectionDisableKey]
	if ok {
		router.Router.SetStartable(false)
	}

	if len(errMap) > 0 {
		return fmt.Errorf("Router %v UpdateSettings error(s): %v", router.id, base.FlattenErrorMap(errMap))
	} else {
		return nil
	}
}

func (router *Router) newWrappedMCRequest() (*base.WrappedMCRequest, error) {
	if router.req_creator != nil {
		return router.req_creator(router.topic)
	} else {
		return &base.WrappedMCRequest{Seqno: 0,
			Req: &mc.MCRequest{},
		}, nil
	}
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
