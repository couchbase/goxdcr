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
	filter     *Filter
	routingMap map[uint16]string // pvbno -> partId. This defines the loading balancing strategy of which vbnos would be routed to which part
	topic      string
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

	// Instead of wasting memory, use these datapools
	dcpObjRecycler    utilities.RecycleObjFunc
	targetColInfoPool utilities.TargetCollectionInfoPoolIface
	dataPool          utilities.DataPoolIface
	mcRequestPool     *base.MCRequestPool

	// When DataFiltered events are raised, instead of looking up the latest manifestID
	// just use the last known successful manifestID here instead
	lastSuccessfulManifestId uint64
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
 * (TODO: MB-38023)
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
	started                uint32
	logger                 *log.CommonLogger

	/*
	 * When retry fails, these members record the needed information
	 */
	// Collectively, these are the mappings that this collectionsRouter has seen broken
	brokenMapMtx      sync.RWMutex
	brokenMapping     metadata.CollectionNamespaceMapping
	lastKnownManifest *metadata.CollectionsManifest // should be associated with a brokenMap
	routingUpdater    CollectionsRoutingUpdater
	ignoreDataFunc    IgnoreDataEventer

	fatalErrorFunc func(error)
}

// A collection router is critically important to not only route to the target collection, but
// also coordinate the checkpointing and throughseqno service
//
//  Checkpointing:
//  --------------
//
//	The following takes place in a specific VB.
//	1. Given seqno [0, n) [n+1, y], where n < y, and n is a mutation that is not able to be sent to the target because it couldn’t be mapped. [n+1, y] dictates a range of mutations that have been sent.
//	2. The through sequence number should be n-1.
//	3. Router decides that mutation with seqno n (let’s call it mutation N) is not routable and declares its mapping broken (i.e. c1 -> c1 is broken)
//	4. Router Raises the “Routing Update” via an event with the following info embedded in the event:
//	   a. Broken Mapping (c1 -> c1)
//	   b. Backfill Mapping (nil)
//	   c. Target Manifest ID that the mappings is accurate (i.e. A)
//	   d. Seqno N.
//	5. The recipients of the “Routing Update” will be “Checkpoint Manager” and “Backfill Manager” (new), and the actions below should block (4)
//	   a. Backfill Manager (Not exist yet) will care about the backfill mapping:
//	      i. Once receive the Routing Update, ensure that the backfills that are needed are known. If not know, add these backfills and persist to disk.
//	         1. In this example, backfill mapping is nil, so it’s a no-op.
//	   b. Checkpoint Manager cares about the broken mapping:
//	      i.  Before receiving the Routing Update, any checkpointing done will receive a through sequence number of n-1, and checkpoints will contain no broken map.
//	      ii. After receiving the Routing Update, at this time, any checkpointing will receive a through seqno of n-1, and will contain broken map.
//	         1. When resuming from checkpoint, the broken mapping is loaded back to the Router, so Router knows to invoke future backfills.
//
//	6. Once (5) is finished, it unblocks (4). Router then Raise the “Ignore Event”
//	   a. Ignore event says mutation N is ignored.
//	7. The recipient of the “Ignore Event” is ThroughSeqno Tracker service.
//	   a. Through Seqno will add N to its “ignored list”. Through Sequence number will now move from “n-1” to “y”.
//	   b. Whenever Checkpoint Manager calls “GetThroughSeqno” for this VB from this point on , it will get “y” (or later).
//	      i. Checkpoint Manager *must* have the broken mapping (which it got from (5)) before it should receive ‘y” during checkpointing procedures.
//	         1. This is such that if the broken mapping is no longer broken, a backfill can be created. See resume scenario below.
//
//	Resuming:
//  ---------
//	Continue from the algorithm. Say the checkpoint contains broken mapping (c1 -> c1) and is at sequence number y.
//	Say a new target collection c1 was created, with manifest ID “B”
//
//	When the checkpoint is loaded, the services’ states are restored:
//	1. Checkpoint manager loads the above checkpoint
//	   a. It knows that last broken mapping contains c1 -> c1
//	2. Router’s state is restored.
//	   a. Last known manifest ID is A
//	   b. Last known broken mapping contains (c1 -> c1)
//	3. DCP’s state is restored.
//	   a. Starts streaming data from [y+1, inf)
//	   b. ThroughSeqno service is empty
//	   c. Backfill Manager is empty
//	   d. Collections Manifest Service loads target Manifest A from metakv.
//
//	Now, DCP starts streaming from y+1
//	1. For Router’s perspective, It is possible that y+1 maps to the broken mapping of (c1 -> c1).
//	   a. Router is already aware of it, which means both CkptMgr and BackfillMgr are aware of this too.
//	      i. Router does not raise Routing Event, no-op here.
//	   b. Router raises Ignore event only.
//	      i. ThroughSeqno service will now return y+1 as through sequence number.
//	2. Now, XDCR is made aware of a new target manifest B. Manifest B now knows about target collection c1.
//	   a. This can be done either via the Collections Manifest Service, or the I/O path. In this example, let’s say the I/O path.
//	   b. Router creates n Routing Update Event with the following information:
//	      i.   Broken Mapping (nil)
//	      ii.  Backfill Mapping (c1 -> c1)
//	      iii. Target Manifest Version B
//	      iv.  Mutation seqno y+2
//	3. Backfill Manager receives the event and looks at the backfill mapping.
//	   i.  It sees that it doesn’t have the backfill information, creates and persists it to disk
//	   ii. C1 -> C1 backfill [0, y+2] will start.
//	       a. This will cover y and y+1 mutation that XDCR previously ignored.
//	4. Checkpoint Manager receives the event. Since it only cares about the broken mapping, its state will reflect “no broken maps”.
//	   a. Any future checkpoints will no longer contain broken maps.
//	5. Now, mutation y+2 will be routed successfully.

func NewCollectionsRouter(colManifestSvc service_def.CollectionsManifestSvc,
	spec *metadata.ReplicationSpecification,
	logger *log.CommonLogger,
	routingUpdater CollectionsRoutingUpdater,
	ignoreDataFunc IgnoreDataEventer,
	fatalErrorFunc func(error)) *CollectionsRouter {
	return &CollectionsRouter{
		collectionsManifestSvc: colManifestSvc,
		spec:                   spec,
		logger:                 logger,
		brokenMapping:          make(metadata.CollectionNamespaceMapping),
		routingUpdater:         routingUpdater,
		ignoreDataFunc:         ignoreDataFunc,
		fatalErrorFunc:         fatalErrorFunc,
	}
}

func (c *CollectionsRouter) Start() error {
	atomic.StoreUint32(&c.started, 1)
	return nil
}

func (c *CollectionsRouter) Stop() error {
	atomic.CompareAndSwapUint32(&c.started, 1, 0)
	return nil
}

func (c *CollectionsRouter) IsRunning() bool {
	return atomic.LoadUint32(&c.started) != 0
}

func (c *CollectionsRouter) UpdateBrokenMappingsPair(brokenMappings *metadata.CollectionNamespaceMapping, targetManifestId uint64) {
	if !c.IsRunning() || brokenMappings == nil || len(*brokenMappings) == 0 {
		return
	}

	// Just like how checkpoint manager is the consolidation of all broken map events raised by the router
	// This place is where the consolidation occurs if somehow checkpoint manager has inconsistent mappings
	c.brokenMapMtx.RLock()
	var currentManifestId uint64
	if c.lastKnownManifest != nil {
		currentManifestId = c.lastKnownManifest.Uid()
	}
	currentBrokenMap := c.brokenMapping.Clone()
	c.brokenMapMtx.RUnlock()

	// The key is to always pick the newer manifest, and if they are the same,
	// consolidate the brokenmaps
	needToUpdate := currentManifestId < targetManifestId
	needToUpdate2 := currentManifestId > 0 && currentManifestId == targetManifestId && (len(currentBrokenMap) < len(*brokenMappings) || !currentBrokenMap.IsSame(*brokenMappings))

	if needToUpdate {
		manifest, err := c.collectionsManifestSvc.GetSpecificTargetManifest(c.spec, targetManifestId)
		if err != nil {
			err = fmt.Errorf("Collections Router %v error - unable to find last known target manifest version %v from collectionsManifestSvc - err: %v",
				c.spec.Id, targetManifestId, err)
			// TODO MB-38445
			c.fatalErrorFunc(err)
			return
		}

		c.brokenMapMtx.Lock()
		var checkManifestId uint64
		if c.lastKnownManifest != nil {
			checkManifestId = c.lastKnownManifest.Uid()
		}
		if checkManifestId == currentManifestId &&
			c.brokenMapping.IsSame(currentBrokenMap) {
			// things were same as before
			c.lastKnownManifest = manifest
			c.brokenMapping = *brokenMappings
		} else if checkManifestId == targetManifestId {
			// Target manifest got bumped up between the rlock and lock
			needToUpdate2 = true
		}
		c.brokenMapMtx.Unlock()
	}

	if needToUpdate2 {
		c.brokenMapMtx.Lock()
		var checkManifestId uint64
		if c.lastKnownManifest != nil {
			checkManifestId = c.lastKnownManifest.Uid()
		}
		if checkManifestId == targetManifestId {
			c.brokenMapping.Consolidate(*brokenMappings)
		}
		c.brokenMapMtx.Unlock()
	}
}

// No-Concurrent call
func (c *CollectionsRouter) RouteReqToLatestTargetManifest(namespace *base.CollectionNamespace) (colId uint32, manifestId uint64, err error) {
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
			err = fmt.Errorf("Target manifest version %v does not include scope %v collection %v - mutations with this mapping will need to be backfilled", manifestId, namespace.ScopeName, namespace.CollectionName)
			c.logger.Warnf(err.Error())
		}
	}
	return
}

func (c *CollectionsRouter) handleNewManifestChanges(latestManifest *metadata.CollectionsManifest) (err error) {
	if latestManifest == nil {
		err = base.ErrorInvalidInput
		return
	}

	c.brokenMapMtx.RLock()
	if c.lastKnownManifest == nil {
		c.brokenMapMtx.RUnlock()
		c.brokenMapMtx.Lock()
		if c.lastKnownManifest == nil {
			c.lastKnownManifest = latestManifest
		}
		c.brokenMapMtx.Unlock()
		return
	}

	if latestManifest.Uid() == c.lastKnownManifest.Uid() {
		c.brokenMapMtx.RUnlock()
		return
	}

	lastKnownManifestId := c.lastKnownManifest.Uid()
	lastKnownManifestClone := c.lastKnownManifest.Clone()
	c.brokenMapMtx.RUnlock()

	// Diff should catch this error, but check here just in case
	if lastKnownManifestId > latestManifest.Uid() {
		err = fmt.Errorf("Odd error: New manifest coming in is older than currently known manifest")
		return
	}

	added, _, _, err := latestManifest.Diff(&lastKnownManifestClone)
	if err != nil {
		err = fmt.Errorf("Unable to diff manifests between %v and %v: %v", lastKnownManifestClone.Uid(), latestManifest.Uid(), err)
		return
	}

	var routingInfo CollectionsRoutingInfo
	c.brokenMapMtx.RLock()
	if c.lastKnownManifest.Uid() != lastKnownManifestId {
		// Something changed from underneath, abort
		c.brokenMapMtx.RUnlock()
		return
	} else {
		var fixedMapping metadata.CollectionNamespaceMapping
		brokenMappingClone := c.brokenMapping.Clone()
		c.brokenMapMtx.RUnlock()

		if len(added) > 0 && len(c.brokenMapping) > 0 {
			// These are new target collections - see if the broken ones can be backfilled
			fixedMapping = brokenMappingClone.GetSubsetBasedOnAddedTargets(added)
		}

		if len(fixedMapping) > 0 {
			c.brokenMapMtx.Lock()
			if c.lastKnownManifest.Uid() != lastKnownManifestId || !c.brokenMapping.IsSame(brokenMappingClone) {
				// something changed from underneath, abort
				c.brokenMapMtx.Unlock()
				return
			}
			c.lastKnownManifest = latestManifest
			c.brokenMapping = c.brokenMapping.Delete(fixedMapping)
			routingInfo.BrokenMap = c.brokenMapping.Clone()
			routingInfo.BackfillMap = fixedMapping.Clone()
			routingInfo.TargetManifestId = latestManifest.Uid()
			if c.logger.GetLogLevel() > log.LogLevelDebug {
				c.logger.Debugf("Based on target manifestID %v These mappings need backfilling: %v\n", latestManifest.Uid(), fixedMapping.String())
			}
			c.brokenMapMtx.Unlock()
		} else {
			// No need to raise fixed mapping event but still need to bubble the latest pair of {brokenMap, targetManifestId} to checkpoint manager
			// This is so that checkpoint manager will have the "latest" possible manifest that goes with this brokenMap when checkpointing
			c.brokenMapMtx.Lock()
			if c.lastKnownManifest.Uid() != lastKnownManifestId || !c.brokenMapping.IsSame(brokenMappingClone) {
				// something changed from underneath, abort
				c.brokenMapMtx.Unlock()
				return
			}
			c.lastKnownManifest = latestManifest
			routingInfo.BrokenMap = c.brokenMapping.Clone()
			routingInfo.TargetManifestId = latestManifest.Uid()
			if c.logger.GetLogLevel() > log.LogLevelDebug {
				c.logger.Debugf("Based on target manifestID %v These mappings are broken: %v\n", latestManifest.Uid(), routingInfo.BrokenMap.String())
			}
			c.brokenMapMtx.Unlock()
		}
	}

	c.routingUpdater(routingInfo)
	return
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
	fatalErrorFunc func(error)) error {

	for partId, _ := range downStreamParts {
		_, exists := c[partId]
		if !exists {
			c[partId] = NewCollectionsRouter(colManifestSvc, spec, logger, routingUpdater, ignoreDataFunc, fatalErrorFunc)
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

func (c CollectionsRoutingMap) UpdateBrokenMappingsPair(brokenMaps *metadata.CollectionNamespaceMapping, targetManifestId uint64) {
	for _, collectionsRouter := range c {
		collectionsRouter.UpdateBrokenMappingsPair(brokenMaps, targetManifestId)
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
	logger_context *log.LoggerContext,
	utilsIn utilities.UtilsIface,
	throughputThrottlerSvc service_def.ThroughputThrottlerSvc,
	isHighReplication bool,
	filterExpDelType base.FilterExpDelType,
	collectionsManifestSvc service_def.CollectionsManifestSvc,
	dcpObjRecycler utilities.RecycleObjFunc) (*Router, error) {

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
		utils:                  utilsIn,
		isHighReplication:      base.NewAtomicBooleanType(isHighReplication),
		throughputThrottlerSvc: throughputThrottlerSvc,
		collectionsRouting:     make(CollectionsRoutingMap),
		dcpObjRecycler:         dcpObjRecycler,
		targetColInfoPool:      utilities.NewTargetCollectionInfoPool(),
		dataPool:               utilities.NewDataPool(),
		mcRequestPool:          base.NewMCRequestPool(spec.Id, nil /*logger*/),
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
		startable, router.Start, router.Stop, (connector.CollectionsRerouteFunc)(router.RouteCollection),
		(utilities.RecycleObjFunc)(router.recycleDataObj))

	routingUpdater := func(info CollectionsRoutingInfo) {
		routingEvent := common.NewEvent(common.BrokenRoutingUpdateEvent, info, router, nil, nil)
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
		CollectionsRoutingUpdater(routingUpdater), IgnoreDataEventer(ignoreRequestFunc), fatalErrorFunc)

	router.Logger().Infof("%v created with %d downstream parts isHighReplication=%v and filter=%v\n", router.id, len(downStreamParts), isHighReplication, filterPrintMsg)
	return router, nil
}

// NOTE - The caller of this ComposeMCRequest must separately call ConstructUniqueKey()
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

	// this means implicit routing only for now
	wrapped_req.ColNamespace = wrappedEvent.ColNamespace

	wrapped_req.ColInfo = router.targetColInfoPool.Get()

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

func (router *Router) getDataFilteredAdditional(uprEvent *mcc.UprEvent) interface{} {
	return DataFilteredAdditional{Seqno: uprEvent.Seqno,
		// For filtered document, send the latestSuccessfulManifestId
		// so that the throughSeqno service can get the most updated manifestId
		// for checkpointing if there is an overwhelming number of filtered docs
		ManifestId: atomic.LoadUint64(&router.lastSuccessfulManifestId),
	}
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
		router.RaiseEvent(common.NewEvent(common.DataFiltered, uprEvent, router, nil, router.getDataFilteredAdditional(uprEvent)))
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
				router.RaiseEvent(common.NewEvent(common.DataFiltered, uprEvent, router, nil, router.getDataFilteredAdditional(uprEvent)))
			}
			// Let supervisor set the err instead of the router, to minimize pipeline interruption
			return result, nil
		}
	}

	mcRequest, err := router.ComposeMCRequest(wrappedUpr)
	router.dcpObjRecycler(wrappedUpr)
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

	mcRequest.ConstructUniqueKey()
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

	// Shortcut implicit only mapping
	var colId uint32
	var manifestId uint64
	var err error

	if mcRequest.ColNamespace == nil || mcRequest.ColNamespace.IsDefault() {
		// TODO - implicit mapping default collection specific
		colId = 0
	} else {
		colId, manifestId, err = router.collectionsRouting[partId].RouteReqToLatestTargetManifest(mcRequest.ColNamespace)
		mcRequest.ColInfo.ManifestId = manifestId
		if err != nil {
			if err == base.ErrorIgnoreRequest {
				if router.Logger().GetLogLevel() >= log.LogLevelDebug {
					router.Logger().Debugf("Request map is already marked broken for %v. Ready to be ignored", string(mcRequest.Req.Key))
				}
				router.collectionsRouting[partId].ignoreDataFunc(mcRequest)
				err = base.ErrorRequestAlreadyIgnored
			} else {
				router.collectionsRouting[partId].recordUnroutableRequest(mcRequest)
				err = base.ErrorIgnoreRequest
			}
			return err
		}
	}

	// no need to truncate if we use the leb128CidLen
	leb128Cid, leb128CidLen, err := base.NewUleb128(colId, router.dataPool.GetByteSlice, true /*truncate*/)
	if err != nil {
		err = fmt.Errorf("LEB encoding for collection ID %v error: %v", colId, err)
		router.dataPool.PutByteSlice(leb128Cid)
		return err
	}

	totalLen := len(mcRequest.Req.Key) + leb128CidLen
	mcRequest.ColInfo.ColIDPrefixedKeyLen = totalLen
	mcRequest.ColInfo.ColIDPrefixedKey, err = router.dataPool.GetByteSlice(uint64(totalLen))
	if err != nil {
		mcRequest.ColInfo.ColIDPrefixedKey = make([]byte, totalLen, totalLen)
		err = nil
	} else {
		mcRequest.ColInfo.ColIDPrefixedKey = mcRequest.ColInfo.ColIDPrefixedKey[:0]
	}

	// Embed collection ID in the beginning of the key
	copy(mcRequest.ColInfo.ColIDPrefixedKey[0:leb128CidLen], leb128Cid[0:leb128CidLen])
	copy(mcRequest.ColInfo.ColIDPrefixedKey[leb128CidLen:totalLen], mcRequest.Req.Key)
	mcRequest.Req.Key = mcRequest.ColInfo.ColIDPrefixedKey[0:totalLen]
	mcRequest.Req.Keylen = totalLen

	atomic.StoreUint64(&router.lastSuccessfulManifestId, manifestId)
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

	// When checkpoint manager starts up, it loads the last known target manifest ID
	// and sends it as part of the UpdateVBTimestamp() call.
	// This section will set the router's lastKnownManifestId to such value
	// so that if immediately a filtered event is raised, the right manifest ID
	// can be raised alongisde it.
	// It is highly possible that this value can be transient... that is, when another successful
	// mutation has gone through the router, that the lastSuccessfulManifestId will jump to the
	// latest known target manifest ID. That is ok, though, because the corner case mentioned above
	// is covered
	ts_obj := router.utils.GetSettingFromSettings(settings, DCP_VBTimestamp)
	if ts_obj != nil {
		new_ts, ok := settings[DCP_VBTimestamp].(map[uint16]*base.VBTimestamp)
		if ok && new_ts != nil && router.Router.IsStartable() {
			// Find the max targetManifestId
			var maxManifestId uint64
			for _, ts := range new_ts {
				if ts.ManifestIDs.TargetManifestId > maxManifestId {
					maxManifestId = ts.ManifestIDs.TargetManifestId
				}
			}
			atomic.StoreUint64(&router.lastSuccessfulManifestId, maxManifestId)
		}
	}

	pair, ok := settings[metadata.BrokenMappingsPair].([]interface{})
	if ok && len(pair) == 2 {
		brokenMappings, ok2 := pair[0].(*metadata.CollectionNamespaceMapping)
		targetManifestId, ok := pair[1].(uint64)
		if ok && ok2 && router.Router.IsStartable() {
			router.collectionsRouting.UpdateBrokenMappingsPair(brokenMappings, targetManifestId)
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
	newReq.ColInfo = nil
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
	case *base.CollectionNamespace:
		if router.dcpObjRecycler != nil {
			router.dcpObjRecycler(obj)
		}
	case *base.WrappedMCRequest:
		if router.mcRequestPool != nil {
			req := obj.(*base.WrappedMCRequest)
			if req.ColNamespace != nil {
				req.ColNamespace.ScopeName = ""
				req.ColNamespace.CollectionName = ""
				router.dcpObjRecycler(req.ColNamespace)
			}

			if req.ColInfo != nil && router.targetColInfoPool != nil {
				if req.ColInfo.ColIDPrefixedKeyLen > 0 && router.dataPool != nil {
					req.ColInfo.ColIDPrefixedKeyLen = 0
					router.dataPool.PutByteSlice(req.ColInfo.ColIDPrefixedKey)
				}
				req.ColInfo.ColIDPrefixedKeyLen = 0
				req.ColInfo.ManifestId = 0
				router.targetColInfoPool.Put(req.ColInfo)
			}
			router.mcRequestPool.Put(obj.(*base.WrappedMCRequest))
		}
	default:
		panic(fmt.Sprintf("Coding bug type is %v", reflect.TypeOf(obj)))
	}
}
