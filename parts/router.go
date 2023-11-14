// Copyright 2018-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package parts

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"

	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	baseFilter "github.com/couchbase/goxdcr/base/filter"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/connector"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
)

var ErrorInvalidDataForRouter = errors.New("Input data to Router is invalid.")
var ErrorNoDownStreamNodesForRouter = errors.New("No downstream nodes have been defined for the Router.")
var ErrorNoRoutingMapForRouter = errors.New("No routingMap has been defined for Router.")
var ErrorInvalidRoutingMapForRouter = errors.New("routingMap in Router is invalid.")
var ErrorPipelineRestartDueToExplicitMapChange = errors.New("explicit mapping has changed and pipeline needs to restart to reflect the mapping change")
var ErrorTargetNsSubsetUnmapped = errors.New("a subset of the target namespaces are unmapped and need to be specially handled")
var ErrorTargetDegraded = errors.New("target connectivity shows degraded")

var IsHighReplicationKey = "IsHighReplication"
var NeedToThrottleKey = "NeedToThrottle"
var FilterExpDelKey = base.FilterExpDelKey
var ForceCollectionDisableKey = "ForceCollectionDisable"
var BackfillPersistErrKey = "Backfill Persist Callback Error"
var CkptMgrBrokenMappingErrKey = "Checkpoint Manager routing update Callback Error"
var MobileCompatible = base.MobileCompatibleKey

// A function used by the router to raise routing updates to other services that need to know
type CollectionsRoutingUpdater func(CollectionsRoutingInfo) error

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
	// Chose one of the following (like C union)
	BrokenMap           metadata.CollectionNamespaceMapping
	BackfillMap         metadata.CollectionNamespaceMapping
	ExplicitBackfillMap metadata.CollectionNamespaceMappingsDiffPair
	// Only used (and required) along with BrokenMap
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

type CollectionsMgtAtomicType uint32

func (c *CollectionsMgtAtomicType) Set(value base.CollectionsMgtType) {
	atomic.StoreUint32((*uint32)(c), uint32(value))
}

func (c *CollectionsMgtAtomicType) Get() base.CollectionsMgtType {
	return base.CollectionsMgtType(atomic.LoadUint32((*uint32)(c)))
}

type RouterIface interface {
	Start() error
	Stop() error
	Route(data interface{}) (map[string]interface{}, error)
	RouteCollection(data interface{}, partId string, origUprEvent *base.WrappedUprEvent) error
}

// XDCR Router does two things:
// 1. converts UprEvent(DCP) to MCRequest (MemCached)
// 2. routes MCRequest to downstream parts
type Router struct {
	id string
	*connector.Router
	filter     baseFilter.Filter
	routingMap map[uint16]string // pvbno -> partId. This defines the loading balancing strategy of which vbnos would be routed to which part
	topic      string
	// whether lww conflict resolution mode has been enabled
	sourceCRMode    base.ConflictResolutionMode
	utils           utilities.UtilsIface
	expDelMode      FilterExpDelAtomicType
	collectionModes CollectionsMgtAtomicType
	started         uint32
	stopped         uint32
	finCh           chan bool

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
	dataPool          base.DataPool
	mcRequestPool     *base.MCRequestPool

	// When DataFiltered events are raised, instead of looking up the latest manifestID
	// just use the last known successful manifestID here instead
	lastSuccessfulManifestId uint64

	explicitMapChangeHandler func(diff metadata.CollectionNamespaceMappingsDiffPair)

	remoteClusterCapability metadata.Capability

	migrationUIRaiser func(string)

	connectivityStatusGetter func() (metadata.ConnectivityStatus, error)
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
 * If RemoteClusterAgent pulled the laggy node last, it may not have the latest manifest and declares
 * the mapping broken
 *
 * 2. Broken Mapping
 * When a CollectionsRouter determines that a mapping is no longer valid
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
 * 3. Backfills Needed
 * Backfills need to be created when a mapped target collection is created. There are two ways to detect
 * it - 1. data path, and 2. static path (when no data is flowing)
 * CollectionsRouter is considered the data path detection, and will raise backfill if it is needed
 * The static path will be taken care of by Collections Manifest Service
 */
type CollectionsRouter struct {
	parentRouterId         string
	collectionsManifestSvc service_def.CollectionsManifestSvc
	started                uint32
	finCh                  chan bool
	logger                 *log.CommonLogger
	spec                   *metadata.ReplicationSpecification // Read-Only at startup
	targetManifestRefresh  time.Duration

	/*
	 * When retry fails, these members record the needed information
	 */
	// Collectively, these are the mappings that this collectionsRouter has seen broken
	brokenDenyMtx         sync.RWMutex
	brokenMapping         metadata.CollectionNamespaceMapping
	brokenMappingIdx      metadata.CollectionNamespaceMappingIdx
	brokenMapDblChkTicker *time.Ticker
	brokenMapDblChkKicker *time.Timer
	denyMapping           metadata.CollectionNamespaceMapping
	denyMappingIdx        metadata.CollectionNamespaceMappingIdx
	lastKnownTgtManifest  *metadata.CollectionsManifest // should be associated with a brokenMap

	routingUpdater CollectionsRoutingUpdater
	ignoreDataFunc IgnoreDataEventer

	fatalErrorFunc func(error, interface{})

	// Collections Mapping related
	mappingMtx           sync.RWMutex
	explicitMappings     metadata.CollectionNamespaceMapping // nil for implicit mapping
	explicitMappingIdx   metadata.CollectionNamespaceMappingIdx
	collectionMode       base.CollectionsMgtType
	cachedRules          metadata.CollectionsMappingRulesType
	lastKnownSrcManifest *metadata.CollectionsManifest

	explicitMapChangeHandler func(diff metadata.CollectionNamespaceMappingsDiffPair)
	migrationUIRaiser        func(message string)
	connectivityStatusGetter func() (metadata.ConnectivityStatus, error)

	waitingForFirstMutation uint32
	startIdleKicker         *time.Timer
	startIdleKickerMtx      sync.RWMutex
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

func NewCollectionsRouter(id string, colManifestSvc service_def.CollectionsManifestSvc, spec *metadata.ReplicationSpecification, logger *log.CommonLogger, routingUpdater CollectionsRoutingUpdater, ignoreDataFunc IgnoreDataEventer, fatalErrorFunc func(error, interface{}), explicitMapChangeHandler func(diff metadata.CollectionNamespaceMappingsDiffPair), migrationUIRaiser func(string), connectivityStatusGetter func() (metadata.ConnectivityStatus, error)) *CollectionsRouter {
	return &CollectionsRouter{
		parentRouterId:           id,
		collectionsManifestSvc:   colManifestSvc,
		spec:                     spec,
		logger:                   logger,
		brokenMapping:            make(metadata.CollectionNamespaceMapping),
		denyMapping:              make(metadata.CollectionNamespaceMapping),
		routingUpdater:           routingUpdater,
		ignoreDataFunc:           ignoreDataFunc,
		fatalErrorFunc:           fatalErrorFunc,
		explicitMapChangeHandler: explicitMapChangeHandler,
		migrationUIRaiser:        migrationUIRaiser,
		targetManifestRefresh:    time.Duration(base.ManifestRefreshTgtInterval) * time.Second,
		connectivityStatusGetter: connectivityStatusGetter,
		finCh:                    make(chan bool),
	}
}

func (c *CollectionsRouter) Start() error {
	atomic.StoreUint32(&c.started, 1)
	c.mappingMtx.RLock()
	modes := c.spec.Settings.GetCollectionModes()
	curSrcMan, curTgtMan, err := c.collectionsManifestSvc.GetLatestManifests(c.spec, false)
	if err != nil {
		c.mappingMtx.RUnlock()
		return err
	}
	rules := c.spec.Settings.GetCollectionsRoutingRules().Clone()
	c.mappingMtx.RUnlock()

	if modes.IsExplicitMapping() {
		c.logger.Infof("CollectionsRouter %v started in explicit mapping mode with rules %v", c.parentRouterId, rules)
	} else if modes.IsMigrationOn() {
		c.logger.Infof("CollectionsRouter %v started in migration mode with rules %v", c.parentRouterId, rules)
	} else {
		c.logger.Infof("CollectionsRouter %v started in implicit mapping mode", c.parentRouterId)
		c.initializeInternalsForImplicitMapping(modes)
		return nil
	}

	pair := metadata.CollectionsManifestPair{
		Source: curSrcMan,
		Target: curTgtMan,
	}
	err = c.initializeInternalsForExplicitOrMigration(pair, modes, rules)
	return err
}

func (c *CollectionsRouter) initializeInternalsForImplicitMapping(modes base.CollectionsMgtType) {
	c.mappingMtx.Lock()
	defer c.mappingMtx.Unlock()
	c.collectionMode = modes
}

func (c *CollectionsRouter) initializeInternalsForExplicitOrMigration(pair metadata.CollectionsManifestPair, modes base.CollectionsMgtType, rules metadata.CollectionsMappingRulesType) error {
	var err error
	c.mappingMtx.Lock()
	c.lastKnownSrcManifest = pair.Source

	checkAndHandleSpecialMigration(&rules, &modes)

	c.explicitMappings, err = metadata.NewCollectionNamespaceMappingFromRules(pair, modes, rules, false, false)
	if err != nil {
		return err
	}
	c.explicitMappingIdx = c.explicitMappings.CreateLookupIndex()
	c.collectionMode = modes
	c.cachedRules = rules
	c.mappingMtx.Unlock()
	return err
}

func (c *CollectionsRouter) IsFilteredSystemCollection(namespace *base.CollectionNamespace) bool {
	if namespace.ScopeName != base.SystemScopeName {
		return false
	}
	if c.spec.Settings.GetFilterSystemScope() == false {
		// Nothing is filtered when filterSystemScope is false
		return false
	}
	for _, col := range base.FilterSystemScopePassthruCollections {
		if namespace.CollectionName == col {
			return false
		}
	}
	return true
}

func (c *CollectionsRouter) Stop() error {
	if atomic.CompareAndSwapUint32(&c.started, 1, 0) {
		c.logger.Infof("CollectionsRouter %v is now stopping", c.parentRouterId)
		close(c.finCh)
	}
	return nil
}

func (c *CollectionsRouter) IsRunning() bool {
	return atomic.LoadUint32(&c.started) != 0
}

// When router is started, this method loads the broken map pair
// The brokenMappingsRO passed in is shared between multiple collectionsRouter and should be handled carefully as such
func (c *CollectionsRouter) UpdateBrokenMappingsPair(brokenMappingsRO *metadata.CollectionNamespaceMapping, targetManifestId uint64, isRollBack bool) {
	if !c.IsRunning() || brokenMappingsRO == nil || len(*brokenMappingsRO) == 0 {
		return
	}

	// Just like how checkpoint manager is the consolidation of all broken map events raised by the router
	// This place is where the consolidation occurs if somehow checkpoint manager has inconsistent mappings
	c.brokenDenyMtx.RLock()
	var currentManifestId uint64
	if c.lastKnownTgtManifest != nil {
		currentManifestId = c.lastKnownTgtManifest.Uid()
	}
	currentBrokenMap := c.brokenMapping.Clone()
	c.brokenDenyMtx.RUnlock()

	// The key is to always pick the newer manifest, and if they are the same,
	// consolidate the brokenmaps
	// If a manifest rolls backwards, it means that a rollback happened and ckptMgr is telling this router about
	// the rollback checkpoint's stored target manifestID and also the older broken map
	// For rollback case, since router's brokenmap spans multiple VBs, cast the same broken map capture net
	// Also, it is almost guaranteed that targetManifestId <= currentManifestId for rollback, but to prevent any
	// odd situations, use isRollBack boolean to enforce this explicitly
	needToUpdate := currentManifestId < targetManifestId && !isRollBack
	needToUpdate2 := currentManifestId > 0 && currentManifestId == targetManifestId && (len(currentBrokenMap) < len(*brokenMappingsRO) || !currentBrokenMap.IsSame(*brokenMappingsRO))

	if needToUpdate {
		// This section will set the router's internal broken map and last known target manifest to the values
		// being sent over
		manifest, err := c.collectionsManifestSvc.GetSpecificTargetManifest(c.spec, targetManifestId)
		if err != nil {
			// Since the targetManifestId is given by Checkpoint manager, this means collection manifest service
			// should have kept a copy of the manifest around
			err = fmt.Errorf("Collections Router %v error - unable to find last known target manifest version %v from collectionsManifestSvc - err: %v",
				c.spec.Id, targetManifestId, err)
			// TODO MB-38445
			c.fatalErrorFunc(err, nil)
			return
		}

		c.brokenDenyMtx.Lock()
		var checkManifestId uint64
		if c.lastKnownTgtManifest != nil {
			checkManifestId = c.lastKnownTgtManifest.Uid()
		}
		if checkManifestId == currentManifestId &&
			c.brokenMapping.IsSame(currentBrokenMap) {
			// things were same as before
			c.lastKnownTgtManifest = manifest
			c.brokenMapping = brokenMappingsRO.Clone()
			c.brokenMappingIdx = c.brokenMapping.CreateLookupIndex()
		} else if checkManifestId == targetManifestId {
			// Target manifest got bumped up between the rlock and lock
			needToUpdate2 = true
		}
		c.brokenDenyMtx.Unlock()
	}

	if needToUpdate2 {
		c.brokenDenyMtx.Lock()
		var checkManifestId uint64
		if c.lastKnownTgtManifest != nil {
			checkManifestId = c.lastKnownTgtManifest.Uid()
		}
		if checkManifestId == targetManifestId {
			// Only consolidate if the router's view of the target is agreed
			c.brokenMapping.Consolidate(*brokenMappingsRO)
		}
		c.brokenDenyMtx.Unlock()
	}

	atomic.StoreUint32(&c.waitingForFirstMutation, 1)
	c.startIdleKickerMtx.Lock()
	c.startIdleKicker = time.AfterFunc(10*time.Second, func() {
		latestTargetManifest, err := c.collectionsManifestSvc.GetSpecificTargetManifest(c.spec, math.MaxUint64)
		if err == nil {
			c.handleNewTgtManifestChanges(latestTargetManifest)
		}
	})
	c.startIdleKickerMtx.Unlock()
}

// No-Concurrent call
func (c *CollectionsRouter) RouteReqToLatestTargetManifest(wrappedMCReq *base.WrappedMCRequest, eventForMigration *base.WrappedUprEvent) (colIds []uint32, manifestId uint64, backfillPersistHadErr bool, unmappedNamespaces metadata.CollectionNamespaceMapping, migrationColIdNsMap map[uint32]*base.CollectionNamespace, err error) {
	if !c.IsRunning() {
		err = PartStoppedError
		return
	}

	c.checkAndDisableFirstMutationKicker()

	c.mappingMtx.RLock()
	isImplicitMapping := c.collectionMode.IsImplicitMapping()
	isMigrationMode := c.collectionMode.IsMigrationOn()
	c.mappingMtx.RUnlock()

	namespace := wrappedMCReq.GetSourceCollectionNamespace()

	if isImplicitMapping && c.IsFilteredSystemCollection(namespace) {
		err = base.ErrorIgnoreRequest
		return
	}
	var latestSourceManifest *metadata.CollectionsManifest
	var latestTargetManifest *metadata.CollectionsManifest

	if isImplicitMapping {
		latestTargetManifest, err = c.collectionsManifestSvc.GetSpecificTargetManifest(c.spec, math.MaxUint64)
	} else {
		if !isMigrationMode {
			// Check to see if this source namespace has already been marked denied, which means
			// it wasn't even in the rules to be matched
			c.brokenDenyMtx.RLock()
			_, _, _, exists := c.denyMapping.Get(namespace, c.denyMappingIdx)
			c.brokenDenyMtx.RUnlock()
			if exists {
				err = base.ErrorIgnoreRequest
				return
			}
		} else {
			// Migration mode means all data is coming from default source collection, thus no denyMapping
		}
		latestSourceManifest, latestTargetManifest, err = c.collectionsManifestSvc.GetLatestManifests(c.spec, false)
	}
	if err != nil {
		return
	}
	if !isImplicitMapping && latestSourceManifest == nil {
		err = fmt.Errorf("received nil latest source manifest")
		return
	}
	if latestTargetManifest == nil {
		err = fmt.Errorf("received nil latest target manifest")
		return
	}

	if !isImplicitMapping {
		err = c.handleExplicitMappingUpdate(latestSourceManifest, latestTargetManifest)
		if err != nil {
			return
		}
	}

	err = c.handleNewTgtManifestChanges(latestTargetManifest)
	backfillPersistHadErr = err != nil && strings.Contains(err.Error(), BackfillPersistErrKey)

	if err != nil && !backfillPersistHadErr {
		return
	}

	if isImplicitMapping {
		var colId uint32
		manifestId, colId, err = c.implicitMap(namespace, latestTargetManifest)
		colIds = append(colIds, colId)
	} else {
		manifestId, colIds, unmappedNamespaces, migrationColIdNsMap, err = c.explicitMap(wrappedMCReq, latestTargetManifest, eventForMigration)
	}
	return
}

func (c *CollectionsRouter) handleExplicitMappingUpdate(latestSourceManifest, latestTargetManifest *metadata.CollectionsManifest) error {
	if latestSourceManifest == nil || latestTargetManifest == nil {
		return fmt.Errorf("Nil source or target manifest given")
	}

	c.mappingMtx.RLock()
	isExplicitMapping := c.explicitMappings != nil
	var lastKnownUid uint64
	if c.lastKnownSrcManifest != nil {
		lastKnownUid = c.lastKnownSrcManifest.Uid()
	}
	var lastKnownTgtUid uint64
	if c.lastKnownTgtManifest != nil {
		lastKnownTgtUid = c.lastKnownTgtManifest.Uid()
	}
	c.mappingMtx.RUnlock()

	if !isExplicitMapping || latestSourceManifest.Uid() <= lastKnownUid && latestTargetManifest.Uid() <= lastKnownTgtUid {
		return nil
	}

	c.mappingMtx.RLock()
	mappingMode := c.collectionMode
	rules := c.cachedRules.Clone()
	cachedExplicitMap := c.explicitMappings.Clone()
	lastKnownSrcManifestId := c.lastKnownSrcManifest.Uid()
	c.mappingMtx.RUnlock()
	manifestsPair := metadata.CollectionsManifestPair{
		Source: latestSourceManifest,
		Target: latestTargetManifest,
	}

	explicitMap, err := metadata.NewCollectionNamespaceMappingFromRules(manifestsPair, mappingMode, rules, false, false)
	if err != nil {
		c.logger.Errorf("Unable to create explicit mapping from rules given manifests %v and %v", manifestsPair.Source.Uid(), manifestsPair.Target.Uid())
		return err
	}

	if explicitMap.IsSame(cachedExplicitMap) {
		// Possible that new scopes/collections are created that were not part of the
		// defined explicit rules, and thus no updates needed
		return nil
	}

	// When a new explicit map shows up because the source or target manifest change caused it to be valid,
	// ongoing mutations will start flowing but need to backfill previously missed
	// Restarting the pipeline to ensure a clean cut over using the explicitMapChangeHandler, which will make pipelinemgr:
	// 1. Stop the pipeline
	// 2. Get the latest seqnos from ckpts
	// 3. Create backfill request based on the latest seqnos
	// 4. Start pipelines
	added, removed := c.explicitMappings.Diff(explicitMap)
	pair := metadata.CollectionNamespaceMappingsDiffPair{
		Added:                      added,
		Removed:                    removed,
		CorrespondingSrcManifestId: lastKnownSrcManifestId,
	}
	c.explicitMapChangeHandler(pair)

	// It's possible pipeline may take some time to get to the stop/start part
	// so in the meantime, update the mapping and write to the right target collection for the ongoing stream
	c.mappingMtx.Lock()
	c.explicitMappings = explicitMap
	c.explicitMappingIdx = c.explicitMappings.CreateLookupIndex()
	c.mappingMtx.Unlock()
	return nil
}

func (c *CollectionsRouter) implicitMap(namespace *base.CollectionNamespace, latestTargetManifest *metadata.CollectionsManifest) (uint64, uint32, error) {
	manifestId := latestTargetManifest.Uid()
	colId, err := latestTargetManifest.GetCollectionId(namespace.ScopeName, namespace.CollectionName)
	if err == base.ErrorNotFound {

		c.brokenDenyMtx.RLock()
		_, _, _, exists := c.brokenMapping.Get(namespace, c.brokenMappingIdx)
		c.brokenDenyMtx.RUnlock()

		if exists {
			err = base.ErrorIgnoreRequest
		} else {
			err = fmt.Errorf("Target manifest version %v does not include scope %v collection %v - mutations with this mapping will need to be backfilled", manifestId, namespace.ScopeName, namespace.CollectionName)
			c.logger.Warnf(err.Error())
		}
	}
	return manifestId, colId, err
}

func (c *CollectionsRouter) explicitMap(wrappedMCReq *base.WrappedMCRequest, latestTargetManifest *metadata.CollectionsManifest, eventForMigration *base.WrappedUprEvent) (manifestId uint64, colIds []uint32, unmappedNamespaces metadata.CollectionNamespaceMapping, migrationColIdNsMap map[uint32]*base.CollectionNamespace, err error) {
	srcNamespace := wrappedMCReq.GetSourceCollectionNamespace()
	manifestId = latestTargetManifest.Uid()
	var matchedNamespaces metadata.CollectionNamespaceMapping

	var errMap base.ErrorMap
	if !c.collectionMode.IsMigrationOn() {
		var tgtCollections metadata.CollectionNamespaceList
		tgtCollections, err = c.regularExplicitMap(srcNamespace)
		if err != nil {
			return 0, []uint32{0}, unmappedNamespaces, migrationColIdNsMap, err
		} else if len(tgtCollections) == 0 {
			return 0, []uint32{0}, unmappedNamespaces, migrationColIdNsMap, base.ErrorIgnoreRequest
		}
		matchedNamespaces = make(metadata.CollectionNamespaceMapping)
		matchedNamespaces[metadata.NewSourceCollectionNamespace(srcNamespace)] = tgtCollections
	} else {
		var errMCReqMap map[string]*base.WrappedMCRequest
		matchedNamespaces, errMap, errMCReqMap = c.migrationExplicitMap(eventForMigration, wrappedMCReq)
		if errMap != nil {
			var invalidInput bool
			for _, v := range errMap {
				if v == base.ErrorInvalidInput {
					invalidInput = true
				}
			}
			if len(errMap) == 1 && invalidInput {
				return 0, []uint32{0}, unmappedNamespaces, migrationColIdNsMap, base.ErrorInvalidInput
			}
			// At this point, means at least 1 match failure
			var unableToFilterCnt int
			for key, docErr := range errMap {
				if docErr == base.ErrorInvalidInput {
					continue
				}
				unableToFilterCnt++
				c.ignoreDataFunc(errMCReqMap[key])
			}
			wrappedMCReq.SiblingReqsMtx.RLock()
			totalInstances := len(wrappedMCReq.SiblingReqs) + 1
			wrappedMCReq.SiblingReqsMtx.RUnlock()
			if totalInstances == unableToFilterCnt {
				// The original req + the siblings were all not able be replicated
				err = base.ErrorIgnoreRequest
				return
			}
		}
	}

	var allEmpty = true
	for _, tgtColList := range matchedNamespaces {
		if len(tgtColList) > 1 {
			allEmpty = false
			break
		} else if len(tgtColList) == 1 && !tgtColList[0].IsEmpty() {
			allEmpty = false
			break
		}
	}

	if allEmpty {
		// The source mutation is on the deny list
		err = base.ErrorIgnoreRequest
	}

	if c.collectionMode.IsMigrationOn() {
		migrationColIdNsMap = make(map[uint32]*base.CollectionNamespace)
	}
	var colId uint32
	for sourceNs, tgtCollections := range matchedNamespaces {
		for _, targetNamespace := range tgtCollections {
			colId, err = latestTargetManifest.GetCollectionId(targetNamespace.ScopeName, targetNamespace.CollectionName)
			if err == base.ErrorNotFound {
				if unmappedNamespaces == nil {
					unmappedNamespaces = make(metadata.CollectionNamespaceMapping)
				}
				unmappedNamespaces.AddSingleSourceNsMapping(sourceNs, targetNamespace)
			} else {
				colIds = append(colIds, colId)
				if c.collectionMode.IsMigrationOn() {
					migrationColIdNsMap[colId] = targetNamespace
				}
			}
		}
	}

	if c.collectionMode.IsMigrationOn() && len(matchedNamespaces) > 1 && c.migrationUIRaiser != nil &&
		wrappedMCReq.Req.Opcode != mc.UPR_DELETION && wrappedMCReq.Req.Opcode != mc.UPR_EXPIRATION {
		// Only raise UI message if it is not tombstone. Tombstones will pass all filters.
		msg := fmt.Sprintf("This collection migration pipeline instance for spec %v has experienced a situation where "+
			"a single source mutation with document ID %q is replicated to multiple target collections because it matches multiple "+
			" migration mapping rules. Please check the migration logic if this was not intended.", c.spec.Id, wrappedMCReq.Req.Key)
		c.migrationUIRaiser(msg)
	}

	if len(unmappedNamespaces) == 0 {
		// happy path - all targets mapped
		return
	}

	// Everything matched is unable to be mapped to the target manifest
	if unmappedNamespaces.IsSame(matchedNamespaces) {
		errMsg := fmt.Sprintf("target manifest version %v does not include %v - mutations with this mapping may need to be backfilled", manifestId, unmappedNamespaces.String())
		// All are unmapped - need to raise error
		err = fmt.Errorf(errMsg)
		if c.collectionMode.IsMigrationOn() {
			// migration routing means that the target mapping is empty. Pick one unmapped and get it rdy for brokenMap
			for _, tgtNamespaces := range unmappedNamespaces {
				if len(tgtNamespaces) == 0 {
					continue
				}
				wrappedMCReq.ColInfoMtx.Lock()
				wrappedMCReq.ColInfo.TargetNamespace = tgtNamespaces[0]
				wrappedMCReq.ColInfoMtx.Unlock()
				break
			}
		}
		return
	}

	// A subset of mapping was not mapped, return this err to allow whatever could be replicated to be replicated
	err = ErrorTargetNsSubsetUnmapped
	return
}

func (c *CollectionsRouter) regularExplicitMap(namespace *base.CollectionNamespace) (metadata.CollectionNamespaceList, error) {
	c.mappingMtx.RLock()
	_, _, tgtCollections, exists := c.explicitMappings.Get(namespace, c.explicitMappingIdx)
	c.mappingMtx.RUnlock()

	if !exists || len(tgtCollections) == 0 {
		c.brokenDenyMtx.RLock()
		_, _, _, exists := c.brokenMapping.Get(namespace, c.brokenMappingIdx)
		c.brokenDenyMtx.RUnlock()

		var err error
		if exists {
			err = base.ErrorIgnoreRequest
		} else {
			// Returns non-nil error if namespace is never meant to be replicated
			_, checkErr := c.cachedRules.GetPotentialTargetNamespaces(namespace)
			if checkErr == nil {
				err = fmt.Errorf("current explicit mapping does not include target destination given source scope %v collection %v - this mapping may need to be backfilled", namespace.ScopeName, namespace.CollectionName)
				c.logger.Warnf(err.Error())
			}
		}
		return nil, err
	}
	return tgtCollections, nil
}

func (c *CollectionsRouter) handleNewTgtManifestChanges(latestManifest *metadata.CollectionsManifest) (err error) {
	if latestManifest == nil {
		err = base.ErrorInvalidInput
		return
	}

	c.brokenDenyMtx.RLock()
	if c.lastKnownTgtManifest == nil {
		c.brokenDenyMtx.RUnlock()
		c.brokenDenyMtx.Lock()
		if c.lastKnownTgtManifest == nil {
			c.lastKnownTgtManifest = latestManifest
		}
		c.brokenDenyMtx.Unlock()
		return
	}

	var needToDblChkBrokenMap bool
	if c.brokenMapDblChkTicker != nil {
		select {
		case <-c.brokenMapDblChkTicker.C:
			needToDblChkBrokenMap = true
		default:
			break
		}
	}

	manifestIsNotChanging := latestManifest.Uid() == c.lastKnownTgtManifest.Uid()
	if manifestIsNotChanging && !needToDblChkBrokenMap {
		// No target manifest changes, and we're not waiting to double check XMEM race condition
		c.brokenDenyMtx.RUnlock()
		return
	}

	lastKnownManifestId := c.lastKnownTgtManifest.Uid()
	lastKnownManifestClone := c.lastKnownTgtManifest.Clone()
	c.brokenDenyMtx.RUnlock()

	// Diff should catch this error, but check here just in case
	if lastKnownManifestId > latestManifest.Uid() {
		err = fmt.Errorf("Odd error: New manifest coming in is older than currently known manifest")
		return
	}

	var added metadata.ScopesMap
	var removed metadata.ScopesMap
	if manifestIsNotChanging {
		added = make(metadata.ScopesMap)
		// The only case we hit here if manifest is not changing is if we need to double check broken map
		dblCheckErr := c.doubleCheckBrokenMapAndClearDblChkTicker(latestManifest, added)
		if dblCheckErr != nil {
			c.logger.Warnf("doubleCheckBrokenMapAndClearDblChkTicker returned err %v", dblCheckErr)
		}
	} else {
		added, _, removed, err = latestManifest.Diff(&lastKnownManifestClone)
		if err != nil {
			err = fmt.Errorf("Unable to diff manifests between %v and %v: %v", lastKnownManifestClone.Uid(), latestManifest.Uid(), err)
			return
		}
		if needToDblChkBrokenMap {
			if added == nil {
				added = make(metadata.ScopesMap)
			}
			dblCheckErr := c.doubleCheckBrokenMapAndClearDblChkTicker(latestManifest, added)
			if dblCheckErr != nil {
				c.logger.Warnf("doubleCheckBrokenMapAndClearDblChkTicker2 returned err %v", dblCheckErr)
			}
		}
	}

	err = c.fixBrokenMapAndRaiseBackfill(latestManifest, lastKnownManifestId, removed, added)
	return
}

func (c *CollectionsRouter) fixBrokenMapAndRaiseBackfill(latestManifest *metadata.CollectionsManifest, lastKnownManifestId uint64, removed metadata.ScopesMap, added metadata.ScopesMap) error {
	if !c.IsRunning() {
		return PartStoppedError
	}

	var routingInfo CollectionsRoutingInfo
	var rollbackFunc func()
	var abort bool
	c.brokenDenyMtx.RLock()
	if c.lastKnownTgtManifest.Uid() != lastKnownManifestId {
		// Something changed from underneath, abort
		c.brokenDenyMtx.RUnlock()
		return nil
	} else {
		var fixedMapping metadata.CollectionNamespaceMapping
		brokenMappingClone := c.brokenMapping.Clone()
		c.brokenDenyMtx.RUnlock()

		// If any source manifests are removed, then remove them from brokenmap
		atLeastOneFound := checkIfRemovedSrcNamespacesExistInCurrentBrokenMap(removed, brokenMappingClone)
		if atLeastOneFound {
			brokenMappingClone = c.cleanBrokenMapWithRemovedEntries(brokenMappingClone, removed)
		}

		if len(added) > 0 && len(brokenMappingClone) > 0 {
			// These are new target collections - see if the broken ones can be backfilled
			fixedMapping = brokenMappingClone.GetSubsetBasedOnSpecifiedTargets(added)
			brokenMappingClone = brokenMappingClone.Delete(fixedMapping)
		}

		if len(fixedMapping) > 0 {
			abort, rollbackFunc = c.updateBrokenMapAndRoutingInfo(latestManifest, lastKnownManifestId, brokenMappingClone, fixedMapping, &routingInfo)
			if abort {
				return nil
			}
		} else {
			// No need to raise fixed mapping event but still need to bubble the latest pair of {brokenMap, targetManifestId} to checkpoint manager
			// This is so that checkpoint manager will have the "latest" possible manifest that goes with this brokenMap when checkpointing
			abort = c.updateJustRoutingInfo(latestManifest, lastKnownManifestId, brokenMappingClone, routingInfo)
			if abort {
				return nil
			}
		}
	}

	err := c.routingUpdater(routingInfo)
	if err != nil && c.IsRunning() {
		// If routing updater had error, it means that the potentially either or both ckpt mgr and backfill mgr
		// do not have the most up-to-date brokenmap/repairedmap (routingInfo).
		// Most likely though, it means that backfill was not able to be persisted correctly.
		// Because we've introduced the concept of
		// partial replication (where a source maps to >1 targets, but only can replicate to a subset of them)
		// the broken map in this collection router needs to be rolled back so that the next time a routingInfo pair
		// can be re-raised again to both ckpt mgr and backfill mgr to ensure that any missed subset of targets can
		// be backfilled accordingly.
		// This is necessary because backfill manager only works off of the manifests changes to find out what needs to
		// be backfilled... from the manifest changes only, it cannot tell which subsets of target collections needs to
		// be backfilled. This information *only* exists from the router's perspective
		if rollbackFunc != nil {
			rollbackFunc()
		}
	}
	return err
}

// Given the latest manifest, double check the broken map and for those target collections that should not be broken,
// add them to the added scopesmap, which pretends that a manifest took place and these are added target namespaces
// Internally, also synchronously remove the double-check ticker so if another batch occurs, the next ticker won't be missed
// Do not modify added in case of any connectivity error
func (c *CollectionsRouter) doubleCheckBrokenMapAndClearDblChkTicker(latestManifest *metadata.CollectionsManifest, added metadata.ScopesMap) error {
	// If manifest is not changing, then the backfill should be raised regardless of target health
	remoteClusterHealth, err := c.connectivityStatusGetter()
	if err != nil {
		return err
	}

	c.brokenDenyMtx.Lock()
	if c.brokenMapDblChkTicker != nil {
		c.brokenMapDblChkTicker.Stop()
		c.brokenMapDblChkTicker = nil
		c.brokenMapDblChkKicker.Stop()
		c.brokenMapDblChkKicker = nil
		if remoteClusterHealth == metadata.ConnDegraded {
			c.startDoubleCheckTickerAndKicker()
			c.brokenDenyMtx.Unlock()
			c.logger.Warnf("%v - %v", ErrorTargetDegraded.Error(), "unable to double check broken map - will need to retry double check")
			return ErrorTargetDegraded
		}
	}
	brokenMapClone := c.brokenMapping.Clone()
	c.brokenDenyMtx.Unlock()
	for _, tgtList := range brokenMapClone {
		for _, tgt := range tgtList {
			_, foundErr := latestManifest.GetCollectionId(tgt.ScopeName, tgt.CollectionName)
			if foundErr == nil {
				// This target should exist
				added.AddNamespace(tgt.ScopeName, tgt.CollectionName)
			}
		}
	}
	return nil
}

func (c *CollectionsRouter) cleanBrokenMapWithRemovedEntries(brokenMappingClone metadata.CollectionNamespaceMapping, removed metadata.ScopesMap) metadata.CollectionNamespaceMapping {
	nameSpacesToBeDeleted := getNamespacesToBeDeletedFromBrokenMap(brokenMappingClone, removed)
	c.brokenDenyMtx.Lock()
	c.brokenMapping = c.brokenMapping.Delete(nameSpacesToBeDeleted)
	c.brokenMappingIdx = c.brokenMapping.CreateLookupIndex()
	brokenMappingClone = c.brokenMapping.Clone()
	c.brokenDenyMtx.Unlock()
	return brokenMappingClone
}

func (c *CollectionsRouter) updateJustRoutingInfo(latestManifest *metadata.CollectionsManifest, lastKnownManifestId uint64, brokenMappingClone metadata.CollectionNamespaceMapping, routingInfo CollectionsRoutingInfo) bool {
	c.brokenDenyMtx.Lock()
	if c.lastKnownTgtManifest.Uid() != lastKnownManifestId || !c.brokenMapping.IsSame(brokenMappingClone) {
		// something changed from underneath, abort
		c.brokenDenyMtx.Unlock()
		return true
	}
	c.lastKnownTgtManifest = latestManifest
	routingInfo.BrokenMap = c.brokenMapping.Clone()
	routingInfo.TargetManifestId = latestManifest.Uid()
	if c.logger.GetLogLevel() > log.LogLevelDebug {
		c.logger.Debugf("Based on target manifestID %v These mappings are broken: %v\n", latestManifest.Uid(), routingInfo.BrokenMap.String())
	}
	c.brokenDenyMtx.Unlock()
	return false
}

func (c *CollectionsRouter) updateBrokenMapAndRoutingInfo(latestManifest *metadata.CollectionsManifest, lastKnownManifestId uint64, brokenMappingClone metadata.CollectionNamespaceMapping, fixedMapping metadata.CollectionNamespaceMapping, routingInfo *CollectionsRoutingInfo) (bool, func()) {
	c.brokenDenyMtx.Lock()
	if c.lastKnownTgtManifest.Uid() != lastKnownManifestId {
		// something changed from underneath, abort
		c.brokenDenyMtx.Unlock()
		return true, nil
	}

	var dblCheckBrokenMap bool
	if latestManifest.Uid() == lastKnownManifestId {
		// When we are updating broken map information with fixed mapping information yet the manifests did not change
		// then this is a case where we're trying to double check broken map due to potential race condition by letting
		// it be fixed so things can be sent again
		dblCheckBrokenMap = true
	}

	// Rollback func is needed for in case for downstream errors
	rbPrevKnownTgtManifest := c.lastKnownTgtManifest.Clone()
	rbPrevBrokenMapping := c.brokenMapping.Clone()
	rollbackCallback := func() {
		c.brokenDenyMtx.Lock()
		c.brokenMapping = rbPrevBrokenMapping
		c.brokenMappingIdx = c.brokenMapping.CreateLookupIndex()
		c.lastKnownTgtManifest = &rbPrevKnownTgtManifest
		if dblCheckBrokenMap {
			// If double check broken map, then we need to re-initialize a ticker if it's not there to cover the case
			// where manifests do not change but the brokenMap has racey entries that need to be double checked
			if c.brokenMapDblChkTicker == nil {
				c.startDoubleCheckTickerAndKicker()
			}
		}
		c.brokenDenyMtx.Unlock()
		c.logger.Warnf("Due to error, %v has rolled back to prev brokenMap %v and prev target manifest %v", c.spec.Id, rbPrevBrokenMapping.String(), rbPrevKnownTgtManifest.Uid())
	}

	if !dblCheckBrokenMap {
		// Not double checking broken map means that this path is called due to manifest ID bump
		// If that's the case, then no longer need to do double check
		if c.brokenMapDblChkTicker != nil {
			c.brokenMapDblChkTicker.Stop()
			c.brokenMapDblChkKicker.Stop()
			c.brokenMapDblChkTicker = nil
			c.brokenMapDblChkKicker = nil
		}
	}
	c.lastKnownTgtManifest = latestManifest
	c.brokenMapping = brokenMappingClone.Clone()
	c.brokenMappingIdx = c.brokenMapping.CreateLookupIndex()
	routingInfo.BrokenMap = brokenMappingClone
	routingInfo.BackfillMap = fixedMapping.Clone()
	routingInfo.TargetManifestId = latestManifest.Uid()
	if c.logger.GetLogLevel() > log.LogLevelDebug {
		c.logger.Debugf("Based on target manifestID %v These mappings need backfilling: %v\n", latestManifest.Uid(), fixedMapping.String())
	}

	c.brokenDenyMtx.Unlock()
	return false, rollbackCallback
}

// Lock needs to be held
func (c *CollectionsRouter) startDoubleCheckTickerAndKicker() {
	c.brokenMapDblChkTicker = time.NewTicker(c.targetManifestRefresh)
	c.brokenMapDblChkKicker = time.AfterFunc(5*c.targetManifestRefresh, c.brokenMapIdleDoubleCheck)
}

func getNamespacesToBeDeletedFromBrokenMap(brokenMappingClone metadata.CollectionNamespaceMapping, removed metadata.ScopesMap) metadata.CollectionNamespaceMapping {
	nameSpacesToBeDeleted := make(metadata.CollectionNamespaceMapping)
	for srcNs, tgtNs := range brokenMappingClone {
		_, found := removed.GetCollectionByNames(srcNs.ScopeName, srcNs.CollectionName)
		if found {
			nameSpacesToBeDeleted.AddSingleMapping(srcNs.CollectionNamespace, tgtNs[0])
		}
	}
	return nameSpacesToBeDeleted
}

func checkIfRemovedSrcNamespacesExistInCurrentBrokenMap(removed metadata.ScopesMap, brokenMappingClone metadata.CollectionNamespaceMapping) (atLeastOneFound bool) {
	for scopeName, scopeMap := range removed {
		for collectionName, _ := range scopeMap.Collections {
			_, _, _, exists := brokenMappingClone.Get(&base.CollectionNamespace{
				ScopeName:      scopeName,
				CollectionName: collectionName,
			}, nil)
			if exists {
				atLeastOneFound = true
				return
			}
		}
	}
	return
}

// When a request is considered unroutable, then it needs to be marked to be
// bypassed. Until it is marked, the throughseqNo will remain static, so data loss is safe
// The process must be that the broken mapping is noted before it can be marked bypassed
// Note: Unroutable request means that the req are collection related
func (c *CollectionsRouter) recordUnroutableRequest(req interface{}) {
	wrappedMcr, ok := req.(*base.WrappedMCRequest)
	if !ok {
		panic("coding bug")
	}
	if !c.IsRunning() {
		return
	}

	c.mappingMtx.RLock()
	// When raising an ignore event, prevent the namespace from being recycled
	sourceNamespace := &base.CollectionNamespace{}
	*sourceNamespace = *(wrappedMcr.GetSourceCollectionNamespace())
	var targetNamespaces []*base.CollectionNamespace
	targetNamespaces = append(targetNamespaces, sourceNamespace) // for implicit mapping
	cachedRules := c.cachedRules.Clone()
	modes := c.collectionMode
	c.mappingMtx.RUnlock()
	var err error
	if !modes.IsImplicitMapping() {
		targetNamespaces = targetNamespaces[:0]
		if modes.IsMigrationOn() {
			wrappedMcr.ColInfoMtx.RLock()
			if wrappedMcr.ColInfo == nil || wrappedMcr.ColInfo.TargetNamespace == nil {
				// This happens if explicitMap() was not called due unable to find a collection manifest agent
				// to do the mapping, which can only happen during a replication removal situation
				wrappedMcr.ColInfoMtx.RUnlock()
				return
			}
			// Make a copy to prevent namespace from being recycled
			targetNsCopy := &base.CollectionNamespace{}
			*targetNsCopy = *(wrappedMcr.ColInfo.TargetNamespace)
			wrappedMcr.ColInfoMtx.RUnlock()
			targetNamespaces = append(targetNamespaces, targetNsCopy)
		} else {
			targetNamespaces, err = cachedRules.GetPotentialTargetNamespaces(sourceNamespace)
			if err == nil && len(targetNamespaces) == 0 {
				// Source namespace exists in the rule but is meant to be denied from replication
				// Don't allow replication but it's not a broken-map type either
				c.ignoreDataFunc(wrappedMcr)
				return
			} else if err != nil {
				emptyTargetNamespace := &base.CollectionNamespace{}
				// This source namespace was not enlisted in the rule or compiled map
				// Add it to the router's deny list, because the source ns was never explicitly
				// marked to be replicated
				c.brokenDenyMtx.Lock()
				c.denyMapping.AddSingleMapping(sourceNamespace, emptyTargetNamespace)
				c.denyMappingIdx = c.denyMapping.CreateLookupIndex()
				c.brokenDenyMtx.Unlock()
				// When the collection rule changes occur (via set replication)
				// the pipeline will be restarted, which means a router starts off with a clean denyMapping and
				// a fresh routingRules
				c.ignoreDataFunc(wrappedMcr)
				return
			}
		}
	}

	// At this point, need to compile a unmappedNamespace
	unmappedNamespaces := make(metadata.CollectionNamespaceMapping)
	sourceTypeSourceNamespace := metadata.NewSourceCollectionNamespace(sourceNamespace)
	for _, targetNamespace := range targetNamespaces {
		unmappedNamespaces.AddSingleSourceNsMapping(sourceTypeSourceNamespace, targetNamespace)
	}

	c.updateBrokenMappingAndRaiseNecessaryEvents(wrappedMcr, unmappedNamespaces, true)
}

func (c *CollectionsRouter) brokenMapIdleDoubleCheck() {
	if !c.IsRunning() {
		return
	}

	c.brokenDenyMtx.RLock()
	latestManifestClone := c.lastKnownTgtManifest.Clone()
	c.brokenDenyMtx.RUnlock()

	added := make(metadata.ScopesMap)
	err := c.doubleCheckBrokenMapAndClearDblChkTicker(&latestManifestClone, added)
	if err != nil {
		return
	}

	if len(added) > 0 {
		c.fixBrokenMapAndRaiseBackfill(&latestManifestClone, latestManifestClone.Uid(), nil, added)
	}
}

// When "shouldIgnore" is true, the wrappedMCR is recycled
func (c *CollectionsRouter) updateBrokenMappingAndRaiseNecessaryEvents(wrappedMcr *base.WrappedMCRequest, unmappedNamespaces metadata.CollectionNamespaceMapping, shouldIgnore bool) {
	var routingInfo CollectionsRoutingInfo
	if !c.IsRunning() {
		return
	}

	wrappedMcr.ColInfoMtx.RLock()
	colId := wrappedMcr.ColInfo.ColId
	manifestId := wrappedMcr.ColInfo.ManifestId
	wrappedMcr.ColInfoMtx.RUnlock()

	c.brokenDenyMtx.Lock()
	_, _, findErr := c.lastKnownTgtManifest.GetScopeAndCollectionName(colId)
	if findErr == nil {
		// If this function hits here given the wrappedMCR and it is actually found using lastKnownTgtManifest
		// then it means that this is being called by downstream XMEM reporting back saying that the target KV does
		// not know about the collection... because XMEM doesn't do manifest checks, it just raises the error and comes
		// here blindly
		// We will note a time now and wait for one of the following to happen, whichever occurs first:
		// 1. Time expires when a target manifest refresh should have taken place
		// or
		// 2. The target manifest actually gets updated
		// Then we'll revisit the brokenmap and see if every entry in it are actually still valid
		if c.brokenMapDblChkTicker == nil {
			c.brokenMapDblChkTicker = time.NewTicker(c.targetManifestRefresh)
			c.brokenMapDblChkKicker = time.AfterFunc(5*c.targetManifestRefresh, c.brokenMapIdleDoubleCheck)
		}
	}
	var raiseBrokenEvent bool
	var allAlreadyExist = true
	for sourceNs, targetNamespaces := range unmappedNamespaces {
		for _, targetNamespace := range targetNamespaces {
			alreadyExists := c.brokenMapping.AddSingleMapping(sourceNs.CollectionNamespace, targetNamespace)
			if !alreadyExists {
				allAlreadyExist = false
			}
		}
		if !allAlreadyExist && c.logger.GetLogLevel() > log.LogLevelDebug {
			c.logger.Debugf("Based on target manifest %v brokenmap: %v", manifestId, c.brokenMapping.String())
		}
	}
	if !allAlreadyExist {
		c.brokenMappingIdx = c.brokenMapping.CreateLookupIndex()
		raiseBrokenEvent = true
	}
	routingInfo.BrokenMap = c.brokenMapping.Clone()
	routingInfo.TargetManifestId = manifestId
	c.brokenDenyMtx.Unlock()

	if raiseBrokenEvent {
		// This essentially tells checkpoint manager of the latest broken map so that when it can be restored when resuming pipeline
		err := c.routingUpdater(routingInfo)
		if err != nil {
			c.logger.Errorf("%v - %v", BackfillPersistErrKey, err)
			return
		}
	}

	if shouldIgnore {
		// Once a routing broken event is successfully raised, then allow the data to be ignored
		c.ignoreDataFunc(wrappedMcr)
	}
}

func (c *CollectionsRouter) migrationExplicitMap(wrappedUprEvent *base.WrappedUprEvent, mcReq *base.WrappedMCRequest) (metadata.CollectionNamespaceMapping, base.ErrorMap, map[string]*base.WrappedMCRequest) {
	if wrappedUprEvent == nil || wrappedUprEvent.UprEvent == nil {
		errMap := make(base.ErrorMap)
		errMap["migrationExplicitMap"] = base.ErrorInvalidInput
		return nil, errMap, nil
	}
	// It is possible that this needs to record unroutable request using mcReq only
	c.mappingMtx.RLock()
	defer c.mappingMtx.RUnlock()
	return c.explicitMappings.GetTargetUsingMigrationFilter(wrappedUprEvent, mcReq, c.logger)
}

func (c *CollectionsRouter) checkAndDisableFirstMutationKicker() {
	if atomic.LoadUint32(&c.waitingForFirstMutation) == 1 && atomic.CompareAndSwapUint32(&c.waitingForFirstMutation, 1, 0) {
		c.startIdleKickerMtx.RLock()
		c.startIdleKicker.Stop()
		c.startIdleKickerMtx.RUnlock()
	}
}

func checkAndHandleSpecialMigration(incomingRules *metadata.CollectionsMappingRulesType, modes *base.CollectionsMgtType) {
	if modes.IsMigrationOn() && incomingRules.IsExplicitMigrationRule() {
		// Internally convert migration mode to explicit mapping mode so that the explicit replication will take place
		modes.SetMigration(false)
		modes.SetExplicitMapping(true)
	}
}

// Key - xmem nozzle ID
type CollectionsRoutingMap map[string]*CollectionsRouter

func (c CollectionsRoutingMap) Init(id string, downStreamParts map[string]common.Part, colManifestSvc service_def.CollectionsManifestSvc, spec *metadata.ReplicationSpecification, logger *log.CommonLogger, routingUpdater CollectionsRoutingUpdater, ignoreDataFunc IgnoreDataEventer, fatalErrorFunc func(error, interface{}), explicitMapChangeHandler func(diff metadata.CollectionNamespaceMappingsDiffPair), migrationUIRaiser func(string), connectivityStatusGetter func() (metadata.ConnectivityStatus, error)) error {
	for partId, outNozzlePart := range downStreamParts {
		collectionRouter, exists := c[partId]
		if !exists {
			collectionRouter = NewCollectionsRouter(id, colManifestSvc, spec, logger, routingUpdater, ignoreDataFunc, fatalErrorFunc, explicitMapChangeHandler, migrationUIRaiser, connectivityStatusGetter)
			c[partId] = collectionRouter
		}
		outNozzle, ok := outNozzlePart.(common.OutNozzle)
		if !ok {
			return base.ErrorInvalidInput
		} else {
			outNozzle.SetUpstreamErrReporter(collectionRouter.recordUnroutableRequest)
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

func (c CollectionsRoutingMap) UpdateBrokenMappingsPair(brokenMapsRO *metadata.CollectionNamespaceMapping, targetManifestId uint64, isRollBack bool) {
	for _, collectionsRouter := range c {
		collectionsRouter.UpdateBrokenMappingsPair(brokenMapsRO, targetManifestId, isRollBack)
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
func NewRouter(id string, spec *metadata.ReplicationSpecification, downStreamParts map[string]common.Part, routingMap map[uint16]string, sourceCRMode base.ConflictResolutionMode, logger_context *log.LoggerContext, utilsIn utilities.UtilsIface, throughputThrottlerSvc service_def.ThroughputThrottlerSvc, isHighReplication bool, filterExpDelType base.FilterExpDelType, collectionsManifestSvc service_def.CollectionsManifestSvc, dcpObjRecycler utilities.RecycleObjFunc, explicitMapChangeHandler func(diff metadata.CollectionNamespaceMappingsDiffPair), remoteClusterCapability metadata.Capability, migrationUIRaiser func(string), connectivityStatusGetter func() (metadata.ConnectivityStatus, error)) (*Router, error) {

	topic := spec.Id
	filterExpression, exprFound := spec.Settings.Values[metadata.FilterExpressionKey].(string)
	_, versionFound := spec.Settings.Values[metadata.FilterVersionKey]
	var filterVersion base.FilterVersionType
	if versionFound {
		filterVersion = spec.Settings.Values[metadata.FilterVersionKey].(base.FilterVersionType)
	}

	// These conditions may require in-memory of filter upgrade
	if exprFound && filterExpression != "" {
		if !versionFound || (versionFound && filterVersion == base.FilterVersionKeyOnly) {
			clonedSetting := spec.Settings.Clone()
			var dummyList []string
			clonedSetting.UpgradeFilterIfNeeded(dummyList)
			filterExpression = clonedSetting.FilterExpression
		}
	}

	var filter *baseFilter.FilterImpl
	var err error
	var filterPrintMsg string = "<nil>"

	expDelMode := spec.Settings.GetExpDelMode()
	mobileCompatible := spec.Settings.GetMobileCompatible()
	filter, err = baseFilter.NewFilter(id, filterExpression, utilsIn, expDelMode, mobileCompatible)
	if err != nil {
		return nil, err
	}
	if len(filterExpression) > 0 {
		filterPrintMsg = fmt.Sprintf("%v%v%v", base.UdTagBegin, filter.GetInternalExpr(), base.UdTagEnd)
	}

	router := &Router{
		id:                       id,
		filter:                   filter,
		routingMap:               routingMap,
		topic:                    topic,
		sourceCRMode:             sourceCRMode,
		utils:                    utilsIn,
		isHighReplication:        base.NewAtomicBooleanType(isHighReplication),
		throughputThrottlerSvc:   throughputThrottlerSvc,
		collectionsRouting:       make(CollectionsRoutingMap),
		dcpObjRecycler:           dcpObjRecycler,
		targetColInfoPool:        utilities.NewTargetCollectionInfoPool(),
		dataPool:                 base.NewDataPool(),
		mcRequestPool:            base.NewMCRequestPool(spec.Id, nil /*logger*/),
		explicitMapChangeHandler: explicitMapChangeHandler,
		remoteClusterCapability:  remoteClusterCapability,
		migrationUIRaiser:        migrationUIRaiser,
		connectivityStatusGetter: connectivityStatusGetter,
		finCh:                    make(chan bool),
	}

	router.expDelMode.Set(filterExpDelType)

	startable := true
	// CAPI mode doesn't have any need to start connector
	// If target doesn't support collections, then also do not start
	if spec.Settings.IsCapi() || !router.remoteClusterCapability.HasCollectionSupport() {
		// Note startable can be changed by UpdateSettings() during pipeline startup
		startable = false
	}

	// routingFunc is the main intelligence of the router's functionality
	var routingFunc connector.Routing_Callback_Func = router.Route
	router.Router = connector.NewRouter(id, downStreamParts, &routingFunc, logger_context, "XDCRRouter",
		startable, router.Start, router.Stop, (connector.CollectionsRerouteFunc)(router.RouteCollection),
		(utilities.RecycleObjFunc)(router.recycleDataObj))

	routingUpdater := func(info CollectionsRoutingInfo) error {
		// 1. It should raise a backfill event telling backfill manager to persist the backfill mapping
		// 2. Backfill manager should synchronously persist the backfill.
		//    If it cannot persist, it must not allow the following to continue
		// 3. Once Backfill Manager has persisted it, then the broken event can be raised to the checkpoint manager
		if len(info.BackfillMap) > 0 ||
			len(info.ExplicitBackfillMap.Added) > 0 || len(info.ExplicitBackfillMap.Removed) > 0 {
			syncCh := make(chan error)
			var channels []interface{}
			channels = append(channels, syncCh)
			channels = append(channels, router.finCh)
			backfillEvent := common.NewEvent(common.FixedRoutingUpdateEvent, info, router, channels, nil)
			go router.RaiseEvent(backfillEvent)
			err := <-syncCh
			if err != nil {
				wrappedErr := fmt.Errorf("%v - %v", BackfillPersistErrKey, err.Error())
				router.Logger().Errorf("Unable to persist fixed routing update event: %v", wrappedErr)
				return wrappedErr
			}
		}
		syncCh := make(chan error)
		routingEvent := common.NewEvent(common.BrokenRoutingUpdateEvent, info, router, nil, syncCh)
		go router.RaiseEvent(routingEvent)
		err := <-syncCh
		if err != nil {
			wrappedErr := fmt.Errorf("%v - %v", CkptMgrBrokenMappingErrKey, err.Error())
			return wrappedErr
		}
		return nil
	}

	ignoreRequestFunc := func(req *base.WrappedMCRequest) {
		ignoreEvent := common.NewEvent(common.DataNotReplicated, req, router, nil, utilities.RecycleObjFunc(router.recycleDataObj))
		router.RaiseEvent(ignoreEvent)
	}

	fatalErrorFunc := func(err error, data interface{}) {
		router.RaiseEvent(common.NewEvent(common.ErrorEncountered, data, router, nil, err))
	}

	if router.remoteClusterCapability.HasCollectionSupport() {
		router.collectionsRouting.Init(router.id, downStreamParts, collectionsManifestSvc, spec, router.Logger(), routingUpdater, ignoreRequestFunc, fatalErrorFunc, router.explicitMapChangeHandler, migrationUIRaiser, connectivityStatusGetter)

		modes := spec.Settings.GetCollectionModes()
		router.collectionModes = CollectionsMgtAtomicType(modes)
		router.Logger().Infof("%v created with %d downstream parts isHighReplication=%v, filter=%v, collectionMode=%v\n", router.id, len(downStreamParts), isHighReplication, filterPrintMsg, modes.String())
	} else {
		router.Logger().Infof("%v created with %d downstream parts isHighReplication=%v, filter=%v\n", router.id, len(downStreamParts), isHighReplication, filterPrintMsg)
	}

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
		if router.sourceCRMode == base.CRMode_LWW || router.sourceCRMode == base.CRMode_Custom || event.Opcode == mc.UPR_EXPIRATION {
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
		// base.SKIP_CONFLICT_RESOLUTION_FLAG will be set in Xmem when we actually decide to do source CR only
		if router.sourceCRMode == base.CRMode_LWW || router.sourceCRMode == base.CRMode_Custom {
			// if source bucket is of lww or custom type, add FORCE_ACCEPT_WITH_META_OPS options for memcached
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
	wrapped_req.SrcColNamespaceMtx.Lock()
	wrapped_req.SrcColNamespace = wrappedEvent.ColNamespace
	wrapped_req.SrcColNamespaceMtx.Unlock()

	wrapped_req.ColInfoMtx.Lock()
	wrapped_req.ColInfo = router.targetColInfoPool.Get()
	wrapped_req.ColInfoMtx.Unlock()

	return wrapped_req, nil
}

// These Start() and Stop() operations are not subjected to wait timeout executors
// So they should not block and the operations underneath should be innocuous if not
// stopped in time
func (router *Router) Start() error {
	if atomic.CompareAndSwapUint32(&router.started, 0, 1) {
		if len(router.collectionsRouting) > 0 {
			defer router.Logger().Infof("Router %v started", router.Id())
			router.Logger().Infof("Router %v starting...", router.Id())
			return router.collectionsRouting.StartAll()
		} else {
			return nil
		}
	} else {
		return PartAlreadyStartedError
	}
}

func (router *Router) Stop() error {
	if atomic.CompareAndSwapUint32(&router.started, 1, 0) {
		atomic.StoreUint32(&router.stopped, 1)
		close(router.finCh)
		if len(router.collectionsRouting) > 0 {
			defer router.Logger().Infof("Router %v stopped", router.Id())
			router.Logger().Infof("Router %v stopping...", router.Id())
			return router.collectionsRouting.StopAll()
		} else {
			return nil
		}
	}
	if atomic.LoadUint32(&router.stopped) == 1 {
		return PartAlreadyStoppedError
	} else {
		// Possible to have Stop() called before Start() is called
		return nil
	}
}

func (router *Router) getDataFilteredAdditional(uprEvent *mcc.UprEvent, filteringStatus base.FilteringStatusType) interface{} {
	return DataFilteredAdditional{Seqno: uprEvent.Seqno,
		// For filtered document, send the latestSuccessfulManifestId
		// so that the throughSeqno service can get the most updated manifestId
		// for checkpointing if there is an overwhelming number of filtered docs
		ManifestId:      atomic.LoadUint64(&router.lastSuccessfulManifestId),
		FilteringStatus: filteringStatus,
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

	// Deletion/Expiration filter doesn't apply to system scope
	if wrappedUpr.ColNamespace == nil || wrappedUpr.ColNamespace.ScopeName != base.SystemScopeName {
		shouldContinue := router.ProcessExpDelTTL(uprEvent)
		if !shouldContinue {
			router.RaiseEvent(common.NewEvent(common.DataFiltered, uprEvent, router, nil, router.getDataFilteredAdditional(uprEvent, baseFilter.FilteredOnOthers)))
			return result, nil
		}
	}

	needToReplicate, err, errDesc, failedDpCnt, filteringStatus := router.filter.FilterUprEvent(wrappedUpr)
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
			router.RaiseEvent(common.NewEvent(common.DataFiltered, uprEvent, router, nil, router.getDataFilteredAdditional(uprEvent, filteringStatus)))
		}
		// Let supervisor set the err instead of the router, to minimize pipeline interruption
		return result, nil
	}

	var raiseDataNotReplicated bool
	if wrappedUpr.Flags.CollectionDNE() {
		// Special flag handling here
		raiseDataNotReplicated = true
	}

	mcRequest, err := router.ComposeMCRequest(wrappedUpr)
	if err != nil {
		return nil, router.utils.NewEnhancedError("Error creating new memcached request.", err)
	}
	if mcRequest == nil {
		return nil, fmt.Errorf("Unable to create new mcRequest")
	}

	if raiseDataNotReplicated {
		err := fmt.Errorf("collection ID %v no longer exists, so the data is not to be replicated", wrappedUpr.UprEvent.CollectionId)
		router.RaiseEvent(common.NewEvent(common.DataNotReplicated, mcRequest, router, []interface{}{err}, nil))
		return result, nil
	}

	defer router.dcpObjRecycler(wrappedUpr)

	err = router.RouteCollection(mcRequest, partId, wrappedUpr)
	if err != nil {
		return result, err
	}

	mcRequest.ConstructUniqueKey()
	result[partId] = mcRequest

	return result, nil
}

func (router *Router) RouteCollection(data interface{}, partId string, origUprEvent *base.WrappedUprEvent) error {
	if !router.remoteClusterCapability.HasCollectionSupport() {
		// collections is not being used
		return nil
	}

	mcRequest, ok := data.(*base.WrappedMCRequest)
	if !ok {
		panic(fmt.Sprintf("Coding bug: type: %v", reflect.TypeOf(mcRequest)))
	}

	// Shortcut implicit only mapping
	var colIds []uint32
	var manifestId uint64
	var err error
	var unmappedNamespaces metadata.CollectionNamespaceMapping
	var migrationColIdNamespaceMap map[uint32]*base.CollectionNamespace

	collectionMode := router.collectionModes.Get()
	if collectionMode.IsImplicitMapping() &&
		(mcRequest.GetSourceCollectionNamespace() == nil || mcRequest.GetSourceCollectionNamespace().IsDefault()) {
		colIds = append(colIds, 0)
	} else {
		var backfillPersistHadErr bool
		colIds, manifestId, backfillPersistHadErr, unmappedNamespaces, migrationColIdNamespaceMap, err = router.collectionsRouting[partId].RouteReqToLatestTargetManifest(mcRequest, origUprEvent)
		// If backfillPersistHadErr is set, then it is:
		// 1. Safe to re-raise routingUpdate, as each routingUpdate will try to persist again
		// 2. NOT safe to ignore data - as ignoring data means throughSeqno will move forward
		mcRequest.ColInfoMtx.Lock()
		mcRequest.ColInfo.ManifestId = manifestId
		mcRequest.ColInfoMtx.Unlock()
		if err != nil {
			if err == base.ErrorIgnoreRequest && !backfillPersistHadErr {
				if router.Logger().GetLogLevel() >= log.LogLevelDebug {
					router.Logger().Debugf("Request map is already marked broken for %v. Ready to be ignored", string(mcRequest.Req.Key))
				}
				router.collectionsRouting[partId].ignoreDataFunc(mcRequest)
				err = base.ErrorRequestAlreadyIgnored
			} else if err == ErrorTargetNsSubsetUnmapped && len(unmappedNamespaces) > 0 {
				// This means we should still prepare to send the request to the colIds that have been populated
				// But we need the upper layer to do some special handling
				router.collectionsRouting[partId].updateBrokenMappingAndRaiseNecessaryEvents(mcRequest, unmappedNamespaces, false)
				err = nil
			} else if err == PartStoppedError {
				// don't recordUnroutable
				router.recycleDataObj(mcRequest)
			} else {
				// Record unroutable request will try to re-raise the synchronous routing update
				// Only if it succeeds, will the ignoreDataFunc be called
				router.collectionsRouting[partId].recordUnroutableRequest(mcRequest)
				err = base.ErrorIgnoreRequest
			}
			if err != nil {
				return err
			}
		}
	}

	err = router.prepareMcRequest(colIds, mcRequest, origUprEvent, migrationColIdNamespaceMap)
	if err != nil {
		return err
	}

	atomic.StoreUint64(&router.lastSuccessfulManifestId, manifestId)
	return nil
}

// Sets up the unique Key
// If a request is meant to replicate to >1 target collections, prepare the sibling requests as well
// and notify throughSeqnoTracker svc that there are sibling requests coming for this given source seqno
func (router *Router) prepareMcRequest(colIds []uint32, firstReq *base.WrappedMCRequest, origUprEvent *base.WrappedUprEvent, migrationNamespaceMap map[uint32]*base.CollectionNamespace) error {
	var err error
	var syncCh chan bool = nil
	numCols := len(colIds)
	if numCols > 1 {
		syncCh = make(chan bool)
	}
	for i, colId := range colIds {
		var reqToProcess *base.WrappedMCRequest
		if i == 0 {
			reqToProcess = firstReq
		} else {
			reqToProcess, err = router.ComposeMCRequest(origUprEvent)
			if err != nil {
				return err
			}
			firstReq.SiblingReqsMtx.Lock()
			firstReq.SiblingReqs = append(firstReq.SiblingReqs, reqToProcess)
			firstReq.SiblingReqsMtx.Unlock()
			reqToProcess.Cloned = true
			reqToProcess.ClonedSyncCh = syncCh
		}

		reqToProcess.ColInfoMtx.Lock()
		reqToProcess.ColInfo.ColId = colId
		// no need to truncate if we use the leb128CidLen
		leb128Cid, leb128CidLen, err := base.NewUleb128(colId, router.dataPool.GetByteSlice, true /*truncate*/)
		if err != nil {
			err = fmt.Errorf("LEB encoding for collection ID %v error: %v", colId, err)
			router.dataPool.PutByteSlice(leb128Cid)
			reqToProcess.ColInfoMtx.Unlock()
			return err
		}
		totalLen := len(reqToProcess.Req.Key) + leb128CidLen
		reqToProcess.ColInfo.ColIDPrefixedKeyLen = totalLen
		reqToProcess.ColInfo.ColIDPrefixedKey, err = router.dataPool.GetByteSlice(uint64(totalLen))
		if err != nil {
			reqToProcess.ColInfo.ColIDPrefixedKey = make([]byte, totalLen, totalLen)
			err = nil
		} else {
			reqToProcess.ColInfo.ColIDPrefixedKey = reqToProcess.ColInfo.ColIDPrefixedKey[:0]
		}

		// Embed collection ID in the beginning of the key
		copy(reqToProcess.ColInfo.ColIDPrefixedKey[0:leb128CidLen], leb128Cid[0:leb128CidLen])
		copy(reqToProcess.ColInfo.ColIDPrefixedKey[leb128CidLen:totalLen], reqToProcess.Req.Key)
		reqToProcess.Req.Key = reqToProcess.ColInfo.ColIDPrefixedKey[0:totalLen]
		reqToProcess.Req.Keylen = totalLen

		if migrationNamespaceMap != nil {
			reqToProcess.ColInfo.TargetNamespace = migrationNamespaceMap[colId]
		}
		reqToProcess.ColInfoMtx.Unlock()
	}
	if numCols > 1 {
		firstReq.Cloned = true
		firstReq.ClonedSyncCh = syncCh
		vbno := firstReq.Req.VBucket
		seqno := firstReq.Seqno
		totalInstances := len(colIds)
		isDelete := firstReq.Req.Opcode == mc.UPR_DELETION || firstReq.Req.Opcode == mc.UPR_EXPIRATION
		var data []interface{}
		data = append(data, vbno)
		data = append(data, seqno)
		data = append(data, totalInstances)
		data = append(data, isDelete)
		data = append(data, syncCh)

		router.RaiseEvent(common.NewEvent(common.DataCloned, data, router, nil, nil))
	}
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
	router.filter.SetShouldSkipUncommittedTxn(expDelMode.IsSkipReplicateUncommittedTxnSet())
	router.filter.SetShouldSkipBinaryDocs(expDelMode.IsSkipBinarySet())
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

func (router *Router) setMobileCompatibility(val uint32) {
	router.filter.SetMobileCompatibility(val)
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

	pair, ok := settings[metadata.BrokenMappingsUpdateKey].([]interface{})
	if ok && len(pair) == 3 {
		brokenMappingsRO, ok2 := pair[0].(*metadata.CollectionNamespaceMapping)
		targetManifestId, ok := pair[1].(uint64)
		isRollBack, ok3 := pair[2].(bool)
		if ok && ok2 && ok3 && router.Router.IsStartable() {
			router.collectionsRouting.UpdateBrokenMappingsPair(brokenMappingsRO, targetManifestId, isRollBack)
		}
	}

	collectionsMgtMode, ok := settings[metadata.CollectionsMgtMultiKey].(base.CollectionsMgtType)
	if ok {
		router.collectionModes.Set(collectionsMgtMode)
	}

	mobileCompatibleMode, ok := settings[metadata.MobileCompatibleKey].(int)
	if ok {
		router.setMobileCompatibility(uint32(mobileCompatibleMode))
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
	newReq.ColInfoMtx.Lock()
	newReq.ColInfo = nil
	newReq.ColInfoMtx.Unlock()
	newReq.SrcColNamespaceMtx.Lock()
	newReq.SrcColNamespace = nil
	newReq.SrcColNamespaceMtx.Unlock()
	newReq.Cloned = false
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

			req.ColInfoMtx.Lock()
			if req.ColInfo != nil && router.targetColInfoPool != nil {
				if req.ColInfo.ColIDPrefixedKeyLen > 0 && router.dataPool != nil {
					req.ColInfo.ColIDPrefixedKeyLen = 0
					router.dataPool.PutByteSlice(req.ColInfo.ColIDPrefixedKey)
				}
				req.ColInfo.ColIDPrefixedKeyLen = 0
				req.ColInfo.ManifestId = 0
				router.targetColInfoPool.Put(req.ColInfo)
			}
			req.ColInfoMtx.Unlock()

			req.SlicesToBeReleasedMtx.Lock()
			if len(req.SlicesToBeReleasedByRouter) > 0 {
				for _, slice := range req.SlicesToBeReleasedByRouter {
					router.dataPool.PutByteSlice(slice)
				}
			}
			req.SlicesToBeReleasedByRouter = nil
			req.SlicesToBeReleasedMtx.Unlock()
			req.Cloned = false
			req.ClonedSyncCh = nil
			router.mcRequestPool.Put(obj.(*base.WrappedMCRequest))
		}
	default:
		panic(fmt.Sprintf("Coding bug type is %v", reflect.TypeOf(obj)))
	}
}
