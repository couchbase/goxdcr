// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata_svc

import (
	"crypto/sha256"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const sourceRefreshStr = "sourceRefresh"
const targetRefreshStr = "targetRefresh"

var defaultManifest metadata.CollectionsManifest = metadata.NewDefaultCollectionsManifest()

type CollectionsManifestService struct {
	remoteClusterSvc        service_def.RemoteClusterSvc
	replicationSpecSvc      service_def.ReplicationSpecSvc
	uiLogSvc                service_def.UILogSvc
	utilities               utils.UtilsIface
	checkpointsSvc          service_def.CheckpointsService
	logger                  *log.CommonLogger
	loggerCtx               *log.LoggerContext
	xdcrTopologySvc         service_def.XDCRCompTopologySvc
	metakvSvc               service_def.ManifestsService
	metadataChangeCallbacks []base.MetadataChangeHandlerCallback
	metadataChangeCbMtx     sync.RWMutex

	// Each replication spec has a corresponding Agent based on replicationSpec.Id
	agentsMtx sync.RWMutex
	agentsMap map[string]*CollectionsManifestAgent

	// Each source bucket contains a getter based on sourceBucketName
	// It is possible for multiple replicationSpecs (agents)
	// to have the same source bucket (getter)
	srcBucketGetterMtx     sync.RWMutex
	srcBucketGetters       map[string]*BucketManifestGetter
	srcBucketGettersRefCnt map[string]uint64
}

func NewCollectionsManifestService(remoteClusterSvc service_def.RemoteClusterSvc,
	replicationSpecSvc service_def.ReplicationSpecSvc,
	uiLogSvc service_def.UILogSvc,
	loggerCtx *log.LoggerContext,
	utilities utils.UtilsIface,
	checkpointsSvc service_def.CheckpointsService,
	xdcrTopologySvc service_def.XDCRCompTopologySvc,
	metakvSvc service_def.ManifestsService) (*CollectionsManifestService, error) {
	svc := &CollectionsManifestService{
		remoteClusterSvc:       remoteClusterSvc,
		replicationSpecSvc:     replicationSpecSvc,
		uiLogSvc:               uiLogSvc,
		utilities:              utilities,
		checkpointsSvc:         checkpointsSvc,
		agentsMap:              make(map[string]*CollectionsManifestAgent),
		srcBucketGetters:       make(map[string]*BucketManifestGetter),
		srcBucketGettersRefCnt: make(map[string]uint64),
		loggerCtx:              loggerCtx,
		xdcrTopologySvc:        xdcrTopologySvc,
		metakvSvc:              metakvSvc,
	}
	return svc, svc.start()
}

// When it first starts up, it needs to load the necessary information, including
// 1. All replication specs that exist
// 2. Any perviously persisted manifests
func (c *CollectionsManifestService) start() error {
	c.logger = log.NewLogger("CollectionsManifestSvc", c.loggerCtx)
	c.logger.Infof("CollectionsManifestSvc starting...")

	replSpecs, err := c.replicationSpecSvc.AllReplicationSpecs()
	if err != nil {
		return err
	}
	for _, spec := range replSpecs {
		if spec == nil {
			continue
		}
		err = c.handleNewReplSpec(spec, true /*starting*/)
		if err != nil {
			return err
		}
	}

	c.logger.Infof("CollectionsManifestSvc started")
	return nil
}

// handle new and del should be called sequencially since replSpecService calls the callback sequentially
func (c *CollectionsManifestService) handleNewReplSpec(spec *metadata.ReplicationSpecification, starting bool) error {
	if !starting {
		c.logger.Infof("Handling new spec: %v\n", spec)
	} else {
		c.logger.Infof("Handling existing spec: %v\n", spec)
	}

	// CollectionsManifestService is also the dedicated manifest getter for the source side
	// Target side is handled by remote cluster service
	c.srcBucketGetterMtx.Lock()
	getter, ok := c.srcBucketGetters[spec.SourceBucketName]
	if !ok {
		// NOTE - if the source bucket is deleted and recreated, the manifest UID will go backwards
		// i.e. a brand new source bucket will have a manifest UID of 0 with default scope and collection
		// whereas previous instance of the source bucket has uid of > 0
		// Replication spec service should gc the spec if this is the case, but there is a window
		// when the manifest service may pull a manifest from a reincarnated bucket
		getter = NewBucketManifestGetter(spec.SourceBucketName, c, time.Duration(base.ManifestRefreshSrcInterval)*time.Second)
		c.srcBucketGetters[spec.SourceBucketName] = getter
	}
	c.srcBucketGettersRefCnt[spec.SourceBucketName]++
	c.srcBucketGetterMtx.Unlock()

	c.agentsMtx.Lock()
	agent := NewCollectionsManifestAgent(spec.Id,
		c.remoteClusterSvc, c.checkpointsSvc, c.logger, c.utilities, spec,
		c, getter.GetManifest, c.metakvSvc, c.metadataChangeCb)
	c.agentsMap[spec.Id] = agent
	c.agentsMtx.Unlock()
	return agent.Start()
}

func (c *CollectionsManifestService) handleDelReplSpec(oldSpec *metadata.ReplicationSpecification) {
	c.logger.Infof("Handling deleted spec: %v\n", oldSpec)
	c.agentsMtx.Lock()
	agent, ok := c.agentsMap[oldSpec.Id]
	delete(c.agentsMap, oldSpec.Id)
	c.agentsMtx.Unlock()

	if ok {
		agent.Stop()
		agent.DeleteManifests() // can be called after stop
	}

	c.srcBucketGetterMtx.Lock()
	cnt, ok := c.srcBucketGettersRefCnt[oldSpec.SourceBucketName]
	if ok {
		if cnt > 0 {
			c.srcBucketGettersRefCnt[oldSpec.SourceBucketName]--
			cnt = c.srcBucketGettersRefCnt[oldSpec.SourceBucketName]
		}
		if cnt == 0 {
			delete(c.srcBucketGetters, oldSpec.SourceBucketName)
			delete(c.srcBucketGettersRefCnt, oldSpec.SourceBucketName)
		}
	}
	c.srcBucketGetterMtx.Unlock()
}

// This implements the source side manifest getter
func (c *CollectionsManifestService) CollectionManifestGetter(bucketName string) (*metadata.CollectionsManifest, error) {
	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate,
		clientKey, err := c.xdcrTopologySvc.MyCredentials()
	if err != nil {
		return nil, err
	}
	localConnStr, err := c.xdcrTopologySvc.MyConnectionStr()
	if err != nil {
		return nil, err
	}

	return c.utilities.GetCollectionsManifest(localConnStr, bucketName, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, c.logger)
}

func (c *CollectionsManifestService) ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}, wg *sync.WaitGroup) error {
	if wg != nil {
		defer wg.Done()
	}

	oldSpec, ok := oldVal.(*metadata.ReplicationSpecification)
	if !ok {
		return base.ErrorInvalidInput
	}
	newSpec, ok := newVal.(*metadata.ReplicationSpecification)
	if !ok {
		return base.ErrorInvalidInput
	}

	if oldSpec == nil && newSpec != nil {
		c.handleNewReplSpec(newSpec, false /*starting*/)
	} else if oldSpec != nil && newSpec == nil {
		c.handleDelReplSpec(oldSpec)
	}
	return nil
}

func (c *CollectionsManifestService) getAgent(spec *metadata.ReplicationSpecification) (*CollectionsManifestAgent, error) {
	if spec == nil {
		return nil, fmt.Errorf("Nil spec given for getting agent")
	}
	c.agentsMtx.RLock()
	agent, ok := c.agentsMap[spec.Id]
	c.agentsMtx.RUnlock()
	if !ok {
		return nil, fmt.Errorf("Cannot find agent for spec %v", spec.Id)
	}
	return agent, nil
}

func (c *CollectionsManifestService) GetLatestManifests(spec *metadata.ReplicationSpecification, specMayNotExist bool) (src, tgt *metadata.CollectionsManifest, err error) {
	var agent *CollectionsManifestAgent
	agent, err = c.getAgent(spec)
	if err != nil && !specMayNotExist {
		c.logger.Warnf("Unable to find agent given spec %v - %v", spec, err)
		return
	}

	var agentIsTemporary bool
	if agent == nil && specMayNotExist {
		tempBucketManifestGetter := NewBucketManifestGetter(spec.SourceBucketName, c, time.Duration(base.ManifestRefreshSrcInterval)*time.Second)
		agent = NewCollectionsManifestAgent(spec.Id,
			c.remoteClusterSvc, c.checkpointsSvc, c.logger, c.utilities, spec,
			c, tempBucketManifestGetter.GetManifest, c.metakvSvc, nil)
		agent.SetTempAgent()
		agentIsTemporary = true
		err = agent.Start()
		if err != nil {
			c.logger.Errorf("Unable to start temp agent for %v to get manifests - %v", spec.Id, err)
			return
		}
	}

	src, srcErr := agent.GetSourceManifest()
	tgt, tgtErr := agent.GetTargetManifest()
	if srcErr != nil || tgtErr != nil {
		err = fmt.Errorf("GetLatestManifest - sourceErr: %v targetError: %v", srcErr, tgtErr)
	}

	if agentIsTemporary {
		agent.Stop()
	}
	return
}

// This is used during startup so that Backfill Manager knows what was the last persisted was and figure out differences from here on out
func (c *CollectionsManifestService) GetLastPersistedManifests(spec *metadata.ReplicationSpecification) (*metadata.CollectionsManifestPair, error) {
	agent, err := c.getAgent(spec)
	if err != nil {
		defaultPair := &metadata.CollectionsManifestPair{&defaultManifest, &defaultManifest}
		return defaultPair, err
	}
	return agent.GetLastPersistedManifests()
}

// Entry point by checkpoint manager as part of checkpointing
func (c *CollectionsManifestService) PersistNeededManifests(spec *metadata.ReplicationSpecification) error {
	agent, err := c.getAgent(spec)
	if err != nil {
		return err
	}

	srcErr, tgtErr, _, _ := agent.PersistNeededManifests()
	if srcErr != nil || tgtErr != nil {
		return fmt.Errorf("SourceManifestForDCP persist err: %v TargetManifest persist err: %v", srcErr, tgtErr)
	}
	return nil
}

func (c *CollectionsManifestService) SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback) {
	c.metadataChangeCbMtx.Lock()
	c.metadataChangeCallbacks = append(c.metadataChangeCallbacks, callBack)
	c.metadataChangeCbMtx.Unlock()
}

func (c *CollectionsManifestService) metadataChangeCb(specId string, oldManifestPair, newManifestPair interface{}) error {
	c.metadataChangeCbMtx.RLock()
	defer c.metadataChangeCbMtx.RUnlock()

	var lastErr error
	for _, cb := range c.metadataChangeCallbacks {
		err := cb(specId, oldManifestPair, newManifestPair)
		if err != nil {
			c.logger.Error(err.Error())
			lastErr = err
		}
	}
	return lastErr
}

//  When replication starts, it needs to request specific manifest by version, such as resuming from checkpoint
func (c *CollectionsManifestService) GetSpecificSourceManifest(spec *metadata.ReplicationSpecification, manifestVersion uint64) (*metadata.CollectionsManifest, error) {
	c.agentsMtx.RLock()
	agent, ok := c.agentsMap[spec.Id]
	c.agentsMtx.RUnlock()
	if !ok {
		return nil, fmt.Errorf("Unable to find agent for spec %v\n", spec.Id)
	}
	return agent.GetSpecificSourceManifest(manifestVersion)
}

//  When replication starts, it needs to request specific manifest by version, such as resuming from checkpoint
func (c *CollectionsManifestService) GetSpecificTargetManifest(spec *metadata.ReplicationSpecification, manifestVersion uint64) (*metadata.CollectionsManifest, error) {
	c.agentsMtx.RLock()
	agent, ok := c.agentsMap[spec.Id]
	c.agentsMtx.RUnlock()
	if !ok {
		return nil, fmt.Errorf("Unable to find agent for spec %v\n", spec.Id)
	}
	return agent.GetSpecificTargetManifest(manifestVersion)
}

func (c *CollectionsManifestService) ForceTargetManifestRefresh(spec *metadata.ReplicationSpecification) error {
	c.agentsMtx.RLock()
	agent, ok := c.agentsMap[spec.Id]
	c.agentsMtx.RUnlock()
	if !ok {
		return fmt.Errorf("Unable to find agent for spec %v\n", spec.Id)
	}
	return agent.ForceTargetManifestRefresh()
}

type AgentSrcManifestGetter func() *metadata.CollectionsManifest

type AgentPersistResult struct {
	SrcErr     error
	TgtErr     error
	SrcUpdated bool
	TgtUpdated bool
}

// Each collectionsManifestAgent is in charge of handling manifest information for a specific
// replication. The Service will make decisions and handle book-keeping based on what
// the agents' information
type CollectionsManifestAgent struct {
	id                string
	checkpointsSvc    service_def.CheckpointsService
	logger            *log.CommonLogger
	utilities         utils.UtilsIface
	replicationSpec   *metadata.ReplicationSpecification
	manifestOps       service_def.CollectionsManifestOps
	srcManifestGetter AgentSrcManifestGetter
	finCh             chan bool
	metakvSvc         service_def.ManifestsService
	metadataChangeCb  base.MetadataChangeHandlerCallback

	// The target side manifest getter
	remoteClusterSvc service_def.RemoteClusterSvc

	remoteClusterRefPopulated uint32
	remoteClusterRef          *metadata.RemoteClusterReference

	// Last pulled manifest
	srcMtx         sync.RWMutex
	tgtMtx         sync.RWMutex
	lastSourcePull uint64
	lastTargetPull uint64
	sourceCache    ManifestsCache
	targetCache    ManifestsCache
	// When Backfill manager starts up, it needs to know the highest manifest saved for a replication
	// This must be restored from checkpoint and not from ns_server to ensure no data loss
	lastSourceStoredManifest uint64
	lastTargetStoredManifest uint64

	// Last pulled checkpoint docs
	ckptDocsCache     map[uint16]*metadata.CheckpointsDoc
	ckptDocsCacheTime time.Time

	// Hashes representing the current manifests list stored in metakv
	hashMtx              sync.Mutex
	persistedSrcListHash [sha256.Size]byte
	persistedTgtListHash [sha256.Size]byte

	// atomics to prevent races
	started                uint32
	loadedFromMetakv       uint32
	persistHandlerStarted  uint32
	singlePersistReq       chan bool
	singlePersistResultReq chan bool
	singlePersistResp      chan AgentPersistResult

	// If agent is used as part of spec creation/validation process, then this is considered a tempAgent
	tempAgent bool
}

func NewCollectionsManifestAgent(name string, remoteClusterSvc service_def.RemoteClusterSvc, checkpointsSvc service_def.CheckpointsService, logger *log.CommonLogger, utilities utils.UtilsIface, replicationSpec *metadata.ReplicationSpecification, manifestOps service_def.CollectionsManifestOps, srcManifestGetter AgentSrcManifestGetter, metakvSvc service_def.ManifestsService, metadataChangeCb base.MetadataChangeHandlerCallback) *CollectionsManifestAgent {
	manifestAgent := &CollectionsManifestAgent{
		id:                     name,
		remoteClusterSvc:       remoteClusterSvc,
		checkpointsSvc:         checkpointsSvc,
		logger:                 logger,
		utilities:              utilities,
		replicationSpec:        replicationSpec,
		manifestOps:            manifestOps,
		srcManifestGetter:      srcManifestGetter,
		finCh:                  make(chan bool, 1),
		ckptDocsCache:          make(map[uint16]*metadata.CheckpointsDoc),
		sourceCache:            make(ManifestsCache),
		targetCache:            make(ManifestsCache),
		metakvSvc:              metakvSvc,
		metadataChangeCb:       metadataChangeCb,
		singlePersistReq:       make(chan bool, 1),
		singlePersistResultReq: make(chan bool, 10),
		singlePersistResp:      make(chan AgentPersistResult, 11),
	}
	defaultManifest := metadata.NewDefaultCollectionsManifest()
	manifestAgent.sourceCache[0] = &defaultManifest
	manifestAgent.targetCache[0] = &defaultManifest
	return manifestAgent
}

func (a *CollectionsManifestAgent) refreshAndNotify(refreshImmediately bool) {
	oldSrc, newSrc, srcErr := a.refreshSource()
	oldTgt, newTgt, tgtErr := a.refreshTarget(refreshImmediately)

	if srcErr != nil {
		oldSrc = nil
		newSrc = nil
	}

	if tgtErr != nil {
		oldTgt = nil
		newTgt = nil
	}

	if srcErr == nil || tgtErr == nil {
		a.notifyManifestsChange(oldSrc, newSrc, oldTgt, newTgt)
	}
}

func (a *CollectionsManifestAgent) notifyManifestsChange(oldSrc, newSrc, oldTgt, newTgt *metadata.CollectionsManifest) {
	if a.tempAgent {
		// tempAgents do not notify manifest change
		return
	}
	oldPair := metadata.NewCollectionsManifestPair(oldSrc, oldTgt)
	newPair := metadata.NewCollectionsManifestPair(newSrc, newTgt)
	err := a.metadataChangeCb(a.replicationSpec.Id, oldPair, newPair)
	if err != nil {
		a.logger.Errorf("Error with callback: %v\n", err.Error())
	}
}

func (a *CollectionsManifestAgent) runPeriodicRefresh(refreshImmediately bool) {
	if refreshImmediately {
		a.refreshAndNotify(true /*refreshImmediately*/)
	}

	ticker := time.NewTicker(base.TopologyChangeCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.finCh:
			return
		case <-ticker.C:
			a.refreshAndNotify(false /*refreshImmediately*/)
		}
	}
}

func (a *CollectionsManifestAgent) runPersistRequestHandler() {
	var srcErr error
	var tgtErr error
	var srcUpdated bool
	var tgtUpdated bool

	feedResultBack := func() {
		// Feed result back
		result := AgentPersistResult{srcErr, tgtErr, srcUpdated, tgtUpdated}
		a.singlePersistResp <- result
	}

	atomic.StoreUint32(&a.persistHandlerStarted, 1)

	for {
		select {
		case <-a.finCh:
			atomic.StoreUint32(&a.persistHandlerStarted, 0)
			return
		case <-a.singlePersistReq:
			// One single instance to update the result
			srcErr, tgtErr, srcUpdated, tgtUpdated = a.persistNeededManifestsInternal()

			feedResultBack()
		case <-a.singlePersistResultReq:
			// This channel gets triggered only if above is busy
			feedResultBack()
		}
	}
}

// This method is called when checkpoint manager is doing a checkpointing
// Its job is to read the checkpointed files, and then read its internal pulled caches, dedup and prune
// so that all the manifests that are needed by both source and target are stored in metakv
// If the metakv already contains all the necessary manifests, via the use of sha256 hash, then don't
// update the metakv
func (a *CollectionsManifestAgent) PersistNeededManifests() (error, error, bool, bool) {
	if a.isStopped() || atomic.LoadUint32(&a.persistHandlerStarted) == 0 {
		return parts.PartStoppedError, parts.PartStoppedError, false, false
	}

	// Persisting manifest only happens AFTER the checkpoints have been saved
	// And if a bursty checkpoint operation happens such that this method is called consecutively
	// we must be careful not to overload metakv with the internal() op
	// This is because checkpoints are huge, so ckpt read + manifest writes are expensive
	// In the worst case, there may only be one manifest that is missing, and the latest, burstiest
	// checkpoint will be invalid, and that's ok - we'll just pick an older checkpoint
	var result AgentPersistResult
	select {
	case a.singlePersistReq <- true:
		// This request went through
		result = <-a.singlePersistResp
	default:
		// Request did not go through because persistence is already ongoing. Just get the result of the ongoing process
		select {
		case a.singlePersistResultReq <- true:
			result = <-a.singlePersistResp
		}
	}

	return result.SrcErr, result.TgtErr, result.SrcUpdated, result.TgtUpdated
}

func (a *CollectionsManifestAgent) persistNeededManifestsInternal() (srcErr, tgtErr error, srcUpdated, tgtUpdated bool) {
	// find all the manifest UIDs that existing checkpoints refer for both source and target
	srcList, tgtList, err := a.getAllManifestsUids()
	// Clean up agent's current manifest storage to get rid of unreferred sourcelist
	a.cleanupUnreferredManifests(srcList, tgtList)
	if err != nil {
		srcErr = err
		tgtErr = err
		return
	}

	var srcMetaList metadata.ManifestsList
	var tgtMetaList metadata.ManifestsList

	a.srcMtx.RLock()
	for _, manifest := range a.sourceCache {
		srcMetaList = append(srcMetaList, manifest)
	}
	a.srcMtx.RUnlock()

	a.tgtMtx.RLock()
	for _, manifest := range a.targetCache {
		tgtMetaList = append(tgtMetaList, manifest)
	}
	a.tgtMtx.RUnlock()

	srcMetaList.Sort()
	tgtMetaList.Sort()

	// The hash here are used to remember what was in the metakv,
	// instead of keeping the actual data that was stored
	// Since manifest cache can get really big
	srcListHash, srcHashErr := srcMetaList.Sha256()
	tgtListHash, tgtHashErr := tgtMetaList.Sha256()

	a.hashMtx.Lock()
	defer a.hashMtx.Unlock()

	if srcHashErr != nil {
		a.logger.Warnf("Not storing source hash due to err: %v\n", srcHashErr)
	} else if tgtHashErr != nil {
		a.logger.Warnf("Not storing target hash due to err: %v\n", tgtHashErr)
	}

	if srcHashErr == nil && !reflect.DeepEqual(a.persistedSrcListHash, srcListHash) {
		// metakv is out of date
		srcErr = a.metakvSvc.UpsertSourceManifests(a.replicationSpec, &srcMetaList)
		if srcErr != nil {
			a.logger.Warnf("Upserting source resulted in err: %v\n", srcErr)
		} else {
			srcUpdated = true
		}
		if srcHashErr == nil {
			a.persistedSrcListHash = srcListHash
		}
	}

	if tgtHashErr == nil && !reflect.DeepEqual(a.persistedTgtListHash, tgtListHash) {
		tgtErr = a.metakvSvc.UpsertTargetManifests(a.replicationSpec, &tgtMetaList)
		if tgtErr != nil {
			a.logger.Warnf("Upserting target resulted in err: %v\n", tgtErr)
		} else {
			tgtUpdated = true
		}
		if tgtHashErr == nil {
			a.persistedTgtListHash = tgtListHash
		}
	}

	return
}

func (a *CollectionsManifestAgent) DeleteManifests() error {
	return a.metakvSvc.DelManifests(a.replicationSpec)
}

type ManifestsCache map[uint64]*metadata.CollectionsManifest

func (m ManifestsCache) String() string {
	var output []string
	for k, v := range m {
		output = append(output, fmt.Sprintf("%v:%v\n", k, v))
	}
	return strings.Join(output, " ")
}

func (m ManifestsCache) GetMaxManifestID() uint64 {
	var max uint64
	for k, v := range m {
		if v == nil {
			// This is a problem...
			continue
		}
		if k > max {
			max = k
		}
	}
	return max
}

func (a *CollectionsManifestAgent) loadManifestsFromMetakv() (srcErr, tgtErr error) {
	srcGet, srcErr := a.metakvSvc.GetSourceManifests(a.replicationSpec)
	tgtGet, tgtErr := a.metakvSvc.GetTargetManifests(a.replicationSpec)

	if srcErr != nil && srcErr != service_def.MetadataNotFoundErr {
		a.logger.Warnf("Unable to load source from metakv: %v\n", srcErr)
	} else {
		if srcGet != nil {
			a.hashMtx.Lock()
			srcGet.Sort()
			srcHash, srcHashErr := srcGet.Sha256()
			if srcHashErr == nil {
				a.persistedSrcListHash = srcHash
			}
			a.hashMtx.Unlock()
		}

		a.srcMtx.Lock()
		a.sourceCache = make(ManifestsCache)
		if srcGet != nil {
			var maxPull uint64
			for _, manifest := range *srcGet {
				if manifest == nil {
					continue
				}
				a.sourceCache[manifest.Uid()] = manifest
				if manifest.Uid() > maxPull {
					maxPull = manifest.Uid()
				}
			}
			a.lastSourcePull = maxPull
			a.lastSourceStoredManifest = maxPull

		}
		a.srcMtx.Unlock()
	}

	if tgtErr != nil && tgtErr != service_def.MetadataNotFoundErr {
		a.logger.Warnf("Unable to load target from metakv: %v\n", tgtErr)
	} else {
		if tgtGet != nil {
			a.hashMtx.Lock()
			tgtGet.Sort()
			tgtHash, tgtHashErr := tgtGet.Sha256()
			if tgtHashErr == nil {
				a.persistedTgtListHash = tgtHash
			}
			a.hashMtx.Unlock()
		}

		a.tgtMtx.Lock()
		a.targetCache = make(ManifestsCache)

		if tgtGet != nil {
			var maxPull uint64
			for _, manifest := range *tgtGet {
				if manifest == nil {
					continue
				}
				a.targetCache[manifest.Uid()] = manifest
				if manifest.Uid() > maxPull {
					maxPull = manifest.Uid()
				}
			}
			a.lastTargetPull = maxPull
			a.lastTargetStoredManifest = maxPull
		}

		a.tgtMtx.Unlock()
	}

	if srcErr == nil && tgtErr == nil {
		atomic.StoreUint32(&a.loadedFromMetakv, 1)
	}
	return
}

func (a *CollectionsManifestAgent) populateRemoteClusterRefOnce() error {
	// For a spec to exist, the reference must exist also
	retryOp := func() error {
		var getErr error
		a.remoteClusterRef, getErr = a.remoteClusterSvc.RemoteClusterByUuid(a.replicationSpec.TargetClusterUUID, false)
		return getErr
	}
	err := a.utilities.ExponentialBackoffExecutor("CollectionsManifestSvcHandleNewReplSpec",
		base.DefaultHttpTimeoutWaitTime, base.DefaultHttpTimeoutMaxRetry, base.DefaultHttpTimeoutRetryFactor, retryOp)
	if err != nil {
		if a.tempAgent {
			// tempagent do not panic just return error
			return err
		}
		// Worst case scenario even after 180 seconds, panic and XDCR process will reload everything in order from metakv
		// as in, load the remote cluster reference first, then load the specs
		panic(fmt.Sprintf("RemoteCluster service bootstrapping did not finish within %v and XDCR must restart to try again", base.DefaultHttpTimeout))
	}
	atomic.StoreUint32(&a.remoteClusterRefPopulated, 1)
	return nil
}

// These should not block
func (a *CollectionsManifestAgent) Start() error {
	if atomic.CompareAndSwapUint32(&a.started, 0, 1) {
		if a.tempAgent {
			err := a.populateRemoteClusterRefOnce()
			if err != nil {
				return fmt.Errorf("Temp agent %v unable to populate remote cluster reference %v", a.id, err)
			}
			a.refreshAndNotify(true)
		} else {
			var refreshImmediately bool
			srcErr, tgtErr := a.loadManifestsFromMetakv()
			if srcErr == service_def.MetadataNotFoundErr {
				refreshImmediately = true
				srcErr = nil
			}
			if tgtErr == service_def.MetadataNotFoundErr {
				refreshImmediately = true
				tgtErr = nil
			}

			if srcErr != nil || tgtErr != nil {
				a.logger.Warnf("CollectionsManifestAgent %v starting sourceErr: %v tgtErr: %v", a.replicationSpec.Id, srcErr, tgtErr)
				// For now - since collections isn't officially supported, just log the warning and return nil
				return nil
			}

			go a.populateRemoteClusterRefOnce()
			go a.runPeriodicRefresh(refreshImmediately)
			go a.runPersistRequestHandler()
		}
		return nil
	}
	return parts.PartAlreadyStartedError
}

// Don't block
func (a *CollectionsManifestAgent) Stop() {
	if atomic.CompareAndSwapUint32(&a.started, 1, 0) {
		a.manifestOps = nil
		close(a.finCh)
	}
}

func (a *CollectionsManifestAgent) isStopped() bool {
	return atomic.LoadUint32(&a.started) == 0
}

func (a *CollectionsManifestAgent) GetSourceManifest() (*metadata.CollectionsManifest, error) {
	if a.isStopped() {
		return nil, parts.PartStoppedError
	}

	var err error
	var oldSrc *metadata.CollectionsManifest
	var newSrc *metadata.CollectionsManifest
	a.srcMtx.RLock()
	_, ok := a.sourceCache[a.lastSourcePull]
	if !ok {
		a.srcMtx.RUnlock()
		oldSrc, newSrc, err = a.refreshSource()
		if err == nil {
			a.notifyManifestsChange(oldSrc, newSrc, nil /*oldTgt*/, nil /*newTgt*/)
		}
		a.srcMtx.RLock()
	}
	defer a.srcMtx.RUnlock()
	if err != nil {
		return nil, err
	} else {
		manifest := a.sourceCache[a.lastSourcePull]
		if manifest == nil {
			panic(fmt.Sprintf("sourceCache %v is nil", a.lastSourcePull))
		}
		return manifest, nil
	}
}

// Caller can send in MaxUint64 to get the latest cached version
// If caller passes in an actual non-MaxUint64 version of the manifest, then
// look for the specific version in cache. If it does not exist, then find one that is higher than the requested
// available, fetching it from ns_server if necessary
func (a *CollectionsManifestAgent) GetSpecificSourceManifest(manifestVersion uint64) (*metadata.CollectionsManifest, error) {
	if a.isStopped() {
		return nil, parts.PartStoppedError
	}

	if manifestVersion == 0 {
		return &defaultManifest, nil
	}

	if manifestVersion == math.MaxUint64 {
		a.srcMtx.RLock()
		defer a.srcMtx.RUnlock()
		if a.lastSourcePull == 0 {
			return &defaultManifest, nil
		} else {
			manifest, ok := a.sourceCache[a.lastSourcePull]
			if !ok {
				return nil, fmt.Errorf("Cannot find specific source manifest version %v", a.lastSourcePull)
			} else {
				return manifest, nil
			}
		}
	}

	var err error
	var oldSrc *metadata.CollectionsManifest
	var newSrc *metadata.CollectionsManifest
	a.srcMtx.RLock()
	manifest, ok := a.sourceCache[manifestVersion]
	if !ok || manifest == nil {
		var needEmergencyPull = true
		if manifestVersion <= a.lastSourcePull {
			// It is possible that when XDCR starts up, the collections manifest for a bucket is already at version Y
			// If there's no checkpoint, DCP starts from 0 and could send down mutations that refer to manifest X, where X < Y
			// If some collections exist in X but not in Y because they got deleted, those mutations will not be streamed
			// by DCP.
			// So if someone asks for X, and we don't have it, it should be safe to return Y
			for i := manifestVersion; i <= a.lastSourcePull; i++ {
				if manifest, ok = a.sourceCache[i]; ok && manifest != nil {
					needEmergencyPull = false
					break
				}
			}
		}

		// This covers the case of if manifestVersion > a.lastSourcePull
		if needEmergencyPull {
			a.srcMtx.RUnlock()
			oldSrc, newSrc, err = a.refreshSource()
			if err == nil {
				a.notifyManifestsChange(oldSrc, newSrc, nil /*oldTgt*/, nil /*newTgt*/)
			}
			a.srcMtx.RLock()
			if a.lastSourcePull < manifestVersion {
				// Even after emergency pull, we still cannot provide the caller a >= version of what they asked for
				err = fmt.Errorf("Unable to provide requested version %v even after emergency pull", manifestVersion)
			} else {
				manifest = a.sourceCache[a.lastSourcePull]
				if manifest == nil {
					err = fmt.Errorf("Nil manifest even though emergency pulled")
				}
			}
		}
	}
	a.srcMtx.RUnlock()
	return manifest, err
}

// MAX_UINT64 for manifestversion means the latest one
func (a *CollectionsManifestAgent) GetSpecificTargetManifest(manifestVersion uint64) (*metadata.CollectionsManifest, error) {
	if a.isStopped() {
		return nil, parts.PartStoppedError
	}

	if manifestVersion == 0 {
		return &defaultManifest, nil
	}

	a.tgtMtx.RLock()
	defer a.tgtMtx.RUnlock()

	var err error
	if manifestVersion == math.MaxUint64 {
		manifestVersion = a.lastTargetPull
		if a.lastTargetPull == 0 {
			return &defaultManifest, nil
		}
	}
	manifest, ok := a.targetCache[manifestVersion]
	if !ok {
		manifest = nil
		err = fmt.Errorf("Unable to find target manifest for version %v", manifestVersion)
	}

	return manifest, err
}

// Returns nil pair if not updated
func (a *CollectionsManifestAgent) refreshSource() (oldManifest, newManifest *metadata.CollectionsManifest, err error) {
	return a.refreshSourceCustom(base.BucketInfoOpWaitTime, base.BucketInfoOpMaxRetry, true /*lock*/)
}

// For if caller already holds a write-lock
func (a *CollectionsManifestAgent) refreshSourceNoLock() (oldManifest, newManifest *metadata.CollectionsManifest, err error) {
	return a.refreshSourceCustom(base.BucketInfoOpWaitTime, base.BucketInfoOpMaxRetry, false /*lock*/)
}

func (a *CollectionsManifestAgent) refreshSourceCustom(waitTime time.Duration, maxRetry int, lock bool) (oldManifest, newManifest *metadata.CollectionsManifest, err error) {
	if a.isStopped() {
		return nil, nil, parts.PartStoppedError
	}

	var manifest *metadata.CollectionsManifest
	var ok bool
	getRetry := func() error {
		manifest = a.srcManifestGetter()
		if manifest == nil {
			return fmt.Errorf("Unable to retrieve manifest from source bucket %v\n", a.replicationSpec.SourceBucketName)
		} else {
			return nil
		}
	}
	err = a.utilities.ExponentialBackoffExecutor(sourceRefreshStr, waitTime, maxRetry,
		base.BucketInfoOpRetryFactor, getRetry)
	if err != nil {
		a.logger.Errorf("refreshSource err: %v\n", err)
		if lock {
			a.srcMtx.Lock()
			defer a.srcMtx.Unlock()
		}
		maxID := a.sourceCache.GetMaxManifestID()
		if maxID < a.lastSourcePull {
			a.logger.Warnf("%v is unable to pull a higher source manifest, going back in time from %v to %v", a.lastSourcePull, maxID)
		}
		a.lastSourcePull = maxID
		return
	}

	if lock {
		a.srcMtx.RLock()
	}
	// It is possible that we pulled a default collection and lastSourcePull is initialized at 0
	if a.lastSourcePull < manifest.Uid() || len(a.sourceCache) == 0 {
		if lock {
			a.srcMtx.RUnlock()
			a.srcMtx.Lock()
		}
		a.logger.Infof("CollectionsManifestAgent: Updated source manifest from old version %v to new version %v\n", a.lastTargetPull, manifest.Uid())
		oldManifest, ok = a.sourceCache[a.lastSourcePull]
		a.lastSourcePull = manifest.Uid()
		a.sourceCache[manifest.Uid()] = manifest
		if ok {
			newManifest = manifest
		}
		if lock {
			a.srcMtx.Unlock()
		}
	} else {
		if lock {
			a.srcMtx.RUnlock()
		}
	}
	return
}

func (a *CollectionsManifestAgent) GetTargetManifest() (*metadata.CollectionsManifest, error) {
	if a.isStopped() {
		return nil, parts.PartStoppedError
	}

	var err error
	var oldTgt *metadata.CollectionsManifest
	var newTgt *metadata.CollectionsManifest
	a.tgtMtx.RLock()
	_, ok := a.targetCache[a.lastTargetPull]
	if !ok {
		a.tgtMtx.RUnlock()
		oldTgt, newTgt, err = a.refreshTarget(false)
		if err == nil {
			a.notifyManifestsChange(nil /*oldSrc*/, nil /*newSrc*/, oldTgt, newTgt)
		}
		a.tgtMtx.RLock()
	}
	defer a.tgtMtx.RUnlock()

	if err != nil {
		return nil, err
	} else {
		manifest := a.targetCache[a.lastTargetPull]
		if manifest == nil {
			panic(fmt.Sprintf("TargetCache %v is nil", a.lastTargetPull))
		}
		return manifest, nil
	}
}

// return nils if no update
func (a *CollectionsManifestAgent) refreshTarget(force bool) (*metadata.CollectionsManifest, *metadata.CollectionsManifest, error) {
	return a.refreshTargetCustom(force, true /*lock*/, base.RemoteMcRetryWaitTime, base.MaxRemoteMcRetry)
}

// Write lock must be held
func (a *CollectionsManifestAgent) refreshTargetNoLock(force bool) (*metadata.CollectionsManifest, *metadata.CollectionsManifest, error) {
	return a.refreshTargetCustom(force, false /*lock*/, base.RemoteMcRetryWaitTime, base.MaxRemoteMcRetry)
}

func (a *CollectionsManifestAgent) refreshTargetCustom(force, lock bool, waitTime time.Duration, maxRetry int) (oldManifest, newManifest *metadata.CollectionsManifest, err error) {
	if a.isStopped() {
		return nil, nil, parts.PartStoppedError
	}

	hasSupport, err := a.remoteClusterHasNoCollectionsCapability()
	if err != nil {
		return nil, nil, err
	} else if !hasSupport {
		return nil, nil, base.ErrorTargetCollectionsNotSupported
	}

	var manifest *metadata.CollectionsManifest
	var ok bool
	getRetry := func() error {
		clusterUuid := a.replicationSpec.TargetClusterUUID
		bucketName := a.replicationSpec.TargetBucketName
		manifest, err = a.remoteClusterSvc.GetManifestByUuid(clusterUuid, bucketName, force, a.tempAgent)
		if err != nil && err != ErrorRemoteClusterNoCollectionsCapability {
			a.logger.Errorf("RemoteClusterService GetManifest on %v for bucket %v returned %v\n", clusterUuid, bucketName, err)
			return err
		}
		return nil
	}
	err = a.utilities.ExponentialBackoffExecutor(targetRefreshStr, waitTime, maxRetry, base.RemoteMcRetryFactor, getRetry)
	if err != nil || manifest == nil {
		a.logger.Errorf("refreshTarget returned err: %v\n", err)
		if lock {
			a.tgtMtx.Lock()
			defer a.tgtMtx.Unlock()
		}
		maxID := a.targetCache.GetMaxManifestID()
		if maxID < a.lastTargetPull {
			a.logger.Warnf("%v is unable to pull a higher target manifest, going back in time from %v to %v", a.lastTargetPull, maxID)
		}
		a.lastTargetPull = maxID
		return
	}

	if lock {
		a.tgtMtx.RLock()
	}
	// It is possible that we pulled a default collection and lastTargetPull is initialized at 0
	if a.lastTargetPull < manifest.Uid() || len(a.targetCache) == 0 {
		if lock {
			a.tgtMtx.RUnlock()
			a.tgtMtx.Lock()
		}
		a.logger.Infof("CollectionsManifestAgent: Updated target manifest from old version %v to new version %v\n", a.lastTargetPull, manifest.Uid())
		oldManifest, ok = a.targetCache[a.lastTargetPull]
		a.lastTargetPull = manifest.Uid()
		a.targetCache[manifest.Uid()] = manifest
		if ok {
			newManifest = manifest
		}
		if lock {
			a.tgtMtx.Unlock()
		}
	} else {
		if lock {
			a.tgtMtx.RUnlock()
		}
	}
	return
}

// Gets a sorted list of manifest UIDs that are referred by the checkpoints
func (a *CollectionsManifestAgent) getAllManifestsUids() (srcManifestUids, tgtManifestUids []uint64, err error) {
	ckptDocs, err := a.checkpointsSvc.CheckpointsDocs(a.replicationSpec.Id, false /*needBrokenMapping*/)
	if err != nil {
		a.logger.Warnf("Unable to retrieve checkpoint docs for %v, operating persistence using last cached manifest from %v",
			a.replicationSpec.Id, a.ckptDocsCacheTime)
	} else {
		a.ckptDocsCacheTime = time.Now()
		a.ckptDocsCache = ckptDocs
	}

	srcDedupMap := make(map[uint64]bool)
	tgtDedupMap := make(map[uint64]bool)

	// First go through all the checkpoints
	for _, ckptDoc := range a.ckptDocsCache {
		if ckptDoc == nil {
			continue
		}
		for _, ckptRecord := range ckptDoc.Checkpoint_records {
			if ckptRecord == nil {
				continue
			}
			if ckptRecord.SourceManifestForDCP > 0 {
				srcDedupMap[ckptRecord.SourceManifestForDCP] = true
			}
			if ckptRecord.SourceManifestForBackfillMgr > 0 {
				srcDedupMap[ckptRecord.SourceManifestForBackfillMgr] = true
			}
			if ckptRecord.TargetManifest > 0 {
				tgtDedupMap[ckptRecord.TargetManifest] = true
			}
		}
	}

	// sorted deduped lists
	for uid, _ := range srcDedupMap {
		srcManifestUids = append(srcManifestUids, uid)
	}
	for uid, _ := range tgtDedupMap {
		tgtManifestUids = append(tgtManifestUids, uid)
	}
	base.SortUint64List(srcManifestUids)
	base.SortUint64List(tgtManifestUids)

	srcLen := len(srcManifestUids)
	tgtLen := len(tgtManifestUids)

	srcNeedsResort := srcLen == 0
	tgtNeedsResort := tgtLen == 0

	// Then go through the current cache ... anything lower then the lowest ckpt, don't save
	a.srcMtx.RLock()
	for uid, _ := range a.sourceCache {
		if uid == 0 {
			continue
		}
		if srcLen == 0 || uid > srcManifestUids[0] {
			_, exists := srcDedupMap[uid]
			if !exists {
				srcDedupMap[uid] = true
				srcNeedsResort = true
			}
		}
	}
	a.srcMtx.RUnlock()
	a.tgtMtx.RLock()
	for uid, _ := range a.targetCache {
		if uid == 0 {
			continue
		}
		if tgtLen == 0 || uid > tgtManifestUids[0] {
			_, exists := tgtDedupMap[uid]
			if !exists {
				tgtDedupMap[uid] = true
				tgtNeedsResort = true
			}
		}
	}
	a.tgtMtx.RUnlock()

	// Need to resort if needed
	if srcNeedsResort {
		srcManifestUids = srcManifestUids[:0]
		for uid, _ := range srcDedupMap {
			srcManifestUids = append(srcManifestUids, uid)
		}
		base.SortUint64List(srcManifestUids)
	}
	if tgtNeedsResort {
		tgtManifestUids = tgtManifestUids[:0]
		for uid, _ := range tgtDedupMap {
			tgtManifestUids = append(tgtManifestUids, uid)
		}
		base.SortUint64List(tgtManifestUids)
	}
	return
}

/*
 * Given sourceList and targetList, cleanup manifests that are no longer needed
 * in the collections agent
 */
func (a *CollectionsManifestAgent) cleanupUnreferredManifests(srcList, tgtList []uint64) error {
	var srcErr []uint64
	var tgtErr []uint64
	var err error
	var waitGroup sync.WaitGroup
	var needEmergencySourcePull bool
	var oldSrc *metadata.CollectionsManifest
	var newSrc *metadata.CollectionsManifest
	var needEmergencyTargetPull bool
	var oldTgt *metadata.CollectionsManifest
	var newTgt *metadata.CollectionsManifest

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		replacementMap := make(ManifestsCache)
		replacementMap[0] = &defaultManifest
		a.srcMtx.Lock()
		for _, uid := range srcList {
			manifest, ok := a.sourceCache[uid]
			if !ok {
				srcErr = append(srcErr, uid)
			} else {
				replacementMap[uid] = manifest
			}
		}
		// For safety, check the last source pull to ensure we didn't lose the last refresh
		if a.lastSourcePull > 0 {
			_, lastPullExists := replacementMap[a.lastSourcePull]
			_, lastPullOk := a.sourceCache[a.lastSourcePull]
			if !lastPullExists && lastPullOk {
				replacementMap[a.lastSourcePull] = a.sourceCache[a.lastSourcePull]
			} else {
				// Unable to find the lastSourcePull. Need to force a refresh before giving access back
				needEmergencySourcePull = true
			}
		}
		a.sourceCache = replacementMap
		if needEmergencySourcePull {
			var srcPullErr error
			oldSrc, newSrc, srcPullErr = a.refreshSourceNoLock()
			if srcPullErr != nil {
				// Don't notify any new sources
				oldSrc = nil
				newSrc = nil
			}
		}
		a.srcMtx.Unlock()
	}()

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		replacementMap := make(ManifestsCache)
		replacementMap[0] = &defaultManifest
		a.tgtMtx.Lock()
		for _, uid := range tgtList {
			manifest, ok := a.targetCache[uid]
			if !ok {
				tgtErr = append(tgtErr, uid)
			} else {
				replacementMap[uid] = manifest
			}
		}
		// For safety, check the last target pull to ensure we didn't lose the last refresh
		if a.lastTargetPull > 0 {
			_, lastPullExists := replacementMap[a.lastTargetPull]
			_, lastPullOk := a.targetCache[a.lastTargetPull]
			if !lastPullExists && lastPullOk {
				replacementMap[a.lastTargetPull] = a.targetCache[a.lastTargetPull]
			} else {
				needEmergencyTargetPull = true
			}
		}
		a.targetCache = replacementMap
		if needEmergencyTargetPull {
			var tgtPullErr error
			oldTgt, newTgt, tgtPullErr = a.refreshTargetNoLock(true /*force*/)
			if tgtPullErr != nil {
				oldTgt = nil
				newTgt = nil
			}
		}
		a.tgtMtx.Unlock()
	}()
	waitGroup.Wait()

	if needEmergencySourcePull || needEmergencyTargetPull {
		// Any out-of-periodic refresh will require notification
		a.notifyManifestsChange(oldSrc, newSrc, oldTgt, newTgt)
	}

	if len(srcErr) > 0 || len(tgtErr) > 0 {
		err = fmt.Errorf("Pruning %v, unable to find srcMmanifests %v and targetManifests %v",
			a.replicationSpec.Id, srcErr, tgtErr)
		a.logger.Warnf(err.Error())
	}
	return err
}

func (a *CollectionsManifestAgent) GetLastPersistedManifests() (*metadata.CollectionsManifestPair, error) {
	if atomic.LoadUint32(&a.loadedFromMetakv) == 0 {
		return nil, fmt.Errorf("metakv manifests Has not been loaded yet")
	} else if a.isStopped() {
		return nil, parts.PartStoppedError
	}

	a.srcMtx.RLock()
	srcManifest, ok := a.sourceCache[a.lastSourceStoredManifest]
	localLastStored := a.lastSourceStoredManifest
	a.srcMtx.RUnlock()
	if !ok {
		if localLastStored > 0 {
			return nil, fmt.Errorf("Cannot find source manifest version %v", localLastStored)
		} else {
			srcManifest = &defaultManifest
		}
	}

	a.tgtMtx.RLock()
	tgtManifest, ok := a.targetCache[a.lastTargetStoredManifest]
	localLastStored = a.lastTargetStoredManifest
	a.tgtMtx.RUnlock()
	if !ok {
		if localLastStored > 0 {
			return nil, fmt.Errorf("Cannot find manifest version %v", localLastStored)
		} else {
			tgtManifest = &defaultManifest
		}
	}

	return &metadata.CollectionsManifestPair{srcManifest, tgtManifest}, nil
}

func (a *CollectionsManifestAgent) ForceTargetManifestRefresh() error {
	oldTgt, newTgt, err := a.refreshTarget(true)
	if err == nil {
		a.notifyManifestsChange(nil /*oldSrc*/, nil /*newSrc*/, oldTgt, newTgt)
	}
	return err
}

func (a *CollectionsManifestAgent) SetTempAgent() {
	a.tempAgent = true
}

// Unit test func
func (a *CollectionsManifestAgent) testGetMetaLists(srcUids, tgtUids []uint64) (srcList, tgtList metadata.ManifestsList) {
	for _, uid := range srcUids {
		srcList = append(srcList, a.sourceCache[uid])
	}
	for _, uid := range tgtUids {
		tgtList = append(tgtList, a.targetCache[uid])
	}
	return
}

func (a *CollectionsManifestAgent) remoteClusterHasNoCollectionsCapability() (bool, error) {
	if atomic.LoadUint32(&a.remoteClusterRefPopulated) == 0 {
		return false, fmt.Errorf("%v - RemoteClusterReference has not finished populating", a.id)
	}

	capability, err := a.remoteClusterSvc.GetCapability(a.remoteClusterRef)
	if err != nil {
		return false, fmt.Errorf("%v getting capability: %v", a.id, err)
	}

	return capability.HasCollectionSupport(), nil
}
