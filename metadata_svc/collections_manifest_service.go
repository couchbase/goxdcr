// Copyright (c) 2013-2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the refific language governing permissions
// and limitations under the License.

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
	}
	c.srcBucketGetterMtx.Lock()
	getter, ok := c.srcBucketGetters[spec.SourceBucketName]
	if !ok {
		// NOTE - if the source bucket is deleted and recreated, the manifest UID will go backwards
		// i.e. a brand new source bucket will have a manifest UID of 0 with default scope and collection
		// whereas previous instance of the source bucket has uid of > 0
		// Replication spec service should gc the spec if this is the case, but there is a window
		// when the manifest service may pull a manifest from a reincarnated bucket
		getter = NewBucketManifestGetter(spec.SourceBucketName, c, base.TopologyChangeCheckInterval)
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

func (c *CollectionsManifestService) ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}) error {
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
		return nil, base.ErrorInvalidInput
	}
	c.agentsMtx.RLock()
	agent, ok := c.agentsMap[spec.Id]
	c.agentsMtx.RUnlock()
	if !ok {
		return nil, base.ErrorInvalidInput
	}
	return agent, nil
}

func (c *CollectionsManifestService) GetLatestManifests(spec *metadata.ReplicationSpecification) (src, tgt *metadata.CollectionsManifest, err error) {
	var agent service_def.CollectionsManifestAgentIface
	agent, err = c.getAgent(spec)
	if err != nil {
		return
	}

	src = agent.GetSourceManifest()
	tgt = agent.GetTargetManifest()
	return
}

func (c *CollectionsManifestService) GetOngoingManifests(spec *metadata.ReplicationSpecification, vb uint16) (*metadata.CollectionsManifestPair, error) {
	agent, err := c.getAgent(spec)
	if err != nil {
		defaultPair := &metadata.CollectionsManifestPair{&defaultManifest, &defaultManifest}
		return defaultPair, err
	}
	return agent.GetOngoingManifests(vb), nil
}

// Entry point by checkpoint manager as part of checkpointing
func (c *CollectionsManifestService) PersistNeededManifests(spec *metadata.ReplicationSpecification) error {
	agent, err := c.getAgent(spec)
	if err != nil {
		return err
	}

	srcErr, tgtErr, _, _ := agent.PersistNeededManifests()
	if srcErr != nil || tgtErr != nil {
		return fmt.Errorf("SourceManifest persist err: %v TargetManifest persist err: %v", srcErr.Error(), tgtErr.Error())
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

func (c *CollectionsManifestService) GetSourceManifestForNozzle(spec *metadata.ReplicationSpecification, vblist []uint16) *metadata.CollectionsManifest {
	c.agentsMtx.RLock()
	agent, ok := c.agentsMap[spec.Id]
	c.agentsMtx.RUnlock()
	if !ok {
		c.logger.Warnf("Unable to get agent for spec %v\n", spec.Id)
		return &defaultManifest
	}
	return agent.GetAndRecordSourceManifest(vblist)
}

func (c *CollectionsManifestService) GetTargetManifestForNozzle(spec *metadata.ReplicationSpecification, vblist []uint16) *metadata.CollectionsManifest {
	c.agentsMtx.RLock()
	agent, ok := c.agentsMap[spec.Id]
	c.agentsMtx.RUnlock()
	if !ok {
		defaultManifest := metadata.NewDefaultCollectionsManifest()
		c.logger.Warnf("Unable to get agent for spec %v\n", spec.Id)
		return &defaultManifest
	}
	return agent.GetAndRecordTargetManifest(vblist)
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
	remoteClusterSvc  service_def.RemoteClusterSvc
	checkpointsSvc    service_def.CheckpointsService
	logger            *log.CommonLogger
	utilities         utils.UtilsIface
	replicationSpec   *metadata.ReplicationSpecification
	manifestOps       service_def.CollectionsManifestOps
	srcManifestGetter AgentSrcManifestGetter
	finCh             chan bool
	metakvSvc         service_def.ManifestsService
	metadataChangeCb  base.MetadataChangeHandlerCallback

	// Last pulled manifest
	srcMtx         sync.RWMutex
	tgtMtx         sync.RWMutex
	lastSourcePull uint64
	lastTargetPull uint64
	sourceCache    ManifestsCache
	targetCache    ManifestsCache

	// Last pulled manifest by nozzles
	srcNozzleMtx     sync.RWMutex
	srcNozzlePullMap map[uint16]*metadata.CollectionsManifest
	tgtNozzleMtx     sync.RWMutex
	tgtNozzlePullMap map[uint16]*metadata.CollectionsManifest

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
	singlePersistReq       chan bool
	singlePersistResultReq chan bool
	singlePersistResp      chan AgentPersistResult
}

func NewCollectionsManifestAgent(name string,
	remoteClusterSvc service_def.RemoteClusterSvc,
	checkpointsSvc service_def.CheckpointsService,
	logger *log.CommonLogger,
	utilities utils.UtilsIface,
	replicationSpec *metadata.ReplicationSpecification,
	manifestOps service_def.CollectionsManifestOps,
	srcManifestGetter AgentSrcManifestGetter,
	metakvSvc service_def.ManifestsService,
	metadataChangeCb base.MetadataChangeHandlerCallback) *CollectionsManifestAgent {
	return &CollectionsManifestAgent{
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
		srcNozzlePullMap:       make(map[uint16]*metadata.CollectionsManifest),
		tgtNozzlePullMap:       make(map[uint16]*metadata.CollectionsManifest),
	}
}

func (a *CollectionsManifestAgent) runPeriodicRefresh() {
	ticker := time.NewTicker(base.TopologyChangeCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.finCh:
			return
		case <-ticker.C:
			oldSrc, newSrc := a.refreshSource()
			oldTgt, newTgt := a.refreshTarget()

			if newSrc != nil || newTgt != nil {
				oldPair := metadata.NewCollectionsManifestPair(oldSrc, oldTgt)
				newPair := metadata.NewCollectionsManifestPair(newSrc, newTgt)
				err := a.metadataChangeCb(a.replicationSpec.Id, oldPair, newPair)
				if err != nil {
					a.logger.Errorf("Error with callback: %v\n", err.Error())
				}
			}
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

	for {
		select {
		case <-a.finCh:
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
func (a *CollectionsManifestAgent) PersistNeededManifests() (srcErr, tgtErr error, srcUpdated, tgtUpdated bool) {
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
	srcList, tgtList, err := a.getAllManifestsUids()
	a.pruneManifests(srcList, tgtList)
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

	srcListHash, srcHashErr := srcMetaList.Sha256()
	tgtListHash, tgtHashErr := tgtMetaList.Sha256()

	a.hashMtx.Lock()
	defer a.hashMtx.Unlock()

	if srcHashErr != nil {
		a.logger.Warnf("Not storing source hash due to err: %v\n", srcHashErr)
	} else if tgtHashErr != nil {
		a.logger.Warnf("Not storing target hash due to err: %v\n", tgtHashErr)
	}

	if srcHashErr != nil || !reflect.DeepEqual(a.persistedSrcListHash, srcListHash) {
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

	if tgtHashErr != nil || !reflect.DeepEqual(a.persistedTgtListHash, tgtListHash) {
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

type ManifestsCache map[uint64]*metadata.CollectionsManifest

func (m ManifestsCache) String() string {
	var output []string
	for k, v := range m {
		output = append(output, fmt.Sprintf("%v:%v\n", k, v))
	}
	return strings.Join(output, " ")
}

func (a *CollectionsManifestAgent) loadManifestsFromMetakv() error {
	srcGet, srcErr := a.metakvSvc.GetSourceManifests(a.replicationSpec)
	tgtGet, tgtErr := a.metakvSvc.GetTargetManifests(a.replicationSpec)

	if srcErr != nil {
		a.logger.Warnf("Unable to load source from metakv: %v\n", srcErr)
	} else if srcGet != nil {
		a.srcMtx.Lock()
		a.sourceCache = make(ManifestsCache)
		for _, manifest := range *srcGet {
			if manifest == nil {
				continue
			}
			a.sourceCache[manifest.Uid()] = manifest
		}
		a.srcMtx.Unlock()
	}

	if tgtErr != nil {
		a.logger.Warnf("Unable to load target from metakv: %v\n", tgtErr)
	} else if tgtGet != nil {
		a.tgtMtx.Lock()
		a.targetCache = make(ManifestsCache)
		for _, manifest := range *tgtGet {
			if manifest == nil {
				continue
			}
			a.targetCache[manifest.Uid()] = manifest
		}
		a.tgtMtx.Unlock()
	}

	atomic.StoreUint32(&a.loadedFromMetakv, 1)
	if srcErr != nil {
		return srcErr
	} else if tgtErr != nil {
		return tgtErr
	} else {
		return nil
	}
}

// These should not block
func (a *CollectionsManifestAgent) Start() error {
	if atomic.CompareAndSwapUint32(&a.started, 0, 1) {
		err := a.loadManifestsFromMetakv()
		go a.runPeriodicRefresh()
		go a.runPersistRequestHandler()
		return err
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

func (a *CollectionsManifestAgent) GetSourceManifest() *metadata.CollectionsManifest {
	a.srcMtx.RLock()
	_, ok := a.sourceCache[a.lastSourcePull]
	if !ok {
		a.srcMtx.RUnlock()
		a.refreshSource()
		a.srcMtx.RLock()
	}
	defer a.srcMtx.RUnlock()
	return a.sourceCache[a.lastSourcePull]
}

func (a *CollectionsManifestAgent) GetAndRecordSourceManifest(vblist []uint16) *metadata.CollectionsManifest {
	manifest := a.GetSourceManifest()

	a.srcNozzleMtx.Lock()
	defer a.srcNozzleMtx.Unlock()

	for _, vb := range vblist {
		a.srcNozzlePullMap[vb] = manifest
	}

	return manifest
}

// Returns nil pair if not updated
func (a *CollectionsManifestAgent) refreshSource() (oldManifest, newManifest *metadata.CollectionsManifest) {
	var manifest *metadata.CollectionsManifest
	var ok bool
	getRetry := func() error {
		manifest = a.srcManifestGetter()
		if manifest == nil {
			// Give a empty manifest for good measure
			manifest = &metadata.CollectionsManifest{}
			return fmt.Errorf("Unable to retrieve manifest from source bucket %v\n", a.replicationSpec.SourceBucketName)
		} else {
			return nil
		}
	}
	retryErr := a.utilities.ExponentialBackoffExecutor(sourceRefreshStr, base.BucketInfoOpWaitTime, base.BucketInfoOpMaxRetry,
		base.BucketInfoOpRetryFactor, getRetry)
	if retryErr != nil {
		defaultManifest := metadata.NewDefaultCollectionsManifest()
		manifest = &defaultManifest
	}

	a.srcMtx.RLock()
	if a.lastSourcePull < manifest.Uid() || a.lastSourcePull == 0 && manifest.Uid() == 0 {
		a.srcMtx.RUnlock()
		a.srcMtx.Lock()
		oldManifest, ok = a.sourceCache[a.lastSourcePull]
		a.lastSourcePull = manifest.Uid()
		a.sourceCache[manifest.Uid()] = manifest
		if ok {
			newManifest = manifest
		}
		a.srcMtx.Unlock()
	} else {
		a.srcMtx.RUnlock()
	}
	return
}

func (a *CollectionsManifestAgent) GetTargetManifest() *metadata.CollectionsManifest {
	a.tgtMtx.RLock()
	_, ok := a.targetCache[a.lastTargetPull]
	if !ok {
		a.tgtMtx.RUnlock()
		a.refreshTarget()
		a.tgtMtx.RLock()
	}
	defer a.tgtMtx.RUnlock()
	return a.targetCache[a.lastTargetPull]
}

func (a *CollectionsManifestAgent) GetAndRecordTargetManifest(vblist []uint16) *metadata.CollectionsManifest {
	manifest := a.GetTargetManifest()

	a.tgtNozzleMtx.Lock()
	defer a.tgtNozzleMtx.Unlock()

	for _, vb := range vblist {
		a.tgtNozzlePullMap[vb] = manifest
	}

	return manifest
}

// return nils if no update
func (a *CollectionsManifestAgent) refreshTarget() (oldManifest, newManifest *metadata.CollectionsManifest) {
	var manifest *metadata.CollectionsManifest
	var err error
	var ok bool
	getRetry := func() error {
		clusterUuid := a.replicationSpec.TargetClusterUUID
		bucketName := a.replicationSpec.TargetBucketName
		manifest, err = a.remoteClusterSvc.GetManifestByUuid(clusterUuid, bucketName, false /*forceRefresh*/)
		if err != nil {
			a.logger.Errorf("RemoteClusterService GetManifest on %v for bucket %v returned %v\n", clusterUuid, bucketName, err)
			manifest = &metadata.CollectionsManifest{}
			return err
		} else {
			return nil
		}
	}
	retryErr := a.utilities.ExponentialBackoffExecutor(targetRefreshStr, base.RemoteMcRetryWaitTime, base.MaxRemoteMcRetry,
		base.RemoteMcRetryFactor, getRetry)
	if retryErr != nil {
		defaultManifest := metadata.NewDefaultCollectionsManifest()
		manifest = &defaultManifest
	}

	a.tgtMtx.RLock()
	if a.lastTargetPull < manifest.Uid() || a.lastTargetPull == 0 && manifest.Uid() == 0 {
		a.tgtMtx.RUnlock()
		a.tgtMtx.Lock()
		oldManifest, ok = a.targetCache[a.lastTargetPull]
		a.lastTargetPull = manifest.Uid()
		a.targetCache[manifest.Uid()] = manifest
		if ok {
			newManifest = manifest
		}
		a.tgtMtx.Unlock()
	} else {
		a.tgtMtx.RUnlock()
	}
	return
}

func (a *CollectionsManifestAgent) getAllManifestsUids() (srcManifestUids, tgtManifestUids []uint64, err error) {
	ckptDocs, err := a.checkpointsSvc.CheckpointsDocs(a.replicationSpec.Id)
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
			srcDedupMap[ckptRecord.SourceManifest] = true
			tgtDedupMap[ckptRecord.TargetManifest] = true
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

func (a *CollectionsManifestAgent) pruneManifests(srcList, tgtList []uint64) error {
	var srcErr []uint64
	var tgtErr []uint64
	var err error

	replacementMap := make(map[uint64]*metadata.CollectionsManifest)
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
		}
	}
	a.sourceCache = replacementMap
	a.srcMtx.Unlock()

	replacementMap = make(ManifestsCache)
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
		}
	}
	a.targetCache = replacementMap
	a.tgtMtx.Unlock()

	if len(srcErr) > 0 || len(tgtErr) > 0 {
		err = fmt.Errorf("Pruning %v, unable to find srcMmanifests %v and targetManifests %v",
			a.replicationSpec.Id, srcErr, tgtErr)
		a.logger.Warnf(err.Error())
	}
	return err
}

func (a *CollectionsManifestAgent) GetOngoingManifests(vb uint16) *metadata.CollectionsManifestPair {
	a.srcNozzleMtx.RLock()
	srcManifest, ok := a.srcNozzlePullMap[vb]
	a.srcNozzleMtx.RUnlock()
	if !ok {
		srcManifest = &defaultManifest
	}
	a.tgtNozzleMtx.RLock()
	tgtManifest, ok := a.tgtNozzlePullMap[vb]
	a.tgtNozzleMtx.RUnlock()
	if !ok {
		tgtManifest = &defaultManifest
	}
	return &metadata.CollectionsManifestPair{srcManifest, tgtManifest}
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
