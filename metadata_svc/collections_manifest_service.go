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
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/parts"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/couchbase/goxdcr/v8/utils"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

const sourceRefreshStr = "sourceRefresh"
const targetRefreshStr = "targetRefresh"

var defaultManifest metadata.CollectionsManifest = metadata.NewDefaultCollectionsManifest()
var collectionManifestCounter uint32

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
	bucketTopologySvc       service_def.BucketTopologySvc
	bucketSvcId             string
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

	peerManifestGetterMtx sync.RWMutex
	peerManifestGetter    service_def.PeerManifestsGetter

	xdcrInjDelaySec uint32
}

func NewCollectionsManifestService(remoteClusterSvc service_def.RemoteClusterSvc,
	replicationSpecSvc service_def.ReplicationSpecSvc,
	uiLogSvc service_def.UILogSvc,
	loggerCtx *log.LoggerContext,
	utilities utils.UtilsIface,
	checkpointsSvc service_def.CheckpointsService,
	xdcrTopologySvc service_def.XDCRCompTopologySvc,
	bucketTopooSvc service_def.BucketTopologySvc,
	metakvSvc service_def.ManifestsService) (*CollectionsManifestService, error) {
	svc := &CollectionsManifestService{
		remoteClusterSvc:       remoteClusterSvc,
		replicationSpecSvc:     replicationSpecSvc,
		uiLogSvc:               uiLogSvc,
		utilities:              utilities,
		checkpointsSvc:         checkpointsSvc,
		bucketTopologySvc:      bucketTopooSvc,
		bucketSvcId:            fmt.Sprintf("colManifestSvc_%v", base.GetIterationId(&collectionManifestCounter)),
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

	// Do this first as replSvc needs it
	c.replicationSpecSvc.SetManifestsGetter(c.GetLatestManifests)

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

var startupPathNotSet error = fmt.Errorf("Starting path does not have getter set")

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
		getter = NewBucketManifestGetter(spec.SourceBucketName, c, time.Duration(base.ManifestRefreshSrcInterval)*time.Second, spec)
		c.srcBucketGetters[spec.SourceBucketName] = getter
	}
	c.srcBucketGettersRefCnt[spec.SourceBucketName]++
	c.srcBucketGetterMtx.Unlock()

	// peerManifestGetter can only be set after main.go has initiated p2p handlers
	peerManifestGetter := func(specId, specInternalId string) (*metadata.CollectionsManifestPair, error) {
		c.peerManifestGetterMtx.RLock()
		defer c.peerManifestGetterMtx.RUnlock()
		if c.peerManifestGetter == nil {
			return nil, startupPathNotSet
		}
		return c.peerManifestGetter(specId, specInternalId)
	}

	c.agentsMtx.Lock()
	agent := NewCollectionsManifestAgent(spec.Id, spec.InternalId,
		c.remoteClusterSvc, c.checkpointsSvc, c.bucketTopologySvc, c.logger, c.utilities, spec,
		c, getter.GetManifest, c.metakvSvc, c.metadataChangeCb, peerManifestGetter)
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

// This implements the source side manifest getter. It will not issue REST command if the manifest UID has not changed.
func (c *CollectionsManifestService) CollectionManifestGetter(bucketName string, hasStoredManifest bool, storedManifestUid uint64, spec *metadata.ReplicationSpecification) (*metadata.CollectionsManifest, error) {
	if hasStoredManifest {
		// Check if the stored manifest UID is the same as the current one
		if notificationCh, err := c.bucketTopologySvc.SubscribeToLocalBucketFeed(spec, c.bucketSvcId); err == nil {
			notification := <-notificationCh
			currentManifestUid := notification.GetSourceCollectionManifestUid()
			notification.Recycle()
			c.bucketTopologySvc.UnSubscribeLocalBucketFeed(spec, c.bucketSvcId)
			if currentManifestUid == storedManifestUid {
				return nil, base.ErrorCollectionManifestNotChanged
			}
		}
	}

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
	} else if newSpec != nil {
		// replSpecChange
		delaySec := newSpec.Settings.GetIntSettingValue(metadata.DevColManifestSvcDelaySec)
		portSpecifier := newSpec.Settings.GetIntSettingValue(metadata.DevNsServerPortSpecifier)
		if delaySec > 0 {
			if portSpecifier > 0 {
				myhostAddr, _ := c.xdcrTopologySvc.MyHostAddr()
				myPortNumber, _ := base.GetPortNumber(myhostAddr)
				// If err, myPortNumber is 0 and won't match portSpecifier anyway
				if int(myPortNumber) == portSpecifier {
					c.logger.Infof("XDCR Dev Injection for this node %v set delay to %v seconds", portSpecifier, delaySec)
					atomic.StoreUint32(&c.xdcrInjDelaySec, uint32(delaySec))
				}
			} else {
				c.logger.Infof("XDCR Dev Injection set delay to %v seconds", delaySec)
			}
		} else if delaySec == 0 {
			atomic.StoreUint32(&c.xdcrInjDelaySec, 0)
		}
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

func (c *CollectionsManifestService) GetStartingManifests(spec *metadata.ReplicationSpecification) (src, tgt *metadata.CollectionsManifest, err error) {
	manifestsPair, err := c.peerManifestGetter(spec.Id, spec.InternalId)
	if err != nil {
		// It is potentially possible target doesn't support collections
		origErr := err

		ref, err := c.remoteClusterSvc.RemoteClusterByUuid(spec.TargetClusterUUID, false)
		if err != nil {
			return nil, nil, err
		}

		rcCapability, err := c.remoteClusterSvc.GetCapability(ref)
		if err != nil {
			return nil, nil, err
		}

		if !rcCapability.HasCollectionSupport() {
			return nil, nil, base.ErrorTargetCollectionsNotSupported
		} else {
			return nil, nil, origErr
		}
	} else if manifestsPair == nil {
		return nil, nil, fmt.Errorf("GetStartingManifests with %v-%v has empty manifestsPair",
			spec.Id, spec.InternalId)
	} else if manifestsPair.Source == nil {
		return nil, nil, fmt.Errorf("GetStartingManifests with %v-%v has empty source manifest",
			spec.Id, spec.InternalId)
	} else if manifestsPair.Target == nil {
		return nil, nil, fmt.Errorf("GetStartingManifests with %v-%v has empty target manifest",
			spec.Id, spec.InternalId)
	}

	return manifestsPair.Source, manifestsPair.Target, nil
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
		tempBucketManifestGetter := NewBucketManifestGetter(spec.SourceBucketName, c, time.Duration(base.ManifestRefreshSrcInterval)*time.Second, spec)

		c.waitForManifestGetter()
		c.peerManifestGetterMtx.RLock()
		peerManifestGetter := c.peerManifestGetter
		c.peerManifestGetterMtx.RUnlock()

		agent = NewCollectionsManifestAgent(spec.Id, spec.InternalId,
			c.remoteClusterSvc, c.checkpointsSvc, c.bucketTopologySvc, c.logger, c.utilities, spec,
			c, tempBucketManifestGetter.GetManifest, c.metakvSvc, nil, peerManifestGetter)
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
	var agent *CollectionsManifestAgent
	var err error
	getAgentOp := func() error {
		var innerErr error
		agent, innerErr = c.getAgent(spec)
		return innerErr
	}
	err = c.utilities.ExponentialBackoffExecutor(fmt.Sprintf("GetLastPersistedManifests.getAgent(%v)", spec.Id),
		base.RetryIntervalMetakv, base.MaxNumOfMetakvRetries, base.MetaKvBackoffFactor, getAgentOp)
	if err != nil {
		return nil, err
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
	if delaySec := atomic.LoadUint32(&c.xdcrInjDelaySec); delaySec > 0 {
		c.logger.Infof("XDCR Dev Injection Manifest pair callback notify force delay of %v seconds", delaySec)
		time.Sleep(time.Duration(delaySec) * time.Second)
	}

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

// When replication starts, it needs to request specific manifest by version, such as resuming from checkpoint
func (c *CollectionsManifestService) GetSpecificSourceManifest(spec *metadata.ReplicationSpecification, manifestVersion uint64) (*metadata.CollectionsManifest, error) {
	c.agentsMtx.RLock()
	agent, ok := c.agentsMap[spec.Id]
	c.agentsMtx.RUnlock()
	if !ok {
		return nil, fmt.Errorf("Unable to find agent for spec %v\n", spec.Id)
	}
	return agent.GetSpecificSourceManifest(manifestVersion)
}

// When replication starts, it needs to request specific manifest by version, such as resuming from checkpoint
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

func (c *CollectionsManifestService) GetAllCachedManifests(spec *metadata.ReplicationSpecification) (map[uint64]*metadata.CollectionsManifest, map[uint64]*metadata.CollectionsManifest, error) {
	c.agentsMtx.RLock()
	agent, ok := c.agentsMap[spec.Id]
	c.agentsMtx.RUnlock()
	if !ok {
		return nil, nil, fmt.Errorf("Unable to find agent for spec %v\n", spec.Id)
	}
	return agent.GetAllCachedManifests()
}

func (c *CollectionsManifestService) PersistReceivedManifests(spec *metadata.ReplicationSpecification, srcManifests, tgtManifests map[uint64]*metadata.CollectionsManifest) error {
	c.agentsMtx.RLock()
	agent, ok := c.agentsMap[spec.Id]
	c.agentsMtx.RUnlock()
	if !ok {
		return fmt.Errorf("Unable to find agent for spec %v\n", spec.Id)
	}
	return agent.PersistReceivedManifests(srcManifests, tgtManifests)
}

func (c *CollectionsManifestService) SetPeerManifestsGetter(getter service_def.PeerManifestsGetter) {
	c.peerManifestGetterMtx.Lock()
	defer c.peerManifestGetterMtx.Unlock()
	c.peerManifestGetter = getter
}

func (c *CollectionsManifestService) waitForManifestGetter() {
	var manifestGetterFound bool
	for !manifestGetterFound {
		c.peerManifestGetterMtx.RLock()
		manifestGetterFound = c.peerManifestGetter != nil
		c.peerManifestGetterMtx.RUnlock()
		if !manifestGetterFound {
			time.Sleep(time.Duration(base.ManifestsGetterSleepTimeSecs) * time.Second)
		}
	}
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
	internalId        string
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
	remoteClusterSvc  service_def.RemoteClusterSvc
	bucketTopologySvc service_def.BucketTopologySvc
	bucketSvcId       string

	remoteClusterRefPopulated uint32
	remoteClusterRef          *metadata.RemoteClusterReference

	// Last pulled manifest
	srcMtx         sync.RWMutex
	tgtMtx         sync.RWMutex
	lastSourcePull uint64
	lastTargetPull uint64
	sourceCache    metadata.ManifestsCache
	targetCache    metadata.ManifestsCache
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
	srcLoadedFromMetakv    bool
	tgtLoadedFromMetaKv    bool
	metakvInitDone         chan bool
	refreshAndNotifyIsRdy  chan bool
	persistHandlerStarted  uint32
	singlePersistReq       chan bool
	singlePersistResultReq chan bool
	singlePersistResp      chan AgentPersistResult

	// If agent is used as part of spec creation/validation process, then this is considered a tempAgent
	tempAgent bool

	peerManifestGetter service_def.PeerManifestsGetter
}

func NewCollectionsManifestAgent(name string, internalId string, remoteClusterSvc service_def.RemoteClusterSvc, checkpointsSvc service_def.CheckpointsService, bucketTopoSvc service_def.BucketTopologySvc, logger *log.CommonLogger, utilities utils.UtilsIface, replicationSpec *metadata.ReplicationSpecification, manifestOps service_def.CollectionsManifestOps, srcManifestGetter AgentSrcManifestGetter, metakvSvc service_def.ManifestsService, metadataChangeCb base.MetadataChangeHandlerCallback, peerManifestsGetter service_def.PeerManifestsGetter) *CollectionsManifestAgent {
	manifestAgent := &CollectionsManifestAgent{
		id:                     name,
		internalId:             internalId,
		remoteClusterSvc:       remoteClusterSvc,
		checkpointsSvc:         checkpointsSvc,
		bucketTopologySvc:      bucketTopoSvc,
		bucketSvcId:            fmt.Sprintf("colManifestAgt_%v", base.GetIterationId(&collectionManifestCounter)),
		logger:                 logger,
		utilities:              utilities,
		replicationSpec:        replicationSpec,
		manifestOps:            manifestOps,
		srcManifestGetter:      srcManifestGetter,
		finCh:                  make(chan bool, 1),
		ckptDocsCache:          make(map[uint16]*metadata.CheckpointsDoc),
		sourceCache:            make(metadata.ManifestsCache),
		targetCache:            make(metadata.ManifestsCache),
		metakvSvc:              metakvSvc,
		metadataChangeCb:       metadataChangeCb,
		singlePersistReq:       make(chan bool, 1),
		singlePersistResultReq: make(chan bool, 10),
		singlePersistResp:      make(chan AgentPersistResult, 11),
		metakvInitDone:         make(chan bool),
		refreshAndNotifyIsRdy:  make(chan bool),
		peerManifestGetter:     peerManifestsGetter,
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

// Similar to refreshAndNotify in nature, but without actual pulling
func (a *CollectionsManifestAgent) registerPeerSentManifests(pair *metadata.CollectionsManifestPair) {

	// With peer sent manifests, this case can only happen during replication creation in a limited time window
	// This means there should not be a pre-existing replication before this call
	// As such, we want to make sure the "baseline" is the actual baseline manifest that was sent
	// so that we don't un-necessarily raise backfill which could cause extra un-necessary work to be done

	a.registerSourcePull(true, pair.Source)
	a.registerTargetPull(true, pair.Target)
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
		a.logger.Errorf("%v - Error with callback: %v\n", a.id, err.Error())
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
	if err != nil {
		srcErr = err
		tgtErr = err
		return
	}
	// Clean up agent's current manifest storage to get rid of unreferred sourcelist
	err = a.cleanupUnreferredManifests(srcList, tgtList)
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
		a.logger.Warnf("%v - Not storing source hash due to err: %v\n", a.id, srcHashErr)
	} else if tgtHashErr != nil {
		a.logger.Warnf("%v - Not storing target hash due to err: %v\n", a.id, tgtHashErr)
	}

	if srcHashErr == nil && !reflect.DeepEqual(a.persistedSrcListHash, srcListHash) {
		// metakv is out of date
		srcErr = a.metakvSvc.UpsertSourceManifests(a.replicationSpec, &srcMetaList)
		if srcErr != nil {
			a.logger.Warnf("%v - Upserting source resulted in err: %v\n", a.id, srcErr)
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
			a.logger.Warnf("%v - Upserting target resulted in err: %v\n", a.id, tgtErr)
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

func (a *CollectionsManifestAgent) loadManifestsFromMetakv() (srcErr, tgtErr error) {
	defer func() {
		close(a.metakvInitDone)
	}()
	srcGet, srcErr := a.metakvSvc.GetSourceManifests(a.replicationSpec)
	tgtGet, tgtErr := a.metakvSvc.GetTargetManifests(a.replicationSpec)

	if srcErr != nil && srcErr != service_def.MetadataNotFoundErr {
		a.logger.Warnf("%v - Unable to load source from metakv: %v\n", a.id, srcErr)
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
		a.sourceCache = make(metadata.ManifestsCache)
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
		a.logger.Warnf("%v - Unable to load target from metakv: %v\n", a.id, tgtErr)
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
		a.targetCache = make(metadata.ManifestsCache)

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

	if srcErr == nil {
		a.srcLoadedFromMetakv = true
	}
	if tgtErr == nil {
		a.tgtLoadedFromMetaKv = true
	}

	a.srcMtx.Lock()
	if a.sourceCache[0] == nil {
		a.sourceCache[0] = &defaultManifest
	}
	a.srcMtx.Unlock()

	a.tgtMtx.Lock()
	if a.targetCache[0] == nil {
		a.targetCache[0] = &defaultManifest
	}
	a.tgtMtx.Unlock()

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
		var refreshImmediately bool
		var peerSentManifests bool
		var manifestsPair *metadata.CollectionsManifestPair
		var err error

		if a.tempAgent {
			err := a.populateRemoteClusterRefOnce()
			if err != nil {
				return fmt.Errorf("Temp agent %v unable to populate remote cluster reference %v", a.id, err)
			}
			refreshImmediately = true
		} else {
			var loadPeersManifest bool

			// If manifests are found from metakv, this means that the replication has already been started
			// and checkpoints already exist
			srcErr, tgtErr := a.loadManifestsFromMetakv()
			if srcErr == service_def.MetadataNotFoundErr {
				srcErr = nil
				loadPeersManifest = true
			}
			if tgtErr == service_def.MetadataNotFoundErr {
				tgtErr = nil
				loadPeersManifest = true
			}

			if loadPeersManifest {
				// Otherwise, replication could be created at this time
				// Try loading to see if peer has sent us manifests as part of replication creation
				// If so, no need to load refresh immediately
				manifestsPair, err = a.peerManifestGetter(a.id, a.internalId)
				if err != nil || manifestsPair == nil || manifestsPair.Source == nil || manifestsPair.Target == nil {
					refreshImmediately = true
				} else {
					peerSentManifests = true
				}
			}

			if srcErr != nil || tgtErr != nil {
				a.logger.Warnf("%v - starting sourceErr: %v tgtErr: %v", a.replicationSpec.Id, srcErr, tgtErr)
				// Do not return error here as colletions must continue to work
			}
			go a.populateRemoteClusterRefOnce()
			go a.runPeriodicRefresh()
			go a.runPersistRequestHandler()
		}

		if peerSentManifests {
			a.registerPeerSentManifests(manifestsPair)
		} else {
			a.refreshAndNotify(refreshImmediately)
		}
		close(a.refreshAndNotifyIsRdy)
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

	<-a.refreshAndNotifyIsRdy

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
				err = fmt.Errorf("Unable to provide requested version %v even after emergency pull. Last pulled version: %v",
					manifestVersion, a.lastSourcePull)
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
	if lock {
		a.srcMtx.RLock()
	}
	srcCacheLen := len(a.sourceCache)
	lastSrcPullChk := a.lastSourcePull
	if lock {
		a.srcMtx.RUnlock()
	}
	if srcCacheLen > 0 {
		// If sourceCache has been initialized, we only need to refresh if manifestUid has changed
		currentManifestUid, err := a.getCurrentCollectionManifestUid()
		if err == nil && currentManifestUid == lastSrcPullChk {
			return nil, nil, base.ErrorCollectionManifestNotChanged
		}
	}

	var manifest *metadata.CollectionsManifest
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
		a.logger.Errorf("%v - refreshSource err: %v\n", a.id, err)
		if lock {
			a.srcMtx.Lock()
			defer a.srcMtx.Unlock()
		}
		maxID := a.sourceCache.GetMaxManifestID()
		if maxID < a.lastSourcePull {
			a.logger.Warnf("%v - %v is unable to pull a higher source manifest, going back in time from %v to %v", a.id, a.lastSourcePull, maxID)
		}
		a.lastSourcePull = maxID
		return
	}

	oldManifest, newManifest = a.registerSourcePull(lock, manifest)
	return
}

func (a *CollectionsManifestAgent) registerSourcePull(lock bool, manifest *metadata.CollectionsManifest) (*metadata.CollectionsManifest, *metadata.CollectionsManifest) {
	var ok bool
	var oldManifest *metadata.CollectionsManifest
	var newManifest *metadata.CollectionsManifest

	if lock {
		a.srcMtx.RLock()
	}
	// It is possible that we pulled a default collection and lastSourcePull is initialized at 0
	if a.lastSourcePull < manifest.Uid() || len(a.sourceCache) == 0 {
		if lock {
			a.srcMtx.RUnlock()
			a.srcMtx.Lock()
		}
		a.logger.Infof("%v - Updated source manifest from old version %v to new version %v\n", a.id, a.lastSourcePull, manifest.Uid())
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
	return oldManifest, newManifest
}

func (a *CollectionsManifestAgent) getCurrentCollectionManifestUid() (uint64, error) {
	notificationCh, err := a.bucketTopologySvc.SubscribeToLocalBucketFeed(a.replicationSpec, a.bucketSvcId)
	if err != nil {
		return 0, err
	}
	defer a.bucketTopologySvc.UnSubscribeLocalBucketFeed(a.replicationSpec, a.bucketSvcId)
	notification := <-notificationCh
	defer notification.Recycle()
	manifestUid := notification.GetSourceCollectionManifestUid()
	return manifestUid, nil
}
func (a *CollectionsManifestAgent) GetTargetManifest() (*metadata.CollectionsManifest, error) {
	if a.isStopped() {
		return nil, parts.PartStoppedError
	}

	<-a.refreshAndNotifyIsRdy

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
	getRetry := func() error {
		clusterUuid := a.replicationSpec.TargetClusterUUID
		bucketName := a.replicationSpec.TargetBucketName
		manifest, err = a.remoteClusterSvc.GetManifestByUuid(clusterUuid, bucketName, force, a.tempAgent)
		if err != nil && err != ErrorRemoteClusterNoCollectionsCapability {
			a.logger.Errorf("%v - RemoteClusterService GetManifest on %v for bucket %v returned %v\n", a.id, clusterUuid, bucketName, err)
			return err
		}
		return nil
	}
	err = a.utilities.ExponentialBackoffExecutor(targetRefreshStr, waitTime, maxRetry, base.RemoteMcRetryFactor, getRetry)

	if err != nil || manifest == nil {
		a.logger.Errorf("%v - refreshTarget returned err: %v\n", a.id, err)
		if lock {
			a.tgtMtx.Lock()
			defer a.tgtMtx.Unlock()
		}
		maxID := a.targetCache.GetMaxManifestID()
		if maxID < a.lastTargetPull {
			a.logger.Warnf("%v - %v is unable to pull a higher target manifest, going back in time from %v to %v", a.id, a.lastTargetPull, maxID)
		}
		a.lastTargetPull = maxID
		return
	}

	oldManifest, newManifest = a.registerTargetPull(lock, manifest)
	return
}

func (a *CollectionsManifestAgent) registerTargetPull(lock bool, manifest *metadata.CollectionsManifest) (*metadata.CollectionsManifest, *metadata.CollectionsManifest) {
	var oldManifest *metadata.CollectionsManifest
	var newManifest *metadata.CollectionsManifest
	var ok bool

	if lock {
		a.tgtMtx.RLock()
	}
	// It is possible that we pulled a default collection and lastTargetPull is initialized at 0
	if a.lastTargetPull < manifest.Uid() || len(a.targetCache) == 0 {
		if lock {
			a.tgtMtx.RUnlock()
			a.tgtMtx.Lock()
		}
		a.logger.Infof("%v -  Updated target manifest from old version %v to new version %v\n", a.id, a.lastTargetPull, manifest.Uid())
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
	return oldManifest, newManifest
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
		replacementMap := make(metadata.ManifestsCache)
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
		replacementMap := make(metadata.ManifestsCache)
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
		a.logger.Warnf("%v - %v", a.id, err.Error())
	}
	return err
}

// Returns error if neither source nor target were persisted
// Otherwise, return a pair of which if it was persisted, the member would be non-nil
func (a *CollectionsManifestAgent) GetLastPersistedManifests() (*metadata.CollectionsManifestPair, error) {
	<-a.metakvInitDone
	if !a.srcLoadedFromMetakv && !a.tgtLoadedFromMetaKv {
		return nil, fmt.Errorf("no manifests exist in metakv")
	} else if a.isStopped() {
		return nil, parts.PartStoppedError
	}

	retVal := &metadata.CollectionsManifestPair{}
	if a.srcLoadedFromMetakv {
		a.srcMtx.RLock()
		srcManifest, ok := a.sourceCache[a.lastSourceStoredManifest]
		localLastStored := a.lastSourceStoredManifest
		a.srcMtx.RUnlock()
		if !ok {
			if localLastStored > 0 {
				// Shouldn't be the case
				return nil, fmt.Errorf("Cannot find source manifest version %v", localLastStored)
			} else {
				srcManifest = &defaultManifest
			}
		}
		retVal.Source = srcManifest
	}

	if a.tgtLoadedFromMetaKv {
		a.tgtMtx.RLock()
		tgtManifest, ok := a.targetCache[a.lastTargetStoredManifest]
		localLastStored := a.lastTargetStoredManifest
		a.tgtMtx.RUnlock()
		if !ok {
			if localLastStored > 0 {
				// Shouldn't be the case
				return nil, fmt.Errorf("Cannot find manifest version %v", localLastStored)
			} else {
				tgtManifest = &defaultManifest
			}
		}
		retVal.Target = tgtManifest
	}

	return retVal, nil
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

func (a *CollectionsManifestAgent) GetAllCachedManifests() (map[uint64]*metadata.CollectionsManifest, map[uint64]*metadata.CollectionsManifest, error) {
	a.srcMtx.RLock()
	srcCache := a.sourceCache.Clone()
	a.srcMtx.RUnlock()
	if srcCache == nil {
		newSrc := make(metadata.ManifestsCache)
		srcCache = &newSrc
	}

	a.tgtMtx.RLock()
	tgtCache := a.targetCache.Clone()
	a.tgtMtx.RUnlock()
	if tgtCache == nil {
		newTgt := make(metadata.ManifestsCache)
		tgtCache = &newTgt
	}

	return *srcCache, *tgtCache, nil
}

// Should only be called after corresponding checkpoints that refer to these manifests have been persisted
// Because persisting will go through GC and clean up any manifests not referred by any checkpoints
func (a *CollectionsManifestAgent) PersistReceivedManifests(srcManifests map[uint64]*metadata.CollectionsManifest, tgtManifests map[uint64]*metadata.CollectionsManifest) error {
	a.srcMtx.Lock()
	for srcManifestId, srcManifest := range srcManifests {
		_, exists := a.sourceCache[srcManifestId]
		if !exists {
			a.sourceCache[srcManifestId] = srcManifest
		}
	}
	a.srcMtx.Unlock()

	a.tgtMtx.Lock()
	for tgtManifestId, tgtManifest := range tgtManifests {
		_, exists := a.targetCache[tgtManifestId]
		if !exists {
			a.targetCache[tgtManifestId] = tgtManifest
		}
	}
	a.tgtMtx.Unlock()

	srcErr, tgtErr, _, _ := a.PersistNeededManifests()
	if srcErr != nil || tgtErr != nil {
		return fmt.Errorf("srcErr %v tgtErr %v", srcErr, tgtErr)
	}

	return nil
}
