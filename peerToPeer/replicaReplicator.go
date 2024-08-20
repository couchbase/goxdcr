// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package peerToPeer

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type ReplicaReplicator interface {
	Start()
	Stop()
	HandleSpecCreation(spec *metadata.ReplicationSpecification)
	HandleSpecDeletion(oldSpec *metadata.ReplicationSpecification)
	HandleSpecChange(oldSpec *metadata.ReplicationSpecification, newSpec *metadata.ReplicationSpecification)
	RequestImmediatePush(replId string) error
}

type ReplicaReplicatorImpl struct {
	bucketTopologySvc  service_def.BucketTopologySvc
	ckptSvc            service_def.CheckpointsService
	backfillReplSvc    service_def.BackfillReplSvc
	colManifestSvc     service_def.CollectionsManifestSvc
	replicationSpecSvc service_def.ReplicationSpecSvc

	utils          utilities.UtilsIface
	reverseMapPool *utilities.KvVbMapPool

	unitTest bool

	startOnce      sync.Once
	finCh          chan bool
	tickerReloadCh chan replicatorReloadPair

	logger       *log.CommonLogger
	replicaCache ReplicaCache

	agentMap    map[string]*ReplicatorAgentImpl // key is specId
	agentMapMtx sync.RWMutex

	minIntervalMtx sync.Mutex
	minInterval    time.Duration

	sendReqFunc func(reqs PeersVBPeriodicReplicateReqs) error
}

const moduleName = "ReplicaReplicator"

func NewReplicaReplicator(bucketTopologySvc service_def.BucketTopologySvc, loggerCtx *log.LoggerContext, ckptSvc service_def.CheckpointsService, backfillReplSvc service_def.BackfillReplSvc, utils utilities.UtilsIface, collectionsManifestSvc service_def.CollectionsManifestSvc, replicationSpecSvc service_def.ReplicationSpecSvc, sendReqsFunc func(reqs PeersVBPeriodicReplicateReqs) error) *ReplicaReplicatorImpl {
	return &ReplicaReplicatorImpl{
		bucketTopologySvc:  bucketTopologySvc,
		logger:             log.NewLogger(moduleName, loggerCtx),
		agentMap:           map[string]*ReplicatorAgentImpl{},
		replicaCache:       NewReplicaCache(bucketTopologySvc, loggerCtx),
		ckptSvc:            ckptSvc,
		backfillReplSvc:    backfillReplSvc,
		replicationSpecSvc: replicationSpecSvc,
		utils:              utils,
		colManifestSvc:     collectionsManifestSvc,
		finCh:              make(chan bool),
		tickerReloadCh:     make(chan replicatorReloadPair, 50 /* not likely but give it a buffer */),
		minInterval:        time.Duration(metadata.ReplicateCkptIntervalConfig.MaxValue) * time.Minute,
		sendReqFunc:        sendReqsFunc,
		reverseMapPool:     utilities.NewKvVbMapPool(),
	}
}

type replicatorReloadPair struct {
	interval time.Duration
	isNew    bool
}

func (r *ReplicaReplicatorImpl) HandleSpecCreation(newSpec *metadata.ReplicationSpecification) {
	r.replicaCache.HandleSpecCreation(newSpec)
	// When specs are created or XDCR boots up, they all start together
	// Since replicator operations are heavy as it hits metakv and also other nodes
	// it is better to add randomnized start delay so they all don't hit at once
	randSecInt := rand.Int() % 60 // Random sec to add up to 60 seconds
	if r.unitTest {
		randSecInt = 0
	}
	agent := NewReplicatorAgent(newSpec, r.logger, r.utils, r.ckptSvc, r.backfillReplSvc, r.colManifestSvc, randSecInt,
		r.replicationSpecSvc, newSpec.InternalId, r.bucketTopologySvc)
	r.agentMapMtx.Lock()
	r.agentMap[newSpec.Id] = agent
	r.agentMapMtx.Unlock()
	agent.Start()

	incomingInterval := newSpec.Settings.GetReplicateCkptInterval()
	r.checkAndUpdateTicker(incomingInterval, 0)
}

func (r *ReplicaReplicatorImpl) GetAgent(specId string) (ReplicatorAgent, error) {
	r.agentMapMtx.RLock()
	defer r.agentMapMtx.RUnlock()

	agent, found := r.agentMap[specId]
	if !found {
		return nil, base.ErrorNotFound
	}
	return agent, nil
}

// oldSpecDuration was the value of a spec changed or deleted... otherwise 0 if this fn is being called as part of spec creation
func (r *ReplicaReplicatorImpl) checkAndUpdateTicker(potentialMinInterval time.Duration, oldSpecDuration time.Duration) {
	var needToReload bool
	r.minIntervalMtx.Lock()
	if oldSpecDuration == 0 {
		// new spec, then potentialMinInterval is the interval of the new incoming spec
		if potentialMinInterval < r.minInterval {
			r.minInterval = potentialMinInterval
			needToReload = true
		}
	} else {
		// non new spec, then potentialMinInterval is the minInterval of all currently existing specs
		if oldSpecDuration == r.minInterval && potentialMinInterval != oldSpecDuration {
			r.minInterval = potentialMinInterval
			needToReload = true
		}
	}
	r.minIntervalMtx.Unlock()

	if needToReload {
		pair := replicatorReloadPair{
			interval: potentialMinInterval,
			isNew:    oldSpecDuration == 0,
		}
		r.tickerReloadCh <- pair
	}
}

func (r *ReplicaReplicatorImpl) HandleSpecDeletion(oldSpec *metadata.ReplicationSpecification) {
	r.replicaCache.HandleSpecDeletion(oldSpec)

	r.agentMapMtx.Lock()
	agent := r.agentMap[oldSpec.Id]
	delete(r.agentMap, oldSpec.Id)
	atLeastOneLeft := len(r.agentMap) > 0
	r.agentMapMtx.Unlock()

	if agent != nil {
		agent.Stop()
	}

	if atLeastOneLeft {
		r.refreshIntervalTicker(oldSpec.Settings.GetReplicateCkptInterval())
	}
}

// Go through the all the agents and make the replicator's interval the min of
// all the agents' intervals
func (r *ReplicaReplicatorImpl) refreshIntervalTicker(oldSpecDuration time.Duration) {
	minIntervalInExistence := time.Duration(metadata.ReplicateCkptIntervalConfig.MaxValue) * time.Minute

	r.minIntervalMtx.Lock()
	curInterval := r.minInterval
	r.minIntervalMtx.Unlock()

	r.agentMapMtx.RLock()
	for _, checkAgent := range r.agentMap {
		oneInterval, err := checkAgent.GetReplicationInterval()
		if err != nil {
			r.logger.Errorf("refreshIntervalTicker err with agent %v - %v", checkAgent.specId, err)
			continue
		}
		if oneInterval < minIntervalInExistence {
			minIntervalInExistence = oneInterval
		}
	}
	r.agentMapMtx.RUnlock()

	if minIntervalInExistence != curInterval {
		r.checkAndUpdateTicker(minIntervalInExistence, oldSpecDuration)
	}
}

func (r *ReplicaReplicatorImpl) HandleSpecChange(oldSpec, newSpec *metadata.ReplicationSpecification) {
	if newSpec.Id != oldSpec.Id {
		r.logger.Errorf("SpecID mismatch - %v vs %v", oldSpec.Id, newSpec.Id)
		return
	}
	specId := newSpec.Id
	r.agentMapMtx.RLock()
	agent := r.agentMap[specId]
	r.agentMapMtx.RUnlock()
	if agent == nil {
		r.logger.Errorf("Unable to find agent with spec %v", specId)
		return
	}

	oldInterval := oldSpec.Settings.GetReplicateCkptInterval()
	cbFunc := func() {
		r.refreshIntervalTicker(oldInterval)
	}
	agent.SetUpdatedSpecAsync(newSpec, cbFunc)
}

func (r *ReplicaReplicatorImpl) Start() {
	r.startOnce.Do(func() {
		go r.run()
	})
}

func (r *ReplicaReplicatorImpl) Stop() {
	close(r.finCh)
}

func (r *ReplicaReplicatorImpl) run() {
	// Will start off with a long period and then have it set to the minimum interval as needed
	ticker := time.NewTicker(base.ReplicateCkptInterval)
	tickerHasNotFiredBefore := true
	for {
		select {
		case <-r.finCh:
			return
		case <-ticker.C:
			if tickerHasNotFiredBefore {
				// If ticker has not fired before, it is possible that specs are being added and/or changed
				// This includes when system starts up
				// When agents are first launched during start-up, they will introduce one minute of staggered startup
				// time to prevent overloading metakv
				// So over here, we will give it one minute + a few seconds for good measure before starting the
				// pull-and-send routines
				time.Sleep(65 * time.Second)
				tickerHasNotFiredBefore = false
			}
			err := r.pullAndSend()
			if err != nil {
				r.logger.Errorf("Unable to do periodic ckpt replicate this time around due to %v", err)
			}
		case timeBoolPair := <-r.tickerReloadCh:
			if timeBoolPair.isNew {
				r.logger.Infof("Initialized ticker to %v", timeBoolPair.interval)
			} else {
				r.logger.Infof("Updated ticker to %v", timeBoolPair.interval)
			}
			ticker.Reset(timeBoolPair.interval)
			tickerHasNotFiredBefore = timeBoolPair.isNew
		}
	}
}

func (r *ReplicaReplicatorImpl) pullAndSend() error {
	populateMap := r.populateInformationFromAgents()
	if len(populateMap) == 0 {
		// Nothing to send
		return nil
	}

	toSendMap, errMap := r.reOrganizePopulateMap(populateMap)
	if len(errMap) > 0 {
		return errors.New(base.FlattenErrorMap(errMap))
	}

	return r.sendReqFunc(toSendMap)
}

func (r *ReplicaReplicatorImpl) pullAndSendSpecific(replId string) error {
	populateMap, err := r.populateInformationFromOneAgent(replId)
	if err != nil {
		return err
	}
	if len(populateMap) == 0 {
		r.logger.Warnf("PullAndSendSpecific(%v) called but nothing is there to send", replId)
		return nil
	}

	toSendMap, errMap := r.reOrganizePopulateMap(populateMap)
	if len(errMap) > 0 {
		return errors.New(base.FlattenErrorMap(errMap))
	}

	return r.sendReqFunc(toSendMap)
}

func (r *ReplicaReplicatorImpl) populateInformationFromAgents() map[*metadata.ReplicationSpecification]*VBPeriodicReplicateReq {
	populateMap := make(map[*metadata.ReplicationSpecification]*VBPeriodicReplicateReq)
	r.agentMapMtx.RLock()
	for _, agent := range r.agentMap {
		specClone, infoToSerialize, err := agent.GetAndClearInfoToReplicate()
		if err != nil || infoToSerialize == nil || infoToSerialize.IsEmpty() || infoToSerialize.PushReqIsEmpty() {
			continue
		}
		populateMap[specClone] = infoToSerialize
	}
	r.agentMapMtx.RUnlock()
	return populateMap
}

func (r *ReplicaReplicatorImpl) populateInformationFromOneAgent(replId string) (map[*metadata.ReplicationSpecification]*VBPeriodicReplicateReq, error) {
	agent, err := r.GetAgent(replId)
	if err != nil {
		return nil, err
	}

	specClone, infoToSerialize, err := agent.GetAndClearInfoToReplicate()
	if err != nil {
		return nil, err
	}
	populateMap := make(map[*metadata.ReplicationSpecification]*VBPeriodicReplicateReq)
	populateMap[specClone] = infoToSerialize
	return populateMap, nil
}

func (r *ReplicaReplicatorImpl) reOrganizePopulateMap(specToReqMap map[*metadata.ReplicationSpecification]*VBPeriodicReplicateReq) (PeersVBPeriodicReplicateReqs, base.ErrorMap) {
	peerNodeToRequestListMap := make(PeersVBPeriodicReplicateReqs)
	errMap := make(base.ErrorMap)

	for spec, reqForSpec := range specToReqMap {
		replicaCnt, replicaMap, replicaTranslateMap, unlockFunc, err := r.replicaCache.GetReplicaInfo(spec)
		if err != nil {
			errMap[spec.Id] = fmt.Errorf("getting replicaInfo - %v", err)
			continue
		}

		if replicaCnt == 0 {
			unlockFunc()
			continue
		}

		nodeVBOwnership := base.ReverseVBNodesMap(*replicaMap, r.reverseMapPool.Get)
		for nodeKVName, vbsList := range *nodeVBOwnership {
			nodeNsServerName, nsServerFound := (*replicaTranslateMap)[nodeKVName]
			if !nsServerFound {
				errMap[spec.Id] = fmt.Errorf("ns_server entry not found given %v", nodeKVName)
				continue
			}

			subReq := reqForSpec.GetSubsetBasedOnVBList(vbsList)
			if peerNodeToRequestListMap[nodeNsServerName] == nil {
				peerNodeToRequestListMap[nodeNsServerName] = &VBPeriodicReplicateReqList{}
			}

			*peerNodeToRequestListMap[nodeNsServerName] = append(*peerNodeToRequestListMap[nodeNsServerName], subReq)
		}
		r.reverseMapPool.Put(nodeVBOwnership)
		unlockFunc()
	}

	return peerNodeToRequestListMap, errMap
}

func (r *ReplicaReplicatorImpl) RequestImmediatePush(replId string) error {
	agent, err := r.GetAgent(replId)
	if err != nil {
		return err
	}
	fetchErr := agent.RequestImmediateDataGather()
	if fetchErr != nil {
		return fetchErr
	}

	return r.pullAndSendSpecific(replId)
}

type ReplicatorAgent interface {
	Start()
	Stop()
	SetUpdatedSpecAsync(spec *metadata.ReplicationSpecification, cbFunc func())
	GetAndClearInfoToReplicate() (*metadata.ReplicationSpecification, *VBPeriodicReplicateReq, error)
	RequestImmediateDataGather() error
}

type ReplicatorAgentImpl struct {
	logger    *log.CommonLogger
	startOnce sync.Once
	initDelay time.Duration
	utils     utilities.UtilsIface
	isRunning uint32

	ckptSvc           service_def.CheckpointsService
	backfillReplSvc   service_def.BackfillReplSvc
	colManifestSvc    service_def.CollectionsManifestSvc
	replSpecSvc       service_def.ReplicationSpecSvc
	bucketTopologySvc service_def.BucketTopologySvc

	spec          *metadata.ReplicationSpecification
	specId        string
	srcBucketName string
	internalId    string
	bucketSvcId   string

	finCh            chan bool
	reloadCh         chan agentReloadPair
	immediateFetchCh chan chan error

	latestInfoMtx sync.Mutex
	latestInfo    *VBPeriodicReplicateReq

	sourceBucketTopologyCh chan service_def.SourceNotification

	latestCachedSourceNotification    service_def.SourceNotification
	latestCachedSourceNotificationMtx sync.RWMutex
	latestVBs                         []uint16
}

type agentReloadPair struct {
	newTimeInterval time.Duration
	callbackFunc    func()
}

var agentSubscriberCounter uint32

func NewReplicatorAgent(spec *metadata.ReplicationSpecification, logger *log.CommonLogger, utils utilities.UtilsIface, ckptSvc service_def.CheckpointsService, backfillReplSvc service_def.BackfillReplSvc, colManifestSvc service_def.CollectionsManifestSvc, randSecInt int, replicationSpecSvc service_def.ReplicationSpecSvc, internalId string, bucketTopologySvc service_def.BucketTopologySvc) *ReplicatorAgentImpl {
	return &ReplicatorAgentImpl{
		logger:                 logger,
		spec:                   spec,
		specId:                 spec.Id,
		srcBucketName:          spec.SourceBucketName,
		bucketSvcId:            spec.Id + "_replAgent_" + base.GetIterationId(&agentSubscriberCounter),
		reloadCh:               make(chan agentReloadPair, base.P2PReplicaReplicatorReloadChSize),
		finCh:                  make(chan bool),
		initDelay:              time.Duration(randSecInt) * time.Second,
		utils:                  utils,
		ckptSvc:                ckptSvc,
		backfillReplSvc:        backfillReplSvc,
		colManifestSvc:         colManifestSvc,
		replSpecSvc:            replicationSpecSvc,
		internalId:             internalId,
		sourceBucketTopologyCh: make(chan service_def.SourceNotification, base.BucketTopologyWatcherChanLen),
		bucketTopologySvc:      bucketTopologySvc,
		immediateFetchCh:       make(chan chan error),
	}
}

func (a *ReplicatorAgentImpl) Start() {
	a.startOnce.Do(func() { go a.run() })
}

func (a *ReplicatorAgentImpl) Stop() {
	close(a.finCh)
}

func (a *ReplicatorAgentImpl) SetUpdatedSpecAsync(spec *metadata.ReplicationSpecification, cbFunc func()) {
	if atomic.LoadUint32(&a.isRunning) == 0 {
		return
	}

	specClone := spec.Clone()
	newInterval := specClone.Settings.GetReplicateCkptInterval()

	pair := agentReloadPair{
		newTimeInterval: newInterval,
		callbackFunc:    cbFunc,
	}
	select {
	case a.reloadCh <- pair:
		// Done
	default:
		a.logger.Warnf("%v reloadCh is full: SetUpdatedSpecAsync is ignored", a.specId)
	}
}

func (a *ReplicatorAgentImpl) GetReplicationInterval() (time.Duration, error) {
	spec, err := a.replSpecSvc.ReplicationSpecReadOnly(a.specId)
	if err != nil {
		return 0, err
	}
	return spec.Settings.GetReplicateCkptInterval(), nil
}

func (a *ReplicatorAgentImpl) ReplicationIsPaused() bool {
	spec, err := a.replSpecSvc.ReplicationSpecReadOnly(a.specId)
	if err != nil {
		a.logger.Warnf("Unable to get repl %v - %v so assuming replication is paused", err)
		return true
	}
	return !spec.Settings.Active
}

func (a *ReplicatorAgentImpl) run() {
	if a.initDelay.Seconds() > 0 {
		base.WaitForTimeoutOrFinishSignal(a.initDelay, a.finCh)
	}

	atomic.StoreUint32(&a.isRunning, 1)
	defer atomic.StoreUint32(&a.isRunning, 0)

	interval, err := a.GetReplicationInterval()
	if err != nil {
		a.logger.Errorf("Unable to get replication interval %v - %v", a.specId, err)
		return
	}
	a.sourceBucketTopologyCh, err = a.bucketTopologySvc.SubscribeToLocalBucketFeed(a.spec, a.bucketSvcId)
	if err != nil {
		a.logger.Errorf("%v - Unable to subscribe to local bucket feed %v", a.specId, err)
		return
	}

	ticker := time.NewTicker(interval)
	for {
		select {
		case <-a.finCh:
			err := a.bucketTopologySvc.UnSubscribeLocalBucketFeed(a.spec, a.bucketSvcId)
			if err != nil {
				a.logger.Errorf("%v - Unable to unsubscribe to local bucket feed %v", a.specId, err)
			}
			return
		case notification := <-a.sourceBucketTopologyCh:
			a.latestCachedSourceNotificationMtx.Lock()
			if a.latestCachedSourceNotification != nil {
				a.latestCachedSourceNotification.Recycle()
			}
			a.latestCachedSourceNotification = notification.Clone(1).(service_def.SourceNotification)
			kv_vb_map := a.latestCachedSourceNotification.GetSourceVBMapRO()
			a.latestVBs = kv_vb_map.GetSortedVBList()
			a.latestCachedSourceNotificationMtx.Unlock()
		case timeAndCb := <-a.reloadCh:
			ticker.Reset(timeAndCb.newTimeInterval)
			timeAndCb.callbackFunc()
		case <-ticker.C:
			if a.ReplicationIsPaused() {
				// Skip this interval
				a.logger.Infof("Ckpt push for replication %v is paused - skipping...", a.specId)
				break
			}
			fetchErr := a.fetchAndCacheLatestReplInfo()
			if fetchErr != nil {
				a.logger.Errorf("Fetching replication %v for ckpt push had err %v - skipping...", a.specId, fetchErr)
				// Try again next time
			}
		case respCh := <-a.immediateFetchCh:
			fetchErr := a.fetchAndCacheLatestReplInfo()
			if fetchErr != nil {
				a.logger.Errorf("Immediate Fetching replication %v for ckpt push had err %v", a.specId, fetchErr)
			}
			select {
			case respCh <- fetchErr:
			case <-a.finCh:
				break
			}
		}
	}

}

func (a *ReplicatorAgentImpl) fetchAndCacheLatestReplInfo() error {
	replicateReq, fetchErr := a.FetchLatestReplicationsInfo()
	if fetchErr != nil {
		return fetchErr
	}
	a.latestInfoMtx.Lock()
	a.latestInfo = replicateReq
	a.latestInfoMtx.Unlock()
	return nil
}

func (a *ReplicatorAgentImpl) getVBsListClone() []uint16 {
	a.latestCachedSourceNotificationMtx.RLock()
	defer a.latestCachedSourceNotificationMtx.RUnlock()
	return base.CloneUint16List(a.latestVBs)
}

// Fetch all data and get them ready to be replicated
func (a *ReplicatorAgentImpl) FetchLatestReplicationsInfo() (*VBPeriodicReplicateReq, error) {
	backfillSpecPreClone, err := a.backfillReplSvc.BackfillReplSpec(a.specId)
	if err == nil && backfillSpecPreClone != nil && backfillSpecPreClone.InternalId != a.internalId {
		return nil, fmt.Errorf("BackfillSpec Internal ID mismatch - expected %v got %v", a.internalId, backfillSpecPreClone.InternalId)
	}
	backfillSpec := backfillSpecPreClone.Clone()

	checkSpec, err := a.replSpecSvc.ReplicationSpecReadOnly(a.specId)
	if err != nil {
		return nil, err
	}
	if checkSpec.InternalId != a.internalId {
		return nil, fmt.Errorf("ReplSpec Internal ID mismatch - expected %v got %v", a.internalId, checkSpec.InternalId)
	}

	mainCkpts, mainCkptErr := a.ckptSvc.CheckpointsDocs(a.specId, true)
	if mainCkptErr != nil {
		a.logger.Errorf("Error getting ckpt docs for %v - %v", a.specId, mainCkptErr)
		mainCkpts = nil
	}

	if mainCkpts != nil {
		castedDocs := metadata.VBsCkptsDocMap(mainCkpts)
		if !castedDocs.InternalIdMatch(a.internalId) {
			return nil, fmt.Errorf("MainCkpts internalID mismatch - expected %v", a.internalId)
		}
	}

	var bkptCkpts map[uint16]*metadata.CheckpointsDoc
	var backfillErr error
	if backfillSpec != nil {
		backfillReplId := common.ComposeFullTopic(a.specId, common.BackfillPipeline)
		bkptCkpts, backfillErr = a.ckptSvc.CheckpointsDocs(backfillReplId, true)
		if backfillErr != nil {
			bkptCkpts = nil
		}
		if bkptCkpts != nil {
			castedDocs := metadata.VBsCkptsDocMap(bkptCkpts)
			if !castedDocs.InternalIdMatch(a.internalId) {
				return nil, fmt.Errorf("BackfillCkpt internalID mismatch - expected %v", a.internalId)
			}
		}
	}
	if mainCkpts != nil && backfillErr != nil {
		return nil, fmt.Errorf("Unable to get mainCkpt %v backfillCkpt %v", mainCkptErr, backfillErr)
	}

	// need to filter out information so that only the VBs of which I'm the master is sent to the replicas
	// This is so that backfill ckpts + tasks are in tandem with mainline ckpts, and we're not "gossipping"
	// out-of-date checkpoints/backfillTasks/BackfillCkpt tuples that can potentially propagate back to the
	// VB master itself and dirty the master
	myVBList := base.SortUint16List(a.getVBsListClone())
	for vbno, _ := range mainCkpts {
		_, found := base.SearchUint16List(myVBList, vbno)
		if !found {
			delete(mainCkpts, vbno)
		}
	}
	if len(bkptCkpts) > 0 {
		for vbno, _ := range bkptCkpts {
			_, found := base.SearchUint16List(myVBList, vbno)
			if !found {
				delete(bkptCkpts, vbno)
			}
		}
	}

	nameOnlySpec := &metadata.ReplicationSpecification{}
	nameOnlySpec.Id = a.specId
	srcManifests, tgtManifests, fetchManifestErr := a.colManifestSvc.GetAllCachedManifests(nameOnlySpec)
	if fetchManifestErr != nil {
		return nil, fetchManifestErr
	}

	combinedVBs := getCombinedVBs(mainCkpts, bkptCkpts)
	req := NewVBPeriodicReplicateReq(a.specId, a.srcBucketName, combinedVBs, a.internalId)
	var hasThingsToSend bool
	if len(mainCkpts) > 0 {
		err = req.LoadMainReplication(mainCkpts, srcManifests, tgtManifests)
		if err != nil {
			return nil, err
		}
		hasThingsToSend = true
	}

	if backfillSpec != nil && backfillSpec.VBTasksMap != nil && backfillSpec.VBTasksMap.ContainsAtLeastOneTaskForVBs(myVBList) {
		err = req.LoadBackfillReplication(backfillSpec.VBTasksMap.FilterBasedOnVBs(myVBList), bkptCkpts, srcManifests,
			tgtManifests, backfillSpec.SourceManifestUid)
		if err != nil {
			return nil, err
		}
		hasThingsToSend = true
	}

	if !hasThingsToSend {
		return nil, fmt.Errorf("Nothing to send yet")
	}

	return req, nil
}

func getCombinedVBs(mainCkpts map[uint16]*metadata.CheckpointsDoc, bkptCkpts map[uint16]*metadata.CheckpointsDoc) []uint16 {
	combinedVBs := make(map[uint16]bool)
	for vbno, _ := range mainCkpts {
		if _, exists := combinedVBs[vbno]; !exists {
			combinedVBs[vbno] = true
		}
	}
	for vbno, _ := range bkptCkpts {
		if _, exists := combinedVBs[vbno]; !exists {
			combinedVBs[vbno] = true
		}
	}
	var vblist []uint16
	for vbno, _ := range combinedVBs {
		vblist = append(vblist, vbno)
	}
	return base.SortUint16List(vblist)
}

func (a *ReplicatorAgentImpl) GetAndClearInfoToReplicate() (*metadata.ReplicationSpecification, *VBPeriodicReplicateReq, error) {
	specClone, err := a.replSpecSvc.ReplicationSpecReadOnly(a.specId)
	if err != nil {
		return nil, nil, err
	}

	retInfo := &VBPeriodicReplicateReq{}
	a.latestInfoMtx.Lock()
	if a.latestInfo != nil {
		*retInfo = *a.latestInfo
		a.latestInfo = nil
	}
	a.latestInfoMtx.Unlock()
	return specClone, retInfo, nil
}
func (a *ReplicatorAgentImpl) RequestImmediateDataGather() error {
	replicatorStopErr := fmt.Errorf("Cannot requested gather because replicator agent is stopping")
	respReady := make(chan error)
	select {
	case a.immediateFetchCh <- respReady:
		select {
		case err := <-respReady:
			return err
		case <-a.finCh:
			return replicatorStopErr
		}
	case <-a.finCh:
		return replicatorStopErr
	}
}
