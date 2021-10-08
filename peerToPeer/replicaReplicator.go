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
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"math/rand"
	"sync"
	"time"
)

type ReplicaReplicator interface {
	Start()
	Stop()
	HandleSpecCreation(spec *metadata.ReplicationSpecification)
	HandleSpecDeletion(oldSpec *metadata.ReplicationSpecification)
	HandleSpecChange(oldSpec *metadata.ReplicationSpecification, newSpec *metadata.ReplicationSpecification)
}

type ReplicaReplicatorImpl struct {
	bucketTopologySvc  service_def.BucketTopologySvc
	ckptSvc            service_def.CheckpointsService
	backfillReplSvc    service_def.BackfillReplSvc
	colManifestSvc     service_def.CollectionsManifestSvc
	replicationSpecSvc service_def.ReplicationSpecSvc

	utils utilities.UtilsIface

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
	agent := NewReplicatorAgent(newSpec, r.logger, r.utils, r.ckptSvc, r.backfillReplSvc, r.colManifestSvc, randSecInt, r.replicationSpecSvc)
	r.agentMapMtx.Lock()
	r.agentMap[newSpec.Id] = agent
	r.agentMapMtx.Unlock()
	agent.Start()

	incomingInterval := newSpec.Settings.GetReplicateCkptInterval()
	r.checkAndUpdateTicker(incomingInterval, 0)
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

		nodeVBOwnership := base.ReverseVBNodesMap(replicaMap)
		for nodeKVName, vbsList := range nodeVBOwnership {
			nodeNsServerName, nsServerFound := replicaTranslateMap[nodeKVName]
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

		unlockFunc()
	}

	return peerNodeToRequestListMap, errMap
}

type ReplicatorAgent interface {
	Start()
	Stop()
	SetUpdatedSpecAsync(spec *metadata.ReplicationSpecification, cbFunc func())
	GetAndClearInfoToReplicate() (*metadata.ReplicationSpecification, *VBPeriodicReplicateReq, error)
}

type ReplicatorAgentImpl struct {
	logger    *log.CommonLogger
	startOnce sync.Once
	initDelay time.Duration
	utils     utilities.UtilsIface

	ckptSvc         service_def.CheckpointsService
	backfillReplSvc service_def.BackfillReplSvc
	colManifestSvc  service_def.CollectionsManifestSvc
	replSpecSvc     service_def.ReplicationSpecSvc

	specId        string
	srcBucketName string

	finCh    chan bool
	reloadCh chan agentReloadPair

	latestInfoMtx sync.Mutex
	latestInfo    *VBPeriodicReplicateReq
}

type agentReloadPair struct {
	newTimeInterval time.Duration
	callbackFunc    func()
}

func NewReplicatorAgent(spec *metadata.ReplicationSpecification, logger *log.CommonLogger, utils utilities.UtilsIface, ckptSvc service_def.CheckpointsService, backfillReplSvc service_def.BackfillReplSvc, colManifestSvc service_def.CollectionsManifestSvc, randSecInt int, replicationSpecSvc service_def.ReplicationSpecSvc) *ReplicatorAgentImpl {

	return &ReplicatorAgentImpl{
		logger:          logger,
		specId:          spec.Id,
		srcBucketName:   spec.SourceBucketName,
		reloadCh:        make(chan agentReloadPair, 10),
		finCh:           make(chan bool),
		initDelay:       time.Duration(randSecInt) * time.Second,
		utils:           utils,
		ckptSvc:         ckptSvc,
		backfillReplSvc: backfillReplSvc,
		colManifestSvc:  colManifestSvc,
		replSpecSvc:     replicationSpecSvc,
	}
}

func (a *ReplicatorAgentImpl) Start() {
	a.startOnce.Do(func() { go a.run() })
}

func (a *ReplicatorAgentImpl) Stop() {
	close(a.finCh)
}

func (a *ReplicatorAgentImpl) SetUpdatedSpecAsync(spec *metadata.ReplicationSpecification, cbFunc func()) {
	specClone := spec.Clone()
	newInterval := specClone.Settings.GetReplicateCkptInterval()

	pair := agentReloadPair{
		newTimeInterval: newInterval,
		callbackFunc:    cbFunc,
	}
	a.reloadCh <- pair
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
	interval, err := a.GetReplicationInterval()
	if err != nil {
		a.logger.Errorf("Unable to get replication interval %v - %v", a.specId, err)
		return
	}
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-a.finCh:
			return
		case timeAndCb := <-a.reloadCh:
			ticker.Reset(timeAndCb.newTimeInterval)
			timeAndCb.callbackFunc()
		case <-ticker.C:
			if a.ReplicationIsPaused() {
				// Skip this interval
				a.logger.Infof("Ckpt push for replication %v is paused - skipping...", a.specId)
				break
			}

			replicateReq, fetchErr := a.FetchLatestReplicationsInfo()
			if fetchErr != nil {
				a.logger.Errorf("Fetching replication %v for ckpt push had err %v - skipping...", a.specId, fetchErr)
				// Try again next time
				continue
			}
			a.latestInfoMtx.Lock()
			a.latestInfo = replicateReq
			a.latestInfoMtx.Unlock()
		}
	}

}

// Fetch all data and get them ready to be replicated
func (a *ReplicatorAgentImpl) FetchLatestReplicationsInfo() (*VBPeriodicReplicateReq, error) {
	backfillSpec, _ := a.backfillReplSvc.BackfillReplSpec(a.specId)

	mainCkpts, mainCkptErr := a.ckptSvc.CheckpointsDocs(a.specId, true)
	if mainCkptErr != nil {
		mainCkpts = nil
	}

	var bkptCkpts map[uint16]*metadata.CheckpointsDoc
	var backfillErr error
	if backfillSpec != nil {
		backfillReplId := common.ComposeFullTopic(a.specId, common.BackfillPipeline)
		bkptCkpts, backfillErr = a.ckptSvc.CheckpointsDocs(backfillReplId, true)
		if backfillErr != nil {
			bkptCkpts = nil
		}
	}

	if mainCkpts != nil && backfillErr != nil {
		return nil, fmt.Errorf("Unable to get mainCkpt %v backfillCkpt %v", mainCkptErr, backfillErr)
	}

	nameOnlySpec := &metadata.ReplicationSpecification{}
	nameOnlySpec.Id = a.specId
	srcManifests, tgtManifests, fetchManifestErr := a.colManifestSvc.GetAllCachedManifests(nameOnlySpec)
	if fetchManifestErr != nil {
		return nil, fetchManifestErr
	}

	combinedVBs := getCombinedVBs(mainCkpts, bkptCkpts)
	req := NewVBPeriodicReplicateReq(a.specId, a.srcBucketName, combinedVBs)
	err := req.LoadMainReplication(mainCkpts, srcManifests, tgtManifests)
	if err != nil {
		return nil, err
	}

	if backfillSpec != nil && backfillSpec.VBTasksMap != nil && backfillSpec.VBTasksMap.ContainsAtLeastOneTask() {
		err = req.LoadBackfillReplication(backfillSpec.VBTasksMap, bkptCkpts, srcManifests, tgtManifests)
		if err != nil {
			return nil, err
		}
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
