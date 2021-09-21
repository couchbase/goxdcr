// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package peerToPeer

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"math/rand"
	"sync"
	"time"
)

type ReplicaReplicator interface {
	HandleSpecCreation(spec *metadata.ReplicationSpecification)
	HandleSpecDeletion(oldSpec *metadata.ReplicationSpecification)
	HandleSpecChange(oldSpec *metadata.ReplicationSpecification, newSpec *metadata.ReplicationSpecification)
}

type ReplicaReplicatorImpl struct {
	bucketTopologySvc service_def.BucketTopologySvc
	ckptSvc           service_def.CheckpointsService
	backfillReplSvc   service_def.BackfillReplSvc
	commAPI           PeerToPeerCommAPI
	utils             utilities.UtilsIface

	logger       *log.CommonLogger
	replicaCache ReplicaCache

	agentMap    map[string]*ReplicatorAgentImpl // key is specId
	agentMapMtx sync.RWMutex
}

const moduleName = "ReplicaReplicator"

func NewReplicaReplicator(bucketTopologySvc service_def.BucketTopologySvc, loggerCtx *log.LoggerContext, ckptSvc service_def.CheckpointsService, backfillReplSvc service_def.BackfillReplSvc, commAPI PeerToPeerCommAPI, utils utilities.UtilsIface) *ReplicaReplicatorImpl {
	return &ReplicaReplicatorImpl{
		bucketTopologySvc: bucketTopologySvc,
		logger:            log.NewLogger(moduleName, loggerCtx),
		agentMap:          map[string]*ReplicatorAgentImpl{},
		replicaCache:      NewReplicaCache(bucketTopologySvc, loggerCtx),
		ckptSvc:           ckptSvc,
		backfillReplSvc:   backfillReplSvc,
		commAPI:           commAPI,
		utils:             utils,
	}
}

func (r *ReplicaReplicatorImpl) HandleSpecCreation(newSpec *metadata.ReplicationSpecification) {
	r.replicaCache.HandleSpecCreation(newSpec)
	agent := NewReplicatorAgent(newSpec, r.logger, r.utils)
	r.agentMapMtx.Lock()
	r.agentMap[newSpec.Id] = agent
	r.agentMapMtx.Unlock()
	agent.Start()
}

func (r *ReplicaReplicatorImpl) HandleSpecDeletion(oldSpec *metadata.ReplicationSpecification) {
	r.replicaCache.HandleSpecDeletion(oldSpec)

	r.agentMapMtx.Lock()
	agent := r.agentMap[oldSpec.Id]
	r.agentMapMtx.Unlock()

	if agent != nil {
		agent.Stop()
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
	go agent.SetUpdatedSpec(newSpec)
}

type ReplicatorAgent interface {
	Start()
	Stop()
	SetUpdatedSpec(spec *metadata.ReplicationSpecification)
}

type ReplicatorAgentImpl struct {
	logger    *log.CommonLogger
	spec      *metadata.ReplicationSpecification // last-heard responsible spec
	specMtx   sync.RWMutex
	startOnce sync.Once
	initDelay time.Duration
	utils     utilities.UtilsIface

	finCh    chan bool
	reloadCh chan time.Duration
}

func (a *ReplicatorAgentImpl) Start() {
	a.startOnce.Do(func() { go a.run() })
}

func (a *ReplicatorAgentImpl) Stop() {
	close(a.finCh)
}

func (a *ReplicatorAgentImpl) SetUpdatedSpec(spec *metadata.ReplicationSpecification) {
	specClone := spec.Clone()
	a.specMtx.Lock()
	a.spec = specClone
	a.specMtx.Unlock()
	newInterval := specClone.Settings.GetReplicateCkptInterval()
	a.reloadCh <- newInterval
}

func (a *ReplicatorAgentImpl) GetReplicationInterval() time.Duration {
	a.specMtx.RLock()
	defer a.specMtx.RUnlock()
	return a.spec.Settings.GetReplicateCkptInterval()
}

func (a *ReplicatorAgentImpl) run() {
	base.WaitForTimeoutOrFinishSignal(a.initDelay, a.finCh)
	ticker := time.NewTicker(a.GetReplicationInterval())
	for {
		select {
		case <-a.finCh:
			return
		case newTime := <-a.reloadCh:
			ticker.Reset(newTime)
		case <-ticker.C:
			// TODO
		}
	}

}

func NewReplicatorAgent(spec *metadata.ReplicationSpecification, logger *log.CommonLogger, utils utilities.UtilsIface) *ReplicatorAgentImpl {
	// When specs are created or XDCR boots up, they all start together
	// Since replicator operations are heavy as it hits metakv and also other nodes
	// it is better to add randomnized start delay so they all don't hit at once
	randSecondInt := rand.Int() % 60 // Random sec to add up to 60 seconds

	return &ReplicatorAgentImpl{
		logger:    logger,
		spec:      spec,
		reloadCh:  make(chan time.Duration),
		finCh:     make(chan bool),
		initDelay: time.Duration(randSecondInt) * time.Second,
		utils:     utils,
	}
}
