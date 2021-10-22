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
	"sync"
)

type ReplicaCache interface {
	HandleSpecCreation(spec *metadata.ReplicationSpecification)
	HandleSpecDeletion(oldSpec *metadata.ReplicationSpecification)

	// Remember to unlock when done and use the resources quickly
	// Should only unlock if err is nil
	GetReplicaInfo(spec *metadata.ReplicationSpecification) (replicaCnt int, replicaMap base.VbHostsMapType, replicaTranslateMap base.StringStringMap, unlockFunc func(), err error)
}

const ReplicaCacheSubscriberId = "p2pReplicaCache"

type ReplicaCacheImpl struct {
	bucketTopologySvc service_def.BucketTopologySvc

	logger          *log.CommonLogger
	specsMonitorMap map[string]*ReplicaCacheMonitor
	specsMonitorMtx sync.RWMutex
}

func NewReplicaCache(bucketTopologySvc service_def.BucketTopologySvc, loggerCtx *log.LoggerContext) *ReplicaCacheImpl {
	cache := &ReplicaCacheImpl{
		bucketTopologySvc: bucketTopologySvc,
		specsMonitorMap:   map[string]*ReplicaCacheMonitor{},
		logger:            log.NewLogger(ReplicaCacheSubscriberId, loggerCtx),
	}
	return cache
}

func (r *ReplicaCacheImpl) HandleSpecCreation(spec *metadata.ReplicationSpecification) {
	sourceNotificationCh, err := r.bucketTopologySvc.SubscribeToLocalBucketFeed(spec, ReplicaCacheSubscriberId)
	if err != nil {
		r.logger.Errorf("Unable to handle spec creation for spec %v due to %v", spec.Id, err)
		return
	}
	monitor := NewReplicaCacheMonitor(sourceNotificationCh)
	go monitor.Run()
	r.specsMonitorMtx.Lock()
	r.specsMonitorMap[spec.Id] = monitor
	r.specsMonitorMtx.Unlock()
}

func (r *ReplicaCacheImpl) HandleSpecDeletion(oldSpec *metadata.ReplicationSpecification) {
	err := r.bucketTopologySvc.UnSubscribeLocalBucketFeed(oldSpec, ReplicaCacheSubscriberId)
	if err != nil {
		r.logger.Errorf("Unable to handle spec deletion for spec %v due to %v", oldSpec.Id, err)
		return
	}
	r.specsMonitorMtx.Lock()
	monitor := r.specsMonitorMap[oldSpec.Id]
	delete(r.specsMonitorMap, oldSpec.Id)
	r.specsMonitorMtx.Unlock()
	if monitor != nil {
		go monitor.Stop()
	}
}

func (r *ReplicaCacheImpl) GetReplicaInfo(spec *metadata.ReplicationSpecification) (replicaCnt int, replicaMap base.VbHostsMapType, replicaTranslateMap base.StringStringMap, unlockFunc func(), err error) {
	r.specsMonitorMtx.RLock()
	monitor := r.specsMonitorMap[spec.Id]
	r.specsMonitorMtx.RUnlock()

	if monitor == nil {
		err = base.ErrorNotFound
		return
	}

	replicaCnt, replicaMap, replicaTranslateMap, unlockFunc = monitor.GetReplicaInfo()
	return
}

type ReplicaCacheMonitor struct {
	sourceCh chan service_def.SourceNotification
	finCh    chan bool

	cacheMtx            sync.RWMutex
	replicaCnt          int
	replicaMap          map[uint16][]string
	replicaTranslateMap map[string]string
	replicaMember       []uint16
}

func (m *ReplicaCacheMonitor) Run() {
	for {
		select {
		case <-m.finCh:
			return
		case notification := <-m.sourceCh:
			m.cacheMtx.Lock()
			m.replicaCnt, m.replicaMap, m.replicaTranslateMap, m.replicaMember = notification.GetReplicasInfo()
			m.cacheMtx.Unlock()
		}
	}
}

func (m *ReplicaCacheMonitor) Stop() {
	close(m.finCh)
}

func (m *ReplicaCacheMonitor) GetReplicaInfo() (int, map[uint16][]string, map[string]string, func()) {
	unlockFunc := func() {
		m.cacheMtx.RUnlock()
	}
	m.cacheMtx.RLock()
	return m.replicaCnt, m.replicaMap, m.replicaTranslateMap, unlockFunc
}

func NewReplicaCacheMonitor(srcCh chan service_def.SourceNotification) *ReplicaCacheMonitor {
	return &ReplicaCacheMonitor{
		finCh:    make(chan bool),
		sourceCh: srcCh,
	}
}
