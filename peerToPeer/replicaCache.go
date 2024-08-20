// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package peerToPeer

import (
	"fmt"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	"sync"
)

type ReplicaCache interface {
	HandleSpecCreation(spec *metadata.ReplicationSpecification)
	HandleSpecDeletion(oldSpec *metadata.ReplicationSpecification)

	// Remember to unlock when done and use the resources quickly
	// Should only unlock if err is nil
	GetReplicaInfo(spec *metadata.ReplicationSpecification) (replicaCnt int, replicaMap *base.VbHostsMapType, replicaTranslateMap *base.StringStringMap, unlockFunc func(), err error)
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

var replicaCacheIteration uint32

func (r *ReplicaCacheImpl) HandleSpecCreation(spec *metadata.ReplicationSpecification) {
	replicaCacheId := ReplicaCacheSubscriberId + base.GetIterationId(&replicaCacheIteration)
	sourceNotificationCh, err := r.bucketTopologySvc.SubscribeToLocalBucketFeed(spec, replicaCacheId)
	if err != nil {
		r.logger.Errorf("Unable to handle spec creation for spec %v due to %v", spec.Id, err)
		return
	}

	unsubsFunc := func() {
		err := r.bucketTopologySvc.UnSubscribeLocalBucketFeed(spec, replicaCacheId)
		if err != nil {
			r.logger.Errorf("Unable to handle spec deletion for spec %v due to %v", spec.Id, err)
			return
		}
	}

	monitor := NewReplicaCacheMonitor(sourceNotificationCh, unsubsFunc)
	go monitor.Run()
	r.specsMonitorMtx.Lock()
	r.specsMonitorMap[spec.Id] = monitor
	r.specsMonitorMtx.Unlock()
}

func (r *ReplicaCacheImpl) HandleSpecDeletion(oldSpec *metadata.ReplicationSpecification) {
	r.specsMonitorMtx.Lock()
	monitor := r.specsMonitorMap[oldSpec.Id]
	delete(r.specsMonitorMap, oldSpec.Id)
	r.specsMonitorMtx.Unlock()
	if monitor != nil {
		go monitor.Stop()
	}
}

func (r *ReplicaCacheImpl) GetReplicaInfo(spec *metadata.ReplicationSpecification) (replicaCnt int, replicaMap *base.VbHostsMapType, replicaTranslateMap *base.StringStringMap, unlockFunc func(), err error) {
	r.specsMonitorMtx.RLock()
	monitor := r.specsMonitorMap[spec.Id]
	r.specsMonitorMtx.RUnlock()

	if monitor == nil {
		err = base.ErrorNotFound
		return
	}

	replicaCnt, replicaMap, replicaTranslateMap, unlockFunc, err = monitor.GetReplicaInfo()
	return
}

type ReplicaCacheMonitor struct {
	sourceCh   chan service_def.SourceNotification
	finCh      chan bool
	unsubsFunc func()

	cacheMtx           sync.RWMutex
	latestNotification service_def.Notification
}

func (m *ReplicaCacheMonitor) Run() {
	for {
		select {
		case <-m.finCh:
			m.cacheMtx.RLock()
			if m.latestNotification != nil {
				m.latestNotification.Recycle()
			}
			m.cacheMtx.RUnlock()
			return
		case notification := <-m.sourceCh:
			m.cacheMtx.Lock()
			if m.latestNotification != nil {
				m.latestNotification.Recycle()
			}
			m.latestNotification = notification.Clone(1).(service_def.SourceNotification)
			m.cacheMtx.Unlock()
			notification.Recycle()
		}
	}
}

func (m *ReplicaCacheMonitor) Stop() {
	close(m.finCh)
	if m.unsubsFunc != nil {
		m.unsubsFunc()
	}
}

func (m *ReplicaCacheMonitor) GetReplicaInfo() (int, *base.VbHostsMapType, *base.StringStringMap, func(), error) {
	unlockFunc := func() {
		m.cacheMtx.RUnlock()
	}
	m.cacheMtx.RLock()
	if m.latestNotification == nil {
		m.cacheMtx.RUnlock()
		return -1, nil, nil, nil, fmt.Errorf("ReplicaCache has not received a source event yet")
	}
	replicaCnt, replicaMap, translateMap, _ := m.latestNotification.GetReplicasInfo()
	return replicaCnt, replicaMap, translateMap, unlockFunc, nil
}

func NewReplicaCacheMonitor(srcCh chan service_def.SourceNotification, unsubsFunc func()) *ReplicaCacheMonitor {
	return &ReplicaCacheMonitor{
		finCh:      make(chan bool),
		sourceCh:   srcCh,
		unsubsFunc: unsubsFunc,
	}
}
