// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_impl

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
	"sync"
	"sync/atomic"
	"time"
)

type BucketTopologyService struct {
	remClusterSvc       service_def.RemoteClusterSvc
	xdcrCompTopologySvc service_def.XDCRCompTopologySvc
	utils               utils.UtilsIface
	refreshInterval     time.Duration
	logger              *log.CommonLogger

	// Key is bucket Name
	srcBucketWatchers    map[string]*BucketTopologySvcWatcher
	srcBucketWatchersCnt map[string]int
	srcBucketWatchersMtx sync.RWMutex
}

func NewBucketTopologyService(xdcrCompTopologySvc service_def.XDCRCompTopologySvc, remClusterSvc service_def.RemoteClusterSvc, utils utils.UtilsIface, refreshInterval time.Duration, loggerContext *log.LoggerContext) *BucketTopologyService {
	return &BucketTopologyService{
		remClusterSvc:        remClusterSvc,
		xdcrCompTopologySvc:  xdcrCompTopologySvc,
		logger:               log.NewLogger("BucketTopologySvc", loggerContext),
		utils:                utils,
		srcBucketWatchers:    map[string]*BucketTopologySvcWatcher{},
		srcBucketWatchersCnt: map[string]int{},
		refreshInterval:      refreshInterval,
	}
}

func (b *BucketTopologyService) RegisterLocalBucket(spec *metadata.ReplicationSpecification) (service_def.BucketTopologySvcWatcher, chan *Notification, error) {
	if spec == nil {
		return nil, nil, base.ErrorNilPtr
	}

	b.srcBucketWatchersMtx.Lock()

	watcher, exists := b.srcBucketWatchers[spec.SourceBucketName]
	if exists {
		b.srcBucketWatchersCnt[spec.SourceBucketName]++
		b.srcBucketWatchersMtx.Unlock()
		return watcher, watcher.registerAndGetCh(spec), nil
	}

	watcher = NewBucketTopologySvcWatcher(spec.SourceBucketName, spec.SourceBucketUUID, b.refreshInterval, b.logger, true, b.xdcrCompTopologySvc, b.utils)
	b.srcBucketWatchers[spec.SourceBucketName] = watcher
	b.srcBucketWatchersCnt[spec.SourceBucketName]++
	b.srcBucketWatchersMtx.Unlock()
	return watcher, watcher.registerAndGetCh(spec), watcher.Start()
}

func (b *BucketTopologyService) RegisterRemoteBucket(spec *metadata.ReplicationSpecification) (service_def.BucketTopologySvcWatcher, *service_def.Notification, error) {
	watcher := NewBucketTopologySvcWatcher(spec.TargetBucketName, spec.TargetBucketUUID, b.refreshInterval, b.logger, false, nil, nil)
	return watcher, nil, nil
}

func (b *BucketTopologyService) UnRegisterLocalBucket(spec *metadata.ReplicationSpecification) error {
	if spec == nil {
		return base.ErrorNilPtr
	}

	b.srcBucketWatchersMtx.RLock()
	_, exists := b.srcBucketWatchersCnt[spec.SourceBucketName]
	_, exists2 := b.srcBucketWatchers[spec.SourceBucketName]
	if !exists || !exists2 {
		b.srcBucketWatchersMtx.RUnlock()
		return base.ErrorResourceDoesNotExist
	}
	b.srcBucketWatchersMtx.RUnlock()

	b.srcBucketWatchers[spec.SourceBucketName].unregisterCh(spec)

	b.srcBucketWatchersMtx.Lock()
	defer b.srcBucketWatchersMtx.Unlock()

	var err error

	b.srcBucketWatchersCnt[spec.SourceBucketName]--
	if b.srcBucketWatchersCnt[spec.SourceBucketName] == 0 {
		err = b.srcBucketWatchers[spec.SourceBucketName].Stop()
		delete(b.srcBucketWatchers, spec.SourceBucketName)
		delete(b.srcBucketWatchersCnt, spec.SourceBucketName)
	}
	return err
}

func (b *BucketTopologyService) UnRegisterRemoteBucket(spec *metadata.ReplicationSpecification) error {

	return nil
}

type BucketTopologySvcWatcher struct {
	bucketName string
	bucketUUID string
	source     bool

	finCh     chan bool
	startOnce sync.Once

	notifyChsMtx sync.RWMutex
	notifyChs    map[string]chan *Notification

	refreshInterval time.Duration

	latestCacheMtx sync.RWMutex
	latestCached   *Notification

	logger    *log.CommonLogger
	isStarted uint32
	isStopped uint32

	xdcrCompTopologySvc service_def.XDCRCompTopologySvc

	// ** Local getters **
	// Gets a map of KV and VBs
	localKVVbMapUpdater func() (map[string][]uint16, error)
}

func NewBucketTopologySvcWatcher(bucketName, bucketUuid string, refreshInterval time.Duration, logger *log.CommonLogger, source bool, xdcrCompTopologySvc service_def.XDCRCompTopologySvc, utils utils.UtilsIface) *BucketTopologySvcWatcher {
	watcher := &BucketTopologySvcWatcher{
		bucketName:          bucketName,
		bucketUUID:          bucketUuid,
		finCh:               make(chan bool),
		refreshInterval:     refreshInterval,
		logger:              logger,
		source:              source,
		xdcrCompTopologySvc: xdcrCompTopologySvc,
		latestCached:        NewNotification(source),
		notifyChs:           map[string]chan *Notification{},
	}

	if source {
		watcher.localKVVbMapUpdater = func() (map[string][]uint16, error) {
			connStr, err := xdcrCompTopologySvc.MyConnectionStr()
			if err != nil {
				return nil, err
			}
			userName, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := xdcrCompTopologySvc.MyCredentials()
			if err != nil {
				return nil, err
			}
			bucketInfo, err := utils.GetBucketInfo(connStr, bucketName, userName, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, logger)
			if err != nil {
				return nil, err
			}
			serverVBMap, err := utils.GetServerVBucketsMap(connStr, bucketName, bucketInfo)
			if err != nil {
				return nil, err
			}

			nodes, err := watcher.xdcrCompTopologySvc.MyKVNodes()
			if err != nil {
				return nil, fmt.Errorf("Failed to get my KV nodes, err=%v\n", err)
			}
			if len(nodes) == 0 {
				return nil, base.ErrorNoSourceKV
			}
			sourceKvVbMap := make(map[string][]uint16)
			for _, node := range nodes {
				if vbnos, ok := serverVBMap[node]; ok {
					sourceKvVbMap[node] = vbnos
				}
			}
			watcher.latestCacheMtx.Lock()
			watcher.latestCached.KvVbMap = serverVBMap
			watcher.latestCached.NumberOfSourceNodes = len(serverVBMap)
			watcher.latestCached.SourceVBMap = sourceKvVbMap
			watcher.latestCacheMtx.Unlock()
			return serverVBMap, nil
		}
	}

	return watcher
}

func (bw *BucketTopologySvcWatcher) Start() error {
	if atomic.CompareAndSwapUint32(&bw.isStarted, 0, 1) {
		go bw.run()
		return nil
	}
	return fmt.Errorf("Watcher for %v Already started", bw.bucketName)
}

func (bw *BucketTopologySvcWatcher) run() {
	bw.logger.Infof("Starting watcher for local? %v bucket of %v with UUID %v", bw.source, bw.bucketName, bw.bucketUUID)
	defer bw.logger.Infof("Stopped watcher for local? %v bucket of %v with UUID %v", bw.source, bw.bucketName, bw.bucketUUID)
	ticker := time.NewTicker(bw.refreshInterval)

	initCh := make(chan bool, 1)
	initCh <- true
	for {
		select {
		case <-bw.finCh:
			return
		case <-initCh:
			bw.updateOnce()
		case <-ticker.C:
			bw.updateOnce()
		}
	}
}

func (bw *BucketTopologySvcWatcher) updateOnce() {
	if bw.source {
		bw.localKVVbMapUpdater()

	}

	bw.latestCacheMtx.RLock()
	notification := Notification{
		Source:              bw.source,
		NumberOfSourceNodes: bw.latestCached.NumberOfSourceNodes,
		SourceVBMap:         bw.latestCached.SourceVBMap,
		KvVbMap:             bw.latestCached.KvVbMap,
	}
	bw.latestCacheMtx.RUnlock()

	bw.notifyChsMtx.RLock()
	for _, ch := range bw.notifyChs {
		ch <- &notification
	}
	bw.notifyChsMtx.RUnlock()
}

func (bw *BucketTopologySvcWatcher) Stop() error {
	if atomic.CompareAndSwapUint32(&bw.isStopped, 0, 1) {
		close(bw.finCh)
	}
	return nil
}

func (bw *BucketTopologySvcWatcher) registerAndGetCh(spec *metadata.ReplicationSpecification) chan *Notification {
	bw.notifyChsMtx.Lock()
	defer bw.notifyChsMtx.Unlock()

	ch, exists := bw.notifyChs[spec.Id]
	if exists {
		return ch
	}

	newCh := make(chan *Notification, base.BucketTopologyWatcherChanLen)
	bw.notifyChs[spec.Id] = newCh
	return newCh
}

func (bw *BucketTopologySvcWatcher) unregisterCh(spec *metadata.ReplicationSpecification) {
	bw.notifyChsMtx.RLock()

	// When unregistering, need to flush all channels before closing
forloop:
	for {
		select {
		case <-bw.notifyChs[spec.Id]:
		default:
			break forloop
		}
	}
	bw.notifyChsMtx.RUnlock()

	bw.notifyChsMtx.Lock()
	defer bw.notifyChsMtx.Unlock()

	close(bw.notifyChs[spec.Id])
	delete(bw.notifyChs, spec.Id)
}

type Notification struct {
	Source bool

	// Source only
	NumberOfSourceNodes int
	SourceVBMap         map[string][]uint16

	// Shared between source and target
	KvVbMap map[string][]uint16
}

func NewNotification(isSource bool) *Notification {
	return &Notification{
		NumberOfSourceNodes: 0,
		KvVbMap:             make(map[string][]uint16),
		SourceVBMap:         map[string][]uint16{},
		Source:              isSource,
	}
}

func (n *Notification) IsSourceNotification() bool {
	return n.Source
}

func (n *Notification) Clone() *Notification {
	if n == nil {
		return nil
	}
	return &Notification{
		NumberOfSourceNodes: n.NumberOfSourceNodes,
		Source:              n.Source,
		KvVbMap:             n.KvVbMap,
	}
}

func (n *Notification) GetNumberOfSourceNodes() (int, error) {
	return n.NumberOfSourceNodes, nil
}

func (n *Notification) GetKvVbMapRO() (map[string][]uint16, error) {
	return n.KvVbMap, nil
}
