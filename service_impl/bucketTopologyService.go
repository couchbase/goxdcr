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

func NewBucketTopologyService(xdcrCompTopologySvc service_def.XDCRCompTopologySvc, remClusterSvc service_def.RemoteClusterSvc, utils utils.UtilsIface, refreshInterval time.Duration, loggerContext *log.LoggerContext, replicationSpecService service_def.ReplicationSpecSvc) (*BucketTopologyService, error) {
	b := &BucketTopologyService{
		remClusterSvc:        remClusterSvc,
		xdcrCompTopologySvc:  xdcrCompTopologySvc,
		logger:               log.NewLogger("BucketTopologySvc", loggerContext),
		utils:                utils,
		srcBucketWatchers:    map[string]*BucketTopologySvcWatcher{},
		srcBucketWatchersCnt: map[string]int{},
		refreshInterval:      refreshInterval,
	}
	return b, b.loadFromReplSpecSvc(replicationSpecService)
}

func (b *BucketTopologyService) loadFromReplSpecSvc(replSpecSvc service_def.ReplicationSpecSvc) error {
	specs, err := replSpecSvc.AllReplicationSpecs()
	if err != nil {
		return err
	}

	for _, spec := range specs {
		watcher := b.getOrCreateLocalWatcher(spec)
		watcher.Start()
	}
	return nil
}

func (b *BucketTopologyService) SubscribeToLocalBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) (chan service_def.Notification, error) {
	if spec == nil {
		return nil, base.ErrorNilPtr
	}

	if spec.SourceBucketName == "" {
		return nil, fmt.Errorf("Empty source bucket name for spec %v", spec.Id)
	}

	b.srcBucketWatchersMtx.Lock()
	defer b.srcBucketWatchersMtx.Unlock()
	watcher, exists := b.srcBucketWatchers[spec.SourceBucketName]
	if exists {
		return watcher.registerAndGetCh(spec, subscriberId), nil
	}

	return nil, fmt.Errorf("SubscribeToLocalBucketFeed could not find watcher for %v", spec.SourceBucketName)
}

func (b *BucketTopologyService) getOrCreateLocalWatcher(spec *metadata.ReplicationSpecification) *BucketTopologySvcWatcher {
	b.srcBucketWatchersMtx.Lock()
	defer b.srcBucketWatchersMtx.Unlock()
	watcher, exists := b.srcBucketWatchers[spec.SourceBucketName]
	if !exists {
		watcher = NewBucketTopologySvcWatcher(spec.SourceBucketName, spec.SourceBucketUUID, b.refreshInterval, b.logger, true, b.xdcrCompTopologySvc, b.utils)
		b.srcBucketWatchers[spec.SourceBucketName] = watcher
	}
	b.srcBucketWatchersCnt[spec.SourceBucketName]++
	return watcher
}

func (b *BucketTopologyService) SubscribeToRemoteBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) (chan service_def.Notification, error) {
	//watcher := NewBucketTopologySvcWatcher(spec.TargetBucketName, spec.TargetBucketUUID, b.refreshInterval, b.logger, false, nil, nil)
	return nil, nil
}

func (b *BucketTopologyService) UnSubscribeLocalBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) error {
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

	b.srcBucketWatchers[spec.SourceBucketName].unregisterCh(spec, subscriberId)
	return nil
}

func (b *BucketTopologyService) handleSpecDeletion(spec *metadata.ReplicationSpecification) error {
	if spec == nil {
		return base.ErrorNilPtr
	}

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

func (b *BucketTopologyService) UnSubscribeRemoteBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) error {

	return nil
}

func (b *BucketTopologyService) ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}, wg *sync.WaitGroup) error {
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
		watcher := b.getOrCreateLocalWatcher(newSpec)
		localStartErr := watcher.Start()
		if localStartErr != nil {
			b.logger.Errorf("Error starting local watcher for %v - %v", newSpec, localStartErr)
		}

		b.logger.Infof("Registered bucket monitor for %v", newSpec.Id)
		// TODO - register remote bucket
	} else if oldSpec != nil && newSpec == nil {
		err := b.handleSpecDeletion(oldSpec)
		if err != nil {
			b.logger.Errorf("Unable to unregister local bucket for %v", oldSpec)
			return err
		}
		b.logger.Infof("Unregistered bucket monitor for %v", oldSpec.Id)
	}

	return nil
}

type BucketTopologySvcWatcher struct {
	bucketName string
	bucketUUID string
	source     bool

	finCh     chan bool
	startOnce sync.Once

	notifyChsMtx sync.RWMutex
	// Key is a "spec + subscriber ID"
	notifyChs map[string]chan service_def.Notification

	refreshInterval time.Duration

	latestCacheMtx sync.RWMutex
	latestCached   *Notification
	cachePopulated bool

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
		notifyChs:           map[string]chan service_def.Notification{},
	}

	if source {
		watcher.localKVVbMapUpdater = func() (map[string][]uint16, error) {
			connStr, err := xdcrCompTopologySvc.MyConnectionStr()
			if err != nil {
				watcher.logger.Errorf("%v bucket connStr error %v", bucketName, err)
				return nil, err
			}
			userName, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := xdcrCompTopologySvc.MyCredentials()
			if err != nil {
				watcher.logger.Errorf("%v bucket credentials error %v", bucketName, err)
				return nil, err
			}
			bucketInfo, err := utils.GetBucketInfo(connStr, bucketName, userName, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, logger)
			if err != nil {
				watcher.logger.Errorf("%v bucket bucketInfo error %v", bucketName, err)
				return nil, err
			}
			serverVBMap, err := utils.GetServerVBucketsMap(connStr, bucketName, bucketInfo)
			if err != nil {
				watcher.logger.Errorf("%v bucket server VBMap error %v", bucketName, err)
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
			watcher.cachePopulated = true
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
		_, err := bw.localKVVbMapUpdater()
		if err != nil {
			bw.logger.Errorf("BucketTopologySvcWatcher for local bucket %v updating resulted in err %v - bypassing notification", bw.bucketName, err)
			return
		}
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
		timeout := time.NewTicker(1 * time.Second)
		select {
		case ch <- &notification:
		// sent
		case <-timeout.C:
			// provide a bail out path
			continue
		}
	}
	bw.notifyChsMtx.RUnlock()
}

func (bw *BucketTopologySvcWatcher) Stop() error {
	if atomic.CompareAndSwapUint32(&bw.isStopped, 0, 1) {
		close(bw.finCh)
	}
	return nil
}

func (bw *BucketTopologySvcWatcher) registerAndGetCh(spec *metadata.ReplicationSpecification, subscriberId string) chan service_def.Notification {
	bw.notifyChsMtx.RLock()
	ch, exists := bw.notifyChs[spec.Id]
	if exists {
		bw.notifyChsMtx.RUnlock()
		return ch
	}
	bw.notifyChsMtx.RUnlock()

	newCh := make(chan service_def.Notification, base.BucketTopologyWatcherChanLen)

	fullSubscriberId := compileFullSubscriberId(spec, subscriberId)
	bw.notifyChsMtx.Lock()
	bw.notifyChs[fullSubscriberId] = newCh
	bw.notifyChsMtx.Unlock()

	// When someone first registers and subscribes, it prob expects some data - feed it the latest if it's not nil
	bw.latestCacheMtx.RLock()
	if bw.cachePopulated {
		notification := bw.latestCached.CloneRO()
		newCh <- notification
	}
	bw.latestCacheMtx.RUnlock()
	return newCh
}

func compileFullSubscriberId(spec *metadata.ReplicationSpecification, id string) string {
	return fmt.Sprintf("%v_%v", spec.Id, id)
}

func (bw *BucketTopologySvcWatcher) unregisterCh(spec *metadata.ReplicationSpecification, subscriberId string) {
	fullSubscriberId := compileFullSubscriberId(spec, subscriberId)

	bw.notifyChsMtx.RLock()
	// When unregistering, need to flush all channels before closing
forloop:
	for {
		select {
		case <-bw.notifyChs[fullSubscriberId]:
		default:
			break forloop
		}
	}
	bw.notifyChsMtx.RUnlock()

	bw.notifyChsMtx.Lock()
	defer bw.notifyChsMtx.Unlock()

	delete(bw.notifyChs, fullSubscriberId)
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

func (n *Notification) CloneRO() service_def.Notification {
	if n == nil {
		return nil
	}
	return &Notification{
		NumberOfSourceNodes: n.NumberOfSourceNodes,
		Source:              n.Source,
		KvVbMap:             n.KvVbMap,
		SourceVBMap:         n.SourceVBMap,
	}
}

func (n *Notification) GetNumberOfSourceNodes() (int, error) {
	return n.NumberOfSourceNodes, nil
}

func (n *Notification) GetKvVbMapRO() (map[string][]uint16, error) {
	return n.KvVbMap, nil
}

func (n *Notification) GetSourceVBMapRO() (map[string][]uint16, error) {
	return n.SourceVBMap, nil
}
