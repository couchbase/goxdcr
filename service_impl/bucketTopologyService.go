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

	tgtBucketWatchers    map[string]*BucketTopologySvcWatcher
	tgtBucketWatchersCnt map[string]int
	tgtBucketWatchersMtx sync.RWMutex
}

func NewBucketTopologyService(xdcrCompTopologySvc service_def.XDCRCompTopologySvc, remClusterSvc service_def.RemoteClusterSvc, utils utils.UtilsIface, refreshInterval time.Duration, loggerContext *log.LoggerContext, replicationSpecService service_def.ReplicationSpecSvc) (*BucketTopologyService, error) {
	b := &BucketTopologyService{
		remClusterSvc:        remClusterSvc,
		xdcrCompTopologySvc:  xdcrCompTopologySvc,
		logger:               log.NewLogger("BucketTopologySvc", loggerContext),
		utils:                utils,
		srcBucketWatchers:    map[string]*BucketTopologySvcWatcher{},
		srcBucketWatchersCnt: map[string]int{},
		tgtBucketWatchers:    map[string]*BucketTopologySvcWatcher{},
		tgtBucketWatchersCnt: map[string]int{},
		refreshInterval:      refreshInterval,
	}
	return b, b.loadFromReplSpecSvc(replicationSpecService)
}

func (b *BucketTopologyService) loadFromReplSpecSvc(replSpecSvc service_def.ReplicationSpecSvc) error {
	specs, err := replSpecSvc.AllReplicationSpecs()
	if err != nil {
		return err
	}

	var waitGrp sync.WaitGroup
	for _, spec := range specs {
		waitGrp.Add(2)
		go func() {
			defer waitGrp.Done()
			watcher := b.getOrCreateLocalWatcher(spec)
			watcher.Start()
		}()
		go func() {
			defer waitGrp.Done()

			retryOp := func() error {
				watcher, startErr := b.getOrCreateRemoteWatcher(spec)
				if startErr != nil {
					return err
				}
				watcher.Start()
				return nil
			}
			retryErr := b.utils.ExponentialBackoffExecutor("BucketTopologyServiceLoadSpec",
				base.DefaultHttpTimeoutWaitTime, base.DefaultHttpTimeoutMaxRetry, base.DefaultHttpTimeoutRetryFactor, retryOp)
			if retryErr != nil {
				panic(fmt.Sprintf("Bucket Topology service bootstrapping %v did not finish within %v and XDCR must restart to try again", spec.Id, base.DefaultHttpTimeout))
			}
		}()
	}
	waitGrp.Wait()
	return nil
}

func (b *BucketTopologyService) SubscribeToLocalBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) (chan service_def.SourceNotification, error) {
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
		retCh := watcher.registerAndGetCh(spec, subscriberId).(chan service_def.SourceNotification)
		return retCh, nil
	}

	return nil, fmt.Errorf("SubscribeToLocalBucketFeed could not find watcher for %v", spec.SourceBucketName)
}

func (b *BucketTopologyService) getOrCreateLocalWatcher(spec *metadata.ReplicationSpecification) *BucketTopologySvcWatcher {
	b.srcBucketWatchersMtx.Lock()
	defer b.srcBucketWatchersMtx.Unlock()
	watcher, exists := b.srcBucketWatchers[spec.SourceBucketName]
	if !exists {
		srcFunc := func() error {
			connStr, err := b.xdcrCompTopologySvc.MyConnectionStr()
			if err != nil {
				watcher.logger.Errorf("%v bucket connStr error %v", spec.SourceBucketName, err)
				return err
			}
			userName, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := b.xdcrCompTopologySvc.MyCredentials()
			if err != nil {
				watcher.logger.Errorf("%v bucket credentials error %v", spec.SourceBucketName, err)
				return err
			}
			bucketInfo, err := b.utils.GetBucketInfo(connStr, spec.SourceBucketName, userName, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, b.logger)
			if err != nil {
				watcher.logger.Errorf("%v bucket bucketInfo error %v", spec.SourceBucketName, err)
				return err
			}
			serverVBMap, err := b.utils.GetServerVBucketsMap(connStr, spec.SourceBucketName, bucketInfo)
			if err != nil {
				watcher.logger.Errorf("%v bucket server VBMap error %v", spec.SourceBucketName, err)
				return err
			}

			nodes, err := watcher.xdcrCompTopologySvc.MyKVNodes()
			if err != nil {
				return fmt.Errorf("Failed to get my KV nodes, err=%v\n", err)
			}
			if len(nodes) == 0 {
				return base.ErrorNoSourceKV
			}
			sourceKvVbMap := make(map[string][]uint16)
			for _, node := range nodes {
				if vbnos, ok := serverVBMap[node]; ok {
					sourceKvVbMap[node] = vbnos
				}
			}
			watcher.latestCacheMtx.Lock()
			watcher.cachePopulated = true
			watcher.latestCached.Source = true
			watcher.latestCached.KvVbMap = serverVBMap
			watcher.latestCached.NumberOfSourceNodes = len(serverVBMap)
			watcher.latestCached.SourceVBMap = sourceKvVbMap
			watcher.latestCacheMtx.Unlock()
			return nil
		}

		watcher = NewBucketTopologySvcWatcher(spec.SourceBucketName, spec.SourceBucketUUID, b.refreshInterval, b.logger, true, b.xdcrCompTopologySvc, srcFunc)
		b.srcBucketWatchers[spec.SourceBucketName] = watcher
	}
	b.srcBucketWatchersCnt[spec.SourceBucketName]++
	return watcher
}

func (b *BucketTopologyService) getOrCreateRemoteWatcher(spec *metadata.ReplicationSpecification) (*BucketTopologySvcWatcher, error) {
	if spec == nil {
		return nil, base.ErrorNilPtr
	}

	if spec.TargetBucketName == "" {
		return nil, fmt.Errorf("Empty target bucket name for spec %v", spec.Id)
	}

	if spec.TargetClusterUUID == "" {
		return nil, fmt.Errorf("Empty target cluster UUID for spec %v", spec.Id)
	}

	ref, err := b.remClusterSvc.RemoteClusterByUuid(spec.TargetClusterUUID, false)
	if err != nil {
		return nil, fmt.Errorf("Unable to find reference for spec %v", spec.Id)
	}

	bucketInfoGetter, err := b.remClusterSvc.GetBucketInfoGetter(ref, spec.TargetBucketName)
	if err != nil {
		return nil, fmt.Errorf("Unable to get remote bucketInfo getter for spec %v", spec.Id)
	}

	b.tgtBucketWatchersMtx.Lock()
	defer b.tgtBucketWatchersMtx.Unlock()
	watcher, exists := b.tgtBucketWatchers[spec.TargetBucketName]
	if !exists {
		getterFunc := func() error {
			targetBucketInfo, shouldUseExternal, connStr, err := bucketInfoGetter()
			if err != nil {
				return err
			}
			targetBucketUUID, err := b.utils.GetBucketUuidFromBucketInfo(spec.TargetBucketName, targetBucketInfo, b.logger)
			if err != nil {
				return err
			}
			targetServerVBMap, err := b.utils.GetRemoteServerVBucketsMap(connStr, spec.TargetBucketName, targetBucketInfo, shouldUseExternal)
			if err != nil {
				return err
			}

			watcher.latestCacheMtx.Lock()
			watcher.cachePopulated = true
			watcher.latestCached.Source = false
			watcher.latestCached.TargetServerVBMap = targetServerVBMap
			watcher.latestCached.TargetBucketUUID = targetBucketUUID
			watcher.latestCached.TargetBucketInfo = targetBucketInfo
			watcher.latestCacheMtx.Unlock()
			return nil
		}
		watcher = NewBucketTopologySvcWatcher(spec.TargetBucketName, spec.TargetBucketUUID, b.refreshInterval, b.logger, false, b.xdcrCompTopologySvc, getterFunc)
		b.tgtBucketWatchers[spec.TargetBucketName] = watcher
	}
	b.tgtBucketWatchersCnt[spec.TargetBucketName]++
	return watcher, nil
}

func (b *BucketTopologyService) SubscribeToRemoteBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) (chan service_def.TargetNotification, error) {
	if spec == nil {
		return nil, base.ErrorNilPtr
	}

	if spec.TargetBucketName == "" {
		return nil, fmt.Errorf("Empty target bucket name for spec %v", spec.Id)
	}

	b.tgtBucketWatchersMtx.Lock()
	defer b.tgtBucketWatchersMtx.Unlock()
	watcher, exists := b.tgtBucketWatchers[spec.TargetBucketName]
	if exists {
		return watcher.registerAndGetCh(spec, subscriberId).(chan service_def.TargetNotification), nil
	}

	return nil, fmt.Errorf("SubscribeToRemoteBucketFeed could not find watcher for %v", spec.TargetBucketName)
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

	var err error
	b.srcBucketWatchersMtx.Lock()
	b.srcBucketWatchersCnt[spec.SourceBucketName]--
	if b.srcBucketWatchersCnt[spec.SourceBucketName] == 0 {
		err = b.srcBucketWatchers[spec.SourceBucketName].Stop()
		delete(b.srcBucketWatchers, spec.SourceBucketName)
		delete(b.srcBucketWatchersCnt, spec.SourceBucketName)
	}
	b.srcBucketWatchersMtx.Unlock()

	b.tgtBucketWatchersMtx.Lock()
	b.tgtBucketWatchersCnt[spec.TargetBucketName]--
	if b.tgtBucketWatchersCnt[spec.TargetBucketName] == 0 {
		err = b.tgtBucketWatchers[spec.TargetBucketName].Stop()
		delete(b.tgtBucketWatchers, spec.TargetBucketName)
		delete(b.tgtBucketWatchersCnt, spec.TargetBucketName)
	}
	b.tgtBucketWatchersMtx.Unlock()

	return err
}

func (b *BucketTopologyService) UnSubscribeRemoteBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) error {
	if spec == nil {
		return base.ErrorNilPtr
	}

	b.tgtBucketWatchersMtx.RLock()
	_, exists := b.tgtBucketWatchersCnt[spec.TargetBucketName]
	_, exists2 := b.tgtBucketWatchers[spec.TargetBucketName]
	if !exists || !exists2 {
		b.tgtBucketWatchersMtx.RUnlock()
		return base.ErrorResourceDoesNotExist
	}
	b.tgtBucketWatchersMtx.RUnlock()

	b.tgtBucketWatchers[spec.TargetBucketName].unregisterCh(spec, subscriberId)
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

		remoteWatcher, remoteErr := b.getOrCreateRemoteWatcher(newSpec)
		if remoteErr != nil {
			b.logger.Errorf("Error getting remote watcher for %v - %v", newSpec, remoteErr)
		}
		remoteStartErr := remoteWatcher.Start()
		if remoteStartErr != nil {
			b.logger.Errorf("Error starting remote watcher for %v - %v", newSpec, remoteStartErr)
		}
		b.logger.Infof("Registered bucket monitor for %v", newSpec.Id)
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
	notifyChs map[string]interface{}

	refreshInterval time.Duration

	latestCacheMtx sync.RWMutex
	latestCached   *Notification
	cachePopulated bool

	logger    *log.CommonLogger
	isStarted uint32
	isStopped uint32

	xdcrCompTopologySvc service_def.XDCRCompTopologySvc

	getterFunc func() error
}

func NewBucketTopologySvcWatcher(bucketName, bucketUuid string, refreshInterval time.Duration, logger *log.CommonLogger, source bool, xdcrCompTopologySvc service_def.XDCRCompTopologySvc, getterFunc func() error) *BucketTopologySvcWatcher {
	watcher := &BucketTopologySvcWatcher{
		bucketName:          bucketName,
		bucketUUID:          bucketUuid,
		finCh:               make(chan bool),
		refreshInterval:     refreshInterval,
		logger:              logger,
		source:              source,
		xdcrCompTopologySvc: xdcrCompTopologySvc,
		latestCached:        NewNotification(source),
		notifyChs:           make(map[string]interface{}),
		getterFunc:          getterFunc,
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
	err := bw.getterFunc()
	if err != nil {
		bw.logger.Errorf("BucketTopologySvcWatcher for local? %v bucket %v updating resulted in err %v - bypassing notification", bw.source, bw.bucketName, err)
		return
	}

	bw.latestCacheMtx.RLock()
	notification := Notification{
		Source:              bw.source,
		NumberOfSourceNodes: bw.latestCached.NumberOfSourceNodes,
		SourceVBMap:         bw.latestCached.SourceVBMap,
		KvVbMap:             bw.latestCached.KvVbMap,
		TargetServerVBMap:   bw.latestCached.TargetServerVBMap,
		TargetBucketUUID:    bw.latestCached.TargetBucketUUID,
	}
	bw.latestCacheMtx.RUnlock()

	bw.notifyChsMtx.RLock()
	for _, chRaw := range bw.notifyChs {
		timeout := time.NewTicker(1 * time.Second)
		if bw.source {
			select {
			case chRaw.(chan service_def.SourceNotification) <- &notification:
			// sent
			case <-timeout.C:
				// provide a bail out path
				continue
			}
		} else {
			select {
			case chRaw.(chan service_def.TargetNotification) <- &notification:
			// sent
			case <-timeout.C:
				// provide a bail out path
				continue
			}
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

func (bw *BucketTopologySvcWatcher) registerAndGetCh(spec *metadata.ReplicationSpecification, subscriberId string) interface{} {
	bw.notifyChsMtx.RLock()
	ch, exists := bw.notifyChs[spec.Id]
	if exists {
		bw.notifyChsMtx.RUnlock()
		return ch
	}
	bw.notifyChsMtx.RUnlock()

	fullSubscriberId := compileFullSubscriberId(spec, subscriberId)
	bw.notifyChsMtx.Lock()
	if bw.source {
		newCh := make(chan service_def.SourceNotification, base.BucketTopologyWatcherChanLen)
		bw.notifyChs[fullSubscriberId] = newCh
	} else {
		newCh := make(chan service_def.TargetNotification, base.BucketTopologyWatcherChanLen)
		bw.notifyChs[fullSubscriberId] = newCh
	}
	bw.notifyChsMtx.Unlock()

	// When someone first registers and subscribes, it prob expects some data - feed it the latest if it's not nil
	bw.latestCacheMtx.RLock()
	defer bw.latestCacheMtx.RUnlock()
	if bw.cachePopulated {
		if bw.source {
			notification := bw.latestCached.CloneRO().(service_def.SourceNotification)
			bw.notifyChs[fullSubscriberId].(chan service_def.SourceNotification) <- notification
		} else {
			notification := bw.latestCached.CloneRO().(service_def.TargetNotification)
			bw.notifyChs[fullSubscriberId].(chan service_def.TargetNotification) <- notification
		}
	}
	return bw.notifyChs[fullSubscriberId]
}

func compileFullSubscriberId(spec *metadata.ReplicationSpecification, id string) string {
	return fmt.Sprintf("%v_%v", spec.Id, id)
}

func (bw *BucketTopologySvcWatcher) unregisterCh(spec *metadata.ReplicationSpecification, subscriberId string) {
	fullSubscriberId := compileFullSubscriberId(spec, subscriberId)

	bw.notifyChsMtx.Lock()
	defer bw.notifyChsMtx.Unlock()

	delete(bw.notifyChs, fullSubscriberId)
}

type Notification struct {
	Source bool

	// Source only
	NumberOfSourceNodes int
	SourceVBMap         map[string][]uint16
	KvVbMap             map[string][]uint16

	// Target only
	TargetBucketUUID  string
	TargetServerVBMap map[string][]uint16
	TargetBucketInfo  map[string]interface{}
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

func (n *Notification) CloneRO() interface{} {
	if n == nil {
		return nil
	}
	return &Notification{
		NumberOfSourceNodes: n.NumberOfSourceNodes,
		Source:              n.Source,
		KvVbMap:             n.KvVbMap,
		SourceVBMap:         n.SourceVBMap,
		TargetBucketUUID:    n.TargetBucketUUID,
		TargetBucketInfo:    n.TargetBucketInfo,
		TargetServerVBMap:   n.TargetServerVBMap,
	}
}

func (n *Notification) GetNumberOfSourceNodes() int {
	return n.NumberOfSourceNodes
}

func (n *Notification) GetKvVbMapRO() map[string][]uint16 {
	return n.KvVbMap
}

func (n *Notification) GetSourceVBMapRO() map[string][]uint16 {
	return n.SourceVBMap
}

func (n *Notification) GetTargetServerVBMap() map[string][]uint16 {
	return n.TargetServerVBMap
}

func (n *Notification) GetTargetBucketUUID() string {
	return n.TargetBucketUUID
}

func (n *Notification) GetTargetBucketInfo() map[string]interface{} {
	return n.TargetBucketInfo
}
