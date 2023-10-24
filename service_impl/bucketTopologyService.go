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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/streamApiWatcher"
	"github.com/couchbase/goxdcr/utils"
)

type BucketTopologyObjsPool struct {
	KvVbMapPool       *utils.KvVbMapPool
	DcpStatsMapPool   *utils.DcpStatsMapPool
	StringStringPool  *utils.StringStringMapPool
	HighSeqnosMapPool *utils.HighSeqnosMapPool
	VbSeqnoMapPool    *utils.VbSeqnoMapPool
	VbHostsMapPool    *utils.VbHostsMapPool
	StringSlicePool   *utils.StringSlicePool
}

func NewBucketTopologyObjsPool() *BucketTopologyObjsPool {
	stringSlicePool := utils.NewStringSlicePool()
	return &BucketTopologyObjsPool{
		KvVbMapPool:       utils.NewKvVbMapPool(),
		DcpStatsMapPool:   utils.NewDcpStatsMapPool(),
		StringStringPool:  utils.NewStringStringMapPool(),
		HighSeqnosMapPool: utils.NewHighSeqnosMapPool(),
		VbSeqnoMapPool:    utils.NewVbSeqnoMapPool(),
		VbHostsMapPool:    utils.NewVbHostsMapPool(stringSlicePool),
		StringSlicePool:   stringSlicePool,
	}
}

type BucketTopologyService struct {
	remClusterSvc       service_def.RemoteClusterSvc
	xdcrCompTopologySvc service_def.XDCRCompTopologySvc
	securitySvc         service_def.SecuritySvc
	utils               utils.UtilsIface
	refreshInterval     time.Duration
	logger              *log.CommonLogger
	healthCheckInterval time.Duration

	// Key is bucket Name
	srcBucketWatchers    map[string]*BucketTopologySvcWatcher
	srcBucketWatchersCnt map[string]int
	srcBucketWatchersMtx sync.RWMutex

	// Key is targetClusterUUID+bucketName
	tgtBucketWatchers    map[string]*BucketTopologySvcWatcher
	tgtBucketWatchersCnt map[string]int
	tgtBucketWatchersMtx sync.RWMutex

	streamApiGetter streamApiWatcher.StreamApiGetterFunc
}

func NewBucketTopologyService(xdcrCompTopologySvc service_def.XDCRCompTopologySvc, remClusterSvc service_def.RemoteClusterSvc, utils utils.UtilsIface, refreshInterval time.Duration, loggerContext *log.LoggerContext, replicationSpecService service_def.ReplicationSpecSvc, healthCheckInterval time.Duration,
	securitySvc service_def.SecuritySvc, streamApiGetter streamApiWatcher.StreamApiGetterFunc) (*BucketTopologyService, error) {
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
		healthCheckInterval:  healthCheckInterval,
		securitySvc:          securitySvc,
		streamApiGetter:      streamApiGetter,
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
		specCpy := spec.Clone()
		go func() {
			defer waitGrp.Done()

			localRetryOp := func() error {
				watcher := b.getOrCreateLocalWatcher(specCpy)
				if watcher == nil {
					return base.ErrorNilPtr
				}
				localStartErr := watcher.Start()
				if localStartErr != nil && localStartErr != ErrorWatcherAlreadyStarted {
					b.logger.Errorf("Error starting local watcher for %v - %v", specCpy, localStartErr)
					return localStartErr
				}
				return nil
			}

			retryErr := b.utils.ExponentialBackoffExecutor("BucketTopologyServiceLoadSpec (local)",
				base.DefaultHttpTimeoutWaitTime, base.DefaultHttpTimeoutMaxRetry, base.DefaultHttpTimeoutRetryFactor, localRetryOp)
			if retryErr != nil {
				panic(fmt.Sprintf("Bucket Topology service bootstrapping %v did not successfully start local and XDCR must restart to try again", specCpy.Id))
			}
		}()
		go func() {
			defer waitGrp.Done()

			retryOp := func() error {
				watcher, startErr := b.getOrCreateRemoteWatcher(specCpy)
				if startErr != nil {
					b.logger.Errorf("getOrCreateRemoteWatcher has error: %v", startErr)
					return startErr
				}
				startErr = watcher.Start()
				if startErr != nil && startErr != ErrorWatcherAlreadyStarted {
					b.logger.Errorf("Error starting remote watcher for %v - %v", specCpy.Id, startErr)
					return startErr
				}
				return nil
			}
			retryErr := b.utils.ExponentialBackoffExecutor("BucketTopologyServiceLoadSpec (remote)",
				base.DefaultHttpTimeoutWaitTime, base.DefaultHttpTimeoutMaxRetry, base.DefaultHttpTimeoutRetryFactor, retryOp)
			if retryErr != nil {
				panic(fmt.Sprintf("Bucket Topology service bootstrapping %v did not finish within %v and XDCR must restart to try again", specCpy.Id, base.DefaultHttpTimeout))
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

	if isKvNode, isKvNodeErr := b.xdcrCompTopologySvc.IsKVNode(); isKvNodeErr == nil && !isKvNode {
		return nil, base.ErrorNoSourceNozzle
	}

	b.srcBucketWatchersMtx.Lock()
	defer b.srcBucketWatchersMtx.Unlock()
	watcher, exists := b.srcBucketWatchers[spec.SourceBucketName]
	if exists {
		retCh := watcher.registerAndGetCh(spec, subscriberId, TOPOLOGY, nil).(chan service_def.SourceNotification)
		return retCh, nil
	}

	return nil, fmt.Errorf("SubscribeToLocalBucketFeed could not find watcher for %v", spec.SourceBucketName)
}

func (b *BucketTopologyService) getOrCreateLocalWatcher(spec *metadata.ReplicationSpecification) *BucketTopologySvcWatcher {
	b.srcBucketWatchersMtx.Lock()
	defer b.srcBucketWatchersMtx.Unlock()
	watcher, exists := b.srcBucketWatchers[spec.SourceBucketName]
	if !exists {
		watcher = NewBucketTopologySvcWatcher(spec.SourceBucketName, spec.SourceBucketUUID, b.logger, true, b.xdcrCompTopologySvc)
		b.srcBucketWatchers[spec.SourceBucketName] = watcher

		intervalFuncMap := make(IntervalFuncMap)
		dcpStatsFunc := b.getDcpStatsUpdater(spec, watcher)
		intervalFuncMap[DCPSTATSCHECK] = make(IntervalInnerFuncMap)
		intervalFuncMap[DCPSTATSCHECK][b.healthCheckInterval] = dcpStatsFunc

		dcpStatsLegacyFunc := b.getDcpStatsLegacyUpdater(spec, watcher)
		intervalFuncMap[DCPSTATSCHECKLEGACY] = make(IntervalInnerFuncMap)
		intervalFuncMap[DCPSTATSCHECKLEGACY][b.healthCheckInterval] = dcpStatsLegacyFunc

		defaultPipelineStatsInterval := time.Duration(metadata.DefaultPipelineStatsIntervalMs) * time.Millisecond
		highSeqnoFunc := b.getHighSeqnosUpdater(spec, watcher, false)
		intervalFuncMap[HIGHSEQNOS] = make(IntervalInnerFuncMap)
		intervalFuncMap[HIGHSEQNOS][defaultPipelineStatsInterval] = highSeqnoFunc

		highSeqnoFuncLegacy := b.getHighSeqnosUpdater(spec, watcher, true)
		intervalFuncMap[HIGHSEQNOSLEGACY] = make(IntervalInnerFuncMap)
		intervalFuncMap[HIGHSEQNOSLEGACY][defaultPipelineStatsInterval] = highSeqnoFuncLegacy

		watcher.intervalFuncMap = intervalFuncMap

		callback := b.getStreamApiCallback(spec, watcher)
		watcher.streamApi = b.streamApiGetter(base.ObserveBucketPath+spec.SourceBucketName, b.xdcrCompTopologySvc, b.utils, callback, b.logger)
	}
	b.srcBucketWatchersCnt[spec.SourceBucketName]++
	return watcher
}

// This is called to update the watcher when StreamApi received a new result
func (b *BucketTopologyService) getStreamApiCallback(spec *metadata.ReplicationSpecification, watcher *BucketTopologySvcWatcher) func() {
	return func() {
		updater := b.getLocalBucketTopologyUpdater(spec, watcher)
		err := updater()
		if err != nil {
			b.logger.Errorf("StreamApi update for local BucketTopologySvcWatcher for bucket %v resulted in err '%v'", spec.SourceBucketName, err.Error())
			return
		}
		watcher.latestCacheMtx.RLock()
		notification := watcher.latestCached.Clone(1).(*Notification) // Set to 1 by default, changed later
		watcher.latestCacheMtx.RUnlock()
		mutex := &watcher.topologyNotifyMtx
		channelsMap := watcher.topologyNotifyChs
		mutex.RLock()
		defer mutex.RUnlock()
		if len(channelsMap) == 0 {
			notification.Recycle()
			return
		}
		watcher.sendNotificationAfterUpdate(notification, channelsMap, TOPOLOGY)
	}
}

func (b *BucketTopologyService) getDcpStatsUpdater(spec *metadata.ReplicationSpecification, watcher *BucketTopologySvcWatcher) func() error {
	dcpStatsFunc := func() error {
		nodes, err := watcher.xdcrCompTopologySvc.MyKVNodes()
		if err != nil {
			return fmt.Errorf("Failed to get my KV nodes, err=%v\n", err)
		}
		if len(nodes) == 0 {
			return base.ErrorNoSourceKV
		}
		dcp_stats := watcher.objsPool.DcpStatsMapPool.Get(nodes)
		userAgent := fmt.Sprintf("Goxdcr BucketTopologyWatcher %v", spec.SourceBucketName)
		var features utils.HELOFeatures
		watcher.kvMemClientsMtx.Lock()
		for _, serverAddr := range nodes {
			// TODO - optimize locking
			client, err := b.utils.GetMemcachedClient(serverAddr, spec.SourceBucketName, watcher.kvMemClients, userAgent, base.KeepAlivePeriod, watcher.logger, features)
			if err != nil {
				watcher.kvMemClientsMtx.Unlock()
				return err
			}

			stats_map := watcher.objsPool.StringStringPool.Get([]string{base.DCP_STAT_NAME})
			err = client.StatsMapForSpecifiedStats(base.DCP_STAT_NAME, *stats_map)
			if err != nil {
				watcher.logger.Warnf("%v Error getting dcp stats for kv %v. err=%v", userAgent, serverAddr, err)
				err1 := client.Close()
				if err1 != nil {
					watcher.logger.Warnf("%v error from closing connection for %v is %v\n", userAgent, serverAddr, err1)
				}
				delete(watcher.kvMemClients, serverAddr)
				watcher.kvMemClientsMtx.Unlock()
				watcher.objsPool.StringStringPool.Put(stats_map)
				watcher.objsPool.DcpStatsMapPool.Put(dcp_stats)
				return err
			} else {
				(*dcp_stats)[serverAddr] = stats_map
			}
		}
		watcher.kvMemClientsMtx.Unlock()
		watcher.latestCacheMtx.Lock()
		replacementNotification := watcher.latestCached.Clone(1).(*Notification)
		replacementNotification.DcpStatsMap = dcp_stats
		watcher.latestCached.Recycle()
		watcher.latestCached = replacementNotification
		watcher.latestCacheMtx.Unlock()
		return nil
	}
	return dcpStatsFunc
}

func (b *BucketTopologyService) getDcpStatsLegacyUpdater(spec *metadata.ReplicationSpecification, watcher *BucketTopologySvcWatcher) func() error {
	dcpStatsFunc := func() error {
		nodes, err := watcher.xdcrCompTopologySvc.MyKVNodes()
		if err != nil {
			return fmt.Errorf("Failed to get my KV nodes, err=%v\n", err)
		}
		if len(nodes) == 0 {
			return base.ErrorNoSourceKV
		}
		dcp_stats := watcher.objsPool.DcpStatsMapPool.Get(nodes)
		userAgent := fmt.Sprintf("Goxdcr BucketTopologyWatcherLegacy %v", spec.SourceBucketName)
		watcher.kvMemClientsLegacyMtx.Lock()
		for _, serverAddr := range nodes {
			// If the remote cluster does not support collections, then only get the stat for default collection
			var features utils.HELOFeatures
			// This is reverse logic because to only get stat for the default collection, we need to enable collection
			// so we can ask specifically for a subset, aka the default collection
			features.Collections = true
			// TODO - optimize locking
			client, err := b.utils.GetMemcachedClient(serverAddr, spec.SourceBucketName, watcher.kvMemClientsLegacy, userAgent, base.KeepAlivePeriod, watcher.logger, features)
			if err != nil {
				watcher.kvMemClientsLegacyMtx.Unlock()
				return err
			}

			stats_map := watcher.objsPool.StringStringPool.Get([]string{base.DCP_STAT_NAME})

			err = client.StatsMapForSpecifiedStats(base.DCP_STAT_NAME, *stats_map)
			if err != nil {
				watcher.logger.Warnf("%v Error getting dcp stats for kv %v. err=%v", userAgent, serverAddr, err)
				err1 := client.Close()
				if err1 != nil {
					watcher.logger.Warnf("%v error from closing connection for %v is %v\n", userAgent, serverAddr, err1)
				}
				delete(watcher.kvMemClientsLegacy, serverAddr)
				watcher.kvMemClientsLegacyMtx.Unlock()
				watcher.objsPool.StringStringPool.Put(stats_map)
				watcher.objsPool.DcpStatsMapPool.Put(dcp_stats)
				return err
			} else {
				(*dcp_stats)[serverAddr] = stats_map
			}
			if delaySec := atomic.LoadUint64(&watcher.devDcpStatsLegacyDelay); delaySec > 0 {
				randFraction := float64(rand.Int()%100) / 100.0 // get a random delay of one second
				timeToSleep := (time.Duration(int(delaySec)) + time.Duration(randFraction)) * time.Second
				time.Sleep(timeToSleep)
			}
		}
		watcher.kvMemClientsLegacyMtx.Unlock()
		watcher.latestCacheMtx.Lock()
		replacementNotification := watcher.latestCached.Clone(1).(*Notification)
		replacementNotification.DcpStatsMapLegacy = dcp_stats
		watcher.latestCached.Recycle()
		watcher.latestCached = replacementNotification
		watcher.latestCacheMtx.Unlock()
		return nil
	}
	watcher.setDevLegacyDelay(spec)
	return dcpStatsFunc
}

// When cluster uses strict encryption, we need to use loopback address for local server
// and set the key in serverVBMap accordingly
func (b *BucketTopologyService) updateLocalServerVBucketMapIfNeeded(serverVBMap *base.KvVBMapType, bucketInfo map[string]interface{}) error {
	if b.xdcrCompTopologySvc.IsMyClusterEncryptionLevelStrict() == false {
		return nil
	}
	loopback := base.LocalHostName
	if base.IsIpV4Blocked() {
		loopback = base.LocalHostNameIpv6
	}
	currentHostAddr, err := b.utils.GetCurrentHostnameFromBucketInfo(bucketInfo)
	if err != nil {
		return err
	}
	currentHostName := base.GetHostName(currentHostAddr)
	if currentHostName == loopback {
		return nil
	}
	for server, vbs := range *serverVBMap {
		hostName := base.GetHostName(server)
		if hostName == currentHostName {
			// Change the map to use loopback
			port, err := base.GetPortNumber(server)
			if err != nil {
				return err
			}
			delete((*serverVBMap), server)
			(*serverVBMap)[base.GetHostAddr(loopback, port)] = vbs
		}
	}
	return nil
}

func (b *BucketTopologyService) getLocalBucketTopologyUpdater(spec *metadata.ReplicationSpecification, watcher *BucketTopologySvcWatcher) func() error {
	topologyFunc := func() error {
		connStr, err := b.xdcrCompTopologySvc.MyConnectionStr()
		if err != nil {
			watcher.logger.Errorf("%v bucket connStr error %v", spec.SourceBucketName, err)
			return err
		}
		bucketInfo := watcher.streamApi.GetResult()

		serversList, err := b.utils.GetServersListFromBucketInfo(bucketInfo)
		if err != nil {
			watcher.logger.Errorf("%v bucketInfo unable to parse server list %v", spec.SourceBucketName, err)
			return err
		}
		serverVBMap, err := b.utils.GetServerVBucketsMap(connStr, spec.SourceBucketName, bucketInfo, watcher.objsPool.KvVbMapPool.Get, serversList)
		if err != nil {
			watcher.logger.Errorf("%v bucket server VBMap error %v", spec.SourceBucketName, err)
			return err
		}
		err = b.updateLocalServerVBucketMapIfNeeded(serverVBMap, bucketInfo)
		if err != nil {
			return fmt.Errorf("%v Failed to update local serverVBucket map. err=%v", spec.SourceBucketName, err)
		}

		nodesList, err := b.utils.GetHostNamesFromBucketInfo(bucketInfo)
		if err != nil {
			return fmt.Errorf("%v Failed to get nodesList from bucketInfo err=%v", spec.SourceBucketName, err)
		}
		replicasMap, translateMap, numOfReplicas, vbReplicaMember, err := b.utils.GetReplicasInfo(bucketInfo, b.securitySvc.IsClusterEncryptionLevelStrict(), watcher.objsPool.StringStringPool.Get(nodesList), watcher.objsPool.VbHostsMapPool.Get, watcher.objsPool.StringSlicePool.Get)
		if err != nil {
			if err != watcher.replicaLastWarnErr {
				watcher.replicaLastWarnErr = err
				watcher.logger.Warnf("%v replicasInfo error %v", spec.SourceBucketName, err)
			}
			// Continue anyway as it is most likely due to node uninit
			replicasMap = watcher.objsPool.VbHostsMapPool.Get(nil).Clear()
			translateMap = watcher.objsPool.StringStringPool.Get(nil).Clear()
			numOfReplicas = 0
			vbReplicaMember = []uint16{}
		} else {
			watcher.replicaLastWarnErr = nil
		}

		nodes, err := watcher.xdcrCompTopologySvc.MyKVNodes()
		if err != nil {
			return fmt.Errorf("Failed to get my KV nodes, err=%v\n", err)
		}
		if len(nodes) == 0 {
			return base.ErrorNoSourceKV
		}

		sourceKvVbMap := watcher.objsPool.KvVbMapPool.Get(nodes)
		for _, node := range nodes {
			if vbnos, ok := (*serverVBMap)[node]; ok {
				(*sourceKvVbMap)[node] = vbnos
			}
		}

		storageBackend, err := b.utils.BucketStorageBackend(bucketInfo)
		if err != nil {
			// This shouldn't happen.
			watcher.logger.Errorf("%v Failed to get source storageBackend. Error=%v", spec.SourceBucketName, err.Error())
		}

		manifestUid, err := b.utils.GetCollectionManifestUidFromBucketInfo(bucketInfo)
		if err != nil {
			// This shouldn't happen.
			watcher.logger.Errorf("%v Failed to get source %v. Error=%v", spec.SourceBucketName, base.CollectionsManifestUidKey, err.Error())
		}

		// In mixed mode, this will return false with an error, which is OK
		crossClusterVer, _ := b.utils.GetCrossClusterVersioningFromBucketInfo(bucketInfo)

		// In mixed mode, this will return 0 with an error, which is OK
		pruningWindownHrs, _ := b.utils.GetVersionPruningWindowHrs(bucketInfo)

		var vbMaxCas []interface{}
		if crossClusterVer {
			vbMaxCas, err = b.utils.GetVbucketsMaxCas(bucketInfo)
			if err != nil {
				// This shouldn't happen.
				watcher.logger.Errorf("%v Failed to get source %v. Error=%v", spec.SourceBucketName, base.VbucketsMaxCasKey, err.Error())
			}
		}

		watcher.latestCacheMtx.Lock()
		if !watcher.cachePopulated {
			watcher.cachePopulated = true
			watcher.source = true
		}
		replacementNotification := watcher.latestCached.Clone(1).(*Notification)
		replacementNotification.KvVbMap = serverVBMap
		replacementNotification.NumberOfSourceNodes = len(*serverVBMap)
		replacementNotification.SourceVBMap = sourceKvVbMap
		replacementNotification.SourceReplicasMap = replicasMap
		replacementNotification.SourceReplicasTranslateMap = translateMap
		replacementNotification.SourceReplicaCnt = numOfReplicas
		replacementNotification.SourceVbReplicasMember = vbReplicaMember
		replacementNotification.SourceStorageBackend = storageBackend
		replacementNotification.SourceCollectioManifestUid = manifestUid
		replacementNotification.LocalBucketTopologyUpdateTime = time.Now()
		replacementNotification.EnableCrossClusterVersioning = crossClusterVer
		replacementNotification.VersionPruningWindowHrs = pruningWindownHrs
		replacementNotification.VbucketsMaxCas = vbMaxCas

		watcher.latestCached.Recycle()
		watcher.latestCached = replacementNotification
		watcher.latestCacheMtx.Unlock()
		return nil
	}
	return topologyFunc
}

func getTargetWatcherKey(spec *metadata.ReplicationSpecification) string {
	return fmt.Sprintf("%v_%v", spec.TargetClusterUUID, spec.TargetBucketName)
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
		return nil, fmt.Errorf("Unable to find remote cluster reference for spec %v", spec.Id)
	}

	bucketInfoGetter, err := b.remClusterSvc.GetBucketInfoGetter(ref, spec.TargetBucketName)
	if err != nil {
		return nil, fmt.Errorf("Unable to get remote bucketInfo getter for spec %v", spec.Id)
	}

	b.tgtBucketWatchersMtx.Lock()
	defer b.tgtBucketWatchersMtx.Unlock()
	watcher, exists := b.tgtBucketWatchers[getTargetWatcherKey(spec)]
	if !exists {
		watcher = NewBucketTopologySvcWatcher(spec.TargetBucketName, spec.TargetBucketUUID, b.logger, false, b.xdcrCompTopologySvc)
		b.tgtBucketWatchers[getTargetWatcherKey(spec)] = watcher

		topologyUpdateFunc := func() error {
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
			perUpdateRef, err := b.remClusterSvc.RemoteClusterByUuid(spec.TargetClusterUUID, false)
			if err != nil {
				return err
			}

			nodesList, err := b.utils.GetHostNamesFromBucketInfo(targetBucketInfo)
			if err != nil {
				return err
			}

			replicasMap, translateMap, numOfReplicas, vbReplicaMember, err := b.utils.GetReplicasInfo(targetBucketInfo, perUpdateRef.IsHttps(), watcher.objsPool.StringStringPool.Get(nodesList), watcher.objsPool.VbHostsMapPool.Get, watcher.objsPool.StringSlicePool.Get)
			if err != nil {
				if err != watcher.replicaLastWarnErr {
					watcher.replicaLastWarnErr = err
					watcher.logger.Warnf("%v target replicasInfo error %v", spec.TargetBucketName, err)
				}
				// Odd error but continue anyway
				replicasMap = watcher.objsPool.VbHostsMapPool.Get(nil).Clear()
				translateMap = watcher.objsPool.StringStringPool.Get(nil).Clear()
				numOfReplicas = 0
				vbReplicaMember = []uint16{}
			} else {
				watcher.replicaLastWarnErr = nil
			}

			storageBackend, err := b.utils.BucketStorageBackend(targetBucketInfo)
			if err != nil {
				// This can happen if target is older version
				storageBackend = ""
			}

			watcher.latestCacheMtx.Lock()
			if !watcher.cachePopulated {
				watcher.cachePopulated = true
				watcher.source = false
			}

			replacementNotification := watcher.latestCached.Clone(1).(*Notification)
			replacementNotification.TargetServerVBMap = (*base.KvVBMapType)(&targetServerVBMap)
			replacementNotification.TargetBucketUUID = targetBucketUUID
			replacementNotification.TargetBucketInfo = (base.BucketInfoMapType)(targetBucketInfo)
			replacementNotification.TargetReplicasMap = replicasMap
			replacementNotification.TargetReplicasTranslateMap = translateMap
			replacementNotification.TargetReplicaCnt = numOfReplicas
			replacementNotification.TargetVbReplicasMember = vbReplicaMember
			replacementNotification.TargetStorageBackend = storageBackend
			watcher.latestCached.Recycle()
			watcher.latestCached = replacementNotification
			watcher.latestCacheMtx.Unlock()
			return nil
		}
		intervalFuncMap := make(IntervalFuncMap)
		intervalFuncMap[TOPOLOGY] = make(IntervalInnerFuncMap)
		intervalFuncMap[TOPOLOGY][b.refreshInterval] = topologyUpdateFunc

		watcher.intervalFuncMap = intervalFuncMap
	}
	b.tgtBucketWatchersCnt[getTargetWatcherKey(spec)]++
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
	watcher, exists := b.tgtBucketWatchers[getTargetWatcherKey(spec)]
	if exists {
		return watcher.registerAndGetCh(spec, subscriberId, TOPOLOGY, nil).(chan service_def.TargetNotification), nil
	}

	return nil, fmt.Errorf("SubscribeToRemoteBucketFeed could not find watcher for %v", spec.TargetBucketName)
}

func (b *BucketTopologyService) UnSubscribeLocalBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) error {
	return b.unSubscribeLocalInternal(spec, subscriberId, TOPOLOGY)
}

func (b *BucketTopologyService) handleSpecDeletion(spec *metadata.ReplicationSpecification) error {
	if spec == nil {
		return base.ErrorNilPtr
	}

	var err error
	b.srcBucketWatchersMtx.Lock()
	b.srcBucketWatchersCnt[spec.SourceBucketName]--
	b.srcBucketWatchers[spec.SourceBucketName].handleSpecDeletion(spec.Id)
	if b.srcBucketWatchersCnt[spec.SourceBucketName] == 0 {
		err = b.srcBucketWatchers[spec.SourceBucketName].Stop()
		delete(b.srcBucketWatchers, spec.SourceBucketName)
		delete(b.srcBucketWatchersCnt, spec.SourceBucketName)
	}
	b.srcBucketWatchersMtx.Unlock()

	b.tgtBucketWatchersMtx.Lock()
	b.tgtBucketWatchersCnt[getTargetWatcherKey(spec)]--
	if b.tgtBucketWatchersCnt[getTargetWatcherKey(spec)] == 0 {
		err = b.tgtBucketWatchers[getTargetWatcherKey(spec)].Stop()
		delete(b.tgtBucketWatchers, getTargetWatcherKey(spec))
		delete(b.tgtBucketWatchersCnt, getTargetWatcherKey(spec))
	}
	b.tgtBucketWatchersMtx.Unlock()

	return err
}

func (b *BucketTopologyService) UnSubscribeRemoteBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) error {
	if spec == nil {
		return base.ErrorNilPtr
	}

	b.tgtBucketWatchersMtx.RLock()
	_, exists := b.tgtBucketWatchersCnt[getTargetWatcherKey(spec)]
	_, exists2 := b.tgtBucketWatchers[getTargetWatcherKey(spec)]
	if !exists || !exists2 {
		b.tgtBucketWatchersMtx.RUnlock()
		return base.ErrorResourceDoesNotExist
	}
	b.tgtBucketWatchers[getTargetWatcherKey(spec)].unregisterCh(spec, subscriberId, TOPOLOGY)
	b.tgtBucketWatchersMtx.RUnlock()
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
		var waitGrp sync.WaitGroup
		waitGrp.Add(2)

		go func() {
			defer waitGrp.Done()

			localRetryOp := func() error {
				watcher := b.getOrCreateLocalWatcher(newSpec)
				if watcher == nil {
					return base.ErrorNilPtr
				}
				localStartErr := watcher.Start()
				if localStartErr != nil && localStartErr != ErrorWatcherAlreadyStarted {
					b.logger.Errorf("Error starting local watcher for %v - %v", newSpec, localStartErr)
					return localStartErr
				}
				return nil
			}
			retryErr := b.utils.ExponentialBackoffExecutor("BucketTopologyServiceLoadSpecLocal",
				base.DefaultHttpTimeoutWaitTime, base.DefaultHttpTimeoutMaxRetry, base.DefaultHttpTimeoutRetryFactor, localRetryOp)
			if retryErr != nil {
				panic(fmt.Sprintf("Bucket Topology service (local) bootstrapping %v did not finish within %v and XDCR must restart to try again", newSpec.Id, base.DefaultHttpTimeout))
			}
		}()

		go func() {
			defer waitGrp.Done()
			retryRemoteOp := func() error {
				remoteWatcher, remoteErr := b.getOrCreateRemoteWatcher(newSpec)
				if remoteErr != nil {
					b.logger.Errorf("Error getting remote watcher for %v - %v", newSpec, remoteErr)
					return remoteErr
				}
				remoteStartErr := remoteWatcher.Start()
				if remoteStartErr != nil && remoteStartErr != ErrorWatcherAlreadyStarted {
					b.logger.Errorf("Error starting remote watcher for %v - %v", newSpec, remoteStartErr)
					return remoteStartErr
				}
				return nil
			}
			retryErr := b.utils.ExponentialBackoffExecutor("BucketTopologyServiceLoadSpecRemote",
				base.DefaultHttpTimeoutWaitTime, base.DefaultHttpTimeoutMaxRetry, base.DefaultHttpTimeoutRetryFactor, retryRemoteOp)
			if retryErr != nil {
				panic(fmt.Sprintf("Bucket Topology service (remote) bootstrapping %v did not finish within %v and XDCR must restart to try again", newSpec.Id, base.DefaultHttpTimeout))
			}
		}()
		waitGrp.Wait()
		b.logger.Infof("Registered bucket monitor for %v", newSpec.Id)
	} else if oldSpec != nil && newSpec == nil {
		err := b.handleSpecDeletion(oldSpec)
		if err != nil {
			b.logger.Errorf("Unable to unregister local bucket for %v", oldSpec)
			return err
		}
		b.logger.Infof("Unregistered bucket monitor for %v", oldSpec.Id)
	} else {
		// Changes
		b.checkAndUpdateSpecSettingChanges(oldSpec, newSpec)
	}

	return nil
}

func (b *BucketTopologyService) SubscribeToLocalBucketDcpStatsFeed(spec *metadata.ReplicationSpecification, subscriberId string) (chan service_def.SourceNotification, error) {
	if spec == nil {
		return nil, base.ErrorNilPtr
	}

	if spec.SourceBucketName == "" {
		return nil, fmt.Errorf("Empty source bucket name for spec %v", spec.Id)
	}

	if isKvNode, isKvNodeErr := b.xdcrCompTopologySvc.IsKVNode(); isKvNodeErr == nil && !isKvNode {
		return nil, base.ErrorNoSourceNozzle
	}

	b.srcBucketWatchersMtx.Lock()
	defer b.srcBucketWatchersMtx.Unlock()
	watcher, exists := b.srcBucketWatchers[spec.SourceBucketName]
	if exists {
		retCh := watcher.registerAndGetCh(spec, subscriberId, DCPSTATSCHECK, nil).(chan service_def.SourceNotification)
		return retCh, nil
	}
	return nil, fmt.Errorf("SubscribeToLocalBucketDcpStatsFeed could not find watcher for %v", spec.SourceBucketName)
}

func (b *BucketTopologyService) SubscribeToLocalBucketDcpStatsLegacyFeed(spec *metadata.ReplicationSpecification, subscriberId string) (chan service_def.SourceNotification, error) {
	if spec == nil {
		return nil, base.ErrorNilPtr
	}

	if spec.SourceBucketName == "" {
		return nil, fmt.Errorf("Empty source bucket name for spec %v", spec.Id)
	}

	if isKvNode, isKvNodeErr := b.xdcrCompTopologySvc.IsKVNode(); isKvNodeErr == nil && !isKvNode {
		return nil, base.ErrorNoSourceNozzle
	}

	b.srcBucketWatchersMtx.Lock()
	defer b.srcBucketWatchersMtx.Unlock()
	watcher, exists := b.srcBucketWatchers[spec.SourceBucketName]
	if exists {
		retCh := watcher.registerAndGetCh(spec, subscriberId, DCPSTATSCHECKLEGACY, nil).(chan service_def.SourceNotification)
		return retCh, nil
	}
	return nil, fmt.Errorf("SubscribeToLocalBucketDcpStatsLegacyFeed could not find watcher for %v", spec.SourceBucketName)
}

func (b *BucketTopologyService) UnSubscribeToLocalBucketDcpStatsFeed(spec *metadata.ReplicationSpecification, subscriberId string) error {
	return b.unSubscribeLocalInternal(spec, subscriberId, DCPSTATSCHECK)
}

func (b *BucketTopologyService) UnSubscribeToLocalBucketDcpStatsLegacyFeed(spec *metadata.ReplicationSpecification, subscriberId string) error {
	return b.unSubscribeLocalInternal(spec, subscriberId, DCPSTATSCHECKLEGACY)
}

func (b *BucketTopologyService) UnSubscribeToLocalBucketHighSeqnosFeed(spec *metadata.ReplicationSpecification, subscriberId string) error {
	return b.unSubscribeLocalInternal(spec, subscriberId, HIGHSEQNOS)
}

func (b *BucketTopologyService) UnSubscribeToLocalBucketHighSeqnosLegacyFeed(spec *metadata.ReplicationSpecification, subscriberId string) error {
	return b.unSubscribeLocalInternal(spec, subscriberId, HIGHSEQNOSLEGACY)
}

func (b *BucketTopologyService) unSubscribeLocalInternal(spec *metadata.ReplicationSpecification, subscriberId string, feedType string) error {
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
	b.srcBucketWatchers[spec.SourceBucketName].unregisterCh(spec, subscriberId, feedType)
	b.srcBucketWatchersMtx.RUnlock()
	return nil
}

func (b *BucketTopologyService) getHighSeqnosUpdater(spec *metadata.ReplicationSpecification, watcher *BucketTopologySvcWatcher, legacyMode bool) func() error {
	updaterFunc := func() error {
		settings := spec.Settings.ToMap(false)
		metadata.GetSettingFromSettingsMap(settings, metadata.PipelineStatsIntervalKey, spec.GetReplicationSpec().Settings.StatsInterval)
		var collectionIds []uint32
		var features utils.HELOFeatures
		features.CompressionType = base.CompressionTypeNone
		watcher.latestCacheMtx.RLock()
		if !watcher.cachePopulated {
			watcher.latestCacheMtx.RUnlock()
			return fmt.Errorf("Cache is not populated yet to get highseqnos")
		}
		kv_vb_map := watcher.latestCached.SourceVBMap.Clone()
		watcher.latestCacheMtx.RUnlock()

		if legacyMode {
			// This is reverse logic because to only get stat for the default collection, we need to enable collection
			// so we can ask specifically for a subset, aka the default collection
			features.Collections = true
			collectionIds = append(collectionIds, base.DefaultCollectionId)
		}

		highseqno_map := watcher.objsPool.HighSeqnosMapPool.Get(kv_vb_map.GetKeyList())
		userAgent := fmt.Sprintf("Goxdcr BucketTopologyWatcher %v", spec.SourceBucketName)
		var freeInCaseOfErrList []*map[uint16]uint64

		// Temp notification is used for if partial VBs are not parsed
		watcher.latestCacheMtx.RLock()
		temporaryNotification := watcher.latestCached.Clone(1).(*Notification)
		watcher.latestCacheMtx.RUnlock()
		defer temporaryNotification.Recycle()

		watcher.kvMemClientsMtx.Lock()
		for serverAddr, vbnos := range kv_vb_map {
			// TODO - optimizing locking
			client, err := b.utils.GetMemcachedClient(serverAddr, spec.SourceBucketName, watcher.kvMemClients, userAgent, base.KeepAlivePeriod, watcher.logger, features)
			if err != nil {
				watcher.kvMemClientsMtx.Unlock()
				for _, toRecycleMap := range freeInCaseOfErrList {
					watcher.objsPool.VbSeqnoMapPool.Put(toRecycleMap)
				}
				watcher.objsPool.HighSeqnosMapPool.Put(highseqno_map)
				return err
			}

			oneSeqnoMap := watcher.objsPool.VbSeqnoMapPool.Get(vbnos)
			freeInCaseOfErrList = append(freeInCaseOfErrList, oneSeqnoMap)
			_, updatedStatsMap, vbsUnableToParse, err := b.utils.GetHighSeqNos(vbnos, client, &watcher.statsMap, collectionIds, oneSeqnoMap)
			if err != nil {
				if err == base.ErrorNoVbSpecified {
					err = fmt.Errorf("KV node %v has no vbucket assigned to it", serverAddr)
				}
				// We should really return err if nothing could be parsed, because this means the update() call has
				// failed and subscribers will not hear back any event at all
				watcher.logger.Warnf("%v Error getting high seqno for kv %v. err=%v", userAgent, serverAddr, err)
				err1 := client.Close()
				if err1 != nil {
					watcher.logger.Warnf("%v error from closing connection for %v is %v\n", userAgent, serverAddr, err1)
				}
				delete(watcher.kvMemClients, serverAddr)
				watcher.kvMemClientsMtx.Unlock()
				for _, toRecycleMap := range freeInCaseOfErrList {
					watcher.objsPool.VbSeqnoMapPool.Put(toRecycleMap)
				}
				watcher.objsPool.HighSeqnosMapPool.Put(highseqno_map)
				return err
			} else {
				if len(vbsUnableToParse) > 0 && temporaryNotification != nil {
					// If partial VBs were not able to be parsed (i.e stuck rebalancing, etc), and we have old data,
					// then we could try our best to set so that the stats will be as accurate as possible...
					// instead of getting stuck, use the previous instance of the statsMap to find old data
					lastCachedData := temporaryNotification.HighSeqnoMap
					if lastCachedData == nil || len(*lastCachedData) == 0 || (*lastCachedData)[serverAddr] == nil {
						// cache not instantiated yet... just use whatever we have and do replacement at the end
					} else {
						lastSeqnoMap := (*lastCachedData)[serverAddr]
						for _, failedVB := range vbsUnableToParse {
							var seqnoToReuse uint64
							if lastSeqnoMap != nil {
								if seqno, exists := (*lastSeqnoMap)[failedVB]; exists {
									seqnoToReuse = seqno
								}
							}
							(*oneSeqnoMap)[failedVB] = seqnoToReuse
						}
					}
				}
				watcher.statsMap = *updatedStatsMap
				(*highseqno_map)[serverAddr] = oneSeqnoMap
				watcher.kvMemClients[serverAddr] = client
			}
		}
		watcher.kvMemClientsMtx.Unlock()

		watcher.latestCacheMtx.Lock()
		replacementNotification := watcher.latestCached.Clone(1).(*Notification)
		if legacyMode {
			replacementNotification.HighSeqnoMapLegacy = highseqno_map
		} else {
			replacementNotification.HighSeqnoMap = highseqno_map
		}
		watcher.latestCached.Recycle()
		watcher.latestCached = replacementNotification
		watcher.latestCacheMtx.Unlock()
		return nil
	}
	return updaterFunc
}

// Returns a duration updater
func (b *BucketTopologyService) SubscribeToLocalBucketHighSeqnosFeed(spec *metadata.ReplicationSpecification, subscriberId string, requestedInterval time.Duration) (chan service_def.SourceNotification, func(time.Duration), error) {
	if spec == nil {
		return nil, nil, base.ErrorNilPtr
	}

	if spec.SourceBucketName == "" {
		return nil, nil, fmt.Errorf("Empty source bucket name for spec %v", spec.Id)
	}

	if isKvNode, isKvNodeErr := b.xdcrCompTopologySvc.IsKVNode(); isKvNodeErr == nil && !isKvNode {
		return nil, nil, base.ErrorNoSourceNozzle
	}

	b.srcBucketWatchersMtx.Lock()
	defer b.srcBucketWatchersMtx.Unlock()
	watcher, exists := b.srcBucketWatchers[spec.SourceBucketName]
	if exists {
		intOpts := HighSeqnosOpts{
			Spec:              spec,
			RequestedInterval: requestedInterval,
			SubscriberId:      subscriberId,
		}
		retCh := watcher.registerAndGetCh(spec, subscriberId, HIGHSEQNOS, intOpts).(chan service_def.SourceNotification)
		intervalUpdateFunc := func(newInterval time.Duration) {
			updateOpts := HighSeqnosOpts{
				Spec:              spec,
				RequestedInterval: newInterval,
				SubscriberId:      subscriberId,
			}
			err := watcher.updateSettings(HIGHSEQNOS, updateOpts)
			if err != nil {
				b.logger.Errorf("Unable to update watcher setting for %v given opts %v", HIGHSEQNOS, updateOpts)
			}
		}
		return retCh, intervalUpdateFunc, nil
	}
	return nil, nil, fmt.Errorf("SubscribeToLocalBucketHighSeqnosFeed could not find watcher for %v", spec.SourceBucketName)
}

// Returns a duration updater
func (b *BucketTopologyService) SubscribeToLocalBucketHighSeqnosLegacyFeed(spec *metadata.ReplicationSpecification, subscriberId string, requestedInterval time.Duration) (chan service_def.SourceNotification, func(time.Duration), error) {
	if spec == nil {
		return nil, nil, base.ErrorNilPtr
	}

	if spec.SourceBucketName == "" {
		return nil, nil, fmt.Errorf("Empty source bucket name for spec %v", spec.Id)
	}

	if isKvNode, isKvNodeErr := b.xdcrCompTopologySvc.IsKVNode(); isKvNodeErr == nil && !isKvNode {
		return nil, nil, base.ErrorNoSourceNozzle
	}

	b.srcBucketWatchersMtx.Lock()
	defer b.srcBucketWatchersMtx.Unlock()
	watcher, exists := b.srcBucketWatchers[spec.SourceBucketName]
	if exists {
		intOpts := HighSeqnosOpts{
			Spec:              spec,
			RequestedInterval: requestedInterval,
			SubscriberId:      subscriberId,
		}
		retCh := watcher.registerAndGetCh(spec, subscriberId, HIGHSEQNOSLEGACY, intOpts).(chan service_def.SourceNotification)
		intervalUpdateFunc := func(newInterval time.Duration) {
			updateOpts := HighSeqnosOpts{
				Spec:              spec,
				RequestedInterval: newInterval,
				SubscriberId:      subscriberId,
			}
			err := watcher.updateSettings(HIGHSEQNOSLEGACY, updateOpts)
			if err != nil {
				b.logger.Errorf("Unable to update watcher setting for %v given opts %v", HIGHSEQNOSLEGACY, updateOpts)
			}
		}
		return retCh, intervalUpdateFunc, nil
	}
	return nil, nil, fmt.Errorf("SubscribeToLocalBucketHighSeqnosLegacyFeed could not find watcher for %v", spec.SourceBucketName)
}

func (b *BucketTopologyService) RegisterGarbageCollect(specId string, srcBucketName string, vbno uint16, requestId string, gcFunc func() error, timeToFire time.Duration) error {
	if specId == "" {
		return base.ErrorInvalidInput
	}
	b.srcBucketWatchersMtx.RLock()
	watcher, exists := b.srcBucketWatchers[srcBucketName]
	b.srcBucketWatchersMtx.RUnlock()
	if !exists {
		return base.ErrorNotFound
	}
	return watcher.RegisterGarbageCollect(specId, vbno, requestId, gcFunc, timeToFire)
}

func (b *BucketTopologyService) checkAndUpdateSpecSettingChanges(oldSpec *metadata.ReplicationSpecification, newSpec *metadata.ReplicationSpecification) {
	// For now, only dev delay changes
	if newSpec == nil || newSpec.Settings == nil {
		return
	}
	delaySec := newSpec.Settings.GetIntSettingValue(metadata.DevBucketTopologyLegacyDelay)
	if delaySec == 0 {
		return
	}

	b.srcBucketWatchersMtx.RLock()
	defer b.srcBucketWatchersMtx.RUnlock()
	watcher, exists := b.srcBucketWatchers[newSpec.SourceBucketName]
	if !exists {
		return
	}
	watcher.setDevLegacyDelay(newSpec)
}

type BucketTopologySvcWatcher struct {
	bucketName string
	bucketUUID string
	source     bool

	finCh     chan bool
	startOnce sync.Once

	// Key is a "spec + subscriber ID"
	topologyNotifyMtx      sync.RWMutex
	topologyNotifyChs      map[string]interface{}
	dcpStatsMtx            sync.RWMutex
	dcpStatsChs            map[string]interface{}
	dcpStatsLegacyMtx      sync.RWMutex
	dcpStatsLegacyChs      map[string]interface{}
	highSeqnosChsMtx       sync.RWMutex
	highSeqnosChs          map[string]interface{}
	highSeqnosLegacyChsMtx sync.RWMutex
	highSeqnosLegacyChs    map[string]interface{}

	latestCacheMtx sync.RWMutex
	latestCached   *Notification
	cachePopulated bool

	logger       *log.CommonLogger
	firstToStart uint32
	isStarted    uint32
	isStopped    uint32

	xdcrCompTopologySvc service_def.XDCRCompTopologySvc

	// For bootstrapping
	intervalFuncMap IntervalFuncMap

	// For runtime
	watchersTickersMap      WatchersTickerMap
	watchersTickersValueMap WatchersTickerValueMap
	watchersTickersMapMtx   sync.RWMutex

	// For DCP stats
	kvMemClients    map[string]mcc.ClientIface
	kvMemClientsMtx sync.Mutex

	kvMemClientsLegacy    map[string]mcc.ClientIface
	kvMemClientsLegacyMtx sync.Mutex

	// Used internally to call KV to prevent garbage - must only be used by one updater goroutine
	statsMap map[string]string

	// For highSeqnos and highSeqnosLegacy, intervals need to be changed dynamically
	// The ideas here is to separate operation into the actual GET op and the passing of data to the receiver
	// The GET op is dictated by the subscriber who wants it the most frequently, and then only pass to
	// each individual receiver as to not overwhelm it
	// Each key is a replication and each replication has a desired time interval to receive data
	// The idea is watcher need to call REST API's at the smallest duration (highSeqnosIntervals)
	// But only feed the latest data back a specific requester only if the requester expects it
	// ... i.e the channel whenever a ticker has fired (highSeqnosReceiverFired)
	highSeqnosIntervals     map[string]time.Duration
	highSeqnosReceiverFired map[string]*time.Ticker // Key is replId + subscriberID
	highSeqnosTrackersMtx   sync.RWMutex

	highSeqnosIntervalsLegacy     map[string]time.Duration
	highSeqnosReceiverFiredLegacy map[string]*time.Ticker // Key is replId + subscriberID
	highSeqnosTrackersMtxLegacy   sync.RWMutex

	// Only applicable to source watcher
	gcMapMtx sync.RWMutex
	// ReplicationId -> vbno -> requestId
	gcMap             GcMapType
	gcPruneMap        GcMapPruneMapType // To remove really outdated gc requests
	gcMapUndergoingGc bool
	gcMapGcAbort      bool
	gcPruneWindow     time.Duration

	objsPool *BucketTopologyObjsPool

	replicaLastWarnErr error

	nonKVNodeLastTimeWarned    time.Time
	nonKVNodeLastTimeWarnedMtx sync.Mutex

	streamApi streamApiWatcher.StreamApiWatcher

	devDcpStatsLegacyDelay uint64
}

type GcMapType map[string]VbnoReqMapType
type VbnoReqMapType map[uint16]RequestMapType
type RequestMapType map[string]*GcRequest

type GcMapPruneMapType map[string]VbnoPruneMapType
type VbnoPruneMapType map[uint16]time.Time

// IntervalFuncMap is keyed by type
// Each type will have a single len IntervalInnerFuncMap
type IntervalFuncMap map[string]IntervalInnerFuncMap
type IntervalInnerFuncMap map[time.Duration]func() error

type WatchersCallbacksFuncMap map[string]func(opts interface{}) error
type WatchersTickerMap map[string]*time.Ticker
type WatchersTickerValueMap map[string]time.Duration

// Legacy below is needed to replicate to target cluster that is < 7.0
// Once <7.0 is EOL'ed, then LEGACY can be removed
const (
	TOPOLOGY            = "topology"
	DCPSTATSCHECK       = "dcpStats"
	DCPSTATSCHECKLEGACY = "dcpStatsLegacy"
	HIGHSEQNOS          = "vbHighSeqnos"
	HIGHSEQNOSLEGACY    = "vbHighSeqnosLegacy" // Legacy means it only receives default collection high seqnos
)

type HighSeqnosOpts struct {
	Spec              *metadata.ReplicationSpecification
	RequestedInterval time.Duration
	SubscriberId      string
}

type GcRequest struct {
	timeToFire time.Time
	funcToFire func() error
}

func NewBucketTopologySvcWatcher(bucketName, bucketUuid string, logger *log.CommonLogger, source bool, xdcrCompTopologySvc service_def.XDCRCompTopologySvc) *BucketTopologySvcWatcher {
	sharedPool := NewBucketTopologyObjsPool()
	watcher := &BucketTopologySvcWatcher{
		bucketName:                    bucketName,
		bucketUUID:                    bucketUuid,
		finCh:                         make(chan bool),
		logger:                        logger,
		source:                        source,
		xdcrCompTopologySvc:           xdcrCompTopologySvc,
		latestCached:                  NewNotification(source, sharedPool),
		topologyNotifyChs:             make(map[string]interface{}),
		kvMemClients:                  map[string]mcc.ClientIface{},
		kvMemClientsLegacy:            map[string]mcc.ClientIface{},
		dcpStatsChs:                   map[string]interface{}{},
		dcpStatsLegacyChs:             map[string]interface{}{},
		statsMap:                      nil,
		watchersTickersMap:            WatchersTickerMap{},
		watchersTickersValueMap:       WatchersTickerValueMap{},
		highSeqnosChs:                 map[string]interface{}{},
		highSeqnosLegacyChs:           map[string]interface{}{},
		highSeqnosIntervals:           map[string]time.Duration{},
		highSeqnosReceiverFired:       map[string]*time.Ticker{},
		highSeqnosIntervalsLegacy:     map[string]time.Duration{},
		highSeqnosReceiverFiredLegacy: map[string]*time.Ticker{},
		gcMap:                         GcMapType{},
		gcPruneMap:                    GcMapPruneMapType{},
		gcPruneWindow:                 base.BucketTopologyGCPruneTime,
		objsPool:                      sharedPool,
	}
	watcher.latestCached.SetNumberOfReaders(1)
	return watcher
}

var ErrorWatcherAlreadyStarted = fmt.Errorf("Watcher is already started")

func (bw *BucketTopologySvcWatcher) Start() error {
	if atomic.CompareAndSwapUint32(&bw.firstToStart, 0, 1) {
		var initDone sync.WaitGroup
		initDone.Add(1)
		if bw.streamApi != nil {
			bw.streamApi.Start()
		}
		go bw.run(&initDone)
		initDone.Wait()
		atomic.StoreUint32(&bw.isStarted, 1)
		return nil
	}
	return ErrorWatcherAlreadyStarted
}

func (bw *BucketTopologySvcWatcher) run(initDone *sync.WaitGroup) {
	bw.logger.Infof("Starting watcher for local? %v bucket of %v with UUID %v", bw.source, bw.bucketName, bw.bucketUUID)
	defer bw.logger.Infof("Stopped watcher for local? %v bucket of %v with UUID %v", bw.source, bw.bucketName, bw.bucketUUID)

	if bw.source {
		go func() {
			scanTicker := time.NewTicker(base.BucketTopologyGCScanTime)
			for {
				select {
				case <-bw.finCh:
					return
				case <-scanTicker.C:
					bw.runGC()
				}
			}
		}()
	}

	for updateType, intervalAndFunc := range bw.intervalFuncMap {
		for interval, updateFunc := range intervalAndFunc {
			// Make copies because these references can change before the go func() is executed
			updateTypeCpy := updateType
			intervalCpy := interval
			funcCpy := updateFunc
			initDone.Add(1)
			go func() {
				initCh := make(chan bool, 1)
				initCh <- true
				ticker := time.NewTicker(intervalCpy)
				bw.watchersTickersMapMtx.Lock()
				bw.watchersTickersMap[updateTypeCpy] = ticker
				bw.watchersTickersValueMap[updateTypeCpy] = intervalCpy
				bw.watchersTickersMapMtx.Unlock()
				for {
					select {
					case <-bw.finCh:
						return
					case <-initCh:
						bw.updateOnce(updateTypeCpy, funcCpy)
						initDone.Done()
					case <-ticker.C:
						bw.updateOnce(updateTypeCpy, funcCpy)
					}
				}
			}()
		}
	}
	initDone.Done()

	for {
		select {
		case <-bw.finCh:
			bw.closeKvConns()
			return
		}
	}
}

func (bw *BucketTopologySvcWatcher) runGC() {
	var lastPopulatedCache service_def.SourceNotification
	bw.latestCacheMtx.RLock()
	populated := bw.cachePopulated
	bw.latestCacheMtx.RUnlock()
	if !populated {
		// Don't do anything
		return
	}
	// myself as a reader
	bw.latestCacheMtx.RLock()
	lastPopulatedCache = bw.latestCached.Clone(1).(service_def.SourceNotification)
	bw.latestCacheMtx.RUnlock()
	defer lastPopulatedCache.Recycle()

	srcVBMap := lastPopulatedCache.GetSourceVBMapRO()
	if len(srcVBMap) > 1 {
		// Not supposed to happen
		var keys []string
		for key := range srcVBMap {
			keys = append(keys, key)
		}
		bw.logger.Warnf("srcVBMap shows more than one node %v", keys)
	} else if len(srcVBMap) == 0 {
		// Possibly because this node is not a KV node
		return
	}
	var sortedVBsList []uint16

	for _, vbsList := range srcVBMap {
		sortedVBsList = base.SortUint16List(base.CloneUint16List(vbsList))
	}

	var toBeDeleted []func()
	bw.gcMapMtx.Lock()
	bw.gcMapUndergoingGc = true
	bw.gcMapMtx.Unlock()

	// Ensure that this variable MUST be set to false when this function is done
	defer func() {
		bw.gcMapMtx.Lock()
		bw.gcMapUndergoingGc = false
		bw.gcMapMtx.Unlock()
	}()

	bw.gcMapMtx.RLock()
	if bw.gcMapGcAbort {
		bw.gcMapMtx.RUnlock()
		return
	}
	for specIdPreCopy, gcForEachSpec := range bw.gcMap {
		specId := specIdPreCopy
		for vbnoPreCopy, idMap := range gcForEachSpec {
			vbno := vbnoPreCopy
			// See if vb still belongs to this node
			// If GetSourceVBMapRO() changes to have more than this source node, need to update logic
			_, vbStillExists := base.SearchVBInSortedList(vbno, sortedVBsList)
			if vbStillExists {
				// Don't do any GC since this node still owns the VB and/or has re-owned the VB
				// However, check to see if this VB has been owned (i.e. never been re-registered)
				// for an Expiry period. If so, this means that all these registered requests
				// are out of date and should be removed
				lastRegisteredTime := bw.gcPruneMap[specId][vbno]
				if time.Now().After(lastRegisteredTime.Add(bw.gcPruneWindow)) {
					toBeDeleted = append(toBeDeleted, func() {
						// Clean up the prune time
						delete(bw.gcPruneMap[specId], vbno)
						if len(bw.gcPruneMap[specId]) == 0 {
							delete(bw.gcPruneMap, specId)
						}
						// Clean up this VB for this spec
						delete(bw.gcMap[specId], vbno)
						if len(bw.gcMap[specId]) == 0 {
							delete(bw.gcMap, specId)
						}
					})
				}
				continue
			}
			numberOfIdRequests := len(idMap)
			var numGCFired int
			var individualIdDelete []func()

			// Perform GC
			for id, req := range idMap {
				if time.Now().After(req.timeToFire) {
					numGCFired++
					err := req.funcToFire()
					if err != nil {
						bw.logger.Warnf("GC for spec %v vbno %v id %v resulted in err %v",
							specId, vbno, id, err)
					}
					individualIdDelete = append(individualIdDelete, func() {
						delete(bw.gcMap[specId][vbno], id)
					})
				}
			}
			if numGCFired == numberOfIdRequests {
				// Everything fired, no need to keep this around
				toBeDeleted = append(toBeDeleted, func() { delete(bw.gcMap[specId], vbno) })
			} else {
				// Only a few subset need to be removed
				toBeDeleted = append(toBeDeleted, individualIdDelete...)
			}
		}
	}
	bw.gcMapMtx.RUnlock()

	bw.gcMapMtx.Lock()
	if !bw.gcMapGcAbort {
		for _, delFunc := range toBeDeleted {
			delFunc()
		}
	}
	bw.gcMapMtx.Unlock()
}

func (bw *BucketTopologySvcWatcher) updateOnce(updateType string, customUpdateFunc func() error) {
	if isKvNode, isKvNodeErr := bw.xdcrCompTopologySvc.IsKVNode(); isKvNodeErr == nil && !isKvNode {
		logAgainUnix := time.Now().Add(-(1 * time.Hour)).Unix()
		var logAgain bool
		bw.nonKVNodeLastTimeWarnedMtx.Lock()
		lastLoggedTimeUnix := bw.nonKVNodeLastTimeWarned.Unix()
		if logAgainUnix > lastLoggedTimeUnix {
			logAgain = true
			bw.nonKVNodeLastTimeWarned = time.Now()
		}
		bw.nonKVNodeLastTimeWarnedMtx.Unlock()
		if logAgain {
			bw.logger.Warnf("BucketTopologySvcWather for bucket %v not running because this node is not a KV node", bw.bucketName)
		}
	}

	var mutex *sync.RWMutex
	var channelsMap map[string]interface{}
	switch updateType {
	case TOPOLOGY:
		channelsMap = bw.topologyNotifyChs
		mutex = &bw.topologyNotifyMtx
	case DCPSTATSCHECK:
		channelsMap = bw.dcpStatsChs
		mutex = &bw.dcpStatsMtx
	case DCPSTATSCHECKLEGACY:
		channelsMap = bw.dcpStatsLegacyChs
		mutex = &bw.dcpStatsLegacyMtx
	case HIGHSEQNOS:
		channelsMap = bw.highSeqnosChs
		mutex = &bw.highSeqnosChsMtx
	case HIGHSEQNOSLEGACY:
		channelsMap = bw.highSeqnosLegacyChs
		mutex = &bw.highSeqnosLegacyChsMtx
	default:
		panic(fmt.Sprintf("Unknown type: %v", updateType))
	}

	// If no one is subscribed, no need to run the updater except for
	// - TOPOLOGY, which is for sure needed
	// - HIGHSEQNOS, this is needed for active as well as paused pipeline status which will subscribe, calculate and unsubscribe.
	// If we don't update HIGHSEQNOS when there is no subscriber, paused replication status will not be accurate,
	// and if topology change causing server name change, we will incorrectly report total_changes as 0 and changes_left as negative for paused pipeline.
	if updateType != TOPOLOGY && updateType != HIGHSEQNOS && updateType != HIGHSEQNOSLEGACY {
		mutex.RLock()
		chanLen := len(channelsMap)
		mutex.RUnlock()
		if chanLen == 0 {
			return
		}
	}

	err := customUpdateFunc()
	if err != nil {
		bw.logger.Errorf("BucketTopologySvcWatcher for local? %v bucket %v updating resulted in err %v - bypassing notification", bw.source, bw.bucketName, err)
		return
	}
	bw.latestCacheMtx.RLock()
	notification := bw.latestCached.Clone(1).(*Notification) // Set to 1 by default, changed later
	bw.latestCacheMtx.RUnlock()

	mutex.RLock()
	defer mutex.RUnlock()
	if len(channelsMap) == 0 {
		notification.Recycle()
		return
	}
	bw.sendNotificationAfterUpdate(notification, channelsMap, updateType)
}

func (bw *BucketTopologySvcWatcher) sendNotificationAfterUpdate(notification *Notification, channelsMap map[string]interface{}, updateType string) {
	var waitGrp sync.WaitGroup
	notification.SetNumberOfReaders(len(channelsMap))
	for channelName, chRaw := range channelsMap {
		if !bw.shouldSendToCh(channelName, updateType) {
			notification.Recycle()
			continue
		}

		waitGrp.Add(1)
		go func(chRaw interface{}, isSource bool) {
			defer waitGrp.Done()
			timeout := time.NewTimer(1 * time.Second)
			if isSource {
				select {
				case chRaw.(chan service_def.SourceNotification) <- notification:
					// sent
					timeout.Stop()
				case <-timeout.C:
					// provide a bail out path
					notification.Recycle()
				}
			} else {
				select {
				case chRaw.(chan service_def.TargetNotification) <- notification:
					// sent
					timeout.Stop()
				case <-timeout.C:
					// provide a bail out path
					notification.Recycle()
				}
			}
		}(chRaw, bw.source)
	}
	waitGrp.Wait()
}

func (bw *BucketTopologySvcWatcher) Stop() error {
	if atomic.CompareAndSwapUint32(&bw.isStopped, 0, 1) {
		close(bw.finCh)
		if bw.streamApi != nil {
			bw.streamApi.Stop()
		}
	}
	return nil
}

func (bw *BucketTopologySvcWatcher) registerAndGetCh(spec *metadata.ReplicationSpecification, subscriberId string, chType string, opts interface{}) interface{} {
	for atomic.LoadUint32(&bw.isStarted) == 0 {
		time.Sleep(100 * time.Millisecond)
	}

	var specifiedChs map[string]interface{}
	var mutex *sync.RWMutex
	switch chType {
	case TOPOLOGY:
		specifiedChs = bw.topologyNotifyChs
		mutex = &bw.topologyNotifyMtx
	case DCPSTATSCHECK:
		specifiedChs = bw.dcpStatsChs
		mutex = &bw.dcpStatsMtx
	case DCPSTATSCHECKLEGACY:
		specifiedChs = bw.dcpStatsLegacyChs
		mutex = &bw.dcpStatsLegacyMtx
	case HIGHSEQNOS:
		specifiedChs = bw.highSeqnosChs
		mutex = &bw.highSeqnosChsMtx
		defer bw.setHighSeqnosInterval(opts.(HighSeqnosOpts), false)
	case HIGHSEQNOSLEGACY:
		specifiedChs = bw.highSeqnosLegacyChs
		mutex = &bw.highSeqnosLegacyChsMtx
		defer bw.setHighSeqnosInterval(opts.(HighSeqnosOpts), true)
	default:
		panic(fmt.Sprintf("Unknown type %v", chType))
	}

	fullSubscriberId := compileFullSubscriberId(spec, subscriberId)

	bw.latestCacheMtx.RLock()
	cachePopulated := bw.cachePopulated
	var initialSrcNotification service_def.SourceNotification
	var initialTgtNotification service_def.TargetNotification
	if cachePopulated {
		if bw.source {
			initialSrcNotification = bw.latestCached.Clone(1).(service_def.SourceNotification)
		} else {
			initialTgtNotification = bw.latestCached.Clone(1).(service_def.TargetNotification)
		}
	}
	bw.latestCacheMtx.RUnlock()

	mutex.Lock()
	defer mutex.Unlock()
	if bw.source {
		newCh := make(chan service_def.SourceNotification, base.BucketTopologyWatcherChanLen)
		specifiedChs[fullSubscriberId] = newCh
	} else {
		newCh := make(chan service_def.TargetNotification, base.BucketTopologyWatcherChanLen)
		specifiedChs[fullSubscriberId] = newCh
	}

	// When someone first registers and subscribes, it prob expects some data - feed it the latest if it's not nil
	if cachePopulated {
		if bw.source {
			specifiedChs[fullSubscriberId].(chan service_def.SourceNotification) <- initialSrcNotification
		} else {
			specifiedChs[fullSubscriberId].(chan service_def.TargetNotification) <- initialTgtNotification
		}
	}
	return specifiedChs[fullSubscriberId]
}

func compileFullSubscriberId(spec *metadata.ReplicationSpecification, id string) string {
	return fmt.Sprintf("%v_%v", spec.Id, id)
}

func (bw *BucketTopologySvcWatcher) unregisterCh(spec *metadata.ReplicationSpecification, subscriberId string, chType string) {
	var specifiedChs map[string]interface{}
	var mutex *sync.RWMutex
	switch chType {
	case TOPOLOGY:
		specifiedChs = bw.topologyNotifyChs
		mutex = &bw.topologyNotifyMtx
	case DCPSTATSCHECK:
		specifiedChs = bw.dcpStatsChs
		mutex = &bw.dcpStatsMtx
	case DCPSTATSCHECKLEGACY:
		specifiedChs = bw.dcpStatsLegacyChs
		mutex = &bw.dcpStatsLegacyMtx
	case HIGHSEQNOS:
		specifiedChs = bw.highSeqnosChs
		mutex = &bw.highSeqnosChsMtx
		bw.cleanupHighSeqnosInternalData(spec, subscriberId, false)
	case HIGHSEQNOSLEGACY:
		specifiedChs = bw.highSeqnosLegacyChs
		mutex = &bw.highSeqnosLegacyChsMtx
		bw.cleanupHighSeqnosInternalData(spec, subscriberId, true)
	default:
		panic(fmt.Sprintf("Unknown type %v", chType))
	}

	fullSubscriberId := compileFullSubscriberId(spec, subscriberId)

	mutex.Lock()
	defer mutex.Unlock()

	delete(specifiedChs, fullSubscriberId)
}

func (bw *BucketTopologySvcWatcher) closeKvConns() {
	bw.kvMemClientsMtx.Lock()
	for serverAddr, client := range bw.kvMemClients {
		err := client.Close()
		if err != nil {
			bw.logger.Warnf("%v error from closing connection for %v is %v\n", bw.bucketName, serverAddr, err)
		}
	}
	bw.kvMemClients = make(map[string]mcc.ClientIface)
	bw.kvMemClientsMtx.Unlock()

	bw.kvMemClientsLegacyMtx.Lock()
	for serverAddr, client := range bw.kvMemClientsLegacy {
		err := client.Close()
		if err != nil {
			bw.logger.Warnf("%v error from closing connection for %v is %v\n", bw.bucketName, serverAddr, err)
		}
	}
	bw.kvMemClientsLegacy = make(map[string]mcc.ClientIface)
	bw.kvMemClientsLegacyMtx.Unlock()

}

func (bw *BucketTopologySvcWatcher) updateSettings(chType string, opts interface{}) error {
	switch chType {
	case HIGHSEQNOS:
		bw.setHighSeqnosInterval(opts.(HighSeqnosOpts), false)
		return nil
	case HIGHSEQNOSLEGACY:
		bw.setHighSeqnosInterval(opts.(HighSeqnosOpts), true)
		return nil
	default:
		// Nothing is to be updated
		return base.ErrorNotSupported
	}
}

func (bw *BucketTopologySvcWatcher) setHighSeqnosInterval(opts HighSeqnosOpts, legacy bool) {
	mtx := &bw.highSeqnosTrackersMtx
	subscriberToIntervalMap := bw.highSeqnosIntervals
	receiverFiredMap := bw.highSeqnosReceiverFired
	chType := HIGHSEQNOS
	if legacy {
		mtx = &bw.highSeqnosTrackersMtxLegacy
		subscriberToIntervalMap = bw.highSeqnosIntervalsLegacy
		receiverFiredMap = bw.highSeqnosReceiverFiredLegacy
		chType = HIGHSEQNOSLEGACY
	}

	mtx.Lock()
	fullKey := compileFullSubscriberId(opts.Spec, opts.SubscriberId)

	subscriberToIntervalMap[fullKey] = opts.RequestedInterval
	_, exists := receiverFiredMap[fullKey]
	if !exists {
		receiverFiredMap[fullKey] = time.NewTicker(opts.RequestedInterval)
	} else {
		receiverFiredMap[fullKey].Reset(opts.RequestedInterval)
	}

	shortestInterval := opts.RequestedInterval
	for _, currentIntervalForASubscriber := range subscriberToIntervalMap {
		if currentIntervalForASubscriber < shortestInterval {
			shortestInterval = currentIntervalForASubscriber
		}
	}
	mtx.Unlock()

	bw.setWatchersTickerIfNecessary(opts.Spec.Id, shortestInterval, chType)
}

func (bw *BucketTopologySvcWatcher) setWatchersTickerIfNecessary(specId string, shortestInterval time.Duration, chType string) {
	bw.watchersTickersMapMtx.RLock()
	defer bw.watchersTickersMapMtx.RUnlock()

	if shortestInterval < bw.watchersTickersValueMap[chType] {
		bw.watchersTickersMapMtx.RUnlock()
		bw.watchersTickersMapMtx.Lock()

		// Check again
		if shortestInterval < bw.watchersTickersValueMap[chType] {
			bw.watchersTickersValueMap[chType] = shortestInterval
			bw.watchersTickersMap[chType].Reset(shortestInterval)
			bw.logger.Infof("spec %v Setting overall ticker for %v to %v", specId, chType, shortestInterval)
		}

		bw.watchersTickersMapMtx.Unlock()
		bw.watchersTickersMapMtx.RLock()
	}
}

// Called when a subscriber has unsubscribed and its lowest value no longer applies
func (bw *BucketTopologySvcWatcher) setWatchersTickerAfterUnsubscribe(specId string, newShortestInterval time.Duration, chType string) {
	bw.watchersTickersMapMtx.Lock()
	defer bw.watchersTickersMapMtx.Unlock()

	bw.watchersTickersValueMap[chType] = newShortestInterval
	bw.watchersTickersMap[chType].Reset(newShortestInterval)
	bw.logger.Infof("spec %v Setting overall ticker for %v to %v", specId, chType, newShortestInterval)
}

func (bw *BucketTopologySvcWatcher) shouldSendToCh(name string, updateType string) bool {
	switch updateType {
	case HIGHSEQNOS:
		return bw.checkHighSeqnosReceiverAwaitingData(name, false)
	case HIGHSEQNOSLEGACY:
		return bw.checkHighSeqnosReceiverAwaitingData(name, true)
	default:
		return true
	}
}

func (bw *BucketTopologySvcWatcher) checkHighSeqnosReceiverAwaitingData(name string, legacy bool) bool {
	bw.highSeqnosTrackersMtx.RLock()
	var ticker *time.Ticker
	var found bool
	if !legacy {
		ticker, found = bw.highSeqnosReceiverFired[name]
	} else {
		ticker, found = bw.highSeqnosReceiverFiredLegacy[name]
	}
	bw.highSeqnosTrackersMtx.RUnlock()
	if !found {
		// It is raceful and possible that the timer did not stop in time during unsubscribing
		// and the timer has fired already
		return false
	}
	select {
	case <-ticker.C:
		return true
	default:
		return false
	}
}

func (bw *BucketTopologySvcWatcher) cleanupHighSeqnosInternalData(spec *metadata.ReplicationSpecification, subscriberId string, legacy bool) {
	mtx := &bw.highSeqnosTrackersMtx
	subscriberToIntervalMap := bw.highSeqnosIntervals
	receiverFiredMap := bw.highSeqnosReceiverFired
	chType := HIGHSEQNOS
	if legacy {
		mtx = &bw.highSeqnosTrackersMtxLegacy
		subscriberToIntervalMap = bw.highSeqnosIntervalsLegacy
		receiverFiredMap = bw.highSeqnosReceiverFiredLegacy
		chType = HIGHSEQNOSLEGACY
	}

	mtx.RLock()
	var shortestIntervalBeforeRemoval time.Duration
	var first = true
	for _, currentIntervalForASubscriber := range subscriberToIntervalMap {
		if first {
			shortestIntervalBeforeRemoval = currentIntervalForASubscriber
			first = false
			continue
		}
		if currentIntervalForASubscriber < shortestIntervalBeforeRemoval {
			shortestIntervalBeforeRemoval = currentIntervalForASubscriber
		}
	}
	mtx.RUnlock()

	mtx.Lock()
	fullKey := compileFullSubscriberId(spec, subscriberId)

	delete(subscriberToIntervalMap, fullKey)
	ticker, exists := receiverFiredMap[fullKey]
	if exists {
		ticker.Stop()
	}
	delete(receiverFiredMap, fullKey)

	first = true
	var shortestIntervalAfterRemoval time.Duration
	for _, currentIntervalForASubscriber := range subscriberToIntervalMap {
		if first {
			shortestIntervalAfterRemoval = currentIntervalForASubscriber
			first = false
			continue
		}
		if currentIntervalForASubscriber < shortestIntervalAfterRemoval {
			shortestIntervalAfterRemoval = currentIntervalForASubscriber
		}
	}
	mtx.Unlock()

	if shortestIntervalAfterRemoval != 0 && shortestIntervalBeforeRemoval != shortestIntervalAfterRemoval {
		bw.setWatchersTickerAfterUnsubscribe(spec.Id, shortestIntervalAfterRemoval, chType)
	}
}

func (bw *BucketTopologySvcWatcher) RegisterGarbageCollect(specId string, vbno uint16, id string, gcFunc func() error, fireDuration time.Duration) error {
	bw.gcMapMtx.Lock()
	defer bw.gcMapMtx.Unlock()
	if bw.gcMapUndergoingGc {
		return service_def.ErrorBucketTopSvcUndergoingGC
	}

	gcForSpec, specExists := bw.gcMap[specId]
	if !specExists {
		gcForSpec = make(VbnoReqMapType)
		bw.gcMap[specId] = gcForSpec
	}

	gcPruneForSpec, specExpExists := bw.gcPruneMap[specId]
	if !specExpExists {
		gcPruneForSpec = make(VbnoPruneMapType)
		bw.gcPruneMap[specId] = gcPruneForSpec
	}

	idMap, vbExists := gcForSpec[vbno]
	if !vbExists {
		idMap = make(RequestMapType)
		gcForSpec[vbno] = idMap
	}

	// Update this VB's last registered timestamp
	gcPruneForSpec[vbno] = time.Now()

	idMap[id] = &GcRequest{
		timeToFire: time.Now().Add(fireDuration),
		funcToFire: gcFunc,
	}
	return nil
}

func (bw *BucketTopologySvcWatcher) handleSpecDeletion(specId string) {
	// First see if spec exists
	bw.gcMapMtx.RLock()
	_, specExists := bw.gcMap[specId]
	_, specExpExists := bw.gcPruneMap[specId]
	bw.gcMapMtx.RUnlock()

	if !specExists && !specExpExists {
		return
	}

	bw.gcMapMtx.Lock()
	if bw.gcMapUndergoingGc {
		bw.gcMapGcAbort = true
	}
	delete(bw.gcMap, specId)
	delete(bw.gcPruneMap, specId)
	bw.gcMapMtx.Unlock()
}

func (bw *BucketTopologySvcWatcher) setDevLegacyDelay(spec *metadata.ReplicationSpecification) {
	if bw == nil || spec == nil || spec.Settings == nil {
		return
	}
	delaySec := spec.Settings.GetIntSettingValue(metadata.DevBucketTopologyLegacyDelay)
	if delaySec > 0 {
		atomic.StoreUint64(&bw.devDcpStatsLegacyDelay, uint64(delaySec))
		bw.logger.Infof("DcpStatsLegacyDelay set to variation of %v seconds", delaySec)
	}
}

type Notification struct {
	Source     bool
	ObjPool    *BucketTopologyObjsPool
	NumReaders uint32

	// Source only
	NumberOfSourceNodes           int
	SourceVBMap                   *base.KvVBMapType
	KvVbMap                       *base.KvVBMapType
	DcpStatsMap                   *base.DcpStatsMapType
	DcpStatsMapLegacy             *base.DcpStatsMapType
	HighSeqnoMap                  *base.HighSeqnosMapType
	HighSeqnoMapLegacy            *base.HighSeqnosMapType
	SourceReplicaCnt              int
	SourceReplicasMap             *base.VbHostsMapType  // len() of 0 if no replicas
	SourceReplicasTranslateMap    *base.StringStringMap // nil if not initialized
	SourceVbReplicasMember        []uint16
	SourceStorageBackend          string
	SourceCollectioManifestUid    uint64
	LocalBucketTopologyUpdateTime time.Time
	EnableCrossClusterVersioning  bool
	VbucketsMaxCas                []interface{}
	VersionPruningWindowHrs       int

	// Target only
	TargetBucketUUID           string
	TargetServerVBMap          *base.KvVBMapType
	TargetBucketInfo           base.BucketInfoMapType
	TargetReplicaCnt           int
	TargetReplicasMap          *base.VbHostsMapType  // len() of 0 if no replicas
	TargetReplicasTranslateMap *base.StringStringMap // nil if not initialized
	TargetVbReplicasMember     []uint16
	TargetStorageBackend       string
}

func NewNotification(isSource bool, pool *BucketTopologyObjsPool) *Notification {
	sourceVBMap := make(base.KvVBMapType)
	kvVbMap := make(base.KvVBMapType)
	dcpStatsMap := make(base.DcpStatsMapType)
	dcpStatsMapLegacy := make(base.DcpStatsMapType)
	highSeqnoMap := make(base.HighSeqnosMapType)
	highSeqnoMapLegacy := make(base.HighSeqnosMapType)
	sourceReplicasMap := make(base.VbHostsMapType)
	sourceReplicasTranslateMap := make(base.StringStringMap)

	targetServerVBMap := make(base.KvVBMapType)
	targetBucketInfo := make(base.BucketInfoMapType)
	targetReplicasMap := make(base.VbHostsMapType)
	targetReplicasTranslateMap := make(base.StringStringMap)

	return &Notification{
		Source:                     isSource,
		ObjPool:                    pool,
		NumberOfSourceNodes:        0,
		SourceVBMap:                &sourceVBMap,
		KvVbMap:                    &kvVbMap,
		DcpStatsMap:                &dcpStatsMap,
		DcpStatsMapLegacy:          &dcpStatsMapLegacy,
		HighSeqnoMap:               &highSeqnoMap,
		HighSeqnoMapLegacy:         &highSeqnoMapLegacy,
		SourceReplicasMap:          &sourceReplicasMap,
		SourceReplicasTranslateMap: &sourceReplicasTranslateMap,

		TargetServerVBMap:          &targetServerVBMap,
		TargetBucketInfo:           targetBucketInfo,
		TargetReplicasMap:          &targetReplicasMap,
		TargetReplicasTranslateMap: &targetReplicasTranslateMap,
	}
}

func (n *Notification) IsSourceNotification() bool {
	return n.Source
}

func (n *Notification) SetNumberOfReaders(readers int) {
	atomic.StoreUint32(&n.NumReaders, uint32(readers))
}

func (n *Notification) Recycle() {
	if n == nil || n.ObjPool == nil {
		return
	}

	if atomic.AddUint32(&n.NumReaders, ^uint32(0)) != uint32(0) {
		return
	}

	if n.SourceVBMap != nil {
		n.ObjPool.KvVbMapPool.Put(n.SourceVBMap)
	}

	if n.KvVbMap != nil {
		n.ObjPool.KvVbMapPool.Put(n.KvVbMap)
	}

	if n.DcpStatsMap != nil {
		for _, vMap := range *n.DcpStatsMap {
			if vMap != nil {
				n.ObjPool.StringStringPool.Put(vMap)
			}
		}
		n.ObjPool.DcpStatsMapPool.Put(n.DcpStatsMap)
	}

	if n.DcpStatsMapLegacy != nil {
		for _, vMap := range *n.DcpStatsMapLegacy {
			if vMap != nil {
				n.ObjPool.StringStringPool.Put(vMap)
			}
		}
		n.ObjPool.DcpStatsMapPool.Put(n.DcpStatsMapLegacy)
	}

	if n.HighSeqnoMap != nil {
		for _, vMap := range *n.HighSeqnoMap {
			if vMap != nil {
				n.ObjPool.VbSeqnoMapPool.Put(vMap)
			}
		}
		n.ObjPool.HighSeqnosMapPool.Put(n.HighSeqnoMap)
	}

	if n.HighSeqnoMapLegacy != nil {
		for _, vMap := range *n.HighSeqnoMapLegacy {
			if vMap != nil {
				n.ObjPool.VbSeqnoMapPool.Put(vMap)
			}
		}
		n.ObjPool.HighSeqnosMapPool.Put(n.HighSeqnoMapLegacy)
	}
	if n.SourceReplicasMap != nil {
		n.ObjPool.VbHostsMapPool.Put(n.SourceReplicasMap)
	}

	if n.SourceReplicasTranslateMap != nil {
		n.ObjPool.StringStringPool.Put(n.SourceReplicasTranslateMap)
	}

	if n.TargetServerVBMap != nil {
		n.ObjPool.KvVbMapPool.Put(n.TargetServerVBMap)
	}

	if n.TargetReplicasMap != nil {
		n.ObjPool.VbHostsMapPool.Put(n.TargetReplicasMap)
	}

	if n.TargetReplicasTranslateMap != nil {
		n.ObjPool.StringStringPool.Put(n.TargetReplicasTranslateMap)
	}
}

func (n *Notification) Clone(numOfReaders int) interface{} {
	if n == nil {
		return nil
	}
	if atomic.LoadUint32(&n.NumReaders) == 0 {
		// This means that Recycle() has been called already and the data below is invalid
		// Check call paths to ensure no one has called recycle before it's done
		panic("Should not be 0")
	}
	return &Notification{
		ObjPool:    n.ObjPool,
		Source:     n.Source,
		NumReaders: uint32(numOfReaders),

		NumberOfSourceNodes:           n.NumberOfSourceNodes,
		SourceVBMap:                   n.SourceVBMap.GreenClone(n.ObjPool.KvVbMapPool.Get),
		KvVbMap:                       n.KvVbMap.GreenClone(n.ObjPool.KvVbMapPool.Get),
		DcpStatsMap:                   n.DcpStatsMap.GreenClone(n.ObjPool.DcpStatsMapPool.Get, n.ObjPool.StringStringPool.Get),
		DcpStatsMapLegacy:             n.DcpStatsMapLegacy.GreenClone(n.ObjPool.DcpStatsMapPool.Get, n.ObjPool.StringStringPool.Get),
		HighSeqnoMap:                  n.HighSeqnoMap.GreenClone(n.ObjPool.HighSeqnosMapPool.Get, n.ObjPool.VbSeqnoMapPool.Get),
		HighSeqnoMapLegacy:            n.HighSeqnoMapLegacy.GreenClone(n.ObjPool.HighSeqnosMapPool.Get, n.ObjPool.VbSeqnoMapPool.Get),
		SourceReplicaCnt:              n.SourceReplicaCnt,
		SourceReplicasMap:             n.SourceReplicasMap.GreenClone(n.ObjPool.VbHostsMapPool.Get, n.ObjPool.StringSlicePool.Get),
		SourceReplicasTranslateMap:    n.SourceReplicasTranslateMap.GreenClone(n.ObjPool.StringStringPool.Get),
		SourceVbReplicasMember:        base.CloneUint16List(n.SourceVbReplicasMember),
		SourceStorageBackend:          n.SourceStorageBackend,
		SourceCollectioManifestUid:    n.SourceCollectioManifestUid,
		LocalBucketTopologyUpdateTime: n.LocalBucketTopologyUpdateTime,
		EnableCrossClusterVersioning:  n.EnableCrossClusterVersioning,
		VbucketsMaxCas:                n.VbucketsMaxCas,
		VersionPruningWindowHrs:       n.VersionPruningWindowHrs,

		TargetBucketUUID:           n.TargetBucketUUID,
		TargetServerVBMap:          n.TargetServerVBMap.GreenClone(n.ObjPool.KvVbMapPool.Get),
		TargetBucketInfo:           n.TargetBucketInfo.Clone(),
		TargetReplicaCnt:           n.TargetReplicaCnt,
		TargetReplicasMap:          n.TargetReplicasMap.GreenClone(n.ObjPool.VbHostsMapPool.Get, n.ObjPool.StringSlicePool.Get),
		TargetReplicasTranslateMap: n.TargetReplicasTranslateMap.GreenClone(n.ObjPool.StringStringPool.Get),
		TargetVbReplicasMember:     base.CloneUint16List(n.TargetVbReplicasMember),
		TargetStorageBackend:       n.TargetStorageBackend,
	}
}

func (n *Notification) GetNumberOfSourceNodes() int {
	return n.NumberOfSourceNodes
}

func (n *Notification) GetKvVbMapRO() base.KvVBMapType {
	return *n.KvVbMap
}

func (n *Notification) GetSourceVBMapRO() base.KvVBMapType {
	return *n.SourceVBMap
}

func (n *Notification) GetSourceStorageBackend() string {
	return n.SourceStorageBackend
}

func (n *Notification) GetSourceCollectionManifestUid() uint64 {
	return n.SourceCollectioManifestUid
}
func (n *Notification) GetTargetServerVBMap() base.KvVBMapType {
	return *n.TargetServerVBMap
}

func (n *Notification) GetTargetBucketUUID() string {
	return n.TargetBucketUUID
}

func (n *Notification) GetTargetBucketInfo() base.BucketInfoMapType {
	return n.TargetBucketInfo
}

func (n *Notification) GetTargetStorageBackend() string {
	return n.TargetStorageBackend
}
func (n *Notification) GetDcpStatsMap() base.DcpStatsMapType {
	return *n.DcpStatsMap
}

func (n *Notification) GetDcpStatsMapLegacy() base.DcpStatsMapType {
	return *n.DcpStatsMapLegacy
}

func (n *Notification) GetHighSeqnosMap() base.HighSeqnosMapType {
	return *n.HighSeqnoMap
}

func (n *Notification) GetHighSeqnosMapLegacy() base.HighSeqnosMapType {
	return *n.HighSeqnoMapLegacy
}

func (n *Notification) GetReplicasInfo() (int, *base.VbHostsMapType, *base.StringStringMap, []uint16) {
	if n.IsSourceNotification() {
		return n.SourceReplicaCnt, n.SourceReplicasMap, n.SourceReplicasTranslateMap, n.SourceVbReplicasMember
	} else {
		return n.TargetReplicaCnt, n.TargetReplicasMap, n.TargetReplicasTranslateMap, n.TargetVbReplicasMember
	}
}

func (n *Notification) GetLocalTopologyUpdatedTime() time.Time {
	return n.LocalBucketTopologyUpdateTime
}

func (n *Notification) GetEnableCrossClusterVersioning() bool {
	return n.EnableCrossClusterVersioning
}

func (n *Notification) GetVersionPruningWindowHrs() int {
	return n.VersionPruningWindowHrs
}

func (n *Notification) GetVbucketsMaxCas() []interface{} {
	if n.EnableCrossClusterVersioning {
		return n.VbucketsMaxCas
	}
	return []interface{}{}
}
