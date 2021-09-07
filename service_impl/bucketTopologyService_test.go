package service_impl

import (
	"encoding/json"
	"fmt"
	mocks2 "github.com/couchbase/gomemcached/client/mocks"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	mocks "github.com/couchbase/goxdcr/service_def/mocks"
	utilities "github.com/couchbase/goxdcr/utils"
	utilsMock "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"io/ioutil"
	"testing"
	"time"
)

func setupBTSBoilerPlate() (*mocks.RemoteClusterSvc, *utilsMock.UtilsIface, *mocks.XDCRCompTopologySvc, *utilities.Utilities, *mocks.ReplicationSpecSvc, *mocks2.ClientIface) {
	remClusterSvc := &mocks.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	xdcrCompTopologySvc := &mocks.XDCRCompTopologySvc{}
	utilsReal := utilities.NewUtilities()
	replSpecSvc := &mocks.ReplicationSpecSvc{}
	mcClient := &mocks2.ClientIface{}

	return remClusterSvc, utils, xdcrCompTopologySvc, utilsReal, replSpecSvc, mcClient
}

const (
	srcBucketName  = "srcBucket"
	srcBucketUuid  = "a1b2c3d4e5"
	tgtClusterUuid = "b2c3d4e5f6"
	tgtBucketName  = "tgtBucket"
	tgtBucketName2 = "tgtBucket2"
	tgtBucketUuid  = "c3d4e5f6g7"
	tgtBucketUuid2 = "c3d4e5f6g72"
)

func setupBTSSpecs() *metadata.ReplicationSpecification {
	spec, _ := metadata.NewReplicationSpecification(srcBucketName, srcBucketUuid, tgtClusterUuid, tgtBucketName, tgtBucketUuid)
	return spec
}

func getTestRemRef() *metadata.RemoteClusterReference {
	ref, _ := metadata.NewRemoteClusterReference("uuid", "name", "hostname", "username", "password", "", false, "", nil, nil, nil, nil)
	return ref
}

func getCapability() metadata.Capability {
	return metadata.UnitTestGetCollectionsCapability()
}

func setupMocksBTS(remClusterSvc *mocks.RemoteClusterSvc, xdcrTopologySvc *mocks.XDCRCompTopologySvc, utils *utilsMock.UtilsIface, bucketInfo map[string]interface{}, utilsReal *utilities.Utilities, kvNodes []string, replSpecSvc *mocks.ReplicationSpecSvc, specsList []*metadata.ReplicationSpecification, ref *metadata.RemoteClusterReference, cap metadata.Capability, mcClient *mocks2.ClientIface) {
	var connStr = "dummyConnStr"
	xdcrTopologySvc.On("MyConnectionStr").Return(connStr, nil)
	xdcrTopologySvc.On("MyCredentials").Return("", "", base.HttpAuthMechPlain, nil, false, nil, nil, nil)
	xdcrTopologySvc.On("MyKVNodes").Return(kvNodes, nil)
	xdcrTopologySvc.On("IsMyClusterEncryptionLevelStrict").Return(false)
	utils.On("GetBucketInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bucketInfo, nil)
	utils.On("ExponentialBackoffExecutor", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		utilsFunc := args.Get(4).(utilities.ExponentialOpFunc)
		utilsFunc()
	}).Return(nil)

	bucketUuidGetterFunc := func() string {
		bucketUuid, _ := utilsReal.GetBucketUuidFromBucketInfo("", bucketInfo, nil)
		return bucketUuid
	}
	utils.On("GetBucketUuidFromBucketInfo", mock.Anything, mock.Anything, mock.Anything).Return(bucketUuidGetterFunc(), nil)

	vbMapGetter := func() map[string][]uint16 {
		result, _ := utilsReal.GetServerVBucketsMap("", "", bucketInfo)
		return result
	}
	utils.On("GetServerVBucketsMap", mock.Anything, mock.Anything, mock.Anything).Return(vbMapGetter(), nil)
	utils.On("GetRemoteServerVBucketsMap", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(vbMapGetter(), nil)
	utils.On("GetMemcachedClient", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mcClient, nil)

	highSeqnosMap := make(map[uint16]uint64)
	for i := uint16(0); i < 1024; i++ {
		highSeqnosMap[i] = uint64(100 + int(i))
	}
	utils.On("GetHighSeqNos", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(highSeqnosMap, nil, nil)

	replMap := make(map[string]*metadata.ReplicationSpecification)
	for _, spec := range specsList {
		replMap[spec.Id] = spec
	}
	replSpecSvc.On("AllReplicationSpecs").Return(replMap, nil)

	remClusterSvc.On("RemoteClusterByUuid", mock.Anything, mock.Anything).Return(ref, nil)
	bucketInfoGetter := func() (map[string]interface{}, bool, string, error) {
		return bucketInfo, false, connStr, nil
	}
	remClusterSvc.On("GetBucketInfoGetter", mock.Anything, mock.Anything).Return(service_def.BucketInfoGetter(bucketInfoGetter), nil)
	remClusterSvc.On("GetCapability", mock.Anything).Return(cap, nil)

	mcClient.On("StatsMap", mock.Anything).Return(nil, nil)
}

func getBucketMap() (map[string]interface{}, []string) {
	file := "../utils/testInternalData/pools_default_buckets_b2.json"
	jsonData, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}
	retMap := make(map[string]interface{})
	err = json.Unmarshal(jsonData, &retMap)
	if err != nil {
		panic(err)
	}
	var bucketInfo map[string]interface{} = retMap
	vbucketServerMapObj, ok := bucketInfo[base.VBucketServerMapKey]
	if !ok {
		panic("not ok")
	}
	vbucketServerMap, _ := vbucketServerMapObj.(map[string]interface{})
	serverListObj, _ := vbucketServerMap[base.ServerListKey]
	serverList, _ := serverListObj.([]interface{})
	servers := make([]string, len(serverList))
	for index, serverName := range serverList {
		serverNameStr, _ := serverName.(string)
		servers[index] = serverNameStr
	}

	return retMap, servers
}

func TestBucketTopologyServiceRegister(t *testing.T) {
	fmt.Println("============== Test case start: TestBucketTopologyServiceRegister =================")
	defer fmt.Println("============== Test case end: TestBucketTopologyServiceRegister =================")
	assert := assert.New(t)

	remClusterSvc, utils, xdcrCompTopologySvc, utilsReal, replSpecSvc, mcClient := setupBTSBoilerPlate()
	bucketMap, kvNames := getBucketMap()
	setupMocksBTS(remClusterSvc, xdcrCompTopologySvc, utils, bucketMap, utilsReal, kvNames, replSpecSvc, nil, getTestRemRef(), getCapability(), mcClient)

	bts, err := NewBucketTopologyService(xdcrCompTopologySvc, remClusterSvc, utils, 100*time.Millisecond, log.DefaultLoggerContext, replSpecSvc, 100*time.Millisecond)
	assert.NotNil(bts)
	assert.Nil(err)

	spec, _ := metadata.NewReplicationSpecification(srcBucketName, srcBucketUuid, tgtClusterUuid, tgtBucketName, tgtBucketUuid)
	spec2, _ := metadata.NewReplicationSpecification(srcBucketName, srcBucketUuid, tgtClusterUuid, tgtBucketName2, tgtBucketUuid2)

	watcher1 := bts.getOrCreateLocalWatcher(spec)
	assert.Nil(watcher1.Start())
	watcher2 := bts.getOrCreateLocalWatcher(spec2)
	assert.NotNil(watcher2.Start())
	assert.Equal(watcher1, watcher2)

	// Register two specs sharing the same source bucket, same instance of watcher
	specNotifyCh, err := bts.SubscribeToLocalBucketFeed(spec, "")
	assert.Nil(err)
	assert.NotNil(specNotifyCh)

	spec2NotifyCh, err := bts.SubscribeToLocalBucketFeed(spec2, "")
	assert.Nil(err)
	assert.NotSame(specNotifyCh, spec2NotifyCh)
	assert.Equal(2, bts.srcBucketWatchersCnt[srcBucketName])
	assert.Len(bts.srcBucketWatchers, 1)
	var watcher *BucketTopologySvcWatcher
	for _, v := range bts.srcBucketWatchers {
		watcher = v
	}
	assert.NotNil(watcher)
	assert.Equal(uint32(0), watcher.isStopped)
	assert.NotNil(spec2NotifyCh)

	notification1 := <-specNotifyCh
	notification2 := <-spec2NotifyCh
	assert.NotNil(notification1)
	assert.NotNil(notification2)
	srcVBMap1 := notification1.GetSourceVBMapRO()
	srcVBMap2 := notification2.GetSourceVBMapRO()
	numSrcNodes1 := notification1.GetNumberOfSourceNodes()
	numSrcNodes2 := notification2.GetNumberOfSourceNodes()
	assert.Equal(srcVBMap1, srcVBMap2)
	assert.Equal(numSrcNodes1, numSrcNodes2)

	// Modifying latestCached should not affect downstream
	assert.Equal(srcVBMap1, watcher.latestCached.SourceVBMap)
	watcher.latestCacheMtx.Lock()
	watcher.latestCached.SourceVBMap["randomTestModification"] = []uint16{1, 2, 3}
	watcher.latestCacheMtx.Unlock()
	assert.NotEqual(srcVBMap1, watcher.latestCached.SourceVBMap)

	// Nothing to get
	select {
	case <-specNotifyCh:
		assert.True(false)
	default:
		break
	}

	// The next iteration should be the same
	time.Sleep(100 * time.Millisecond)
	notification1New := <-specNotifyCh
	srcVBMap1New := notification1New.GetSourceVBMapRO()
	assert.Equal(srcVBMap1New, watcher.latestCached.SourceVBMap)
	// With the old data not modified - hacky because watcher.latestCached has been reset to the same data
	// Check by modifying both - and make sure that the references are not shared
	watcher.latestCacheMtx.Lock()
	watcher.latestCached.SourceVBMap["randomTestModification"] = []uint16{1, 2, 3}
	watcher.latestCacheMtx.Unlock()
	assert.NotEqual(srcVBMap1New, watcher.latestCached.SourceVBMap)
	srcVBMap1New["randomTestModification"] = []uint16{1, 2, 3}
	assert.NotEqual(srcVBMap1New, srcVBMap1)

	// Unregister one, the instance should still be there
	assert.Nil(bts.handleSpecDeletion(spec))
	assert.Len(bts.srcBucketWatchers, 1)

	// Unregister second one, gc it
	assert.Nil(bts.handleSpecDeletion(spec2))
	assert.Len(bts.srcBucketWatchers, 0)
	assert.Len(bts.srcBucketWatchersCnt, 0)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(uint32(1), watcher.isStopped)
}

func TestBucketTopologyServiceWithLodedSpecs(t *testing.T) {
	fmt.Println("============== Test case start: TestBucketTopologyServiceRegister =================")
	defer fmt.Println("============== Test case end: TestBucketTopologyServiceRegister =================")
	assert := assert.New(t)

	remClusterSvc, utils, xdcrCompTopologySvc, utilsReal, replSpecSvc, mcClient := setupBTSBoilerPlate()
	bucketMap, kvNames := getBucketMap()

	spec, _ := metadata.NewReplicationSpecification(srcBucketName, srcBucketUuid, tgtClusterUuid, tgtBucketName, tgtBucketUuid)
	spec2, _ := metadata.NewReplicationSpecification(srcBucketName, srcBucketUuid, tgtClusterUuid, tgtBucketName2, tgtBucketUuid2)
	specList := []*metadata.ReplicationSpecification{spec, spec2}
	setupMocksBTS(remClusterSvc, xdcrCompTopologySvc, utils, bucketMap, utilsReal, kvNames, replSpecSvc, specList, nil, getCapability(), mcClient)

	bts, err := NewBucketTopologyService(xdcrCompTopologySvc, remClusterSvc, utils, 10*time.Second, log.DefaultLoggerContext, replSpecSvc, 10*time.Second)
	assert.NotNil(bts)
	assert.Nil(err)

	assert.NotEqual(0, len(bts.srcBucketWatchers))
	bts.srcBucketWatchersMtx.RLock()
	watcherCnt := bts.srcBucketWatchersCnt[spec.SourceBucketName]
	watcher2Cnt := bts.srcBucketWatchersCnt[spec2.SourceBucketName]
	watcher := bts.srcBucketWatchers[spec.SourceBucketName]
	bts.srcBucketWatchersMtx.RUnlock()
	assert.Equal(2, watcherCnt)
	assert.Equal(2, watcher2Cnt)
	assert.NotNil(watcher)

	// When starting up with replication spec already loaded, it should have some data pulled already
	time.Sleep(100 * time.Millisecond)
	watcher.latestCacheMtx.RLock()
	assert.NotEqual(0, watcher.latestCached.NumberOfSourceNodes)
	//latestCacheCopy := watcher.latestCached.CloneRO().(serviceDefReal.SourceNotification)
	watcher.latestCacheMtx.RUnlock()

	immediateCh := watcher.registerAndGetCh(spec, "", TOPOLOGY, nil).(chan service_def.SourceNotification)
	select {
	case <-immediateCh:
	default:
		// test failed
		assert.False(true)
	}
}

func TestBucketTopologyServiceHighSeqnos(t *testing.T) {
	fmt.Println("============== Test case start: TestBucketTopologyServiceHighSeqnos =================")
	defer fmt.Println("============== Test case end: TestBucketTopologyServiceHighSeqnos =================")
	assert := assert.New(t)

	remClusterSvc, utils, xdcrCompTopologySvc, utilsReal, replSpecSvc, mcClient := setupBTSBoilerPlate()
	bucketMap, kvNames := getBucketMap()
	setupMocksBTS(remClusterSvc, xdcrCompTopologySvc, utils, bucketMap, utilsReal, kvNames, replSpecSvc, nil, getTestRemRef(), getCapability(), mcClient)

	bts, err := NewBucketTopologyService(xdcrCompTopologySvc, remClusterSvc, utils, 100*time.Millisecond, log.DefaultLoggerContext, replSpecSvc, 100*time.Millisecond)
	assert.NotNil(bts)
	assert.Nil(err)

	spec, _ := metadata.NewReplicationSpecification(srcBucketName, srcBucketUuid, tgtClusterUuid, tgtBucketName, tgtBucketUuid)
	spec2, _ := metadata.NewReplicationSpecification(srcBucketName, srcBucketUuid, tgtClusterUuid, tgtBucketName2, tgtBucketUuid2)

	watcher1 := bts.getOrCreateLocalWatcher(spec)
	assert.Nil(watcher1.Start())
	watcher2 := bts.getOrCreateLocalWatcher(spec2)
	assert.NotNil(watcher2.Start())
	assert.Equal(watcher1, watcher2)

	feed1, _, err := bts.SubscribeToLocalBucketHighSeqnosFeed(spec, "sub1", 110*time.Millisecond)
	assert.Nil(err)

	feed2, updater2, err := bts.SubscribeToLocalBucketHighSeqnosFeed(spec, "sub2", 500*time.Millisecond)
	assert.Nil(err)

	watcher1.highSeqnosTrackersMtx.RLock()
	assert.Len(watcher1.highSeqnosIntervals, 2)
	watcher1.highSeqnosTrackersMtx.RUnlock()

	// When subscribing, it'll pass one first, so soak that one up
	select {
	case <-feed1:
	}

	select {
	case <-feed2:
	}

	// Then the next one should have some delay
	time.Sleep(150 * time.Millisecond)

	// Feed 1 should get something and feed 2 should not
	select {
	case notification := <-feed1:
		notification.GetDcpStatsMap()
	default:
		assert.True(false)
	}
	select {
	case <-feed2:
		assert.True(false)
	default:
		break
	}

	// The once threshold passed, it'll get it
	time.Sleep(550 * time.Millisecond)
	select {
	case <-feed2:
		break
	default:
		assert.True(false)
	}

	// Set update interval to 90 sec
	updater2(90 * time.Millisecond)

	// After ~ 120 ms, both should receive updates
	time.Sleep(120 * time.Millisecond)
	select {
	case notification := <-feed1:
		notification.GetDcpStatsMap()
	default:
		assert.True(false)
	}
	select {
	case notification := <-feed2:
		notification.GetDcpStatsMap()
	default:
		assert.True(false)
	}

	intervalAndFunc := watcher1.intervalFuncMap[HIGHSEQNOS]
	for _, updateFunc := range intervalAndFunc {
		updateFuncCpy := updateFunc
		watcher1.updateOnce(HIGHSEQNOS, updateFuncCpy)
	}
}

func TestBucketTopologyWatcherGC(t *testing.T) {
	fmt.Println("============== Test case start: TestBucketTopologyWatcherGC =================")
	defer fmt.Println("============== Test case end: TestBucketTopologyWatcherGC =================")
	assert := assert.New(t)

	remClusterSvc, utils, xdcrCompTopologySvc, utilsReal, replSpecSvc, mcClient := setupBTSBoilerPlate()
	bucketMap, kvNames := getBucketMap()
	setupMocksBTS(remClusterSvc, xdcrCompTopologySvc, utils, bucketMap, utilsReal, kvNames, replSpecSvc, nil, getTestRemRef(), getCapability(), mcClient)

	bts, err := NewBucketTopologyService(xdcrCompTopologySvc, remClusterSvc, utils, 100*time.Millisecond, log.DefaultLoggerContext, replSpecSvc, 100*time.Millisecond)
	assert.NotNil(bts)
	assert.Nil(err)

	spec, _ := metadata.NewReplicationSpecification(srcBucketName, srcBucketUuid, tgtClusterUuid, tgtBucketName, tgtBucketUuid)

	watcher1 := bts.getOrCreateLocalWatcher(spec)
	customPruneWindow := 250 * time.Millisecond
	watcher1.gcPruneWindow = customPruneWindow
	assert.Nil(watcher1.Start())
	// Stop the pull routine and manually hack around to see if GC works
	watcher1.Stop()

	delete(watcher1.latestCached.SourceVBMap, "192.168.0.116:12002")

	var gc1Called bool
	var gc2Called bool
	gcFunc1 := func() error {
		gc1Called = true
		return nil
	}
	gcFunc2 := func() error {
		gc2Called = true
		return nil
	}
	assert.Nil(watcher1.RegisterGarbageCollect(spec.Id, 512, "id1", gcFunc1, 100*time.Millisecond))
	assert.Nil(watcher1.RegisterGarbageCollect(spec.Id, 0, "id2", gcFunc2, 100*time.Millisecond))

	assert.Equal(1, len(watcher1.gcMap[spec.Id][0]))
	assert.Equal(1, len(watcher1.gcMap[spec.Id][512]))

	time.Sleep(150 * time.Millisecond)
	watcher1.runGC()
	assert.False(gc1Called)
	assert.True(gc2Called)

	assert.Equal(0, len(watcher1.gcMap[spec.Id][0]))
	assert.Equal(1, len(watcher1.gcMap[spec.Id][512]))

	time.Sleep(customPruneWindow)
	watcher1.runGC()
	assert.Equal(0, len(watcher1.gcMap[spec.Id][512]))
	assert.Equal(0, len(watcher1.gcMap[spec.Id]))
	assert.Equal(0, len(watcher1.gcMap))
}
