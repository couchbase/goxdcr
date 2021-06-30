package service_impl

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	mocks2 "github.com/couchbase/goxdcr/service_def/mocks"
	utilities "github.com/couchbase/goxdcr/utils"
	utilsMock "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/nelio2k/goxdcr/service_def/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"io/ioutil"
	"testing"
	"time"
)

func setupBTSBoilerPlate() (*mocks.RemoteClusterSvc, *utilsMock.UtilsIface, *mocks2.XDCRCompTopologySvc, *utilities.Utilities, *mocks.ReplicationSpecSvc) {
	remClusterSvc := &mocks.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	xdcrCompTopologySvc := &mocks2.XDCRCompTopologySvc{}
	utilsReal := utilities.NewUtilities()
	replSpecSvc := &mocks.ReplicationSpecSvc{}

	return remClusterSvc, utils, xdcrCompTopologySvc, utilsReal, replSpecSvc
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

func setupMocksBTS(remClusterSvc *mocks.RemoteClusterSvc, xdcrTopologySvc *mocks2.XDCRCompTopologySvc, utils *utilsMock.UtilsIface, bucketInfo map[string]interface{}, utilsReal *utilities.Utilities, kvNodes []string, replSpecSvc *mocks.ReplicationSpecSvc, specsList []*metadata.ReplicationSpecification) {
	xdcrTopologySvc.On("MyConnectionStr").Return("dummyConnStr", nil)
	xdcrTopologySvc.On("MyCredentials").Return("", "", base.HttpAuthMechPlain, nil, false, nil, nil, nil)
	xdcrTopologySvc.On("MyKVNodes").Return(kvNodes, nil)
	utils.On("GetBucketInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bucketInfo, nil)

	vbMapToRet, vbMapErr := utilsReal.GetServerVBucketsMap("", "", bucketInfo)
	utils.On("GetServerVBucketsMap", mock.Anything, mock.Anything, mock.Anything).Return(vbMapToRet, vbMapErr)

	replMap := make(map[string]*metadata.ReplicationSpecification)
	for _, spec := range specsList {
		replMap[spec.Id] = spec
	}
	replSpecSvc.On("AllReplicationSpecs").Return(replMap, nil)
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
	var kvNames []string
	for k, _ := range retMap {
		kvNames = append(kvNames, k)
	}
	return retMap, kvNames
}

func TestBucketTopologyServiceRegister(t *testing.T) {
	fmt.Println("============== Test case start: TestBucketTopologyServiceRegister =================")
	defer fmt.Println("============== Test case end: TestBucketTopologyServiceRegister =================")
	assert := assert.New(t)

	remClusterSvc, utils, xdcrCompTopologySvc, utilsReal, replSpecSvc := setupBTSBoilerPlate()
	bucketMap, kvNames := getBucketMap()
	setupMocksBTS(remClusterSvc, xdcrCompTopologySvc, utils, bucketMap, utilsReal, kvNames, replSpecSvc, nil)

	bts, err := NewBucketTopologyService(xdcrCompTopologySvc, remClusterSvc, utils, 100*time.Millisecond, log.DefaultLoggerContext, replSpecSvc)
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
	srcVBMap1, _ := notification1.GetSourceVBMapRO()
	srcVBMap2, _ := notification2.GetSourceVBMapRO()
	numSrcNodes1, _ := notification1.GetNumberOfSourceNodes()
	numSrcNodes2, _ := notification2.GetNumberOfSourceNodes()
	assert.Equal(srcVBMap1, srcVBMap2)
	assert.Equal(numSrcNodes1, numSrcNodes2)

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

	remClusterSvc, utils, xdcrCompTopologySvc, utilsReal, replSpecSvc := setupBTSBoilerPlate()
	bucketMap, kvNames := getBucketMap()

	spec, _ := metadata.NewReplicationSpecification(srcBucketName, srcBucketUuid, tgtClusterUuid, tgtBucketName, tgtBucketUuid)
	spec2, _ := metadata.NewReplicationSpecification(srcBucketName, srcBucketUuid, tgtClusterUuid, tgtBucketName2, tgtBucketUuid2)
	specList := []*metadata.ReplicationSpecification{spec, spec2}
	setupMocksBTS(remClusterSvc, xdcrCompTopologySvc, utils, bucketMap, utilsReal, kvNames, replSpecSvc, specList)

	bts, err := NewBucketTopologyService(xdcrCompTopologySvc, remClusterSvc, utils, 10*time.Second, log.DefaultLoggerContext, replSpecSvc)
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
	latestCacheCopy := watcher.latestCached.CloneRO()
	watcher.latestCacheMtx.RUnlock()

	immediateCh := watcher.registerAndGetCh(spec, "")
	select {
	case testReceive := <-immediateCh:
		assert.Equal(latestCacheCopy.IsSourceNotification(), testReceive.IsSourceNotification())
	default:
		// test failed
		assert.False(true)
	}
}
