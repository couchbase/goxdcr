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

func setupBTSBoilerPlate() (*mocks.RemoteClusterSvc, *utilsMock.UtilsIface, *mocks2.XDCRCompTopologySvc, *utilities.Utilities) {
	remClusterSvc := &mocks.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	xdcrCompTopologySvc := &mocks2.XDCRCompTopologySvc{}
	utilsReal := utilities.NewUtilities()

	return remClusterSvc, utils, xdcrCompTopologySvc, utilsReal
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

func setupMocksBTS(remClusterSvc *mocks.RemoteClusterSvc, xdcrTopologySvc *mocks2.XDCRCompTopologySvc, utils *utilsMock.UtilsIface, bucketInfo map[string]interface{}, utilsReal *utilities.Utilities, kvNodes []string) {
	xdcrTopologySvc.On("MyConnectionStr").Return("dummyConnStr", nil)
	xdcrTopologySvc.On("MyCredentials").Return("", "", base.HttpAuthMechPlain, nil, false, nil, nil, nil)
	xdcrTopologySvc.On("MyKVNodes").Return(kvNodes, nil)
	utils.On("GetBucketInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bucketInfo, nil)

	vbMapToRet, vbMapErr := utilsReal.GetServerVBucketsMap("", "", bucketInfo)
	utils.On("GetServerVBucketsMap", mock.Anything, mock.Anything, mock.Anything).Return(vbMapToRet, vbMapErr)
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

	remClusterSvc, utils, xdcrCompTopologySvc, utilsReal := setupBTSBoilerPlate()
	bucketMap, kvNames := getBucketMap()
	setupMocksBTS(remClusterSvc, xdcrCompTopologySvc, utils, bucketMap, utilsReal, kvNames)

	bts := NewBucketTopologyService(xdcrCompTopologySvc, remClusterSvc, utils, 100*time.Millisecond, log.DefaultLoggerContext)
	assert.NotNil(bts)

	spec, _ := metadata.NewReplicationSpecification(srcBucketName, srcBucketUuid, tgtClusterUuid, tgtBucketName, tgtBucketUuid)
	spec2, _ := metadata.NewReplicationSpecification(srcBucketName, srcBucketUuid, tgtClusterUuid, tgtBucketName2, tgtBucketUuid2)

	// Register two specs sharing the same source bucket, same instance of watcher
	srcWatcher, specNotifyCh, err := bts.RegisterLocalBucket(spec)
	assert.Nil(err)
	assert.NotNil(srcWatcher)
	assert.NotNil(specNotifyCh)

	srcWatcher2, spec2NotifyCh, err := bts.RegisterLocalBucket(spec2)
	assert.Nil(err)
	assert.NotNil(srcWatcher2)
	assert.Equal(srcWatcher, srcWatcher2)
	assert.Equal(2, bts.srcBucketWatchersCnt[srcBucketName])
	assert.Len(bts.srcBucketWatchers, 1)
	var watcher *BucketTopologySvcWatcher
	for _, v := range bts.srcBucketWatchers {
		watcher = v
	}
	assert.NotNil(watcher)
	assert.Equal(uint32(0), watcher.isStopped)
	assert.NotNil(spec2NotifyCh)

	checkChannel(specNotifyCh, assert, true)
	checkChannel(spec2NotifyCh, assert, true)

	notification1 := <-specNotifyCh
	notification2 := <-spec2NotifyCh
	assert.NotNil(notification1)
	assert.NotNil(notification2)
	assert.Equal(notification1.SourceVBMap, notification2.SourceVBMap)
	assert.Equal(notification1.NumberOfSourceNodes, notification2.NumberOfSourceNodes)

	// Unregister one, the instance should still be there
	assert.Nil(bts.UnRegisterLocalBucket(spec))
	assert.Len(bts.srcBucketWatchers, 1)
	// Check first ch closed func
	defer func() {
		if r := recover(); r != nil {
			// Recover closed channel check
		}
	}()
	checkChannel(specNotifyCh, assert, false)
	checkChannel(spec2NotifyCh, assert, true)

	// Unregister second one, gc it
	assert.Nil(bts.UnRegisterLocalBucket(spec2))
	assert.Len(bts.srcBucketWatchers, 0)
	assert.Len(bts.srcBucketWatchersCnt, 0)

	checkChannel(specNotifyCh, assert, false)
	checkChannel(spec2NotifyCh, assert, false)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(uint32(1), watcher.isStopped)
}

func checkChannel(specNotifyCh chan *Notification, assert *assert.Assertions, channelOK bool) {
	var ch1ok bool
	select {
	case _, ch1ok = <-specNotifyCh:
	// nothing
	default:
		ch1ok = true
	}
	if channelOK {
		assert.True(ch1ok)
	} else {
		assert.False(ch1ok)
	}
}
