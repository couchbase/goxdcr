// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_impl

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mcc "github.com/couchbase/gomemcached/client"
	clientMocks "github.com/couchbase/gomemcached/client/mocks"
	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	Component "github.com/couchbase/goxdcr/v8/component"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	service_defMock "github.com/couchbase/goxdcr/v8/service_def/mocks"
	"github.com/couchbase/goxdcr/v8/utils"
	utilsMock "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Test constants
const (
	testBucketName              = "testBucket"
	testClusterUuid             = "123456789"
	testServerAddr              = "127.0.0.1:11210"
	testServerAddr2             = "127.0.0.1:11211"
	testMaxConnectionsPerServer = 5
)

// Helper functions for creating test data
func createTestRemoteClusterRef() *metadata.RemoteClusterReference {
	ref, _ := metadata.NewRemoteClusterReference(testClusterUuid, "testCluster", "localhost:8091", "admin", "password", "", false, "", nil, nil, nil, nil)
	return ref
}

func createTestLogger() *log.CommonLogger {
	return log.NewLogger("bucketStatsGetterTest", log.DefaultLoggerContext)
}

func createTestKvVbMap() base.KvVBMapType {
	kvVbMap := make(base.KvVBMapType)
	kvVbMap[testServerAddr] = []uint16{0, 1, 2, 3}
	kvVbMap[testServerAddr2] = []uint16{4, 5, 6, 7}
	return kvVbMap
}

// setupTestRemoteMemcachedComponent properly initializes a RemoteMemcachedComponent for testing
func setupTestRemoteMemcachedComponent(provider *ClusterBucketStatsProvider, utils *utilsMock.UtilsIface, logger *log.CommonLogger) {
	kvVbMap := createTestKvVbMap()
	provider.remoteMemcachedComponent = Component.NewRemoteMemcachedComponent(logger, provider.finCh, utils, testBucketName, "test", testMaxConnectionsPerServer)
	provider.remoteMemcachedComponent.TargetKvVbMap = func() (base.KvVBMapType, error) {
		return kvVbMap, nil
	}
	// Set RefGetter to return a test ref (needed for InitConnections)
	testRef := createTestRemoteClusterRef()
	provider.remoteMemcachedComponent.SetRefGetter(func() *metadata.RemoteClusterReference {
		return testRef
	})

	mockClient := &clientMocks.ClientIface{}
	mockClient.On("Close").Return(nil)
	utils.On("GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()

	// Mark the provider as initialized since we've manually set up the component
	provider.initDone.Store(true)
}

func createTestFailoverLog() *base.BucketFailoverLog {
	failoverLog := &base.BucketFailoverLog{
		FailoverLogMap: make(base.FailoverLogMapType),
	}
	for vb := uint16(0); vb < 8; vb++ {
		failoverLog.FailoverLogMap[vb] = &base.FailoverLog{
			NumEntries: 2,
			LogTable: []*base.FailoverEntry{
				{Uuid: 123456, HighSeqno: 1000 + uint64(vb)},
				{Uuid: 789012, HighSeqno: 2000 + uint64(vb)},
			},
		}
	}
	return failoverLog
}

// setupClusterBucketStatsProvider creates a test provider with mocks
func setupClusterBucketStatsProvider() (*ClusterBucketStatsProvider, *utilsMock.UtilsIface, *service_defMock.BucketTopologySvc, *service_defMock.RemoteClusterSvc) {
	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	return provider, utils, bucketTopologySvc, remoteClusterSvc
}

// ============= Test Cases for ClusterBucketStatsProvider =============

func TestClusterBucketStatsProvider_NewClusterBucketStatsProvider(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_NewClusterBucketStatsProvider =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_NewClusterBucketStatsProvider =================")
	assert := assert.New(t)

	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	assert.NotNil(provider)
	assert.Equal(testBucketName, provider.bucketName)
	assert.Equal(testClusterUuid, provider.clusterUuid)
	assert.NotNil(provider.finCh)
	assert.Equal(uint64(0), provider.subscriptionCounter)
}

func TestClusterBucketStatsProvider_Init_Success(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_Init_Success =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_Init_Success =================")
	assert := assert.New(t)

	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	// Setup mocks for Init
	ref := createTestRemoteClusterRef()
	remoteClusterSvc.On("RemoteClusterByUuid", testClusterUuid, false).Return(ref, nil)
	remoteClusterSvc.On("ShouldUseAlternateAddress", ref).Return(false, nil)

	// Mock bucket topology subscription
	targetNotificationCh := make(chan service_def.TargetNotification, 1)
	targetNotification := &service_defMock.TargetNotification{}
	kvVbMap := createTestKvVbMap()
	targetNotification.On("GetTargetServerVBMap").Return(kvVbMap)
	targetNotification.On("Recycle").Return()
	targetNotificationCh <- targetNotification

	bucketTopologySvc.On("SubscribeToRemoteBucketFeed", mock.AnythingOfType("*metadata.ReplicationSpecification"), mock.AnythingOfType("string")).Return(targetNotificationCh, nil)
	bucketTopologySvc.On("UnSubscribeRemoteBucketFeed", mock.AnythingOfType("*metadata.ReplicationSpecification"), mock.AnythingOfType("string")).Return(nil)

	// Mock GetRemoteMemcachedConnection for memcached client creation
	mcClient := &clientMocks.ClientIface{}
	mcClient.On("Close").Return(nil)
	utils.On("GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mcClient, nil)

	// Mock ExecWithTimeout to execute immediately
	utils.On("ExecWithTimeout", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		fn := args.Get(0).(func() error)
		fn()
	}).Return(nil)

	err := provider.Init()
	assert.NoError(err)
	assert.NotNil(provider.remoteMemcachedComponent)

	// Init should be idempotent
	err = provider.Init()
	assert.NoError(err)

	provider.Close()
}

func TestClusterBucketStatsProvider_Init_TargetBucketTopologyNotReady(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_Init_TargetBucketTopologyNotReady =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_Init_TargetBucketTopologyNotReady =================")
	assert := assert.New(t)

	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	ref := createTestRemoteClusterRef()
	remoteClusterSvc.On("RemoteClusterByUuid", testClusterUuid, false).Return(ref, nil)
	remoteClusterSvc.On("ShouldUseAlternateAddress", ref).Return(false, nil)

	// Mock empty notification channel (topology not ready)
	targetNotificationCh := make(chan service_def.TargetNotification, 1)
	bucketTopologySvc.On("SubscribeToRemoteBucketFeed", mock.AnythingOfType("*metadata.ReplicationSpecification"), mock.AnythingOfType("string")).Return(targetNotificationCh, nil)
	bucketTopologySvc.On("UnSubscribeRemoteBucketFeed", mock.AnythingOfType("*metadata.ReplicationSpecification"), mock.AnythingOfType("string")).Return(nil)

	utils.On("ExecWithTimeout", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		fn := args.Get(0).(func() error)
		fn()
	}).Return(nil)

	err := provider.Init()
	// Should get error because topology is not ready
	assert.Error(err)
	assert.Contains(err.Error(), "Target bucket topology does not have any cached data yet")

	provider.Close()
}

func TestClusterBucketStatsProvider_Close_Success(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_Close_Success =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_Close_Success =================")
	assert := assert.New(t)

	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	err := provider.Close()
	assert.NoError(err)

	// Close should be idempotent
	err = provider.Close()
	assert.NoError(err)

	// Verify finCh is closed
	select {
	case <-provider.finCh:
		// Expected
	default:
		t.Fatal("finCh should be closed")
	}
}

func TestClusterBucketStatsProvider_GetFailoverLog_Success(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_GetFailoverLog_Success =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_GetFailoverLog_Success =================")
	assert := assert.New(t)

	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()
	mcClient := &clientMocks.ClientIface{}

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	// Setup mocks
	setupTestRemoteMemcachedComponent(provider, utils, logger)

	// Mock GetKvClient - initialize channels and add clients to the pool
	mcClient.On("Close").Return(nil)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, provider.remoteMemcachedComponent.MaxConnsPerServer)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, provider.remoteMemcachedComponent.MaxConnsPerServer)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr] <- mcClient
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr2] <- mcClient

	// Mock GetFailoverLog
	failoverLog := createTestFailoverLog()
	utils.On("GetFailoverLog", mcClient).Return(failoverLog.FailoverLogMap, nil)

	vblist := []uint16{0, 1, 2, 3, 4, 5, 6, 7}
	finCh := make(chan bool, 1)
	defer close(finCh)
	result, errMap, err := provider.GetFailoverLog(&base.FailoverLogRequest{VBuckets: vblist, FinCh: finCh})

	assert.NoError(err)
	assert.NotNil(result)
	assert.Equal(len(vblist), len(result.FailoverLogMap))
	assert.Empty(errMap)

	for _, vb := range vblist {
		log, ok := result.FailoverLogMap[vb]
		assert.True(ok)
		assert.NotNil(log)
		assert.Equal(uint64(2), log.NumEntries)
	}

	provider.Close()
}

func TestClusterBucketStatsProvider_GetFailoverLog_PartialFailure(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_GetFailoverLog_PartialFailure =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_GetFailoverLog_PartialFailure =================")
	assert := assert.New(t)

	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()
	mcClient := &clientMocks.ClientIface{}
	mcClient2 := &clientMocks.ClientIface{}

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	setupTestRemoteMemcachedComponent(provider, utils, logger)
	mcClient.On("Close").Return(nil)
	mcClient2.On("Close").Return(nil)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, provider.remoteMemcachedComponent.MaxConnsPerServer)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, provider.remoteMemcachedComponent.MaxConnsPerServer)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr] <- mcClient
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr2] <- mcClient2

	// First server succeeds - but only has vbuckets 0-3
	failoverLogServer1 := &base.BucketFailoverLog{
		FailoverLogMap: make(base.FailoverLogMapType),
	}
	for vb := uint16(0); vb < 4; vb++ {
		failoverLogServer1.FailoverLogMap[vb] = &base.FailoverLog{
			NumEntries: 2,
			LogTable: []*base.FailoverEntry{
				{Uuid: 123456, HighSeqno: 1000 + uint64(vb)},
				{Uuid: 123455, HighSeqno: 500 + uint64(vb)},
			},
		}
	}
	utils.On("GetFailoverLog", mcClient).Return(failoverLogServer1.FailoverLogMap, nil)

	// Second server fails
	utils.On("GetFailoverLog", mcClient2).Return(nil, errors.New("failed to get failover log"))
	utils.On("IsSeriousNetError", mock.Anything).Return(false)

	vblist := []uint16{0, 1, 2, 3, 4, 5, 6, 7}
	finCh := make(chan bool, 1)
	defer close(finCh)
	result, errMap, err := provider.GetFailoverLog(&base.FailoverLogRequest{VBuckets: vblist, FinCh: finCh})

	assert.NoError(err)
	assert.NotNil(result)
	assert.NotEmpty(errMap)
	assert.Contains(errMap, testServerAddr2)
	assert.Contains(errMap, "MissingVbs")

	// Only first server's vbuckets should have data
	assert.Equal(4, len(result.FailoverLogMap))

	provider.Close()
}

func TestClusterBucketStatsProvider_GetFailoverLog_NetworkError(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_GetFailoverLog_NetworkError =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_GetFailoverLog_NetworkError =================")
	assert := assert.New(t)

	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()
	mcClient := &clientMocks.ClientIface{}

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	setupTestRemoteMemcachedComponent(provider, utils, logger)
	mcClient.On("Close").Return(nil)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, provider.remoteMemcachedComponent.MaxConnsPerServer)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, provider.remoteMemcachedComponent.MaxConnsPerServer)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr] <- mcClient
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr2] <- mcClient

	// Simulate serious network error
	networkErr := errors.New("connection reset by peer")
	utils.On("GetFailoverLog", mock.Anything).Return(nil, networkErr)
	utils.On("IsSeriousNetError", networkErr).Return(true)
	mcClient.On("Close").Return(nil)

	vblist := []uint16{0, 1, 2, 3, 4, 5, 6, 7}
	finCh := make(chan bool, 1)
	defer close(finCh)
	result, errMap, err := provider.GetFailoverLog(&base.FailoverLogRequest{VBuckets: vblist, FinCh: finCh})

	assert.NoError(err)
	assert.NotNil(result)
	assert.NotEmpty(errMap)
	assert.Contains(errMap, "MissingVbs")
	assert.Equal(0, len(result.FailoverLogMap))

	provider.Close()
}

func TestClusterBucketStatsProvider_GetVBucketStats_Success(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_GetVBucketStats_Success =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_GetVBucketStats_Success =================")
	assert := assert.New(t)

	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()
	mcClient := &clientMocks.ClientIface{}

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	setupTestRemoteMemcachedComponent(provider, utils, logger)
	mcClient.On("Close").Return(nil)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, provider.remoteMemcachedComponent.MaxConnsPerServer)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, provider.remoteMemcachedComponent.MaxConnsPerServer)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr] <- mcClient
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr2] <- mcClient

	// Mock GetVBucketStats to return VBucketStatsMap directly
	expectedVBStatsMap := make(base.VBucketStatsMap)
	for vb := uint16(0); vb < 8; vb++ {
		expectedVBStatsMap[vb] = &base.VBucketStats{
			Uuid:      123456,
			HighSeqno: 1000 + uint64(vb),
		}
	}
	utils.On("GetVBucketStats", mock.Anything, mcClient).Return(expectedVBStatsMap, nil)

	requestOpts := &base.VBucketStatsRequest{
		VBuckets:   []uint16{0, 1, 2, 3, 4, 5, 6, 7},
		MaxCasOnly: false,
		FinCh:      make(chan bool, 1),
	}
	defer close(requestOpts.FinCh)

	result, errMap, err := provider.GetVBucketStats(requestOpts)

	assert.NoError(err)
	assert.NotNil(result)
	assert.Equal(len(requestOpts.VBuckets), len(result.VBStatsMap))
	assert.Empty(errMap)

	for _, vb := range requestOpts.VBuckets {
		stats, ok := result.VBStatsMap[vb]
		assert.True(ok)
		assert.NotNil(stats)
		assert.Equal(uint64(123456), stats.Uuid)
		assert.Equal(uint64(1000+vb), stats.HighSeqno)
	}

	provider.Close()
}

func TestClusterBucketStatsProvider_GetVBucketStats_MaxCasOnly(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_GetVBucketStats_MaxCasOnly =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_GetVBucketStats_MaxCasOnly =================")
	assert := assert.New(t)

	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()
	mcClient := &clientMocks.ClientIface{}

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	setupTestRemoteMemcachedComponent(provider, utils, logger)
	mcClient.On("Close").Return(nil)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, provider.remoteMemcachedComponent.MaxConnsPerServer)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, provider.remoteMemcachedComponent.MaxConnsPerServer)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr] <- mcClient
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr2] <- mcClient

	// Mock GetVBucketStats to return VBucketStatsMap with MaxCas
	expectedVBStatsMap := make(base.VBucketStatsMap)
	for vb := uint16(0); vb < 8; vb++ {
		expectedVBStatsMap[vb] = &base.VBucketStats{
			MaxCas: uint64(9999999) + uint64(vb),
		}
	}
	utils.On("GetVBucketStats", mock.Anything, mcClient).Return(expectedVBStatsMap, nil)

	requestOpts := &base.VBucketStatsRequest{
		VBuckets:   []uint16{0, 1, 2, 3, 4, 5, 6, 7},
		MaxCasOnly: true,
		FinCh:      make(chan bool, 1),
	}
	defer close(requestOpts.FinCh)

	result, errMap, err := provider.GetVBucketStats(requestOpts)

	assert.NoError(err)
	assert.NotNil(result)
	assert.Equal(len(requestOpts.VBuckets), len(result.VBStatsMap))
	assert.Empty(errMap)

	for _, vb := range requestOpts.VBuckets {
		stats, ok := result.VBStatsMap[vb]
		assert.True(ok)
		assert.NotNil(stats)
		assert.Equal(uint64(9999999+uint64(vb)), stats.MaxCas)
	}

	provider.Close()
}

func TestClusterBucketStatsProvider_GetVBucketStats_InvalidRequest(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_GetVBucketStats_InvalidRequest =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_GetVBucketStats_InvalidRequest =================")
	assert := assert.New(t)

	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	// Test with empty vbucket list
	requestOpts := &base.VBucketStatsRequest{
		VBuckets:   []uint16{},
		MaxCasOnly: false,
		FinCh:      make(chan bool, 1),
	}
	defer close(requestOpts.FinCh)

	result, errMap, err := provider.GetVBucketStats(requestOpts)

	assert.Error(err)
	assert.Nil(result)
	assert.Nil(errMap)
	assert.Contains(err.Error(), "invalid request options")

	provider.Close()
}

func TestClusterBucketStatsProvider_GetVBucketStats_PartialFailure(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_GetVBucketStats_PartialFailure =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_GetVBucketStats_PartialFailure =================")
	assert := assert.New(t)

	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()
	mcClient := &clientMocks.ClientIface{}
	mcClient2 := &clientMocks.ClientIface{}

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	setupTestRemoteMemcachedComponent(provider, utils, logger)
	mcClient.On("Close").Return(nil)
	mcClient2.On("Close").Return(nil)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, provider.remoteMemcachedComponent.MaxConnsPerServer)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, provider.remoteMemcachedComponent.MaxConnsPerServer)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr] <- mcClient
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr2] <- mcClient2

	// First server succeeds - returns stats for vbuckets 0-3
	expectedVBStatsMap := make(base.VBucketStatsMap)
	for vb := uint16(0); vb < 4; vb++ {
		expectedVBStatsMap[vb] = &base.VBucketStats{
			Uuid:      123456,
			HighSeqno: 1000 + uint64(vb),
		}
	}
	utils.On("GetVBucketStats", mock.Anything, mcClient).Return(expectedVBStatsMap, nil)

	// Second server fails
	networkErr := errors.New("timeout")
	utils.On("GetVBucketStats", mock.Anything, mcClient2).Return(base.VBucketStatsMap(nil), networkErr)
	utils.On("IsSeriousNetError", networkErr).Return(false)

	requestOpts := &base.VBucketStatsRequest{
		VBuckets:   []uint16{0, 1, 2, 3, 4, 5, 6, 7},
		MaxCasOnly: false,
		FinCh:      make(chan bool, 1),
	}
	defer close(requestOpts.FinCh)

	result, errMap, err := provider.GetVBucketStats(requestOpts)

	assert.NoError(err)
	assert.NotNil(result)
	assert.NotEmpty(errMap)
	assert.Contains(errMap, testServerAddr2)
	assert.Contains(errMap, "MissingVbs")

	// Only first server's vbuckets should have data
	assert.Equal(4, len(result.VBStatsMap))

	provider.Close()
}

func TestClusterBucketStatsProvider_Concurrent_GetFailoverLog(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_Concurrent_GetFailoverLog =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_Concurrent_GetFailoverLog =================")
	assert := assert.New(t)

	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()
	mcClient := &clientMocks.ClientIface{}

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	setupTestRemoteMemcachedComponent(provider, utils, logger)
	mcClient.On("Close").Return(nil)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, provider.remoteMemcachedComponent.MaxConnsPerServer)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, provider.remoteMemcachedComponent.MaxConnsPerServer)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr] <- mcClient
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr2] <- mcClient

	failoverLog := createTestFailoverLog()
	utils.On("GetFailoverLog", mock.AnythingOfType("*mocks.ClientIface")).Return(failoverLog.FailoverLogMap, nil)
	utils.On("GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mcClient, nil)

	// Execute concurrent requests
	var wg sync.WaitGroup
	numConcurrentRequests := 10
	vblist := []uint16{0, 1, 2, 3, 4, 5, 6, 7}

	for i := 0; i < numConcurrentRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			finCh := make(chan bool, 1)
			defer close(finCh)
			result, errMap, err := provider.GetFailoverLog(&base.FailoverLogRequest{VBuckets: vblist, FinCh: finCh})
			assert.NoError(err)
			assert.NotNil(result)
			assert.Empty(errMap)
			assert.Equal(len(vblist), len(result.FailoverLogMap))
		}()
	}

	wg.Wait()
	// given that we did >5 concurrent requests, we should have
	// maxed out the connections per server
	assert.Equal(testMaxConnectionsPerServer, len(provider.remoteMemcachedComponent.KvMemClients[testServerAddr]))
	assert.Equal(testMaxConnectionsPerServer, len(provider.remoteMemcachedComponent.KvMemClients[testServerAddr2]))
	provider.Close()
}

func TestClusterBucketStatsProvider_Idempotent_GetFailoverLog(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_Idempotent_GetFailoverLog =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_Idempotent_GetFailoverLog =================")
	assert := assert.New(t)

	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()
	mcClient := &clientMocks.ClientIface{}

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	setupTestRemoteMemcachedComponent(provider, utils, logger)
	mcClient.On("Close").Return(nil)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, provider.remoteMemcachedComponent.MaxConnsPerServer)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, provider.remoteMemcachedComponent.MaxConnsPerServer)
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr] <- mcClient
	provider.remoteMemcachedComponent.KvMemClients[testServerAddr2] <- mcClient

	failoverLog := createTestFailoverLog()
	utils.On("GetFailoverLog", mock.AnythingOfType("*mocks.ClientIface")).Return(failoverLog.FailoverLogMap, nil)

	vblist := []uint16{0, 1, 2, 3, 4, 5, 6, 7}

	// Call multiple times - should be idempotent
	for i := 0; i < 5; i++ {
		finCh := make(chan bool, 1)
		result, errMap, err := provider.GetFailoverLog(&base.FailoverLogRequest{VBuckets: vblist, FinCh: finCh})
		close(finCh)
		assert.NoError(err)
		assert.NotNil(result)
		assert.Empty(errMap)
		assert.Equal(len(vblist), len(result.FailoverLogMap))
	}

	provider.Close()
}

// ============= Test Cases for CngBucketStatsProvider =============

func TestCngBucketStatsProvider_NewCngBucketStatsProvider(t *testing.T) {
	fmt.Println("============== Test case start: TestCngBucketStatsProvider_NewCngBucketStatsProvider =================")
	defer fmt.Println("============== Test case end: TestCngBucketStatsProvider_NewCngBucketStatsProvider =================")
	assert := assert.New(t)

	utils := &utilsMock.UtilsIface{}
	logger := createTestLogger()
	getGrpcOpts := func() *base.GrpcOptions {
		return &base.GrpcOptions{
			ConnStr:        "localhost:18098",
			UseTLS:         false,
			GetCredentials: nil,
			CaCert:         nil,
		}
	}

	provider := NewCngBucketStatsProvider(testBucketName, utils, logger, getGrpcOpts)

	assert.NotNil(provider)
	assert.Equal(testBucketName, provider.bucketName)
	assert.NotNil(provider.finCh)
	assert.NotNil(provider.getGrpcOpts)
}

func TestCngBucketStatsProvider_Init_Success(t *testing.T) {
	fmt.Println("============== Test case start: TestCngBucketStatsProvider_Init_Success =================")
	defer fmt.Println("============== Test case end: TestCngBucketStatsProvider_Init_Success =================")
	// Skip creating a cngConn
	t.Skip("Skip creating a cngConn")
}

func TestCngBucketStatsProvider_Close_Success(t *testing.T) {
	fmt.Println("============== Test case start: TestCngBucketStatsProvider_Close_Success =================")
	defer fmt.Println("============== Test case end: TestCngBucketStatsProvider_Close_Success =================")
	assert := assert.New(t)

	utils := &utilsMock.UtilsIface{}
	logger := createTestLogger()
	getGrpcOpts := func() *base.GrpcOptions {
		return &base.GrpcOptions{
			ConnStr:        "localhost:18098",
			UseTLS:         false,
			GetCredentials: nil,
			CaCert:         nil,
		}
	}

	provider := NewCngBucketStatsProvider(testBucketName, utils, logger, getGrpcOpts)

	err := provider.Close()
	assert.NoError(err)

	// Close should be idempotent
	err = provider.Close()
	assert.NoError(err)

	// Verify finCh is closed
	select {
	case <-provider.finCh:
		// Expected
	default:
		t.Fatal("finCh should be closed")
	}
}

func TestCngBucketStatsProvider_GetFailoverLog_Success(t *testing.T) {
	fmt.Println("============== Test case start: TestCngBucketStatsProvider_GetFailoverLog_Success =================")
	defer fmt.Println("============== Test case end: TestCngBucketStatsProvider_GetFailoverLog_Success =================")
	assert := assert.New(t)

	utilsMockObj := &utilsMock.UtilsIface{}
	logger := createTestLogger()
	getGrpcOpts := func() *base.GrpcOptions {
		return &base.GrpcOptions{
			ConnStr:        "localhost:18098",
			UseTLS:         false,
			GetCredentials: nil,
			CaCert:         nil,
		}
	}

	provider := NewCngBucketStatsProvider(testBucketName, utilsMockObj, logger, getGrpcOpts)
	// Mock cngConn to avoid nil pointer
	provider.cngConn = &base.CngConn{}

	vblist := []uint16{0, 1, 2, 3}

	// Mock CngGetVbucketInfo to simulate successful response
	utilsMockObj.On("CngGetVbucketInfo", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		handler := args.Get(2).(utils.GrpcStreamHandler[*internal_xdcr_v1.GetVbucketInfoResponse])

		// Create response with all vbuckets
		vbInfoList := make([]*internal_xdcr_v1.GetVbucketInfoResponse_VbucketState, len(vblist))
		for i, vb := range vblist {
			hist := []*internal_xdcr_v1.GetVbucketInfoResponse_HistoryEntry{
				{
					Uuid:  123456,
					Seqno: 1000 + uint64(vb),
				},
			}
			vbInfoList[i] = &internal_xdcr_v1.GetVbucketInfoResponse_VbucketState{
				VbucketId: uint32(vb),
				Uuid:      123456,
				History:   hist,
			}
		}

		resp := &internal_xdcr_v1.GetVbucketInfoResponse{
			Vbuckets: vbInfoList,
		}
		handler.OnMessage(resp)

		// Signal completion
		handler.OnComplete()
	}).Return()

	finCh := make(chan bool, 1)
	defer close(finCh)
	result, errMap, err := provider.GetFailoverLog(&base.FailoverLogRequest{VBuckets: vblist, FinCh: finCh})

	assert.NoError(err)
	assert.NotNil(result)
	assert.Empty(errMap)
	assert.Equal(len(vblist), len(result.FailoverLogMap))

	// Verify each vbucket has failover log
	for _, vb := range vblist {
		log, err := result.GetFailoverLog(vb)
		assert.NoError(err)
		assert.NotNil(log)
		assert.Equal(uint64(1), log.NumEntries)
		entry, err := log.GetEntry(0)
		assert.NoError(err)
		assert.Equal(uint64(123456), entry.Uuid)
		assert.Equal(uint64(1000+uint64(vb)), entry.HighSeqno)
	}

	utilsMockObj.AssertExpectations(t)
	provider.Close()
}

func TestCngBucketStatsProvider_GetVBucketStats_Success(t *testing.T) {
	fmt.Println("============== Test case start: TestCngBucketStatsProvider_GetVBucketStats_Success =================")
	defer fmt.Println("============== Test case end: TestCngBucketStatsProvider_GetVBucketStats_Success =================")
	assert := assert.New(t)

	utilsMockObj := &utilsMock.UtilsIface{}
	logger := createTestLogger()
	getGrpcOpts := func() *base.GrpcOptions {
		return &base.GrpcOptions{
			ConnStr:        "localhost:18098",
			UseTLS:         false,
			GetCredentials: nil,
			CaCert:         nil,
		}
	}

	provider := NewCngBucketStatsProvider(testBucketName, utilsMockObj, logger, getGrpcOpts)
	// Mock cngConn to avoid nil pointer
	provider.cngConn = &base.CngConn{}

	vblist := []uint16{0, 1, 2, 3}

	// Mock CngGetVbucketInfo to simulate successful response
	utilsMockObj.On("CngGetVbucketInfo", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		handler := args.Get(2).(utils.GrpcStreamHandler[*internal_xdcr_v1.GetVbucketInfoResponse])

		// Create response with all vbuckets
		vbInfoList := make([]*internal_xdcr_v1.GetVbucketInfoResponse_VbucketState, len(vblist))
		for i, vb := range vblist {
			vbInfoList[i] = &internal_xdcr_v1.GetVbucketInfoResponse_VbucketState{
				VbucketId: uint32(vb),
				Uuid:      123456,
				HighSeqno: 1000 + uint64(vb),
			}
		}

		resp := &internal_xdcr_v1.GetVbucketInfoResponse{
			Vbuckets: vbInfoList,
		}
		handler.OnMessage(resp)

		// Signal completion
		handler.OnComplete()
	}).Return()

	requestOpts := &base.VBucketStatsRequest{
		VBuckets:   vblist,
		MaxCasOnly: false,
	}

	result, errMap, err := provider.GetVBucketStats(requestOpts)

	assert.NoError(err)
	assert.NotNil(result)
	assert.Empty(errMap)
	assert.Equal(len(vblist), len(result.VBStatsMap))

	// Verify each vbucket has stats
	for _, vb := range vblist {
		stats, ok := result.VBStatsMap[vb]
		assert.True(ok)
		assert.NotNil(stats)
		assert.Equal(uint64(123456), stats.Uuid)
		assert.Equal(uint64(1000+uint64(vb)), stats.HighSeqno)
	}

	utilsMockObj.AssertExpectations(t)
	provider.Close()
}

func TestCngBucketStatsProvider_GetFailoverLog_Error(t *testing.T) {
	fmt.Println("============== Test case start: TestCngBucketStatsProvider_GetFailoverLog_Error =================")
	defer fmt.Println("============== Test case end: TestCngBucketStatsProvider_GetFailoverLog_Error =================")
	assert := assert.New(t)

	utilsMockObj := &utilsMock.UtilsIface{}
	logger := createTestLogger()
	getGrpcOpts := func() *base.GrpcOptions {
		return &base.GrpcOptions{
			ConnStr:        "localhost:18098",
			UseTLS:         false,
			GetCredentials: nil,
			CaCert:         nil,
		}
	}

	provider := NewCngBucketStatsProvider(testBucketName, utilsMockObj, logger, getGrpcOpts)
	// Mock cngConn to avoid nil pointer
	provider.cngConn = &base.CngConn{}

	vblist := []uint16{0, 1, 2, 3}

	// Mock CngGetVbucketInfo to simulate error
	utilsMockObj.On("CngGetVbucketInfo", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		handler := args.Get(2).(utils.GrpcStreamHandler[*internal_xdcr_v1.GetVbucketInfoResponse])

		// Simulate error
		handler.OnError(fmt.Errorf("gRPC connection error"))
	}).Return()

	finCh := make(chan bool, 1)
	defer close(finCh)
	result, errMap, err := provider.GetFailoverLog(&base.FailoverLogRequest{VBuckets: vblist, FinCh: finCh})

	assert.Error(err)
	assert.Nil(result)
	assert.Nil(errMap)
	assert.Contains(err.Error(), "failed to get failover log")

	utilsMockObj.AssertExpectations(t)
	provider.Close()
}

func TestCngBucketStatsProvider_GetVBucketStats_Error(t *testing.T) {
	fmt.Println("============== Test case start: TestCngBucketStatsProvider_GetVBucketStats_Error =================")
	defer fmt.Println("============== Test case end: TestCngBucketStatsProvider_GetVBucketStats_Error =================")
	assert := assert.New(t)

	utilsMockObj := &utilsMock.UtilsIface{}
	logger := createTestLogger()
	getGrpcOpts := func() *base.GrpcOptions {
		return &base.GrpcOptions{
			ConnStr:        "localhost:18098",
			UseTLS:         false,
			GetCredentials: nil,
			CaCert:         nil,
		}
	}

	provider := NewCngBucketStatsProvider(testBucketName, utilsMockObj, logger, getGrpcOpts)
	// Mock cngConn to avoid nil pointer
	provider.cngConn = &base.CngConn{}

	vblist := []uint16{0, 1, 2, 3}

	// Mock CngGetVbucketInfo to simulate error
	utilsMockObj.On("CngGetVbucketInfo", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		handler := args.Get(2).(utils.GrpcStreamHandler[*internal_xdcr_v1.GetVbucketInfoResponse])

		// Simulate error
		handler.OnError(fmt.Errorf("gRPC timeout error"))
	}).Return()

	requestOpts := &base.VBucketStatsRequest{
		VBuckets:   vblist,
		MaxCasOnly: false,
	}

	result, errMap, err := provider.GetVBucketStats(requestOpts)

	assert.Error(err)
	assert.Nil(result)
	assert.Nil(errMap)
	assert.Contains(err.Error(), "failed to get vbucket stats")

	utilsMockObj.AssertExpectations(t)
	provider.Close()
}

// ============= Edge Case Tests =============

func TestClusterBucketStatsProvider_EmptyVBList(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_EmptyVBList =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_EmptyVBList =================")
	assert := assert.New(t)

	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	setupTestRemoteMemcachedComponent(provider, utils, logger)

	emptyVblist := []uint16{}
	finCh := make(chan bool, 1)
	defer close(finCh)
	result, errMap, err := provider.GetFailoverLog(&base.FailoverLogRequest{VBuckets: emptyVblist, FinCh: finCh})

	assert.Error(err)
	assert.Nil(result)
	assert.Nil(errMap)

	provider.Close()
}

func TestClusterBucketStatsProvider_KvVbMapError(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_KvVbMapError =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_KvVbMapError =================")
	assert := assert.New(t)

	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	setupTestRemoteMemcachedComponent(provider, utils, logger)
	provider.remoteMemcachedComponent.TargetKvVbMap = func() (base.KvVBMapType, error) {
		return nil, errors.New("failed to get topology")
	}

	vblist := []uint16{0, 1, 2, 3}
	finCh := make(chan bool, 1)
	defer close(finCh)
	result, errMap, err := provider.GetFailoverLog(&base.FailoverLogRequest{VBuckets: vblist, FinCh: finCh})

	assert.Error(err)
	assert.Nil(result)
	assert.Nil(errMap)
	assert.Contains(err.Error(), "failed to get server list")

	provider.Close()
}

func TestClusterBucketStatsProvider_SubscriptionCounter_Increment(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_SubscriptionCounter_Increment =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_SubscriptionCounter_Increment =================")
	assert := assert.New(t)

	remoteClusterSvc := &service_defMock.RemoteClusterSvc{}
	utils := &utilsMock.UtilsIface{}
	bucketTopologySvc := &service_defMock.BucketTopologySvc{}
	logger := createTestLogger()

	provider := NewClusterBucketStatsProvider(testBucketName, testClusterUuid, remoteClusterSvc, utils, bucketTopologySvc, testMaxConnectionsPerServer, logger)

	// Setup mocks for Init
	ref := createTestRemoteClusterRef()
	remoteClusterSvc.On("RemoteClusterByUuid", testClusterUuid, false).Return(ref, nil)
	remoteClusterSvc.On("ShouldUseAlternateAddress", ref).Return(false, nil)

	callCount := 0
	bucketTopologySvc.On("SubscribeToRemoteBucketFeed", mock.AnythingOfType("*metadata.ReplicationSpecification"), mock.AnythingOfType("string")).Run(func(args mock.Arguments) {
		callCount++
	}).Return(make(chan service_def.TargetNotification, 1), nil)
	bucketTopologySvc.On("UnSubscribeRemoteBucketFeed", mock.AnythingOfType("*metadata.ReplicationSpecification"), mock.AnythingOfType("string")).Return(nil)

	utils.On("ExecWithTimeout", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		fn := args.Get(0).(func() error)
		fn()
	}).Return(nil)

	// Init multiple times should only call setup once due to sync.Once
	provider.Init()
	provider.Init()
	provider.Init()

	// Verify subscription counter was used
	assert.GreaterOrEqual(callCount, 1)
	assert.Equal(uint64(callCount), provider.subscriptionCounter)

	provider.Close()
}

func TestClusterBucketStatsProvider_StartOp_RaceCondition(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_StartOp_RaceCondition =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_StartOp_RaceCondition =================")
	assert := assert.New(t)

	provider, mockUtils, mockBucketTopologySvc, mockRemoteClusterSvc := setupClusterBucketStatsProvider()
	setupBasicMocksForProvider(provider, mockUtils, mockBucketTopologySvc, mockRemoteClusterSvc)

	var wg sync.WaitGroup
	const numGoroutines = 100

	// Start many goroutines trying to start operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Half of them will try to start ops
			if id%2 == 0 {
				err := provider.canStartOp()
				if err == nil {
					time.Sleep(time.Microsecond * 10)
				}
			}
		}(i)
	}

	// Close in the middle
	time.Sleep(5 * time.Millisecond)

	// Wait for Close() to complete
	closeDone := make(chan bool)
	go func() {
		provider.Close()
		close(closeDone)
	}()

	wg.Wait()
	<-closeDone

	// After close, all new startOp attempts should fail
	err := provider.canStartOp()
	assert.Error(err)
	assert.Contains(err.Error(), "closed")
}

func TestClusterBucketStatsProvider_Close_WaitsForActiveOperations(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_Close_WaitsForActiveOperations =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_Close_WaitsForActiveOperations =================")
	assert := assert.New(t)

	provider, mockUtils, mockBucketTopologySvc, mockRemoteClusterSvc := setupClusterBucketStatsProvider()
	setupBasicMocksForProvider(provider, mockUtils, mockBucketTopologySvc, mockRemoteClusterSvc)

	const numOps = 10
	opsCompleted := make([]atomic.Bool, numOps)

	var wg sync.WaitGroup

	// Start several long-running operations
	for i := 0; i < numOps; i++ {
		err := provider.canStartOp()
		assert.NoError(err)

		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Simulate work
			time.Sleep(100 * time.Millisecond)
			opsCompleted[idx].Store(true)
		}(i)
	}

	// Wait a bit to ensure operations have started
	time.Sleep(10 * time.Millisecond)

	// Close should wait for all operations to complete
	closeDone := make(chan bool)
	go func() {
		provider.Close()
		close(closeDone)
	}()

	// Wait for close to complete
	select {
	case <-closeDone:
		// Good, close completed
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not complete in time - possible deadlock")
	}

	wg.Wait()

	// Verify all operations completed
	for i := 0; i < numOps; i++ {
		assert.True(opsCompleted[i].Load(), fmt.Sprintf("Operation %d should have completed", i))
	}
}

func TestClusterBucketStatsProvider_StartOp_AfterClose(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_StartOp_AfterClose =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_StartOp_AfterClose =================")
	assert := assert.New(t)

	provider, mockUtils, mockBucketTopologySvc, mockRemoteClusterSvc := setupClusterBucketStatsProvider()
	setupBasicMocksForProvider(provider, mockUtils, mockBucketTopologySvc, mockRemoteClusterSvc)

	// Close the provider
	provider.Close()

	// Try to start operation after close
	err := provider.canStartOp()

	assert.Error(err)
	assert.Contains(err.Error(), "ClusterBucketStatsProvider is closed")
}

func TestClusterBucketStatsProvider_ConcurrentOperationsAndClose_NoDeadlock(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_ConcurrentOperationsAndClose_NoDeadlock =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_ConcurrentOperationsAndClose_NoDeadlock =================")

	provider, mockUtils, mockBucketTopologySvc, mockRemoteClusterSvc := setupClusterBucketStatsProvider()

	// Setup basic mocks
	setupBasicMocksForProvider(provider, mockUtils, mockBucketTopologySvc, mockRemoteClusterSvc)

	mockUtils.On("GetFailoverLog", mock.Anything).Return(make(base.FailoverLogMapType), nil)
	mockUtils.On("GetVBucketStats", mock.Anything, mock.Anything).Return(make(base.VBucketStatsMap), nil)

	var wg sync.WaitGroup
	const numOps = 20

	// Start multiple operations concurrently
	for i := 0; i < numOps; i++ {
		wg.Add(2)

		// GetFailoverLog operations
		go func() {
			defer wg.Done()
			request := &base.FailoverLogRequest{
				VBuckets: []uint16{0, 1},
				FinCh:    make(chan bool),
			}
			provider.GetFailoverLog(request)
		}()

		// GetVBucketStats operations
		go func() {
			defer wg.Done()
			request := &base.VBucketStatsRequest{
				VBuckets:   []uint16{0, 1},
				FinCh:      make(chan bool),
				MaxCasOnly: false,
			}
			provider.GetVBucketStats(request)
		}()
	}

	// Wait a bit then close
	time.Sleep(50 * time.Millisecond)

	// Close should not deadlock
	closeDone := make(chan bool)
	go func() {
		provider.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Deadlock detected: Close did not complete in time")
	}

	wg.Wait()
}

func TestClusterBucketStatsProvider_MultipleClose_Idempotent(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_MultipleClose_Idempotent =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_MultipleClose_Idempotent =================")
	assert := assert.New(t)

	provider, mockUtils, mockBucketTopologySvc, mockRemoteClusterSvc := setupClusterBucketStatsProvider()
	setupBasicMocksForProvider(provider, mockUtils, mockBucketTopologySvc, mockRemoteClusterSvc)

	// Close multiple times
	err1 := provider.Close()
	err2 := provider.Close()
	err3 := provider.Close()

	// All should succeed
	assert.NoError(err1)
	assert.NoError(err2)
	assert.NoError(err3)

	// Provider should be closed
	assert.True(provider.isClosed())
}

func TestClusterBucketStatsProvider_ConcurrentClose_RaceCondition(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_ConcurrentClose_RaceCondition =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_ConcurrentClose_RaceCondition =================")

	provider, mockUtils, mockBucketTopologySvc, mockRemoteClusterSvc := setupClusterBucketStatsProvider()
	setupBasicMocksForProvider(provider, mockUtils, mockBucketTopologySvc, mockRemoteClusterSvc)

	var wg sync.WaitGroup
	const numClosers = 20

	// Many goroutines trying to close
	for i := 0; i < numClosers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			provider.Close()
		}()
	}

	wg.Wait()

	// Should not panic and should be closed
	assert.True(t, provider.isClosed())
}

func TestClusterBucketStatsProvider_OperationAfterClose_Fails(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_OperationAfterClose_Fails =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_OperationAfterClose_Fails =================")
	assert := assert.New(t)

	provider, mockUtils, mockBucketTopologySvc, mockRemoteClusterSvc := setupClusterBucketStatsProvider()
	setupBasicMocksForProvider(provider, mockUtils, mockBucketTopologySvc, mockRemoteClusterSvc)

	// Close the provider
	provider.Close()

	// Try GetFailoverLog after close
	request1 := &base.FailoverLogRequest{
		VBuckets: []uint16{0, 1},
		FinCh:    make(chan bool),
	}
	_, _, err1 := provider.GetFailoverLog(request1)
	assert.Error(err1)
	assert.Contains(err1.Error(), "closed")

	// Try GetVBucketStats after close
	request2 := &base.VBucketStatsRequest{
		VBuckets:   []uint16{0, 1},
		FinCh:      make(chan bool),
		MaxCasOnly: false,
	}
	_, _, err2 := provider.GetVBucketStats(request2)
	assert.Error(err2)
	assert.Contains(err2.Error(), "closed")
}

func TestClusterBucketStatsProvider_StartOp_EndOp_Balance(t *testing.T) {
	fmt.Println("============== Test case start: TestClusterBucketStatsProvider_StartOp_EndOp_Balance =================")
	defer fmt.Println("============== Test case end: TestClusterBucketStatsProvider_StartOp_EndOp_Balance =================")
	assert := assert.New(t)

	provider, mockUtils, mockBucketTopologySvc, mockRemoteClusterSvc := setupClusterBucketStatsProvider()
	setupBasicMocksForProvider(provider, mockUtils, mockBucketTopologySvc, mockRemoteClusterSvc)

	const numOps = 100
	var wg sync.WaitGroup

	// Start many operations
	for i := 0; i < numOps; i++ {
		err := provider.canStartOp()
		assert.NoError(err)

		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Microsecond * 10)
		}()
	}

	// Wait for all to complete
	wg.Wait()

	// Close should complete immediately since all ops are done
	closeDone := make(chan bool)
	go func() {
		provider.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		// Success
	case <-time.After(10 * time.Millisecond):
		t.Fatal("Close did not complete immediately after all ops finished")
	}
}

// Helper function to setup basic mocks for testing
func setupBasicMocksForProvider(provider *ClusterBucketStatsProvider, mockUtils *utilsMock.UtilsIface, mockBucketTopologySvc *service_defMock.BucketTopologySvc, mockRemoteClusterSvc *service_defMock.RemoteClusterSvc) {
	// Setup remote cluster service
	testRef := createTestRemoteClusterRef()
	mockRemoteClusterSvc.On("RemoteClusterByUuid", mock.Anything, mock.Anything).Return(testRef, nil)
	mockRemoteClusterSvc.On("ShouldUseAlternateAddress", mock.Anything).Return(false, nil)

	// Setup bucket topology service
	mockTargetNotification := &service_defMock.TargetNotification{}
	kvVbMap := createTestKvVbMap()
	mockTargetNotification.On("GetTargetServerVBMap").Return(kvVbMap)
	mockTargetNotification.On("Recycle").Return()

	targetNotificationCh := make(chan service_def.TargetNotification, 1)
	targetNotificationCh <- mockTargetNotification
	mockBucketTopologySvc.On("SubscribeToRemoteBucketFeed", mock.Anything, mock.Anything).Return(targetNotificationCh, nil)
	mockBucketTopologySvc.On("UnSubscribeRemoteBucketFeed", mock.Anything, mock.Anything).Return(nil)

	// Setup memcached connection
	mockClient := &clientMocks.ClientIface{}
	mockClient.On("Close").Return(nil)
	mockUtils.On("GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockClient, nil)

	// Setup ExecWithTimeout
	mockUtils.On("ExecWithTimeout", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		fn := args.Get(0).(func() error)
		fn()
	}).Return(nil)

	// Initialize provider
	provider.Init()
}
