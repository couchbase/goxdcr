// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package Component

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mcc "github.com/couchbase/gomemcached/client"
	clientMocks "github.com/couchbase/gomemcached/client/mocks"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
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
	testServerAddr3             = "127.0.0.1:11212"
	testMaxConnectionsPerServer = 5
	testUserAgent               = "testAgent"
	testServerAddr1SslConnStr   = "127.0.0.1:11207"
	testServerAddr2SslConnStr   = "127.0.0.1:11208"
	testServerAddr3SslConnStr   = "127.0.0.1:11209"
)

// Helper functions for creating test data

func createTestLogger() *log.CommonLogger {
	return log.NewLogger("targetConnComponentTest", log.DefaultLoggerContext)
}

func createTestRemoteClusterRef() *metadata.RemoteClusterReference {
	ref, _ := metadata.NewRemoteClusterReference(testClusterUuid, "testCluster", "localhost:8091", "admin", "password", "", false, "", nil, nil, nil, nil)
	return ref
}

func createTestKvVbMap() base.KvVBMapType {
	kvVbMap := make(base.KvVBMapType)
	kvVbMap[testServerAddr] = []uint16{0, 1, 2, 3}
	kvVbMap[testServerAddr2] = []uint16{4, 5, 6, 7}
	return kvVbMap
}

func createTestKvVbMapThreeServers() base.KvVBMapType {
	kvVbMap := make(base.KvVBMapType)
	kvVbMap[testServerAddr] = []uint16{0, 1, 2}
	kvVbMap[testServerAddr2] = []uint16{3, 4, 5}
	kvVbMap[testServerAddr3] = []uint16{6, 7, 8}
	return kvVbMap
}

func createMockClient() *clientMocks.ClientIface {
	mockClient := &clientMocks.ClientIface{}
	mockClient.On("Close").Return(nil)
	return mockClient
}

func setupBasicRemoteMemcachedComponent(maxConns int) (*RemoteMemcachedComponent, *utilsMock.UtilsIface, chan bool) {
	logger := createTestLogger()
	finCh := make(chan bool)
	utils := &utilsMock.UtilsIface{}

	component := NewRemoteMemcachedComponent(logger, finCh, utils, testBucketName, testUserAgent, maxConns)

	// Set up basic getters
	testRef := createTestRemoteClusterRef()
	component.SetRefGetter(func() *metadata.RemoteClusterReference {
		return testRef
	})

	kvVbMap := createTestKvVbMap()
	component.SetTargetKvVbMapGetter(func() (base.KvVBMapType, error) {
		return kvVbMap, nil
	})

	component.SetAlternateAddressChecker(func(ref *metadata.RemoteClusterReference) (bool, error) {
		return false, nil
	})

	return component, utils, finCh
}

// ============= Test Cases for RemoteMemcachedComponent =============

func TestRemoteMemcachedComponent_NewRemoteMemcachedComponent(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_NewRemoteMemcachedComponent =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_NewRemoteMemcachedComponent =================")
	assert := assert.New(t)

	logger := createTestLogger()
	finCh := make(chan bool)
	utils := &utilsMock.UtilsIface{}

	component := NewRemoteMemcachedComponent(logger, finCh, utils, testBucketName, testUserAgent, testMaxConnectionsPerServer)

	assert.NotNil(component)
	assert.NotNil(component.InitConnDone)
	assert.Equal(finCh, component.FinishCh)
	assert.Equal(logger, component.LoggerImpl)
	assert.NotNil(component.KvMemClients)
	assert.Equal(0, len(component.KvMemClients))
	assert.Equal(testBucketName, component.TargetBucketname)
	assert.Equal(testUserAgent, component.UserAgent)
	assert.Equal(testMaxConnectionsPerServer, component.MaxConnsPerServer)
}

func TestRemoteMemcachedComponent_SetMaxConnectionsPerServer_ValidValues(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_SetMaxConnectionsPerServer_ValidValues =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_SetMaxConnectionsPerServer_ValidValues =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(5)
	defer close(finCh)

	// Test setting to a valid value
	component.SetMaxConnectionsPerServer(10)
	assert.Equal(10, component.MaxConnsPerServer)

	// Test setting to another valid value
	component.SetMaxConnectionsPerServer(3)
	assert.Equal(3, component.MaxConnsPerServer)
}

func TestRemoteMemcachedComponent_SetMaxConnectionsPerServer_InvalidValues(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_SetMaxConnectionsPerServer_InvalidValues =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_SetMaxConnectionsPerServer_InvalidValues =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(5)
	defer close(finCh)

	// Test setting to 0 should set to 1
	component.SetMaxConnectionsPerServer(0)
	assert.Equal(1, component.MaxConnsPerServer)

	// Test setting to negative should set to 1
	component.SetMaxConnectionsPerServer(-5)
	assert.Equal(1, component.MaxConnsPerServer)
}

func TestRemoteMemcachedComponent_SetMaxConnectionsPerServer_WithExistingConnections(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_SetMaxConnectionsPerServer_WithExistingConnections =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_SetMaxConnectionsPerServer_WithExistingConnections =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(5)
	defer close(finCh)

	// Create a channel with some clients
	clientChan := make(chan mcc.ClientIface, 5)
	mockClient1 := createMockClient()
	mockClient2 := createMockClient()
	mockClient3 := createMockClient()

	clientChan <- mockClient1
	clientChan <- mockClient2
	clientChan <- mockClient3

	component.KvMemClients[testServerAddr] = clientChan

	// Reduce max connections to 2 - should close excess connection
	component.SetMaxConnectionsPerServer(2)

	assert.Equal(2, component.MaxConnsPerServer)

	// New channel should have capacity 2 and contain at most 2 clients
	newChan := component.KvMemClients[testServerAddr]
	assert.NotNil(newChan)
	assert.Equal(2, cap(newChan))
	assert.LessOrEqual(len(newChan), 2)

	// Verify mockClient3.Close() was called
	mockClient3.AssertCalled(t, "Close")
}

func TestRemoteMemcachedComponent_ReconfigureConnectionPool_IncreaseSize(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_ReconfigureConnectionPool_IncreaseSize =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_ReconfigureConnectionPool_IncreaseSize =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(2)
	defer close(finCh)

	// Create a channel with 2 clients
	clientChan := make(chan mcc.ClientIface, 2)
	mockClient1 := createMockClient()
	mockClient2 := createMockClient()

	clientChan <- mockClient1
	clientChan <- mockClient2

	component.KvMemClients[testServerAddr] = clientChan

	// Increase capacity
	component.SetMaxConnectionsPerServer(5)

	newChan := component.KvMemClients[testServerAddr]
	assert.Equal(5, cap(newChan))
	assert.Equal(2, len(newChan)) // Both clients should still be there
}

func TestRemoteMemcachedComponent_ReconfigureConnectionPool_DecreaseSize(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_ReconfigureConnectionPool_DecreaseSize =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_ReconfigureConnectionPool_DecreaseSize =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(5)
	defer close(finCh)

	// Create a channel with 3 clients
	clientChan := make(chan mcc.ClientIface, 5)
	mockClient1 := createMockClient()
	mockClient2 := createMockClient()
	mockClient3 := createMockClient()

	clientChan <- mockClient1
	clientChan <- mockClient2
	clientChan <- mockClient3

	component.KvMemClients[testServerAddr] = clientChan

	// Decrease capacity to 2
	component.SetMaxConnectionsPerServer(2)

	newChan := component.KvMemClients[testServerAddr]
	assert.Equal(2, cap(newChan))
	assert.Equal(2, len(newChan))

	// One of the clients should have been closed
	mockClient3.AssertCalled(t, "Close")
}

func TestRemoteMemcachedComponent_InitConnections_Success_NonSSL(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_InitConnections_Success_NonSSL =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_InitConnections_Success_NonSSL =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	mockClient := createMockClient()
	utils.On("GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockClient, nil)

	err := component.InitConnections()

	assert.NoError(err)
	assert.NotNil(component.KvMemClients[testServerAddr])
	assert.NotNil(component.KvMemClients[testServerAddr2])

	// Each server should have one client in the pool
	assert.Equal(1, len(component.KvMemClients[testServerAddr]))
	assert.Equal(1, len(component.KvMemClients[testServerAddr2]))
}

func TestRemoteMemcachedComponent_InitConnections_Success_SSL(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_InitConnections_Success_SSL =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_InitConnections_Success_SSL =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Create SSL-enabled ref
	sslRef, _ := metadata.NewRemoteClusterReference(testClusterUuid, "testCluster", "localhost:18091", "admin", "password", "", true, "", nil, nil, nil, nil)
	sslRef.SetEncryptionType(metadata.EncryptionType_Full)

	component.SetRefGetter(func() *metadata.RemoteClusterReference {
		return sslRef
	})

	mockClient := createMockClient()

	// Mock SSL port map
	sslPortMap := base.SSLPortMap{
		testServerAddr:  11207,
		testServerAddr2: 11208,
	}
	utils.On("GetMemcachedSSLPortMap", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(sslPortMap, nil)

	// Mock TLS connection creation
	utils.On("ExponentialBackoffExecutorWithFinishSignal", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockClient, nil)

	err := component.InitConnections()

	assert.NoError(err)
	assert.NotNil(component.SslConStrMap)
	assert.Equal(2, len(component.SslConStrMap))
	assert.Equal(testServerAddr1SslConnStr, component.SslConStrMap[testServerAddr])
	assert.Equal(testServerAddr2SslConnStr, component.SslConStrMap[testServerAddr2])
}

func TestRemoteMemcachedComponent_InitConnections_KvVbMapError(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_InitConnections_KvVbMapError =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_InitConnections_KvVbMapError =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Override TargetKvVbMap to return error
	component.SetTargetKvVbMapGetter(func() (base.KvVBMapType, error) {
		return nil, errors.New("failed to get topology")
	})

	err := component.InitConnections()

	assert.Error(err)
	assert.Contains(err.Error(), "failed to get topology")
}

func TestRemoteMemcachedComponent_InitConnections_ClientCreationPartialFailure(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_InitConnections_ClientCreationPartialFailure =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_InitConnections_ClientCreationPartialFailure =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	mockClient := createMockClient()

	// First call succeeds, subsequent calls fail
	utils.On("GetRemoteMemcachedConnection", testServerAddr, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockClient, nil).Once()
	utils.On("GetRemoteMemcachedConnection", testServerAddr2, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("connection failed"))

	err := component.InitConnections()

	// Should not return error even if some clients fail to initialize
	assert.NoError(err)

	// At least one server should have a client
	totalClients := len(component.KvMemClients[testServerAddr]) + len(component.KvMemClients[testServerAddr2])
	assert.Equal(totalClients, 1)
}

func TestRemoteMemcachedComponent_InitConnections_Idempotent(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_InitConnections_Idempotent =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_InitConnections_Idempotent =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	mockClient := createMockClient()
	utils.On("GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockClient, nil)

	// Call InitConnections multiple times
	err1 := component.InitConnections()
	err2 := component.InitConnections()
	err3 := component.InitConnections()

	assert.NoError(err1)
	assert.NoError(err2)
	assert.NoError(err3)

	// Should only initialize once (due to sync.Once)
	// Each server should still have exactly one client
	assert.Equal(1, len(component.KvMemClients[testServerAddr]))
	assert.Equal(1, len(component.KvMemClients[testServerAddr2]))
}

func TestRemoteMemcachedComponent_InitConnections_ConcurrentCalls(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_InitConnections_ConcurrentCalls =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_InitConnections_ConcurrentCalls =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	mockClient := createMockClient()
	utils.On("GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockClient, nil)

	// Call InitConnections concurrently
	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := component.InitConnections()
			assert.NoError(err)
		}()
	}

	wg.Wait()

	// Should only initialize once due to sync.Once
	assert.Equal(1, len(component.KvMemClients[testServerAddr]))
	assert.Equal(1, len(component.KvMemClients[testServerAddr2]))
}

func TestRemoteMemcachedComponent_WaitForInitConnDone_FinishChannelClosed(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_WaitForInitConnDone_FinishChannelClosed =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_WaitForInitConnDone_FinishChannelClosed =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)

	// Close finish channel before waiting
	close(finCh)

	// Should return immediately
	done := make(chan bool)
	go func() {
		component.WaitForInitConnDone()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		assert.Fail("WaitForInitConnDone did not return when finish channel closed")
	}
}

func TestRemoteMemcachedComponent_WaitForInitConnDone_InitCompletes(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_WaitForInitConnDone_InitCompletes =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_WaitForInitConnDone_InitCompletes =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	mockClient := createMockClient()
	utils.On("GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockClient, nil)

	// Start InitConnections in goroutine
	go func() {
		component.InitConnections()
	}()

	// Should return when init completes
	done := make(chan bool)
	go func() {
		component.WaitForInitConnDone()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		assert.Fail("WaitForInitConnDone did not return when init completed")
	}
}

func TestRemoteMemcachedComponent_AcquireClient_FromPool(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_AcquireClient_FromPool =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_AcquireClient_FromPool =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Pre-populate pool with a client
	mockClient := createMockClient()
	clientChan := make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	clientChan <- mockClient
	component.KvMemClients[testServerAddr] = clientChan

	// Acquire client - should come from pool
	client, err := component.AcquireClient(testServerAddr)

	assert.NoError(err)
	assert.Equal(mockClient, client)
	assert.Equal(0, len(clientChan)) // Pool should be empty now
}

func TestRemoteMemcachedComponent_AcquireClient_CreateNew(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_AcquireClient_CreateNew =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_AcquireClient_CreateNew =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	mockClient := createMockClient()
	utils.On("GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockClient, nil)

	// Pool is empty - should create new client
	client, err := component.AcquireClient(testServerAddr)

	assert.NoError(err)
	assert.NotNil(client)

	// verify that the mock was called
	utils.AssertExpectations(t)
	utils.AssertCalled(t, "GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestRemoteMemcachedComponent_AcquireClient_CreationError(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_AcquireClient_CreationError =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_AcquireClient_CreationError =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	utils.On("GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("connection failed"))

	// Should fail to create client
	client, err := component.AcquireClient(testServerAddr)

	assert.Error(err)
	assert.Nil(client)
	assert.Contains(err.Error(), "connection failed")
}

func TestRemoteMemcachedComponent_ReleaseClient_ToPool(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_ReleaseClient_ToPool =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_ReleaseClient_ToPool =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Create empty pool
	clientChan := make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.KvMemClients[testServerAddr] = clientChan

	mockClient := createMockClient()

	// Release client - should go to pool
	component.ReleaseClient(testServerAddr, mockClient)

	assert.Equal(1, len(clientChan))

	// Verify it's the same client
	clientFromPool := <-clientChan
	assert.Equal(mockClient, clientFromPool)
}

func TestRemoteMemcachedComponent_ReleaseClient_PoolFull(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_ReleaseClient_PoolFull =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_ReleaseClient_PoolFull =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(2)
	defer close(finCh)

	// Create pool with capacity 2
	clientChan := make(chan mcc.ClientIface, 2)
	component.KvMemClients[testServerAddr] = clientChan

	// Fill the pool
	mockClient1 := createMockClient()
	mockClient2 := createMockClient()
	clientChan <- mockClient1
	clientChan <- mockClient2

	// Try to release another client - should be closed
	mockClient3 := createMockClient()
	component.ReleaseClient(testServerAddr, mockClient3)

	// Pool should still have 2 clients
	assert.Equal(2, len(clientChan))

	// mockClient3 should have been closed
	mockClient3.AssertCalled(t, "Close")
}

func TestRemoteMemcachedComponent_ReleaseClient_NilClient(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_ReleaseClient_NilClient =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_ReleaseClient_NilClient =================")

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Release nil client - should be no-op
	component.ReleaseClient(testServerAddr, nil)

	// Should not panic or error
}

func TestRemoteMemcachedComponent_AcquireRelease_Concurrent(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_AcquireRelease_Concurrent =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_AcquireRelease_Concurrent =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(10)
	defer close(finCh)

	// Mock client creation - return new client each time
	utils.On("GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(createMockClient(), nil)

	// Concurrently acquire and release clients
	var wg sync.WaitGroup
	numGoroutines := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Acquire
			client, err := component.AcquireClient(testServerAddr)
			assert.NoError(err)
			assert.NotNil(client)

			// Simulate some work
			time.Sleep(1 * time.Millisecond)

			// Release
			component.ReleaseClient(testServerAddr, client)
		}()
	}

	wg.Wait()

	// Pool should not exceed max connections
	clientChan := component.KvMemClients[testServerAddr]
	assert.LessOrEqual(len(clientChan), 10)
	utils.AssertExpectations(t)
	utils.AssertCalled(t, "GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestRemoteMemcachedComponent_GetOrCreateClientChannel_Concurrent(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_GetOrCreateClientChannel_Concurrent =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_GetOrCreateClientChannel_Concurrent =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Concurrently create channel
	var wg sync.WaitGroup
	numGoroutines := 100
	channels := make([]chan mcc.ClientIface, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			channels[idx] = component.createClientChannel(testServerAddr)
		}(i)
	}

	wg.Wait()

	// All goroutines should get the same channel
	firstChannel := channels[0]
	for i := 1; i < numGoroutines; i++ {
		assert.Equal(firstChannel, channels[i])
	}

	// Only one channel should be created
	assert.Equal(1, len(component.KvMemClients))
}

func TestRemoteMemcachedComponent_CloseConnections(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_CloseConnections =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_CloseConnections =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Add some clients to pools
	mockClient1 := createMockClient()
	mockClient2 := createMockClient()
	mockClient3 := createMockClient()

	clientChan1 := make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	clientChan2 := make(chan mcc.ClientIface, testMaxConnectionsPerServer)

	clientChan1 <- mockClient1
	clientChan1 <- mockClient2
	clientChan2 <- mockClient3

	component.KvMemClients[testServerAddr] = clientChan1
	component.KvMemClients[testServerAddr2] = clientChan2

	// Close all connections
	component.Close()

	// All clients should have been closed
	mockClient1.AssertCalled(t, "Close")
	mockClient2.AssertCalled(t, "Close")
	mockClient3.AssertCalled(t, "Close")

	// Maps should be empty
	assert.Equal(0, len(component.KvMemClients))
}

func TestRemoteMemcachedComponent_DeleteMemClientsNoLock(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_DeleteMemClientsNoLock =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_DeleteMemClientsNoLock =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Add clients to a pool
	mockClient1 := createMockClient()
	mockClient2 := createMockClient()

	clientChan := make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	clientChan <- mockClient1
	clientChan <- mockClient2

	component.KvMemClients[testServerAddr] = clientChan
	component.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.knownServers[testServerAddr] = true
	component.knownServers[testServerAddr2] = true

	// Delete clients for one server
	component.DeleteMemClientsNoLock(testServerAddr)

	// Clients should be closed
	mockClient1.AssertCalled(t, "Close")
	mockClient2.AssertCalled(t, "Close")

	// Server should be removed from KvMemClients map
	// Note: DeleteMemClientsNoLock does NOT remove from knownServers;
	// that's done by removeServers which wraps this method.
	_, exists := component.KvMemClients[testServerAddr]
	assert.False(exists)

	// Other server should still exist
	_, exists = component.KvMemClients[testServerAddr2]
	assert.True(exists)
}

func TestRemoteMemcachedComponent_DeleteMemClientsNoLock_NonExistentServer(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_DeleteMemClientsNoLock_NonExistentServer =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_DeleteMemClientsNoLock_NonExistentServer =================")

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Delete non-existent server - should not panic
	component.DeleteMemClientsNoLock("nonexistent:1234")

	// Should not error or panic
}

func TestRemoteMemcachedComponent_MonitorTopology_RemovesStaleServers(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_MonitorTopology_RemovesStaleServers =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_MonitorTopology_RemovesStaleServers =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)

	// Use a mutex-protected getter to avoid racing with MonitorTopology reads
	var topoMtx sync.Mutex
	currentKvVbMap := createTestKvVbMapThreeServers()
	component.SetTargetKvVbMapGetter(func() (base.KvVBMapType, error) {
		topoMtx.Lock()
		defer topoMtx.Unlock()
		return currentKvVbMap, nil
	})

	// Add clients for all 3 servers
	mockClient1 := createMockClient()
	mockClient2 := createMockClient()
	mockClient3 := createMockClient()

	component.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.KvMemClients[testServerAddr3] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)

	// Also populate knownServers so diffTopology has the correct baseline
	component.knownServers[testServerAddr] = true
	component.knownServers[testServerAddr2] = true
	component.knownServers[testServerAddr3] = true

	component.KvMemClients[testServerAddr] <- mockClient1
	component.KvMemClients[testServerAddr2] <- mockClient2
	component.KvMemClients[testServerAddr3] <- mockClient3

	// Adjust the topology change check interval to 100ms
	originalTopologyChangeCheckInterval := base.TopologyChangeCheckInterval
	base.TopologyChangeCheckInterval = 100 * time.Millisecond

	// Start monitoring
	monitorDone := make(chan struct{})
	go func() {
		component.MonitorTopology()
		close(monitorDone)
	}()

	// Simulate topology change - remove server3
	time.Sleep(100 * time.Millisecond)
	topoMtx.Lock()
	currentKvVbMap = createTestKvVbMap()
	topoMtx.Unlock()

	// Wait for topology check (check interval is adjusted to 100ms)
	time.Sleep(200 * time.Millisecond)

	// Stop MonitorTopology before asserting on shared state
	close(finCh)
	<-monitorDone
	base.TopologyChangeCheckInterval = originalTopologyChangeCheckInterval

	// Server3 should have been removed
	_, exists := component.KvMemClients[testServerAddr3]
	assert.False(exists)
	mockClient3.AssertCalled(t, "Close")

	// Server1 should still exist
	_, exists = component.KvMemClients[testServerAddr]
	assert.True(exists)
	assert.Equal(1, len(component.KvMemClients[testServerAddr]))
	assert.Equal(mockClient1, <-component.KvMemClients[testServerAddr])

	// Server2 should still exist
	_, exists = component.KvMemClients[testServerAddr2]
	assert.True(exists)
	assert.Equal(1, len(component.KvMemClients[testServerAddr2]))
	assert.Equal(mockClient2, <-component.KvMemClients[testServerAddr2])
}

func TestRemoteMemcachedComponent_MonitorTopology_HandlesErrors(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_MonitorTopology_HandlesErrors =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_MonitorTopology_HandlesErrors =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)

	// First return success, then error
	var callCount atomic.Int64
	component.SetTargetKvVbMapGetter(func() (base.KvVBMapType, error) {
		cnt := callCount.Add(1)
		if cnt == 1 {
			return createTestKvVbMap(), nil
		}
		return nil, errors.New("topology fetch error")
	})

	// Adjust the topology change check interval to 100ms
	originalTopologyChangeCheckInterval := base.TopologyChangeCheckInterval
	base.TopologyChangeCheckInterval = 100 * time.Millisecond

	// Start monitoring
	monitorDone := make(chan struct{})
	go func() {
		component.MonitorTopology()
		close(monitorDone)
	}()

	// Wait a bit then close
	time.Sleep(500 * time.Millisecond)
	// ensure the callCount to TargetKvVbMapGetter is greater than 1
	assert.Greater(callCount.Load(), int64(1))

	// close the finish channel and wait for MonitorTopology to exit
	close(finCh)
	<-monitorDone
	base.TopologyChangeCheckInterval = originalTopologyChangeCheckInterval

	// Should not panic on error
}

func TestRemoteMemcachedComponent_MonitorTopology_StopsOnFinish(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_MonitorTopology_StopsOnFinish =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_MonitorTopology_StopsOnFinish =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)

	// Start monitoring
	monitorDone := make(chan bool)
	go func() {
		component.MonitorTopology()
		close(monitorDone)
	}()

	// Close finish channel
	close(finCh)

	// Monitor should stop
	select {
	case <-monitorDone:
		// Success
	case <-time.After(1 * time.Second):
		assert.Fail("MonitorTopology did not stop when finish channel closed")
	}
}

func TestRemoteMemcachedComponent_InitSSLConStrMap_Success(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_InitSSLConStrMap_Success =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_InitSSLConStrMap_Success =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Create SSL-enabled ref
	sslRef, _ := metadata.NewRemoteClusterReference(testClusterUuid, "testCluster", "localhost:18091", "admin", "password", "", true, "", nil, nil, nil, nil)
	sslRef.SetEncryptionType(metadata.EncryptionType_Full)

	component.SetRefGetter(func() *metadata.RemoteClusterReference {
		return sslRef
	})

	// Mock SSL port map
	sslPortMap := base.SSLPortMap{
		testServerAddr:  11207,
		testServerAddr2: 11208,
	}
	utils.On("GetMemcachedSSLPortMap", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(sslPortMap, nil)

	err := component.InitSSLConStrMap()

	assert.NoError(err)
	assert.NotNil(component.SslConStrMap)
	assert.Equal(2, len(component.SslConStrMap))

	// Verify SSL connection strings are formed correctly
	assert.Equal(testServerAddr1SslConnStr, component.SslConStrMap[testServerAddr])
	assert.Equal(testServerAddr2SslConnStr, component.SslConStrMap[testServerAddr2])
}

func TestRemoteMemcachedComponent_InitSSLConStrMap_PortMapError(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_InitSSLConStrMap_PortMapError =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_InitSSLConStrMap_PortMapError =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Create SSL-enabled ref
	sslRef, _ := metadata.NewRemoteClusterReference(testClusterUuid, "testCluster", "localhost:18091", "admin", "password", "", true, "", nil, nil, nil, nil)
	sslRef.SetEncryptionType(metadata.EncryptionType_Full)

	component.SetRefGetter(func() *metadata.RemoteClusterReference {
		return sslRef
	})

	// Mock error
	utils.On("GetMemcachedSSLPortMap", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("port map error"))

	err := component.InitSSLConStrMap()

	assert.Error(err)
	assert.Contains(err.Error(), "port map error")
}

func TestRemoteMemcachedComponent_InitSSLConStrMap_MissingPort(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_InitSSLConStrMap_MissingPort =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_InitSSLConStrMap_MissingPort =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Create SSL-enabled ref
	sslRef, _ := metadata.NewRemoteClusterReference(testClusterUuid, "testCluster", "localhost:18091", "admin", "password", "", true, "", nil, nil, nil, nil)
	sslRef.SetEncryptionType(metadata.EncryptionType_Full)

	component.SetRefGetter(func() *metadata.RemoteClusterReference {
		return sslRef
	})

	// Mock incomplete port map (missing one server)
	sslPortMap := base.SSLPortMap{
		testServerAddr: 11207,
		// testServerAddr2 is missing
	}
	utils.On("GetMemcachedSSLPortMap", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(sslPortMap, nil)

	err := component.InitSSLConStrMap()

	assert.Error(err)
	assert.Contains(err.Error(), "can't get remote memcached ssl port")
}

func TestRemoteMemcachedComponent_GetNewMemcachedClient_NonSSL(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_GetNewMemcachedClient_NonSSL =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_GetNewMemcachedClient_NonSSL =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	mockClient := createMockClient()
	utils.On("GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockClient, nil)

	client, err := component.GetNewMemcachedClient(testServerAddr)

	assert.NoError(err)
	assert.NotNil(client)
	assert.Equal(mockClient, client)
}

func TestRemoteMemcachedComponent_GetNewMemcachedClient_SSL(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_GetNewMemcachedClient_SSL =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_GetNewMemcachedClient_SSL =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Create SSL-enabled ref
	sslRef, _ := metadata.NewRemoteClusterReference(testClusterUuid, "testCluster", "localhost:18091", "admin", "password", "", true, "", nil, nil, nil, nil)
	sslRef.SetEncryptionType(metadata.EncryptionType_Full)

	component.SetRefGetter(func() *metadata.RemoteClusterReference {
		return sslRef
	})

	// Set SSL connection string map
	component.SslConStrMap = map[string]string{
		testServerAddr: "127.0.0.1:11207",
	}

	mockClient := createMockClient()
	utils.On("ExponentialBackoffExecutorWithFinishSignal", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockClient, nil)

	client, err := component.GetNewMemcachedClient(testServerAddr)

	assert.NoError(err)
	assert.NotNil(client)
}

// Race condition tests
func TestRemoteMemcachedComponent_ConcurrentReconfiguration(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_ConcurrentReconfiguration =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_ConcurrentReconfiguration =================")

	component, utils, finCh := setupBasicRemoteMemcachedComponent(10)
	defer close(finCh)

	utils.On("GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(createMockClient(), nil)

	// Add some initial clients
	for i := 0; i < 5; i++ {
		clientChan := make(chan mcc.ClientIface, 10)
		clientChan <- createMockClient()
		clientChan <- createMockClient()
		component.KvMemClients[fmt.Sprintf("server%d:11210", i)] = clientChan
	}

	var wg sync.WaitGroup

	// Concurrent reconfigurations
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			newMax := 5 + (id % 10)
			component.SetMaxConnectionsPerServer(newMax)
		}(i)
	}

	// Concurrent client acquisitions
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			server := fmt.Sprintf("server%d:11210", id%5)
			client, err := component.AcquireClient(server)
			if err == nil && client != nil {
				time.Sleep(time.Millisecond)
				component.ReleaseClient(server, client)
			}
		}(i)
	}

	wg.Wait()

	utils.AssertExpectations(t)
	utils.AssertCalled(t, "GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)

	// verify that each server has atleast one client
	for _, clientChan := range component.KvMemClients {
		assert.GreaterOrEqual(t, len(clientChan), 1)
	}
}

func TestRemoteMemcachedComponent_ConcurrentClose(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_ConcurrentClose =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_ConcurrentClose =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	utils.On("GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(createMockClient(), nil)

	// Add some clients
	for i := 0; i < 3; i++ {
		clientChan := make(chan mcc.ClientIface, testMaxConnectionsPerServer)
		clientChan <- createMockClient()
		component.KvMemClients[fmt.Sprintf("server%d:11210", i)] = clientChan
	}

	var wg sync.WaitGroup

	// Multiple concurrent close calls
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			component.Close()
		}()
	}

	wg.Wait()

	// Should not panic
	assert.Equal(0, len(component.KvMemClients))
}

// Edge case tests

func TestRemoteMemcachedComponent_EmptyKvVbMap(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_EmptyKvVbMap =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_EmptyKvVbMap =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Set empty KV map
	component.SetTargetKvVbMapGetter(func() (base.KvVBMapType, error) {
		return make(base.KvVBMapType), nil
	})

	err := component.InitConnections()

	assert.NoError(err)
	assert.Equal(0, len(component.KvMemClients))

	utils.AssertNotCalled(t, "GetRemoteMemcachedConnection")
}

func TestRemoteMemcachedComponent_LargeScaleConcurrency(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_LargeScaleConcurrency =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_LargeScaleConcurrency =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(50)
	defer close(finCh)

	utils.On("GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(createMockClient(), nil)

	var wg sync.WaitGroup
	numOperations := 1000
	successCount := 0
	var successMutex sync.Mutex

	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			server := testServerAddr
			if id%2 == 0 {
				server = testServerAddr2
			}

			client, err := component.AcquireClient(server)
			if err != nil {
				return
			}

			// Simulate varying work durations
			time.Sleep(time.Duration(id%10) * time.Microsecond)

			component.ReleaseClient(server, client)

			successMutex.Lock()
			successCount++
			successMutex.Unlock()
		}(i)
	}

	wg.Wait()

	assert.Equal(numOperations, successCount)

	// Verify pools don't exceed limits
	for _, clientChan := range component.KvMemClients {
		assert.LessOrEqual(len(clientChan), 50)
	}
}

func TestRemoteMemcachedComponent_ConcurrentMonitorTopologyAndClose(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_ConcurrentMonitorTopologyAndClose =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_ConcurrentMonitorTopologyAndClose =================")

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)

	// Add some clients
	mockClient1 := createMockClient()
	component.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.KvMemClients[testServerAddr] <- mockClient1
	component.knownServers[testServerAddr] = true

	var wg sync.WaitGroup

	// change the topology change check interval to 100ms
	originalTopologyChangeCheckInterval := base.TopologyChangeCheckInterval
	base.TopologyChangeCheckInterval = 100 * time.Millisecond
	defer func() {
		base.TopologyChangeCheckInterval = originalTopologyChangeCheckInterval
	}()

	// Start MonitorTopology
	wg.Add(1)
	go func() {
		defer wg.Done()
		component.MonitorTopology()
	}()

	// Give MonitorTopology time to start
	time.Sleep(199 * time.Millisecond)

	// Concurrently close connections and finish channel
	wg.Add(1)
	go func() {
		defer wg.Done()
		component.Close()
	}()

	// Close finish channel to stop MonitorTopology
	close(finCh)

	wg.Wait()

	// Should not panic or deadlock
}

func TestRemoteMemcachedComponent_MonitorTopology_RapidTopologyChanges(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_MonitorTopology_RapidTopologyChanges =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_MonitorTopology_RapidTopologyChanges =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)

	// Set up topology that changes rapidly
	var topologyVersion atomic.Int64
	component.SetTargetKvVbMapGetter(func() (base.KvVBMapType, error) {
		v := topologyVersion.Add(1)
		if v%2 == 0 {
			return createTestKvVbMap(), nil
		}
		return createTestKvVbMapThreeServers(), nil
	})

	// Add clients for both topologies
	mockClient1 := createMockClient()
	mockClient2 := createMockClient()
	mockClient3 := createMockClient()

	component.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.KvMemClients[testServerAddr3] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)

	// Also populate knownServers so diffTopology has the correct baseline
	component.knownServers[testServerAddr] = true
	component.knownServers[testServerAddr2] = true
	component.knownServers[testServerAddr3] = true

	component.KvMemClients[testServerAddr] <- mockClient1
	component.KvMemClients[testServerAddr2] <- mockClient2
	component.KvMemClients[testServerAddr3] <- mockClient3

	// change the topology change check interval to 100ms
	originalTopologyChangeCheckInterval := base.TopologyChangeCheckInterval
	base.TopologyChangeCheckInterval = 100 * time.Millisecond

	// Start monitoring
	monitorDone := make(chan struct{})
	go func() {
		component.MonitorTopology()
		close(monitorDone)
	}()

	// Wait for some topology checks
	time.Sleep(2 * time.Second)

	// ensure the callCount to TargetKvVbMapGetter is greater than 2
	assert.Greater(topologyVersion.Load(), int64(2))

	// Stop monitoring and restore global state
	close(finCh)
	<-monitorDone
	base.TopologyChangeCheckInterval = originalTopologyChangeCheckInterval

	// Should not panic despite rapid changes
}

func TestRemoteMemcachedComponent_DiffTopology_DetectsNewAndObsoleteServers(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_DiffTopology_DetectsNewAndObsoleteServers =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_DiffTopology_DetectsNewAndObsoleteServers =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Known servers are server1 and server2
	component.knownServers[testServerAddr] = true
	component.knownServers[testServerAddr2] = true
	component.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)

	// Topology now has server2 and server3 (server1 removed, server3 added)
	newKvVbMap := make(base.KvVBMapType)
	newKvVbMap[testServerAddr2] = []uint16{0, 1, 2, 3}
	newKvVbMap[testServerAddr3] = []uint16{4, 5, 6, 7}
	component.SetTargetKvVbMapGetter(func() (base.KvVBMapType, error) {
		return newKvVbMap, nil
	})

	serversToDelete, serversToAdd, err := component.diffTopology()
	assert.NoError(err)
	assert.Equal([]string{testServerAddr}, serversToDelete)
	assert.Equal([]string{testServerAddr3}, serversToAdd)
}

func TestRemoteMemcachedComponent_DiffTopology_NoChanges(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_DiffTopology_NoChanges =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_DiffTopology_NoChanges =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Known servers are server1 and server2
	component.knownServers[testServerAddr] = true
	component.knownServers[testServerAddr2] = true
	component.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)

	// Topology still has server1 and server2
	serversToDelete, serversToAdd, err := component.diffTopology()
	assert.NoError(err)
	assert.Empty(serversToDelete)
	assert.Empty(serversToAdd)
}

func TestRemoteMemcachedComponent_DiffTopology_ErrorFetchingKvVbMap(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_DiffTopology_ErrorFetchingKvVbMap =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_DiffTopology_ErrorFetchingKvVbMap =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	component.SetTargetKvVbMapGetter(func() (base.KvVBMapType, error) {
		return nil, errors.New("topology fetch error")
	})

	serversToDelete, serversToAdd, err := component.diffTopology()
	assert.Error(err)
	assert.Nil(serversToDelete)
	assert.Nil(serversToAdd)
}

func TestRemoteMemcachedComponent_AddServers(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_AddServers =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_AddServers =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Start with server1 only
	component.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.knownServers[testServerAddr] = true

	// Add server2 and server3
	component.poolConfigMtx.Lock()
	component.addServers([]string{testServerAddr2, testServerAddr3})
	component.poolConfigMtx.Unlock()

	// addServers only updates knownServers; KvMemClients channels are created
	// lazily on-demand by AcquireClient, so KvMemClients should be unchanged.
	assert.Len(component.KvMemClients, 1, "KvMemClients should still only contain server1")

	// Verify knownServers tracks all 3 servers
	assert.True(component.knownServers[testServerAddr])
	assert.True(component.knownServers[testServerAddr2])
	assert.True(component.knownServers[testServerAddr3])
}

func TestRemoteMemcachedComponent_AddServers_SkipsDuplicates(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_AddServers_SkipsDuplicates =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_AddServers_SkipsDuplicates =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Start with server1 already in the pool with a client
	mockClient := createMockClient()
	component.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.KvMemClients[testServerAddr] <- mockClient
	component.knownServers[testServerAddr] = true

	// Try to add server1 again
	component.poolConfigMtx.Lock()
	component.addServers([]string{testServerAddr})
	component.poolConfigMtx.Unlock()

	// Should not overwrite the existing channel - the client should still be there
	assert.Len(component.KvMemClients, 1)
	assert.Equal(1, len(component.KvMemClients[testServerAddr]))
}

func TestRemoteMemcachedComponent_MonitorTopology_AddsNewServers(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_MonitorTopology_AddsNewServers =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_MonitorTopology_AddsNewServers =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)

	// Start with only server1 and server2
	component.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.knownServers[testServerAddr] = true
	component.knownServers[testServerAddr2] = true

	// Topology returns 3 servers (server3 is new)
	kvVbMapThree := createTestKvVbMapThreeServers()
	component.SetTargetKvVbMapGetter(func() (base.KvVBMapType, error) {
		return kvVbMapThree, nil
	})

	// Adjust the topology change check interval to 100ms
	originalTopologyChangeCheckInterval := base.TopologyChangeCheckInterval
	base.TopologyChangeCheckInterval = 100 * time.Millisecond

	// Start monitoring
	monitorDone := make(chan struct{})
	go func() {
		component.MonitorTopology()
		close(monitorDone)
	}()

	// Wait for at least one topology check
	time.Sleep(300 * time.Millisecond)

	// Stop MonitorTopology before reading shared state
	close(finCh)
	<-monitorDone
	base.TopologyChangeCheckInterval = originalTopologyChangeCheckInterval

	// Server3 should have been added to knownServers.
	// KvMemClients channels are created lazily by AcquireClient.
	assert.True(component.knownServers[testServerAddr3], "New server3 should be tracked in knownServers")
}

func TestRemoteMemcachedComponent_MonitorTopology_AddsNewServersWithSSL(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_MonitorTopology_AddsNewServersWithSSL =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_MonitorTopology_AddsNewServersWithSSL =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)

	// Create SSL-enabled ref
	sslRef, _ := metadata.NewRemoteClusterReference(testClusterUuid, "testCluster", "localhost:18091", "admin", "password", "", true, "", nil, nil, nil, nil)
	sslRef.SetEncryptionType(metadata.EncryptionType_Full)
	component.SetRefGetter(func() *metadata.RemoteClusterReference {
		return sslRef
	})

	// Mock SSL port map for all 3 servers
	sslPortMap := base.SSLPortMap{
		testServerAddr:  11207,
		testServerAddr2: 11208,
		testServerAddr3: 11209,
	}
	utils.On("GetMemcachedSSLPortMap", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(sslPortMap, nil)

	// Mock ExponentialBackoffExecutor to call the actual InitSSLConStrMap function
	utils.On("ExponentialBackoffExecutor", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			// Call InitSSLConStrMap to populate SslConStrMap
			component.InitSSLConStrMap()
		}).
		Return(nil)

	// Start with only server1 and server2
	component.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.knownServers[testServerAddr] = true
	component.knownServers[testServerAddr2] = true

	// Topology returns 3 servers (server3 is new)
	kvVbMapThree := createTestKvVbMapThreeServers()
	component.SetTargetKvVbMapGetter(func() (base.KvVBMapType, error) {
		return kvVbMapThree, nil
	})

	// Adjust the topology change check interval to 100ms
	originalTopologyChangeCheckInterval := base.TopologyChangeCheckInterval
	base.TopologyChangeCheckInterval = 100 * time.Millisecond

	// Start monitoring
	monitorDone := make(chan struct{})
	go func() {
		component.MonitorTopology()
		close(monitorDone)
	}()

	// Wait for at least one topology check
	time.Sleep(300 * time.Millisecond)

	// Stop MonitorTopology before reading shared state
	close(finCh)
	<-monitorDone
	base.TopologyChangeCheckInterval = originalTopologyChangeCheckInterval

	// Server3 should have been added to knownServers (channels are created lazily)
	knownServer3 := component.knownServers[testServerAddr3]
	assert.True(knownServer3, "New server3 should be tracked in knownServers")

	// SSL map should have been refreshed and include server3
	assert.NotNil(component.SslConStrMap)
	assert.Equal(testServerAddr3SslConnStr, component.SslConStrMap[testServerAddr3])

	// Verify GetMemcachedSSLPortMap was called (SSL map refresh triggered)
	utils.AssertCalled(t, "GetMemcachedSSLPortMap", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// TestRemoteMemcachedComponent_MonitorTopology_SSLRefreshFailsThenSucceeds verifies that when
// InitSSLConStrMap fails the exponential retry on the first MonitorTopology cycle, the new servers
// are NOT added to knownServers. On the next cycle, the SSL refresh succeeds and the servers are added.
func TestRemoteMemcachedComponent_MonitorTopology_SSLRefreshFailsThenSucceeds(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_MonitorTopology_SSLRefreshFailsThenSucceeds =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_MonitorTopology_SSLRefreshFailsThenSucceeds =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)

	// Create SSL-enabled ref
	sslRef, _ := metadata.NewRemoteClusterReference(testClusterUuid, "testCluster", "localhost:18091", "admin", "password", "", true, "", nil, nil, nil, nil)
	sslRef.SetEncryptionType(metadata.EncryptionType_Full)
	component.SetRefGetter(func() *metadata.RemoteClusterReference {
		return sslRef
	})

	// Mock SSL port map for all 3 servers (needed when InitSSLConStrMap actually executes)
	sslPortMap := base.SSLPortMap{
		testServerAddr:  11207,
		testServerAddr2: 11208,
		testServerAddr3: 11209,
	}
	utils.On("GetMemcachedSSLPortMap", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(sslPortMap, nil)

	// First call to ExponentialBackoffExecutor: fail (simulates all retries exhausted)
	utils.On("ExponentialBackoffExecutor", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("SSL refresh failed after retries")).Once()

	// Second call to ExponentialBackoffExecutor: succeed and actually run InitSSLConStrMap
	utils.On("ExponentialBackoffExecutor", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			component.InitSSLConStrMap()
		}).
		Return(nil)

	// Start with only server1 and server2
	component.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.knownServers[testServerAddr] = true
	component.knownServers[testServerAddr2] = true

	// Topology returns 3 servers (server3 is new)
	kvVbMapThree := createTestKvVbMapThreeServers()
	component.SetTargetKvVbMapGetter(func() (base.KvVBMapType, error) {
		return kvVbMapThree, nil
	})

	// Adjust the topology change check interval to 100ms
	originalTopologyChangeCheckInterval := base.TopologyChangeCheckInterval
	base.TopologyChangeCheckInterval = 100 * time.Millisecond

	// Start monitoring
	monitorDone := make(chan struct{})
	go func() {
		component.MonitorTopology()
		close(monitorDone)
	}()

	// After first cycle (~100ms): SSL refresh failed, server3 should NOT be added
	time.Sleep(150 * time.Millisecond)

	component.poolConfigMtx.RLock()
	knownAfterFirstCycle := component.knownServers[testServerAddr3]
	component.poolConfigMtx.RUnlock()

	assert.False(knownAfterFirstCycle, "Server3 should NOT be in knownServers after failed SSL refresh")

	// After second cycle (~200ms): SSL refresh succeeds, server3 should be added
	time.Sleep(200 * time.Millisecond)

	// Stop MonitorTopology before reading SslConStrMap (which is unprotected)
	close(finCh)
	<-monitorDone
	base.TopologyChangeCheckInterval = originalTopologyChangeCheckInterval

	// Now safe to read shared state without locks
	knownAfterSecondCycle := component.knownServers[testServerAddr3]
	assert.True(knownAfterSecondCycle, "Server3 should be in knownServers after successful SSL refresh")

	// SSL map should have been refreshed and include server3
	assert.NotNil(component.SslConStrMap)
	assert.Equal(testServerAddr3SslConnStr, component.SslConStrMap[testServerAddr3])
}

// TestRemoteMemcachedComponent_MonitorTopology_SSLRefreshPersistentFailure verifies that when
// InitSSLConStrMap persistently fails exponential retry across multiple MonitorTopology cycles,
// the new servers are never added to knownServers or KvMemClients.
func TestRemoteMemcachedComponent_MonitorTopology_SSLRefreshPersistentFailure(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_MonitorTopology_SSLRefreshPersistentFailure =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_MonitorTopology_SSLRefreshPersistentFailure =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)

	// Create SSL-enabled ref
	sslRef, _ := metadata.NewRemoteClusterReference(testClusterUuid, "testCluster", "localhost:18091", "admin", "password", "", true, "", nil, nil, nil, nil)
	sslRef.SetEncryptionType(metadata.EncryptionType_Full)
	component.SetRefGetter(func() *metadata.RemoteClusterReference {
		return sslRef
	})

	// Track how many times ExponentialBackoffExecutor is called
	var backoffCallCount int
	var backoffCallCountMtx sync.Mutex

	// All calls to ExponentialBackoffExecutor fail (SSL consistently unavailable)
	utils.On("ExponentialBackoffExecutor", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			backoffCallCountMtx.Lock()
			backoffCallCount++
			backoffCallCountMtx.Unlock()
		}).
		Return(errors.New("SSL refresh failed after retries"))

	// Start with only server1 and server2
	component.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.knownServers[testServerAddr] = true
	component.knownServers[testServerAddr2] = true

	// Topology returns 3 servers (server3 is new)
	kvVbMapThree := createTestKvVbMapThreeServers()
	component.SetTargetKvVbMapGetter(func() (base.KvVBMapType, error) {
		return kvVbMapThree, nil
	})

	// Adjust the topology change check interval to 100ms
	originalTopologyChangeCheckInterval := base.TopologyChangeCheckInterval
	base.TopologyChangeCheckInterval = 100 * time.Millisecond

	// Start monitoring
	monitorDone := make(chan struct{})
	go func() {
		component.MonitorTopology()
		close(monitorDone)
	}()

	// Wait for multiple topology cycles
	time.Sleep(500 * time.Millisecond)

	// Stop MonitorTopology before reading shared state
	close(finCh)
	<-monitorDone
	base.TopologyChangeCheckInterval = originalTopologyChangeCheckInterval

	// Server3 should NEVER be added since SSL refresh always fails
	_, existsServer3 := component.KvMemClients[testServerAddr3]
	knownServer3 := component.knownServers[testServerAddr3]
	totalServers := len(component.KvMemClients)

	assert.False(existsServer3, "Server3 should NOT be in KvMemClients when SSL refresh persistently fails")
	assert.False(knownServer3, "Server3 should NOT be in knownServers when SSL refresh persistently fails")
	assert.Equal(2, totalServers, "Should still have only the original 2 servers")

	// Verify that multiple retry attempts were made
	backoffCallCountMtx.Lock()
	callCount := backoffCallCount
	backoffCallCountMtx.Unlock()
	assert.Greater(callCount, 1, "ExponentialBackoffExecutor should have been called multiple times across topology cycles")

	// SslConStrMap should be nil (never populated)
	assert.Nil(component.SslConStrMap)
}

// TestRemoteMemcachedComponent_GetNewMemcachedClient_SSL_MissingSslConStr verifies that
// GetNewMemcachedClient returns ErrSslConStrNotFound when the SSL connection string for
// a server has not been populated yet (e.g., the server is newly added and MonitorTopology
// hasn't refreshed the SSL map yet).
func TestRemoteMemcachedComponent_GetNewMemcachedClient_SSL_MissingSslConStr(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_GetNewMemcachedClient_SSL_MissingSslConStr =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_GetNewMemcachedClient_SSL_MissingSslConStr =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Create SSL-enabled ref
	sslRef, _ := metadata.NewRemoteClusterReference(testClusterUuid, "testCluster", "localhost:18091", "admin", "password", "", true, "", nil, nil, nil, nil)
	sslRef.SetEncryptionType(metadata.EncryptionType_Full)
	component.SetRefGetter(func() *metadata.RemoteClusterReference {
		return sslRef
	})

	// Populate SslConStrMap with only server1, missing server2
	component.SslConStrMap = map[string]string{
		testServerAddr: testServerAddr1SslConnStr,
	}

	// Trying to create a client for server2 should fail with ErrSslConStrNotFound
	client, err := component.GetNewMemcachedClient(testServerAddr2)

	assert.Nil(client)
	assert.Error(err)
	assert.True(errors.Is(err, ErrSslConStrNotFound), "Error should wrap ErrSslConStrNotFound")
	assert.Contains(err.Error(), testServerAddr2, "Error message should contain the server address")
}

// TestRemoteMemcachedComponent_MonitorTopology_SSLRefreshFailsThenSucceeds_WithAcquireClient
// is an end-to-end test that verifies: when a new server is added to the topology with SSL,
// the SSL refresh initially fails, AcquireClient for the new server returns ErrSslConStrNotFound
// (which callers can retry), and once MonitorTopology successfully refreshes the SSL map on a
// subsequent cycle, AcquireClient succeeds.
func TestRemoteMemcachedComponent_MonitorTopology_SSLRefreshFailsThenSucceeds_WithAcquireClient(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_MonitorTopology_SSLRefreshFailsThenSucceeds_WithAcquireClient =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_MonitorTopology_SSLRefreshFailsThenSucceeds_WithAcquireClient =================")
	assert := assert.New(t)

	component, utils, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)

	// Create SSL-enabled ref
	sslRef, _ := metadata.NewRemoteClusterReference(testClusterUuid, "testCluster", "localhost:18091", "admin", "password", "", true, "", nil, nil, nil, nil)
	sslRef.SetEncryptionType(metadata.EncryptionType_Full)
	component.SetRefGetter(func() *metadata.RemoteClusterReference {
		return sslRef
	})

	// Mock SSL port map for all 3 servers
	sslPortMap := base.SSLPortMap{
		testServerAddr:  11207,
		testServerAddr2: 11208,
		testServerAddr3: 11209,
	}
	utils.On("GetMemcachedSSLPortMap", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(sslPortMap, nil)

	mockClient := createMockClient()
	// Mock TLS connection creation for when AcquireClient eventually succeeds
	utils.On("ExponentialBackoffExecutorWithFinishSignal", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockClient, nil)

	// First call to ExponentialBackoffExecutor (MonitorTopology SSL refresh): fail
	utils.On("ExponentialBackoffExecutor", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("SSL refresh failed after retries")).Once()

	// Second call to ExponentialBackoffExecutor (MonitorTopology SSL refresh): succeed
	utils.On("ExponentialBackoffExecutor", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			component.InitSSLConStrMap()
		}).
		Return(nil)

	// Start with server1 and server2, with SSL map already populated for them
	component.SslConStrMap = map[string]string{
		testServerAddr:  testServerAddr1SslConnStr,
		testServerAddr2: testServerAddr2SslConnStr,
	}
	component.KvMemClients[testServerAddr] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.KvMemClients[testServerAddr2] = make(chan mcc.ClientIface, testMaxConnectionsPerServer)
	component.knownServers[testServerAddr] = true
	component.knownServers[testServerAddr2] = true

	// Topology returns 3 servers (server3 is new)
	kvVbMapThree := createTestKvVbMapThreeServers()
	component.SetTargetKvVbMapGetter(func() (base.KvVBMapType, error) {
		return kvVbMapThree, nil
	})

	// Adjust the topology change check interval to 100ms
	originalTopologyChangeCheckInterval := base.TopologyChangeCheckInterval
	base.TopologyChangeCheckInterval = 100 * time.Millisecond

	// Start monitoring
	monitorDone := make(chan struct{})
	go func() {
		component.MonitorTopology()
		close(monitorDone)
	}()

	// After first cycle: SSL refresh failed, server3 not yet in the pool.
	// AcquireClient for server3 should fail with ErrSslConStrNotFound
	// (Because the channel is not yet created and createClientChannel will be called,
	// but GetNewMemcachedClient will fail because SslConStrMap doesn't have server3)
	time.Sleep(150 * time.Millisecond)

	client, err := component.AcquireClient(testServerAddr3)
	assert.Nil(client, "AcquireClient for server3 should fail before SSL map is refreshed")
	assert.Error(err)
	assert.True(errors.Is(err, ErrSslConStrNotFound),
		"AcquireClient should return ErrSslConStrNotFound for server3 before SSL refresh")

	// After second cycle: SSL refresh succeeds, server3 added with SSL map populated.
	// AcquireClient for server3 should now succeed.
	time.Sleep(200 * time.Millisecond)

	client, err = component.AcquireClient(testServerAddr3)
	assert.NotNil(client, "AcquireClient for server3 should succeed after SSL map is refreshed")
	assert.NoError(err)

	// Clean up
	component.ReleaseClient(testServerAddr3, client)
	close(finCh)
	<-monitorDone
	base.TopologyChangeCheckInterval = originalTopologyChangeCheckInterval
}

func TestRemoteMemcachedComponent_InitSSLConStrMap_AlternateAddressError(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_InitSSLConStrMap_AlternateAddressError =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_InitSSLConStrMap_AlternateAddressError =================")
	assert := assert.New(t)

	component, _, finCh := setupBasicRemoteMemcachedComponent(testMaxConnectionsPerServer)
	defer close(finCh)

	// Create SSL-enabled ref
	sslRef, _ := metadata.NewRemoteClusterReference(testClusterUuid, "testCluster", "localhost:18091", "admin", "password", "", true, "", nil, nil, nil, nil)
	sslRef.SetEncryptionType(metadata.EncryptionType_Full)

	component.SetRefGetter(func() *metadata.RemoteClusterReference {
		return sslRef
	})

	// Set alternate address checker to return error
	component.SetAlternateAddressChecker(func(ref *metadata.RemoteClusterReference) (bool, error) {
		return false, errors.New("alternate address check failed")
	})

	err := component.InitSSLConStrMap()

	assert.Error(err)
	assert.Contains(err.Error(), "alternate address check failed")
}

func TestRemoteMemcachedComponent_RapidAcquireReleaseWithReconfiguration(t *testing.T) {
	fmt.Println("============== Test case start: TestRemoteMemcachedComponent_RapidAcquireReleaseWithReconfiguration =================")
	defer fmt.Println("============== Test case end: TestRemoteMemcachedComponent_RapidAcquireReleaseWithReconfiguration =================")
	assert := assert.New(t)

	// Test parameters
	const (
		numServers           = 3
		initialMaxConns      = 10
		numAcquireReleaseGRs = 100 // Goroutines doing acquire/release
		numReconfigureGRs    = 20  // Goroutines doing reconfigurations
		operationDuration    = 2 * time.Second
	)

	// Setup component with initial configuration
	component, utils, finCh := setupBasicRemoteMemcachedComponent(initialMaxConns)
	defer close(finCh)

	// Mock client creation to return unique mock clients
	var mockClientCounter int
	var mockClientMtx sync.Mutex
	utils.On("GetRemoteMemcachedConnection", mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(func(string, string, string, string, string, bool, time.Duration, *log.CommonLogger) mcc.ClientIface {
			mockClientMtx.Lock()
			defer mockClientMtx.Unlock()
			mockClientCounter++
			return createMockClient()
		}, nil)

	// Initialize server addresses for the test
	serverAddresses := []string{testServerAddr, testServerAddr2, testServerAddr3}

	// Pre-populate each server with some initial clients to make the test more realistic
	for _, serverAddr := range serverAddresses {
		clientChan := make(chan mcc.ClientIface, initialMaxConns)
		for i := 0; i < 3; i++ {
			clientChan <- createMockClient()
		}
		component.KvMemClients[serverAddr] = clientChan
	}

	// Metrics for validation
	var (
		totalAcquireAttempts  int64
		totalAcquireSuccesses int64
		totalAcquireFailures  int64
		totalReleaseOps       int64
		totalReconfigures     int64

		metricsLock sync.Mutex
	)

	// Channel to signal all goroutines to stop
	stopChan := make(chan struct{})
	var wg sync.WaitGroup

	// Start goroutines that rapidly acquire and release clients
	for i := 0; i < numAcquireReleaseGRs; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			localAcquireAttempts := int64(0)
			localAcquireSuccesses := int64(0)
			localAcquireFailures := int64(0)
			localReleaseOps := int64(0)

			for {
				select {
				case <-stopChan:
					// Update global metrics before exiting
					metricsLock.Lock()
					totalAcquireAttempts += localAcquireAttempts
					totalAcquireSuccesses += localAcquireSuccesses
					totalAcquireFailures += localAcquireFailures
					totalReleaseOps += localReleaseOps
					metricsLock.Unlock()
					return
				default:
					// Pick a random server
					serverAddr := serverAddresses[goroutineID%numServers]

					// Acquire a client
					localAcquireAttempts++
					client, err := component.AcquireClient(serverAddr)
					if err != nil {
						localAcquireFailures++
						continue
					}
					localAcquireSuccesses++

					// Simulate some work with the client
					time.Sleep(time.Microsecond * time.Duration(goroutineID%10))

					// Release the client
					component.ReleaseClient(serverAddr, client)
					localReleaseOps++
				}
			}
		}(i)
	}

	// Start goroutines that rapidly reconfigure the pool with varying sizes
	for i := 0; i < numReconfigureGRs; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			localReconfigures := int64(0)

			for {
				select {
				case <-stopChan:
					// Update global metrics before exiting
					metricsLock.Lock()
					totalReconfigures += localReconfigures
					metricsLock.Unlock()
					return
				default:
					// Reconfigure with different pool sizes (between 5 and 20)
					newMaxConns := 5 + (goroutineID % 16)
					component.SetMaxConnectionsPerServer(newMaxConns)
					localReconfigures++

					// Small delay between reconfigurations
					time.Sleep(time.Millisecond * time.Duration(5+goroutineID%10))
				}
			}
		}(i)
	}

	// Let the test run for the specified duration
	time.Sleep(operationDuration)

	// Signal all goroutines to stop
	close(stopChan)

	// Wait for all goroutines to complete
	wg.Wait()

	// Validation: Verify no panics occurred (implicit - test reached this point)
	fmt.Printf("Test completed without panics\n")

	// Validation: Verify operations actually happened
	assert.Greater(totalAcquireAttempts, int64(0))
	assert.Greater(totalAcquireSuccesses, int64(0))
	assert.Greater(totalReleaseOps, int64(0))
	assert.Greater(totalReconfigures, int64(0))

	releaseDelta := totalAcquireSuccesses - totalReleaseOps
	assert.LessOrEqual(releaseDelta, int64(numAcquireReleaseGRs),
		"Release count should be close to acquire count (delta should not exceed number of goroutines)")

	// Validation: Verify pool sizes respect current limits
	component.KvMemClientsMtx.RLock()
	currentMaxConns := component.MaxConnsPerServer
	for serverAddr, clientChan := range component.KvMemClients {
		poolSize := len(clientChan)
		poolCapacity := cap(clientChan)

		assert.LessOrEqual(poolSize, currentMaxConns,
			"Pool size for %s should not exceed max connections", serverAddr)
		assert.Equal(currentMaxConns, poolCapacity,
			"Pool capacity for %s should match current max connections", serverAddr)
	}
	component.KvMemClientsMtx.RUnlock()

	// Validation: Verify all servers are still present
	assert.Len(component.KvMemClients, numServers, "Should still have all server pools")

	// Cleanup verification: Close all connections and verify cleanup
	component.Close()
	assert.Len(component.KvMemClients, 0, "All pools should be cleaned up")
}
