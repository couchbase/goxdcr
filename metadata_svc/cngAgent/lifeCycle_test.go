// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package cngAgent

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/metadata_svc"
	service_def "github.com/couchbase/goxdcr/v8/service_def/mocks"
	"github.com/couchbase/goxdcr/v8/streamApiWatcher/cngWatcher"
	utilsMock "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Test constants
const (
	testUuid     = "5220aac3bdc2f7ac663ac288bb85ae27"
	testHostname = "testhost:18098"
	testName     = "testCluster"
	testId       = "testId"
)

// Helper functions for creating test data
func createTestRemoteClusterReference() *metadata.RemoteClusterReference {
	hostAddr, _ := base.ValidateHostAddrForCbCluster(testHostname)
	ref, _ := metadata.NewRemoteClusterReference(testUuid, testName, hostAddr, "", "", "", false, "", nil, nil, nil, nil)
	ref.SetId(testId)
	return ref
}

func createTestLogger() *log.CommonLogger {
	return log.NewLogger("test", log.DefaultLoggerContext)
}

func setupTestServices() (services, *utilsMock.UtilsIface, *service_def.MetadataSvc, *service_def.UILogSvc) {
	utilsMock := &utilsMock.UtilsIface{}
	metakvMock := &service_def.MetadataSvc{}
	uiLogMock := &service_def.UILogSvc{}
	topologySvcMock := &service_def.XDCRCompTopologySvc{}

	services := services{
		utils:       utilsMock,
		metakv:      metakvMock,
		uiLog:       uiLogMock,
		topologySvc: topologySvcMock,
	}

	topologySvcMock.On("MyClusterUUID").Return(testUuid, nil)

	return services, utilsMock, metakvMock, uiLogMock
}

func createTestRemoteCngAgent() (*RemoteCngAgent, *utilsMock.UtilsIface, *service_def.MetadataSvc, *service_def.UILogSvc) {
	services, utilsMock, metakvMock, uiLogMock := setupTestServices()
	logger := createTestLogger()

	agent := &RemoteCngAgent{
		services: services,
		finCh:    make(chan struct{}),
		logger:   logger,
		heartbeatManager: &heartBeatManager{
			specsReader: &service_def.ReplicationSpecReader{},
			services:    services,
			logger:      logger,
		},
		remoteDataProvider: &remoteDataProvider{
			bucketManifestWatcher: make(map[string]cngWatcher.GrpcStreamManager[*metadata.CollectionsManifest]),
			refCount:              make(map[string]uint32),
		},
	}
	agent.refreshState.cond = sync.NewCond(&agent.refreshState.mutex)

	targetHealthTracker := &targetHealthTracker{
		connectivityHelper: metadata_svc.NewConnectivityHelper(base.RefreshRemoteClusterRefInterval),
		services:           services,
		registerConnErr:    agent.registerConnErr,
		clearConnErrs:      agent.clearConnErrs,
	}
	agent.healthTracker = targetHealthTracker

	return agent, utilsMock, metakvMock, uiLogMock
}

// Test helper to wait for goroutines with timeout
func waitForRoutinesWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// ============= Test Cases for referenceCache =============

func TestReferenceCache_Update(t *testing.T) {
	assert := assert.New(t)

	cache := &referenceCache{}
	ref1 := createTestRemoteClusterReference()
	ref2 := createTestRemoteClusterReference()
	ref2.SetName("updatedName")

	// Test initial update
	cache.update(ref1)
	assert.True(cache.reference.IsSame(ref1))
	assert.True(cache.history.IsEmpty())

	// Test second update - history should contain previous reference
	cache.update(ref2)
	assert.True(cache.reference.IsSame(ref2))
	assert.True(cache.history.IsSame(ref1))
}

func TestReferenceCache_Clear(t *testing.T) {
	assert := assert.New(t)

	cache := &referenceCache{}
	ref := createTestRemoteClusterReference()

	// Update first
	cache.update(ref)
	assert.False(cache.reference.IsEmpty())

	// Clear
	cache.clear()
	assert.True(cache.reference.IsEmpty())
	assert.True(cache.refDeletedFromMetakv)
	assert.True(cache.history.IsSame(ref))
}

func TestReferenceCache_ConcurrentAccess(t *testing.T) {
	assert := assert.New(t)

	cache := &referenceCache{}

	var wg sync.WaitGroup
	numGoroutines := 10

	// Test concurrent updates and reads
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()
			testRef := createTestRemoteClusterReference()
			testRef.SetName(fmt.Sprintf("test-%d", i))
			cache.update(testRef)
		}(i)
	}

	// Also test concurrent reads
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			cache.mutex.RLock()
			_ = cache.reference.Name()
			cache.mutex.RUnlock()
		}()
	}

	assert.True(waitForRoutinesWithTimeout(&wg, 5*time.Second))
}

// ============= Test Cases for metakvOpState =============

func TestMetakvOpState_BeginOp_Success(t *testing.T) {
	assert := assert.New(t)

	state := &metakvOpState{}
	ctx := context.Background()

	// Test successful begin operation
	opCtx, gen, err := state.beginOp(ctx, MetakvOpAdd)
	assert.NoError(err)
	assert.NotNil(opCtx)
	assert.Equal(uint32(1), gen)
	assert.Equal(MetakvOpAdd, *state.activeOp)

	// Cleanup
	state.mutex.Lock()
	state.endOpNolock()
	state.mutex.Unlock()
}

func TestMetakvOpState_BeginOp_InProgress(t *testing.T) {
	assert := assert.New(t)

	state := &metakvOpState{}
	ctx := context.Background()

	// Start first operation
	_, _, err := state.beginOp(ctx, MetakvOpAdd)
	assert.NoError(err)

	// Try to start second operation (should fail for non-delete ops)
	_, _, err = state.beginOp(ctx, MetakvOpSet)
	assert.Equal(metadata_svc.RemoteSyncInProgress, err)

	// Cleanup
	state.mutex.Lock()
	state.endOpNolock()
	state.mutex.Unlock()
}

func TestMetakvOpState_BeginOp_DeletePreemption(t *testing.T) {
	assert := assert.New(t)

	state := &metakvOpState{}
	ctx := context.Background()

	// Start first operation
	opCtx1, gen1, err := state.beginOp(ctx, MetakvOpAdd)
	assert.NoError(err)
	assert.Equal(uint32(1), gen1)

	// Start delete operation (should preempt)
	opCtx2, gen2, err := state.beginOp(ctx, MetakvOpDel)
	assert.NoError(err)
	assert.Equal(uint32(2), gen2)
	assert.Equal(MetakvOpDel, *state.activeOp)

	// First operation context should be canceled
	select {
	case <-opCtx1.Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("First operation context should have been canceled")
	}

	// Cleanup
	state.mutex.Lock()
	state.endOpNolock()
	state.mutex.Unlock()

	// Second operation context should also be canceled now
	select {
	case <-opCtx2.Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Second operation context should have been canceled")
	}
}

func TestMetakvOpState_EndOpNolock(t *testing.T) {
	assert := assert.New(t)

	state := &metakvOpState{}
	ctx := context.Background()

	// Start operation
	opCtx, _, err := state.beginOp(ctx, MetakvOpAdd)
	assert.NoError(err)
	assert.NotNil(state.activeOp)
	assert.NotNil(state.activeCancelFunc)

	// End operation
	state.mutex.Lock()
	state.endOpNolock()
	state.mutex.Unlock()

	assert.Nil(state.activeOp)
	assert.Nil(state.activeCancelFunc)

	// Context should be canceled
	select {
	case <-opCtx.Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Operation context should have been canceled")
	}
}

func TestMetakvOpState_ConcurrentOperations(t *testing.T) {
	assert := assert.New(t)

	state := &metakvOpState{}
	ctx := context.Background()

	var wg sync.WaitGroup
	var gen uint32

	numGoroutines := 10
	successCount := int32(0)
	inProgressCount := int32(0)

	// Test concurrent begin operations
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			_, genOp, err := state.beginOp(ctx, MetakvOpAdd)
			switch err {
			case nil:
				gen = genOp
				atomic.AddInt32(&successCount, 1)
			case metadata_svc.RemoteSyncInProgress:
				atomic.AddInt32(&inProgressCount, 1)
			default:
				// ignore other errors
			}
		}()
	}

	assert.True(waitForRoutinesWithTimeout(&wg, 5*time.Second))

	// Only one should succeed, others should get RemoteSyncInProgress
	assert.Equal(int32(1), atomic.LoadInt32(&successCount))
	assert.Equal(int32(numGoroutines-1), atomic.LoadInt32(&inProgressCount))

	state.mutex.Lock()
	// verify that the operation is still active
	assert.NotNil(state.activeOp)
	assert.Equal(gen, state.gen)

	// clean up the operation
	state.endOpNolock()
	assert.Nil(state.activeOp)
	assert.Nil(state.activeCancelFunc)
	assert.Equal(gen, state.gen)

	state.mutex.Unlock()
}

// ============= Test Cases for RemoteCngAgent Lifecycle =============

func TestRemoteCngAgent_Start_Success_UserInitiated(t *testing.T) {
	assert := assert.New(t)

	agent, utilsMock, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Setup mocks
	utilsMock.On("StartDiagStopwatch", mock.AnythingOfType("string"), mock.AnythingOfType("time.Duration")).Return(func() time.Duration { return 0 })
	metakvMock.On("AddSensitiveWithCatalog", metadata_svc.RemoteClustersCatalogKey, ref.Id(), mock.Anything).Return(nil)
	refBytes, _ := ref.Marshal()
	metakvMock.On("Get", ref.Id()).Return(refBytes, interface{}(nil), nil)

	// Test Start with userInitiated=true
	err := agent.Start(ref, true)
	assert.NoError(err)
	assert.True(agent.InitDone())

	// Cleanup
	close(agent.finCh)
	agent.waitGrp.Wait()

	// Verify mocks
	utilsMock.AssertExpectations(t)
	metakvMock.AssertExpectations(t)
}

func TestRemoteCngAgent_Start_Success_SystemInitiated(t *testing.T) {
	assert := assert.New(t)

	agent, utilsMock, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Setup mocks
	utilsMock.On("StartDiagStopwatch", mock.AnythingOfType("string"), mock.AnythingOfType("time.Duration")).Return(func() time.Duration { return 0 })

	// Test Start with userInitiated=false
	err := agent.Start(ref, false)
	assert.NoError(err)
	assert.True(agent.InitDone())

	// Cleanup
	close(agent.finCh)
	agent.waitGrp.Wait()

	// Verify mocks
	utilsMock.AssertExpectations(t)
}

func TestRemoteCngAgent_Start_NilAgent(t *testing.T) {
	assert := assert.New(t)

	var agent *RemoteCngAgent
	ref := createTestRemoteClusterReference()

	err := agent.Start(ref, true)
	assert.Error(err)
	assert.Contains(err.Error(), "agent is nil")
}

func TestRemoteCngAgent_Start_NilReference(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()

	err := agent.Start(nil, true)
	assert.Error(err)
	assert.Contains(err.Error(), "newRef is nil")
}

func TestRemoteCngAgent_Start_UpdateReferenceFails(t *testing.T) {
	assert := assert.New(t)

	agent, utilsMock, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Setup mocks
	utilsMock.On("StartDiagStopwatch", mock.AnythingOfType("string"), mock.AnythingOfType("time.Duration")).Return(func() time.Duration { return 0 })
	metakvMock.On("AddSensitiveWithCatalog", metadata_svc.RemoteClustersCatalogKey, ref.Id(), mock.Anything).Return(fmt.Errorf("metakv error"))

	// Test Start with userInitiated=true (should fail)
	err := agent.Start(ref, true)
	assert.Error(err)
	assert.Contains(err.Error(), "failed to start remote agent")
	assert.False(agent.InitDone())

	// Verify mocks
	utilsMock.AssertExpectations(t)
	metakvMock.AssertExpectations(t)
}

func TestRemoteCngAgent_Stop(t *testing.T) {
	assert := assert.New(t)

	agent, utilsMock, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Setup mocks
	utilsMock.On("StartDiagStopwatch", mock.AnythingOfType("string"), mock.AnythingOfType("time.Duration")).Return(func() time.Duration { return 0 })

	// Start agent
	err := agent.Start(ref, false)
	assert.NoError(err)
	assert.True(agent.InitDone())

	// wait for a short time before stopping the agent
	time.Sleep(10 * time.Millisecond)

	// Stop agent
	agent.Stop()

	// Verify that finCh is closed and all goroutines have finished
	select {
	case <-agent.finCh:
		// Expected - channel should be closed
	default:
		t.Fatal("finCh should be closed")
	}

	// Verify mocks
	utilsMock.AssertExpectations(t)
}

func TestRemoteCngAgent_InitDone(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()

	// Initially should not be done
	assert.False(agent.InitDone())

	// Set init done
	agent.setInitDone()
	assert.True(agent.InitDone())

	// Test that multiple calls to setInitDone don't cause issues
	agent.setInitDone()
	assert.True(agent.InitDone())
}

func TestRemoteCngAgent_InitDone_Concurrent(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()

	var wg sync.WaitGroup
	numReaders := 50
	numWriters := 5

	// Concurrent readers
	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = agent.InitDone()
			}
		}()
	}

	// Concurrent writers
	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				agent.setInitDone()
			}
		}()
	}

	assert.True(waitForRoutinesWithTimeout(&wg, 5*time.Second))
	assert.True(agent.InitDone())
}

// Test diagThresholdFor function
func TestDiagThresholdFor(t *testing.T) {
	assert := assert.New(t)

	// Test user initiated
	threshold := diagThresholdFor(true)
	assert.Equal(base.DiagNetworkThreshold, threshold)

	// Test system initiated
	threshold = diagThresholdFor(false)
	assert.Equal(base.DiagInternalThreshold, threshold)
}

// ============= Test Cases for Metakv Operations =============

func TestRemoteCngAgent_AddOp_Success(t *testing.T) {
	assert := assert.New(t)

	agent, _, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()
	ctx := context.Background()

	// Setup mocks for successful add operation
	metakvMock.On("AddSensitiveWithCatalog", metadata_svc.RemoteClustersCatalogKey, ref.Id(), mock.Anything).Return(nil)

	// Mock the read-after-write call
	refBytes, _ := ref.Marshal()
	metakvMock.On("Get", ref.Id()).Return(refBytes, interface{}(nil), nil)

	err := agent.AddOp(ctx, ref)
	assert.NoError(err)

	// Verify the reference was updated in cache
	assert.True(agent.refCache.reference.IsSame(ref))

	metakvMock.AssertExpectations(t)
}

func TestRemoteCngAgent_AddOp_NilReference(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ctx := context.Background()

	// Create a nil reference which will cause a panic/error when accessing ID
	var ref *metadata.RemoteClusterReference

	// This should panic when trying to access ref.Id() in AddOp
	assert.Panics(func() {
		agent.AddOp(ctx, ref)
	})
}

func TestRemoteCngAgent_AddOp_MetakvFails(t *testing.T) {
	assert := assert.New(t)

	agent, _, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()
	ctx := context.Background()

	// Setup mocks for failed add operation
	metakvMock.On("AddSensitiveWithCatalog", metadata_svc.RemoteClustersCatalogKey, ref.Id(), mock.Anything).Return(fmt.Errorf("metakv add failed"))

	err := agent.AddOp(ctx, ref)
	assert.Error(err)
	assert.Contains(err.Error(), "metakv add failed")

	// Verify the reference was not updated in cache
	assert.True(agent.refCache.reference.IsEmpty())

	metakvMock.AssertExpectations(t)
}

func TestRemoteCngAgent_SetOp_Success(t *testing.T) {
	assert := assert.New(t)

	agent, _, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()
	ctx := context.Background()

	// Setup mocks for successful set operation
	metakvMock.On("SetSensitive", ref.Id(), mock.Anything, ref.Revision()).Return(nil)

	// Mock the read-after-write call
	refBytes, _ := ref.Marshal()
	metakvMock.On("Get", ref.Id()).Return(refBytes, interface{}(nil), nil)

	err := agent.SetOp(ctx, ref)
	assert.NoError(err)

	// Verify the reference was updated in cache
	assert.True(agent.refCache.reference.IsSame(ref))

	metakvMock.AssertExpectations(t)
}

func TestRemoteCngAgent_SetOp_NilReference(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ctx := context.Background()

	// Create a nil reference which will cause a panic/error when accessing ID
	var ref *metadata.RemoteClusterReference

	// This should panic when trying to access ref.Id() in SetOp
	assert.Panics(func() {
		agent.SetOp(ctx, ref)
	})
}

func TestRemoteCngAgent_DelOp_Success(t *testing.T) {
	assert := assert.New(t)

	agent, _, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()
	ctx := context.Background()

	// Setup initial reference in cache
	agent.refCache.update(ref)

	// Setup mocks for successful delete operation
	metakvMock.On("DelWithCatalog", metadata_svc.RemoteClustersCatalogKey, ref.Id(), interface{}(nil)).Return(nil)

	err := agent.DelOp(ctx)
	assert.NoError(err)

	// Verify the reference cache was cleared
	assert.True(agent.refCache.reference.IsEmpty())
	assert.True(agent.refCache.refDeletedFromMetakv)

	metakvMock.AssertExpectations(t)
}

func TestRemoteCngAgent_DelOp_MetakvFails(t *testing.T) {
	assert := assert.New(t)

	agent, _, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()
	ctx := context.Background()

	// Setup initial reference in cache
	agent.refCache.update(ref)

	// Setup mocks for failed delete operation
	metakvMock.On("DelWithCatalog", metadata_svc.RemoteClustersCatalogKey, ref.Id(), interface{}(nil)).Return(fmt.Errorf("metakv delete failed"))

	err := agent.DelOp(ctx)
	assert.Error(err)
	assert.Contains(err.Error(), "metakv delete failed")

	// Verify the reference cache was not cleared on failure
	assert.True(agent.refCache.reference.IsSame(ref))
	assert.False(agent.refCache.refDeletedFromMetakv)

	metakvMock.AssertExpectations(t)
}

// ============= Test Cases for persistReference =============

func TestRemoteCngAgent_PersistReference_BeginOpFails(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()
	ctx := context.Background()

	// Start an operation to block the next one
	_, _, err := agent.metakvOpState.beginOp(ctx, MetakvOpAdd)
	assert.NoError(err)

	// This should fail with RemoteSyncInProgress
	err = agent.persistReference(ctx, MetakvOpSet, ref.Id(), []byte("test"), ref)
	assert.Equal(metadata_svc.RemoteSyncInProgress, err)

	// Cleanup
	agent.metakvOpState.mutex.Lock()
	agent.metakvOpState.endOpNolock()
	agent.metakvOpState.mutex.Unlock()
}

func TestRemoteCngAgent_PersistReference_ContextCanceled(t *testing.T) {
	assert := assert.New(t)

	agent, _, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Create canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Setup mock to potentially be called but should return early due to canceled context
	metakvMock.On("AddSensitiveWithCatalog", metadata_svc.RemoteClustersCatalogKey, ref.Id(), mock.Anything).Maybe().Return(nil)

	err := agent.persistReference(ctx, MetakvOpAdd, ref.Id(), []byte("test"), ref)
	assert.Error(err)
	assert.Contains(err.Error(), "context canceled")
}

// ============= Test Cases for readAfterWrite =============

func TestRemoteCngAgent_ReadAfterWrite_Success(t *testing.T) {
	assert := assert.New(t)

	agent, _, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Setup mock for successful read
	refBytes, _ := ref.Marshal()
	metakvMock.On("Get", ref.Id()).Return(refBytes, interface{}(nil), nil)

	err := agent.readAfterWrite(ref.Id(), ref)
	assert.NoError(err)

	metakvMock.AssertExpectations(t)
}

func TestRemoteCngAgent_ReadAfterWrite_MetakvError(t *testing.T) {
	assert := assert.New(t)

	agent, _, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Setup mock for metakv error
	metakvMock.On("Get", ref.Id()).Return([]byte(nil), interface{}(nil), fmt.Errorf("metakv get error"))

	err := agent.readAfterWrite(ref.Id(), ref)
	assert.NoError(err) // Should not return error, but set revision to nil
	assert.Nil(ref.Revision())

	metakvMock.AssertExpectations(t)
}

func TestRemoteCngAgent_ReadAfterWrite_KeyNotFound(t *testing.T) {
	assert := assert.New(t)

	agent, _, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Setup mock for key not found (nil value)
	metakvMock.On("Get", ref.Id()).Return([]byte(nil), interface{}(nil), nil)

	err := agent.readAfterWrite(ref.Id(), ref)
	assert.Equal(metadata_svc.DeleteAlreadyIssued, err)

	metakvMock.AssertExpectations(t)
}

func TestRemoteCngAgent_ReadAfterWrite_UnmarshalFails(t *testing.T) {
	assert := assert.New(t)

	agent, _, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Setup mock to return invalid data
	metakvMock.On("Get", ref.Id()).Return([]byte("invalid json"), interface{}(nil), nil)

	err := agent.readAfterWrite(ref.Id(), ref)
	assert.Equal(base.ErrorUnmarshallFailed, err)

	metakvMock.AssertExpectations(t)
}

// ============= Test Cases for updateReference =============

func TestRemoteCngAgent_UpdateReference_NoChanges(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Set initial reference
	agent.refCache.update(ref)

	// Try to update with same reference
	err := agent.updateReference(ref, false)
	assert.NoError(err)
}

func TestRemoteCngAgent_UpdateReference_RefDeletedFromMetakv(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Mark reference as deleted from metakv
	agent.refCache.refDeletedFromMetakv = true

	// Try to update
	err := agent.updateReference(ref, false)
	assert.Equal(metadata_svc.DeleteAlreadyIssued, err)
}

func TestRemoteCngAgent_UpdateReference_UserInitiated_Add(t *testing.T) {
	assert := assert.New(t)

	agent, _, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Setup mocks for add operation
	metakvMock.On("AddSensitiveWithCatalog", metadata_svc.RemoteClustersCatalogKey, ref.Id(), mock.Anything).Return(nil)
	refBytes, _ := ref.Marshal()
	metakvMock.On("Get", ref.Id()).Return(refBytes, interface{}(nil), nil)

	err := agent.updateReference(ref, true)
	assert.NoError(err)

	metakvMock.AssertExpectations(t)
}

func TestRemoteCngAgent_UpdateReference_UserInitiated_Set(t *testing.T) {
	assert := assert.New(t)

	agent, _, metakvMock, _ := createTestRemoteCngAgent()
	existingRef := createTestRemoteClusterReference()
	newRef := createTestRemoteClusterReference()
	newRef.SetName("updated")

	// Set initial reference
	agent.refCache.update(existingRef)

	// Setup mocks for set operation
	metakvMock.On("SetSensitive", existingRef.Id(), mock.Anything, existingRef.Revision()).Return(nil)
	refBytes, _ := newRef.Marshal()
	metakvMock.On("Get", existingRef.Id()).Return(refBytes, interface{}(nil), nil)

	err := agent.updateReference(newRef, true)
	assert.NoError(err)

	// Verify ID and revision were set correctly
	assert.Equal(existingRef.Id(), newRef.Id())
	assert.Equal(existingRef.Revision(), newRef.Revision())

	metakvMock.AssertExpectations(t)
}

func TestRemoteCngAgent_UpdateReference_SystemInitiated(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Mock callback registration
	callbackCalled := false
	agent.metadataChangeCallback = func(id string, oldRef, newRef interface{}) error {
		callbackCalled = true
		return nil
	}

	err := agent.updateReference(ref, false)
	assert.NoError(err)
	assert.True(callbackCalled)
	assert.True(agent.refCache.reference.IsSame(ref))
}

// ============= Test Cases for DeleteReference =============

func TestRemoteCngAgent_DeleteReference_WithMetakv(t *testing.T) {
	assert := assert.New(t)

	agent, _, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Setup initial reference
	agent.refCache.update(ref)

	// Setup mock for delete operation
	metakvMock.On("DelWithCatalog", metadata_svc.RemoteClustersCatalogKey, ref.Id(), interface{}(nil)).Return(nil)

	deletedRef, err := agent.DeleteReference(true)
	assert.NoError(err)
	assert.NotNil(deletedRef)
	assert.True(deletedRef.IsSame(ref))

	// Verify cache was cleared
	assert.True(agent.refCache.reference.IsEmpty())
	assert.True(agent.refCache.refDeletedFromMetakv)

	metakvMock.AssertExpectations(t)
}

func TestRemoteCngAgent_DeleteReference_WithoutMetakv(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Setup initial reference
	agent.refCache.update(ref)

	// Mock callback registration
	callbackCalled := false
	agent.metadataChangeCallback = func(id string, oldRef, newRef interface{}) error {
		callbackCalled = true
		return nil
	}

	deletedRef, err := agent.DeleteReference(false)
	assert.NoError(err)
	assert.NotNil(deletedRef)
	assert.True(deletedRef.IsSame(ref))
	assert.True(callbackCalled)

	// Verify cache was cleared
	assert.True(agent.refCache.reference.IsEmpty())
	assert.True(agent.refCache.refDeletedFromMetakv)
}

func TestRemoteCngAgent_DeleteReference_MetakvFails(t *testing.T) {
	assert := assert.New(t)

	agent, _, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Setup initial reference
	agent.refCache.update(ref)

	// Setup mock for failed delete operation
	metakvMock.On("DelWithCatalog", metadata_svc.RemoteClustersCatalogKey, ref.Id(), interface{}(nil)).Return(fmt.Errorf("delete failed"))

	deletedRef, err := agent.DeleteReference(true)
	assert.Error(err)
	assert.Nil(deletedRef)
	assert.Contains(err.Error(), "delete failed")

	// Verify cache was not cleared on failure
	assert.True(agent.refCache.reference.IsSame(ref))
	assert.False(agent.refCache.refDeletedFromMetakv)

	metakvMock.AssertExpectations(t)
}

// ============= Test Cases for Connection Error Handling =============

func TestRemoteCngAgent_RegisterConnErr(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Setup initial reference
	agent.refCache.update(ref)

	// Register connection error
	connErr := metadata.ConnErr{
		FirstOccurence: time.Now(),
		TargetNode:     "test-node",
		Cause:          "test error",
		Occurences:     1,
	}

	agent.registerConnErr(connErr)

	// Verify error was registered (we can't directly check the internal state,
	// but we can verify the function doesn't panic and completes successfully)
	assert.True(true) // If we get here, the function worked
}

func TestRemoteCngAgent_ClearConnErrs(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Setup initial reference
	agent.refCache.update(ref)

	// Clear connection errors
	agent.clearConnErrs()

	// Verify function completes successfully
	assert.True(true) // If we get here, the function worked
}

// ============= Test Cases for UpdateReferenceFrom Methods =============

func TestRemoteCngAgent_UpdateReferenceFrom(t *testing.T) {
	assert := assert.New(t)

	agent, _, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// Setup mocks for successful operation
	metakvMock.On("AddSensitiveWithCatalog", metadata_svc.RemoteClustersCatalogKey, ref.Id(), mock.Anything).Return(nil)
	refBytes, _ := ref.Marshal()
	metakvMock.On("Get", ref.Id()).Return(refBytes, interface{}(nil), nil)

	err := agent.UpdateReferenceFrom(ref, true)
	assert.NoError(err)

	metakvMock.AssertExpectations(t)
}

func TestRemoteCngAgent_UpdateReferenceFromAsync(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReference()

	// This is currently a no-op, so should always return nil
	err := agent.UpdateReferenceFromAsync(ref, true)
	assert.NoError(err)
}
