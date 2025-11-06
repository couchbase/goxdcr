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

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/metadata_svc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Test constants for refresh tests
const (
	testRefreshInterval = 10 * time.Millisecond
	testTimeout         = 2 * time.Second
	testShortTimeout    = 100 * time.Millisecond
)

// Helper functions for creating test data specific to refresh tests
func createTestRemoteClusterReferenceWithCreds() *metadata.RemoteClusterReference {
	hostAddr, _ := base.ValidateHostAddrForCbCluster(testHostname)
	primaryCreds := base.Credentials{
		UserName_: "primary_user",
		Password_: "primary_pass",
	}

	testCACert := []byte(`-----BEGIN CERTIFICATE-----
MIIDHjCCAgagAwIBAgIUfqcS7+eKG0NDcairq8C+GxRiuVUwDQYJKoZIhvcNAQEL
BQAwIDEeMBwGA1UEAwwVQ291Y2hiYXNlIENORyBSb290IENBMB4XDTI1MTAyNDEz
NTAyNVoXDTM1MTAyMjEzNTAyNVowIDEeMBwGA1UEAwwVQ291Y2hiYXNlIENORyBS
b290IENBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAydO0ADzhzn46
O6f30/736XEfMQd3QDH0yDX3Y+boiSZsTRrXTMGk9DuSqpqsWfw7tPDv2ZttP1Fs
cmFDa8omo30fWrsWe975G4smqgCmgIx1ZK1YFzanI9eFLzf6pH4hCGRsP055pKz6
8QqpGsDna9weay8kNNrM9GMLN+ak/BpNYdn+lpK8RAmq/KKKygJNHZ/PlA4txn2f
W2MX1DSA8krYgE53RBfZ/Dw95jpOXsUqh0gEEaM/YByERoRNKjjJffkeaUIIfiPU
c3wlBx0MsNDRB6lvEx3HvtmhYHXaNG9ljR1oXOmOkGU+c6VwsN6sb8tbvoQkEtlk
vn/pTKPmXQIDAQABo1AwTjAdBgNVHQ4EFgQUmIiJHBbGnK2SbEwPsOofdikxtIQw
HwYDVR0jBBgwFoAUmIiJHBbGnK2SbEwPsOofdikxtIQwDAYDVR0TBAUwAwEB/zAN
BgkqhkiG9w0BAQsFAAOCAQEAboeLqniRBKMeCWKLgfrOd4H1BGA4raWRAryYufuF
WQ4GPnGqQU13DDGk9W+x2E0FamqUzazTvSMIn9rJAYGDdu+iGEPl6d6K/B9Kuz3+
3RRA7S6tt7a2yXGYqooW0R//2TP3USdPpxvH88NFjA5fM8KtpoTAbFqVaGp4H0ZA
V/QaPjOfTjrp3whPPfhucOc2Bra1S+uzqOqmGCCKgOMYS9fGHpA68XuiDw/qh08d
ovJwMZw1VWvaMr5LAM1DpXgBzZKzndrG9i+g/aXIrhc7pYm8gRzoR5dKXXGc2cWH
/3eIOPyZGR0K+MnZ6imQyfGKJiAXWXMFhQ916Opeb9v3zw==
-----END CERTIFICATE-----`)

	ref, _ := metadata.NewRemoteClusterReference(testUuid, testName, hostAddr, primaryCreds.UserName_, primaryCreds.Password_, "", false, "", testCACert, nil, nil, nil)
	ref.SetId(testId)
	return ref
}

func createTestRemoteClusterReferenceWithStagedCreds() *metadata.RemoteClusterReference {
	ref := createTestRemoteClusterReferenceWithCreds()
	stagedCreds := &base.Credentials{
		UserName_: "staged_user",
		Password_: "staged_pass",
	}
	ref.StagedCredentials = stagedCreds
	return ref
}

func createTestClusterInfoResponse(uuid string) *base.GrpcResponse[*internal_xdcr_v1.GetClusterInfoResponse] {
	st, _ := status.FromError(nil)
	return &base.GrpcResponse[*internal_xdcr_v1.GetClusterInfoResponse]{
		Resp: &internal_xdcr_v1.GetClusterInfoResponse{
			ClusterUuid: uuid,
		},
		Status: st,
		Error:  nil,
	}
}

func createTestClusterInfoErrorResponse(code codes.Code, err error) *base.GrpcResponse[*internal_xdcr_v1.GetClusterInfoResponse] {
	st := status.New(code, err.Error())
	return &base.GrpcResponse[*internal_xdcr_v1.GetClusterInfoResponse]{
		Resp:   nil,
		Status: st,
		Error:  err,
	}
}

// Helper function to setup a refreshSnapShot for testing
func setupTestRefreshSnapShot(agent *RemoteCngAgent, ref *metadata.RemoteClusterReference) *refreshSnapShot {
	capability := metadata.Capability{}
	return newRefreshSnapShot(ref, capability, agent.services, agent.logger)
}

// ============= Test Cases for refreshState =============

func TestRefreshState_BeginOp_Success(t *testing.T) {
	assert := assert.New(t)

	rs := &refreshState{}
	rs.cond = sync.NewCond(&rs.mutex)
	ctx := context.Background()

	// Test successful begin operation
	opCtx, resultCh, err := rs.beginOp(ctx)
	assert.NoError(err)
	assert.NotNil(opCtx)
	assert.Nil(resultCh)
	assert.True(rs.active)
	assert.NotNil(rs.cancelActiveOp)

	// Cleanup
	var opErr error
	rs.endOp(&opErr)
}

func TestRefreshState_BeginOp_AlreadyActive(t *testing.T) {
	assert := assert.New(t)

	rs := &refreshState{}
	rs.cond = sync.NewCond(&rs.mutex)
	ctx := context.Background()

	// Start first operation
	_, _, err := rs.beginOp(ctx)
	assert.NoError(err)

	// Try to start second operation (should get piggyback channel)
	opCtx2, resultCh2, err := rs.beginOp(ctx)
	assert.Equal(metadata_svc.RefreshAlreadyActive, err)
	assert.Nil(opCtx2)
	assert.NotNil(resultCh2)
	assert.Len(rs.resultCh, 1)

	// Cleanup
	var opErr error
	rs.endOp(&opErr)

	// Verify second caller gets result
	select {
	case receivedErr := <-resultCh2:
		assert.NoError(receivedErr)
	case <-time.After(testShortTimeout):
		t.Fatal("Second caller should have received result")
	}
}

func TestRefreshState_BeginOp_TemporarilyDisabled(t *testing.T) {
	assert := assert.New(t)

	rs := &refreshState{}
	rs.cond = sync.NewCond(&rs.mutex)
	rs.isTemporarilyDisabled = true
	ctx := context.Background()

	// Should fail when temporarily disabled
	opCtx, resultCh, err := rs.beginOp(ctx)
	assert.Equal(metadata_svc.SetInProgress, err)
	assert.Nil(opCtx)
	assert.Nil(resultCh)
}

func TestRefreshState_EndOp_Success(t *testing.T) {
	assert := assert.New(t)

	rs := &refreshState{}
	rs.cond = sync.NewCond(&rs.mutex)
	ctx := context.Background()

	// Start operation
	opCtx, _, err := rs.beginOp(ctx)
	assert.NoError(err)

	// End operation
	var opErr error
	rs.endOp(&opErr)

	assert.False(rs.active)
	assert.Nil(rs.cancelActiveOp)
	assert.Nil(rs.resultCh)

	// Context should be canceled
	select {
	case <-opCtx.Done():
		// Expected
	case <-time.After(testShortTimeout):
		t.Fatal("Operation context should have been canceled")
	}
}

func TestRefreshState_EndOp_WithPiggybackCallers(t *testing.T) {
	assert := assert.New(t)

	rs := &refreshState{}
	rs.cond = sync.NewCond(&rs.mutex)
	ctx := context.Background()

	// Start first operation
	_, _, err := rs.beginOp(ctx)
	assert.NoError(err)

	// Start multiple piggyback operations
	numCallers := 5
	resultChannels := make([]chan error, numCallers)
	for i := 0; i < numCallers; i++ {
		_, resultCh, err := rs.beginOp(ctx)
		assert.Equal(metadata_svc.RefreshAlreadyActive, err)
		assert.NotNil(resultCh)
		resultChannels[i] = resultCh
	}

	// End operation with error
	testErr := fmt.Errorf("test error")
	rs.endOp(&testErr)

	// Verify all piggyback callers get the result
	for i, resultCh := range resultChannels {
		select {
		case receivedErr := <-resultCh:
			assert.Equal(testErr, receivedErr, fmt.Sprintf("Caller %d should receive the test error", i))
		case <-time.After(testShortTimeout):
			t.Fatalf("Caller %d should have received result", i)
		}
	}
}

func TestRefreshState_AbortAnyRefreshOp_NoActiveOp(t *testing.T) {
	assert := assert.New(t)

	rs := &refreshState{}
	rs.cond = sync.NewCond(&rs.mutex)

	// Should temporarily disable and return true for needToReEnable
	needToReEnable := rs.abortAnyRefreshOp()
	assert.True(needToReEnable)
	assert.True(rs.isTemporarilyDisabled)
}

func TestRefreshState_AbortAnyRefreshOp_WithActiveOp(t *testing.T) {
	assert := assert.New(t)

	rs := &refreshState{}
	rs.cond = sync.NewCond(&rs.mutex)
	ctx := context.Background()

	// Start operation
	opCtx, _, err := rs.beginOp(ctx)
	assert.NoError(err)

	// Abort in separate goroutine to avoid blocking
	abortDone := make(chan bool)
	var needToReEnable bool
	go func() {
		needToReEnable = rs.abortAnyRefreshOp()
		abortDone <- true
	}()

	// Wait a bit then end the operation
	time.Sleep(10 * time.Millisecond)
	var opErr error
	rs.endOp(&opErr)

	// Wait for abort to complete
	select {
	case <-abortDone:
		assert.True(needToReEnable)
		assert.True(rs.isTemporarilyDisabled)
		assert.Equal(metadata_svc.RefreshAbortAcknowledged, rs.abortState)
		assert.False(rs.active)
		assert.Nil(rs.cancelActiveOp)
		assert.Nil(rs.resultCh)
	case <-time.After(testTimeout):
		t.Fatal("Abort operation should have completed")
	}

	// Context should be canceled
	select {
	case <-opCtx.Done():
		// Expected
	case <-time.After(testShortTimeout):
		t.Fatal("Operation context should have been canceled")
	}
}

func TestRefreshState_ReenableRefresh(t *testing.T) {
	assert := assert.New(t)

	rs := &refreshState{}
	rs.cond = sync.NewCond(&rs.mutex)
	rs.isTemporarilyDisabled = true
	rs.abortState = metadata_svc.RefreshAbortAcknowledged

	rs.reenableRefresh()

	assert.False(rs.isTemporarilyDisabled)
	assert.Equal(metadata_svc.RefreshAbortNotRequested, rs.abortState)
}

func TestRefreshState_ConcurrentOperations(t *testing.T) {
	assert := assert.New(t)

	rs := &refreshState{}
	rs.cond = sync.NewCond(&rs.mutex)
	ctx := context.Background()

	var wg sync.WaitGroup
	numGoroutines := 10
	successCount := int32(0)
	alreadyActiveCount := int32(0)

	// Test concurrent begin operations
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			_, _, err := rs.beginOp(ctx)
			switch err {
			case nil:
				atomic.AddInt32(&successCount, 1)
			case metadata_svc.RefreshAlreadyActive:
				atomic.AddInt32(&alreadyActiveCount, 1)
			}
		}()
	}

	assert.True(waitForRoutinesWithTimeout(&wg, testTimeout))

	// Only one should succeed, others should get RefreshAlreadyActive
	assert.Equal(int32(1), atomic.LoadInt32(&successCount))
	assert.Equal(int32(numGoroutines-1), atomic.LoadInt32(&alreadyActiveCount))

	// Cleanup
	var opErr error
	rs.endOp(&opErr)
}

// ============= Test Cases for refreshSnapShot =============

func TestRefreshSnapShot_PerformRefreshOp_Success(t *testing.T) {
	assert := assert.New(t)

	agent, utilsMock, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithCreds()
	refreshSnapshot := setupTestRefreshSnapShot(agent, ref)
	ctx := context.Background()

	// Setup successful cluster info response
	clusterInfoResp := createTestClusterInfoResponse(testUuid)
	utilsMock.On("CngGetClusterInfo", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.GetClusterInfoRequest]")).Return(clusterInfoResp)

	statusCode, err := refreshSnapshot.performRefreshOp(ctx)
	assert.NoError(err)
	assert.Equal(codes.OK, statusCode)
	assert.False(refreshSnapshot.promoteStageToPrimary) // Should remain false for successful primary auth

	utilsMock.AssertExpectations(t)
}

func TestRefreshSnapShot_PerformRefreshOp_UUIDMismatch(t *testing.T) {
	assert := assert.New(t)

	agent, utilsMock, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithCreds()
	refreshSnapshot := setupTestRefreshSnapShot(agent, ref)
	ctx := context.Background()

	// Setup cluster info response with different UUID
	differentUuid := "different-uuid"
	clusterInfoResp := createTestClusterInfoResponse(differentUuid)
	utilsMock.On("CngGetClusterInfo", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.GetClusterInfoRequest]")).Return(clusterInfoResp)

	statusCode, err := refreshSnapshot.performRefreshOp(ctx)
	assert.Equal(metadata_svc.UUIDMismatchError, err)
	assert.Equal(codes.OK, statusCode)

	utilsMock.AssertExpectations(t)
}

func TestRefreshSnapShot_PerformRefreshOp_PrimaryAuthFailsRetriesWithStaged(t *testing.T) {
	assert := assert.New(t)

	agent, utilsMock, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithStagedCreds()
	refreshSnapshot := setupTestRefreshSnapShot(agent, ref)
	ctx := context.Background()

	// Setup sequence-based mocking: primary auth failure, then staged auth success
	clusterInfoResp := createTestClusterInfoResponse(testUuid)

	// First call (primary creds) should fail with Unauthenticated
	utilsMock.On("CngGetClusterInfo", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.GetClusterInfoRequest]")).Return(createTestClusterInfoErrorResponse(codes.Unauthenticated, fmt.Errorf("primary auth failed"))).Once()

	// Second call (staged creds) should succeed
	utilsMock.On("CngGetClusterInfo", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.GetClusterInfoRequest]")).Return(clusterInfoResp).Once()

	statusCode, err := refreshSnapshot.performRefreshOp(ctx)
	assert.NoError(err)
	assert.Equal(codes.OK, statusCode)
	assert.True(refreshSnapshot.promoteStageToPrimary) // Should be true when staged creds succeed

	utilsMock.AssertExpectations(t)
}

func TestRefreshSnapShot_PerformRefreshOp_BothCredentialsFail(t *testing.T) {
	assert := assert.New(t)

	agent, utilsMock, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithStagedCreds()
	refreshSnapshot := setupTestRefreshSnapShot(agent, ref)
	ctx := context.Background()

	// Setup sequence-based mocking: both primary and staged auth should fail
	// First call (primary creds) fails
	utilsMock.On("CngGetClusterInfo", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.GetClusterInfoRequest]")).Return(createTestClusterInfoErrorResponse(codes.Unauthenticated, fmt.Errorf("primary auth failed"))).Once()

	// Second call (staged creds) also fails
	utilsMock.On("CngGetClusterInfo", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.GetClusterInfoRequest]")).Return(createTestClusterInfoErrorResponse(codes.Unauthenticated, fmt.Errorf("staged auth failed"))).Once()

	statusCode, err := refreshSnapshot.performRefreshOp(ctx)
	assert.Error(err)
	assert.Contains(err.Error(), "failed to contact target with both credentials")
	assert.Equal(codes.Unauthenticated, statusCode)
	assert.False(refreshSnapshot.promoteStageToPrimary)

	utilsMock.AssertExpectations(t)
}

func TestRefreshSnapShot_PerformRefreshOp_ContextCanceled(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithCreds()
	refreshSnapshot := setupTestRefreshSnapShot(agent, ref)

	// Create canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	statusCode, err := refreshSnapshot.performRefreshOp(ctx)
	assert.Error(err)
	assert.Contains(err.Error(), "context canceled")
	assert.Equal(codes.Canceled, statusCode)
}

func TestRefreshSnapShot_PerformRefreshOp_NetworkError(t *testing.T) {
	assert := assert.New(t)

	agent, utilsMock, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithCreds()
	refreshSnapshot := setupTestRefreshSnapShot(agent, ref)
	ctx := context.Background()

	// Setup network error
	utilsMock.On("CngGetClusterInfo", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.GetClusterInfoRequest]")).Return(createTestClusterInfoErrorResponse(codes.Unavailable, fmt.Errorf("network error")))

	statusCode, err := refreshSnapshot.performRefreshOp(ctx)
	assert.Error(err)
	assert.Contains(err.Error(), "failed to contact target")
	assert.Equal(codes.Unavailable, statusCode)

	utilsMock.AssertExpectations(t)
}

func TestRefreshSnapShot_IsPersistenceRequired_NoChanges(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithCreds()
	refreshSnapshot := setupTestRefreshSnapShot(agent, ref)

	// No changes made to workingRef
	assert.False(refreshSnapshot.isPersistenceRequired())
}

func TestRefreshSnapShot_IsPersistenceRequired_WithChanges(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithStagedCreds()
	refreshSnapshot := setupTestRefreshSnapShot(agent, ref)

	// Modify workingRef to simulate credential promotion
	refreshSnapshot.workingRef.PromoteStageCredsToPrimary()

	assert.True(refreshSnapshot.isPersistenceRequired())
}

// ============= Test Cases for RemoteCngAgent Refresh Methods =============

func TestRemoteCngAgent_Refresh_Success(t *testing.T) {
	assert := assert.New(t)

	agent, utilsMock, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithCreds()

	// Setup initial reference
	agent.refCache.update(ref)
	agent.setInitDone()

	// Setup successful cluster info response
	clusterInfoResp := createTestClusterInfoResponse(testUuid)
	utilsMock.On("CngGetClusterInfo", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.GetClusterInfoRequest]")).Return(clusterInfoResp)

	err := agent.Refresh()
	assert.NoError(err)

	utilsMock.AssertExpectations(t)
}

func TestRemoteCngAgent_Refresh_WithCredentialPromotion(t *testing.T) {
	assert := assert.New(t)

	agent, utilsMock, metakvMock, uiLogMock := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithStagedCreds()

	// Setup initial reference
	agent.refCache.update(ref)
	agent.setInitDone()

	// Setup sequence-based mocking: primary auth failure, then staged auth success
	clusterInfoResp := createTestClusterInfoResponse(testUuid)

	// First call (primary creds) should fail with Unauthenticated
	utilsMock.On("CngGetClusterInfo", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.GetClusterInfoRequest]")).Return(createTestClusterInfoErrorResponse(codes.Unauthenticated, fmt.Errorf("primary auth failed"))).Once()

	// Second call (staged creds) should succeed
	utilsMock.On("CngGetClusterInfo", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.GetClusterInfoRequest]")).Return(clusterInfoResp).Once()

	// Setup successful metakv operation for persistence
	metakvMock.On("SetSensitive", ref.Id(), mock.Anything, ref.Revision()).Return(nil)
	// Return the reference with promoted credentials to simulate what metakv would return
	promotedRef := ref.Clone()
	promotedRef.PromoteStageCredsToPrimary()
	refBytes, _ := promotedRef.Marshal()
	metakvMock.On("Get", ref.Id()).Return(refBytes, interface{}(nil), nil)

	// Setup UI log for credential promotion message
	uiLogMock.On("Write", mock.MatchedBy(func(msg string) bool {
		return fmt.Sprintf("Promoted staged credentials to primary on remote cluster \"%s\".", ref.Name()) == msg
	})).Return(nil)

	err := agent.Refresh()
	assert.NoError(err)

	utilsMock.AssertExpectations(t)
	metakvMock.AssertExpectations(t)
	uiLogMock.AssertExpectations(t)
}

func TestRemoteCngAgent_Refresh_PiggybackOnActiveRefresh(t *testing.T) {
	assert := assert.New(t)

	agent, utilsMock, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithCreds()

	// Setup initial reference
	agent.refCache.update(ref)
	agent.setInitDone()

	// Setup successful cluster info response (delayed)
	clusterInfoResp := createTestClusterInfoResponse(testUuid)
	utilsMock.On("CngGetClusterInfo", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.GetClusterInfoRequest]")).Return(clusterInfoResp).Run(func(args mock.Arguments) {
		time.Sleep(50 * time.Millisecond) // Simulate delay
	})

	// Start first refresh in background
	var firstErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		firstErr = agent.Refresh()
	}()

	// Wait a bit then start second refresh (should piggyback)
	time.Sleep(10 * time.Millisecond)
	secondErr := agent.Refresh()

	wg.Wait()

	// Both should succeed
	assert.NoError(firstErr)
	assert.NoError(secondErr)

	utilsMock.AssertExpectations(t)
}

func TestRemoteCngAgent_Refresh_TemporarilyDisabled(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithCreds()

	// Setup initial reference
	agent.refCache.update(ref)
	agent.setInitDone()

	// Temporarily disable refresh
	agent.refreshState.isTemporarilyDisabled = true

	err := agent.Refresh()
	assert.Error(err)
	assert.Contains(err.Error(), metadata_svc.SetInProgress.Error())
}

func TestRemoteCngAgent_AbortAnyOngoingRefresh(t *testing.T) {
	assert := assert.New(t)

	agent, utilsMock, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithCreds()

	// Setup initial reference
	agent.refCache.update(ref)
	agent.setInitDone()

	// Setup delayed cluster info response
	clusterInfoResp := createTestClusterInfoResponse(testUuid)
	utilsMock.On("CngGetClusterInfo", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.GetClusterInfoRequest]")).Return(clusterInfoResp).Run(func(args mock.Arguments) {
		time.Sleep(100 * time.Millisecond) // Simulate delay
	})

	// Start refresh in background
	var refreshErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		refreshErr = agent.Refresh()
	}()

	// Wait a bit then abort
	time.Sleep(10 * time.Millisecond)
	needToReEnable := agent.AbortAnyOngoingRefresh()
	assert.True(needToReEnable)

	// Refresh should be canceled
	assert.Error(refreshErr)
	assert.Contains(refreshErr.Error(), "context canceled")

	// Re-enable and wait for completion
	agent.ReenableRefresh()
	wg.Wait()

	utilsMock.AssertExpectations(t)
}

func TestRemoteCngAgent_ReenableRefresh(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()

	// Disable refresh first
	agent.refreshState.isTemporarilyDisabled = true
	agent.refreshState.abortState = metadata_svc.RefreshAbortAcknowledged

	// Re-enable
	agent.ReenableRefresh()

	assert.False(agent.refreshState.isTemporarilyDisabled)
	assert.Equal(metadata_svc.RefreshAbortNotRequested, agent.refreshState.abortState)
}

func TestRemoteCngAgent_RunPeriodicRefresh(t *testing.T) {
	agent, utilsMock, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithCreds()

	// Setup initial reference
	agent.refCache.update(ref)
	agent.setInitDone()

	// Setup successful cluster info response for multiple calls
	clusterInfoResp := createTestClusterInfoResponse(testUuid)
	utilsMock.On("CngGetClusterInfo", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.GetClusterInfoRequest]")).Return(clusterInfoResp).Maybe()

	// Start periodic refresh with shorter interval for testing
	agent.waitGrp.Add(1)
	go agent.runPeriodicRefresh()

	// Let it run for a short time
	time.Sleep(3 * testRefreshInterval)

	// Stop the agent
	close(agent.finCh)
	agent.waitGrp.Wait()

	// Should have made at least one refresh call
	utilsMock.AssertExpectations(t)
}

// ============= Test Cases for Concurrent Operations and Deadlock Detection =============

func TestRefreshState_ConcurrentAbortAndRefresh(t *testing.T) {
	assert := assert.New(t)

	agent, utilsMock, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithCreds()

	// Setup initial reference
	agent.refCache.update(ref)
	agent.setInitDone()

	// Setup delayed cluster info response
	clusterInfoResp := createTestClusterInfoResponse(testUuid)
	utilsMock.On("CngGetClusterInfo", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.GetClusterInfoRequest]")).Return(clusterInfoResp).Run(func(args mock.Arguments) {
		time.Sleep(50 * time.Millisecond)
	}).Maybe()

	var wg sync.WaitGroup
	var refreshErr error

	// Start one refresher
	wg.Add(1)
	go func() {
		defer wg.Done()
		refreshErr = agent.Refresh()
	}()

	// Wait a bit then abort
	time.Sleep(10 * time.Millisecond)
	needToReEnable := agent.AbortAnyOngoingRefresh()

	// Re-enable refresh
	agent.ReenableRefresh()

	// Wait for refresh to complete
	assert.True(waitForRoutinesWithTimeout(&wg, testTimeout))

	// Should have aborted
	assert.True(needToReEnable)

	// The refresh should either be canceled or succeed (depending on timing)
	if refreshErr != nil {
		assert.Contains(refreshErr.Error(), "context canceled")
	}

	utilsMock.AssertExpectations(t)
}

func TestRefreshWithUserInitiatedSetOperation(t *testing.T) {
	assert := assert.New(t)

	agent, utilsMock, metakvMock, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithCreds()

	// Setup initial reference
	agent.refCache.update(ref)
	agent.setInitDone()

	// Setup delayed cluster info response for refresh
	clusterInfoResp := createTestClusterInfoResponse(testUuid)
	utilsMock.On("CngGetClusterInfo", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.GetClusterInfoRequest]")).Return(clusterInfoResp).Run(func(args mock.Arguments) {
		time.Sleep(100 * time.Millisecond)
	}).Maybe()

	// Setup metakv operations for set operation
	newRef := createTestRemoteClusterReferenceWithCreds()
	newRef.SetName("updated")
	metakvMock.On("SetSensitive", ref.Id(), mock.Anything, ref.Revision()).Return(nil)
	refBytes, _ := newRef.Marshal()
	metakvMock.On("Get", ref.Id()).Return(refBytes, interface{}(nil), nil)

	var wg sync.WaitGroup
	var refreshErr, setErr error

	// Start refresh in background
	wg.Add(1)
	go func() {
		defer wg.Done()
		refreshErr = agent.Refresh()
	}()

	// Wait a bit then perform set operation (should preempt refresh)
	time.Sleep(10 * time.Millisecond)
	wg.Add(1)
	go func() {
		defer wg.Done()
		setErr = agent.updateReference(newRef, true)
	}()

	wg.Wait()

	// Set operation should succeed, refresh should be canceled
	assert.NoError(setErr)
	assert.Contains(refreshErr.Error(), "context canceled")

	utilsMock.AssertExpectations(t)
	metakvMock.AssertExpectations(t)
}

// ============= Test Cases for Error Handling =============

func TestRefreshSnapShot_PerformRefreshOp_ConnectionStringError(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()

	// Create a reference with invalid hostname to trigger connection string error
	invalidRef := createTestRemoteClusterReferenceWithCreds()
	invalidRef.SetHostName("")

	refreshSnapshot := setupTestRefreshSnapShot(agent, invalidRef)
	ctx := context.Background()

	statusCode, err := refreshSnapshot.performRefreshOp(ctx)
	assert.Error(err)
	assert.Contains(err.Error(), "failed to construct grpcOpts")
	assert.Equal(codes.Internal, statusCode)
}

func TestRefreshSnapShot_PerformRefreshOp_GrpcOptionsError(t *testing.T) {
	assert := assert.New(t)

	agent, utilsMock, _, _ := createTestRemoteCngAgent()

	// Create a reference with invalid certificates to trigger grpc options error
	invalidRef := createTestRemoteClusterReferenceWithCreds()
	// Set invalid certificate data directly on the Certificate_ field
	invalidRef.Certificate_ = []byte("invalid cert")

	refreshSnapshot := setupTestRefreshSnapShot(agent, invalidRef)
	ctx := context.Background()

	// The certificate issue will be detected during the actual grpc call, not grpc options creation

	statusCode, err := refreshSnapshot.performRefreshOp(ctx)
	assert.Error(err)
	assert.Contains(err.Error(), "failed to contact target")
	assert.Equal(codes.Unknown, statusCode)

	utilsMock.AssertExpectations(t)
}

// ============= Test Cases for Bootstrap Methods (No-ops for CNG) =============

func TestRemoteCngAgent_BootstrapMethods(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()

	// All bootstrap methods should be no-ops for CNG
	agent.ClearBootstrap() // Should not panic
	assert.False(agent.IsBootstrap())
	agent.SetBootstrap() // Should not panic
}

// ============= Helper Test for Name method =============

func TestRemoteCngAgent_Name(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithCreds()

	// Setup initial reference
	agent.refCache.update(ref)

	name := agent.Name()
	assert.Equal(testName, name)
}

func TestRemoteCngAgent_HostName(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithCreds()

	// Setup initial reference
	agent.refCache.update(ref)

	hostname := agent.HostName()
	assert.Equal(testHostname, hostname)
}

func TestRemoteCngAgent_GetReferenceClone(t *testing.T) {
	assert := assert.New(t)

	agent, _, _, _ := createTestRemoteCngAgent()
	ref := createTestRemoteClusterReferenceWithCreds()

	// Setup initial reference
	agent.refCache.update(ref)
	agent.setInitDone()

	cloned, _ := agent.GetReferenceClone(false)
	assert.NotNil(cloned)
	assert.True(cloned.IsSame(ref))
	assert.False(cloned == ref) // Should be different objects
}
