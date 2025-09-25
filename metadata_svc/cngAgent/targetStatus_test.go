// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package cngAgent

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/metadata_svc"
	serviceMocks "github.com/couchbase/goxdcr/v8/service_def/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
)

// Test constants for targetStatus tests
const (
	testTargetNode = "testHost:18098"
)

// Helper functions for creating test data specific to targetStatus tests

func createTestTargetHealthTracker() (*targetHealthTracker, *serviceMocks.ConnectivityHelperSvc, *serviceMocks.UILogSvc) {
	connectivityHelperMock := &serviceMocks.ConnectivityHelperSvc{}
	uiLogMock := &serviceMocks.UILogSvc{}

	services := services{
		uiLog: uiLogMock,
	}

	// Create mock callback functions
	mockRegisterConnErr := func(ce metadata.ConnErr) {}
	mockClearConnErrs := func() {}

	tracker := &targetHealthTracker{
		connectivityHelper: connectivityHelperMock,
		services:           services,
		registerConnErr:    mockRegisterConnErr,
		clearConnErrs:      mockClearConnErrs,
	}

	return tracker, connectivityHelperMock, uiLogMock
}

func createTestRemoteCngAgentWithHealthTracker() (*RemoteCngAgent, *serviceMocks.ConnectivityHelperSvc, *serviceMocks.UILogSvc) {
	tracker, connectivityHelperMock, uiLogMock := createTestTargetHealthTracker()

	services, _, _, _ := setupTestServices()
	// Replace uiLogMock with the one from tracker setup
	services.uiLog = uiLogMock
	logger := createTestLogger()

	agent := &RemoteCngAgent{
		services:      services,
		finCh:         make(chan struct{}),
		logger:        logger,
		healthTracker: tracker,
	}
	agent.refreshState.cond = sync.NewCond(&agent.refreshState.mutex)

	// Set up test reference
	ref := createTestRemoteClusterReference()
	agent.refCache.reference.LoadFrom(ref)

	// Update tracker callbacks to use agent methods
	tracker.registerConnErr = agent.registerConnErr
	tracker.clearConnErrs = agent.clearConnErrs

	return agent, connectivityHelperMock, uiLogMock
}

// ============= Test Cases for targetHealthTracker Basic Methods =============

func TestTargetHealthTracker_IncrementConnErrCount(t *testing.T) {
	assert := assert.New(t)
	tracker, _, _ := createTestTargetHealthTracker()

	// Initial count should be 0
	assert.Equal(0, tracker.getConnErrCount())

	// Increment several times
	tracker.incrementConnErrCount()
	assert.Equal(1, tracker.getConnErrCount())

	tracker.incrementConnErrCount()
	assert.Equal(2, tracker.getConnErrCount())

	tracker.incrementConnErrCount()
	assert.Equal(3, tracker.getConnErrCount())
}

func TestTargetHealthTracker_ResetConnErrCount(t *testing.T) {
	assert := assert.New(t)
	tracker, _, _ := createTestTargetHealthTracker()

	// Increment count first
	tracker.incrementConnErrCount()
	tracker.incrementConnErrCount()
	tracker.incrementConnErrCount()
	assert.Equal(3, tracker.getConnErrCount())

	// Reset should set to 0
	tracker.resetConnErrCount()
	assert.Equal(0, tracker.getConnErrCount())

	// Reset when already 0 should remain 0
	tracker.resetConnErrCount()
	assert.Equal(0, tracker.getConnErrCount())
}

func TestTargetHealthTracker_GetConnErrCount_ThreadSafety(t *testing.T) {
	assert := assert.New(t)
	tracker, _, _ := createTestTargetHealthTracker()

	var wg sync.WaitGroup
	numGoroutines := 10
	incrementsPerGoroutine := 100

	// Start multiple goroutines incrementing the count
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerGoroutine; j++ {
				tracker.incrementConnErrCount()
			}
		}()
	}

	// Start multiple goroutines reading the count
	readCounts := make(chan int, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			count := tracker.getConnErrCount()
			readCounts <- count
		}()
	}

	wg.Wait()
	close(readCounts)

	// Final count should be exact
	finalCount := tracker.getConnErrCount()
	assert.Equal(numGoroutines*incrementsPerGoroutine, finalCount)

	// All read counts should be valid (between 0 and final count)
	for count := range readCounts {
		assert.GreaterOrEqual(count, 0)
		assert.LessOrEqual(count, finalCount)
	}
}

func TestTargetHealthTracker_AuthErrorReportStatus(t *testing.T) {
	assert := assert.New(t)
	tracker, _, _ := createTestTargetHealthTracker()

	// Initial status should be AuthErrNotReported (default zero value)
	assert.Equal(metadata_svc.AuthErrNotReported, tracker.getAuthErrorReportStatus())

	// Set to AuthErrReported
	tracker.setAuthErrorReportStatus(metadata_svc.AuthErrReported)
	assert.Equal(metadata_svc.AuthErrReported, tracker.getAuthErrorReportStatus())

	// Set back to AuthErrNotReported
	tracker.setAuthErrorReportStatus(metadata_svc.AuthErrNotReported)
	assert.Equal(metadata_svc.AuthErrNotReported, tracker.getAuthErrorReportStatus())
}

func TestTargetHealthTracker_AuthErrorReportStatus_ThreadSafety(t *testing.T) {
	assert := assert.New(t)
	tracker, _, _ := createTestTargetHealthTracker()

	var wg sync.WaitGroup
	numGoroutines := 10

	// Start goroutines alternating between setting AuthErrReported and AuthErrNotReported
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if j%2 == 0 {
					tracker.setAuthErrorReportStatus(metadata_svc.AuthErrReported)
				} else {
					tracker.setAuthErrorReportStatus(metadata_svc.AuthErrNotReported)
				}
			}
		}(i)
	}

	// Start goroutines reading the status
	statusReads := make(chan metadata_svc.AuthErrReportStatus, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status := tracker.getAuthErrorReportStatus()
			statusReads <- status
		}()
	}

	wg.Wait()
	close(statusReads)

	// Final status should be one of the valid values
	finalStatus := tracker.getAuthErrorReportStatus()
	assert.True(finalStatus == metadata_svc.AuthErrNotReported)

	// All read statuses should be valid
	var valReadCount int = 0
	for status := range statusReads {
		valReadCount++
		assert.True(status == metadata_svc.AuthErrReported || status == metadata_svc.AuthErrNotReported)
	}
	assert.Equal(valReadCount, numGoroutines)
}

// ============= Test Cases for targetHealthTracker updateHealth Method =============

func TestTargetHealthTracker_UpdateHealth_Unauthenticated(t *testing.T) {
	assert := assert.New(t)
	tracker, connectivityHelperMock, uiLogMock := createTestTargetHealthTracker()

	// Set up initial error count
	tracker.incrementConnErrCount()
	tracker.incrementConnErrCount()
	assert.Equal(2, tracker.getConnErrCount())

	// Set up mocks for unauthenticated case
	connectivityHelperMock.On("MarkNode", testTargetNode, metadata.ConnAuthErr).Return(true, false).Once()
	uiLogMock.On("Write", mock.MatchedBy(func(msg string) bool {
		return fmt.Sprintf("An authentication error occured while connecting to %s. Please check the remote reference credentials", testTargetNode) == msg
	})).Once()
	connectivityHelperMock.On("MarkIpFamilyError", false).Once()

	// Test unauthenticated error
	err := errors.New("authentication failed")
	tracker.updateHealth(testTargetNode, codes.Unauthenticated, err)

	// Should reset connection error count
	assert.Equal(0, tracker.getConnErrCount())

	connectivityHelperMock.AssertExpectations(t)
	uiLogMock.AssertExpectations(t)
}

func TestTargetHealthTracker_UpdateHealth_Unauthenticated_NoChange(t *testing.T) {
	tracker, connectivityHelperMock, uiLogMock := createTestTargetHealthTracker()

	// Set up mocks for unauthenticated case with no state change
	connectivityHelperMock.On("MarkNode", testTargetNode, metadata.ConnAuthErr).Return(false, false).Once()
	// No uiLog.Write expected since changed=false
	connectivityHelperMock.On("MarkIpFamilyError", false).Once()

	// Test unauthenticated error with no state change
	err := errors.New("authentication failed")
	tracker.updateHealth(testTargetNode, codes.Unauthenticated, err)

	connectivityHelperMock.AssertExpectations(t)
	uiLogMock.AssertExpectations(t)
}

func TestTargetHealthTracker_UpdateHealth_OK(t *testing.T) {
	assert := assert.New(t)
	tracker, connectivityHelperMock, uiLogMock := createTestTargetHealthTracker()

	// Set up initial error count
	tracker.incrementConnErrCount()
	tracker.incrementConnErrCount()
	tracker.incrementConnErrCount()
	assert.Equal(3, tracker.getConnErrCount())

	// Set configuration changed flag
	tracker.mutex.Lock()
	tracker.configurationChanged = false
	tracker.mutex.Unlock()

	// Set up mocks for OK case with auth error fixed
	connectivityHelperMock.On("MarkNode", testTargetNode, metadata.ConnValid).Return(true, true).Once()
	uiLogMock.On("Write", mock.MatchedBy(func(msg string) bool {
		return fmt.Sprintf("The remote reference credentials to %s have now been fixed", testTargetNode) == msg
	})).Once()
	connectivityHelperMock.On("MarkIpFamilyError", false).Once()

	// Test OK status
	tracker.updateHealth(testTargetNode, codes.OK, nil)

	// Should reset connection error count and set configuration changed
	assert.Equal(0, tracker.getConnErrCount())

	tracker.mutex.RLock()
	assert.True(tracker.configurationChanged)
	tracker.mutex.RUnlock()

	connectivityHelperMock.AssertExpectations(t)
	uiLogMock.AssertExpectations(t)
}

func TestTargetHealthTracker_UpdateHealth_OK_NoAuthErrorFixed(t *testing.T) {
	assert := assert.New(t)
	tracker, connectivityHelperMock, uiLogMock := createTestTargetHealthTracker()

	// Set up mocks for OK case with no auth error fixed
	connectivityHelperMock.On("MarkNode", testTargetNode, metadata.ConnValid).Return(false, false).Once()
	// No uiLog.Write expected since authErrFixed=false
	connectivityHelperMock.On("MarkIpFamilyError", false).Once()
	// Test OK status
	tracker.updateHealth(testTargetNode, codes.OK, nil)

	// Configuration changed should remain false
	tracker.mutex.RLock()
	assert.False(tracker.configurationChanged)
	tracker.mutex.RUnlock()

	connectivityHelperMock.AssertExpectations(t)
	uiLogMock.AssertExpectations(t)
}

func TestTargetHealthTracker_UpdateHealth_ConnectionError(t *testing.T) {
	assert := assert.New(t)
	tracker, connectivityHelperMock, _ := createTestTargetHealthTracker()

	// Initial count should be 0
	assert.Equal(0, tracker.getConnErrCount())

	// Set up mocks for connection error
	connectivityHelperMock.On("MarkNode", testTargetNode, metadata.ConnError).Return(false, false).Once()
	connectivityHelperMock.On("MarkIpFamilyError", false).Once()

	// Test connection error (any code other than OK or Unauthenticated)
	err := errors.New("connection refused")
	tracker.updateHealth(testTargetNode, codes.Unavailable, err)

	// Should increment connection error count
	assert.Equal(1, tracker.getConnErrCount())

	connectivityHelperMock.AssertExpectations(t)
}

func TestTargetHealthTracker_UpdateHealth_ConnectionError_WithIpFamilyError(t *testing.T) {
	assert := assert.New(t)
	tracker, connectivityHelperMock, _ := createTestTargetHealthTracker()

	// Set up mocks for connection error with IP family error
	connectivityHelperMock.On("MarkNode", testTargetNode, metadata.ConnError).Return(false, false).Once()
	connectivityHelperMock.On("MarkIpFamilyError", true).Once()

	// Test connection error with IP family error message
	err := errors.New("connect: " + base.IpFamilyOnlyErrorMessage + "additional details")
	tracker.updateHealth(testTargetNode, codes.FailedPrecondition, err)

	// Should increment connection error count and mark IP family error
	assert.Equal(1, tracker.getConnErrCount())

	connectivityHelperMock.AssertExpectations(t)
}

func TestTargetHealthTracker_UpdateHealth_ConnectionError_NilError(t *testing.T) {
	assert := assert.New(t)
	tracker, connectivityHelperMock, _ := createTestTargetHealthTracker()

	// Set up mocks for connection error with nil error
	connectivityHelperMock.On("MarkNode", testTargetNode, metadata.ConnError).Return(false, false).Once()
	connectivityHelperMock.On("MarkIpFamilyError", false).Once()

	// Test connection error with nil error
	tracker.updateHealth(testTargetNode, codes.Internal, nil)

	// Should increment connection error count
	assert.Equal(1, tracker.getConnErrCount())

	connectivityHelperMock.AssertExpectations(t)
}

func TestTargetHealthTracker_UpdateHealth_MultipleConnectionErrors(t *testing.T) {
	assert := assert.New(t)
	tracker, connectivityHelperMock, _ := createTestTargetHealthTracker()

	// Set up mocks for multiple connection errors
	connectivityHelperMock.On("MarkNode", testTargetNode, metadata.ConnError).Return(false, false).Times(3)
	connectivityHelperMock.On("MarkIpFamilyError", false).Times(3)

	// Test multiple connection errors
	for i := 0; i < 3; i++ {
		err := fmt.Errorf("connection error %d", i)
		tracker.updateHealth(testTargetNode, codes.Aborted, err)
	}

	// Should have incremented error count 3 times
	assert.Equal(3, tracker.getConnErrCount())

	connectivityHelperMock.AssertExpectations(t)
}

// ============= Test Cases for RemoteCngAgent Configuration Methods =============

func TestRemoteCngAgent_ConfigurationHasChanged_True(t *testing.T) {
	assert := assert.New(t)
	agent, _, _ := createTestRemoteCngAgentWithHealthTracker()

	// Set configuration changed to true
	agent.healthTracker.mutex.Lock()
	agent.healthTracker.configurationChanged = true
	agent.healthTracker.mutex.Unlock()

	assert.True(agent.ConfigurationHasChanged())
}

func TestRemoteCngAgent_ConfigurationHasChanged_False(t *testing.T) {
	assert := assert.New(t)
	agent, _, _ := createTestRemoteCngAgentWithHealthTracker()

	// Configuration changed should be false by default
	assert.False(agent.ConfigurationHasChanged())
}

func TestRemoteCngAgent_ResetConfigChangeState(t *testing.T) {
	assert := assert.New(t)
	agent, _, _ := createTestRemoteCngAgentWithHealthTracker()

	// Set configuration changed to true first
	agent.healthTracker.mutex.Lock()
	agent.healthTracker.configurationChanged = true
	agent.healthTracker.mutex.Unlock()

	assert.True(agent.ConfigurationHasChanged())

	// Reset should set it to false
	agent.ResetConfigChangeState()
	assert.False(agent.ConfigurationHasChanged())
}

// ============= Test Cases for RemoteCngAgent GetConnectivityStatus =============

func TestRemoteCngAgent_GetConnectivityStatus_ValidConnection(t *testing.T) {
	assert := assert.New(t)
	agent, connectivityHelperMock, _ := createTestRemoteCngAgentWithHealthTracker()

	// Mock hostname
	hostname := testHostname
	agent.refCache.mutex.Lock()
	agent.refCache.reference.SetActiveHostName(hostname)
	agent.refCache.mutex.Unlock()

	// Set up mock to return ConnValid
	connectivityHelperMock.On("GetNodeStatus", hostname).Return(metadata.ConnValid).Once()

	status := agent.GetConnectivityStatus()
	assert.Equal(metadata.ConnValid, status)

	connectivityHelperMock.AssertExpectations(t)
}

func TestRemoteCngAgent_GetConnectivityStatus_AuthError(t *testing.T) {
	assert := assert.New(t)
	agent, connectivityHelperMock, _ := createTestRemoteCngAgentWithHealthTracker()

	// Mock hostname
	hostname := testHostname
	agent.refCache.mutex.Lock()
	agent.refCache.reference.SetActiveHostName(hostname)
	agent.refCache.mutex.Unlock()

	// Set up mock to return ConnAuthErr
	connectivityHelperMock.On("GetNodeStatus", hostname).Return(metadata.ConnAuthErr).Once()

	status := agent.GetConnectivityStatus()
	assert.Equal(metadata.ConnAuthErr, status)

	connectivityHelperMock.AssertExpectations(t)
}

func TestRemoteCngAgent_GetConnectivityStatus_DegradedConnection(t *testing.T) {
	assert := assert.New(t)
	agent, connectivityHelperMock, _ := createTestRemoteCngAgentWithHealthTracker()

	// Mock hostname
	hostname := testHostname
	agent.refCache.mutex.Lock()
	agent.refCache.reference.SetActiveHostName(hostname)
	agent.refCache.mutex.Unlock()

	// Set connection error count below threshold
	for i := 0; i < base.MaxAllowedRCDegradedCyclesForCng-1; i++ {
		agent.healthTracker.incrementConnErrCount()
	}

	// Set up mock to return ConnError (which will be converted to ConnDegraded)
	connectivityHelperMock.On("GetNodeStatus", hostname).Return(metadata.ConnError).Once()

	status := agent.GetConnectivityStatus()
	assert.Equal(metadata.ConnDegraded, status)

	connectivityHelperMock.AssertExpectations(t)
}

func TestRemoteCngAgent_GetConnectivityStatus_ErrorConnection(t *testing.T) {
	assert := assert.New(t)
	agent, connectivityHelperMock, _ := createTestRemoteCngAgentWithHealthTracker()

	// Mock hostname
	hostname := testHostname
	agent.refCache.mutex.Lock()
	agent.refCache.reference.SetActiveHostName(hostname)
	agent.refCache.mutex.Unlock()

	// Set connection error count at or above threshold
	for i := 0; i < base.MaxAllowedRCDegradedCyclesForCng; i++ {
		agent.healthTracker.incrementConnErrCount()
	}

	// Set up mock to return ConnError
	connectivityHelperMock.On("GetNodeStatus", hostname).Return(metadata.ConnError).Once()

	status := agent.GetConnectivityStatus()
	assert.Equal(metadata.ConnError, status)

	connectivityHelperMock.AssertExpectations(t)
}

func TestRemoteCngAgent_GetConnectivityStatus_ErrorConnection_ExceedsThreshold(t *testing.T) {
	assert := assert.New(t)
	agent, connectivityHelperMock, _ := createTestRemoteCngAgentWithHealthTracker()

	// Mock hostname
	hostname := testHostname
	agent.refCache.mutex.Lock()
	agent.refCache.reference.SetActiveHostName(hostname)
	agent.refCache.mutex.Unlock()

	// Set connection error count well above threshold
	for i := 0; i < base.MaxAllowedRCDegradedCyclesForCng+5; i++ {
		agent.healthTracker.incrementConnErrCount()
	}

	// Set up mock to return ConnError
	connectivityHelperMock.On("GetNodeStatus", hostname).Return(metadata.ConnError).Once()

	status := agent.GetConnectivityStatus()
	assert.Equal(metadata.ConnError, status)

	connectivityHelperMock.AssertExpectations(t)
}

// ============= Test Cases for RemoteCngAgent GetUnreportedAuthError =============

func TestRemoteCngAgent_GetUnreportedAuthError_ValidConnection_ResetReportedStatus(t *testing.T) {
	assert := assert.New(t)
	agent, connectivityHelperMock, _ := createTestRemoteCngAgentWithHealthTracker()

	// Set initial auth error status to reported
	agent.healthTracker.setAuthErrorReportStatus(metadata_svc.AuthErrReported)

	// Set up mock to return ConnValid
	connectivityHelperMock.On("GetNodeStatus", testHostname).Return(metadata.ConnValid).Once()

	result := agent.GetUnreportedAuthError()
	assert.False(result)

	// Status should be reset to not reported
	assert.Equal(metadata_svc.AuthErrNotReported, agent.healthTracker.getAuthErrorReportStatus())

	connectivityHelperMock.AssertExpectations(t)
}

func TestRemoteCngAgent_GetUnreportedAuthError_ValidConnection_AlreadyNotReported(t *testing.T) {
	assert := assert.New(t)
	agent, connectivityHelperMock, _ := createTestRemoteCngAgentWithHealthTracker()

	// Initial auth error status should be not reported by default
	assert.Equal(metadata_svc.AuthErrNotReported, agent.healthTracker.getAuthErrorReportStatus())

	// Set up mock to return ConnValid
	connectivityHelperMock.On("GetNodeStatus", testHostname).Return(metadata.ConnValid).Once()

	result := agent.GetUnreportedAuthError()
	assert.False(result)

	// Status should remain not reported
	assert.Equal(metadata_svc.AuthErrNotReported, agent.healthTracker.getAuthErrorReportStatus())

	connectivityHelperMock.AssertExpectations(t)
}

func TestRemoteCngAgent_GetUnreportedAuthError_AuthError_FirstTime(t *testing.T) {
	assert := assert.New(t)
	agent, connectivityHelperMock, _ := createTestRemoteCngAgentWithHealthTracker()

	// Initial auth error status should be not reported by default
	assert.Equal(metadata_svc.AuthErrNotReported, agent.healthTracker.getAuthErrorReportStatus())

	// Set up mock to return ConnAuthErr
	connectivityHelperMock.On("GetNodeStatus", testHostname).Return(metadata.ConnAuthErr).Once()

	result := agent.GetUnreportedAuthError()
	assert.True(result) // Should report first time

	// Status should be set to reported
	assert.Equal(metadata_svc.AuthErrReported, agent.healthTracker.getAuthErrorReportStatus())

	connectivityHelperMock.AssertExpectations(t)
}

func TestRemoteCngAgent_GetUnreportedAuthError_AuthError_AlreadyReported(t *testing.T) {
	assert := assert.New(t)
	agent, connectivityHelperMock, _ := createTestRemoteCngAgentWithHealthTracker()

	// Set auth error status to already reported
	agent.healthTracker.setAuthErrorReportStatus(metadata_svc.AuthErrReported)

	// Set up mock to return ConnAuthErr
	connectivityHelperMock.On("GetNodeStatus", testHostname).Return(metadata.ConnAuthErr).Once()

	result := agent.GetUnreportedAuthError()
	assert.False(result) // Should not report again

	// Status should remain reported
	assert.Equal(metadata_svc.AuthErrReported, agent.healthTracker.getAuthErrorReportStatus())

	connectivityHelperMock.AssertExpectations(t)
}

func TestRemoteCngAgent_GetUnreportedAuthError_DegradedConnection(t *testing.T) {
	assert := assert.New(t)
	agent, connectivityHelperMock, _ := createTestRemoteCngAgentWithHealthTracker()

	// Set up mock to return ConnDegraded
	connectivityHelperMock.On("GetNodeStatus", testHostname).Return(metadata.ConnError).Once()

	result := agent.GetUnreportedAuthError()
	assert.False(result) // Should not report for degraded connection

	// Status should remain not reported (default)
	assert.Equal(metadata_svc.AuthErrNotReported, agent.healthTracker.getAuthErrorReportStatus())

	connectivityHelperMock.AssertExpectations(t)
}

func TestRemoteCngAgent_GetUnreportedAuthError_ErrorConnection(t *testing.T) {
	assert := assert.New(t)
	agent, connectivityHelperMock, _ := createTestRemoteCngAgentWithHealthTracker()

	// Set connection error count at or above threshold
	for i := 0; i < base.MaxAllowedRCDegradedCyclesForCng; i++ {
		agent.healthTracker.incrementConnErrCount()
	}

	// Set up mock to return ConnError
	connectivityHelperMock.On("GetNodeStatus", testHostname).Return(metadata.ConnError).Once()

	result := agent.GetUnreportedAuthError()
	assert.False(result) // Should not report for general connection error

	// Status should remain not reported (default)
	assert.Equal(metadata_svc.AuthErrNotReported, agent.healthTracker.getAuthErrorReportStatus())

	connectivityHelperMock.AssertExpectations(t)
}

// ============= Integration Tests =============

func TestTargetHealthTracker_Integration_AuthErrorFlow(t *testing.T) {
	assert := assert.New(t)
	agent, connectivityHelperMock, uiLogMock := createTestRemoteCngAgentWithHealthTracker()

	hostname := testHostname
	agent.refCache.mutex.Lock()
	agent.refCache.reference.SetActiveHostName(hostname)
	agent.refCache.mutex.Unlock()

	// Simulate auth error flow
	connectivityHelperMock.On("MarkNode", hostname, metadata.ConnAuthErr).Return(true, false).Once()
	uiLogMock.On("Write", mock.MatchedBy(func(msg string) bool {
		return fmt.Sprintf("An authentication error occured while connecting to %s. Please check the remote reference credentials", hostname) == msg
	})).Once()
	connectivityHelperMock.On("GetNodeStatus", hostname).Return(metadata.ConnAuthErr).Once()
	connectivityHelperMock.On("MarkIpFamilyError", false).Once()

	// Initial status should be not reported
	assert.Equal(metadata_svc.AuthErrNotReported, agent.healthTracker.getAuthErrorReportStatus())

	// Simulate auth error
	agent.healthTracker.updateHealth(hostname, codes.Unauthenticated, errors.New("auth failed"))

	// Check unreported auth error (should return true first time)
	result := agent.GetUnreportedAuthError()
	assert.True(result)
	assert.Equal(metadata_svc.AuthErrReported, agent.healthTracker.getAuthErrorReportStatus())

	connectivityHelperMock.AssertExpectations(t)
	uiLogMock.AssertExpectations(t)
}

func TestTargetHealthTracker_Integration_AuthErrorRecoveryFlow(t *testing.T) {
	assert := assert.New(t)
	agent, connectivityHelperMock, uiLogMock := createTestRemoteCngAgentWithHealthTracker()

	hostname := testHostname
	agent.refCache.mutex.Lock()
	agent.refCache.reference.SetActiveHostName(hostname)
	agent.refCache.mutex.Unlock()

	// Set auth error status to reported (simulating previous auth error)
	agent.healthTracker.setAuthErrorReportStatus(metadata_svc.AuthErrReported)

	// Simulate successful connection after auth error
	connectivityHelperMock.On("MarkNode", hostname, metadata.ConnValid).Return(true, true).Once()
	uiLogMock.On("Write", mock.MatchedBy(func(msg string) bool {
		return fmt.Sprintf("The remote reference credentials to %s have now been fixed", hostname) == msg
	})).Once()
	connectivityHelperMock.On("GetNodeStatus", hostname).Return(metadata.ConnValid).Once()
	connectivityHelperMock.On("MarkIpFamilyError", false).Once()

	// Simulate successful connection
	agent.healthTracker.updateHealth(hostname, codes.OK, nil)

	// Check unreported auth error (should return false and reset status)
	result := agent.GetUnreportedAuthError()
	assert.False(result)
	assert.Equal(metadata_svc.AuthErrNotReported, agent.healthTracker.getAuthErrorReportStatus())

	// Configuration should be marked as changed due to auth error fix
	assert.True(agent.ConfigurationHasChanged())

	connectivityHelperMock.AssertExpectations(t)
	uiLogMock.AssertExpectations(t)
}

func TestTargetHealthTracker_Integration_ConnectionErrorFlow(t *testing.T) {
	assert := assert.New(t)
	agent, connectivityHelperMock, _ := createTestRemoteCngAgentWithHealthTracker()

	hostname := testHostname
	agent.refCache.mutex.Lock()
	agent.refCache.reference.SetActiveHostName(hostname)
	agent.refCache.mutex.Unlock()

	// Simulate multiple connection errors leading to degraded status
	for i := 0; i < base.MaxAllowedRCDegradedCyclesForCng-1; i++ {
		connectivityHelperMock.On("MarkNode", hostname, metadata.ConnError).Return(false, false).Once()
		connectivityHelperMock.On("MarkIpFamilyError", false).Once()

		agent.healthTracker.updateHealth(hostname, codes.Unavailable, errors.New("connection error"))
	}

	// Should be degraded (below threshold)
	connectivityHelperMock.On("GetNodeStatus", hostname).Return(metadata.ConnError).Once()
	status := agent.GetConnectivityStatus()
	assert.Equal(metadata.ConnDegraded, status)

	// One more error should push it to error status
	connectivityHelperMock.On("MarkNode", hostname, metadata.ConnError).Return(false, false).Once()
	connectivityHelperMock.On("MarkIpFamilyError", false).Once()
	agent.healthTracker.updateHealth(hostname, codes.Unavailable, errors.New("connection error"))

	// Should be error (at threshold)
	connectivityHelperMock.On("GetNodeStatus", hostname).Return(metadata.ConnError).Once()
	status = agent.GetConnectivityStatus()
	assert.Equal(metadata.ConnError, status)

	connectivityHelperMock.AssertExpectations(t)
}
