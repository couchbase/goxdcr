// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package cngAgent

import (
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	serviceMocks "github.com/couchbase/goxdcr/v8/service_def/mocks"
	utilsMock "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Test constants for heartbeat tests
const (
	testSourceClusterUUID = "source-cluster-uuid-123"
	testSourceClusterName = "TestSourceCluster"
	testHostAddr          = "10.1.1.1:18091"
	testTargetConnStr     = "couchbases://target.example.com:11207"
	testRemoteClusterName = "TestRemoteCluster"
)

func createTestGrpcOpts() *base.GrpcOptions {
	return &base.GrpcOptions{
		ConnStr: testTargetConnStr,
		GetCredentials: func() *base.Credentials {
			return &base.Credentials{
				UserName_: "testuser",
				Password_: "testpass",
			}
		},
		UseTLS: false,
	}
}

// Helper functions for creating test data specific to heartbeat tests

func createTestHeartbeatManager() (*heartBeatManager, *serviceMocks.XDCRCompTopologySvc, *serviceMocks.ReplicationSpecReader, *utilsMock.UtilsIface) {
	topologyMock := &serviceMocks.XDCRCompTopologySvc{}
	specsReaderMock := &serviceMocks.ReplicationSpecReader{}
	utilsMock := &utilsMock.UtilsIface{}

	services := services{
		utils:       utilsMock,
		topologySvc: topologyMock,
	}

	logger := createTestLogger()

	hbManager := &heartBeatManager{
		remoteClusterUuid: testUuid,
		specsReader:       specsReaderMock,
		services:          services,
		logger:            logger,
	}

	return hbManager, topologyMock, specsReaderMock, utilsMock
}

func createTestHeartbeatMetadata() *metadata.HeartbeatMetadata {
	spec1 := &metadata.ReplicationSpecification{}
	spec1.Id = "spec1"

	spec2 := &metadata.ReplicationSpecification{}
	spec2.Id = "spec2"

	return &metadata.HeartbeatMetadata{
		SourceClusterUUID: testSourceClusterUUID,
		SourceClusterName: testSourceClusterName,
		SourceSpecsList:   []*metadata.ReplicationSpecification{spec1, spec2},
		NodesList:         []string{testHostAddr, "10.1.1.2:18091"},
		TTL:               30 * time.Minute,
	}
}

func createTestRemoteClusterReferenceWithConnStr() *metadata.RemoteClusterReference {
	hostAddr, _ := base.ValidateHostAddrForCbCluster(testHostname)
	ref, _ := metadata.NewRemoteClusterReference(testUuid, testRemoteClusterName, hostAddr, "", "", "", false, "", nil, nil, nil, nil)
	ref.SetId(testId)
	return ref
}

// ============= Test Cases for allowedToSendHeartbeats =============

func TestHeartBeatManager_AllowedToSendHeartbeats_HeartbeatDisabled(t *testing.T) {
	assert := assert.New(t)
	hbManager, _, _, _ := createTestHeartbeatManager()

	// Mock heartbeat disabled
	originalValue := base.SrcHeartbeatEnabled
	base.SrcHeartbeatEnabled = false
	defer func() { base.SrcHeartbeatEnabled = originalValue }()

	result := hbManager.allowedToSendHeartbeats("example.com", testUuid)
	assert.False(result)
}

func TestHeartBeatManager_AllowedToSendHeartbeats_CapellaTarget_SkipEnabled(t *testing.T) {
	assert := assert.New(t)
	hbManager, _, _, _ := createTestHeartbeatManager()

	// Mock heartbeat enabled but skip Capella targets
	originalEnabled := base.SrcHeartbeatEnabled
	originalSkipCapella := base.SrcHeartbeatSkipCapellaTarget
	base.SrcHeartbeatEnabled = true
	base.SrcHeartbeatSkipCapellaTarget = true
	defer func() {
		base.SrcHeartbeatEnabled = originalEnabled
		base.SrcHeartbeatSkipCapellaTarget = originalSkipCapella
	}()

	// Test with Capella hostname
	result := hbManager.allowedToSendHeartbeats("cluster.cloud.couchbase.com", testUuid)
	assert.False(result)
}

func TestHeartBeatManager_AllowedToSendHeartbeats_CapellaTarget_SkipDisabled(t *testing.T) {
	assert := assert.New(t)
	hbManager, _, _, _ := createTestHeartbeatManager()

	// Mock heartbeat enabled and don't skip Capella targets
	originalEnabled := base.SrcHeartbeatEnabled
	originalSkipCapella := base.SrcHeartbeatSkipCapellaTarget
	base.SrcHeartbeatEnabled = true
	base.SrcHeartbeatSkipCapellaTarget = false
	defer func() {
		base.SrcHeartbeatEnabled = originalEnabled
		base.SrcHeartbeatSkipCapellaTarget = originalSkipCapella
	}()

	// Test with Capella hostname
	result := hbManager.allowedToSendHeartbeats("cluster.cloud.couchbase.com", testUuid)
	assert.True(result)
}

func TestHeartBeatManager_AllowedToSendHeartbeats_IntraCluster_SkipDisabled(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, _, _ := createTestHeartbeatManager()

	// Mock heartbeat enabled and don't skip intra-cluster
	originalEnabled := base.SrcHeartbeatEnabled
	originalSkipIntra := base.SrcHeartbeatSkipIntraCluster
	base.SrcHeartbeatEnabled = true
	base.SrcHeartbeatSkipIntraCluster = false
	defer func() {
		base.SrcHeartbeatEnabled = originalEnabled
		base.SrcHeartbeatSkipIntraCluster = originalSkipIntra
	}()

	result := hbManager.allowedToSendHeartbeats("example.com", testUuid)
	assert.True(result)

	// Should not call topology service if intra-cluster skip is disabled
	topologyMock.AssertNotCalled(t, "MyClusterUUID")
}

func TestHeartBeatManager_AllowedToSendHeartbeats_IntraCluster_SkipEnabled_DifferentUUID(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, _, _ := createTestHeartbeatManager()

	// Mock heartbeat enabled and skip intra-cluster
	originalEnabled := base.SrcHeartbeatEnabled
	originalSkipIntra := base.SrcHeartbeatSkipIntraCluster
	base.SrcHeartbeatEnabled = true
	base.SrcHeartbeatSkipIntraCluster = true
	defer func() {
		base.SrcHeartbeatEnabled = originalEnabled
		base.SrcHeartbeatSkipIntraCluster = originalSkipIntra
	}()

	// Setup mock to return different cluster UUID
	topologyMock.On("MyClusterUUID").Return("different-uuid", nil).Once()

	result := hbManager.allowedToSendHeartbeats("example.com", testUuid)
	assert.True(result)

	topologyMock.AssertExpectations(t)
}

func TestHeartBeatManager_AllowedToSendHeartbeats_IntraCluster_SkipEnabled_SameUUID(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, _, _ := createTestHeartbeatManager()

	// Mock heartbeat enabled and skip intra-cluster
	originalEnabled := base.SrcHeartbeatEnabled
	originalSkipIntra := base.SrcHeartbeatSkipIntraCluster
	base.SrcHeartbeatEnabled = true
	base.SrcHeartbeatSkipIntraCluster = true
	defer func() {
		base.SrcHeartbeatEnabled = originalEnabled
		base.SrcHeartbeatSkipIntraCluster = originalSkipIntra
	}()

	// Setup mock to return same cluster UUID
	topologyMock.On("MyClusterUUID").Return(testUuid, nil).Once()

	result := hbManager.allowedToSendHeartbeats("example.com", testUuid)
	assert.False(result)

	topologyMock.AssertExpectations(t)
}

func TestHeartBeatManager_AllowedToSendHeartbeats_IntraCluster_TopologyError(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, _, _ := createTestHeartbeatManager()

	// Mock heartbeat enabled and skip intra-cluster
	originalEnabled := base.SrcHeartbeatEnabled
	originalSkipIntra := base.SrcHeartbeatSkipIntraCluster
	base.SrcHeartbeatEnabled = true
	base.SrcHeartbeatSkipIntraCluster = true
	defer func() {
		base.SrcHeartbeatEnabled = originalEnabled
		base.SrcHeartbeatSkipIntraCluster = originalSkipIntra
	}()

	// Setup mock to return error
	topologyMock.On("MyClusterUUID").Return("", fmt.Errorf("topology error")).Once()

	result := hbManager.allowedToSendHeartbeats("example.com", testUuid)
	assert.True(result) // Should return true if UUID comparison fails

	topologyMock.AssertExpectations(t)
}

// ============= Test Cases for specsReaderReady =============

func TestHeartBeatManager_SpecsReaderReady_Nil(t *testing.T) {
	assert := assert.New(t)
	hbManager, _, _, _ := createTestHeartbeatManager()

	// Default specsReader should be nil initially after setup
	hbManager.specsReader = nil

	result := hbManager.specsReaderReady()
	assert.False(result)
}

func TestHeartBeatManager_SpecsReaderReady_NotNil(t *testing.T) {
	assert := assert.New(t)
	hbManager, _, _, _ := createTestHeartbeatManager()

	// specsReader should be set in createTestHeartbeatManager
	result := hbManager.specsReaderReady()
	assert.True(result)
}

// ============= Test Cases for CanSendHeartbeats =============

func TestHeartBeatManager_CanSendHeartbeats_NotOrchestrator(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, _, _ := createTestHeartbeatManager()

	// Mock topology service to return that this node is not orchestrator
	topologyMock.On("IsOrchestratorNode").Return(false, nil).Once()

	result := hbManager.CanSendHeartbeats()
	assert.False(result)

	topologyMock.AssertExpectations(t)
}

func TestHeartBeatManager_CanSendHeartbeats_TopologyError(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, _, _ := createTestHeartbeatManager()

	// Mock topology service to return error
	topologyMock.On("IsOrchestratorNode").Return(false, fmt.Errorf("topology error")).Once()

	result := hbManager.CanSendHeartbeats()
	assert.False(result)

	topologyMock.AssertExpectations(t)
}

func TestHeartBeatManager_CanSendHeartbeats_SpecsReaderNotReady(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, _, _ := createTestHeartbeatManager()

	// Mock topology service to return that this node is orchestrator
	topologyMock.On("IsOrchestratorNode").Return(true, nil).Once()

	// Set specs reader to nil to make it not ready
	hbManager.specsReader = nil

	result := hbManager.CanSendHeartbeats()
	assert.False(result)

	topologyMock.AssertExpectations(t)
}

func TestHeartBeatManager_CanSendHeartbeats_Success(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, _, _ := createTestHeartbeatManager()

	// Mock topology service to return that this node is orchestrator
	topologyMock.On("IsOrchestratorNode").Return(true, nil).Once()

	// specsReader is set in createTestHeartbeatManager

	result := hbManager.CanSendHeartbeats()
	assert.True(result)

	topologyMock.AssertExpectations(t)
}

// ============= Test Cases for generateHeartbeatMetadata =============

func TestHeartBeatManager_GenerateHeartbeatMetadata_TopologyError_ClusterUUID(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, _, _ := createTestHeartbeatManager()
	ref := createTestRemoteClusterReferenceWithConnStr()

	// Mock topology service to return error for cluster UUID
	topologyMock.On("MyClusterUUID").Return("", fmt.Errorf("cluster UUID error")).Once()

	metadata, err := hbManager.generateHeartbeatMetadata(ref)
	assert.Error(err)
	assert.Nil(metadata)
	assert.Contains(err.Error(), "cluster UUID error")

	topologyMock.AssertExpectations(t)
}

func TestHeartBeatManager_GenerateHeartbeatMetadata_TopologyError_ClusterName(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, _, _ := createTestHeartbeatManager()
	ref := createTestRemoteClusterReferenceWithConnStr()

	// Mock topology service calls
	topologyMock.On("MyClusterUUID").Return(testSourceClusterUUID, nil).Once()
	topologyMock.On("MyClusterName").Return("", fmt.Errorf("cluster name error")).Once()

	metadata, err := hbManager.generateHeartbeatMetadata(ref)
	assert.Error(err)
	assert.Nil(metadata)
	assert.Contains(err.Error(), "cluster name error")

	topologyMock.AssertExpectations(t)
}

func TestHeartBeatManager_GenerateHeartbeatMetadata_EmptyClusterName(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, specsReaderMock, _ := createTestHeartbeatManager()
	ref := createTestRemoteClusterReferenceWithConnStr()

	// Create test specs
	specs := []*metadata.ReplicationSpecification{}
	specsTypeCasted := metadata.ReplSpecList(specs)

	// Mock topology service calls
	topologyMock.On("MyClusterUUID").Return(testSourceClusterUUID, nil).Once()
	topologyMock.On("MyClusterName").Return("   ", nil).Once() // Empty name with spaces
	topologyMock.On("PeerNodesAdminAddrs").Return([]string{"peer1:18091", "peer2:18091"}, nil).Once()
	topologyMock.On("MyHostAddr").Return(testHostAddr, nil).Once()

	// Mock specs reader
	specsReaderMock.On("AllReplicationSpecsWithRemote", ref).Return(specs, nil).Once()

	metadata, err := hbManager.generateHeartbeatMetadata(ref)
	assert.NoError(err)
	assert.NotNil(metadata)
	assert.Equal(testSourceClusterUUID, metadata.SourceClusterUUID)
	assert.Equal(base.UnknownSourceClusterName, metadata.SourceClusterName) // Should use unknown name
	assert.Equal(specsTypeCasted, metadata.SourceSpecsList)
	assert.Contains(metadata.NodesList, testHostAddr)
	assert.Contains(metadata.NodesList, "peer1:18091")
	assert.Contains(metadata.NodesList, "peer2:18091")

	topologyMock.AssertExpectations(t)
	specsReaderMock.AssertExpectations(t)
}

func TestHeartBeatManager_GenerateHeartbeatMetadata_SpecsError(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, specsReaderMock, _ := createTestHeartbeatManager()
	ref := createTestRemoteClusterReferenceWithConnStr()

	// Mock topology service calls
	topologyMock.On("MyClusterUUID").Return(testSourceClusterUUID, nil).Once()
	topologyMock.On("MyClusterName").Return(testSourceClusterName, nil).Once()

	// Mock specs reader to return error
	specsReaderMock.On("AllReplicationSpecsWithRemote", ref).Return(([]*metadata.ReplicationSpecification)(nil), fmt.Errorf("specs error")).Once()

	metadata, err := hbManager.generateHeartbeatMetadata(ref)
	assert.Error(err)
	assert.Nil(metadata)
	assert.Contains(err.Error(), "specs error")

	topologyMock.AssertExpectations(t)
	specsReaderMock.AssertExpectations(t)
}

func TestHeartBeatManager_GenerateHeartbeatMetadata_PeerNodesError(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, specsReaderMock, _ := createTestHeartbeatManager()
	ref := createTestRemoteClusterReferenceWithConnStr()

	// Create test specs
	specs := []*metadata.ReplicationSpecification{}

	// Mock topology service calls
	topologyMock.On("MyClusterUUID").Return(testSourceClusterUUID, nil).Once()
	topologyMock.On("MyClusterName").Return(testSourceClusterName, nil).Once()
	topologyMock.On("PeerNodesAdminAddrs").Return(([]string)(nil), fmt.Errorf("peer nodes error")).Once()

	// Mock specs reader
	specsReaderMock.On("AllReplicationSpecsWithRemote", ref).Return(specs, nil).Once()

	metadata, err := hbManager.generateHeartbeatMetadata(ref)
	assert.Error(err)
	assert.Nil(metadata)
	assert.Contains(err.Error(), "peer nodes error")

	topologyMock.AssertExpectations(t)
	specsReaderMock.AssertExpectations(t)
}

func TestHeartBeatManager_GenerateHeartbeatMetadata_MyHostAddrError(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, specsReaderMock, _ := createTestHeartbeatManager()
	ref := createTestRemoteClusterReferenceWithConnStr()

	// Create test specs
	specs := []*metadata.ReplicationSpecification{}

	// Mock topology service calls
	topologyMock.On("MyClusterUUID").Return(testSourceClusterUUID, nil).Once()
	topologyMock.On("MyClusterName").Return(testSourceClusterName, nil).Once()
	topologyMock.On("PeerNodesAdminAddrs").Return([]string{"peer1:18091"}, nil).Once()
	topologyMock.On("MyHostAddr").Return("", fmt.Errorf("host addr error")).Once()

	// Mock specs reader
	specsReaderMock.On("AllReplicationSpecsWithRemote", ref).Return(specs, nil).Once()

	metadata, err := hbManager.generateHeartbeatMetadata(ref)
	assert.Error(err)
	assert.Nil(metadata)
	assert.Contains(err.Error(), "host addr error")

	topologyMock.AssertExpectations(t)
	specsReaderMock.AssertExpectations(t)
}

func TestHeartBeatManager_GenerateHeartbeatMetadata_Success(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, specsReaderMock, _ := createTestHeartbeatManager()
	ref := createTestRemoteClusterReferenceWithConnStr()

	// Create test specs
	spec1 := &metadata.ReplicationSpecification{}
	spec1.Id = "spec1"
	spec2 := &metadata.ReplicationSpecification{}
	spec2.Id = "spec2"
	specs := []*metadata.ReplicationSpecification{spec1, spec2}
	specsTypeCasted := metadata.ReplSpecList(specs)

	// Mock original heartbeat settings
	originalMinInterval := base.SrcHeartbeatMinInterval
	originalMaxIntervalFactor := base.SrcHeartbeatMaxIntervalFactor
	defer func() {
		base.SrcHeartbeatMinInterval = originalMinInterval
		base.SrcHeartbeatMaxIntervalFactor = originalMaxIntervalFactor
	}()

	// Set test values
	base.SrcHeartbeatMinInterval = 1 * time.Minute
	base.SrcHeartbeatMaxIntervalFactor = 5

	// Mock topology service calls
	topologyMock.On("MyClusterUUID").Return(testSourceClusterUUID, nil).Once()
	topologyMock.On("MyClusterName").Return(testSourceClusterName, nil).Once()
	topologyMock.On("PeerNodesAdminAddrs").Return([]string{"peer1:18091", "peer2:18091"}, nil).Once()
	topologyMock.On("MyHostAddr").Return(testHostAddr, nil).Once()

	// Mock specs reader
	specsReaderMock.On("AllReplicationSpecsWithRemote", ref).Return(specs, nil).Once()

	metadata, err := hbManager.generateHeartbeatMetadata(ref)
	assert.NoError(err)
	assert.NotNil(metadata)

	// Verify metadata fields
	assert.Equal(testSourceClusterUUID, metadata.SourceClusterUUID)
	assert.Equal(testSourceClusterName, metadata.SourceClusterName)
	assert.Equal(specsTypeCasted, metadata.SourceSpecsList)
	assert.Len(metadata.NodesList, 3) // 2 peers + 1 local
	assert.Contains(metadata.NodesList, testHostAddr)
	assert.Contains(metadata.NodesList, "peer1:18091")
	assert.Contains(metadata.NodesList, "peer2:18091")

	expectedTTL := time.Duration(base.SrcHeartbeatExpiryFactor) * base.SrcHeartbeatMaxInterval()
	assert.Equal(expectedTTL, metadata.TTL)

	topologyMock.AssertExpectations(t)
	specsReaderMock.AssertExpectations(t)
}

// ============= Test Cases for composeHeartBeatRequest =============

func TestHeartBeatManager_ComposeHeartBeatRequest_HostAddrError(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, _, _ := createTestHeartbeatManager()
	metadata := createTestHeartbeatMetadata()

	// Mock topology service to return error
	topologyMock.On("MyHostAddr").Return("", fmt.Errorf("host addr error")).Once()

	req, err := hbManager.composeHeartBeatRequest(metadata, testTargetConnStr)
	assert.Error(err)
	assert.Nil(req)
	assert.Contains(err.Error(), "host addr error")

	topologyMock.AssertExpectations(t)
}

func TestHeartBeatManager_ComposeHeartBeatRequest_Success(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, _, _ := createTestHeartbeatManager()
	metadata := createTestHeartbeatMetadata()

	// Mock topology service
	topologyMock.On("MyHostAddr").Return(testHostAddr, nil).Once()

	req, err := hbManager.composeHeartBeatRequest(metadata, testTargetConnStr)
	assert.NoError(err)
	assert.NotNil(req)

	// Verify request fields
	assert.Equal(testHostAddr, req.Sender)
	assert.Equal(testTargetConnStr, req.TargetAddr)
	assert.Equal(metadata.SourceClusterUUID, req.SourceClusterUUID)
	assert.Equal(metadata.SourceClusterName, req.SourceClusterName)
	assert.Equal(metadata.NodesList, req.NodesList)
	assert.Equal(metadata.TTL, req.TTL)

	// Verify specs are included by serializing and checking payload is not empty
	_, err = req.Serialize()
	assert.NoError(err)

	topologyMock.AssertExpectations(t)
}

// ============= Test Cases for sendHeartbeat =============

func TestHeartBeatManager_SendHeartbeat_ComposeRequestError(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, _, _ := createTestHeartbeatManager()
	metadata := createTestHeartbeatMetadata()

	// Mock MyConnectionStr to succeed but MyHostAddr to fail during compose request
	topologyMock.On("MyHostAddr").Return("", fmt.Errorf("host addr error")).Once()

	response := hbManager.sendHeartbeat(metadata, createTestGrpcOpts)
	assert.NotNil(response)
	assert.NotEqual(codes.OK, response.Code())
	assert.Equal(codes.Unknown, response.Code())
	assert.Error(response.Error)
	assert.Contains(response.Error.Error(), "host addr error")

	topologyMock.AssertExpectations(t)
}

func TestHeartBeatManager_SendHeartbeat_Success(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, _, utilsMock := createTestHeartbeatManager()
	metadata := createTestHeartbeatMetadata()

	// Mock topology service for compose request
	topologyMock.On("MyHostAddr").Return(testHostAddr, nil).Once()

	// Create expected successful response
	successResponse := &base.GrpcResponse[*internal_xdcr_v1.HeartbeatResponse]{
		Status: status.New(codes.OK, "success"),
		Resp:   &internal_xdcr_v1.HeartbeatResponse{},
		Error:  nil,
	}

	// Mock the CngHeartbeat RPC call using mock.Run()
	utilsMock.On("CngHeartbeat", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.HeartbeatRequest]")).
		Run(func(args mock.Arguments) {
			// Verify the grpc options and request are properly formed
			client := args.Get(0).(base.CngClient)
			request := args.Get(1).(*base.GrpcRequest[*internal_xdcr_v1.HeartbeatRequest])

			// Verify grpc options contain connection string
			assert.NotNil(client)

			// Verify request contains heartbeat payload
			assert.NotNil(request)
			assert.NotNil(request.Request)
			assert.NotEmpty(request.Request.Payload)

			// Verify context has timeout
			assert.NotNil(request.Context)
			_, hasDeadline := request.Context.Deadline()
			assert.True(hasDeadline)
		}).
		Return(successResponse).
		Once()

	response := hbManager.sendHeartbeat(metadata, createTestGrpcOpts)
	assert.NotNil(response)
	assert.Equal(codes.OK, response.Code())
	assert.NoError(response.Error)

	// Verify that lastSentHeartbeatMetadata was updated on success
	hbManager.mutex.RLock()
	assert.Equal(metadata, hbManager.lastSentHeartbeatMetadata)
	hbManager.mutex.RUnlock()

	topologyMock.AssertExpectations(t)
	utilsMock.AssertExpectations(t)
}

func TestHeartBeatManager_SendHeartbeat_RPCFailure(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, _, utilsMock := createTestHeartbeatManager()
	metadata := createTestHeartbeatMetadata()

	// Mock topology service for compose request
	topologyMock.On("MyHostAddr").Return(testHostAddr, nil).Once()

	// Create expected failure response
	failureResponse := &base.GrpcResponse[*internal_xdcr_v1.HeartbeatResponse]{
		Status: status.New(codes.Unavailable, "connection failed"),
		Resp:   nil,
		Error:  fmt.Errorf("connection failed"),
	}

	// Mock the CngHeartbeat RPC call to fail
	utilsMock.On("CngHeartbeat", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.HeartbeatRequest]")).
		Run(func(args mock.Arguments) {
			// Verify the request is properly formed even when it will fail
			request := args.Get(1).(*base.GrpcRequest[*internal_xdcr_v1.HeartbeatRequest])
			assert.NotNil(request.Request.Payload)
		}).
		Return(failureResponse).
		Once()

	// Set initial lastSentHeartbeatMetadata to verify it doesn't get updated on failure
	originalMetadata := createTestHeartbeatMetadata()
	originalMetadata.SourceClusterUUID = "original"
	hbManager.mutex.Lock()
	hbManager.lastSentHeartbeatMetadata = originalMetadata
	hbManager.mutex.Unlock()

	response := hbManager.sendHeartbeat(metadata, createTestGrpcOpts)
	assert.NotNil(response)
	assert.Equal(codes.Unavailable, response.Code())
	assert.Error(response.Error)

	// Verify that lastSentHeartbeatMetadata was NOT updated on failure
	hbManager.mutex.RLock()
	assert.Equal(originalMetadata, hbManager.lastSentHeartbeatMetadata)
	assert.NotEqual(metadata, hbManager.lastSentHeartbeatMetadata)
	hbManager.mutex.RUnlock()

	topologyMock.AssertExpectations(t)
	utilsMock.AssertExpectations(t)
}

// ============= Test Cases for heartbeat statistics =============

func TestHeartbeatStats_Update_Success(t *testing.T) {
	assert := assert.New(t)
	stats := &heartbeatStats{}

	// Create successful response
	successResponse := &base.GrpcResponse[*internal_xdcr_v1.HeartbeatResponse]{
		Status: status.New(codes.OK, "success"),
		Resp:   &internal_xdcr_v1.HeartbeatResponse{},
		Error:  nil,
	}

	// Initial values should be zero
	assert.Equal(uint64(0), stats.totalHeartbeatsSent)
	assert.Equal(uint64(0), stats.successfulHeartbeats)
	assert.Equal(uint64(0), stats.connectionErrors)
	assert.Equal(uint64(0), stats.otherErrors)

	stats.update(successResponse)

	// After successful update
	assert.Equal(uint64(1), stats.totalHeartbeatsSent)
	assert.Equal(uint64(1), stats.successfulHeartbeats)
	assert.Equal(uint64(0), stats.connectionErrors)
	assert.Equal(uint64(0), stats.otherErrors)
}

func TestHeartbeatStats_Update_ConnectionErrors(t *testing.T) {
	assert := assert.New(t)
	stats := &heartbeatStats{}

	connectionErrorCodes := []codes.Code{
		codes.Unavailable,
		codes.DeadlineExceeded,
		codes.Canceled,
		codes.Unauthenticated,
		codes.PermissionDenied,
	}

	for i, errorCode := range connectionErrorCodes {
		response := &base.GrpcResponse[*internal_xdcr_v1.HeartbeatResponse]{
			Status: status.New(errorCode, "connection error"),
			Resp:   nil,
			Error:  fmt.Errorf("connection error"),
		}

		stats.update(response)

		expectedCount := uint64(i + 1)
		assert.Equal(expectedCount, stats.totalHeartbeatsSent)
		assert.Equal(uint64(0), stats.successfulHeartbeats)
		assert.Equal(expectedCount, stats.connectionErrors)
		assert.Equal(uint64(0), stats.otherErrors)
	}
}

func TestHeartbeatStats_Update_OtherErrors(t *testing.T) {
	assert := assert.New(t)
	stats := &heartbeatStats{}

	otherErrorCodes := []codes.Code{
		codes.InvalidArgument,
		codes.NotFound,
		codes.AlreadyExists,
		codes.FailedPrecondition,
		codes.Aborted,
		codes.OutOfRange,
		codes.Unimplemented,
		codes.Internal,
		codes.DataLoss,
	}

	for i, errorCode := range otherErrorCodes {
		response := &base.GrpcResponse[*internal_xdcr_v1.HeartbeatResponse]{
			Status: status.New(errorCode, "other error"),
			Resp:   nil,
			Error:  fmt.Errorf("other error"),
		}

		stats.update(response)

		expectedCount := uint64(i + 1)
		assert.Equal(expectedCount, stats.totalHeartbeatsSent)
		assert.Equal(uint64(0), stats.successfulHeartbeats)
		assert.Equal(uint64(0), stats.connectionErrors)
		assert.Equal(expectedCount, stats.otherErrors)
	}
}

func TestHeartbeatStats_Update_Mixed(t *testing.T) {
	assert := assert.New(t)
	stats := &heartbeatStats{}

	// Test sequence: Success, Connection Error, Other Error, Success
	responses := []*base.GrpcResponse[*internal_xdcr_v1.HeartbeatResponse]{
		{Status: status.New(codes.OK, "success"), Resp: &internal_xdcr_v1.HeartbeatResponse{}, Error: nil},
		{Status: status.New(codes.Unavailable, "conn error"), Resp: nil, Error: fmt.Errorf("conn error")},
		{Status: status.New(codes.Internal, "other error"), Resp: nil, Error: fmt.Errorf("other error")},
		{Status: status.New(codes.OK, "success"), Resp: &internal_xdcr_v1.HeartbeatResponse{}, Error: nil},
	}

	for _, response := range responses {
		stats.update(response)
	}

	assert.Equal(uint64(4), stats.totalHeartbeatsSent)
	assert.Equal(uint64(2), stats.successfulHeartbeats)
	assert.Equal(uint64(1), stats.connectionErrors)
	assert.Equal(uint64(1), stats.otherErrors)
}

// ============= Integration test for full heartbeat flow =============

func TestHeartBeatManager_FullHeartbeatFlow_Integration(t *testing.T) {
	assert := assert.New(t)
	hbManager, topologyMock, specsReaderMock, utilsMock := createTestHeartbeatManager()
	ref := createTestRemoteClusterReferenceWithConnStr()

	// Set up the full flow mocks

	// 1. CanSendHeartbeats checks
	topologyMock.On("IsOrchestratorNode").Return(true, nil).Once()

	// 2. generateHeartbeatMetadata mocks
	spec1 := &metadata.ReplicationSpecification{}
	spec1.Id = "integration-spec-1"
	specs := []*metadata.ReplicationSpecification{spec1}
	specsTypeCasted := metadata.ReplSpecList(specs)
	topologyMock.On("MyClusterUUID").Return(testSourceClusterUUID, nil).Once()
	topologyMock.On("MyClusterName").Return(testSourceClusterName, nil).Once()
	topologyMock.On("PeerNodesAdminAddrs").Return([]string{"peer1:18091"}, nil).Once()
	topologyMock.On("MyHostAddr").Return(testHostAddr, nil).Times(2) // Once for metadata generation, once for request composition

	specsReaderMock.On("AllReplicationSpecsWithRemote", ref).Return(specs, nil).Once()

	// 3. sendHeartbeat mocks
	successResponse := &base.GrpcResponse[*internal_xdcr_v1.HeartbeatResponse]{
		Status: status.New(codes.OK, "integration test success"),
		Resp:   &internal_xdcr_v1.HeartbeatResponse{},
		Error:  nil,
	}

	utilsMock.On("CngHeartbeat", mock.AnythingOfType("*internal_xdcr_v1.xdcrServiceClient"), mock.AnythingOfType("*base.GrpcRequest[*github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1.HeartbeatRequest]")).
		Run(func(args mock.Arguments) {
			request := args.Get(1).(*base.GrpcRequest[*internal_xdcr_v1.HeartbeatRequest])
			assert.NotNil(request.Request.Payload)
		}).
		Return(successResponse).
		Once()

	// Execute the full flow

	// 1. Check if can send heartbeats
	canSend := hbManager.CanSendHeartbeats()
	assert.True(canSend)

	// 2. Generate heartbeat metadata
	metadata, err := hbManager.generateHeartbeatMetadata(ref)
	assert.NoError(err)
	assert.NotNil(metadata)
	assert.Equal(testSourceClusterUUID, metadata.SourceClusterUUID)
	assert.Equal(testSourceClusterName, metadata.SourceClusterName)
	assert.Equal(specsTypeCasted, metadata.SourceSpecsList)

	// 3. Send heartbeat
	response := hbManager.sendHeartbeat(metadata, createTestGrpcOpts)
	assert.NotNil(response)
	assert.Equal(codes.OK, response.Code())
	assert.NoError(response.Error)

	// 4. Update statistics
	hbManager.heartbeatStats.update(response)

	// Verify final state
	assert.Equal(uint64(1), hbManager.heartbeatStats.totalHeartbeatsSent)
	assert.Equal(uint64(1), hbManager.heartbeatStats.successfulHeartbeats)
	assert.Equal(uint64(0), hbManager.heartbeatStats.connectionErrors)
	assert.Equal(uint64(0), hbManager.heartbeatStats.otherErrors)

	// Verify lastSentHeartbeatMetadata was updated
	hbManager.mutex.RLock()
	assert.Equal(metadata, hbManager.lastSentHeartbeatMetadata)
	hbManager.mutex.RUnlock()

	// Verify all mocks were called as expected
	topologyMock.AssertExpectations(t)
	specsReaderMock.AssertExpectations(t)
	utilsMock.AssertExpectations(t)
}

// ============= Test Cases for SameAs method testing (if needed for metadata comparison) =============

func TestHeartbeatMetadata_SameAs(t *testing.T) {
	assert := assert.New(t)

	// Create two identical metadata objects
	metadata1 := createTestHeartbeatMetadata()
	metadata2 := createTestHeartbeatMetadata()

	// They should be the same
	assert.True(metadata1.SameAs(metadata2))
	assert.True(metadata2.SameAs(metadata1))

	// Test with different UUIDs
	metadata3 := createTestHeartbeatMetadata()
	metadata3.SourceClusterUUID = "different-uuid"
	assert.False(metadata1.SameAs(metadata3))

	// Test with different cluster names
	metadata4 := createTestHeartbeatMetadata()
	metadata4.SourceClusterName = "DifferentClusterName"
	assert.False(metadata1.SameAs(metadata4))

	// Test with nil comparison
	assert.False(metadata1.SameAs(nil))
}
