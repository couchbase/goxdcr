// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package replication_manager

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/metadata_svc"
	rmMocks "github.com/couchbase/goxdcr/v8/replication_manager/mocks"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/couchbase/goxdcr/v8/service_def/mocks"
	utilsMock "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// setupMockAuth configures authentication to use mock credentials that always allow access
// Returns a cleanup function that restores the original authenticator
func setupMockAuth() func() {
	// Save original cbauth.Default
	originalDefault := cbauth.Default

	// Create mock authenticator
	mockAuth := &rmMocks.Authenticator{}
	mockCreds := &rmMocks.Creds{}
	mockCreds.On("Name").Return("testuser")
	mockCreds.On("Domain").Return("local")
	mockCreds.On("IsAllowed", mock.Anything).Return(true, nil)

	mockAuth.On("AuthWebCreds", mock.Anything).Return(mockCreds, nil)

	// Replace cbauth.Default with mock
	cbauth.Default = mockAuth

	// Return cleanup function
	return func() {
		cbauth.Default = originalDefault
	}
}

// setupTestAdminport creates a minimal adminport instance for testing
func setupTestAdminport() (*Adminport, *mocks.RemoteClusterSvc, *mocks.ReplicationSpecSvc) {
	utilsIface := &utilsMock.UtilsIface{}
	remoteClusterSvc := &mocks.RemoteClusterSvc{}
	replSpecSvc := &mocks.ReplicationSpecSvc{}
	securitySvc := &mocks.SecuritySvc{}
	xdcrCompTopologySvc := &mocks.XDCRCompTopologySvc{}

	// Set up minimal mocks
	securitySvc.On("IsClusterEncryptionStrictOrAll").Return(false)
	xdcrCompTopologySvc.On("IsMyClusterEncryptionStrictOrAll").Return(false)
	xdcrCompTopologySvc.On("MyClusterCompatibility").Return(7, nil).Maybe()
	xdcrCompTopologySvc.On("IsMyClusterEnterprise").Return(true, nil).Maybe()

	adminport := NewAdminport("localhost", 9000, 9001, nil, utilsIface, nil, securitySvc, xdcrCompTopologySvc)

	return adminport, remoteClusterSvc, replSpecSvc
}

// mockReplicationManager sets up the global replication_mgr for testing
func mockReplicationManager(remoteClusterSvc service_def.RemoteClusterSvc, replSpecSvc service_def.ReplicationSpecSvc) func() {
	// Save the original services
	originalRemoteClusterSvc := replication_mgr.remote_cluster_svc
	originalReplSpecSvc := replication_mgr.repl_spec_svc
	originalEventlogSvc := replication_mgr.eventlog_svc
	originalAuditSvc := replication_mgr.audit_svc
	originalXdcrTopologySvc := replication_mgr.xdcr_topology_svc

	// Create mock services for audit and eventlog
	eventlogSvc := &mocks.EventLogSvc{}
	eventlogSvc.On("WriteEvent", service_def.DeleteRemoteClusterRefSystemEventId, mock.Anything).Return(nil)
	eventlogSvc.On("WriteEvent", service_def.DeleteReplicationSystemEventId, mock.Anything).Return(nil)
	eventlogSvc.On("WriteEvent", service_def.UpdateRemoteClusterRefSystemEventId, mock.Anything).Return(nil)
	eventlogSvc.On("WriteEvent", service_def.UpdateReplicationSettingSystemEventId, mock.Anything).Return(nil)

	auditSvc := &mocks.AuditSvc{}
	auditSvc.On("Write", uint32(service_def.DeleteRemoteClusterRefEventId), mock.Anything).Return(nil)
	auditSvc.On("Write", uint32(service_def.CancelReplicationEventId), mock.Anything).Return(nil)
	auditSvc.On("Write", uint32(service_def.UpdateRemoteClusterRefEventId), mock.Anything).Return(nil)
	auditSvc.On("Write", uint32(service_def.UpdateReplicationSettingsEventId), mock.Anything).Return(nil)

	// Create mock for XDCRCompTopologySvc
	xdcrTopologySvc := &mocks.XDCRCompTopologySvc{}
	xdcrTopologySvc.On("MyHostAddr").Return("localhost:9000", nil).Maybe()
	xdcrTopologySvc.On("MyClusterCompatibility").Return(7, nil).Maybe()
	xdcrTopologySvc.On("IsMyClusterEnterprise").Return(true, nil).Maybe()

	// Set the mock services
	replication_mgr.remote_cluster_svc = remoteClusterSvc
	replication_mgr.repl_spec_svc = replSpecSvc
	replication_mgr.eventlog_svc = eventlogSvc
	replication_mgr.audit_svc = auditSvc
	replication_mgr.xdcr_topology_svc = xdcrTopologySvc

	// Return a cleanup function that restores the original services
	return func() {
		replication_mgr.remote_cluster_svc = originalRemoteClusterSvc
		replication_mgr.repl_spec_svc = originalReplSpecSvc
		replication_mgr.eventlog_svc = originalEventlogSvc
		replication_mgr.audit_svc = originalAuditSvc
		replication_mgr.xdcr_topology_svc = originalXdcrTopologySvc
	}
}

func TestDoDeleteRemoteClusterRequest(t *testing.T) {
	tests := []struct {
		name                 string
		clusterName          string
		setupMocks           func(*mocks.RemoteClusterSvc, *mocks.ReplicationSpecSvc, string)
		expectedStatusCode   int
		expectedBodyContains []string
		expectedBodyExcludes []string
		description          string
	}{
		{
			name:        "NotFound_UnknownCluster",
			clusterName: "nonexistent-cluster",
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc, clusterName string) {
				unknownClusterError := fmt.Errorf("%v : refName - %v", metadata_svc.UnknownRemoteClusterErrorMessage, clusterName)
				remoteClusterSvc.On("RemoteClusterByRefName", clusterName, false).Return(nil, unknownClusterError)
			},
			expectedStatusCode: http.StatusNotFound,
			expectedBodyContains: []string{
				metadata_svc.UnknownRemoteClusterErrorMessage,
				"nonexistent-cluster",
			},
			description: "Should return 404 when remote cluster does not exist",
		},
		{
			name:        "Success_ClusterDeleted",
			clusterName: "test-cluster",
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc, clusterName string) {
				clusterUuid := "test-uuid-1234"
				ref, _ := metadata.NewRemoteClusterReference(clusterUuid, clusterName, "hostname:8091", "username", "password", "", false, "", nil, nil, nil, nil)
				remoteClusterSvc.On("RemoteClusterByRefName", clusterName, false).Return(ref, nil)
				replSpecSvc.On("AllReplicationSpecs").Return(map[string]*metadata.ReplicationSpecification{}, nil)
				remoteClusterSvc.On("DelRemoteCluster", clusterName).Return(ref, nil)
			},
			expectedStatusCode: http.StatusOK,
			description:        "Should return 200 when cluster is successfully deleted",
		},
		{
			name:        "BadRequest_ClusterReferencedByReplications",
			clusterName: "test-cluster",
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc, clusterName string) {
				clusterUuid := "test-uuid-1234"
				ref, _ := metadata.NewRemoteClusterReference(clusterUuid, clusterName, "hostname:8091", "username", "password", "", false, "", nil, nil, nil, nil)
				remoteClusterSvc.On("RemoteClusterByRefName", clusterName, false).Return(ref, nil)
				spec, _ := metadata.NewReplicationSpecification("srcBucket", "srcUuid", clusterUuid, "tgtBucket", "tgtUuid")
				specs := map[string]*metadata.ReplicationSpecification{spec.Id: spec}
				replSpecSvc.On("AllReplicationSpecs").Return(specs, nil)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBodyContains: []string{
				"Cannot delete remote cluster",
				"test-cluster",
			},
			description: "Should return 400 when cluster is referenced by active replications",
		},
		{
			name:        "BadRequest_OtherValidationError",
			clusterName: "test-cluster",
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc, clusterName string) {
				validationError := errors.New("invalid cluster configuration")
				remoteClusterSvc.On("RemoteClusterByRefName", clusterName, false).Return(nil, validationError)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBodyContains: []string{
				"invalid cluster configuration",
			},
			description: "Should return 400 for other validation errors (not unknown cluster)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			// Setup mock authentication that always allows access
			authCleanup := setupMockAuth()
			defer authCleanup()

			// Setup
			adminport, remoteClusterSvc, replSpecSvc := setupTestAdminport()
			cleanup := mockReplicationManager(remoteClusterSvc, replSpecSvc)
			defer cleanup()

			// Setup mocks based on test case
			tt.setupMocks(remoteClusterSvc, replSpecSvc, tt.clusterName)

			// Create HTTP request
			requestPath := fmt.Sprintf("/pools/default/remoteClusters/%s", tt.clusterName)
			req := httptest.NewRequest(http.MethodDelete, requestPath, nil)

			// Execute
			response, err := adminport.doDeleteRemoteClusterRequest(req)

			// Assertions
			assert.NoError(err, tt.description)
			assert.NotNil(response, tt.description)
			assert.Equal(tt.expectedStatusCode, response.StatusCode, tt.description)

			// Verify body contains expected strings
			responseBody := string(response.Body)
			for _, expectedStr := range tt.expectedBodyContains {
				assert.Contains(responseBody, expectedStr, "Response body should contain: %s", expectedStr)
			}

			// Verify body excludes certain strings if specified
			for _, excludedStr := range tt.expectedBodyExcludes {
				assert.NotContains(responseBody, excludedStr, "Response body should not contain: %s", excludedStr)
			}

			// Verify mock expectations
			remoteClusterSvc.AssertExpectations(t)
			replSpecSvc.AssertExpectations(t)

			// Give goroutines time to complete (for audit/eventlog writes)
			time.Sleep(10 * time.Millisecond)
		})
	}
}

func TestUnknownClusterError_Detection(t *testing.T) {
	assert := assert.New(t)

	// Test that UnknownClusterError correctly identifies unknown cluster errors
	unknownErr := fmt.Errorf("%v : refName - someCluster", metadata_svc.UnknownRemoteClusterErrorMessage)
	assert.True(metadata_svc.UnknownClusterError(unknownErr), "Should detect unknown cluster error")

	// Test that it doesn't false-positive on other errors
	otherErr := errors.New("some other error message")
	assert.False(metadata_svc.UnknownClusterError(otherErr), "Should not detect other errors as unknown cluster")

	// Test nil error
	assert.False(metadata_svc.UnknownClusterError(nil), "Should handle nil error")
}

func TestDoDeleteReplicationRequest(t *testing.T) {
	tests := []struct {
		name                 string
		replicationId        string
		setupMocks           func(*mocks.RemoteClusterSvc, *mocks.ReplicationSpecSvc, string)
		expectedStatusCode   int
		expectedBodyContains []string
		description          string
	}{
		{
			name:          "NotFound_ReplicationDoesNotExist",
			replicationId: "target-cluster-uuid/source-bucket/target-bucket",
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc, replId string) {
				// Mock DelReplicationSpec to return ReplNotFoundErr
				replSpecSvc.On("DelReplicationSpec", replId).Return(nil, errors.New("requested resource not found"))
			},
			expectedStatusCode: http.StatusNotFound,
			expectedBodyContains: []string{
				"requested resource not found",
			},
			description: "Should return 404 when replication does not exist",
		},
		{
			name:          "Success_ReplicationDeleted",
			replicationId: "target-cluster-uuid/source-bucket/target-bucket",
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc, replId string) {
				spec, _ := metadata.NewReplicationSpecification("source-bucket", "srcUuid", "target-cluster-uuid", "target-bucket", "tgtClusterUuid")
				spec.Id = replId
				replSpecSvc.On("DelReplicationSpec", replId).Return(spec, nil)
				// Mock for the goroutine that writes audit/event logs
				remoteClusterSvc.On("GetRemoteClusterNameFromClusterUuid", mock.Anything).Return("test-remote-cluster").Maybe()
			},
			expectedStatusCode: http.StatusOK,
			description:        "Should return 200 when replication is successfully deleted",
		},
		{
			name:          "BadRequest_OtherDeletionError",
			replicationId: "target-cluster-uuid/source-bucket/target-bucket",
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc, replId string) {
				// Mock DelReplicationSpec to return a validation error
				deletionError := errors.New("replication is still active and cannot be deleted")
				replSpecSvc.On("DelReplicationSpec", replId).Return(nil, deletionError)
				replSpecSvc.On("IsReplicationValidationError", deletionError).Return(true)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBodyContains: []string{
				"replication is still active",
			},
			description: "Should return 400 for validation errors during deletion",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			// Setup mock authentication
			authCleanup := setupMockAuth()
			defer authCleanup()

			// Setup
			adminport, remoteClusterSvc, replSpecSvc := setupTestAdminport()
			cleanup := mockReplicationManager(remoteClusterSvc, replSpecSvc)
			defer cleanup()

			// Setup mocks
			tt.setupMocks(remoteClusterSvc, replSpecSvc, tt.replicationId)

			// Create HTTP request
			requestPath := fmt.Sprintf("/settings/replications/%s", tt.replicationId)
			req := httptest.NewRequest(http.MethodDelete, requestPath, nil)

			// Execute
			response, err := adminport.doDeleteReplicationRequest(req)

			// Assertions
			assert.NoError(err, tt.description)
			assert.NotNil(response, tt.description)
			assert.Equal(tt.expectedStatusCode, response.StatusCode, tt.description)

			// Verify body contains expected strings
			responseBody := string(response.Body)
			for _, expectedStr := range tt.expectedBodyContains {
				assert.Contains(responseBody, expectedStr, "Response body should contain: %s", expectedStr)
			}

			// Verify mock expectations
			replSpecSvc.AssertExpectations(t)

			// Give goroutines time to complete
			time.Sleep(10 * time.Millisecond)
		})
	}
}

func TestDoViewReplicationSettingsRequest(t *testing.T) {
	tests := []struct {
		name                 string
		replicationId        string
		setupMocks           func(*mocks.RemoteClusterSvc, *mocks.ReplicationSpecSvc, string)
		expectedStatusCode   int
		expectedBodyContains []string
		description          string
	}{
		{
			name:          "NotFound_ReplicationDoesNotExist",
			replicationId: "target-cluster-uuid/source-bucket/target-bucket",
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc, replId string) {
				// Mock ReplicationSpec to return ReplNotFoundErr
				replSpecSvc.On("ReplicationSpec", replId).Return(nil, errors.New("requested resource not found"))
			},
			expectedStatusCode: http.StatusNotFound,
			expectedBodyContains: []string{
				"requested resource not found",
			},
			description: "Should return 404 when replication does not exist",
		},
		{
			name:          "Success_ReplicationSettingsRetrieved",
			replicationId: "target-cluster-uuid/source-bucket/target-bucket",
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc, replId string) {
				spec, _ := metadata.NewReplicationSpecification("source-bucket", "srcUuid", "target-cluster-uuid", "target-bucket", "tgtClusterUuid")
				spec.Id = replId
				replSpecSvc.On("ReplicationSpec", replId).Return(spec, nil)
			},
			expectedStatusCode: http.StatusOK,
			description:        "Should return 200 when replication settings are successfully retrieved",
		},
		{
			name:          "BadRequest_OtherRetrievalError",
			replicationId: "target-cluster-uuid/source-bucket/target-bucket",
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc, replId string) {
				// Mock ReplicationSpec to return a validation error
				retrievalError := errors.New("database connection error")
				replSpecSvc.On("ReplicationSpec", replId).Return(nil, retrievalError)
				replSpecSvc.On("IsReplicationValidationError", retrievalError).Return(true)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBodyContains: []string{
				"database connection error",
			},
			description: "Should return 400 for validation errors during retrieval",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			// Setup mock authentication
			authCleanup := setupMockAuth()
			defer authCleanup()

			// Setup
			adminport, remoteClusterSvc, replSpecSvc := setupTestAdminport()
			cleanup := mockReplicationManager(remoteClusterSvc, replSpecSvc)
			defer cleanup()

			// Setup mocks
			tt.setupMocks(remoteClusterSvc, replSpecSvc, tt.replicationId)

			// Create HTTP request
			requestPath := fmt.Sprintf("/settings/replications/%s", tt.replicationId)
			req := httptest.NewRequest(http.MethodGet, requestPath, nil)

			// Execute
			response, err := adminport.doViewReplicationSettingsRequest(req)

			// Assertions
			assert.NoError(err, tt.description)
			assert.NotNil(response, tt.description)
			assert.Equal(tt.expectedStatusCode, response.StatusCode, tt.description)

			// Verify body contains expected strings
			responseBody := string(response.Body)
			for _, expectedStr := range tt.expectedBodyContains {
				assert.Contains(responseBody, expectedStr, "Response body should contain: %s", expectedStr)
			}

			// Verify mock expectations
			replSpecSvc.AssertExpectations(t)

			// Give goroutines time to complete
			time.Sleep(10 * time.Millisecond)
		})
	}
}

func TestDoGetRemoteClustersRequest(t *testing.T) {
	tests := []struct {
		name                 string
		queryParams          map[string]string // Query parameters to add to request
		setupMocks           func(*mocks.RemoteClusterSvc, *mocks.ReplicationSpecSvc)
		expectedStatusCode   int
		expectedBodyContains []string
		description          string
	}{
		{
			name: "NotFound_UnknownClusterByUuid",
			queryParams: map[string]string{
				"remoteBucketManifest": "nonexistent-uuid/test-bucket",
			},
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc) {
				unknownClusterError := fmt.Errorf("%v : uuid - %v", metadata_svc.UnknownRemoteClusterErrorMessage, "nonexistent-uuid")
				remoteClusterSvc.On("RemoteClusterByUuid", "nonexistent-uuid", false).Return(nil, unknownClusterError)
				// UnknownClusterError is considered a validation error that will be encoded as 400 BadRequest
				remoteClusterSvc.On("CheckAndUnwrapRemoteClusterError", unknownClusterError).Return(true, unknownClusterError)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBodyContains: []string{
				metadata_svc.UnknownRemoteClusterErrorMessage,
			},
			description: "Should return 400 when remote cluster UUID does not exist (validation error)",
		},
		{
			name:        "Success_AllClusters",
			queryParams: map[string]string{},
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc) {
				clusters := make(map[string]*metadata.RemoteClusterReference)
				remoteClusterSvc.On("RemoteClusters").Return(clusters, nil)
			},
			expectedStatusCode: http.StatusOK,
			description:        "Should return 200 when getting all clusters",
		},
		{
			name: "BadRequest_ManifestRetrievalError",
			queryParams: map[string]string{
				"remoteBucketManifest": "test-uuid/test-bucket",
			},
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc) {
				ref, _ := metadata.NewRemoteClusterReference("test-uuid", "test-cluster", "hostname:8091", "username", "password", "", false, "", nil, nil, nil, nil)
				remoteClusterSvc.On("RemoteClusterByUuid", "test-uuid", false).Return(ref, nil)
				// Mock manifest retrieval to return a validation error
				manifestError := errors.New("failed to retrieve bucket manifest")
				remoteClusterSvc.On("GetManifestByUuid", "test-uuid", "test-bucket", false, true).Return(nil, manifestError)
				remoteClusterSvc.On("CheckAndUnwrapRemoteClusterError", manifestError).Return(true, manifestError)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBodyContains: []string{
				"failed to retrieve bucket manifest",
			},
			description: "Should return 400 when manifest retrieval fails with validation error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			// Setup mock authentication
			authCleanup := setupMockAuth()
			defer authCleanup()

			// Setup
			adminport, remoteClusterSvc, replSpecSvc := setupTestAdminport()
			cleanup := mockReplicationManager(remoteClusterSvc, replSpecSvc)
			defer cleanup()

			// Setup mocks
			tt.setupMocks(remoteClusterSvc, replSpecSvc)

			// Create HTTP request with query parameters
			requestPath := "/pools/default/remoteClusters"
			if len(tt.queryParams) > 0 {
				query := url.Values{}
				for key, value := range tt.queryParams {
					query.Add(key, value)
				}
				requestPath = requestPath + "?" + query.Encode()
			}
			req := httptest.NewRequest(http.MethodGet, requestPath, nil)

			// Execute
			response, err := adminport.doGetRemoteClustersRequest(req)

			// Assertions
			assert.NoError(err, tt.description)
			assert.NotNil(response, tt.description)
			assert.Equal(tt.expectedStatusCode, response.StatusCode, tt.description)

			// Verify body contains expected strings
			responseBody := string(response.Body)
			for _, expectedStr := range tt.expectedBodyContains {
				assert.Contains(responseBody, expectedStr, "Response body should contain: %s", expectedStr)
			}

			// Verify mock expectations
			remoteClusterSvc.AssertExpectations(t)
		})
	}
}

func TestDoChangeRemoteClusterRequest(t *testing.T) {
	tests := []struct {
		name                 string
		clusterName          string
		requestBody          string
		setupMocks           func(*mocks.RemoteClusterSvc, *mocks.ReplicationSpecSvc, string)
		expectedStatusCode   int
		expectedBodyContains []string
		description          string
	}{
		{
			name:        "NotFound_UnknownCluster",
			clusterName: "nonexistent-cluster",
			requestBody: "name=nonexistent-cluster&hostname=remote.example.com&username=admin&password=password",
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc, clusterName string) {
				unknownClusterError := fmt.Errorf("%v : refName - %v", metadata_svc.UnknownRemoteClusterErrorMessage, clusterName)
				remoteClusterSvc.On("SetRemoteCluster", clusterName, mock.Anything).Return(unknownClusterError)
			},
			expectedStatusCode: http.StatusNotFound,
			expectedBodyContains: []string{
				metadata_svc.UnknownRemoteClusterErrorMessage,
			},
			description: "Should return 404 when trying to update non-existent remote cluster",
		},
		{
			name:        "Success_ClusterUpdated",
			clusterName: "test-cluster",
			requestBody: "name=test-cluster&hostname=remote.example.com&username=admin&password=password",
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc, clusterName string) {
				remoteClusterSvc.On("SetRemoteCluster", clusterName, mock.Anything).Return(nil)
			},
			expectedStatusCode: http.StatusOK,
			description:        "Should return 200 when cluster is successfully updated",
		},
		{
			name:        "BadRequest_InvalidConfiguration",
			clusterName: "test-cluster",
			requestBody: "name=test-cluster&hostname=remote.example.com&username=admin&password=password",
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc, clusterName string) {
				// Mock SetRemoteCluster to return a validation error
				configError := errors.New("invalid hostname format")
				remoteClusterSvc.On("SetRemoteCluster", clusterName, mock.Anything).Return(configError)
				remoteClusterSvc.On("CheckAndUnwrapRemoteClusterError", configError).Return(true, configError)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBodyContains: []string{
				"invalid hostname format",
			},
			description: "Should return 400 for validation errors during update",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			// Setup mock authentication
			authCleanup := setupMockAuth()
			defer authCleanup()

			// Setup
			adminport, remoteClusterSvc, replSpecSvc := setupTestAdminport()
			cleanup := mockReplicationManager(remoteClusterSvc, replSpecSvc)
			defer cleanup()

			// Setup mocks
			tt.setupMocks(remoteClusterSvc, replSpecSvc, tt.clusterName)

			// Create HTTP request with form data
			requestPath := fmt.Sprintf("/pools/default/remoteClusters/%s", tt.clusterName)
			req := httptest.NewRequest(http.MethodPost, requestPath, strings.NewReader(tt.requestBody))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			// Execute
			response, err := adminport.doChangeRemoteClusterRequest(req)

			// Assertions
			assert.NoError(err, tt.description)
			assert.NotNil(response, tt.description)
			assert.Equal(tt.expectedStatusCode, response.StatusCode, tt.description)

			// Verify body contains expected strings
			responseBody := string(response.Body)
			for _, expectedStr := range tt.expectedBodyContains {
				assert.Contains(responseBody, expectedStr, "Response body should contain: %s", expectedStr)
			}

			// Verify mock expectations
			remoteClusterSvc.AssertExpectations(t)

			// Give goroutines time to complete
			time.Sleep(10 * time.Millisecond)
		})
	}
}

func TestDoChangeReplicationSettingsRequest(t *testing.T) {
	tests := []struct {
		name                 string
		replicationId        string
		requestBody          string
		setupMocks           func(*mocks.RemoteClusterSvc, *mocks.ReplicationSpecSvc, string)
		expectedStatusCode   int
		expectedBodyContains []string
		description          string
	}{
		{
			name:          "NotFound_ReplicationDoesNotExist",
			replicationId: "target-cluster-uuid/source-bucket/target-bucket",
			requestBody:   "checkpointInterval=600",
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc, replId string) {
				// Mock ReplicationSpec to return ReplNotFoundErr after settings update
				replSpecSvc.On("ReplicationSpec", replId).Return(nil, errors.New("requested resource not found"))
			},
			expectedStatusCode: http.StatusNotFound,
			expectedBodyContains: []string{
				"requested resource not found",
			},
			description: "Should return 404 when replication does not exist",
		},
		{
			name:          "Success_SettingsUpdated",
			replicationId: "target-cluster-uuid/source-bucket/target-bucket",
			requestBody:   "checkpointInterval=600",
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc, replId string) {
				spec, _ := metadata.NewReplicationSpecification("source-bucket", "srcUuid", "target-cluster-uuid", "target-bucket", "tgtClusterUuid")
				spec.Id = replId

				// Mock remote cluster reference
				remoteClusterRef, _ := metadata.NewRemoteClusterReference("testClusterUUID", "target-cluster", "localhost:8091", "admin", "password", "", false, "", nil, nil, nil, nil)
				remoteClusterSvc.On("RemoteClusterByUuid", "target-cluster-uuid", false).Return(remoteClusterRef, nil)
				remoteClusterSvc.On("GetRemoteClusterNameFromClusterUuid", "target-cluster-uuid").Return("target-cluster", nil)

				replSpecSvc.On("ReplicationSpec", replId).Return(spec, nil)
				replSpecSvc.On("ValidateSpecSettings", mock.Anything).Return(nil, nil, nil)
			},
			expectedStatusCode: http.StatusOK,
			description:        "Should return 200 when settings are successfully updated",
		},
		{
			name:          "BadRequest_OtherUpdateError",
			replicationId: "target-cluster-uuid/source-bucket/target-bucket",
			requestBody:   "checkpointInterval=600",
			setupMocks: func(remoteClusterSvc *mocks.RemoteClusterSvc, replSpecSvc *mocks.ReplicationSpecSvc, replId string) {
				spec, _ := metadata.NewReplicationSpecification("source-bucket", "srcUuid", "target-cluster-uuid", "target-bucket", "tgtClusterUuid")
				spec.Id = replId

				// Mock remote cluster reference
				remoteClusterRef, _ := metadata.NewRemoteClusterReference("testClusterUUID", "target-cluster", "localhost:8091", "admin", "password", "", false, "", nil, nil, nil, nil)
				remoteClusterSvc.On("RemoteClusterByUuid", "target-cluster-uuid", false).Return(remoteClusterRef, nil)
				remoteClusterSvc.On("GetRemoteClusterNameFromClusterUuid", "target-cluster-uuid").Return("target-cluster", nil)

				// Mock validation to return an error (validation error)
				validationError := errors.New("invalid checkpoint interval value")
				errorMap := base.ErrorMap{"checkpointInterval": validationError}
				replSpecSvc.On("ReplicationSpec", replId).Return(spec, nil)
				replSpecSvc.On("ValidateSpecSettings", mock.Anything).Return(errorMap, nil)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBodyContains: []string{
				"invalid checkpoint interval value",
			},
			description: "Should return 400 for validation errors during update",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			// Setup mock authentication
			authCleanup := setupMockAuth()
			defer authCleanup()

			// Setup
			adminport, remoteClusterSvc, replSpecSvc := setupTestAdminport()
			cleanup := mockReplicationManager(remoteClusterSvc, replSpecSvc)
			defer cleanup()

			// Setup mocks
			tt.setupMocks(remoteClusterSvc, replSpecSvc, tt.replicationId)

			// Create HTTP request with form data
			requestPath := fmt.Sprintf("/settings/replications/%s", tt.replicationId)
			req := httptest.NewRequest(http.MethodPost, requestPath, strings.NewReader(tt.requestBody))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			// Execute
			response, err := adminport.doChangeReplicationSettingsRequest(req)

			// Assertions
			assert.NoError(err, tt.description)
			assert.NotNil(response, tt.description)
			assert.Equal(tt.expectedStatusCode, response.StatusCode, tt.description)

			// Verify body contains expected strings
			responseBody := string(response.Body)
			for _, expectedStr := range tt.expectedBodyContains {
				assert.Contains(responseBody, expectedStr, "Response body should contain: %s", expectedStr)
			}

			// Verify mock expectations
			replSpecSvc.AssertExpectations(t)

			// Give goroutines time to complete
			time.Sleep(10 * time.Millisecond)
		})
	}
}
