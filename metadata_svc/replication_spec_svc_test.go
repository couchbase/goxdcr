package metadata_svc

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	service_def_real "github.com/couchbase/goxdcr/service_def"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	utilsMock "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"testing"
)

func setupBoilerPlate() (*service_def.XDCRCompTopologySvc,
	*service_def.MetadataSvc,
	*service_def.UILogSvc,
	*service_def.RemoteClusterSvc,
	*service_def.ClusterInfoSvc,
	*utilsMock.UtilsIface,
	*ReplicationSpecService) {

	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	metadataSvcMock := &service_def.MetadataSvc{}
	uiLogSvcMock := &service_def.UILogSvc{}
	remoteClusterMock := &service_def.RemoteClusterSvc{}
	clusterInfoSvcMock := &service_def.ClusterInfoSvc{}
	utilitiesMock := &utilsMock.UtilsIface{}

	emptyCacheEntries := []*service_def_real.MetadataEntry{}
	replicationSpecsCatalogKey := "replicationSpec"
	metadataSvcMock.On("GetAllMetadataFromCatalog", replicationSpecsCatalogKey).Return(emptyCacheEntries, nil)

	replSpecSvc, _ := NewReplicationSpecService(uiLogSvcMock,
		remoteClusterMock,
		metadataSvcMock,
		xdcrTopologyMock,
		clusterInfoSvcMock,
		log.DefaultLoggerContext,
		utilitiesMock)

	return xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc
}

func setupMocks(srcResolutionType string,
	destResolutionType string,
	xdcrTopologyMock *service_def.XDCRCompTopologySvc,
	metadataSvcMock *service_def.MetadataSvc,
	uiLogSvcMock *service_def.UILogSvc,
	remoteClusterMock *service_def.RemoteClusterSvc,
	clusterInfoSvcMock *service_def.ClusterInfoSvc,
	utilitiesMock *utilsMock.UtilsIface,
	replSpecSvc *ReplicationSpecService) {

	// RemoteClusterMock
	mockRemoteClusterRef := &metadata.RemoteClusterReference{Uuid: "1"}
	remoteClusterMock.On("RemoteClusterByRefName", mock.Anything, mock.Anything).Return(mockRemoteClusterRef, nil)

	// Utilities mock
	var port uint16 = 9000
	hostAddr := "localhost:9000"
	utilitiesMock.On("GetHostAddr", "localhost", port).Return(hostAddr)
	myConnectionStr := utilitiesMock.GetHostAddr("localhost", port)
	xdcrTopologyMock.On("MyConnectionStr").Return(myConnectionStr, nil)

	var bucketInfo map[string]interface{}
	bucketType := base.CouchbaseBucketType
	bucketUUID := "0"
	bucketEvictionPolicy := "None"
	var bucketKVVBMap map[string][]uint16
	var err error

	// LOCAL mock
	utilitiesMock.On("BucketValidationInfo", hostAddr,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bucketInfo,
		bucketType, bucketUUID, srcResolutionType, bucketEvictionPolicy, bucketKVVBMap, err)

	// TARGET mock - emptyString since we're feeding a dummy target
	utilitiesMock.On("BucketValidationInfo", "",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bucketInfo,
		bucketType, bucketUUID, destResolutionType, bucketEvictionPolicy, bucketKVVBMap, err)

	nonExistentBucketError := errors.New("NonExistentBucketError")
	utilitiesMock.On("GetNonExistentBucketError").Return(nonExistentBucketError)

	// Pretend a major version is huge (version 2.2 or above)
	utilitiesMock.On("GetClusterCompatibilityFromBucketInfo", mock.Anything, mock.Anything,
		mock.Anything).Return(131100, nil)

	// Xmem mock
	utilitiesMock.On("CheckWhetherClusterIsESBasedOnBucketInfo", mock.Anything).Return(false)
}

/**
 * Tests the base line of xmem to xmem copy
 */
func TestValidateNewReplicationSpec(t *testing.T) {
	assert := assert.New(t)

	fmt.Println("============== Test case start: TestValidateNewReplicationSpec =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno,
		xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc)

	// Begin necessary input to validate
	sourceBucket := "testSrcBucket"
	targetBucket := "testTargetBucket"
	targetCluster := "localHost"
	settings := make(map[string]interface{})

	// Assume XMEM replication type
	settings[metadata.ReplicationType] = metadata.ReplicationTypeXmem

	_, _, _, errMap, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings)
	assert.Equal(len(errMap), 0)
	fmt.Println("============== Test case end: TestValidateNewReplicationSpec =================")
}

/**
 * Tests when the conflict resolution types are different - negative test
 */
func TestNegativeConflictResolutionType(t *testing.T) {
	assert := assert.New(t)

	fmt.Println("============== Test case start: TestNegativeConflictResolutionType =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Lww,
		xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc)

	// Begin necessary input to validate
	sourceBucket := "testSrcBucket"
	targetBucket := "testTargetBucket"
	targetCluster := "localHost"
	settings := make(map[string]interface{})

	// Assume XMEM replication type
	settings[metadata.ReplicationType] = metadata.ReplicationTypeXmem

	_, _, _, errMap, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings)
	// Should have only one error
	assert.Equal(len(errMap), 1)
	fmt.Println("============== Test case end: TestNegativeConflictResolutionType =================")
}

/**
 * MB-23968 - Tests when the conflict resolution types are different but allowed only on elastic search
 */

func TestDifferentConflictResolutionTypeOnCapi(t *testing.T) {
	assert := assert.New(t)

	fmt.Println("============== Test case start: TestDifferentConflictResolutionTypeOnCapi =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Lww,
		xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc)

	// Begin necessary input to validate
	sourceBucket := "testSrcBucket"
	targetBucket := "testTargetBucket"
	targetCluster := "localHost"
	settings := make(map[string]interface{})

	// Assume CAPI (elasticsearch) replication type
	settings[metadata.ReplicationType] = metadata.ReplicationTypeCapi

	_, _, _, errMap, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings)
	// Should pass
	assert.Equal(len(errMap), 0)
	fmt.Println("============== Test case end: TestDifferentConflictResolutionTypeOnCapi =================")
}
