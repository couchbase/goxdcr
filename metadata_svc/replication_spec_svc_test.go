package metadata_svc

import (
	"errors"
	"fmt"
	mcMock "github.com/couchbase/gomemcached/client/mocks"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	service_def_real "github.com/couchbase/goxdcr/service_def"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	utilities "github.com/couchbase/goxdcr/utils"
	utilsMock "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	mrand "math/rand"
	"testing"
)

func setupBoilerPlate() (*service_def.XDCRCompTopologySvc,
	*service_def.MetadataSvc,
	*service_def.UILogSvc,
	*service_def.RemoteClusterSvc,
	*service_def.ClusterInfoSvc,
	*utilsMock.UtilsIface,
	*ReplicationSpecService,
	string,
	string,
	string,
	map[string]interface{},
	*mcMock.ClientIface) {

	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	metadataSvcMock := &service_def.MetadataSvc{}
	uiLogSvcMock := &service_def.UILogSvc{}
	remoteClusterMock := &service_def.RemoteClusterSvc{}
	clusterInfoSvcMock := &service_def.ClusterInfoSvc{}
	utilitiesMock := &utilsMock.UtilsIface{}
	clientMock := &mcMock.ClientIface{}

	emptyCacheEntries := []*service_def_real.MetadataEntry{}
	replicationSpecsCatalogKey := "replicationSpec"
	metadataSvcMock.On("GetAllMetadataFromCatalog", replicationSpecsCatalogKey).Return(emptyCacheEntries, nil)
	utilitiesMock.On("ExponentialBackoffExecutor", "GetAllMetadataFromCatalogReplicationSpec", mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return(nil)

	replSpecSvc, _ := NewReplicationSpecService(uiLogSvcMock,
		remoteClusterMock,
		metadataSvcMock,
		xdcrTopologyMock,
		clusterInfoSvcMock,
		log.DefaultLoggerContext,
		utilitiesMock)

	sourceBucket := "testSrcBucket"
	targetBucket := "testTargetBucket"
	targetCluster := "localHost"
	settings := make(map[string]interface{})

	return xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc, sourceBucket, targetBucket, targetCluster, settings,
		clientMock
}

func generateFakeListOfVBs(capacity int) []uint16 {
	var listOfVBs = make([]uint16, capacity, capacity)
	for i := 0; i < capacity; i++ {
		listOfVBs[i] = (uint16)(mrand.Intn(1024))
	}
	return listOfVBs
}

func setupMocks(srcResolutionType string,
	destResolutionType string,
	xdcrTopologyMock *service_def.XDCRCompTopologySvc,
	metadataSvcMock *service_def.MetadataSvc,
	uiLogSvcMock *service_def.UILogSvc,
	remoteClusterMock *service_def.RemoteClusterSvc,
	clusterInfoSvcMock *service_def.ClusterInfoSvc,
	utilitiesMock *utilsMock.UtilsIface,
	replSpecSvc *ReplicationSpecService,
	clientMock *mcMock.ClientIface,
	isEnterprise bool,
	isElasticSearch bool) {

	// RemoteClusterMock
	mockRemoteClusterRef := &metadata.RemoteClusterReference{Uuid: "1"}
	remoteClusterMock.On("RemoteClusterByRefName", mock.Anything, mock.Anything).Return(mockRemoteClusterRef, nil)

	// Utilities mock
	var port uint16 = 9000
	hostAddr := "localhost:9000"
	utilitiesMock.On("GetHostAddr", "localhost", port).Return(hostAddr)
	myConnectionStr := base.GetHostAddr("localhost", port)

	var bucketInfo map[string]interface{}
	bucketType := base.CouchbaseBucketType
	bucketUUID := "0"
	bucketEvictionPolicy := "None"
	var bucketKVVBMap map[string][]uint16 = make(map[string][]uint16)
	bucketKVVBMap["localhost"] = generateFakeListOfVBs(10)
	var err error

	// MetaKV mock
	metadataSvcMock.On("AddWithCatalog", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	metadataSvcMock.On("DelWithCatalog", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// Nil because this Get is simply to get the revision - if needed, try to link it to AddWithCatalog later
	metadataSvcMock.On("Get", mock.Anything).Return(nil, 1, nil)

	// RemoteClusterSvc mock
	remoteClusterMock.On("GetRemoteClusterNameFromClusterUuid", mock.Anything).Return("TestRemoteCluster")

	// UI Log mock service
	uiLogSvcMock.On("Write", mock.Anything).Return(nil)

	// XDCR Topology Mock
	xdcrTopologyMock.On("MyConnectionStr").Return(myConnectionStr, nil)
	xdcrTopologyMock.On("MyMemcachedAddr").Return(myConnectionStr, nil)
	xdcrTopologyMock.On("IsMyClusterEnterprise").Return(isEnterprise, nil)

	// LOCAL mock
	utilitiesMock.On("BucketValidationInfo", hostAddr,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bucketInfo,
		bucketType, bucketUUID, srcResolutionType, bucketEvictionPolicy, bucketKVVBMap, err)

	// TARGET mock - emptyString since we're feeding a dummy target
	utilitiesMock.On("RemoteBucketValidationInfo", "",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bucketInfo,
		bucketType, bucketUUID, destResolutionType, bucketEvictionPolicy, bucketKVVBMap, err)

	nonExistentBucketError := errors.New("NonExistentBucketError")
	utilitiesMock.On("GetNonExistentBucketError").Return(nonExistentBucketError)

	// Version 5.5
	utilitiesMock.On("GetClusterCompatibilityFromBucketInfo", mock.Anything, mock.Anything,
		mock.Anything).Return(0x50005, nil)

	// Xmem mock
	utilitiesMock.On("CheckWhetherClusterIsESBasedOnBucketInfo", mock.Anything).Return(isElasticSearch)

	// client mock
	clientMock.On("Close").Return(nil)

	var respondFeatures utilities.HELOFeatures
	respondFeatures.CompressionType = base.CompressionTypeSnappy

	utilitiesMock.On("GetMemcachedConnectionWFeatures", mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Return(clientMock, respondFeatures, nil)
	utilitiesMock.On("GetDefaultPoolInfoWithSecuritySettings", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false, base.ClientCertAuthDisable, nil, nil)

}

/**
 * Tests the base line of xmem to xmem copy
 */
func TestValidateNewReplicationSpec(t *testing.T) {
	assert := assert.New(t)

	fmt.Println("============== Test case start: TestValidateNewReplicationSpec =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno,
		xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc, clientMock, true, /*IsEnterprise*/
		false /*IsElastic*/)

	// Assume XMEM replication type
	settings[metadata.ReplicationType] = metadata.ReplicationTypeXmem

	_, _, _, errMap, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings)
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
		clusterInfoSvcMock, utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Lww,
		xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc, clientMock, true, /*IsEnterprise*/
		false /*IsElastic*/)

	// Assume XMEM replication type
	settings[metadata.ReplicationType] = metadata.ReplicationTypeXmem

	_, _, _, errMap, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings)
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
		clusterInfoSvcMock, utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Lww,
		xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc, clientMock, true, /*IsEnterprise*/
		false /*IsElastic*/)

	// Assume CAPI (elasticsearch) replication type
	settings[metadata.ReplicationType] = metadata.ReplicationTypeCapi

	_, _, _, errMap, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings)
	// Should pass
	assert.Equal(len(errMap), 0)
	fmt.Println("============== Test case end: TestDifferentConflictResolutionTypeOnCapi =================")
}

func TestAddReplicationSpec(t *testing.T) {
	assert := assert.New(t)

	fmt.Println("============== Test case start: TestAddReplicationSpecAndVerifyFunc =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc,
		_, _, _, _, clientMock := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Lww,
		xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc, clientMock, true, /*IsEnterprise*/
		false /*IsElastic*/)

	spec := &metadata.ReplicationSpecification{
		Id:               "test",
		InternalId:       "internalTest",
		SourceBucketName: "testSrc",
		TargetBucketName: "testTgt",
		Settings:         metadata.DefaultSettings(),
	}

	assert.Nil(replSpecSvc.AddReplicationSpec(spec, ""))
	checkRep, checkErr := replSpecSvc.ReplicationSpec(spec.Id)
	assert.Nil(checkErr)
	// We created with a 0 revision, metakv returns a 1
	assert.NotEqual(checkRep.Revision, checkRep.Settings.Revision)
	_, delErr := replSpecSvc.DelReplicationSpec(spec.Id)
	assert.Nil(delErr)
	fmt.Println("============== Test case end: TestAddReplicationSpecAndVerifyFunc =================")
}

/**
 * Compression validations - enterprise mode
 */
func TestCompressionPositive(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCompressionPositive =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno,
		xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc, clientMock, true, /*IsEnterprise*/
		false /*IsElastic*/)

	var fullFeatures utilities.HELOFeatures
	fullFeatures.Xattribute = true
	fullFeatures.CompressionType = base.CompressionTypeSnappy
	utilitiesMock.On("GetMemcachedRawConn", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(clientMock, nil)
	utilitiesMock.On("SendHELOWithFeatures", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fullFeatures, nil)

	// Turning off should be allowed
	settings[metadata.CompressionType] = base.CompressionTypeNone
	_, _, _, errMap, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings)
	assert.Equal(len(errMap), 0)

	// Turning on should be allowed
	settings[metadata.CompressionType] = base.CompressionTypeSnappy
	_, _, _, errMap, _, _ = replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings)
	assert.Equal(len(errMap), 0)

	fmt.Println("============== Test case end: TestCompressionPositive =================")
}

func TestCompressionNegNotEnterprise(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCompressionNegNotEnterprise =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno,
		xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc, clientMock, false, /*Enterprise*/
		false /*IsElastic*/)

	var fullFeatures utilities.HELOFeatures
	fullFeatures.Xattribute = true
	fullFeatures.CompressionType = base.CompressionTypeSnappy
	utilitiesMock.On("GetMemcachedRawConn", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(clientMock, nil)
	utilitiesMock.On("SendHELOWithFeatures", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fullFeatures, nil)

	// Turning on should be disallowed
	settings[metadata.CompressionType] = base.CompressionTypeSnappy
	_, _, _, errMap, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings)
	assert.NotEqual(len(errMap), 0)

	fmt.Println("============== Test case end: TestCompressionNegNotEnterprise =================")
}

func TestCompressionNegCAPI(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCompressionNegCAPI =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno,
		xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc, clientMock, false, /*Enterprise*/
		false /*IsElastic*/)

	var fullFeatures utilities.HELOFeatures
	fullFeatures.Xattribute = true
	fullFeatures.CompressionType = base.CompressionTypeSnappy
	utilitiesMock.On("GetMemcachedRawConn", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(clientMock, nil)
	utilitiesMock.On("SendHELOWithFeatures", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fullFeatures, nil)

	// Turning on should be disallowed
	settings[metadata.CompressionType] = base.CompressionTypeSnappy
	settings[metadata.ReplicationType] = metadata.ReplicationTypeCapi
	_, _, _, errMap, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings)
	assert.NotEqual(len(errMap), 0)

	fmt.Println("============== Test case end: TestCompressionNegCAPI =================")
}

func TestCompressionNegNoSnappy(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCompressionNegNoSnappy =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno,
		xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc, clientMock, true, /*IsEnterprise*/
		false /*IsElastic*/)

	var noCompressionFeature utilities.HELOFeatures
	noCompressionFeature.Xattribute = true
	noCompressionFeature.CompressionType = base.CompressionTypeNone
	utilitiesMock.On("GetMemcachedRawConn", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(clientMock, nil)
	utilitiesMock.On("SendHELOWithFeatures", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(noCompressionFeature, nil)

	// Turning off should be allowed
	settings[metadata.CompressionType] = base.CompressionTypeNone
	_, _, _, errMap, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings)
	assert.Equal(len(errMap), 0)

	// Turning on should result in error
	settings[metadata.CompressionType] = base.CompressionTypeSnappy
	_, _, _, errMap, _, _ = replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings)
	assert.NotEqual(len(errMap), 0)

	fmt.Println("============== Test case end: TestCompressionNegNoSnappy =================")
}

func TestElasticSearch(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestElasticSearch =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno,
		xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		clusterInfoSvcMock, utilitiesMock, replSpecSvc, clientMock, true, /*IsEnterprise*/
		true /*IsElastic*/)

	// Xmem using elas
	settings[metadata.ReplicationType] = metadata.ReplicationTypeXmem

	_, _, _, errMap, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings)
	assert.NotEqual(len(errMap), 0)

	fmt.Println("============== Test case start: TestElasticSearch =================")
}
