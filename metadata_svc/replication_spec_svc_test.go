/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadata_svc

import (
	"errors"
	"fmt"
	mrand "math/rand"
	"testing"
	"time"

	mcMock "github.com/couchbase/gomemcached/client/mocks"
	"github.com/couchbase/goxdcr/v8/base"
	commonReal "github.com/couchbase/goxdcr/v8/common"
	common "github.com/couchbase/goxdcr/v8/common/mocks"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/pipeline_manager"
	service_def_real "github.com/couchbase/goxdcr/v8/service_def"
	service_def "github.com/couchbase/goxdcr/v8/service_def/mocks"
	utilities "github.com/couchbase/goxdcr/v8/utils"
	utilsMock "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

var supportabilityCompat int = 458758
var colAndAdvSupportCompat int = 458754
var preAdvFilterCompat int = 393216

func setupBoilerPlate() (*service_def.XDCRCompTopologySvc,
	*service_def.MetadataSvc,
	*service_def.UILogSvc,
	*service_def.RemoteClusterSvc,
	*utilsMock.UtilsIface,
	*ReplicationSpecService,
	string,
	string,
	string,
	map[string]interface{},
	*mcMock.ClientIface,
	*service_def.BackfillReplSvc) {

	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	metadataSvcMock := &service_def.MetadataSvc{}
	uiLogSvcMock := &service_def.UILogSvc{}
	remoteClusterMock := &service_def.RemoteClusterSvc{}
	utilitiesMock := &utilsMock.UtilsIface{}
	clientMock := &mcMock.ClientIface{}
	backfillReplSvc := &service_def.BackfillReplSvc{}
	replSettingSvc := &service_def.ReplicationSettingsSvc{}

	emptyCacheEntries := []*service_def_real.MetadataEntry{}
	replicationSpecsCatalogKey := "replicationSpec"
	metadataSvcMock.On("GetAllMetadataFromCatalog", replicationSpecsCatalogKey).Return(emptyCacheEntries, nil)
	utilitiesMock.On("ExponentialBackoffExecutor", "GetAllMetadataFromCatalogReplicationSpec", mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return(nil)
	utilitiesMock.On("ExponentialBackoffExecutor", "RequestRemoteMonitoring", mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Run(func(args mock.Arguments) {
		actualFunc := args.Get(4).(utilities.ExponentialOpFunc)
		actualFunc()
	}).Return(nil)
	xdcrTopologyMock.On("MyClusterUuid").Return("dummyClusterUUID", nil)

	replSettingSvc.On("GetDefaultReplicationSettings").Return(metadata.DefaultReplicationSettings(), nil)

	replSpecSvc, _ := NewReplicationSpecService(uiLogSvcMock, remoteClusterMock, metadataSvcMock, xdcrTopologyMock, nil, log.DefaultLoggerContext, utilitiesMock, replSettingSvc)
	replSpecSvc.SetManifestsGetter(defaultManifestGetter)

	sourceBucket := "testSrcBucket"
	targetBucket := "testTargetBucket"
	targetCluster := "localHost"
	settings := make(map[string]interface{})

	xdcrTopologyMock.On("IsKVNode").Return(true, nil)

	return xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		utilitiesMock, replSpecSvc, sourceBucket, targetBucket, targetCluster, settings,
		clientMock, backfillReplSvc
}

var defaultManifestGetter = func(spec *metadata.ReplicationSpecification, specMayNotExist bool) (src, tgt *metadata.CollectionsManifest, err error) {
	return &defaultManifest, &defaultManifest, nil
}
var manifestGetterError = func(spec *metadata.ReplicationSpecification, specMayNotExist bool) (src, tgt *metadata.CollectionsManifest, err error) {
	return nil, nil, fmt.Errorf("Dummy")
}

func setupPipelineBoilerPlate(replSpecSvc *ReplicationSpecService, xdcrTopologyMock *service_def.XDCRCompTopologySvc, remoteClusterMock *service_def.RemoteClusterSvc, uiLogSvcMock *service_def.UILogSvc, backfillReplSvc *service_def.BackfillReplSvc, manifestGetter service_def_real.ManifestsGetter) (*pipeline_manager.PipelineManager, *common.PipelineFactory, *service_def.CheckpointsService, *common.Pipeline) {

	pipelineMock := &common.PipelineFactory{}
	utilsNew := utilities.NewUtilities()
	checkPointsSvc := &service_def.CheckpointsService{}
	collectionsManifestSvc := &service_def.CollectionsManifestSvc{}
	capability := metadata.UnitTestGetCollectionsCapability()

	pipelineMock.On("SetPipelineStopCallback", mock.Anything).Return(nil)

	remoteClusterMock.On("GetCapability", mock.Anything).Return(capability, nil)

	pipelineMgr := pipeline_manager.NewPipelineManager(pipelineMock, replSpecSvc, xdcrTopologyMock, remoteClusterMock, checkPointsSvc, uiLogSvcMock, log.DefaultLoggerContext, utilsNew, collectionsManifestSvc, backfillReplSvc, nil, nil)

	testPipeline := &common.Pipeline{}

	replSpecSvc.SetManifestsGetter(manifestGetter)
	return pipelineMgr, pipelineMock, checkPointsSvc, testPipeline
}

func generateFakeListOfVBs(capacity int) []uint16 {
	var listOfVBs = make([]uint16, capacity, capacity)
	for i := 0; i < capacity; i++ {
		listOfVBs[i] = (uint16)(mrand.Intn(1024))
	}
	return listOfVBs
}

var collectionsCapability = metadata.UnitTestGetCollectionsCapability()
var emptyCapability = metadata.Capability{}

func setupMocks(srcResolutionType string, destResolutionType string, xdcrTopologyMock *service_def.XDCRCompTopologySvc, metadataSvcMock *service_def.MetadataSvc, uiLogSvcMock *service_def.UILogSvc, remoteClusterMock *service_def.RemoteClusterSvc, utilitiesMock *utilsMock.UtilsIface, replSpecSvc *ReplicationSpecService, clientMock *mcMock.ClientIface, isEnterprise bool, isElasticSearch bool, compressionPass bool, backfillReplSvc *service_def.BackfillReplSvc, remoteClusterRefreshBusy bool, clusterCompatVersion int, rcCapability metadata.Capability) {

	// RemoteClusterMock
	hostAddr := setupRemoteClusterSvc(remoteClusterMock, remoteClusterRefreshBusy)

	// Compression features for utils mock
	var fullFeatures utilities.HELOFeatures
	fullFeatures.Xattribute = true
	fullFeatures.CompressionType = base.CompressionTypeSnappy
	var noCompressionFeature utilities.HELOFeatures
	noCompressionFeature.Xattribute = true
	noCompressionFeature.CompressionType = base.CompressionTypeNone
	var respondFeatures utilities.HELOFeatures
	respondFeatures.CompressionType = base.CompressionTypeSnappy
	var respondNoFeatures utilities.HELOFeatures
	respondNoFeatures.CompressionType = base.CompressionTypeNone

	// Utilities mock
	var port uint16 = 9000
	utilitiesMock.On("GetHostAddr", "localhost", port).Return(hostAddr)
	//	utilitiesMock.On("GetMemcachedRawConn", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	utilitiesMock.On("GetMemcachedRawConn", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(clientMock, nil)
	if compressionPass {
		utilitiesMock.On("SendHELOWithFeatures", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fullFeatures, nil)
		utilitiesMock.On("GetMemcachedConnectionWFeatures", mock.Anything, mock.Anything, mock.Anything,
			mock.Anything, mock.Anything, mock.Anything).Return(clientMock, respondFeatures, nil)
	} else {
		utilitiesMock.On("SendHELOWithFeatures", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(noCompressionFeature, nil)
		utilitiesMock.On("GetMemcachedConnectionWFeatures", mock.Anything, mock.Anything, mock.Anything,
			mock.Anything, mock.Anything, mock.Anything).Return(clientMock, respondNoFeatures, nil)
	}
	myConnectionStr := base.GetHostAddr("localhost", port)
	utilitiesMock.On("GetSecuritySettingsAndDefaultPoolInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false, nil, nil)
	utilitiesMock.On("GetBucketPasswordFromBucketInfo", mock.Anything, mock.Anything, mock.Anything).Return("", nil)
	utilitiesMock.On("StartDiagStopwatch", mock.Anything, mock.Anything).Return(func() time.Duration { return 0 })

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
	remoteClusterMock.On("RemoteClusterByRefId", mock.Anything, mock.Anything).Return(nil, nil)
	remoteClusterMock.On("GetCapability", mock.Anything).Return(rcCapability, nil)

	// UI Log mock service
	uiLogSvcMock.On("Write", mock.Anything).Return(nil)

	// XDCR Topology Mock
	xdcrTopologyMock.On("MyConnectionStr").Return(myConnectionStr, nil)
	xdcrTopologyMock.On("MyMemcachedAddr").Return(myConnectionStr, nil)
	xdcrTopologyMock.On("IsMyClusterEnterprise").Return(isEnterprise, nil)
	xdcrTopologyMock.On("NumberOfKVNodes").Return(1, nil)
	xdcrTopologyMock.On("MyClusterCompatibility").Return(clusterCompatVersion, nil)

	// LOCAL mock
	utilitiesMock.On("BucketValidationInfo", hostAddr,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bucketInfo,
		bucketType, bucketUUID, srcResolutionType, bucketEvictionPolicy, bucketKVVBMap, err)

	// TARGET mock - emptyString since we're feeding a dummy target
	utilitiesMock.On("RemoteBucketValidationInfo", hostAddr,
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

	// BackfillReplSvc mock - return no backfill
	backfillReplSvc.On("BackfillReplSpec", mock.Anything).Return(nil, base.ErrorNotFound)
}

func setupRemoteClusterSvc(remoteClusterMock *service_def.RemoteClusterSvc, remoteClusterRefreshBusy bool) string {
	hostAddr := "localhost:9000"
	mockRemoteClusterRef, _ := metadata.NewRemoteClusterReference("1", "", hostAddr, "", "", "", false, "", nil, nil, nil, nil)
	if remoteClusterRefreshBusy {
		remoteClusterMock.On("RemoteClusterByRefName", mock.Anything, false).Return(nil, RefreshNotEnabledYet)
		remoteClusterMock.On("RemoteClusterByRefName", mock.Anything, true).Return(mockRemoteClusterRef, nil)
	} else {
		remoteClusterMock.On("RemoteClusterByRefName", mock.Anything, mock.Anything).Return(mockRemoteClusterRef, nil)
	}
	remoteClusterMock.On("RemoteClusterByUuid", "", false).Return(mockRemoteClusterRef, nil)
	remoteClusterMock.On("RemoteClusterByUuid", "", true).Return(mockRemoteClusterRef, nil)
	remoteClusterMock.On("ValidateRemoteCluster", mockRemoteClusterRef).Return(nil)
	remoteClusterMock.On("ShouldUseAlternateAddress", mock.Anything).Return(false, nil)
	remoteClusterMock.On("RequestRemoteMonitoring", mock.Anything).Run(func(arg mock.Arguments) { fmt.Printf("RequestRemoteMonitoring...\n") }).Return(nil)
	remoteClusterMock.On("UnRequestRemoteMonitoring", mock.Anything).Return(nil)
	return hostAddr
}

func setupPipelineMock(
	pipelineMgr *pipeline_manager.PipelineManager,
	testPipeline *common.Pipeline,
	testTopic string,
	pipelineMock *common.PipelineFactory,
	ckptMock *service_def.CheckpointsService,
	spec *metadata.ReplicationSpecification) {

	var emptyNozzles map[string]commonReal.Nozzle
	testPipeline.On("Sources").Return(emptyNozzles)
	// Test pipeline running test
	testPipeline.On("State").Return(commonReal.Pipeline_Running)
	testPipeline.On("InstanceId").Return(testTopic)
	testPipeline.On("SetProgressRecorder", mock.AnythingOfType("common.PipelineProgressRecorder")).Return(nil)
	emptyMap := make(base.ErrorMap)
	testPipeline.On("Start", mock.Anything).Return(emptyMap)
	testPipeline.On("Stop", mock.Anything).Return(emptyMap)
	testPipeline.On("Specification").Return(spec)
	testPipeline.On("Topic").Return(testTopic)
	testPipeline.On("Type").Return(commonReal.MainPipeline)

	pipelineMock.On("NewPipeline", testTopic, mock.AnythingOfType("common.PipelineProgressRecorder")).Return(testPipeline, nil)
	ckptMock.On("DelCheckpointsDocs", mock.Anything).Return(nil)
}

/**
 * Tests the base line of xmem to xmem copy
 */
func TestValidateNewReplicationSpec(t *testing.T) {
	assert := assert.New(t)

	fmt.Println("============== Test case start: TestValidateNewReplicationSpec =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock, backfillReplSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno, xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock, utilitiesMock, replSpecSvc, clientMock, true, false, true, backfillReplSvc, false, colAndAdvSupportCompat, collectionsCapability)

	// Assume XMEM replication type
	settings[metadata.ReplicationTypeKey] = metadata.ReplicationTypeXmem
	_, _, _, errMap, _, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings, false)
	assert.Equal(len(errMap), 0)
	fmt.Println("============== Test case end: TestValidateNewReplicationSpec =================")
}

func TestValidateNewReplicationSpecWithManifests(t *testing.T) {
	assert := assert.New(t)

	fmt.Println("============== Test case start: TestValidateNewReplicationSpecWithManifests =================")
	defer fmt.Println("============== Test case end: TestValidateNewReplicationSpecWithManifests =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock, backfillReplSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno, xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock, utilitiesMock, replSpecSvc, clientMock, true, false, true, backfillReplSvc, false, supportabilityCompat, collectionsCapability)

	// Assume XMEM replication type
	settings[metadata.ReplicationTypeKey] = metadata.ReplicationTypeXmem
	_, _, _, errMap, _, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings, false)
	assert.Equal(len(errMap), 0)
}

func TestValidateNewReplicationSpecWithManifestsWErr(t *testing.T) {
	assert := assert.New(t)

	fmt.Println("============== Test case start: TestValidateNewReplicationSpecWithManifestsWErr =================")
	defer fmt.Println("============== Test case end: TestValidateNewReplicationSpecWithManifestsWErr =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock, backfillReplSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno, xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock, utilitiesMock, replSpecSvc, clientMock, true, false, true, backfillReplSvc, false, supportabilityCompat, collectionsCapability)

	replSpecSvc.SetManifestsGetter(manifestGetterError)

	// Assume XMEM replication type
	settings[metadata.ReplicationTypeKey] = metadata.ReplicationTypeXmem
	_, _, _, errMap, _, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings, false)
	assert.NotEqual(len(errMap), 0)
}

/**
 * Tests when the conflict resolution types are different - negative test
 */
func TestNegativeConflictResolutionType(t *testing.T) {
	assert := assert.New(t)

	fmt.Println("============== Test case start: TestNegativeConflictResolutionType =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock, backfillReplSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Lww, xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock, utilitiesMock, replSpecSvc, clientMock, true, false, true, backfillReplSvc, false, colAndAdvSupportCompat, collectionsCapability)

	// Assume XMEM replication type
	settings[metadata.ReplicationTypeKey] = metadata.ReplicationTypeXmem

	_, _, _, errMap, _, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings, true)
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
		utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock, backfillReplSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Lww, xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock, utilitiesMock, replSpecSvc, clientMock, true, false, false, backfillReplSvc, false, colAndAdvSupportCompat, collectionsCapability)

	// Assume CAPI (elasticsearch) replication type
	settings[metadata.ReplicationTypeKey] = metadata.ReplicationTypeCapi

	_, _, _, errMap, _, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings, false)
	// Should pass
	assert.Equal(len(errMap), 0)
	fmt.Println("============== Test case end: TestDifferentConflictResolutionTypeOnCapi =================")
}

func TestAddReplicationSpec(t *testing.T) {
	assert := assert.New(t)

	fmt.Println("============== Test case start: TestAddReplicationSpecAndVerifyFunc =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		utilitiesMock, replSpecSvc,
		_, _, _, _, clientMock, backfillReplSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Lww, xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock, utilitiesMock, replSpecSvc, clientMock, true, false, true, backfillReplSvc, false, colAndAdvSupportCompat, collectionsCapability)

	spec := &metadata.ReplicationSpecification{
		Id:               "test",
		InternalId:       "internalTest",
		SourceBucketName: "testSrc",
		TargetBucketName: "testTgt",
		Settings:         metadata.DefaultReplicationSettings(),
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
		utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock, backfillReplSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno, xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock, utilitiesMock, replSpecSvc, clientMock, true, false, true, backfillReplSvc, false, colAndAdvSupportCompat, collectionsCapability)

	// Turning off should be allowed
	settings[metadata.CompressionTypeKey] = base.CompressionTypeNone
	_, _, _, errMap, _, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings, true)
	assert.Equal(len(errMap), 0)

	// Turning on should be allowed
	settings[metadata.CompressionTypeKey] = base.CompressionTypeSnappy
	_, _, _, errMap, _, _, _ = replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings, true)
	assert.Equal(len(errMap), 0)

	fmt.Println("============== Test case end: TestCompressionPositive =================")
}

func TestCompressionNegNotEnterprise(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCompressionNegNotEnterprise =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock, backfillReplSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno, xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock, utilitiesMock, replSpecSvc, clientMock, false, false, true, backfillReplSvc, false, colAndAdvSupportCompat, collectionsCapability)

	// Turning on should be disallowed
	settings[metadata.CompressionTypeKey] = base.CompressionTypeSnappy
	_, _, _, errMap, _, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings, true)
	assert.NotEqual(len(errMap), 0)

	// Setting to auto should be disallowed
	settings[metadata.CompressionTypeKey] = base.CompressionTypeAuto
	_, _, _, errMap, _, _, _ = replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings, true)
	assert.NotEqual(len(errMap), 0)

	fmt.Println("============== Test case end: TestCompressionNegNotEnterprise =================")
}

func TestOriginalRegexInvalidateFilter(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestOriginalRegexInvalidateFilter =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock, backfillReplSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno, xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock, utilitiesMock, replSpecSvc, clientMock, true, true, false, backfillReplSvc, false, colAndAdvSupportCompat, collectionsCapability)

	// Xmem using elas
	settings[metadata.FilterExpressionKey] = "^abc"

	_, _, _, _, err, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings, false)
	assert.NotNil(err)

	// If it's an existing replication with an old filter, it's ok
	errMap, err, _ := replSpecSvc.ValidateReplicationSettings(sourceBucket, targetCluster, targetBucket, settings, false)
	assert.Nil(err)
	assert.Equal(0, len(errMap))

	fmt.Println("============== Test case end: TestOriginalRegexInvalidateFilter =================")
}

func TestOriginalRegexUpgradedFilter(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestOriginalRegexUpgradedFilter =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock, backfillReplSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno, xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock, utilitiesMock, replSpecSvc, clientMock, true, true, false, backfillReplSvc, false, colAndAdvSupportCompat, collectionsCapability)

	// Xmem using elas
	settings[metadata.FilterExpressionKey] = base.UpgradeFilter("^abc")

	_, _, _, _, err, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings, false)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestOriginalRegexUpgradedFilter =================")
}

func TestPreventFilterCreateMixedMode(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPreventFilterCreateMixedMode =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock, backfillReplSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno, xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock, utilitiesMock, replSpecSvc, clientMock, true, true, false, backfillReplSvc, false, preAdvFilterCompat, collectionsCapability)

	// Xmem using elas
	settings[metadata.FilterExpressionKey] = base.UpgradeFilter("^abc")

	_, _, _, _, err, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings, false)
	assert.Equal(base.ErrorAdvFilterMixedModeUnsupported, err)
	fmt.Println("============== Test case end: TestPreventFilterCreateMixedMode =================")
}

func TestPreventFilterEditMixedMode(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPreventFilterEditMixedMode =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock, backfillReplSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno, xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock, utilitiesMock, replSpecSvc, clientMock, true, true, false, backfillReplSvc, false, preAdvFilterCompat, collectionsCapability)

	// Xmem using elas
	settings[metadata.FilterVersionKey] = base.FilterVersionAdvanced
	settings[metadata.FilterExpressionKey] = base.UpgradeFilter("^abc")

	_, err, _ := replSpecSvc.ValidateReplicationSettings(sourceBucket, targetCluster, targetBucket, settings, false)
	assert.Equal(base.ErrorAdvFilterMixedModeUnsupported, err)
	fmt.Println("============== Test case end: TestPreventFilterEditMixedMode =================")
}

func TestStripExpiry(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestStripExpiry =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, targetCluster, settings, clientMock, backfillReplSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno, xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock, utilitiesMock, replSpecSvc, clientMock, true, false, false, backfillReplSvc, false, colAndAdvSupportCompat, collectionsCapability)

	settings[metadata.FilterExpDelKey] = base.FilterExpDelStripExpiration

	_, _, _, errMap, _, _, _ := replSpecSvc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings, false)
	assert.Equal(len(errMap), 0)

	fmt.Println("============== Test case start: TestStripExpiry =================")
}

/**
 * Tests the concurrent access of Replication spec and status object cache by both spec update
 * and status update
 */
func TestSpecMetadataCache(t *testing.T) {
	assert := assert.New(t)

	fmt.Println("============== Test case start: TestSpecMetadataCache =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		utilitiesMock, replSpecSvc,
		sourceBucket, targetBucket, _, _, clientMock, backfillReplSvc := setupBoilerPlate()

	pipelineMgr, pipelineMock, ckptMock, testPipeline := setupPipelineBoilerPlate(replSpecSvc, xdcrTopologyMock, remoteClusterMock, uiLogSvcMock, backfillReplSvc, defaultManifestGetter)

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Seqno, base.ConflictResolutionType_Seqno, xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock, utilitiesMock, replSpecSvc, clientMock, true, false, true, backfillReplSvc, false, colAndAdvSupportCompat, collectionsCapability)

	testTopic := "testTopic"

	spec := &metadata.ReplicationSpecification{
		Id:               testTopic,
		InternalId:       "internalTest",
		SourceBucketName: sourceBucket,
		TargetBucketName: targetBucket,
		Settings:         metadata.DefaultReplicationSettings(),
	}

	setupPipelineMock(pipelineMgr, testPipeline, testTopic, pipelineMock, ckptMock, spec)

	//Needs N+1 loops to create N number of concurrent access of the cache
	for i := 0; i < 2; i++ {
		fmt.Println("	Test case TestSpecMetadataCache looping:", i)
		internalId := fmt.Sprintf("%s-%v", testTopic, i)
		spec = &metadata.ReplicationSpecification{
			Id:               testTopic,
			InternalId:       internalId,
			SourceBucketName: sourceBucket,
			TargetBucketName: targetBucket,
			Settings:         metadata.DefaultReplicationSettings(),
		}

		assert.Nil(replSpecSvc.AddReplicationSpec(spec, ""))
		time.Sleep(1 * time.Second)
		rs, _ := pipelineMgr.ReplicationStatus(testTopic)
		assert.Nil(rs)
		pipelineMgr.GetOrCreateReplicationStatus(testTopic, nil)
		assert.Nil(pipelineMgr.Update(testTopic, nil))
		//Change the spec.InternalId to force the removal to clean up the ReplicationStatus
		//objecat from the cache
		spec.InternalId = internalId + "1"
		fmt.Println("	Stopping replication testTopic:", i)
		go pipelineMgr.RemoveReplicationStatus(testTopic)
	}
	//wait for RemoveReplicationStatus to finish
	time.Sleep(2 * time.Second)
	fmt.Println("============== Test case end: TestSpecMetadataCache =================")
}

func TestAddReplicationSpecWhenRefreshNotReady(t *testing.T) {
	assert := assert.New(t)

	fmt.Println("============== Test case start: TestAddReplicationSpecAndVerifyFunc =================")
	xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock,
		utilitiesMock, replSpecSvc,
		_, _, _, _, clientMock, backfillReplSvc := setupBoilerPlate()

	// Begin mocks
	setupMocks(base.ConflictResolutionType_Lww, base.ConflictResolutionType_Lww, xdcrTopologyMock, metadataSvcMock, uiLogSvcMock, remoteClusterMock, utilitiesMock, replSpecSvc, clientMock, true, false, true, backfillReplSvc, true, colAndAdvSupportCompat, collectionsCapability)

	spec := &metadata.ReplicationSpecification{
		Id:               "test",
		InternalId:       "internalTest",
		SourceBucketName: "testSrc",
		TargetBucketName: "testTgt",
		Settings:         metadata.DefaultReplicationSettings(),
	}

	_, _, _, errMap, _, _, _ := replSpecSvc.ValidateNewReplicationSpec(spec.SourceBucketName, spec.TargetClusterUUID, spec.TargetBucketName, spec.Settings.ToMap(false), false)
	assert.Len(errMap, 0)

	assert.Nil(replSpecSvc.AddReplicationSpec(spec, ""))
	checkRep, checkErr := replSpecSvc.ReplicationSpec(spec.Id)
	assert.Nil(checkErr)
	// We created with a 0 revision, metakv returns a 1
	assert.NotEqual(checkRep.Revision, checkRep.Settings.Revision)

	fmt.Println("============== Test case end: TestAddReplicationSpecAndVerifyFunc =================")
}
