package metadata_svc

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/couchbase/goxdcr/utils"
	utilsReal "github.com/couchbase/goxdcr/utils"
	utilsMock "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"io/ioutil"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func setupBoilerPlateCMS() (*service_def.RemoteClusterSvc,
	*service_def.ReplicationSpecSvc,
	*service_def.MetadataSvc,
	*service_def.UILogSvc,
	*utilsMock.UtilsIface,
	utils.UtilsIface,
	*service_def.CheckpointsService,
	*service_def.CollectionsManifestOps,
	*log.CommonLogger,
	map[string]*metadata.ReplicationSpecification,
	*service_def.XDCRCompTopologySvc,
	*service_def.ManifestsService) {

	metadataSvcMock := &service_def.MetadataSvc{}
	uiLogSvcMock := &service_def.UILogSvc{}
	remoteClusterSvcMock := &service_def.RemoteClusterSvc{}
	replicationSpecSvcMock := &service_def.ReplicationSpecSvc{}
	utilsMock := &utilsMock.UtilsIface{}
	utilsReal := utils.NewUtilities()
	checkpointsSvcMock := &service_def.CheckpointsService{}
	manifestOpsMock := &service_def.CollectionsManifestOps{}
	logger := log.NewLogger("CollectionsManifestSvcTest", log.DefaultLoggerContext)
	specsList := make(map[string]*metadata.ReplicationSpecification)
	xdcrTopologySvcMock := &service_def.XDCRCompTopologySvc{}
	manifestsSvc := &service_def.ManifestsService{}

	return remoteClusterSvcMock, replicationSpecSvcMock,
		metadataSvcMock, uiLogSvcMock, utilsMock, utilsReal, checkpointsSvcMock,
		manifestOpsMock, logger, specsList, xdcrTopologySvcMock, manifestsSvc
}

const srcTestDir = "../metadata/testData/"
const destTestDir = "testData/"

func setupMocksCMS(remoteClusterSvc *service_def.RemoteClusterSvc,
	replicationSpecSvc *service_def.ReplicationSpecSvc,
	metadataSvc *service_def.MetadataSvc,
	uiLogSvc *service_def.UILogSvc,
	utilsMock *utilsMock.UtilsIface,
	replicationSpecs map[string]*metadata.ReplicationSpecification,
	checkpointsSvc *service_def.CheckpointsService,
	manifestOpsMock *service_def.CollectionsManifestOps,
	logger *log.CommonLogger,
	xdcrTopologySvc *service_def.XDCRCompTopologySvc,
	sourceBucketExpectedCall int,
	manifestsSvc *service_def.ManifestsService) {

	replicationSpecSvc.On("AllReplicationSpecs").Return(replicationSpecs, nil)
	replicationSpecSvc.On("SetMetadataChangeHandlerCallback", mock.Anything).Return(nil)
	xdcrTopologySvc.On("MyCredentials").Return("Administrator", "wewewe",
		base.HttpAuthMechPlain, nil, false, nil, nil, nil)
	xdcrTopologySvc.On("MyConnectionStr").Return("localhost:9000", nil)

	// manifests
	b1Manifest, err := getMetadataManifest(srcTestDir, "provisionedManifest.json")
	if err != nil {
		panic(fmt.Sprintf("Error: unable to find b1Manifest, err: %v", err))
	}
	b2Manifest, err := getMetadataManifest(destTestDir, "remoteClusterManifest.json")
	if err != nil {
		panic("Error: unable to find b2Manifest")
	}

	utilsMock.On("GetCollectionsManifest", "localhost:9000", "B1",
		"Administrator", "wewewe", mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Return(b1Manifest, nil).Times(sourceBucketExpectedCall)

	b1Manifestv4, _ := getMetadataManifestCustomVersion(srcTestDir, "provisionedManifest.json", 4)
	b1Manifestv5, _ := getMetadataManifestCustomVersion(srcTestDir, "provisionedManifest.json", 5)
	b1Manifestv6, _ := getMetadataManifestCustomVersion(srcTestDir, "provisionedManifest.json", 6)

	b2Manifestv1, err := getMetadataManifestCustomVersion(destTestDir, "remoteClusterManifest.json", 1)
	b2Manifestv2, err := getMetadataManifestCustomVersion(destTestDir, "remoteClusterManifest.json", 2)
	b2Manifestv3, err := getMetadataManifestCustomVersion(destTestDir, "remoteClusterManifest.json", 3)

	srcManifestsList := &metadata.ManifestsList{b1Manifestv4, b1Manifestv5, b1Manifestv6}
	tgtManifestsList := &metadata.ManifestsList{b2Manifestv1, b2Manifestv2, b2Manifestv3}
	manifestsSvc.On("GetSourceManifests", mock.Anything).Return(srcManifestsList, nil)
	manifestsSvc.On("GetTargetManifests", mock.Anything).Return(tgtManifestsList, nil)
	manifestsSvc.On("UpsertSourceManifests", mock.Anything, mock.Anything).Return(nil)
	manifestsSvc.On("UpsertTargetManifests", mock.Anything, mock.Anything).Return(nil)

	setupUtilsExponentialBackoff(utilsMock)

	remoteClusterSvc.On("GetManifestByUuid", targetClusterUUID, targetBucketName, mock.Anything).Return(b2Manifest, nil)

}

func setupCheckpoints(checkpointsSvc *service_def.CheckpointsService, srcList, tgtList []uint64) {
	// the srcList and tgtList are pairs
	if len(srcList) != len(tgtList) {
		panic("Invalid lists")
	}

	var records []*metadata.CheckpointRecord
	for i := 0; i < len(srcList); i++ {
		aRecord := &metadata.CheckpointRecord{
			SourceManifest: srcList[i],
			TargetManifest: tgtList[i],
		}
		records = append(records, aRecord)
	}

	// Do one more just for fun
	aRecord := &metadata.CheckpointRecord{
		SourceManifest: srcList[0],
		TargetManifest: tgtList[0],
	}
	records = append(records, aRecord)

	// Just create a single vbno for simplicity
	checkpointDocs := make(map[uint16]*metadata.CheckpointsDoc)
	checkpointDoc := &metadata.CheckpointsDoc{}
	checkpointDoc.Checkpoint_records = records
	checkpointDocs[0] = checkpointDoc

	checkpointsSvc.On("CheckpointsDocs", mock.Anything, mock.Anything).Return(checkpointDocs, nil)
}

func setupUtilsExponentialBackoff(utilsMock *utilsMock.UtilsIface) {
	getterFunc := func(args mock.Arguments) {
		getterFunc := args.Get(4).(utilsReal.ExponentialOpFunc)
		getterFunc()
	}

	utilsMock.On("ExponentialBackoffExecutor", sourceRefreshStr, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Run(getterFunc).Return(nil)
	utilsMock.On("ExponentialBackoffExecutor", targetRefreshStr, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Run(getterFunc).Return(nil)

}

const sourceBucketName = "B1"
const sourceBucketUUID = "012345"
const targetClusterUUID = "tgt012345"
const targetBucketName = "tgtBucket"
const targetBucketUUID = "543210"

func getMetadataManifestCustomVersion(testDir, fileName string, uid uint64) (*metadata.CollectionsManifest, error) {
	fullFileName := testDir + fileName
	data, err := ioutil.ReadFile(fullFileName)
	if err != nil {
		return nil, err
	}
	manifest, err := metadata.TestNewCollectionsManifestFromBytesWithCustomUid(data, uid)
	return &manifest, err
}

func getMetadataManifest(testDir, fileName string) (*metadata.CollectionsManifest, error) {
	fullFileName := testDir + fileName
	data, err := ioutil.ReadFile(fullFileName)
	if err != nil {
		return nil, err
	}
	manifest, err := metadata.NewCollectionsManifestFromBytes(data)
	return &manifest, err
}

func TestCollectionsManifestSvcSetup(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCollectionsManifestSvcSetup =================")
	remoteClusterSvc, replicationSpecSvc, metadataSvc, uiLogSvc, utilsMock, utilsReal,
		checkpointsSvc, manifestOps, logger, specsList,
		xdcrTopologySvc, manifestsSvc := setupBoilerPlateCMS()

	// 1 spec
	testSpec, _ := metadata.NewReplicationSpecification(sourceBucketName, sourceBucketUUID, targetClusterUUID,
		targetBucketName, targetBucketUUID)
	specsList["testSpec"] = testSpec

	setupMocksCMS(remoteClusterSvc, replicationSpecSvc, metadataSvc, uiLogSvc, utilsMock, specsList,
		checkpointsSvc, manifestOps, logger, xdcrTopologySvc, 1 /*srcBucketExpectedFetchCnt*/, manifestsSvc)

	collectionsManifestService, _ := NewCollectionsManifestService(remoteClusterSvc,
		replicationSpecSvc, uiLogSvc, log.DefaultLoggerContext, utilsMock,
		checkpointsSvc, xdcrTopologySvc, manifestsSvc)

	assert.NotNil(collectionsManifestService)
	assert.NotNil(utilsReal)
	assert.Equal(1, len(collectionsManifestService.agentsMap))

	fmt.Println("============== Test case end: TestCollectionsManifestSvcSetup =================")
}

func TestCollectionsManifestAgentSetup(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCollectionsManifestAgentSetup =================")
	remoteClusterSvc, replicationSpecSvc, metadataSvc, uiLogSvc, utilsMock, utilsReal,
		checkpointsSvc, manifestOps, logger, specsList,
		xdcrTopologySvc, manifestsSvc := setupBoilerPlateCMS()
	assert.NotNil(utilsReal)

	testSpec, _ := metadata.NewReplicationSpecification(sourceBucketName, sourceBucketUUID, targetClusterUUID,
		targetBucketName, targetBucketUUID)
	specsList["testSpec"] = testSpec

	setupMocksCMS(remoteClusterSvc, replicationSpecSvc, metadataSvc, uiLogSvc, utilsMock, specsList,
		checkpointsSvc, manifestOps, logger, xdcrTopologySvc, 1 /*srcBucketExpectedFetchCnt*/, manifestsSvc)

	collectionsManifestService, _ := NewCollectionsManifestService(remoteClusterSvc,
		replicationSpecSvc, uiLogSvc, log.DefaultLoggerContext, utilsMock,
		checkpointsSvc, xdcrTopologySvc, manifestsSvc)

	assert.NotNil(collectionsManifestService)

	agent, ok := collectionsManifestService.agentsMap[testSpec.Id]
	assert.True(ok)
	assert.NotNil(agent)

	manifest := agent.GetSourceManifest()
	assert.NotNil(manifest)

	b1Manifest, err := getMetadataManifest(srcTestDir, "provisionedManifest.json")
	assert.Nil(err)
	assert.True(b1Manifest.Equals(manifest))

	b2Manifest, _ := getMetadataManifest(destTestDir, "remoteClusterManifest.json")

	// All these gets should not trigger a mock error
	manifest = agent.GetSourceManifest()
	assert.NotNil(manifest)
	manifest = agent.GetSourceManifest()
	assert.NotNil(manifest)
	go agent.GetSourceManifest()
	go agent.GetSourceManifest()

	// Service API
	checkManifest, _, err := collectionsManifestService.GetLatestManifests(testSpec)
	assert.Nil(err)
	assert.True(manifest.Equals(checkManifest))

	testSpec2, _ := metadata.NewReplicationSpecification("srcBucket2", sourceBucketUUID, targetClusterUUID,
		targetBucketName, targetBucketUUID)

	var nilSpec *metadata.ReplicationSpecification
	err = collectionsManifestService.ReplicationSpecChangeCallback("dummy", nilSpec, testSpec2)
	assert.Nil(err)
	assert.Equal(2, len(collectionsManifestService.agentsMap))
	err = collectionsManifestService.ReplicationSpecChangeCallback("dummy", testSpec2, nilSpec)
	assert.Nil(err)
	assert.Equal(1, len(collectionsManifestService.agentsMap))

	// test refcount
	testSpec3, _ := metadata.NewReplicationSpecification(sourceBucketName, sourceBucketUUID, targetClusterUUID,
		"targetBucket2", targetBucketUUID)
	collectionsManifestService.handleNewReplSpec(testSpec3, false /*starting*/)

	assert.Equal(2, int(collectionsManifestService.srcBucketGettersRefCnt[sourceBucketName]))
	collectionsManifestService.handleDelReplSpec(testSpec3)
	assert.Equal(1, int(collectionsManifestService.srcBucketGettersRefCnt[sourceBucketName]))

	// Test agent recording and getting
	srcVblist := []uint16{0, 1, 2, 3}
	tgtVblist := []uint16{4, 5, 6}
	assert.Equal(0, len(agent.srcNozzlePullMap))
	assert.Equal(0, len(agent.tgtNozzlePullMap))
	vbSrcManifest := collectionsManifestService.GetSourceManifestForNozzle(testSpec, srcVblist)
	vbTgtManifest := collectionsManifestService.GetTargetManifestForNozzle(testSpec, tgtVblist)
	assert.True(vbSrcManifest.Equals(b1Manifest))
	assert.True(vbTgtManifest.Equals(b2Manifest))
	assert.Equal(4, len(agent.srcNozzlePullMap))
	assert.Equal(3, len(agent.tgtNozzlePullMap))

	// finish refcount
	collectionsManifestService.handleDelReplSpec(testSpec)
	_, ok = collectionsManifestService.srcBucketGettersRefCnt[sourceBucketName]
	assert.False(ok)

	fmt.Println("============== Test case end: TestCollectionsManifestAgentSetup =================")
}

func TestCollectionsAgentDedupManifest(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCollectionsAgentDedupManifest =================")
	remoteClusterSvc, replicationSpecSvc, metadataSvc, uiLogSvc, utilsMock, utilsReal,
		checkpointsSvc, manifestOps, logger, specsList,
		xdcrTopologySvc, manifestsSvc := setupBoilerPlateCMS()
	assert.NotNil(utilsReal)

	setupMocksCMS(remoteClusterSvc, replicationSpecSvc, metadataSvc, uiLogSvc, utilsMock, specsList,
		checkpointsSvc, manifestOps, logger, xdcrTopologySvc, 1 /*srcBucketExpectedFetchCnt*/, manifestsSvc)

	// Checkpoints have 4 5 6 for source and 1 2 3 for target
	ckSrcList := []uint64{4, 5, 6}
	ckTgtList := []uint64{1, 2, 3}
	setupCheckpoints(checkpointsSvc, ckSrcList, ckTgtList)

	//	collectionsManifestService, _ := NewCollectionsManifestService(remoteClusterSvc,
	//		replicationSpecSvc, uiLogSvc, log.DefaultLoggerContext, utilsMock,
	//		checkpointsSvc, xdcrTopologySvc, manifestsSvc)
	//	assert.NotNil(collectionsManifestService)

	testSpec, _ := metadata.NewReplicationSpecification(sourceBucketName, sourceBucketUUID, targetClusterUUID,
		targetBucketName, targetBucketUUID)

	agent := NewCollectionsManifestAgent("testAgent", remoteClusterSvc, checkpointsSvc, logger, utilsMock,
		testSpec, nil /*manifestOps*/, nil /*srcManifestGetter*/, manifestsSvc, nil /*callback*/)
	assert.NotNil(agent)

	agent.Start()

	for atomic.LoadUint32(&agent.started) != 1 {
		time.Sleep(300 * time.Nanosecond)
	}
	for atomic.LoadUint32(&agent.loadedFromMetakv) != 1 {
		time.Sleep(300 * time.Nanosecond)
	}

	// Let agent have cache of uid 7 8 9 source and 4 5 6 tgt
	b1Manifestv7, _ := getMetadataManifestCustomVersion(srcTestDir, "provisionedManifest.json", 7)
	b1Manifestv8, _ := getMetadataManifestCustomVersion(srcTestDir, "provisionedManifest.json", 8)
	b1Manifestv9, _ := getMetadataManifestCustomVersion(srcTestDir, "provisionedManifest.json", 9)

	agent.srcMtx.Lock()
	agent.sourceCache[b1Manifestv7.Uid()] = b1Manifestv7
	agent.sourceCache[b1Manifestv8.Uid()] = b1Manifestv8
	agent.sourceCache[b1Manifestv9.Uid()] = b1Manifestv9
	agent.srcMtx.Unlock()

	b2Manifestv4, err := getMetadataManifestCustomVersion(destTestDir, "remoteClusterManifest.json", 4)
	b2Manifestv5, err := getMetadataManifestCustomVersion(destTestDir, "remoteClusterManifest.json", 5)
	b2Manifestv6, err := getMetadataManifestCustomVersion(destTestDir, "remoteClusterManifest.json", 6)

	agent.tgtMtx.Lock()
	agent.targetCache[b2Manifestv4.Uid()] = b2Manifestv4
	agent.targetCache[b2Manifestv5.Uid()] = b2Manifestv5
	agent.targetCache[b2Manifestv6.Uid()] = b2Manifestv6
	agent.tgtMtx.Unlock()

	srcList, tgtList, err := agent.getAllManifestsUids()

	// src should only have 4 5 6 7 8 9, and target only have 1 2 3 4 5 6
	assert.Nil(err)
	assert.Equal(6, len(srcList))
	assert.Equal(6, len(tgtList))
	assert.Equal(uint64(4), srcList[0])
	assert.Equal(uint64(9), srcList[5])
	assert.Equal(uint64(1), tgtList[0])
	assert.Equal(uint64(6), tgtList[5])

	// Used for pruning - 0 is less than the smallest checkpointed
	agent.tgtMtx.RLock()
	agent.targetCache[0] = b2Manifestv4
	agent.tgtMtx.RUnlock()
	// loaded 3 from checkpoint, added 3 in this test, and 1 more here for a total of 7
	assert.Equal(7, len(agent.targetCache))
	// Used for pruning
	agent.srcMtx.RLock()
	agent.sourceCache[0] = b1Manifestv7
	agent.srcMtx.RUnlock()
	// loaded 3 from checkpoint, added 3 in this test, and 1 more here for a total of 7
	assert.Equal(7, len(agent.sourceCache))

	agent.pruneManifests(srcList, tgtList)
	// Prunes away the 0's added above
	assert.Equal(6, len(agent.sourceCache))
	assert.Equal(6, len(agent.targetCache))

	// Check hashing algorithm for updates
	srcManifestList, tgtManifestList := agent.testGetMetaLists(srcList, tgtList)
	checkSrcHash, err := srcManifestList.Sha256()
	assert.Nil(err)
	assert.False(reflect.DeepEqual(agent.persistedSrcListHash, checkSrcHash))
	checkTgtHash, err := tgtManifestList.Sha256()
	assert.Nil(err)
	assert.False(reflect.DeepEqual(agent.persistedTgtListHash, checkTgtHash))

	srcErr, tgtErr, srcUpdated, tgtUpdated := agent.PersistNeededManifests()
	assert.Nil(srcErr)
	assert.Nil(tgtErr)
	assert.True(srcUpdated)
	assert.True(tgtUpdated)

	// try to persist again with no updates should not write to metakv
	srcErr, tgtErr, srcUpdated, tgtUpdated = agent.PersistNeededManifests()
	assert.Nil(srcErr)
	assert.Nil(tgtErr)
	assert.False(srcUpdated)
	assert.False(tgtUpdated)

	fmt.Println("============== Test case end: TestCollectionsAgentDedupManifest =================")
}
