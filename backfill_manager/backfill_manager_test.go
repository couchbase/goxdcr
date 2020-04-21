package backfill_manager

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
	pipeline_mgr "github.com/couchbase/goxdcr/pipeline_manager/mocks"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"testing"
)

func setupBoilerPlate() (*service_def.CollectionsManifestSvc,
	*service_def.ReplicationSpecSvc,
	*service_def.BackfillReplSvc,
	*pipeline_mgr.PipelineMgrBackfillIface) {
	manifestSvc := &service_def.CollectionsManifestSvc{}
	replSpecSvc := &service_def.ReplicationSpecSvc{}
	backfillReplSvc := &service_def.BackfillReplSvc{}
	pipelineMgr := &pipeline_mgr.PipelineMgrBackfillIface{}

	return manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr
}

func setupMock(manifestSvc *service_def.CollectionsManifestSvc,
	replSpecSvc *service_def.ReplicationSpecSvc,
	backfillReplSvc *service_def.BackfillReplSvc,
	pipelineMgr *pipeline_mgr.PipelineMgrBackfillIface) {

	manifestSvc.On("SetMetadataChangeHandlerCallback", mock.Anything).Return(nil)
	replSpecSvc.On("SetMetadataChangeHandlerCallback", mock.Anything).Return(nil)
	backfillReplSvc.On("SetMetadataChangeHandlerCallback", mock.Anything).Return(nil)
}

func setupReplStartupSpecs(replSpecSvc *service_def.ReplicationSpecSvc,
	specsToFeedBack map[string]*metadata.ReplicationSpecification) {
	replSpecSvc.On("AllReplicationSpecs").Return(specsToFeedBack, nil)
}

func TestBackfillMgrLaunchNoSpecs(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillMgrLaunchNoSpecs =================")
	manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr := setupBoilerPlate()
	setupReplStartupSpecs(replSpecSvc, nil)
	setupMock(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr)

	backfillMgr := NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr)
	assert.NotNil(backfillMgr)

	assert.Nil(backfillMgr.Start())

	fmt.Println("============== Test case end: TestBackfillMgrLaunchNoSpecs =================")
}

func getSpecId(i int) string {
	return fmt.Sprintf("RandId_%v", i)
}

// Returns a map of replId -> spec
// And returns a map of replId -> ManifestPair
func setupStartupSpecs(num int) (map[string]*metadata.ReplicationSpecification, map[string]*metadata.CollectionsManifestPair) {
	specMap := make(map[string]*metadata.ReplicationSpecification)
	colMap := make(map[string]*metadata.CollectionsManifestPair)

	for i := 0; i < num; i++ {
		specId := getSpecId(i)
		internalId := fmt.Sprintf("RandInternalId_%v", i)
		sourceBucket := fmt.Sprintf("RandSourceBucket_%v", i)
		sourceBucketUuid := fmt.Sprintf("RandSourceBucketUUID_%v", i)
		targetClusterUuid := fmt.Sprintf("targetClusterUuid_%v", i)
		targetBucketName := fmt.Sprintf("targetBucketName_%v", i)
		targetBucketUuid := fmt.Sprintf("targetBucketUuid_%v", i)

		repl := &metadata.ReplicationSpecification{
			Id:                specId,
			InternalId:        internalId,
			SourceBucketName:  sourceBucket,
			SourceBucketUUID:  sourceBucketUuid,
			TargetClusterUUID: targetClusterUuid,
			TargetBucketName:  targetBucketName,
			TargetBucketUUID:  targetBucketUuid,
			Settings:          metadata.DefaultReplicationSettings(),
		}

		specMap[specId] = repl

		defaultCollectionMap := make(metadata.CollectionsMap)
		defaultCollectionMap[base.DefaultScopeCollectionName] = metadata.Collection{0, base.DefaultScopeCollectionName}
		defaultScopeMap := make(metadata.ScopesMap)
		defaultScopeMap[base.DefaultScopeCollectionName] = metadata.Scope{0, base.DefaultScopeCollectionName, defaultCollectionMap}

		// Always let tgtManifest be +1
		srcManifest := metadata.UnitTestGenerateCollManifest(uint64(i), defaultScopeMap)
		tgtManifest := metadata.UnitTestGenerateCollManifest(uint64(i+1), defaultScopeMap)

		manifestPair := metadata.NewCollectionsManifestPair(srcManifest, tgtManifest)
		colMap[specId] = manifestPair
	}

	return specMap, colMap
}

func setupStartupManifests(manifestSvc *service_def.CollectionsManifestSvc,
	specMap map[string]*metadata.ReplicationSpecification,
	colMap map[string]*metadata.CollectionsManifestPair) {

	for replId, spec := range specMap {
		manifestPair, ok := colMap[replId]
		if ok {
			manifestSvc.On("GetLastPersistedManifests", spec).Return(manifestPair, nil)
		} else {
			manifestSvc.On("GetLastPersistedManifests", spec).Return(nil, fmt.Errorf("DummyErr"))
		}
	}
}

func TestBackfillMgrLaunchSpecs(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillMgrLaunchSpecs =================")
	manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr := setupBoilerPlate()
	specs, manifestPairs := setupStartupSpecs(5)
	setupReplStartupSpecs(replSpecSvc, specs)
	setupStartupManifests(manifestSvc, specs, manifestPairs)
	setupMock(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr)

	backfillMgr := NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr)
	assert.NotNil(backfillMgr)

	assert.Nil(backfillMgr.Start())

	fmt.Println("============== Test case end: TestBackfillMgrLaunchSpecs =================")
}

func TestBackfillMgrLaunchSpecsWithErr(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillMgrLaunchSpecsWithErr =================")
	manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr := setupBoilerPlate()
	specs, manifestPairs := setupStartupSpecs(5)

	// Delete the 3rd one to simulate error
	delete(manifestPairs, getSpecId(3))

	setupReplStartupSpecs(replSpecSvc, specs)
	setupStartupManifests(manifestSvc, specs, manifestPairs)
	setupMock(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr)

	backfillMgr := NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr)
	assert.NotNil(backfillMgr)

	assert.Nil(backfillMgr.Start())

	// The thrid one should have default manifest
	// TODO MB-38868 - backfill need to fire
	backfillMgr.cacheMtx.RLock()
	srcManifest, exists1 := backfillMgr.cacheSpecSourceMap[getSpecId(3)]
	tgtManifest, exists2 := backfillMgr.cacheSpecTargetMap[getSpecId(3)]
	backfillMgr.cacheMtx.RUnlock()
	assert.True(exists1 && exists2)
	defaultManifest := metadata.NewDefaultCollectionsManifest()
	assert.True(defaultManifest.IsSameAs(srcManifest))
	assert.True(defaultManifest.IsSameAs(tgtManifest))

	fmt.Println("============== Test case end: TestBackfillMgrLaunchSpecsWithErr =================")
}
