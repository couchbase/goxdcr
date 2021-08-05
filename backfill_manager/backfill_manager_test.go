/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software
will be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package backfill_manager

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
	pipeline_mgr "github.com/couchbase/goxdcr/pipeline_manager/mocks"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"io/ioutil"
	"testing"
	"time"
)

func setupBoilerPlate() (*service_def.CollectionsManifestSvc,
	*service_def.ReplicationSpecSvc,
	*service_def.BackfillReplSvc,
	*pipeline_mgr.PipelineMgrBackfillIface,
	*service_def.ClusterInfoSvc,
	*service_def.XDCRCompTopologySvc,
	*service_def.CheckpointsService) {
	manifestSvc := &service_def.CollectionsManifestSvc{}
	replSpecSvc := &service_def.ReplicationSpecSvc{}
	backfillReplSvc := &service_def.BackfillReplSvc{}
	pipelineMgr := &pipeline_mgr.PipelineMgrBackfillIface{}
	clusterInfoSvcMock := &service_def.ClusterInfoSvc{}
	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	checkpointSvcMock := &service_def.CheckpointsService{}

	return manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, clusterInfoSvcMock, xdcrTopologyMock, checkpointSvcMock
}

const sourceBucketName = "sourceBucket"
const sourceBucketUUID = "sourceBucketUuid"
const targetClusterUUID = "targetClusterUuid"
const targetBucketName = "targetBucket"
const targetBucketUUID = "targetBucketUuid"

var defaultSeqnoGetter = func() map[uint16]uint64 {
	retMap := make(map[uint16]uint64)
	for i := uint16(0); i < 1024; i++ {
		retMap[i] = 0
	}
	return retMap
}

var defaultvbMapGetter = func() map[string][]uint16 {
	retMap := make(map[string][]uint16)
	var list []uint16
	for i := uint16(0); i < 1024; i++ {
		list = append(list, i)
	}
	retMap["localhost:9000"] = list
	return retMap
}

func setupMock(manifestSvc *service_def.CollectionsManifestSvc, replSpecSvc *service_def.ReplicationSpecSvc, pipelineMgr *pipeline_mgr.PipelineMgrBackfillIface, clusterInfoSvcMock *service_def.ClusterInfoSvc, xdcrTopologyMock *service_def.XDCRCompTopologySvc, checkpointSvcMock *service_def.CheckpointsService, seqnoGetter func() map[uint16]uint64, localVBMapGetter func() map[string][]uint16, backfillReplSvc *service_def.BackfillReplSvc, additionalSpecIds []string) {

	returnedSpec, _ := metadata.NewReplicationSpecification(sourceBucketName, sourceBucketUUID, targetClusterUUID, targetBucketName, targetBucketUUID)
	var specList = []string{returnedSpec.Id}
	for _, extraId := range additionalSpecIds {
		specList = append(specList, extraId)
	}

	manifestSvc.On("SetMetadataChangeHandlerCallback", mock.Anything).Return(nil)
	replSpecSvc.On("SetMetadataChangeHandlerCallback", mock.Anything).Return(nil)
	replSpecSvc.On("ReplicationSpec", mock.Anything).Return(returnedSpec, nil)
	replSpecSvc.On("AllReplicationSpecIds").Return(specList, nil)
	pipelineMgr.On("GetMainPipelineThroughSeqnos", mock.Anything).Return(seqnoGetter(), nil)
	pipelineMgr.On("BackfillMappingStatusUpdate", mock.Anything, mock.Anything).Return(nil)
	clusterInfoSvcMock.On("GetLocalServerVBucketsMap", mock.Anything, mock.Anything).Return(localVBMapGetter(), nil)
	xdcrTopologyMock.On("MyKVNodes").Return([]string{"localhost:9000"}, nil)

	setupBackfillReplSvcMock(backfillReplSvc)
}

func setupBackfillReplSvcMock(backfillReplSvc *service_def.BackfillReplSvc) {
	backfillReplSvc.On("SetMetadataChangeHandlerCallback", mock.Anything).Return(nil)
	backfillReplSvc.On("AddBackfillReplSpec", mock.Anything).Return(nil)
	backfillReplSvc.On("SetBackfillReplSpec", mock.Anything).Return(nil)
	backfillReplSvc.On("SetCompleteBackfillRaiser", mock.Anything).Return(nil)
	backfillReplSvc.On("DelBackfillReplSpec", mock.Anything).Return(nil, nil)
}

func setupBackfillReplSvcNegMock(backfillReplSvc *service_def.BackfillReplSvc) {
	backfillReplSvc.On("SetMetadataChangeHandlerCallback", mock.Anything).Return(base.ErrorInvalidInput)
	backfillReplSvc.On("AddBackfillReplSpec", mock.Anything).Return(base.ErrorInvalidInput)
	backfillReplSvc.On("SetBackfillReplSpec", mock.Anything).Return(base.ErrorInvalidInput)
	backfillReplSvc.On("DelBackfillReplSpec", mock.Anything).Return(nil, base.ErrorInvalidInput)
	// SetCompleteBackfillRaiser for now can only return nil
	backfillReplSvc.On("SetCompleteBackfillRaiser", mock.Anything).Return(nil)
}

func setupReplStartupSpecs(replSpecSvc *service_def.ReplicationSpecSvc,
	specsToFeedBack map[string]*metadata.ReplicationSpecification) {
	replSpecSvc.On("AllReplicationSpecs").Return(specsToFeedBack, nil)
}

func setupBackfillSpecs(backfillReplSvc *service_def.BackfillReplSvc, parentSpecs map[string]*metadata.ReplicationSpecification) {
	backfillSpecs := make(map[string]*metadata.BackfillReplicationSpec)
	for specId, spec := range parentSpecs {
		vbTaskMap := metadata.NewVBTasksMap()
		bSpec := metadata.NewBackfillReplicationSpec(specId, "dummy", vbTaskMap, spec)
		backfillSpecs[specId] = bSpec
	}
	if len(backfillSpecs) == 0 {
		backfillReplSvc.On("BackfillReplSpec", mock.Anything).Return(nil, base.ErrorNotFound)
	} else {
		for bSpecId, spec := range backfillSpecs {
			backfillReplSvc.On("BackfillReplSpec", bSpecId).Return(spec, nil)
		}
	}
}

func TestBackfillMgrLaunchNoSpecs(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillMgrLaunchNoSpecs =================")
	manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, clusterInfoSvc, xdcrCompTopologySvc, checkpointSvcMock := setupBoilerPlate()
	setupReplStartupSpecs(replSpecSvc, nil)
	setupBackfillSpecs(backfillReplSvc, nil)
	setupMock(manifestSvc, replSpecSvc, pipelineMgr, clusterInfoSvc, xdcrCompTopologySvc, checkpointSvcMock, defaultSeqnoGetter, defaultvbMapGetter, backfillReplSvc, nil)

	backfillMgr := NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, clusterInfoSvc, xdcrCompTopologySvc, checkpointSvcMock)
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
	manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, clusterInfoSvc, xdcrCompTopologySvc, checkpointSvcMock := setupBoilerPlate()
	specs, manifestPairs := setupStartupSpecs(5)
	setupReplStartupSpecs(replSpecSvc, specs)
	setupBackfillSpecs(backfillReplSvc, specs)
	setupStartupManifests(manifestSvc, specs, manifestPairs)
	setupMock(manifestSvc, replSpecSvc, pipelineMgr, clusterInfoSvc, xdcrCompTopologySvc, checkpointSvcMock, defaultSeqnoGetter, defaultvbMapGetter, backfillReplSvc, nil)

	backfillMgr := NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, clusterInfoSvc, xdcrCompTopologySvc, checkpointSvcMock)
	assert.NotNil(backfillMgr)

	assert.Nil(backfillMgr.Start())

	fmt.Println("============== Test case end: TestBackfillMgrLaunchSpecs =================")
}

func TestBackfillMgrLaunchSpecsWithErr(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillMgrLaunchSpecsWithErr =================")
	manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, clusterInfoSvc, xdcrCompTopologySvc, checkpointSvcMock := setupBoilerPlate()
	specs, manifestPairs := setupStartupSpecs(5)

	// Delete the 3rd one to simulate error
	delete(manifestPairs, getSpecId(3))

	setupReplStartupSpecs(replSpecSvc, specs)
	setupBackfillSpecs(backfillReplSvc, specs)
	setupStartupManifests(manifestSvc, specs, manifestPairs)
	setupMock(manifestSvc, replSpecSvc, pipelineMgr, clusterInfoSvc, xdcrCompTopologySvc, checkpointSvcMock, defaultSeqnoGetter, defaultvbMapGetter, backfillReplSvc, nil)

	backfillMgr := NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, clusterInfoSvc, xdcrCompTopologySvc, checkpointSvcMock)
	assert.NotNil(backfillMgr)

	assert.Nil(backfillMgr.Start())

	// The thrid one should have default manifest
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

var testDir = "../metadata/testdata/"
var targetv7 = testDir + "diffTargetv7.json"

// v9a is like v9 but minus one collection (S2:col2)
var targetv9a = testDir + "diffTargetv9a.json"

func TestBackfillMgrSourceCollectionCleanedUp(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillMgrSourceCollectionCleanedUp =================")
	defer fmt.Println("============== Test case end: TestBackfillMgrSourceCollectionCleanedUp =================")
	manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, clusterInfoSvc, xdcrCompTopologySvc, checkpointSvcMock := setupBoilerPlate()
	specs, manifestPairs := setupStartupSpecs(5)

	setupReplStartupSpecs(replSpecSvc, specs)
	setupBackfillSpecs(backfillReplSvc, specs)
	setupStartupManifests(manifestSvc, specs, manifestPairs)
	specId := "RandId_0"
	var additionalSpecIDs []string = []string{specId}
	setupMock(manifestSvc, replSpecSvc, pipelineMgr, clusterInfoSvc, xdcrCompTopologySvc, checkpointSvcMock, defaultSeqnoGetter, defaultvbMapGetter, backfillReplSvc, additionalSpecIDs)

	backfillMgr := NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, clusterInfoSvc, xdcrCompTopologySvc, checkpointSvcMock)
	assert.NotNil(backfillMgr)

	assert.Nil(backfillMgr.Start())

	bytes, err := ioutil.ReadFile(targetv7)
	if err != nil {
		panic(err.Error())
	}
	v7Manifest, err := metadata.NewCollectionsManifestFromBytes(bytes)
	if err != nil {
		panic(err.Error())
	}
	bytes, err = ioutil.ReadFile(targetv9a)
	if err != nil {
		panic(err.Error())
	}
	v9Manifest, err := metadata.NewCollectionsManifestFromBytes(bytes)
	if err != nil {
		panic(err.Error())
	}

	defaultManifest := metadata.NewDefaultCollectionsManifest()
	oldPair := &metadata.CollectionsManifestPair{
		Source: &defaultManifest,
		Target: &defaultManifest,
	}
	newPair := &metadata.CollectionsManifestPair{
		Source: &v7Manifest,
		Target: &v7Manifest,
	}

	// Generated ID: RandId_0
	assert.Nil(backfillMgr.collectionsManifestChangeCb(specId, oldPair, newPair))
	time.Sleep(100 * time.Nanosecond)

	handler := backfillMgr.specToReqHandlerMap[specId]
	assert.NotNil(handler)
	v7BackfillTaskMap := handler.cachedBackfillSpec.VBTasksMap.Clone()

	// Now pretend source went from v7 to v9, and S2:col2 was removed
	// Target hasn't changed
	oldPair.Source = &v7Manifest
	oldPair.Target = &v7Manifest
	newPair.Source = &v9Manifest
	newPair.Target = &v7Manifest

	assert.Nil(backfillMgr.collectionsManifestChangeCb(specId, oldPair, newPair))
	time.Sleep(100 * time.Nanosecond)
	v9BackfillTaskMap := handler.cachedBackfillSpec.VBTasksMap.Clone()

	assert.False(v7BackfillTaskMap.SameAs(v9BackfillTaskMap))

	// v9 backfillTaskMap should not contain S1:col2 as source
	checkSourceNs := &base.CollectionNamespace{
		ScopeName:      "S2",
		CollectionName: "col2",
	}
	for _, tasks := range v9BackfillTaskMap.VBTasksMap {
		mappings := tasks.GetAllCollectionNamespaceMappings()
		for _, mapping := range mappings {
			_, _, _, exists := mapping.Get(checkSourceNs, nil)
			assert.False(exists)
		}
	}

}

func TestBackfillMgrRetry(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillMgrRetry =================")
	defer fmt.Println("============== Test case end: TestBackfillMgrRetry =================")
	manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, clusterInfoSvc, xdcrCompTopologySvc, checkpointSvcMock := setupBoilerPlate()
	specs, manifestPairs := setupStartupSpecs(5)

	setupReplStartupSpecs(replSpecSvc, specs)
	setupBackfillSpecs(backfillReplSvc, specs)
	setupStartupManifests(manifestSvc, specs, manifestPairs)
	specId := "RandId_0"
	var additionalSpecIDs []string = []string{specId}
	setupMock(manifestSvc, replSpecSvc, pipelineMgr, clusterInfoSvc, xdcrCompTopologySvc, checkpointSvcMock, defaultSeqnoGetter, defaultvbMapGetter, backfillReplSvc, additionalSpecIDs)

	backfillMgr := NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, clusterInfoSvc, xdcrCompTopologySvc, checkpointSvcMock)
	assert.NotNil(backfillMgr)
	backfillMgr.retryTimerPeriod = 500 * time.Millisecond

	assert.Nil(backfillMgr.Start())

	// add some tasks
	task1 := BackfillRetryRequest{
		replId: specId,
		req: metadata.CollectionNamespaceMappingsDiffPair{
			Added:                     nil,
			Removed:                   nil,
			RouterLatestSrcManifestId: 2,
		},
		force:                      false,
		correspondingSrcManifestId: 2,
		handler:                    nil, // will get internally
	}
	task2 := BackfillRetryRequest{
		replId: specId,
		req: metadata.CollectionNamespaceMappingsDiffPair{
			Added:                     nil,
			Removed:                   nil,
			RouterLatestSrcManifestId: 1,
		},
		force:                      false,
		correspondingSrcManifestId: 1,
		handler:                    nil, // will get internally
	}
	task3 := BackfillRetryRequest{
		replId: specId,
		req: metadata.CollectionNamespaceMappingsDiffPair{
			Added:                     nil,
			Removed:                   nil,
			RouterLatestSrcManifestId: 1,
		},
		force:                      false,
		correspondingSrcManifestId: 1,
		handler:                    nil, // will get internally
	}

	// Test to ensure that positive cases are done
	backfillMgr.retryBackfillRequest(task1)
	backfillMgr.retryBackfillRequest(task2)
	backfillMgr.retryBackfillRequest(task3)
	backfillMgr.errorRetryQMtx.RLock()
	assert.Len(backfillMgr.errorRetryQueue, 3)
	backfillMgr.errorRetryQMtx.RUnlock()

	time.Sleep(1 * time.Second)

	backfillMgr.errorRetryQMtx.RLock()
	assert.Len(backfillMgr.errorRetryQueue, 0)
	backfillMgr.errorRetryQMtx.RUnlock()

	backfillMgr.Stop()

	// Test for failure condition - new instance of backfillMgr
	_, _, backfillReplSvcBad, _, _, _, _ := setupBoilerPlate()
	//setupReplStartupSpecs(replSpecSvc, specs)
	setupBackfillSpecs(backfillReplSvcBad, specs)
	setupBackfillReplSvcNegMock(backfillReplSvcBad)

	backfillMgr = NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvcBad, pipelineMgr, clusterInfoSvc, xdcrCompTopologySvc, checkpointSvcMock)
	assert.NotNil(backfillMgr)
	backfillMgr.retryTimerPeriod = 500 * time.Millisecond

	assert.Nil(backfillMgr.Start())
	handler := backfillMgr.internalGetHandler(specId)
	handler.backfillReplSvc = backfillReplSvcBad

	// Test to ensure that retry fails
	backfillMgr.retryBackfillRequest(task1)
	backfillMgr.retryBackfillRequest(task2)
	backfillMgr.retryBackfillRequest(task3)
	backfillMgr.errorRetryQMtx.RLock()
	assert.Len(backfillMgr.errorRetryQueue, 3)
	backfillMgr.errorRetryQMtx.RUnlock()

	time.Sleep(1 * time.Second)

	// Not tested here - but look at the logs - there should be 2 instances of retries
	assert.Len(backfillMgr.errorRetryQueue, 3)
}
