/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package backfill_manager

import (
	"fmt"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/peerToPeer"
	pipeline_mgr "github.com/couchbase/goxdcr/v8/pipeline_manager/mocks"
	service_def_real "github.com/couchbase/goxdcr/v8/service_def"
	service_def "github.com/couchbase/goxdcr/v8/service_def/mocks"
	"github.com/couchbase/goxdcr/v8/service_impl"
	utilsMock "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

func setupBoilerPlate() (*service_def.CollectionsManifestSvc, *service_def.ReplicationSpecSvc, *service_def.BackfillReplSvc, *pipeline_mgr.PipelineMgrBackfillIface, *service_def.XDCRCompTopologySvc, *service_def.CheckpointsService, *service_def.BucketTopologySvc, *utilsMock.UtilsIface) {
	manifestSvc := &service_def.CollectionsManifestSvc{}
	replSpecSvc := &service_def.ReplicationSpecSvc{}
	backfillReplSvc := &service_def.BackfillReplSvc{}
	pipelineMgr := &pipeline_mgr.PipelineMgrBackfillIface{}
	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	checkpointSvcMock := &service_def.CheckpointsService{}
	bucketTopologySvc := &service_def.BucketTopologySvc{}
	utils := &utilsMock.UtilsIface{}

	return manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, xdcrTopologyMock, checkpointSvcMock, bucketTopologySvc, utils
}

const sourceBucketName = "sourceBucket"
const sourceBucketUUID = "sourceBucketUuid"
const targetClusterUUID = "targetClusterUuid"
const targetBucketName = "targetBucket"
const targetBucketUUID = "targetBucketUuid"

var defaultSeqnoGetter = func() map[uint16]uint64 {
	retMap := make(map[uint16]uint64)
	for i := uint16(0); i < 1024; i++ {
		retMap[i] = 100
	}
	return retMap
}

const vbsNodeName = "localhost:9000"

var vbsGetter = func(customList []uint16) *base.KvVBMapType {
	retMap := make(base.KvVBMapType)

	var list []uint16
	if customList == nil {
		for i := uint16(0); i < 1024; i++ {
			list = append(list, i)
		}
	} else {
		list = customList
	}
	retMap[vbsNodeName] = list
	return &retMap
}

type backfillMgrTestUtilsRetStruct struct {
	err error
}

func setupMock(manifestSvc *service_def.CollectionsManifestSvc, replSpecSvc *service_def.ReplicationSpecSvc, pipelineMgr *pipeline_mgr.PipelineMgrBackfillIface, xdcrTopologyMock *service_def.XDCRCompTopologySvc, checkpointSvcMock *service_def.CheckpointsService, seqnoGetter func() map[uint16]uint64, localVBMapGetter func([]uint16) *base.KvVBMapType, backfillReplSvc *service_def.BackfillReplSvc, additionalSpecIds []string, bucketTopologySvc *service_def.BucketTopologySvc, pipelineStopCb base.StoppedPipelineCallback, pipelineStopErrCb base.StoppedPipelineErrCallback, utils *utilsMock.UtilsIface, expoRetMap map[string]*backfillMgrTestUtilsRetStruct) *metadata.ReplicationSpecification {

	returnedSpec, _ := metadata.NewReplicationSpecification(sourceBucketName, sourceBucketUUID, targetClusterUUID, targetBucketName, targetBucketUUID)
	var specList = []string{returnedSpec.Id}
	for _, extraId := range additionalSpecIds {
		specList = append(specList, extraId)
	}

	defaultManifest := metadata.NewDefaultCollectionsManifest()

	manifestSvc.On("SetMetadataChangeHandlerCallback", mock.Anything).Return(nil)
	manifestSvc.On("GetLatestManifests", mock.Anything, mock.Anything).Return(&defaultManifest, &defaultManifest, nil)
	replSpecSvc.On("SetMetadataChangeHandlerCallback", mock.Anything).Return(nil)
	replSpecSvc.On("ReplicationSpec", mock.Anything).Return(returnedSpec, nil)
	replSpecSvc.On("AllReplicationSpecIds").Return(specList, nil)
	pipelineMgr.On("GetMainPipelineThroughSeqnos", mock.Anything).Return(seqnoGetter(), nil)
	pipelineMgr.On("BackfillMappingStatusUpdate", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	pipelineMgr.On("RequestBackfill", mock.Anything).Return(nil)
	xdcrTopologyMock.On("MyKVNodes").Return([]string{"localhost:9000"}, nil)
	checkpointSvcMock.On("CheckpointsDocs", mock.Anything, mock.Anything).Return(nil, base.ErrorNotFound)

	sourceCh := make(chan service_def_real.SourceNotification, base.BucketTopologyWatcherChanLen)
	var vbsList []uint16
	for i := uint16(0); i < base.NumberOfVbs; i++ {
		vbsList = append(vbsList, i)
	}
	srcNotification := getDefaultSourceNotification(vbsList)
	go func() {
		for i := 0; i < 50; i++ {
			sourceCh <- srcNotification
			time.Sleep(100 * time.Millisecond)
		}
	}()
	bucketTopologySvc.On("SubscribeToLocalBucketFeed", mock.Anything, mock.Anything).Return(sourceCh, nil)
	bucketTopologySvc.On("UnSubscribeLocalBucketFeed", mock.Anything, mock.Anything).Return(nil)
	bucketTopologySvc.On("RegisterGarbageCollect", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	checkpointSvcMock.On("CheckpointsDocs", mock.Anything, mock.Anything).Return(nil, base.ErrorNotFound)
	if pipelineStopCb != nil && pipelineStopErrCb != nil {
		checkpointSvcMock.On("GetCkptsMappingsCleanupCallback", mock.Anything, mock.Anything, mock.Anything).Return(pipelineStopCb, pipelineStopErrCb)
		pipelineMgr.On("HaltBackfillWithCb", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			pipelineStopCb()
		}).Return(nil)
	}
	setupBackfillReplSvcMock(backfillReplSvc)

	for k, v := range expoRetMap {
		utils.On("ExponentialBackoffExecutor", k, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(v.err)
		utils.On("ExponentialBackoffExecutorWithFinishSignal", k, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, v.err)
	}
	return returnedSpec
}

var topologyObjPool = service_impl.NewBucketTopologyObjsPool()

func getDefaultSourceNotification(customVBsList []uint16) service_def_real.SourceNotification {
	notification := service_impl.NewNotification(true, topologyObjPool)
	notification.NumberOfSourceNodes = 1
	notification.SourceVBMap = vbsGetter(customVBsList)
	notification.KvVbMap = vbsGetter(customVBsList)
	notification.SetNumberOfReaders(50)
	//notification := &service_impl.Notification{
	//	Source:              true,
	//	NumberOfSourceNodes: 1,
	//	SourceVBMap:         vbsGetter(customVBsList),
	//	KvVbMap:             nil,
	//	NumReaders:          1,
	//}
	return notification
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
		bSpec := metadata.NewBackfillReplicationSpec(specId, "dummy", vbTaskMap, spec, 0)
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
	manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, bucketTopologySvc, utils := setupBoilerPlate()
	setupReplStartupSpecs(replSpecSvc, nil)
	setupBackfillSpecs(backfillReplSvc, nil)
	setupMock(manifestSvc, replSpecSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, defaultSeqnoGetter, vbsGetter, backfillReplSvc, nil, bucketTopologySvc, nil, nil, nil, nil)

	backfillMgr := NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, bucketTopologySvc, utils)
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
	manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, bucketTopologySvc, utils := setupBoilerPlate()
	specs, manifestPairs := setupStartupSpecs(5)
	setupReplStartupSpecs(replSpecSvc, specs)
	setupBackfillSpecs(backfillReplSvc, specs)
	setupStartupManifests(manifestSvc, specs, manifestPairs)
	setupMock(manifestSvc, replSpecSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, defaultSeqnoGetter, vbsGetter, backfillReplSvc, nil, bucketTopologySvc, nil, nil, nil, nil)

	backfillMgr := NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, bucketTopologySvc, utils)
	assert.NotNil(backfillMgr)

	assert.Nil(backfillMgr.Start())

	fmt.Println("============== Test case end: TestBackfillMgrLaunchSpecs =================")
}

func TestBackfillMgrLaunchSpecsWithErr(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillMgrLaunchSpecsWithErr =================")
	manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, bucketTopologySvc, utils := setupBoilerPlate()
	specs, manifestPairs := setupStartupSpecs(5)

	// Delete the 3rd one to simulate error
	delete(manifestPairs, getSpecId(3))

	setupReplStartupSpecs(replSpecSvc, specs)
	setupBackfillSpecs(backfillReplSvc, specs)
	setupStartupManifests(manifestSvc, specs, manifestPairs)

	utilsExpoRetMap := make(map[string]*backfillMgrTestUtilsRetStruct)
	utilsExpoRetMap[fmt.Sprintf("retrieveLastPersistedManifest.getAgent(%s)", getSpecId(3))] = &backfillMgrTestUtilsRetStruct{
		err: fmt.Errorf("dummy"),
	}
	setupMock(manifestSvc, replSpecSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, defaultSeqnoGetter, vbsGetter, backfillReplSvc, nil, bucketTopologySvc, nil, nil, utils, utilsExpoRetMap)

	backfillMgr := NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, bucketTopologySvc, utils)
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
	manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, bucketTopologySvc, utils := setupBoilerPlate()
	specs, manifestPairs := setupStartupSpecs(5)

	setupReplStartupSpecs(replSpecSvc, specs)
	setupBackfillSpecs(backfillReplSvc, specs)
	setupStartupManifests(manifestSvc, specs, manifestPairs)
	specId := "RandId_0"
	var additionalSpecIDs []string = []string{specId}
	setupMock(manifestSvc, replSpecSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, defaultSeqnoGetter, vbsGetter, backfillReplSvc, additionalSpecIDs, bucketTopologySvc, nil, nil, nil, nil)

	backfillMgr := NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, bucketTopologySvc, utils)
	assert.NotNil(backfillMgr)

	assert.Nil(backfillMgr.Start())

	time.Sleep(1 * time.Second)

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
	time.Sleep(100 * time.Millisecond)

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
	time.Sleep(100 * time.Millisecond)
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
	manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, bucketTopologySvc, utils := setupBoilerPlate()
	specs, manifestPairs := setupStartupSpecs(5)

	setupReplStartupSpecs(replSpecSvc, specs)
	setupBackfillSpecs(backfillReplSvc, specs)
	setupStartupManifests(manifestSvc, specs, manifestPairs)
	specId := "RandId_0"
	var additionalSpecIDs []string = []string{specId}
	setupMock(manifestSvc, replSpecSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, defaultSeqnoGetter, vbsGetter, backfillReplSvc, additionalSpecIDs, bucketTopologySvc, nil, nil, nil, nil)

	backfillMgr := NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, bucketTopologySvc, utils)
	assert.NotNil(backfillMgr)
	backfillMgr.retryTimerPeriod = 500 * time.Millisecond

	assert.Nil(backfillMgr.Start())

	// add some tasks
	task1 := BackfillRetryRequest{
		replId: specId,
		req: metadata.CollectionNamespaceMappingsDiffPair{
			Added:                      nil,
			Removed:                    nil,
			CorrespondingSrcManifestId: 2,
		},
		force:                      false,
		correspondingSrcManifestId: 2,
		handler:                    nil, // will get internally
	}
	task2 := BackfillRetryRequest{
		replId: specId,
		req: metadata.CollectionNamespaceMappingsDiffPair{
			Added:                      nil,
			Removed:                    nil,
			CorrespondingSrcManifestId: 1,
		},
		force:                      false,
		correspondingSrcManifestId: 1,
		handler:                    nil, // will get internally
	}
	task3 := BackfillRetryRequest{
		replId: specId,
		req: metadata.CollectionNamespaceMappingsDiffPair{
			Added:                      nil,
			Removed:                    nil,
			CorrespondingSrcManifestId: 1,
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
	_, _, backfillReplSvcBad, _, _, _, bucketTopologySvc, utils := setupBoilerPlate()
	//setupReplStartupSpecs(replSpecSvc, specs)
	setupBackfillSpecs(backfillReplSvcBad, specs)
	setupBackfillReplSvcNegMock(backfillReplSvcBad)
	setupMock(manifestSvc, replSpecSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, defaultSeqnoGetter, vbsGetter, backfillReplSvc, additionalSpecIDs, bucketTopologySvc, nil, nil, nil, nil)

	backfillMgr = NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvcBad, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, bucketTopologySvc, utils)
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

func TestBackfillMgrLaunchSpecsThenPeers(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillMgrLaunchSpecsThenPeers =================")
	defer fmt.Println("============== Test case end: TestBackfillMgrLaunchSpecsThenPeers =================")
	manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, bucketTopologySvc, utils := setupBoilerPlate()
	specs, manifestPairs := setupStartupSpecs(5)
	setupReplStartupSpecs(replSpecSvc, specs)
	setupBackfillSpecs(backfillReplSvc, specs)
	setupStartupManifests(manifestSvc, specs, manifestPairs)
	setupMock(manifestSvc, replSpecSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, defaultSeqnoGetter, vbsGetter, backfillReplSvc, nil, bucketTopologySvc, nil, nil, nil, nil)

	backfillMgr := NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, bucketTopologySvc, utils)
	assert.NotNil(backfillMgr)

	assert.Nil(backfillMgr.Start())

	var specIdToUse string
	var specToUse *metadata.ReplicationSpecification
	for specId, oneSpec := range specs {
		specIdToUse = specId
		specToUse = oneSpec
		break
	}

	resp := &peerToPeer.VBMasterCheckResp{
		ResponseCommon:     peerToPeer.NewResponseCommon(peerToPeer.ReqVBMasterChk, "", "", uint32(6), ""),
		ReplicationPayload: peerToPeer.NewReplicationPayload(specId, specToUse.SourceBucketName, ""),
	}

	_, tasks0 := getTaskForVB0(specToUse.SourceBucketName)
	tasks1 := tasks0.Clone()

	vbTaskMap := metadata.NewVBTasksMap()
	vbTaskMap.VBTasksMap[0] = tasks0
	vbTaskMap.VBTasksMap[1] = tasks1
	assert.NotEqual(0, len(tasks0.GetAllCollectionNamespaceMappings()))

	resp.Init()
	resp.InitBucket(specToUse.SourceBucketName)
	bucketVBMapType, unlockFunc := resp.GetReponse()
	(*bucketVBMapType)[specToUse.SourceBucketName].RegisterNotMyVBs([]uint16{0})
	unlockFunc()
	assert.Nil(resp.LoadBackfillTasks(vbTaskMap, specToUse.SourceBucketName, 1))
	checkResp, unlockFunc := resp.GetReponse()
	payload := (*checkResp)[specToUse.SourceBucketName]
	vb0Tasks := payload.GetBackfillVBTasks().VBTasksMap[0]
	manifestId := payload.GetBackfillVBTasksManifestsId()
	unlockFunc()
	assert.NotEqual(0, vb0Tasks.Len())
	assert.Equal(uint64(1), manifestId)

	peersMap := make(peerToPeer.PeersVBMasterCheckRespMap)
	peersMap["dummyNode"] = resp
	settingsMap := make(metadata.ReplicationSettingsMap)
	settingsMap[base.NameKey] = specIdToUse
	settingsMap[peerToPeer.MergeBackfillKey] = peersMap
	assert.Nil(backfillMgr.GetPipelineSvc().UpdateSettings(settingsMap))

}

func getTaskForVB0(srcBucketName string) (*metadata.ReplicationSpecification, *metadata.BackfillTasks) {
	collectionNs := make(metadata.CollectionNamespaceMapping)
	ns1 := &base.CollectionNamespace{
		ScopeName:      "s1",
		CollectionName: "col1",
	}
	collectionNs.AddSingleMapping(ns1, ns1)

	emptySpec, _ := metadata.NewReplicationSpecification(srcBucketName, "", "", "", "")
	ts0 := &metadata.BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{
			Vbno:  0,
			Seqno: 0,
		},
		EndingTimestamp: &base.VBTimestamp{
			Vbno:  0,
			Seqno: 1000,
		},
	}
	task0 := metadata.NewBackfillTask(ts0, []metadata.CollectionNamespaceMapping{collectionNs})
	taskList := metadata.NewBackfillTasksWithTask(task0)
	return emptySpec, &taskList
}

// This test isn't identical to the MB it is trying to reproduce (MB-55354)
// but it is close enough. The idea here is that we have all the following happening concurrently
//  1. A replication spec that is going from paused to active
//  2. A collection mapping rules change that has to occur
//  3. Collection backfill tasks have more namespace (i.e. originally contained S1.col1 but now need to backfill S1.col2)
//     which means the backfill replication needs to be updated
//
// What happens now is that the two things happen in parallel:
//
// First,  1 + 2 causes replication spec change callback to occur. This will cause
// https://github.com/couchbase/goxdcr/blob/a8a1a3d2848fe85da7bb2fb40055f56ef707ef2c/replication_manager/metakv_change_listener.go#L258-L259
// to hit.
// Which eventually means https://github.com/couchbase/goxdcr/blob/a8a1a3d2848fe85da7bb2fb40055f56ef707ef2c/replication_manager/metakv_change_listener.go#L292-L293
// gets hit as well, leading to https://github.com/couchbase/goxdcr/blob/a8a1a3d2848fe85da7bb2fb40055f56ef707ef2c/backfill_manager/backfill_manager.go#L1377
//
// Secondly, 1 + 3 causes this https://github.com/couchbase/goxdcr/blob/a8a1a3d2848fe85da7bb2fb40055f56ef707ef2c/backfill_manager/backfill_manager.go#L534-L548
// to occur. And this block will require a pipeline stop and callback mechanism to fire
//
// The first path will require backfill_request_handler to be free to handle the necessary work
// But, the second path is unable to execute because it's waiting for the pipeline updater to execute it. While
// this is happening, this means that backfill_request_handler is busy and unable to handle the first path
func TestBackfillMgrSpecChangeWithNamespaceChange(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillMgrSpecChangeWithNamespaceChange =================")
	defer fmt.Println("============== Test case end: TestBackfillMgrSpecChangeWithNamespaceChange =================")
	manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, bucketTopologySvc, utils := setupBoilerPlate()
	specs, manifestPairs := setupStartupSpecs(5)
	setupReplStartupSpecs(replSpecSvc, specs)
	setupBackfillSpecs(backfillReplSvc, specs)
	setupStartupManifests(manifestSvc, specs, manifestPairs)

	var ckptMappingsCleanupCallback base.StoppedPipelineCallback = func() error {
		// Sleeping 10 seconds here to simulate pipelineMgr busy trying to raise a backfill while backfill
		// request handler is currently calling backfillReplSpecChangeHandlerCallback
		time.Sleep(10 * time.Second)
		return nil
	}
	var ckptMappingErrCallback base.StoppedPipelineErrCallback = func(err error, cbCalled bool) {

	}
	utilsExpoRetMap := make(map[string]*backfillMgrTestUtilsRetStruct)
	utilsExpoRetMap["explicitMapChange"] = &backfillMgrTestUtilsRetStruct{
		err: nil,
	}
	generatedSpec := setupMock(manifestSvc, replSpecSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, defaultSeqnoGetter, vbsGetter, backfillReplSvc, nil, bucketTopologySvc, ckptMappingsCleanupCallback, ckptMappingErrCallback, utils, utilsExpoRetMap)

	backfillMgr := NewBackfillManager(manifestSvc, replSpecSvc, backfillReplSvc, pipelineMgr, xdcrCompTopologySvc, checkpointSvcMock, bucketTopologySvc, utils)
	assert.NotNil(backfillMgr)

	assert.Nil(backfillMgr.Start())

	id1 := getSpecId(0)
	spec1 := specs[id1]
	vbtaskMap := metadata.NewVBTasksMap()
	vbtasks := metadata.NewBackfillTasks()
	src1 := metadata.NewSourceCollectionNamespace(&base.CollectionNamespace{
		ScopeName:      "s1",
		CollectionName: "col1",
	})
	tgt1 := &base.CollectionNamespace{
		ScopeName:      "s1",
		CollectionName: "col1",
	}
	task := metadata.NewBackfillTask(
		&metadata.BackfillVBTimestamps{
			StartingTimestamp: &base.VBTimestamp{Vbno: 0, Seqno: 1},
			EndingTimestamp:   &base.VBTimestamp{Vbno: 0, Seqno: 10},
		},
		[]metadata.CollectionNamespaceMapping{
			map[*metadata.SourceNamespace]metadata.CollectionNamespaceList{
				src1: metadata.CollectionNamespaceList{tgt1},
			},
		},
	)
	vbtasks.List = append(vbtasks.List, task)
	vbtaskMap.VBTasksMap[0] = &vbtasks

	src2 := metadata.NewSourceCollectionNamespace(&base.CollectionNamespace{
		ScopeName:      "s1",
		CollectionName: "col2",
	})
	tgt2 := &base.CollectionNamespace{
		ScopeName:      "s1",
		CollectionName: "col2",
	}
	task2 := metadata.NewBackfillTask(
		&metadata.BackfillVBTimestamps{
			StartingTimestamp: &base.VBTimestamp{Vbno: 0, Seqno: 1},
			EndingTimestamp:   &base.VBTimestamp{Vbno: 0, Seqno: 10},
		},
		[]metadata.CollectionNamespaceMapping{
			map[*metadata.SourceNamespace]metadata.CollectionNamespaceList{
				src2: metadata.CollectionNamespaceList{tgt2},
			},
		},
	)

	vbTaskMapModified := vbtaskMap.Clone()
	vbTaskMapModified.VBTasksMap[0].List[0] = task2
	spec1.Settings.Active = true
	oldBackfillSpec := metadata.NewBackfillReplicationSpec(spec1.Id, spec1.InternalId, vbtaskMap, spec1, 0)
	newBackfillSpec := metadata.NewBackfillReplicationSpec(spec1.Id, spec1.InternalId, vbTaskMapModified, spec1, 0)

	var mode base.CollectionsMgtType
	mode.SetExplicitMapping(true)

	oldSetting := spec1.Settings.Clone()
	oldSetting.Values[metadata.CollectionsMgtMultiKey] = mode
	routingRules := make(metadata.CollectionsMappingRulesType)
	routingRules[src1.ToIndexString()] = tgt1.ToIndexString()
	oldSetting.Values[metadata.CollectionsMappingRulesKey] = routingRules
	oldSetting.Active = false

	newSetting := oldSetting.Clone()
	newRoutingRules := make(metadata.CollectionsMappingRulesType)
	newRoutingRules[src2.ToIndexString()] = tgt2.ToIndexString()
	newSetting.Values[metadata.CollectionsMappingRulesKey] = newRoutingRules
	newSetting.Active = true

	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	startTime := time.Now()
	go func() {
		backfillMgr.backfillReplSpecChangeHandlerCallback(id1, oldBackfillSpec, newBackfillSpec)
		waitGroup.Done()
	}()
	waitGroup.Wait()

	fmt.Printf("1: elapsed %v\n", time.Since(startTime).Seconds())
	handlerCb, _ := backfillMgr.GetExplicitMappingChangeHandler(spec1.Id, generatedSpec.InternalId, oldSetting, newSetting)
	handlerCb()
	fmt.Printf("2: elapsed %v\n", time.Since(startTime).Seconds())

	elapsed := time.Since(startTime)
	assert.True(int(elapsed.Seconds()) < 3)
}
