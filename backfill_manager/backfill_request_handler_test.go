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
	commonReal "github.com/couchbase/goxdcr/common"
	common "github.com/couchbase/goxdcr/common/mocks"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/parts"
	pipeline_svc "github.com/couchbase/goxdcr/pipeline_svc/mocks"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"io/ioutil"
	"sync"
	"testing"
	"time"
)

func setupBRHBoilerPlate() (*log.CommonLogger, *service_def.BackfillReplSvc) {
	logger := log.NewLogger("BackfillReqHandler", log.DefaultLoggerContext)
	backfillReplSvc := &service_def.BackfillReplSvc{}
	return logger, backfillReplSvc
}

func createSeqnoGetterFunc(topSeqno uint64) SeqnosGetter {
	highMap := make(map[uint16]uint64)
	var vb uint16
	for vb = 0; vb < 1024; vb++ {
		highMap[vb] = topSeqno
	}

	getterFunc := func() (map[uint16]uint64, error) {
		return highMap, nil
	}
	return getterFunc
}

func createVBsGetter() MyVBsGetter {
	var allVBs []uint16
	var vb uint16
	for vb = 0; vb < 1024; vb++ {
		allVBs = append(allVBs, vb)
	}
	getterFunc := func() ([]uint16, error) {
		return allVBs, nil
	}
	return getterFunc
}

func createVBDoneFunc() MyVBsTasksDoneNotifier {
	notifierFunc := func(startNewTask bool) {
		fmt.Printf("Notifierfunc called\n")
	}
	return notifierFunc
}

const SourceBucketName = "sourceBucketName"
const SourceBucketUUID = "sourceBucketUUID"
const TargetClusterUUID = "targetClusterUUID"
const TargetBucketName = "targetBucket"
const TargetBucketUUID = "targetBucketUUID"
const specId = "testSpec"
const specInternalId = "testSpecInternal"

func createTestSpec() *metadata.ReplicationSpecification {
	return &metadata.ReplicationSpecification{Id: specId, InternalId: specInternalId,
		SourceBucketName:  SourceBucketName,
		SourceBucketUUID:  SourceBucketUUID,
		TargetClusterUUID: TargetClusterUUID,
		TargetBucketName:  TargetBucketName,
		TargetBucketUUID:  TargetBucketUUID,
		Settings:          metadata.DefaultReplicationSettings(),
	}
}

func brhMockSourceNozzles() map[string]commonReal.Nozzle {
	nozzleId := "dummyDCP"
	dcpNozzle := &common.Nozzle{}
	dcpNozzle.On("Id").Return(nozzleId)
	dcpNozzle.On("AsyncComponentEventListeners").Return(nil)
	dcpNozzle.On("Connector").Return(nil)
	dcpNozzle.On("RegisterComponentEventListener", mock.Anything, mock.Anything).Return(nil)
	// One nozzle responsible for 1024 vb's
	var vbsList []uint16
	for i := uint16(0); i < base.NumberOfVbs; i++ {
		vbsList = append(vbsList, i)
	}
	dcpNozzle.On("ResponsibleVBs").Return(vbsList)

	retMap := make(map[string]commonReal.Nozzle)
	retMap[nozzleId] = dcpNozzle
	return retMap
}

func brhMockFakePipeline(sourcesMap map[string]commonReal.Nozzle, pipelineState commonReal.PipelineState, ctx *common.PipelineRuntimeContext) (*common.Pipeline, *common.Pipeline) {
	pipeline := &common.Pipeline{}

	pipeline.On("GetAsyncListenerMap").Return(nil)
	pipeline.On("Sources").Return(sourcesMap)
	pipeline.On("SetAsyncListenerMap", mock.Anything).Return(nil)
	pipeline.On("State").Return(pipelineState)
	pipeline.On("RuntimeContext").Return(ctx)
	pipeline.On("FullTopic").Return(unitTestFullTopic)
	pipeline.On("Type").Return(commonReal.MainPipeline)

	backfillPipeline := &common.Pipeline{}

	backfillPipeline.On("GetAsyncListenerMap").Return(nil)
	backfillPipeline.On("Sources").Return(sourcesMap)
	backfillPipeline.On("SetAsyncListenerMap", mock.Anything).Return(nil)
	backfillPipeline.On("State").Return(pipelineState)
	backfillPipeline.On("RuntimeContext").Return(ctx)
	backfillPipeline.On("FullTopic").Return(unitTestFullTopic)
	backfillPipeline.On("Type").Return(commonReal.BackfillPipeline)

	return pipeline, backfillPipeline
}

func brhMockCkptMgr() *pipeline_svc.CheckpointMgrSvc {
	ckptMgr := &pipeline_svc.CheckpointMgrSvc{}
	ckptMgr.On("DelSingleVBCheckpoint", unitTestFullTopic, mock.Anything).Return(nil)
	return ckptMgr
}

func brhMockSupervisor() *pipeline_svc.PipelineSupervisorSvc {
	supervisor := &pipeline_svc.PipelineSupervisorSvc{}
	supervisor.On("OnEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	return supervisor
}

func brhMockPipelineContext(ckptMgr *pipeline_svc.CheckpointMgrSvc,
	supervisor *pipeline_svc.PipelineSupervisorSvc) *common.PipelineRuntimeContext {

	ctx := &common.PipelineRuntimeContext{}
	ctx.On("Service", base.CHECKPOINT_MGR_SVC).Return(ckptMgr)
	ctx.On("Service", base.PIPELINE_SUPERVISOR_SVC).Return(supervisor)
	return ctx
}

func brhMockBackfillReplSvcCommon(svc *service_def.BackfillReplSvc) {
	svc.On("BackfillReplSpec", mock.Anything).Return(nil, base.ErrorNotFound)
}

var unitTestFullTopic string = "BackfillReqHandlerFullTopic"

func TestBackfillReqHandlerStartStop(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillReqHandlerStartStop =================")
	logger, backfillReplSvc := setupBRHBoilerPlate()
	rh := NewCollectionBackfillRequestHandler(logger, specId, backfillReplSvc, createTestSpec(), createSeqnoGetterFunc(100), createVBsGetter(), time.Second, createVBDoneFunc(), nil, nil, nil)
	backfillReplSvc.On("BackfillReplSpec", mock.Anything).Return(nil, base.ErrorNotFound)
	brhMockBackfillReplSvcCommon(backfillReplSvc)

	assert.NotNil(rh)
	assert.Nil(rh.Start())

	time.Sleep(100 * time.Millisecond)

	componentErr := make(chan base.ComponentError, 1)
	var waitGrp sync.WaitGroup

	// Stopping
	waitGrp.Add(1)
	rh.Stop(&waitGrp, componentErr)

	retVal := <-componentErr
	assert.NotNil(retVal)
	assert.Equal(specId, retVal.ComponentId)
	assert.Nil(retVal.Err)

	fmt.Println("============== Test case end: TestBackfillReqHandlerStartStop =================")
}

func TestBackfillReqHandlerCreateReqThenMarkDone(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillReqHandlerCreateReqThenMarkDone =================")
	logger, _ := setupBRHBoilerPlate()
	spec := createTestSpec()
	vbsGetter := createVBsGetter()
	seqnoGetter := createSeqnoGetterFunc(100)
	var addCount int
	var setCount int
	// Make a dummy namespacemapping
	collectionNs := &base.CollectionNamespace{base.DefaultScopeCollectionName, base.DefaultScopeCollectionName}
	dummyNs := &base.CollectionNamespace{"dummy", "dummy"}
	requestMapping := make(metadata.CollectionNamespaceMapping)
	requestMapping.AddSingleMapping(collectionNs, collectionNs)

	backfillReplSvc := &service_def.BackfillReplSvc{}
	brhMockBackfillReplSvcCommon(backfillReplSvc)
	backfillReplSvc.On("AddBackfillReplSpec", mock.Anything).Run(func(args mock.Arguments) { (addCount)++ }).Return(nil)
	backfillReplSvc.On("SetBackfillReplSpec", mock.Anything).Run(func(args mock.Arguments) { (setCount)++ }).Return(nil)

	rh := NewCollectionBackfillRequestHandler(logger, specId, backfillReplSvc, spec, seqnoGetter, vbsGetter, 500*time.Millisecond, createVBDoneFunc(), nil, nil, nil)

	assert.NotNil(rh)
	assert.Nil(rh.Start())

	srcNozzleMap := brhMockSourceNozzles()
	ckptMgr := brhMockCkptMgr()
	supervisor := brhMockSupervisor()
	ctx := brhMockPipelineContext(ckptMgr, supervisor)
	pipeline, backfillPipeline := brhMockFakePipeline(srcNozzleMap, commonReal.Pipeline_Running, ctx)
	assert.Nil(rh.Attach(pipeline))

	// Wait for the go-routine to start
	time.Sleep(10 * time.Millisecond)

	// Calling twice in a row with the same result will result in a single add
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	var err1 error
	var err2 error
	go func() {
		err1 = rh.HandleBackfillRequest(requestMapping)
		waitGroup.Done()
	}()
	go func() {
		err2 = rh.HandleBackfillRequest(requestMapping)
		waitGroup.Done()
	}()
	waitGroup.Wait()

	// At least one is going to be nil and one is going to be duplicate
	assert.True(err1 == nil && err2 == nil)

	// Test cool down period is active
	startTime := time.Now()

	time.Sleep(100 * time.Millisecond)

	// Doing another handle will result in a set
	// Change requestMapping to avoid errorDuplicate
	requestMapping.AddSingleMapping(dummyNs, dummyNs)
	assert.Nil(rh.HandleBackfillRequest(requestMapping))

	time.Sleep(100 * time.Millisecond)

	// Two bursty, concurrent add requests results in a single add
	assert.Equal(1, addCount)
	// One later set request should result in either a single set or two sets
	assert.True(setCount == 1 || setCount == 2)

	// Pretend backfill pipeline started
	assert.Nil(rh.Attach(backfillPipeline))

	assert.Equal(1024, rh.cachedBackfillSpec.VBTasksMap.Len())
	assert.Equal(2, rh.cachedBackfillSpec.VBTasksMap.VBTasksMap[0].Len())
	assert.Nil(rh.HandleVBTaskDone(0))
	time.Sleep(100 * time.Millisecond)
	assert.Equal(1024, rh.cachedBackfillSpec.VBTasksMap.Len())
	assert.Equal(1, rh.cachedBackfillSpec.VBTasksMap.VBTasksMap[0].Len())

	endTime := time.Now()

	// With 2 cooldown periods of 500ms each, this should be > 1 second
	assert.True(endTime.Sub(startTime).Seconds() > 1)
	fmt.Println("============== Test case stop: TestBackfillReqHandlerCreateReqThenMarkDone =================")
}

func TestBackfillReqHandlerCreateReqThenMarkDoneThenDel(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillReqHandlerCreateReqThenMarkDoneThenDel =================")
	logger, _ := setupBRHBoilerPlate()
	spec := createTestSpec()
	vbsGetter := createVBsGetter()
	seqnoGetter := createSeqnoGetterFunc(100)
	var addCount int
	var setCount int
	var delCount int
	// Make a dummy namespacemapping
	collectionNs := &base.CollectionNamespace{base.DefaultScopeCollectionName, base.DefaultScopeCollectionName}
	//	dummyNs := &base.CollectionNamespace{"dummy", "dummy"}
	requestMapping := make(metadata.CollectionNamespaceMapping)
	requestMapping.AddSingleMapping(collectionNs, collectionNs)

	backfillReplSvc := &service_def.BackfillReplSvc{}
	brhMockBackfillReplSvcCommon(backfillReplSvc)
	backfillReplSvc.On("AddBackfillReplSpec", mock.Anything).Run(func(args mock.Arguments) { (addCount)++ }).Return(nil)
	backfillReplSvc.On("SetBackfillReplSpec", mock.Anything).Run(func(args mock.Arguments) { (setCount)++ }).Return(nil)
	backfillReplSvc.On("DelBackfillReplSpec", mock.Anything).Run(func(args mock.Arguments) { (delCount)++ }).Return(nil, nil)

	rh := NewCollectionBackfillRequestHandler(logger, specId, backfillReplSvc, spec, seqnoGetter, vbsGetter, 500*time.Millisecond, createVBDoneFunc(), nil, nil, nil)

	assert.NotNil(rh)
	assert.Nil(rh.Start())

	srcNozzleMap := brhMockSourceNozzles()
	ckptMgr := brhMockCkptMgr()
	supervisor := brhMockSupervisor()
	ctx := brhMockPipelineContext(ckptMgr, supervisor)
	pipeline, backfillPipeline := brhMockFakePipeline(srcNozzleMap, commonReal.Pipeline_Running, ctx)
	assert.Nil(rh.Attach(pipeline))
	assert.Nil(rh.Attach(backfillPipeline))

	// Wait for the go-routine to start
	time.Sleep(10 * time.Millisecond)

	// Manually put requestMapping into the channel
	var reqAndResp ReqAndResp
	reqAndResp.Request = requestMapping
	reqAndResp.PersistResponse = make(chan error, 1)
	reqAndResp.HandleResponse = make(chan error, 1)

	rh.incomingReqCh <- reqAndResp
	err1 := <-reqAndResp.HandleResponse
	assert.Nil(err1)
	err2 := <-reqAndResp.PersistResponse
	assert.Nil(err2)
	assert.Equal(1024, rh.cachedBackfillSpec.VBTasksMap.Len())

	// Test has 1024 VB's
	var handleResponses [1024]chan error
	var persistResponses [1024]chan error
	var waitGrp sync.WaitGroup
	for i := uint16(0); i < 1024; i++ {
		iCopy := i
		waitGrp.Add(1)
		go func() {
			defer waitGrp.Done()
			var reqAndResp ReqAndResp
			reqAndResp.Request = iCopy
			reqAndResp.HandleResponse = make(chan error, 1)
			reqAndResp.PersistResponse = make(chan error, 1)
			handleResponses[iCopy] = reqAndResp.HandleResponse
			persistResponses[iCopy] = reqAndResp.PersistResponse
			rh.doneTaskCh <- reqAndResp
		}()
	}
	waitGrp.Wait()

	var nilErrCnt int
	var syncDelCnt int
	for i := uint16(0); i < 1024; i++ {
		assert.Nil(<-persistResponses[i])
		taskResult := <-handleResponses[i]
		if taskResult == nil {
			nilErrCnt++
		} else if taskResult == errorSyncDel {
			syncDelCnt++
		}
	}
	assert.Equal(1023, nilErrCnt)
	assert.Equal(1, syncDelCnt)

	assert.Equal(0, setCount)
	assert.Equal(1, addCount)
	assert.Equal(1, delCount)

	assert.Nil(rh.cachedBackfillSpec)

	// After delete, the next handle will be an add
	reqAndResp.Request = requestMapping
	reqAndResp.PersistResponse = make(chan error)
	reqAndResp.HandleResponse = make(chan error)

	rh.incomingReqCh <- reqAndResp
	err1 = <-reqAndResp.HandleResponse
	assert.Nil(err1)
	err2 = <-reqAndResp.PersistResponse
	assert.Nil(err2)
	assert.Equal(1024, rh.cachedBackfillSpec.VBTasksMap.Len())

	assert.Equal(0, setCount)
	assert.Equal(2, addCount)
	assert.Equal(1, delCount)
	fmt.Println("============== Test case end: TestBackfillReqHandlerCreateReqThenMarkDoneThenDel =================")
}

func TestBackfillHandlerExplicitMapChange(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillHandlerExplicitMapChange =================")
	defer fmt.Println("============== Test case end: TestBackfillHandlerExplicitMapChange =================")

	logger, _ := setupBRHBoilerPlate()
	spec := createTestSpec()
	vbsGetter := createVBsGetter()
	seqnoGetter := createSeqnoGetterFunc(100)
	mainPipelineSeqnoGetter := createSeqnoGetterFunc(500)
	var addCount int
	var setCount int
	var delCount int
	// Make a dummy namespacemapping
	collectionNs := &base.CollectionNamespace{base.DefaultScopeCollectionName, base.DefaultScopeCollectionName}
	dummyNs := &base.CollectionNamespace{"dummy", "dummy"}
	requestMapping := make(metadata.CollectionNamespaceMapping)
	requestMapping.AddSingleMapping(collectionNs, dummyNs)

	backfillReplSvc := &service_def.BackfillReplSvc{}
	brhMockBackfillReplSvcCommon(backfillReplSvc)
	backfillReplSvc.On("AddBackfillReplSpec", mock.Anything).Run(func(args mock.Arguments) { (addCount)++ }).Return(nil)
	backfillReplSvc.On("SetBackfillReplSpec", mock.Anything).Run(func(args mock.Arguments) { (setCount)++ }).Return(nil)
	backfillReplSvc.On("DelBackfillReplSpec", mock.Anything).Run(func(args mock.Arguments) { (delCount)++ }).Return(nil, nil)

	rh := NewCollectionBackfillRequestHandler(logger, specId, backfillReplSvc, spec, seqnoGetter, vbsGetter, 500*time.Millisecond, createVBDoneFunc(), mainPipelineSeqnoGetter, nil, nil)

	assert.NotNil(rh)
	assert.Nil(rh.Start())

	assert.Nil(rh.cachedBackfillSpec)
	// Test remove when there's no spec
	pair := metadata.CollectionNamespaceMappingsDiffPair{
		Added:   metadata.CollectionNamespaceMapping{},
		Removed: requestMapping,
	}
	err := rh.HandleBackfillRequest(pair)
	assert.Nil(err)
	assert.Nil(rh.cachedBackfillSpec)

	// Test add
	pair.Added = requestMapping
	pair.Removed = metadata.CollectionNamespaceMapping{}
	err = rh.HandleBackfillRequest(pair)
	assert.Nil(err)
	assert.Equal(1024, rh.cachedBackfillSpec.VBTasksMap.Len())

	// Removed should cause the whole thing to be removed
	pair.Added = metadata.CollectionNamespaceMapping{}
	pair.Removed = requestMapping
	err = rh.HandleBackfillRequest(pair)
	assert.Nil(err)
	assert.Equal(0, rh.cachedBackfillSpec.VBTasksMap.Len())

	var info parts.CollectionsRoutingInfo
	info.ExplicitBackfillMap = pair
	var channels []interface{}
	syncCh := make(chan error)
	finCh := make(chan bool)
	channels = append(channels, syncCh)
	channels = append(channels, finCh)
	backfillEvent := commonReal.NewEvent(commonReal.FixedRoutingUpdateEvent, info, nil, channels, nil)
	go rh.OnEvent(backfillEvent)
	err = <-syncCh
	assert.Nil(err)
}

func TestHandleMigrationDiff(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestHandleMigrationDiff =================")
	defer fmt.Println("============== Test case end: TestHandleMigrationDiff =================")

	logger, _ := setupBRHBoilerPlate()
	spec := createTestSpec()
	vbsGetter := createVBsGetter()
	seqnoGetter := createSeqnoGetterFunc(100)
	mainPipelineSeqnoGetter := createSeqnoGetterFunc(500)
	var addCount int
	var setCount int
	var delCount int
	// Make a dummy namespacemapping
	collectionNs := &base.CollectionNamespace{base.DefaultScopeCollectionName, base.DefaultScopeCollectionName}
	dummyNs := &base.CollectionNamespace{"dummy", "dummy"}
	requestMapping := make(metadata.CollectionNamespaceMapping)
	requestMapping.AddSingleMapping(collectionNs, dummyNs)

	backfillReplSvc := &service_def.BackfillReplSvc{}
	brhMockBackfillReplSvcCommon(backfillReplSvc)
	backfillReplSvc.On("AddBackfillReplSpec", mock.Anything).Run(func(args mock.Arguments) { (addCount)++ }).Return(nil)
	backfillReplSvc.On("SetBackfillReplSpec", mock.Anything).Run(func(args mock.Arguments) { (setCount)++ }).Return(nil)
	backfillReplSvc.On("DelBackfillReplSpec", mock.Anything).Run(func(args mock.Arguments) { (delCount)++ }).Return(nil, nil)

	rh := NewCollectionBackfillRequestHandler(logger, specId, backfillReplSvc, spec, seqnoGetter, vbsGetter, 500*time.Millisecond, createVBDoneFunc(), mainPipelineSeqnoGetter, nil, nil)

	assert.NotNil(rh)
	assert.Nil(rh.Start())

	// Create an "Added" one"
	var provisionedFile string = testDir + "provisionedManifest.json"
	data, err := ioutil.ReadFile(provisionedFile)
	assert.Nil(err)
	target, _ := metadata.NewCollectionsManifestFromBytes(data)
	source := metadata.NewDefaultCollectionsManifest()
	manifestPair := metadata.CollectionsManifestPair{
		Source: &source,
		Target: &target,
	}

	var mappingMode base.CollectionsMgtType
	mappingMode.SetMigration(true)

	rules := make(metadata.CollectionsMappingRulesType)
	// Make a rule that says if the doc key starts with "S1_"
	rules["REGEXP_CONTAINS(META().id, \"^S1_\")"] = "S1.col1"
	rules["REGEXP_CONTAINS(META().id, \"^S2_\")"] = "S2.col1"

	explicitMap, err := metadata.NewCollectionNamespaceMappingFromRules(manifestPair, mappingMode, rules, false, true)
	assert.Nil(err)
	assert.NotNil(explicitMap)
	assert.Equal(2, len(explicitMap))

	assert.Nil(rh.cachedBackfillSpec)
	// Test remove when there's no spec
	pair := metadata.CollectionNamespaceMappingsDiffPair{
		Added:   explicitMap,
		Removed: nil,
	}
	err = rh.HandleBackfillRequest(pair)
	assert.Nil(err)
	assert.NotNil(rh.cachedBackfillSpec)
}
