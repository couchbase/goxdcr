package backfill_manager

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	commonReal "github.com/couchbase/goxdcr/common"
	common "github.com/couchbase/goxdcr/common/mocks"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	pipeline_svc "github.com/couchbase/goxdcr/pipeline_svc/mocks"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"sync"
	"testing"
	"time"
)

func setupBRHBoilerPlate() (*log.CommonLogger, *service_def.BackfillReplSvc) {
	logger := log.NewLogger("BackfillReqHandler", log.DefaultLoggerContext)
	backfillReplSvc := &service_def.BackfillReplSvc{}
	return logger, backfillReplSvc
}

func createSeqnoGetterFunc(topSeqno uint64) LatestSeqnoGetter {
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

func brhMockFakePipeline(sourcesMap map[string]commonReal.Nozzle, pipelineState commonReal.PipelineState, ctx *common.PipelineRuntimeContext) *common.Pipeline {
	pipeline := &common.Pipeline{}

	pipeline.On("GetAsyncListenerMap").Return(nil)
	pipeline.On("Sources").Return(sourcesMap)
	pipeline.On("SetAsyncListenerMap", mock.Anything).Return(nil)
	pipeline.On("State").Return(pipelineState)
	pipeline.On("RuntimeContext").Return(ctx)
	pipeline.On("FullTopic").Return(unitTestFullTopic)

	return pipeline
}

func brhMockCkptMgr() *pipeline_svc.CheckpointMgrSvc {
	ckptMgr := &pipeline_svc.CheckpointMgrSvc{}
	ckptMgr.On("DelSingleVBCheckpoint", unitTestFullTopic, mock.Anything).Return(nil)
	return ckptMgr
}

func brhMockPipelineContext(ckptMgr *pipeline_svc.CheckpointMgrSvc) *common.PipelineRuntimeContext {
	ctx := &common.PipelineRuntimeContext{}
	ctx.On("Service", base.CHECKPOINT_MGR_SVC).Return(ckptMgr)

	return ctx
}

func brhMockBackfillReplSvcCommon(svc *service_def.BackfillReplSvc) {
	svc.On("BackfillReplSpec", mock.Anything).Return(nil, base.ErrorNotFound)
}

var maxConcurrentReqs int = 4

var unitTestFullTopic string = "BackfillReqHandlerFullTopic"

func TestBackfillReqHandlerStartStop(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillReqHandlerStartStop =================")
	logger, backfillReplSvc := setupBRHBoilerPlate()
	rh := NewCollectionBackfillRequestHandler(logger, specId, backfillReplSvc, createTestSpec(), createSeqnoGetterFunc(100), createVBsGetter(), maxConcurrentReqs, time.Second)
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
	requestMapping := make(metadata.CollectionNamespaceMapping)
	requestMapping.AddSingleMapping(collectionNs, collectionNs)

	backfillReplSvc := &service_def.BackfillReplSvc{}
	brhMockBackfillReplSvcCommon(backfillReplSvc)
	backfillReplSvc.On("AddBackfillReplSpec", mock.Anything).Run(func(args mock.Arguments) { (addCount)++ }).Return(nil)
	backfillReplSvc.On("SetBackfillReplSpec", mock.Anything).Run(func(args mock.Arguments) { (setCount)++ }).Return(nil)

	rh := NewCollectionBackfillRequestHandler(logger, specId, backfillReplSvc, spec, seqnoGetter, vbsGetter, maxConcurrentReqs, 500*time.Millisecond)

	assert.NotNil(rh)
	assert.Nil(rh.Start())

	srcNozzleMap := brhMockSourceNozzles()
	ckptMgr := brhMockCkptMgr()
	ctx := brhMockPipelineContext(ckptMgr)
	pipeline := brhMockFakePipeline(srcNozzleMap, commonReal.Pipeline_Running, ctx)
	rh.Attach(pipeline)

	// Wait for the go-routine to start
	time.Sleep(10 * time.Millisecond)

	// Calling twice in a row with the same result will result in a single add
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	go func() {
		assert.Nil(rh.HandleBackfillRequest(requestMapping))
		waitGroup.Done()
	}()
	go func() {
		assert.Nil(rh.HandleBackfillRequest(requestMapping))
		waitGroup.Done()
	}()
	waitGroup.Wait()

	// Test cool down period is active
	startTime := time.Now()

	time.Sleep(100 * time.Millisecond)

	// Doing another handle will result in a set
	assert.Nil(rh.HandleBackfillRequest(requestMapping))

	time.Sleep(100 * time.Millisecond)

	// Two bursty, concurrent add requests results in a single add
	assert.Equal(1, addCount)
	// One later set request should result in a single set
	assert.Equal(1, setCount)

	assert.Equal(1024, len(rh.cachedBackfillSpec.VBTasksMap))
	assert.Equal(2, len(*(rh.cachedBackfillSpec.VBTasksMap[0])))
	assert.Nil(rh.HandleVBTaskDone(0))
	time.Sleep(100 * time.Millisecond)
	assert.Equal(1024, len(rh.cachedBackfillSpec.VBTasksMap))
	assert.Equal(1, len(*(rh.cachedBackfillSpec.VBTasksMap[0])))

	endTime := time.Now()

	// With 2 cooldown periods of 500ms each, this should be > 1 second
	assert.True(endTime.Sub(startTime).Seconds() > 1)
	fmt.Println("============== Test case stop: TestBackfillReqHandlerCreateReqThenMarkDone =================")
}
