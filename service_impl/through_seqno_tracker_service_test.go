package service_impl

import (
	"fmt"
	"github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	commonMock "github.com/couchbase/goxdcr/common/mocks"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/parts"
	pipelineSvc "github.com/couchbase/goxdcr/pipeline_svc/mocks"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"testing"
)

func makeCommonEvent(eventType common.ComponentEventType, uprEvent *mcc.UprEvent) *common.Event {
	retEvent := common.NewEvent(eventType, uprEvent, nil, nil, nil)
	return retEvent
}

func setupBoilerPlate() (*commonMock.Pipeline,
	*commonMock.Nozzle,
	*service_def.ReplicationSpecSvc,
	*commonMock.PipelineRuntimeContext,
	*pipelineSvc.PipelineSupervisorSvc) {

	pipelineMock := &commonMock.Pipeline{}
	replSpecSvcMock := &service_def.ReplicationSpecSvc{}
	runtimeCtxMock := &commonMock.PipelineRuntimeContext{}
	pipelineSupervisorSvc := &pipelineSvc.PipelineSupervisorSvc{}

	nozzleMock := &commonMock.Nozzle{}

	return pipelineMock, nozzleMock, replSpecSvcMock, runtimeCtxMock, pipelineSupervisorSvc
}

func setupMocks(pipeline *commonMock.Pipeline,
	sourceNozzle *commonMock.Nozzle,
	replSpecSvc *service_def.ReplicationSpecSvc,
	runtimeCtxMock *commonMock.PipelineRuntimeContext,
	pipelineSupervisorSvc *pipelineSvc.PipelineSupervisorSvc) *ThroughSeqnoTrackerSvc {

	var vbList []uint16
	vbList = append(vbList, 1)
	sourceNozzle.On("ResponsibleVBs").Return(vbList)
	sourceNozzle.On("Id").Return("TestNozzleId")
	sourceNozzle.On("AsyncComponentEventListeners").Return(nil)
	sourceNozzle.On("Connector").Return(nil)

	sourceMap := make(map[string]common.Nozzle)
	sourceMap["dummy"] = sourceNozzle

	runtimeCtxMock.On("Service", base.PIPELINE_SUPERVISOR_SVC).Return(pipelineSupervisorSvc)

	pipeline.On("Topic").Return("UnitTest")
	pipeline.On("Sources").Return(sourceMap)
	pipeline.On("InstanceId").Return("UnitTestInst")
	pipeline.On("GetAsyncListenerMap").Return(nil)
	pipeline.On("SetAsyncListenerMap", mock.Anything).Return(nil)
	pipeline.On("RuntimeContext").Return(runtimeCtxMock)

	replSpecSvc.On("GetDerivedObj", mock.Anything).Return(nil, nil)

	svc := NewThroughSeqnoTrackerSvc(log.DefaultLoggerContext)
	svc.unitTesting = true
	svc.Attach(pipeline)
	return svc
}

func TestTargetSeqnoManifestMap(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestTargetSeqnoManifestMap =================")
	vbListMap := make(SeqnoManifestsMapType)
	vbListMap[0] = newDualSortedSeqnoListWithLock()

	assert.Nil(vbListMap.UpdateOrAppendSeqnoManifest(0 /*vbno*/, 1 /*seqno*/, 0 /*manifestId*/))
	assert.Nil(vbListMap.UpdateOrAppendSeqnoManifest(0 /*vbno*/, 2 /*seqno*/, 0 /*manifestId*/))

	vbListMap[0].lock.RLock()
	assert.Equal(1, len(vbListMap[0].seqno_list_1))
	assert.Equal(1, len(vbListMap[0].seqno_list_2))
	assert.Equal(uint64(2), vbListMap[0].seqno_list_1[0])
	assert.Equal(uint64(0), vbListMap[0].seqno_list_2[0])
	vbListMap[0].lock.RUnlock()

	assert.Nil(vbListMap.UpdateOrAppendSeqnoManifest(0 /*vbno*/, 5 /*seqno*/, 1 /*manifestId*/))

	vbListMap[0].lock.RLock()
	assert.Equal(2, len(vbListMap[0].seqno_list_1))
	assert.Equal(2, len(vbListMap[0].seqno_list_2))
	assert.Equal(uint64(5), vbListMap[0].seqno_list_1[1])
	assert.Equal(uint64(1), vbListMap[0].seqno_list_2[1])
	vbListMap[0].lock.RUnlock()

	assert.Nil(vbListMap.UpdateOrAppendSeqnoManifest(0 /*vbno*/, 7 /*seqno*/, 2 /*manifestId*/))

	// Now truncate say throughseqno is 6...
	// Because 7 is too high, we need to keep 5
	(*DualSortedSeqnoListWithLock)(vbListMap[0]).truncateSeqno1Floor(6)
	vbListMap[0].lock.RLock()
	assert.Equal(2, len(vbListMap[0].seqno_list_1))
	assert.Equal(2, len(vbListMap[0].seqno_list_2))
	assert.Equal(uint64(5), vbListMap[0].seqno_list_1[0])
	assert.Equal(uint64(1), vbListMap[0].seqno_list_2[0])
	vbListMap[0].lock.RUnlock()
	fmt.Println("============== Test case end: TargetSeqnoManifestMap =================")
}

func TestTruncateFloor(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestTruncateFloor =================")

	testList := newDualSortedSeqnoListWithLock()
	testList.appendSeqnos(0, 1, nil)
	testList.appendSeqnos(2, 3, nil)
	testList.appendSeqnos(4, 6, nil)
	testList.appendSeqnos(7, 9, nil)

	list2No, err := testList.getList2BasedonList1Floor(2)
	assert.Nil(err)
	assert.Equal(uint64(3), list2No)
	list2No, err = testList.getList2BasedonList1Floor(3)
	assert.Nil(err)
	assert.Equal(uint64(3), list2No)
	list2No, err = testList.getList2BasedonList1Floor(1)
	assert.Nil(err)
	assert.Equal(uint64(1), list2No)

	testList.truncateSeqno1Floor(6)
	assert.Equal(2, len(testList.seqno_list_1))
	assert.Equal(2, len(testList.seqno_list_2))

	testList = newDualSortedSeqnoListWithLock()
	testList.appendSeqnos(0, 1, nil)
	testList.appendSeqnos(2, 3, nil)
	testList.appendSeqnos(4, 6, nil)
	testList.appendSeqnos(7, 9, nil)

	testList.truncateSeqno1Floor(2)
	assert.Equal(3, len(testList.seqno_list_1))
	assert.Equal(3, len(testList.seqno_list_2))

	// Keep the last one
	testList.truncateSeqno1Floor(11)
	assert.Equal(1, len(testList.seqno_list_1))
	assert.Equal(1, len(testList.seqno_list_2))
	fmt.Println("============== Test case end: TestTruncateFloor =================")
}

// This tests ensures that ignored seqnos are counted as through sequence number
// It is therefore essential that checkpoint manager to document the needed backfills
// before counting the through sequence number
func TestIgnoredEventThroughSeqno(t *testing.T) {
	fmt.Println("============== Test case start: TestIgnoredEventThroughSeqno =================")
	assert := assert.New(t)
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc)

	mutationEvent := &mcc.UprEvent{
		VBucket: 1,
		Opcode:  gomemcached.UPR_MUTATION,
	}

	systemEvent := &mcc.UprEvent{
		VBucket: 1,
		Opcode:  gomemcached.DCP_SYSTEM_EVENT,
	}

	var dataSentAdditional parts.DataSentEventAdditional
	dataSentAdditional.VBucket = 1

	mutationEvent.Seqno = 1
	commonEvent := common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.NotNil(commonEvent)
	assert.Nil(svc.ProcessEvent(commonEvent))
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent := common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	mutationEvent.Seqno = 2
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	systemEvent.Seqno = 3
	commonEvent = common.NewEvent(common.SystemEventReceived, systemEvent, nil, nil, nil)
	oldFilteredLen := svc.vbSystemEventsSeqnoListMap[1].getLengthOfSeqnoLists()
	assert.Nil(svc.ProcessEvent(commonEvent))
	newFilteredLen := svc.vbSystemEventsSeqnoListMap[1].getLengthOfSeqnoLists()
	assert.NotEqual(oldFilteredLen, newFilteredLen)

	systemEvent.Seqno = 4
	commonEvent = common.NewEvent(common.SystemEventReceived, systemEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	assert.Nil(svc.ProcessEvent(commonEvent))
	newFilteredLen = svc.vbSystemEventsSeqnoListMap[1].getLengthOfSeqnoLists()
	assert.NotEqual(oldFilteredLen, newFilteredLen)

	// Happy path - through seqno is at 4
	through_seqno := svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(4), through_seqno)

	// Say 5 and 6 are not replicated
	wrappedMCR := &base.WrappedMCRequest{
		Seqno: 5,
		Req:   &gomemcached.MCRequest{VBucket: 1},
	}
	ignoreEvent := common.NewEvent(common.DataNotReplicated, wrappedMCR, nil, nil, nil)
	oldIgnoreLen := svc.vbIgnoredSeqnoListMap[1].GetLengthOfSeqnoList()
	assert.Nil(svc.ProcessEvent(ignoreEvent))
	newIgnoreLen := svc.vbIgnoredSeqnoListMap[1].GetLengthOfSeqnoList()
	assert.NotEqual(oldIgnoreLen, newIgnoreLen)

	wrappedMCR.Seqno = 6
	ignoreEvent = common.NewEvent(common.DataNotReplicated, wrappedMCR, nil, nil, nil)
	oldIgnoreLen = svc.vbIgnoredSeqnoListMap[1].GetLengthOfSeqnoList()
	assert.Nil(svc.ProcessEvent(ignoreEvent))
	newIgnoreLen = svc.vbIgnoredSeqnoListMap[1].GetLengthOfSeqnoList()
	assert.NotEqual(oldIgnoreLen, newIgnoreLen)

	// Ignored ones should not be counted as through seqno
	through_seqno = svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(6), through_seqno)

	// Say 7 was sent
	mutationEvent.Seqno = 7
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	through_seqno = svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(7), through_seqno)
	fmt.Println("============== Test case end: TestIgnoredEventThroughSeqno =================")
}

func TestOutofOrderSent(t *testing.T) {
	fmt.Println("============== Test case start: TestOutofOrderSent =================")
	assert := assert.New(t)
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc)

	var earlySeqno uint64 = 1
	var laterSeqno uint64 = 2
	var earlyManifest uint64 = 3
	var laterManifest uint64 = 5
	var vbno uint16 = 1

	// A mutation that belongs later in the sequence with lower manifestID gets sent first
	var earlyDataSentAdditional parts.DataSentEventAdditional
	earlyDataSentAdditional.VBucket = vbno
	earlyDataSentAdditional.Seqno = laterSeqno
	earlyDataSentAdditional.ManifestId = earlyManifest
	commonEvent := common.NewEvent(common.DataSent, nil, nil, nil, earlyDataSentAdditional)
	assert.NotNil(commonEvent)
	assert.Nil(svc.ProcessEvent(commonEvent))

	// A mutation that belongs earlier in the sequence got sent later and got tagged with a later manifest
	var laterDataSentAdditional parts.DataSentEventAdditional
	laterDataSentAdditional.VBucket = vbno
	laterDataSentAdditional.Seqno = earlySeqno
	laterDataSentAdditional.ManifestId = laterManifest
	commonEvent = common.NewEvent(common.DataSent, nil, nil, nil, laterDataSentAdditional)
	assert.NotNil(commonEvent)
	assert.Nil(svc.ProcessEvent(commonEvent))

	list1Len := len(svc.vbTgtSeqnoManifestMap[vbno].seqno_list_1)
	list2Len := len(svc.vbTgtSeqnoManifestMap[vbno].seqno_list_2)

	assert.Equal(svc.vbTgtSeqnoManifestMap[vbno].seqno_list_1[list1Len-1], laterSeqno)
	assert.Equal(svc.vbTgtSeqnoManifestMap[vbno].seqno_list_2[list2Len-1], laterManifest)

	fmt.Println("============== Test case end: TestOutofOrderSent =================")
}
