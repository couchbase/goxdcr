/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package service_impl

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	commonMock "github.com/couchbase/goxdcr/v8/common/mocks"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/parts"
	service_def "github.com/couchbase/goxdcr/v8/service_def/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func makeCommonEvent(eventType common.ComponentEventType, uprEvent *mcc.UprEvent) *common.Event {
	retEvent := common.NewEvent(eventType, uprEvent, nil, nil, nil)
	return retEvent
}

func setupBoilerPlate() (*commonMock.Pipeline,
	*commonMock.SourceNozzle,
	*service_def.ReplicationSpecSvc,
	*commonMock.PipelineRuntimeContext,
	*commonMock.PipelineSupervisorSvc) {

	pipelineMock := &commonMock.Pipeline{}
	replSpecSvcMock := &service_def.ReplicationSpecSvc{}
	runtimeCtxMock := &commonMock.PipelineRuntimeContext{}
	pipelineSupervisorSvc := &commonMock.PipelineSupervisorSvc{}

	nozzleMock := &commonMock.SourceNozzle{}

	return pipelineMock, nozzleMock, replSpecSvcMock, runtimeCtxMock, pipelineSupervisorSvc
}

func setupMocks(pipeline *commonMock.Pipeline,
	sourceNozzle *commonMock.SourceNozzle,
	replSpecSvc *service_def.ReplicationSpecSvc,
	runtimeCtxMock *commonMock.PipelineRuntimeContext,
	pipelineSupervisorSvc *commonMock.PipelineSupervisorSvc) *ThroughSeqnoTrackerSvc {

	var vbList []uint16
	vbList = append(vbList, 1)
	sourceNozzle.On("ResponsibleVBs").Return(vbList)
	sourceNozzle.On("Id").Return("TestNozzleId")
	sourceNozzle.On("AsyncComponentEventListeners").Return(nil)
	sourceNozzle.On("Connector").Return(nil)
	sourceNozzle.On("Connectors").Return(nil)

	sourceMap := make(map[string]common.SourceNozzle)
	sourceMap["dummy"] = sourceNozzle

	runtimeCtxMock.On("Service", base.PIPELINE_SUPERVISOR_SVC).Return(pipelineSupervisorSvc)
	runtimeCtxMock.On("Service", "ConflictManager").Return(nil)

	pipeline.On("Topic").Return("UnitTest")
	pipeline.On("FullTopic").Return("UnitTest")
	pipeline.On("Sources").Return(sourceMap)
	pipeline.On("Targets").Return(map[string]common.Nozzle{})
	pipeline.On("InstanceId").Return("UnitTestInst")
	pipeline.On("GetAsyncListenerMap").Return(nil)
	pipeline.On("SetAsyncListenerMap", mock.Anything).Return(nil)
	pipeline.On("RuntimeContext").Return(runtimeCtxMock)
	pipeline.On("Type").Return(common.MainPipeline)

	replSpecSvc.On("GetDerivedObj", mock.Anything).Return(nil, nil)

	svc := NewThroughSeqnoTrackerSvc(log.DefaultLoggerContext, nil)
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
	testList.appendSeqnos(0, 1)
	testList.appendSeqnos(2, 3)
	testList.appendSeqnos(4, 6)
	testList.appendSeqnos(7, 9)

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
	testList.appendSeqnos(0, 1)
	testList.appendSeqnos(2, 3)
	testList.appendSeqnos(4, 6)
	testList.appendSeqnos(7, 9)

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
	mutationEvent.Seqno = 5
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	mutationEvent.Seqno = 6
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

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

	// Pretend everything else from now on is not sent but ignored
	mutationEvent.Seqno = 8
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	mutationEvent.Seqno = 9
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	mutationEvent.Seqno = 10
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	wrappedMCR.Seqno = 8
	wrappedMCR.Req = &gomemcached.MCRequest{VBucket: 1}
	commonEvent = common.NewEvent(common.DataNotReplicated, wrappedMCR, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	through_seqno = svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(8), through_seqno)

	wrappedMCR.Seqno = 10
	commonEvent = common.NewEvent(common.DataNotReplicated, wrappedMCR, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	through_seqno = svc.GetThroughSeqno(1)
	//	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(8), through_seqno)

	wrappedMCR.Seqno = 9
	commonEvent = common.NewEvent(common.DataNotReplicated, wrappedMCR, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(10), through_seqno)

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

func TestClonedData(t *testing.T) {
	fmt.Println("============== Test case start: TestClonedData =================")
	defer fmt.Println("============== Test case end: TestClonedData =================")
	assert := assert.New(t)
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc)

	mutationEvent := &mcc.UprEvent{
		VBucket: 1,
		Opcode:  gomemcached.UPR_MUTATION,
	}

	cloneSyncCh := make(chan bool)
	var dataSentAdditional parts.DataSentEventAdditional
	dataSentAdditional.VBucket = 1
	dataSentAdditional.Cloned = true
	dataSentAdditional.CloneSyncCh = cloneSyncCh

	mutationEvent.Seqno = 1
	commonEvent := common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.NotNil(commonEvent)
	assert.Nil(svc.ProcessEvent(commonEvent))

	// Pretend a event has 1 cloned sibling

	var data []interface{}
	data = append(data, uint16(1)) // vbno
	data = append(data, mutationEvent.Seqno)
	data = append(data, 2)     // total (1 main + 1 sibling)
	data = append(data, false) // not tombstone
	data = append(data, cloneSyncCh)
	clonedEvent := common.NewEvent(common.DataCloned, data, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(clonedEvent))

	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent := common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// The main req + one single sibling req means that seqno shouldn't move since until both events are heard back
	assert.Equal(uint64(0), svc.GetThroughSeqno(1))

	// Second sent event should cause the seqno to move
	assert.Nil(svc.ProcessEvent(sentEvent))
	assert.Equal(uint64(1), svc.GetThroughSeqno(1))
}

func TestOsoModeSimple(t *testing.T) {
	fmt.Println("============== Test case start: TestOsoModeSimple =================")
	defer fmt.Println("============== Test case end: TestOsoModeSimple =================")
	assert := assert.New(t)
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc)

	var osoSnapshotSeqnoCheck uint64
	svc.osoSnapshotRaiser = func(vbno uint16, snapshotSeqno uint64) {
		// When raising raising a snapshot, the seqno should equal the throughSeqno
		assert.Equal(osoSnapshotSeqnoCheck, snapshotSeqno)
	}

	mutationEvent := &mcc.UprEvent{
		VBucket: 1,
		Opcode:  gomemcached.UPR_MUTATION,
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

	// Happy path, seqno is at 1
	through_seqno := svc.GetThroughSeqno(1)
	assert.Equal(uint64(1), through_seqno)

	// Turn on OSO mode now, nothing is supposed to go past what was seen last, which was 1
	var helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh := make(chan bool)
	helperItems[1] = syncCh
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, true /*turn on OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	// seqno 2 received
	mutationEvent.Seqno = 2
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.NotNil(commonEvent)
	assert.Nil(svc.ProcessEvent(commonEvent))

	// Sanity check
	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(1), through_seqno)

	// Finish things up, say DCP turns off OSO
	osoSnapshotSeqnoCheck = 2
	helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, false /*turn off OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	// Sanity check
	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(1), through_seqno)

	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(2), through_seqno)
}

func TestOsoMode(t *testing.T) {
	fmt.Println("============== Test case start: TestOsoMode =================")
	assert := assert.New(t)
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc)

	var osoSnapshotSeqnoCheck uint64
	svc.osoSnapshotRaiser = func(vbno uint16, snapshotSeqno uint64) {
		// When raising raising a snapshot, the seqno should equal the throughSeqno
		assert.Equal(snapshotSeqno, osoSnapshotSeqnoCheck)
	}

	mutationEvent := &mcc.UprEvent{
		VBucket: 1,
		Opcode:  gomemcached.UPR_MUTATION,
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

	// Happy path, seqno is at 2
	through_seqno := svc.GetThroughSeqno(1)
	assert.Equal(uint64(2), through_seqno)

	// Turn on OSO mode now, nothing is supposed to go past what was seen last, which was 2
	var helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh := make(chan bool)
	helperItems[1] = syncCh
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, true /*turn on OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	// Now let's pretend DCP is sending things out of order, in the order of 4, 3, 7
	mutationEvent.Seqno = 4
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// oso mode is on - which means throughSeqno must not go past the last seen seqno when oso mode was on
	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(2), through_seqno)

	mutationEvent.Seqno = 3
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	mutationEvent.Seqno = 7
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	// Before 7 is has been sent acknowledged, osomode is turned off
	osoSnapshotSeqnoCheck = 7
	helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, false /*turn off OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	// Just for sanity check
	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(2), through_seqno)

	// At this point, we're waiting for seqno 7 to be handled. Once it's handled, throughSeqno should move to 7
	// Inject some future seqnos to throw it off
	mutationEvent.Seqno = 10
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// Just for sanity check
	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(2), through_seqno)

	// Lets start a new OSO session for fun, before the 7 has been handled
	helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, true /*turn on OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	// And say DCP sends down a seqno of 11
	mutationEvent.Seqno = 11
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	// Just for sanity check
	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(2), through_seqno)

	// Now 7 is handled, throughSeqno should move to 10, as it has been sent and also as 7 was the missing piece
	mutationEvent.Seqno = 7
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// After DataSent, the throughSeqno should move
	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(10), through_seqno)

	// Finish things up, say DCP turns off OSO
	osoSnapshotSeqnoCheck = 11
	helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, false /*turn off OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	// Sanity check
	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(10), through_seqno)

	// Then we finally sent the seqno of 10
	mutationEvent.Seqno = 11
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// After DataSent, the throughSeqno should move
	through_seqno = svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(11), through_seqno)

	fmt.Println("============== Test case end: TestOsoMode =================")
}

func TestOsoModeWaitingForSlowDataSent(t *testing.T) {
	fmt.Println("============== Test case start: TestOsoModeWaitingForSlowDataSent =================")
	defer fmt.Println("============== Test case end: TestOsoModeWaitingForSlowDataSent =================")
	assert := assert.New(t)
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc)

	var osoSnapshotSeqnoCheck uint64
	svc.osoSnapshotRaiser = func(vbno uint16, snapshotSeqno uint64) {
		// When raising raising a snapshot, the seqno should equal the throughSeqno
		assert.Equal(snapshotSeqno, osoSnapshotSeqnoCheck)
	}

	mutationEvent := &mcc.UprEvent{
		VBucket: 1,
		Opcode:  gomemcached.UPR_MUTATION,
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

	// Happy path, seqno is at 2
	through_seqno := svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(2), through_seqno)

	// Turn on OSO mode now, nothing is supposed to go past what was seen last, which was 2
	var helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh := make(chan bool)
	helperItems[1] = syncCh
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, true /*turn on OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	// DCP sends seqno 3
	mutationEvent.Seqno = 3
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	dataSentAdditional.Seqno = mutationEvent.Seqno

	osoSnapshotSeqnoCheck = 3
	// OSO mode is off - we're still waiting for seqno3 to hear back from Downstream part
	helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, false /*turn off OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	// sanity check
	through_seqno = svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(2), through_seqno)

	// Say seqno 5 is now heard from DCP but we're still waiting for seqno 3 to come back from downstream
	mutationEvent.Seqno = 5
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	dataSentAdditional.Seqno = mutationEvent.Seqno

	// seqno 3 is now finally heard back, and 5 too
	mutationEvent.Seqno = 3
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// When 5 is heard, the snapshot raised should be of seqno 5
	osoSnapshotSeqnoCheck = 5
	mutationEvent.Seqno = 5
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// Sanity check
	through_seqno = svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(5), through_seqno)
	// gap is truncated per truncate call
	//gap1, gap2 = svc.vb_gap_seqno_list_map[1].getSortedSeqnoLists(1, "")
	//assert.Equal(0, len(gap1))
	//assert.Equal(0, len(gap2))

	//==========================
	// Worst case scenario test - from this point on, multiple sessions, and data sent does not occur until the very end
	// DCP will first send seqno 6 (not OSO)
	// Part1: send session 1:  OSO_Start, 10, 8, 9, OSO_end (gap from 6 to 7, eventually)
	// Part2: send non-session: seqno 11
	// Part3: send session 2: OSO_Start, 13, 15, 14 OSO_end (gap from 12, 13, eventually)
	//
	// After DCP has finished everything, then slowly data sent go through the above in reverse order of whatever DCP sent
	// Check along the way to make sure

	mutationEvent.Seqno = 6
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	// sanity check - to make sure that it is not logged into a session since all session should be done
	lastSeenSeqno := svc.vb_last_seen_seqno_map[1].GetSeqno()
	assert.Equal(uint64(6), lastSeenSeqno)
	gap1, gap2 := svc.vb_gap_seqno_list_map[1].getSortedSeqnoLists(1, "")
	assert.Equal(0, len(gap1))
	assert.Equal(0, len(gap2))

	// Part1: Session 1 begins
	helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, true /*turn on OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	mutationEvent.Seqno = 10
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	mutationEvent.Seqno = 8
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	mutationEvent.Seqno = 9
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	// Session 1 ends
	osoSnapshotSeqnoCheck = 10
	helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, false /*turn off OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	// sanity check
	through_seqno = svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(5), through_seqno)

	// Part2: non-session seqno11
	mutationEvent.Seqno = 11
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	// sanity check
	through_seqno = svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(5), through_seqno)

	// Part3: Session 2 begins
	helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, true /*turn on OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	mutationEvent.Seqno = 13
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	mutationEvent.Seqno = 15
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	mutationEvent.Seqno = 14
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	// Session 2 ends
	osoSnapshotSeqnoCheck = 15
	helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, false /*turn off OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	// sanity check
	through_seqno = svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(5), through_seqno)

	// Now, XMEM sends everything out in reverse order
	mutationEvent.Seqno = 14
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	mutationEvent.Seqno = 15
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	mutationEvent.Seqno = 13
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// Sanity check
	through_seqno = svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(5), through_seqno)

	mutationEvent.Seqno = 11
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// Sanity check
	through_seqno = svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(5), through_seqno)

	mutationEvent.Seqno = 9
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	mutationEvent.Seqno = 8
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// Throwing in another OSO session here for good measure to try to see if the system should still behave correctly
	// This section tests to make sure that the new session's lastSeenDcpSeqno should reflect that of the previous session's
	// maxDCPSeen
	helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, true /*turn on OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	mutationEvent.Seqno = 17
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	// With 10 sent last, it'll raise a snapshot for 15
	// It's ok to raise the snapshot now even though throughSeq hasn't moved yet
	osoSnapshotSeqnoCheck = 15
	mutationEvent.Seqno = 10
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// The very first seqno that was received from DCP. ThroughSeqno should have moved after this
	mutationEvent.Seqno = 6
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// Seqno wouldn't have moved because an OSO session is currently open. The 6 should have been moved into the "unsure" list

	// simulate slow KV - where 17 is handled and then OSO end comes
	mutationEvent.Seqno = 17
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// OSO now ends -
	osoSnapshotSeqnoCheck = 17
	helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, false /*turn off OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	// and throughSeqno should have moved
	through_seqno = svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(17), through_seqno)
}

func TestOSOSentFirstOutOfOrder(t *testing.T) {
	fmt.Println("============== Test case start: TestOSOSentFirstOutOfOrder =================")
	defer fmt.Println("============== Test case end: TestOSOSentFirstOutOfOrder =================")
	assert := assert.New(t)
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc)

	var osoSnapshotSeqnoCheck uint64
	var osoSnapshotRaisedCnt uint32
	svc.osoSnapshotRaiser = func(vbno uint16, snapshotSeqno uint64) {
		// When raising raising a snapshot, the seqno should equal the throughSeqno
		assert.Equal(osoSnapshotSeqnoCheck, snapshotSeqno)
		atomic.AddUint32(&osoSnapshotRaisedCnt, 1)
	}

	mutationEvent := &mcc.UprEvent{
		VBucket: 1,
		Opcode:  gomemcached.UPR_MUTATION,
	}

	// This test case will simulate the following from DCP
	// OSO - 3, 2, 1, 4
	// Non-OSO - 5

	// The order of events raised will be out of order, but given that DCP has a sync mechanims for raising OSO, then:
	// 1. OSO Start
	// 2. 5 sent
	// 3. 3 received
	// 4. 2 sent
	// 5. 1 sent
	// 6. 2 received
	// 7. 1 received
	// 8. 4 received
	// 9. 4 sent
	// 10. 5 received
	// 11. 3 sent

	// 1. OSO start
	through_seqno := svc.GetThroughSeqno(1)
	assert.Equal(uint64(0), through_seqno)
	var helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh := make(chan bool)
	helperItems[1] = syncCh
	commonEvent := common.NewEvent(common.OsoSnapshotReceived, true /*turn on OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	var dataSentAdditional parts.DataSentEventAdditional
	dataSentAdditional.VBucket = 1

	// 2. 5 sent
	mutationEvent.Seqno = 5
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent := common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// 3. 3 received
	mutationEvent.Seqno = 3
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	// 4. 2 sent
	mutationEvent.Seqno = 2
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// 5. 1 sent
	mutationEvent.Seqno = 1
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// 2, 1, 4 received
	mutationEvent.Seqno = 2
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	mutationEvent.Seqno = 1
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	mutationEvent.Seqno = 4
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	// 9. 4 sent
	mutationEvent.Seqno = 4
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// 11. 3 sent
	mutationEvent.Seqno = 3
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// Before OSO ends, throughSeqno should not move
	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(0), through_seqno)

	// OSO now ends -
	osoSnapshotSeqnoCheck = 4
	helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, false /*turn off OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}
	assert.Equal(uint32(1), atomic.LoadUint32(&osoSnapshotRaisedCnt))

	// 10. 5 received
	mutationEvent.Seqno = 5
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(5), through_seqno)
}

func TestOSOLargeRangeOutOfOrder(t *testing.T) {
	fmt.Println("============== Test case start: TestOSOLargeRangeOutOfOrder =================")
	defer fmt.Println("============== Test case end: TestOSOLargeRangeOutOfOrder =================")
	assert := assert.New(t)
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc)

	var osoSnapshotSeqnoCheck uint64
	var osoSnapshotRaisedCnt uint32
	svc.osoSnapshotRaiser = func(vbno uint16, snapshotSeqno uint64) {
		// When raising raising a snapshot, the seqno should equal the throughSeqno
		assert.Equal(osoSnapshotSeqnoCheck, snapshotSeqno)
		atomic.AddUint32(&osoSnapshotRaisedCnt, 1)
	}

	mutationEvent := &mcc.UprEvent{
		VBucket: 1,
		Opcode:  gomemcached.UPR_MUTATION,
	}

	// This test case will simulate the following from DCP
	// OSO - 3, 2, 1, 4
	// Non-OSO - 5

	// Try to simulate a middle throughSeqno that does not match OSO snapshot
	// 1. OSO Start
	// 2. 3 received
	// 3. 2 received
	// 4. 1 received
	// 5. 1 sent
	// 6. 2 sent
	// 7. 3 sent
	// 8. 4 received
	// 9. OSO ends
	// 10.Check throughSeqno
	// 11. 4 sent

	// 1. OSO start
	through_seqno := svc.GetThroughSeqno(1)
	assert.Equal(uint64(0), through_seqno)
	var helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh := make(chan bool)
	helperItems[1] = syncCh
	commonEvent := common.NewEvent(common.OsoSnapshotReceived, true /*turn on OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	// 2. 3 received
	mutationEvent.Seqno = 3
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	// 3. 2 received
	mutationEvent.Seqno = 2
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	// 4. 1 received
	mutationEvent.Seqno = 1
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	var dataSentAdditional parts.DataSentEventAdditional
	dataSentAdditional.VBucket = 1

	// 5. 1 sent
	mutationEvent.Seqno = 1
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent := common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// 6. 2 sent
	mutationEvent.Seqno = 2
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// 7. 3 sent
	mutationEvent.Seqno = 3
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// 8. 4 received
	mutationEvent.Seqno = 4
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	// Before OSO end, throughSeqno does not move
	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(0), through_seqno)

	// 9. OSO now ends - should raise snapshot of {4,4}
	osoSnapshotSeqnoCheck = 4
	helperItems = make([]interface{}, 2)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, false /*turn off OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	// After OSO end, throughSeqno does not move
	assert.Equal(uint32(1), atomic.LoadUint32(&osoSnapshotRaisedCnt))
	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(0), through_seqno)

	// 10. 4 sent
	mutationEvent.Seqno = 4
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	assert.Equal(uint32(1), atomic.LoadUint32(&osoSnapshotRaisedCnt))
	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(4), through_seqno)
}
