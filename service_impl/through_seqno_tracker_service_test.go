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
	"math"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	commonMock "github.com/couchbase/goxdcr/v8/common/mocks"
	"github.com/couchbase/goxdcr/v8/conflictlog"
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

func setupBoilerPlate() (*commonMock.Pipeline, *commonMock.SourceNozzle, *service_def.ReplicationSpecSvc, *commonMock.PipelineRuntimeContext, *commonMock.PipelineSupervisorSvc, *commonMock.OutNozzle) {

	pipelineMock := &commonMock.Pipeline{}
	replSpecSvcMock := &service_def.ReplicationSpecSvc{}
	runtimeCtxMock := &commonMock.PipelineRuntimeContext{}
	pipelineSupervisorSvc := &commonMock.PipelineSupervisorSvc{}

	nozzleMock := &commonMock.SourceNozzle{}
	targetNozzles := &commonMock.OutNozzle{}

	return pipelineMock, nozzleMock, replSpecSvcMock, runtimeCtxMock, pipelineSupervisorSvc, targetNozzles
}

func setupMocks(pipeline *commonMock.Pipeline, sourceNozzle *commonMock.SourceNozzle, replSpecSvc *service_def.ReplicationSpecSvc, runtimeCtxMock *commonMock.PipelineRuntimeContext, pipelineSupervisorSvc *commonMock.PipelineSupervisorSvc, targetNozzles *commonMock.OutNozzle, targetVBListToReplace []uint16) *ThroughSeqnoTrackerSvc {

	var vbList []uint16
	vbList = append(vbList, 1)
	sourceNozzle.On("ResponsibleVBs").Return(vbList)
	sourceNozzle.On("Id").Return("TestNozzleId")
	sourceNozzle.On("AsyncComponentEventListeners").Return(nil)
	sourceNozzle.On("Connector").Return(nil)
	sourceNozzle.On("Connectors").Return(nil)

	sourceMap := make(map[string]common.SourceNozzle)
	sourceMap["dummy"] = sourceNozzle

	targetMap := make(map[string]common.Nozzle)
	targetMap["dummy"] = targetNozzles
	if len(targetVBListToReplace) > 0 {
		targetNozzles.On("ResponsibleVBs").Return(targetVBListToReplace)
	} else {
		targetNozzles.On("ResponsibleVBs").Return(vbList)
	}
	targetNozzles.On("Id").Return("TestOutNozzleId")
	targetNozzles.On("GetConflictLogger").Return(nil)

	runtimeCtxMock.On("Service", base.PIPELINE_SUPERVISOR_SVC).Return(pipelineSupervisorSvc)
	runtimeCtxMock.On("Service", "ConflictManager").Return(nil)

	pipeline.On("Topic").Return("UnitTest")
	pipeline.On("FullTopic").Return("UnitTest")
	pipeline.On("Sources").Return(sourceMap)
	pipeline.On("Targets").Return(targetMap)
	pipeline.On("InstanceId").Return("UnitTestInst")
	pipeline.On("GetAsyncListenerMap").Return(nil)
	pipeline.On("SetAsyncListenerMap", mock.Anything).Return(nil)
	pipeline.On("RuntimeContext").Return(runtimeCtxMock)
	pipeline.On("Type").Return(common.MainPipeline)
	pipeline.On("Targets").Return(targetMap)

	replSpecSvc.On("GetDerivedObj", mock.Anything).Return(nil, nil)

	svc := NewThroughSeqnoTrackerSvc(log.DefaultLoggerContext, nil, false)
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
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles, nil)

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

	var manifestAdditional parts.ManifestAdditional
	manifestAdditional.ManifestId = 1
	manifestAdditional.RecycleFunc = func(obj interface{}) {}

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
	ignoreEvent := common.NewEvent(common.DataNotReplicated, wrappedMCR, nil, nil, manifestAdditional)
	oldIgnoreLen := svc.vbIgnoredSeqnoListMap[1].GetLengthOfSeqnoList()
	assert.Nil(svc.ProcessEvent(ignoreEvent))
	newIgnoreLen := svc.vbIgnoredSeqnoListMap[1].GetLengthOfSeqnoList()
	assert.NotEqual(oldIgnoreLen, newIgnoreLen)

	wrappedMCR.Seqno = 6
	ignoreEvent = common.NewEvent(common.DataNotReplicated, wrappedMCR, nil, nil, manifestAdditional)
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
	commonEvent = common.NewEvent(common.DataNotReplicated, wrappedMCR, nil, nil, manifestAdditional)
	assert.Nil(svc.ProcessEvent(commonEvent))
	through_seqno = svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(8), through_seqno)

	wrappedMCR.Seqno = 10
	commonEvent = common.NewEvent(common.DataNotReplicated, wrappedMCR, nil, nil, manifestAdditional)
	assert.Nil(svc.ProcessEvent(commonEvent))
	through_seqno = svc.GetThroughSeqno(1)
	//	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(8), through_seqno)

	wrappedMCR.Seqno = 9
	commonEvent = common.NewEvent(common.DataNotReplicated, wrappedMCR, nil, nil, manifestAdditional)
	assert.Nil(svc.ProcessEvent(commonEvent))
	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(10), through_seqno)

	fmt.Println("============== Test case end: TestIgnoredEventThroughSeqno =================")
}

func TestOutofOrderSent(t *testing.T) {
	fmt.Println("============== Test case start: TestOutofOrderSent =================")
	assert := assert.New(t)
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles, nil)

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
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles, nil)

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
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles, nil)

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
	var helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh := make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(math.MaxUint64)
	helperItems[3] = uint64(0)
	helperItems[4] = uint64(0)
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
	helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(2)
	helperItems[3] = uint64(2)
	helperItems[4] = uint64(1)
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
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles, nil)

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
	var helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh := make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(math.MaxUint64)
	helperItems[3] = uint64(0)
	helperItems[4] = uint64(0)
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
	helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(3)
	helperItems[3] = uint64(7)
	helperItems[4] = uint64(3)
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
	helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(math.MaxUint64)
	helperItems[3] = uint64(0)
	helperItems[4] = uint64(0)
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
	helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(11)
	helperItems[3] = uint64(11)
	helperItems[4] = uint64(1)
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
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles, nil)

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
	var helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh := make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(math.MaxUint64)
	helperItems[3] = uint64(0)
	helperItems[4] = uint64(0)
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
	helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(3)
	helperItems[3] = uint64(3)
	helperItems[4] = uint64(1)
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
	helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(math.MaxUint64)
	helperItems[3] = uint64(0)
	helperItems[4] = uint64(0)
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
	helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(8)
	helperItems[3] = uint64(10)
	helperItems[4] = uint64(3)
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
	helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(math.MaxUint64)
	helperItems[3] = uint64(0)
	helperItems[4] = uint64(0)

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
	helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(13)
	helperItems[3] = uint64(15)
	helperItems[4] = uint64(3)
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
	helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(math.MaxUint64)
	helperItems[3] = uint64(0)
	helperItems[4] = uint64(0)
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
	helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(17)
	helperItems[3] = uint64(17)
	helperItems[4] = uint64(1)
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
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles, nil)

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
	var helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh := make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(math.MaxUint64)
	helperItems[3] = uint64(0)
	helperItems[4] = uint64(0)
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
	helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(1)
	helperItems[3] = uint64(4)
	helperItems[4] = uint64(4)
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
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles, nil)

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
	var helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh := make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(math.MaxUint64)
	helperItems[3] = uint64(0)
	helperItems[4] = uint64(0)
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
	helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(1)
	helperItems[3] = uint64(4)
	helperItems[4] = uint64(4)
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

// This test simulates a 128 VB to 1024 VB test
func TestVariableVBSent(t *testing.T) {
	fmt.Println("============== Test case start: TestVariableVBSent =================")
	defer fmt.Println("============== Test case end: TestVariableVBSent =================")
	assert := assert.New(t)
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles, nil)

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
	var manifestAdditional parts.ManifestAdditional
	manifestAdditional.ManifestId = 1
	manifestAdditional.RecycleFunc = func(obj interface{}) {}

	// First let VB 1 replicate to VB 1
	mutationEvent.Seqno = 1
	commonEvent := common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.NotNil(commonEvent)
	assert.Nil(svc.ProcessEvent(commonEvent))
	dataSentAdditional.Seqno = mutationEvent.Seqno
	sentEvent := common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// Let VB 1 replicate to VB 2
	mutationEvent.Seqno = 2
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	dataSentAdditional.VbucketCommon.VBucket = 2
	dataSentAdditional.Seqno = mutationEvent.Seqno
	dataSentAdditional.VariableVBUsed = true
	dataSentAdditional.OrigSrcVB = 1
	sentEvent = common.NewEvent(common.DataSent, nil, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// Let VB 1 replicate to VB 3
	mutationEvent.Seqno = 3
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	dataSentAdditional.VbucketCommon.VBucket = 3
	dataSentAdditional.Seqno = mutationEvent.Seqno
	dataSentAdditional.VariableVBUsed = true
	dataSentAdditional.OrigSrcVB = 1
	sentEvent = common.NewEvent(common.DataSent, nil, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	through_seqno := svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(3), through_seqno)

	systemEvent.Seqno = 4
	commonEvent = common.NewEvent(common.SystemEventReceived, systemEvent, nil, nil, nil)
	oldFilteredLen := svc.vbSystemEventsSeqnoListMap[1].getLengthOfSeqnoLists()
	assert.Nil(svc.ProcessEvent(commonEvent))
	assert.Nil(svc.ProcessEvent(commonEvent))
	newFilteredLen := svc.vbSystemEventsSeqnoListMap[1].getLengthOfSeqnoLists()
	assert.NotEqual(oldFilteredLen, newFilteredLen)

	// Happy path - through seqno is at 4
	through_seqno = svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(4), through_seqno)

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

	ignoreEvent := common.NewEvent(common.DataNotReplicated, wrappedMCR, nil, nil, manifestAdditional)
	oldIgnoreLen := svc.vbIgnoredSeqnoListMap[1].GetLengthOfSeqnoList()
	assert.Nil(svc.ProcessEvent(ignoreEvent))
	newIgnoreLen := svc.vbIgnoredSeqnoListMap[1].GetLengthOfSeqnoList()
	assert.NotEqual(oldIgnoreLen, newIgnoreLen)

	wrappedMCR.Seqno = 6
	ignoreEvent = common.NewEvent(common.DataNotReplicated, wrappedMCR, nil, nil, manifestAdditional)
	oldIgnoreLen = svc.vbIgnoredSeqnoListMap[1].GetLengthOfSeqnoList()
	assert.Nil(svc.ProcessEvent(ignoreEvent))
	newIgnoreLen = svc.vbIgnoredSeqnoListMap[1].GetLengthOfSeqnoList()
	assert.NotEqual(oldIgnoreLen, newIgnoreLen)

	// Ignored ones should not be counted as through seqno
	through_seqno = svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	assert.Equal(uint64(6), through_seqno)

	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(6), through_seqno)
}

func TestOSOSentFirstOutOfOrderVariableVB(t *testing.T) {
	fmt.Println("============== Test case start: TestOSOSentFirstOutOfOrderVariableVB =================")
	defer fmt.Println("============== Test case end: TestOSOSentFirstOutOfOrderVariableVB =================")
	assert := assert.New(t)
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles := setupBoilerPlate()
	targetVBListToReplace := []uint16{0, 1, 2}

	targetVBRandomRoute := func() uint16 {
		idxRand := rand.Intn(len(targetVBListToReplace))
		return targetVBListToReplace[idxRand]
	}

	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles, targetVBListToReplace)

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
	var helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh := make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(math.MaxUint64)
	helperItems[3] = uint64(0)
	helperItems[4] = uint64(0)
	commonEvent := common.NewEvent(common.OsoSnapshotReceived, true /*turn on OSO*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	select {
	case <-syncCh:
		break
	}

	var dataSentAdditional parts.DataSentEventAdditional
	dataSentAdditional.VariableVBUsed = true
	dataSentAdditional.OrigSrcVB = mutationEvent.VBucket
	// VariableVB Random route
	dataSentAdditional.VBucket = targetVBRandomRoute()
	// Let's say manifest is by default 1 but one item got 5
	dataSentAdditional.ManifestId = 1

	var highestTgtManifestIdToUse = uint64(5)
	// 2. 5 sent
	// For this test, 5 is the "highest" seqno to be have sent - so we need to make sure that the manifestID is also the highest
	mutationEvent.Seqno = 5
	dataSentAdditional.Seqno = mutationEvent.Seqno
	dataSentAdditional.ManifestId = highestTgtManifestIdToUse
	sentEvent := common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))
	dataSentAdditional.ManifestId = 1 // restore to 1

	// 3. 3 received
	mutationEvent.Seqno = 3
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))

	// 4. 2 sent
	mutationEvent.Seqno = 2
	dataSentAdditional.Seqno = mutationEvent.Seqno
	dataSentAdditional.VBucket = targetVBRandomRoute()
	// Let's bump the manifest for this one to 5
	// dataSentAdditional.ManifestId = highestTgtManifestIdToUse
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))
	//dataSentAdditional.ManifestId = 1 // restore to 1

	// 5. 1 sent
	mutationEvent.Seqno = 1
	dataSentAdditional.Seqno = mutationEvent.Seqno
	dataSentAdditional.VBucket = targetVBRandomRoute()
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
	dataSentAdditional.VBucket = targetVBRandomRoute()
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// 11. 3 sent
	mutationEvent.Seqno = 3
	dataSentAdditional.Seqno = mutationEvent.Seqno
	dataSentAdditional.VBucket = targetVBRandomRoute()
	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(sentEvent))

	// Before OSO ends, throughSeqno should not move
	through_seqno = svc.GetThroughSeqno(1)
	assert.Equal(uint64(0), through_seqno)

	// OSO now ends -
	osoSnapshotSeqnoCheck = 4
	helperItems = make([]interface{}, 5)
	helperItems[0] = uint16(1)
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(1)
	helperItems[3] = uint64(4)
	helperItems[4] = uint64(4)
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

	throughSeqnos, _, tgtManifestIds := svc.GetThroughSeqnosAndManifestIds()
	assert.Equal(uint64(5), throughSeqnos[1])
	var highestTgtManifestId uint64
	for _, tgtManifestId := range tgtManifestIds {
		if tgtManifestId > highestTgtManifestId {
			highestTgtManifestId = tgtManifestId
		}
	}
	assert.Equal(highestTgtManifestId, highestTgtManifestIdToUse)
}

func shuffle(l []uint64) {
	rand.Shuffle(len(l), func(i, j int) {
		l[i], l[j] = l[j], l[i]
	})
}

func TestProcessCLoggerRequests(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestProcessCLoggerRequests =================")
	defer fmt.Println("============== Test case end: TestProcessCLoggerRequests =================")

	// Setup
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles, nil)
	svc.cLogExists = true

	// Test data
	vbno := uint16(1)
	cleanup := func(throughSeqno uint64) {
		// No-op for this test
	}
	allSeqnos := []uint64{6, 7, 8, 9, 10, 11, 12, 15, 16, 19, 20, 21, 22, 23, 24, 25}
	base.SortUint64List(allSeqnos)

	// randomly divide allSeqnos to 5 separate lists.
	var sentSeqnoList, failedCRSeqnoList, filteredSeqnoList, systemEventSeqnoList, ignoredSeqnoList []uint64
	systemEventSeqnoList = append(systemEventSeqnoList, 1) // always consists atleast one item
	for i := range len(allSeqnos) {
		listNum := rand.Intn(5)
		switch listNum {
		case 0:
			sentSeqnoList = append(sentSeqnoList, allSeqnos[i])
		case 1:
			failedCRSeqnoList = append(failedCRSeqnoList, allSeqnos[i])
		case 2:
			filteredSeqnoList = append(filteredSeqnoList, allSeqnos[i])
		case 3:
			systemEventSeqnoList = append(systemEventSeqnoList, allSeqnos[i])
		case 4:
			ignoredSeqnoList = append(ignoredSeqnoList, allSeqnos[i])
		}
	}

	setupClogLists := func(cLogReq, cLogRes []uint64) {
		shuffle(cLogReq)
		shuffle(cLogRes)
		svc.cLogTrackerVbMap[vbno] = newDualSortedSeqnoListWithLock()
		svc.cLogTrackerVbMap[vbno].seqno_list_1 = cLogReq
		svc.cLogTrackerVbMap[vbno].seqno_list_2 = cLogRes
	}

	// Test case 1: All conflict logging requests are done
	newThroughSeqno := uint64(22)
	lastThroughSeqno := uint64(5)

	// 1a. no conflicts at all
	cLogReq := []uint64{}
	cLogRes := []uint64{}
	setupClogLists(cLogReq, cLogRes)
	result := svc.processCLoggerRequests(vbno, newThroughSeqno, lastThroughSeqno, sentSeqnoList, failedCRSeqnoList, filteredSeqnoList, systemEventSeqnoList, ignoredSeqnoList, cleanup)
	assert.Equal(newThroughSeqno, result)

	// 1b. conflicts exists, but all done
	cLogReq = []uint64{6, 11, 12, 15, 16}
	cLogRes = []uint64{6, 11, 12, 15, 16}
	setupClogLists(cLogReq, cLogRes)
	result = svc.processCLoggerRequests(vbno, newThroughSeqno, lastThroughSeqno, sentSeqnoList, failedCRSeqnoList, filteredSeqnoList, systemEventSeqnoList, ignoredSeqnoList, cleanup)
	assert.Equal(newThroughSeqno, result)

	// Test case 2: Conflict logging requests are not done
	// 2a. none are done
	cLogReq = []uint64{11, 12, 15, 16}
	cLogRes = []uint64{}
	setupClogLists(cLogReq, cLogRes)
	result = svc.processCLoggerRequests(vbno, newThroughSeqno, lastThroughSeqno, sentSeqnoList, failedCRSeqnoList, filteredSeqnoList, systemEventSeqnoList, ignoredSeqnoList, cleanup)
	assert.Equal(uint64(10), result)

	// 2b. some are not done, in order reqs, result is the previous integer.
	cLogReq = []uint64{6, 11, 12, 15, 16}
	cLogRes = []uint64{6, 11, 12}
	setupClogLists(cLogReq, cLogRes)
	result = svc.processCLoggerRequests(vbno, newThroughSeqno, lastThroughSeqno, sentSeqnoList, failedCRSeqnoList, filteredSeqnoList, systemEventSeqnoList, ignoredSeqnoList, cleanup)
	assert.Equal(uint64(12), result)

	// 2c. some are not done, in order reqs, result is not previous integer.
	cLogReq = []uint64{6, 11, 12, 19, 20}
	cLogRes = []uint64{6, 11, 12}
	setupClogLists(cLogReq, cLogRes)
	result = svc.processCLoggerRequests(vbno, newThroughSeqno, lastThroughSeqno, sentSeqnoList, failedCRSeqnoList, filteredSeqnoList, systemEventSeqnoList, ignoredSeqnoList, cleanup)
	assert.Equal(uint64(16), result)

	// 2d. some are not done, unordered reqs, result is the previous integer.
	cLogReq = []uint64{6, 11, 12, 15, 16}
	cLogRes = []uint64{6, 11, 15}
	setupClogLists(cLogReq, cLogRes)
	result = svc.processCLoggerRequests(vbno, newThroughSeqno, lastThroughSeqno, sentSeqnoList, failedCRSeqnoList, filteredSeqnoList, systemEventSeqnoList, ignoredSeqnoList, cleanup)
	assert.Equal(uint64(11), result)

	// 2e. some are not done, unordered reqs, result is not previous integer.
	cLogReq = []uint64{6, 11, 15, 19, 20}
	cLogRes = []uint64{6, 11, 19}
	setupClogLists(cLogReq, cLogRes)
	result = svc.processCLoggerRequests(vbno, newThroughSeqno, lastThroughSeqno, sentSeqnoList, failedCRSeqnoList, filteredSeqnoList, systemEventSeqnoList, ignoredSeqnoList, cleanup)
	assert.Equal(uint64(12), result)

	// Test case 3: No new candidates found i.e. first cLogReq not done.
	cLogReq = []uint64{6, 11, 12, 15, 16}
	cLogRes = []uint64{12, 15, 16}
	setupClogLists(cLogReq, cLogRes)
	result = svc.processCLoggerRequests(vbno, newThroughSeqno, lastThroughSeqno, sentSeqnoList, failedCRSeqnoList, filteredSeqnoList, systemEventSeqnoList, ignoredSeqnoList, cleanup)
	assert.Equal(lastThroughSeqno, result)

	// Test case 4: all clog requests not done are beyond newThroughSeqno.
	cLogReq = []uint64{6, 11, 12, 15, 16, 24}
	cLogRes = []uint64{6, 11, 12, 15, 16}
	setupClogLists(cLogReq, cLogRes)
	result = svc.processCLoggerRequests(vbno, newThroughSeqno, lastThroughSeqno, sentSeqnoList, failedCRSeqnoList, filteredSeqnoList, systemEventSeqnoList, ignoredSeqnoList, cleanup)
	assert.Equal(newThroughSeqno, result)

	// 4a. same as test 4, but clog requests are only beyond newThroughSeqno.
	cLogReq = []uint64{23, 24, 25}
	cLogRes = []uint64{}
	setupClogLists(cLogReq, cLogRes)
	result = svc.processCLoggerRequests(vbno, newThroughSeqno, lastThroughSeqno, sentSeqnoList, failedCRSeqnoList, filteredSeqnoList, systemEventSeqnoList, ignoredSeqnoList, cleanup)
	assert.Equal(newThroughSeqno, result)

	// Test case 5: test faultyCLogTrackerCnt
	// This input is unrealistic and should not happen.
	cLogReq = []uint64{6, 11, 12, 15, 16, 24}
	cLogRes = []uint64{6, 11, 12, 15, 16, 24, 25}
	setupClogLists(cLogReq, cLogRes)
	assert.Equal(svc.faultyCLogTrackerCnt.Load(), uint64(0))
	result = svc.processCLoggerRequests(vbno, newThroughSeqno, lastThroughSeqno, sentSeqnoList, failedCRSeqnoList, filteredSeqnoList, systemEventSeqnoList, ignoredSeqnoList, cleanup)
	assert.Equal(newThroughSeqno, result)
	assert.Equal(svc.faultyCLogTrackerCnt.Load(), uint64(1))

	// Test case 6: test nonMonotonicThroughSeqnoCnt
	// This input is unrealistic and should not happen.
	cLogReq = []uint64{7, 8, 9}
	cLogRes = []uint64{8}
	setupClogLists(cLogReq, cLogRes)
	assert.Equal(svc.nonMonotonicThroughSeqnoCnt.Load(), uint64(0))
	result = svc.processCLoggerRequests(vbno, newThroughSeqno, lastThroughSeqno, []uint64{1, 2, 3}, []uint64{}, []uint64{}, []uint64{8}, []uint64{}, cleanup)
	assert.Equal(newThroughSeqno, result)
	assert.Equal(svc.nonMonotonicThroughSeqnoCnt.Load(), uint64(1))
}

func TestOSOModeWithCLogger(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestOSOModeWithCLogger =================")
	defer fmt.Println("============== Test case end: TestOSOModeWithCLogger =================")

	// Setup
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles, nil)
	svc.cLogExists = true
	vbno := uint16(1)
	mutationEvent := &mcc.UprEvent{
		VBucket: vbno,
		Opcode:  gomemcached.UPR_MUTATION,
	}
	var dataSentAdditional parts.DataSentEventAdditional
	var cLogReqAdditional conflictlog.CLogReqT
	var cLogRespAdditional conflictlog.CLogRespT
	dataSentAdditional.VBucket = vbno
	cLogReqAdditional.Vbno = vbno
	cLogRespAdditional.Vbno = vbno
	cLogRespAdditional.ThroughSeqnoRelated = true
	raiseEventAndTest := func(event *common.Event, syncCh chan bool, osoMode bool, dcpSentOsoEnd bool, minDCPSeqno, maxDCPSeqno,
		seqnoFroDcp, seqnoHandled, seqnoBeforeSessionStart, cLogReqCnt, cLogResCnt, ts int) {

		assert.NotNil(event)
		if syncCh != nil {
			go func() {
				assert.Nil(svc.ProcessEvent(event))
			}()
			<-syncCh
		} else {
			assert.Nil(svc.ProcessEvent(event))
		}
		var seqno uint64
		if upr, ok := event.Data.(*mcc.UprEvent); ok {
			seqno = upr.Seqno
		} else if dataSent, ok := event.Data.(parts.DataSentEventAdditional); ok {
			seqno = dataSent.Seqno
		} else if cLogReq, ok := event.Data.(conflictlog.CLogReqT); ok {
			seqno = cLogReq.Seqno
		} else if cLogRes, ok := event.Data.(conflictlog.CLogRespT); ok {
			seqno = cLogRes.Seqno
		} else if event.EventType == common.OsoSnapshotReceived {
			assert.Equal(svc.GetThroughSeqno(vbno), uint64(ts))
			// we don't know the seqno to fetch the appropriate session
			return
		} else {
			panic(fmt.Sprintf("implement %T", event.Data))
		}

		oso, session := svc.shouldProcessAsOso(vbno, seqno)
		assert.Equal(svc.osoModeActive, uint32(1))
		assert.Equal(oso, osoMode)
		assert.Equal(session.dcpSentOsoEnd, dcpSentOsoEnd)
		assert.Equal(session.minSeqnoFromDCP, uint64(minDCPSeqno))
		assert.Equal(session.maxSeqnoFromDCP, uint64(maxDCPSeqno))
		assert.Equal(session.seqnosFromDCPCnt, uint64(seqnoFroDcp))
		assert.Equal(session.seqnosHandledCnt, uint64(seqnoHandled))
		assert.Equal(session.dcpSeqnoWhenSessionStarts, uint64(seqnoBeforeSessionStart))
		assert.Equal(session.cLogRequestedCnt, uint64(cLogReqCnt))
		assert.Equal(session.cLogRespondedCnt, uint64(cLogResCnt))
		assert.Equal(svc.GetThroughSeqno(vbno), uint64(ts))
	}

	// let's say we receive a OSO snapshots - OSOStart 2 5 3 4 1 OSOEnd, OSOStart 8 7 6 9 10 OSOEnd, OSOStart 11 OSOEnd
	// and CLogReqs were generated for 3 4 8 and 10.

	// OSO snapshot start
	var helperItems = make([]interface{}, 5)
	helperItems[0] = vbno
	syncCh := make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(math.MaxUint64)
	helperItems[3] = uint64(0)
	helperItems[4] = uint64(0)
	commonEvent := common.NewEvent(common.OsoSnapshotReceived, true /*OSO start*/, nil, helperItems, nil)
	raiseEventAndTest(commonEvent, syncCh, true, false, ^0, 0, 0, 0, 0, 0, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// seqno 2 received
	mutationEvent.Seqno = 2
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	raiseEventAndTest(commonEvent, nil, true, false, 2, 2, 1, 0, 0, 0, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// seqno 5 received
	mutationEvent.Seqno = 5
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	raiseEventAndTest(commonEvent, nil, true, false, 2, 5, 2, 0, 0, 0, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// seqno 5 sent
	mutationEvent.Seqno = 5
	dataSentAdditional.Seqno = 5
	commonEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	raiseEventAndTest(commonEvent, nil, true, false, 2, 5, 2, 1, 0, 0, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// seqno 3 received
	mutationEvent.Seqno = 3
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	raiseEventAndTest(commonEvent, nil, true, false, 2, 5, 3, 1, 0, 0, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// seqno 2 sent
	mutationEvent.Seqno = 2
	dataSentAdditional.Seqno = 2
	commonEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	raiseEventAndTest(commonEvent, nil, true, false, 2, 5, 3, 2, 0, 0, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// cLogReq for seqno 3
	mutationEvent.Seqno = 3
	cLogReqAdditional.Seqno = 3
	syncCh = make(chan bool)
	commonEvent = common.NewEvent(common.ConflictsDetected, cLogReqAdditional, nil, nil, syncCh)
	raiseEventAndTest(commonEvent, syncCh, true, false, 2, 5, 3, 2, 0, 1, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// seqno 3 sent
	mutationEvent.Seqno = 3
	dataSentAdditional.Seqno = 3
	commonEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	raiseEventAndTest(commonEvent, nil, true, false, 2, 5, 3, 3, 0, 1, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// seqno 4 received
	mutationEvent.Seqno = 4
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	raiseEventAndTest(commonEvent, nil, true, false, 2, 5, 4, 3, 0, 1, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// cLogReq for seqno 4
	mutationEvent.Seqno = 4
	cLogReqAdditional.Seqno = 4
	syncCh = make(chan bool)
	commonEvent = common.NewEvent(common.ConflictsDetected, cLogReqAdditional, nil, nil, syncCh)
	raiseEventAndTest(commonEvent, syncCh, true, false, 2, 5, 4, 3, 0, 2, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// seqno 4 sent
	mutationEvent.Seqno = 4
	dataSentAdditional.Seqno = 4
	commonEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	raiseEventAndTest(commonEvent, nil, true, false, 2, 5, 4, 4, 0, 2, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// seqno 1 received
	mutationEvent.Seqno = 1
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	raiseEventAndTest(commonEvent, nil, true, false, 1, 5, 5, 4, 0, 2, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// OSO End received for current snapshot
	// ts is still 0, since some mutations are not sent + conflict logged.
	helperItems = make([]interface{}, 5)
	helperItems[0] = vbno
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(1)
	helperItems[3] = uint64(5)
	helperItems[4] = uint64(5)
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, false /* OSO snapshot end */, nil, helperItems, nil)
	raiseEventAndTest(commonEvent, nil, false, true, 1, 5, 5, 4, 0, 2, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// New OSO snapshot start. However the previous snapshot shouldn't close.
	helperItems = make([]interface{}, 5)
	helperItems[0] = vbno
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(math.MaxUint64)
	helperItems[3] = uint64(0)
	helperItems[4] = uint64(0)
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, true /*OSO start*/, nil, helperItems, nil)
	raiseEventAndTest(commonEvent, syncCh, true, false, ^0, 0, 0, 0, 5, 0, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 2)

	// seqno 8 received
	mutationEvent.Seqno = 8
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	raiseEventAndTest(commonEvent, nil, true, false, 8, 8, 1, 0, 5, 0, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 2)

	// seqno 8 cLog request
	mutationEvent.Seqno = 8
	cLogReqAdditional.Seqno = 8
	syncCh = make(chan bool)
	commonEvent = common.NewEvent(common.ConflictsDetected, cLogReqAdditional, nil, nil, syncCh)
	raiseEventAndTest(commonEvent, syncCh, true, false, 8, 8, 1, 0, 5, 1, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 2)

	// seqno 8 sent
	mutationEvent.Seqno = 8
	dataSentAdditional.Seqno = 8
	commonEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	raiseEventAndTest(commonEvent, nil, true, false, 8, 8, 1, 1, 5, 1, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 2)

	// seqno 1 sent - but cLog not yet done, so throughSeqno still 0.
	mutationEvent.Seqno = 1
	dataSentAdditional.Seqno = 1
	commonEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	raiseEventAndTest(commonEvent, nil, true, true, 1, 5, 5, 5, 0, 2, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 2)

	// seqno 4 cLog response. However the session should not be closed still.
	mutationEvent.Seqno = 4
	cLogRespAdditional.Seqno = 4
	commonEvent = common.NewEvent(common.CLogWriteStatus, cLogRespAdditional, nil, nil, nil)
	raiseEventAndTest(commonEvent, nil, true, true, 1, 5, 5, 5, 0, 2, 1, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 2)

	// seqno 7 received
	mutationEvent.Seqno = 7
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	raiseEventAndTest(commonEvent, nil, true, false, 7, 8, 2, 1, 5, 1, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 2)

	// seqno 6 received
	mutationEvent.Seqno = 6
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	raiseEventAndTest(commonEvent, nil, true, false, 6, 8, 3, 1, 5, 1, 0, 0)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 2)

	// seqno 3 cLog response. With that first session should close.
	mutationEvent.Seqno = 3
	cLogRespAdditional.Seqno = 3
	commonEvent = common.NewEvent(common.CLogWriteStatus, cLogRespAdditional, nil, nil, nil)
	// now the session to check is session 2, because session 1 is done. But throughSeqno is 5.
	raiseEventAndTest(commonEvent, nil, true, false, 6, 8, 3, 1, 5, 1, 0, 5)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// seqno 6 sent
	mutationEvent.Seqno = 6
	dataSentAdditional.Seqno = 6
	commonEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	raiseEventAndTest(commonEvent, nil, true, false, 6, 8, 3, 2, 5, 1, 0, 5)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// seqno 8 cLog response.
	mutationEvent.Seqno = 8
	cLogRespAdditional.Seqno = 8
	commonEvent = common.NewEvent(common.CLogWriteStatus, cLogRespAdditional, nil, nil, nil)
	raiseEventAndTest(commonEvent, nil, true, false, 6, 8, 3, 2, 5, 1, 1, 5)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// seqno 9 received
	mutationEvent.Seqno = 9
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	raiseEventAndTest(commonEvent, nil, true, false, 6, 9, 4, 2, 5, 1, 1, 5)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// seqno 10 received
	mutationEvent.Seqno = 10
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	raiseEventAndTest(commonEvent, nil, true, false, 6, 10, 5, 2, 5, 1, 1, 5)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// OSO End received for current snapshot
	// ts is still 5, since some mutations are not sent + conflict logged.
	helperItems = make([]interface{}, 5)
	helperItems[0] = vbno
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(6)
	helperItems[3] = uint64(10)
	helperItems[4] = uint64(5)
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, false /* OSO snapshot end */, nil, helperItems, nil)
	raiseEventAndTest(commonEvent, nil, true, true, 6, 10, 5, 2, 5, 1, 1, 5)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// seqno 10 cLog request
	mutationEvent.Seqno = 10
	cLogReqAdditional.Seqno = 10
	syncCh = make(chan bool)
	commonEvent = common.NewEvent(common.ConflictsDetected, cLogReqAdditional, nil, nil, syncCh)
	raiseEventAndTest(commonEvent, syncCh, true, true, 6, 10, 5, 2, 5, 2, 1, 5)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// seqno 10 sent
	mutationEvent.Seqno = 10
	dataSentAdditional.Seqno = 10
	commonEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	raiseEventAndTest(commonEvent, nil, true, true, 6, 10, 5, 3, 5, 2, 1, 5)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// seqno 7 sent
	mutationEvent.Seqno = 7
	dataSentAdditional.Seqno = 7
	commonEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	raiseEventAndTest(commonEvent, nil, true, true, 6, 10, 5, 4, 5, 2, 1, 5)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// cLog 10 response. All cLog responses got, but 1 item not sent yet.
	// So throughSeqno is still 5.
	mutationEvent.Seqno = 10
	cLogRespAdditional.Seqno = 10
	commonEvent = common.NewEvent(common.CLogWriteStatus, cLogRespAdditional, nil, nil, nil)
	raiseEventAndTest(commonEvent, nil, true, true, 6, 10, 5, 4, 5, 2, 2, 5)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)

	// New OSO snapshot start. However the previous snapshot shouldn't close.
	helperItems = make([]interface{}, 5)
	helperItems[0] = vbno
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(math.MaxUint64)
	helperItems[3] = uint64(0)
	helperItems[4] = uint64(0)
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, true /*OSO start*/, nil, helperItems, nil)
	raiseEventAndTest(commonEvent, syncCh, true, false, ^0, 0, 0, 0, 10, 0, 0, 5)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 2)

	// seqno 11 cLog request asynchronously before data received.
	// cLogReqCnt will still be 0 because it will go to the unsure buffer of the last session.
	mutationEvent.Seqno = 11
	cLogReqAdditional.Seqno = 11
	syncCh = make(chan bool)
	commonEvent = common.NewEvent(common.ConflictsDetected, cLogReqAdditional, nil, nil, syncCh)
	raiseEventAndTest(commonEvent, syncCh, true, false, ^0, 0, 0, 0, 10, 0, 0, 5)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 2)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions[1].unsureBufferedCLogSeqnos.seqno_list_1), 1)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions[1].unsureBufferedCLogSeqnos.seqno_list_2), 0)

	// seqno 11 cLog response asynchronously before data received.
	// cLogResCnt will still be 0 because it will go to the unsure buffer of the last session.
	mutationEvent.Seqno = 11
	cLogRespAdditional.Seqno = 11
	commonEvent = common.NewEvent(common.CLogWriteStatus, cLogRespAdditional, nil, nil, nil)
	raiseEventAndTest(commonEvent, nil, true, false, ^0, 0, 0, 0, 10, 0, 0, 5)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 2)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions[1].unsureBufferedCLogSeqnos.seqno_list_1), 1)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions[1].unsureBufferedCLogSeqnos.seqno_list_2), 1)

	// seqno 11 received now.
	mutationEvent.Seqno = 11
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	raiseEventAndTest(commonEvent, nil, true, false, 11, 11, 1, 0, 10, 0, 0, 5)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 2)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions[1].unsureBufferedCLogSeqnos.seqno_list_1), 1)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions[1].unsureBufferedCLogSeqnos.seqno_list_2), 1)

	// seqno 9 sent. With this throughSeqno will move to 10.
	// previous session should close.
	mutationEvent.Seqno = 9
	dataSentAdditional.Seqno = 9
	commonEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	// check session stat of remaining session
	raiseEventAndTest(commonEvent, nil, true, false, 11, 11, 1, 0, 10, 0, 0, 10)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions[0].unsureBufferedCLogSeqnos.seqno_list_1), 1)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions[0].unsureBufferedCLogSeqnos.seqno_list_2), 1)

	// OSO End received for recent snapshot
	// ts is still 10, since some mutations are not sent.
	// unsure buffer needs to be processed fully.
	session := svc.vbOsoModeSessionDCPTracker[vbno].sessions[0]
	oldCLogReqCnt := int(session.cLogRequestedCnt)
	oldCLogResCnt := int(session.cLogRespondedCnt)
	helperItems = make([]interface{}, 5)
	helperItems[0] = vbno
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(11)
	helperItems[3] = uint64(11)
	helperItems[4] = uint64(1)
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, false /* OSO snapshot end */, nil, helperItems, nil)
	raiseEventAndTest(commonEvent, syncCh, true, true, 11, 11, 1, 0, 10, 0, 0, 10)
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 1)
	// verify that whole of unsure buffer was processed
	assert.Equal(int(session.cLogRequestedCnt), oldCLogReqCnt+len(session.unsureBufferedCLogSeqnos.seqno_list_1))
	assert.Equal(int(session.cLogRespondedCnt), oldCLogResCnt+len(session.unsureBufferedCLogSeqnos.seqno_list_2))
	assert.Equal(len(svc.cLogTrackerVbMap[vbno].seqno_list_1), 0)
	assert.Equal(len(svc.cLogTrackerVbMap[vbno].seqno_list_2), 0)

	// seqno 11 sent. With this throughSeqno will move to 11.
	// this session should close.
	mutationEvent.Seqno = 11
	dataSentAdditional.Seqno = 11
	commonEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	// there will be no more sessions, therefore just check for throughSeqno
	assert.Nil(svc.ProcessEvent(commonEvent))
	assert.Equal(svc.GetThroughSeqno(vbno), uint64(11))
	assert.Equal(len(svc.vbOsoModeSessionDCPTracker[vbno].sessions), 0)
}

func TestOSOModeWithAsyncDataReceived(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestOSOModeWithAsyncDataReceived =================")
	defer fmt.Println("============== Test case end: TestOSOModeWithAsyncDataReceived =================")

	// Setup
	pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles := setupBoilerPlate()
	svc := setupMocks(pipeline, nozzle, replSpecSvc, runtimeCtx, pipelineSvc, targetNozzles, nil)
	vbno := uint16(1)
	mutationEvent := &mcc.UprEvent{
		VBucket: vbno,
		Opcode:  gomemcached.UPR_MUTATION,
	}
	var dataSentAdditional parts.DataSentEventAdditional
	dataSentAdditional.VBucket = vbno

	// OSO snapshot start
	var helperItems = make([]interface{}, 5)
	helperItems[0] = vbno
	syncCh := make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(math.MaxUint64)
	helperItems[3] = uint64(0)
	helperItems[4] = uint64(0)
	commonEvent := common.NewEvent(common.OsoSnapshotReceived, true /*OSO start*/, nil, helperItems, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	assert.Equal(svc.GetThroughSeqno(vbno), uint64(0))

	// seqno 5 received
	mutationEvent.Seqno = 5
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	assert.Equal(svc.GetThroughSeqno(vbno), uint64(0))

	// seqno 5 sent.
	mutationEvent.Seqno = 5
	dataSentAdditional.Seqno = 5
	commonEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(commonEvent))
	assert.Equal(svc.GetThroughSeqno(vbno), uint64(0))

	// OSO End received for recent snapshot
	helperItems[0] = vbno
	syncCh = make(chan bool)
	helperItems[1] = syncCh
	helperItems[2] = uint64(4)
	helperItems[3] = uint64(5)
	helperItems[4] = uint64(2)
	commonEvent = common.NewEvent(common.OsoSnapshotReceived, false /* OSO snapshot end */, nil, helperItems, nil)
	go svc.ProcessEvent(commonEvent)
	<-syncCh
	assert.Equal(svc.GetThroughSeqno(vbno), uint64(0))

	// seqno 4 received
	mutationEvent.Seqno = 4
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	assert.Equal(svc.GetThroughSeqno(vbno), uint64(0))

	// seqno 4 sent.
	mutationEvent.Seqno = 4
	dataSentAdditional.Seqno = 4
	commonEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	assert.Nil(svc.ProcessEvent(commonEvent))

	assert.Equal(svc.GetThroughSeqno(vbno), uint64(5))
}
