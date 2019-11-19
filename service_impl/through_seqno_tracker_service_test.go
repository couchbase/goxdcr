package service_impl

import (
	"fmt"
	"github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/common"
	commonMock "github.com/couchbase/goxdcr/common/mocks"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/parts"
	"github.com/stretchr/testify/assert"
	"testing"
)

func makeCommonEvent(eventType common.ComponentEventType, uprEvent *mcc.UprEvent) *common.Event {
	//	switch eventType {
	//	case common.DataReceived:
	retEvent := common.NewEvent(eventType, uprEvent, nil, nil, nil)
	return retEvent
	//	default:
	//		return nil
	//	}
}

func setupBoilerPlate() (*commonMock.Pipeline,
	*ThroughSeqnoTrackerSvc,
	*commonMock.Nozzle) {

	pipelineMock := &commonMock.Pipeline{}
	svc := NewThroughSeqnoTrackerSvc(log.DefaultLoggerContext)
	svc.unitTesting = true

	nozzleMock := &commonMock.Nozzle{}

	return pipelineMock, svc, nozzleMock
}

func setupMocks(pipeline *commonMock.Pipeline,
	sourceNozzle *commonMock.Nozzle) {

	var vbList []uint16
	//	var i uint16
	//	for i = 0; i < 1024; i++ {
	//		vbList = append(vbList, i)
	//	}
	vbList = append(vbList, 1)
	sourceNozzle.On("ResponsibleVBs").Return(vbList)

	sourceMap := make(map[string]common.Nozzle)
	sourceMap["dummy"] = sourceNozzle

	pipeline.On("Topic").Return("UnitTest")
	pipeline.On("Sources").Return(sourceMap)
}

// Collections may send out system events AFTER
func TestSeqnoTrackerOutOfOrder(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestSeqnoTrackerOutOfOrder =================")
	pipelineMock, svc, sourceNozzle := setupBoilerPlate()
	setupMocks(pipelineMock, sourceNozzle)

	svc.initialize(pipelineMock)

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

	// First send mutations 1, 2, 5, 6
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
	commonEvent = common.NewEvent(common.DataFiltered, systemEvent, nil, nil, nil)
	oldFilteredLen := svc.vb_filtered_seqno_list_map[1].GetLengthOfSeqnoList()
	assert.Nil(svc.ProcessEvent(commonEvent))
	newFilteredLen := svc.vb_filtered_seqno_list_map[1].GetLengthOfSeqnoList()
	assert.NotEqual(oldFilteredLen, newFilteredLen)

	systemEvent.Seqno = 4
	commonEvent = common.NewEvent(common.DataFiltered, systemEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	assert.Nil(svc.ProcessEvent(commonEvent))
	newFilteredLen = svc.vb_filtered_seqno_list_map[1].GetLengthOfSeqnoList()
	assert.NotEqual(oldFilteredLen, newFilteredLen)

	// Somehow these are not sent out in time and this causes panic
	mutationEvent.Seqno = 5
	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	assert.Nil(svc.ProcessEvent(commonEvent))
	//	dataSentAdditional.Seqno = mutationEvent.Seqno
	//	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	//	assert.Nil(svc.ProcessEvent(sentEvent))

	//	mutationEvent.Seqno = 6
	//	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	//	assert.Nil(svc.ProcessEvent(commonEvent))
	//	dataSentAdditional.Seqno = mutationEvent.Seqno
	//	sentEvent = common.NewEvent(common.DataSent, mutationEvent, nil, nil, dataSentAdditional)
	//	assert.Nil(svc.ProcessEvent(sentEvent))
	//
	//	mutationEvent.Seqno = 7
	//	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	//	assert.Nil(svc.ProcessEvent(commonEvent))
	//
	//	mutationEvent.Seqno = 8
	//	commonEvent = common.NewEvent(common.DataReceived, mutationEvent, nil, nil, nil)
	//	assert.Nil(svc.ProcessEvent(commonEvent))

	through_seqno := svc.GetThroughSeqno(1)
	svc.truncateSeqnoLists(1, through_seqno)
	fmt.Println("============== Test case end: TestSeqnoTrackerOutOfOrder =================")
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
