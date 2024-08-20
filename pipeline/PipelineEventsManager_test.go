/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package pipeline

import (
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	utilsMock "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func setupPemBoilerPlate() (*int64, string, func(string) (*metadata.ReplicationSpecification, error), *log.CommonLogger, *utilsMock.UtilsIface) {
	var idWell int64 = -1
	specName := "testSpec"
	testSpec, _ := metadata.NewReplicationSpecification("srcBucket", "srcUUID", "tgtCluster", "tgtBucket", "tgtBucketUUID")
	specGetter := func(string) (*metadata.ReplicationSpecification, error) {
		return testSpec, nil
	}
	logger := log.NewLogger("testPipelineEventsMgr", log.DefaultLoggerContext)
	utils := &utilsMock.UtilsIface{}
	utils.On("StartDiagStopwatch", mock.Anything, mock.Anything).Return(func() time.Duration { return 0 })
	return &idWell, specName, specGetter, logger, utils
}

func setupBrokenMap() metadata.CollectionNamespaceMapping {
	brokenMap := make(metadata.CollectionNamespaceMapping)
	ns1, _ := base.NewCollectionNamespaceFromString("s1.c1")
	ns1t, _ := base.NewCollectionNamespaceFromString("s1t.c1t")
	ns2, _ := base.NewCollectionNamespaceFromString("s1.c2")
	ns2t, _ := base.NewCollectionNamespaceFromString("s1t.c2t")
	brokenMap.AddSingleMapping(&ns1, &ns1t)
	brokenMap.AddSingleMapping(&ns2, &ns2t)
	return brokenMap
}

func setupSuperLargeBrokenMap() metadata.CollectionNamespaceMapping {
	largeBM := make(metadata.CollectionNamespaceMapping)
	var namespaces []*base.CollectionNamespace
	var maxCnt = 10000
	for i := 0; i < maxCnt; i++ {
		ns, _ := base.NewCollectionNamespaceFromString(fmt.Sprintf("s1.c%v", i))
		namespaces = append(namespaces, &ns)
	}
	for i := 0; i < maxCnt; i++ {
		largeBM.AddSingleMapping(namespaces[i], namespaces[i])
	}
	return largeBM
}

func TestPipelineEventsMgr_AddEvent(t *testing.T) {
	fmt.Println("============== Test case start: TestPipelineEventsMgr_AddEvent =================")
	defer fmt.Println("============== Test case end: TestPipelineEventsMgr_AddEvent =================")
	assert := assert.New(t)

	idWell, specName, specGetter, logger, utils := setupPemBoilerPlate()
	eventsMgr := NewPipelineEventsMgr(idWell, specName, specGetter, logger, utils)

	assert.NotNil(eventsMgr)

	eventsMgr.AddEvent(base.HighPriorityMsg, "dummyHigh", base.NewEventsMap(), nil)
	assert.NotEqual(0, len(eventsMgr.events.EventInfos))
}

func TestPipelineEventsMgr_ContainsEvent(t *testing.T) {
	fmt.Println("============== Test case start: TestPipelineEventsMgr_ContainsEvent =================")
	defer fmt.Println("============== Test case end: TestPipelineEventsMgr_ContainsEvent =================")
	assert := assert.New(t)

	idWell, specName, specGetter, logger, utils := setupPemBoilerPlate()
	eventsMgr := NewPipelineEventsMgr(idWell, specName, specGetter, logger, utils)

	assert.NotNil(eventsMgr)

	brokenMap := setupBrokenMap()
	eventsMgr.LoadLatestBrokenMap(brokenMap)
	eventsList := eventsMgr.GetCurrentEvents()
	assert.NotEqual(0, len(eventsList.EventInfos))

	// Each source namespace is 1 ID
	// Each target namespace is also 1 ID
	// if brokenMapNumEvents is 2, then it means there are 5 total IDs in use:
	// ID 0 - the whole brokenMap event
	// ID 1 - source s1 event
	// ID 2 - source s1.c1
	// ID 3 - target s1t.c1t
	// ID 4 - source s1.c2
	// ID 5 - target s1t.c2t
	var i int
	assert.True(eventsMgr.ContainsEvent(i))
	for i = 1; i < 6; i++ {
		assert.True(eventsMgr.ContainsEvent(i))
	}

	// Validate level and namespace
	getterEvent := base.NewEventInfo()
	getterEvent.EventId = 0
	level, srcDesc, tgtDesc, err := eventsMgr.getIncomingEventBrokenmapLevelAndDesc(getterEvent)
	assert.Nil(err)
	assert.Equal(0, level)
	assert.Equal("", srcDesc)
	getterEvent.EventId++
	level, srcDesc, tgtDesc, err = eventsMgr.getIncomingEventBrokenmapLevelAndDesc(getterEvent)
	assert.Nil(err)
	assert.Equal(1, level)
	assert.Equal("s1", srcDesc)
	getterEvent.EventId++
	level, srcDesc, tgtDesc, err = eventsMgr.getIncomingEventBrokenmapLevelAndDesc(getterEvent)
	assert.Nil(err)
	assert.Equal(2, level)
	assert.True(srcDesc == "s1.c1" || srcDesc == "s1.c2")
	getterEvent.EventId++
	level, srcDesc, tgtDesc, err = eventsMgr.getIncomingEventBrokenmapLevelAndDesc(getterEvent)
	assert.Nil(err)
	assert.Equal(3, level)
	assert.True(srcDesc == "s1.c1" && tgtDesc == "s1t.c1t" || srcDesc == "s1.c2" && tgtDesc == "s1t.c2t")
	getterEvent.EventId++
	level, srcDesc, tgtDesc, err = eventsMgr.getIncomingEventBrokenmapLevelAndDesc(getterEvent)
	assert.Nil(err)
	assert.Equal(2, level)
	assert.True(srcDesc == "s1.c1" || srcDesc == "s1.c2")
	getterEvent.EventId++
	level, srcDesc, tgtDesc, err = eventsMgr.getIncomingEventBrokenmapLevelAndDesc(getterEvent)
	assert.Nil(err)
	assert.Equal(3, level)
	assert.True(srcDesc == "s1.c1" && tgtDesc == "s1t.c1t" || srcDesc == "s1.c2" && tgtDesc == "s1t.c2t")
	getterEvent.EventId++
	level, srcDesc, tgtDesc, err = eventsMgr.getIncomingEventBrokenmapLevelAndDesc(getterEvent)
	assert.NotNil(err)
	// ID 6 should not be in use
	assert.False(eventsMgr.ContainsEvent(i))

	for j := 0; j < i; j++ {
		gotEvent, gotEventErr := eventsMgr.getEvent(j)
		assert.Nil(gotEventErr)
		assert.NotNil(gotEvent)
		assert.Equal(int(gotEvent.EventId), j)
	}
}

func TestPipelineEventsMgr_DismissEvent_LowPriority(t *testing.T) {
	fmt.Println("============== Test case start: TestPipelineEventsMgr_DismissEvent_LowPriority =================")
	defer fmt.Println("============== Test case end: TestPipelineEventsMgr_DismissEvent_LowPriority =================")
	assert := assert.New(t)

	idWell, specName, specGetter, logger, utils := setupPemBoilerPlate()
	eventsMgr := NewPipelineEventsMgr(idWell, specName, specGetter, logger, utils)

	assert.NotNil(eventsMgr)

	eventsMgr.AddEvent(base.LowPriorityMsg, "testLowPriority", base.NewEventsMap(), nil)
	assert.Len(eventsMgr.events.EventInfos, 1)

	events := eventsMgr.GetCurrentEvents()
	events.Mutex.RLock()
	dismissID := events.EventInfos[0].EventId
	assert.Equal(base.LowPriorityMsg, events.EventInfos[0].EventType)
	events.Mutex.RUnlock()

	eventsMgr.DismissEvent(int(dismissID))
	assert.Len(eventsMgr.events.EventInfos, 0)
}

func TestPipelineEventsMgr_DismissEvent_WholeBrokenMap(t *testing.T) {
	fmt.Println("============== Test case start: TestPipelineEventsMgr_DismissEvent_WholeBrokenMap =================")
	defer fmt.Println("============== Test case end: TestPipelineEventsMgr_DismissEvent_WholeBrokenMap =================")
	assert := assert.New(t)

	idWell, specName, specGetter, logger, utils := setupPemBoilerPlate()
	eventsMgr := NewPipelineEventsMgr(idWell, specName, specGetter, logger, utils)

	assert.NotNil(eventsMgr)

	brokenMap := setupBrokenMap()
	eventsMgr.LoadLatestBrokenMap(brokenMap)
	eventsList := eventsMgr.GetCurrentEvents()
	assert.NotEqual(0, len(eventsList.EventInfos))

	eventsMgr.DismissEvent(0)

	eventsList = eventsMgr.GetCurrentEvents()
	assert.Len(eventsList.EventInfos, 0)
}

func TestPipelineEventsMgr_DismissEvent_WholeScope(t *testing.T) {
	fmt.Println("============== Test case start: TestPipelineEventsMgr_DismissEvent_WholeScope =================")
	defer fmt.Println("============== Test case end: TestPipelineEventsMgr_DismissEvent_WholeScope =================")
	assert := assert.New(t)

	idWell, specName, specGetter, logger, utils := setupPemBoilerPlate()
	eventsMgr := NewPipelineEventsMgr(idWell, specName, specGetter, logger, utils)

	assert.NotNil(eventsMgr)

	brokenMap := setupBrokenMap()
	eventsMgr.LoadLatestBrokenMap(brokenMap)
	eventsList := eventsMgr.GetCurrentEvents()
	assert.NotEqual(0, len(eventsList.EventInfos))

	eventsMgr.DismissEvent(1)

	eventsList = eventsMgr.GetCurrentEvents()
	assert.Len(eventsList.EventInfos, 0)
}

func TestPipelineEventsMgr_DismissEvent_SingleScope(t *testing.T) {
	fmt.Println("============== Test case start: TestPipelineEventsMgr_DismissEvent_SingleScope =================")
	defer fmt.Println("============== Test case end: TestPipelineEventsMgr_DismissEvent_SingleScope =================")
	assert := assert.New(t)

	idWell, specName, specGetter, logger, utils := setupPemBoilerPlate()
	eventsMgr := NewPipelineEventsMgr(idWell, specName, specGetter, logger, utils)

	assert.NotNil(eventsMgr)

	brokenMap := setupBrokenMap()
	eventsMgr.LoadLatestBrokenMap(brokenMap)
	eventsList := eventsMgr.GetCurrentEvents()
	assert.NotEqual(0, len(eventsList.EventInfos))

	eventsMgr.DismissEvent(2)

	eventsList = eventsMgr.GetCurrentEvents()
	assert.Len(eventsList.EventInfos, 1)

	// map[1:{1 BrokenMappingInfo s1 {map[4:{4 BrokenMappingInfo s1.c2 {map[5:{5 BrokenMappingInfo s1t.c2t {map[] 0xc000292f80} 0xc000292fa0 0xc000289380}] 0xc000292f40} 0xc000292f60 0xc0003025d0}] 0xc000292e80} 0xc000292ea0 <nil>}]
	s1Map := eventsList.EventInfos[0].EventExtras.EventsMap[1].(*base.EventInfo)
	assert.Len(s1Map.EventExtras.EventsMap, 1)

	// Now dismissing the last complete src->target event should render the whole broken mapping event gone
	eventsMgr.DismissEvent(5)
	eventsList = eventsMgr.GetCurrentEvents()
	assert.Len(eventsList.EventInfos, 0)
}

func TestPipelineEventsMgr_ResetDismissedHistory(t *testing.T) {
	fmt.Println("============== Test case start: TestPipelineEventsMgr_ResetDismissedHistory =================")
	defer fmt.Println("============== Test case end: TestPipelineEventsMgr_ResetDismissedHistory =================")
	assert := assert.New(t)

	idWell, specName, specGetter, logger, utils := setupPemBoilerPlate()
	eventsMgr := NewPipelineEventsMgr(idWell, specName, specGetter, logger, utils)

	assert.NotNil(eventsMgr)

	brokenMap := setupBrokenMap()
	eventsMgr.LoadLatestBrokenMap(brokenMap)
	eventsList := eventsMgr.GetCurrentEvents()
	assert.Len(eventsList.EventInfos, 1)

	// First dismiss the whole brokenMap
	eventsMgr.DismissEvent(0)
	eventsList = eventsMgr.GetCurrentEvents()
	assert.Len(eventsList.EventInfos, 0)

	// Reset dismissal should bring it back
	eventsMgr.ResetDismissedHistory()
	eventsList = eventsMgr.GetCurrentEvents()
	assert.Len(eventsList.EventInfos, 1)
}

func TestPipelineEventsMgr_DismissEvent_SingleScope_ThenRestore(t *testing.T) {
	fmt.Println("============== Test case start: TestPipelineEventsMgr_DismissEvent_SingleScope_ThenRestore =================")
	defer fmt.Println("============== Test case end: TestPipelineEventsMgr_DismissEvent_SingleScope_ThenRestore =================")
	assert := assert.New(t)

	idWell, specName, specGetter, logger, utils := setupPemBoilerPlate()
	eventsMgr := NewPipelineEventsMgr(idWell, specName, specGetter, logger, utils)

	assert.NotNil(eventsMgr)

	brokenMap := setupBrokenMap()
	eventsMgr.LoadLatestBrokenMap(brokenMap)
	eventsList := eventsMgr.GetCurrentEvents()
	assert.NotEqual(0, len(eventsList.EventInfos))

	eventsMgr.DismissEvent(2)

	eventsList = eventsMgr.GetCurrentEvents()
	assert.Len(eventsList.EventInfos, 1)

	var repairedPair metadata.CollectionNamespaceMappingsDiffPair
	repairedPair.Added = make(metadata.CollectionNamespaceMapping)
	source, _ := base.NewCollectionNamespaceFromString("s1.c1")
	target, _ := base.NewCollectionNamespaceFromString("s1t.c1t")
	repairedPair.Added.AddSingleMapping(&source, &target)
	eventsMgr.BackfillUpdateCb(&repairedPair, nil)

	eventsList = eventsMgr.GetCurrentEvents()
}

func TestPipelineEventsMgr_LargeBrokenMap(t *testing.T) {
	fmt.Println("============== Test case start: TestPipelineEventsMgr_DismissEvent_SingleScope_ThenRestore =================")
	defer fmt.Println("============== Test case end: TestPipelineEventsMgr_DismissEvent_SingleScope_ThenRestore =================")
	assert := assert.New(t)

	idWell, specName, specGetter, logger, utils := setupPemBoilerPlate()
	eventsMgr := NewPipelineEventsMgr(idWell, specName, specGetter, logger, utils)

	assert.NotNil(eventsMgr)

	smallBM := setupBrokenMap()
	eventsMgr.LoadLatestBrokenMap(smallBM)
	eventsList := eventsMgr.GetCurrentEvents()
	assert.NotEqual(0, len(eventsList.EventInfos))

	largeBM := setupSuperLargeBrokenMap()
	assert.NotEqual(0, len(largeBM))

	// This test will launch a separate go-routine to keep hammering and getting data while a single setter takes place
	var collectedTimes []time.Duration
	finCh := make(chan bool)
	go func() {
		for {
			select {
			case <-finCh:
				return
			default:
				startTime := time.Now()
				eventsMgr.GetCurrentEvents()
				timeTaken := time.Since(startTime)
				collectedTimes = append(collectedTimes, timeTaken)
			}
		}
	}()

	// This is the single update operation
	// Takes about 26 milli-seconds on 2019 MBP
	startTime := time.Now()
	eventsMgr.LoadLatestBrokenMap(largeBM)
	eventsList = eventsMgr.GetCurrentEvents()
	settingTimeTaken := time.Since(startTime)
	assert.NotEqual(0, len(eventsList.EventInfos))

	// stop collecting data
	close(finCh)

	var maxCheckTime time.Duration
	for _, checkTime := range collectedTimes {
		if checkTime > maxCheckTime {
			maxCheckTime = checkTime
		}
	}

	comparison := settingTimeTaken.Nanoseconds() / maxCheckTime.Nanoseconds()

	// Fail the test if querying is getting close to half as long as setting a time
	// Then this means lock contention is an issue
	assert.True(comparison >= 3)
}

func TestPipelineEventsMgr_UpdateEvent(t *testing.T) {
	fmt.Println("============== Test case start: TestPipelineEventsMgr_UpdateEvent =================")
	defer fmt.Println("============== Test case end: TestPipelineEventsMgr_UpdateEvent =================")
	assert := assert.New(t)

	idWell, specName, specGetter, logger, utils := setupPemBoilerPlate()
	eventsMgr := NewPipelineEventsMgr(idWell, specName, specGetter, logger, utils)

	assert.NotNil(eventsMgr)

	oldMsg := "old test priority event"
	newMsg := "new test priority event"

	eventsMgr.AddEvent(base.LowPriorityMsg, oldMsg, base.NewEventsMap(), nil)
	assert.Len(eventsMgr.events.EventInfos, 1)

	events := eventsMgr.GetCurrentEvents()
	events.Mutex.RLock()
	eventId := events.EventInfos[0].EventId
	assert.Equal(base.LowPriorityMsg, events.EventInfos[0].EventType)
	events.Mutex.RUnlock()

	assert.Nil(eventsMgr.UpdateEvent(eventId, newMsg, nil))
	checkEvent, err := eventsMgr.getEvent(int(eventId))
	assert.Nil(err)
	assert.Equal(newMsg, checkEvent.EventDesc)

	invalidEventId := eventId + 1

	err = eventsMgr.UpdateEvent(invalidEventId, "", nil)
	assert.Equal(base.ErrorNotFound, err)
}
