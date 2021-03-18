package pipeline

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/stretchr/testify/assert"
	"testing"
)

func setupPemBoilerPlate() (*int64, string, func(string) (*metadata.ReplicationSpecification, error), *log.CommonLogger) {
	var idWell int64 = -1
	specName := "testSpec"
	testSpec, _ := metadata.NewReplicationSpecification("srcBucket", "srcUUID", "tgtCluster", "tgtBucket", "tgtBucketUUID")
	specGetter := func(string) (*metadata.ReplicationSpecification, error) {
		return testSpec, nil
	}
	logger := &log.CommonLogger{}
	return &idWell, specName, specGetter, logger
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

func TestPipelineEventsMgr_AddEvent(t *testing.T) {
	fmt.Println("============== Test case start: TestPipelineEventsMgr_AddEvent =================")
	defer fmt.Println("============== Test case end: TestPipelineEventsMgr_AddEvent =================")
	assert := assert.New(t)

	idWell, specName, specGetter, logger := setupPemBoilerPlate()
	eventsMgr := NewPipelineEventsMgr(idWell, specName, specGetter, logger)

	assert.NotNil(eventsMgr)

	eventsMgr.AddEvent(base.HighPriorityMsg, "dummyHigh", base.NewEventsMap())
	assert.NotEqual(0, len(eventsMgr.events.EventInfos))
}

func TestPipelineEventsMgr_ContainsEvent(t *testing.T) {
	fmt.Println("============== Test case start: TestPipelineEventsMgr_ContainsEvent =================")
	defer fmt.Println("============== Test case end: TestPipelineEventsMgr_ContainsEvent =================")
	assert := assert.New(t)

	idWell, specName, specGetter, logger := setupPemBoilerPlate()
	eventsMgr := NewPipelineEventsMgr(idWell, specName, specGetter, logger)

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
	getterEventObj := base.NewEventInfo()
	getterEvent := &getterEventObj
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

	idWell, specName, specGetter, logger := setupPemBoilerPlate()
	eventsMgr := NewPipelineEventsMgr(idWell, specName, specGetter, logger)

	assert.NotNil(eventsMgr)

	eventsMgr.AddEvent(base.LowPriorityMsg, "testLowPriority", base.NewEventsMap())
	assert.Len(eventsMgr.events.EventInfos, 1)

	events := eventsMgr.GetCurrentEvents()
	events.Mutex.RLock()
	dismissID := events.EventInfos[0].EventId
	assert.Equal(base.LowPriorityMsg, events.EventInfos[0].EventType)
	events.Mutex.RUnlock()

	eventsMgr.DismissEvent(int(dismissID))
	assert.Len(eventsMgr.events.EventInfos, 0)
}
