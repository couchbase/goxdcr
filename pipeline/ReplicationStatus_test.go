//go:build !pcre
// +build !pcre

/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package pipeline

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	utilsMock "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
)

func setupBoilerPlate() (*log.CommonLogger,
	string,
	*metadata.ReplicationSpecification,
	ReplicationSpecGetter,
	*ReplicationStatus) {

	testLogger := log.NewLogger("testLogger", log.DefaultLoggerContext)
	specId := "testSpec"
	testSpec, _ := metadata.NewReplicationSpecification("TestSourceBucket", "TestTargetBucket", "targetClusterUUID", "targetBucketName", "targetBucketUUID")
	specGetter := func(string) (*metadata.ReplicationSpecification, error) {
		return testSpec, nil
	}
	utils := &utilsMock.UtilsIface{}
	repStatus := NewReplicationStatus(specId, specGetter, testLogger, nil, utils)

	return testLogger, specId, testSpec, specGetter, repStatus
}

func TestReplicationStatusErrorMap(t *testing.T) {
	fmt.Println("============== Test case start: TestReplicationStatusErrorMap =================")
	assert := assert.New(t)
	_, _, _, _, repStatus := setupBoilerPlate()

	errMsg := "TestError"
	testErr := errors.New(errMsg)
	repStatus.AddError(testErr)
	assert.Equal(1, len(repStatus.err_list))
	repStatus.AddError(testErr)
	assert.Equal(2, len(repStatus.err_list))

	oneMap := make(base.ErrorMap)
	for i := 0; i < 5; i++ {
		oneMap[fmt.Sprintf("%v", i)] = errors.New(fmt.Sprintf("%v", i))
	}

	repStatus.AddErrorsFromMap(oneMap)
	assert.Equal(7, len(repStatus.err_list))
	assert.Equal(repStatus.err_list[6].ErrMsg, errMsg)

	fmt.Println("============== Test case end: TestReplicationStatusErrorMap =================")
}

func TestReplicationStatusErrorMapFull(t *testing.T) {
	fmt.Println("============== Test case start: TestReplicationStatusErrorMapFull =================")
	assert := assert.New(t)
	_, _, _, _, repStatus := setupBoilerPlate()

	fullMap := make(base.ErrorMap)
	for i := 0; i < PipelineErrorMaxEntries+1; i++ {
		fullMap[fmt.Sprintf("%v", i)] = errors.New(fmt.Sprintf("%v", i))
	}

	repStatus.AddErrorsFromMap(fullMap)
	assert.Equal(PipelineErrorMaxEntries, len(repStatus.err_list))

	// Newest string
	newErrString := "NewError"
	repStatus.AddError(errors.New(newErrString))
	assert.Equal(PipelineErrorMaxEntries, len(repStatus.err_list))
	assert.Equal(repStatus.err_list[0].ErrMsg, newErrString)

	fmt.Println("============== Test case end: TestReplicationStatusErrorMapFull =================")
}

func setupEventMgrMock(m PipelineEventsManager, numOfLowMsg int) {
	for i := 0; i < numOfLowMsg; i++ {
		m.AddEvent(base.LowPriorityMsg, fmt.Sprintf("testEvent message %v", numOfLowMsg), base.EventsMap{}, nil)
	}
}

func TestReplicationStatusFillReplInfo(t *testing.T) {
	fmt.Println("============== Test case start: TestReplicationStatusFillReplInfo =================")
	defer fmt.Println("============== Test case end: TestReplicationStatusFillReplInfo =================")

	assert := assert.New(t)
	_, _, _, _, repStatus := setupBoilerPlate()
	setupEventMgrMock(repStatus.eventsManager, 2)

	assert.Equal(int64(1), atomic.LoadInt64(repStatus.eventIdWell))

	eventsList := repStatus.GetEventsManager().GetCurrentEvents()
	assert.Equal(2, eventsList.Len())

	brokenMap := make(metadata.CollectionNamespaceMapping)
	ns1, _ := base.NewCollectionNamespaceFromString("s1.col1")
	ns2, _ := base.NewCollectionNamespaceFromString("s1.col2")
	brokenMap.AddSingleMapping(&ns1, &ns1)
	brokenMap.AddSingleMapping(&ns1, &ns2)

	eventsMgr := repStatus.eventsManager.(*PipelineEventsMgr)

	brokenMapRO, doneFunc := eventsMgr.cachedBrokenMap.GetBrokenMapRO()
	assert.Len(brokenMapRO, 0)
	doneFunc()
	repStatus.GetEventsManager().LoadLatestBrokenMap(brokenMap)
	brokenMapRO, doneFunc = eventsMgr.cachedBrokenMap.GetBrokenMapRO()
	assert.Len(brokenMapRO, 1)
	doneFunc()
	assert.True(eventsMgr.cachedBrokenMap.NeedsToUpdate())
}
