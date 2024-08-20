// +build !pcre

/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package pipeline_manager

import (
	"fmt"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/pipeline"
	PipelineMgrMock "github.com/couchbase/goxdcr/v8/pipeline_manager/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func setupBoilerPlateSerializer() (*PipelineOpSerializer, *PipelineMgrMock.PipelineMgrForSerializer) {
	testLogger := log.NewLogger("testLogger", log.DefaultLoggerContext)
	pipelineMgr := &PipelineMgrMock.PipelineMgrForSerializer{}

	serializer := NewPipelineOpSerializer(pipelineMgr, testLogger)

	return serializer, pipelineMgr
}

var serializerSleepTime = time.Millisecond * 100

func TestPipelineOpSerializerDelete(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPipelineOpSerializerDelete =================")
	serializer, pipelineMgr := setupBoilerPlateSerializer()
	pipelineMgr.On("RemoveReplicationStatus", mock.Anything).Return(nil).Times(1)

	time.Sleep(serializerSleepTime)
	serializer.Delete("TestTopic")
	time.Sleep(serializerSleepTime)
	assert.Equal(0, len(serializer.jobTopicMap))
	serializer.Stop()
	fmt.Println("============== Test case start: TestPipelineOpSerializerDelete =================")
}

func TestPipelineOpSerializerUpdate(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPipelineOpSerializerUpdate =================")
	serializer, pipelineMgr := setupBoilerPlateSerializer()
	pipelineMgr.On("Update", mock.Anything, mock.Anything).Return(nil).Times(1)

	time.Sleep(serializerSleepTime)
	serializer.Update("TestTopic", nil)
	time.Sleep(serializerSleepTime)
	assert.Equal(0, len(serializer.jobTopicMap))
	serializer.Stop()
	fmt.Println("============== Test case start: TestPipelineOpSerializerUpdate =================")
}

func TestPipelineOpSerializerDeleteTwice(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPipelineOpSerializerDeleteTwice =================")
	serializer, pipelineMgr := setupBoilerPlateSerializer()
	pipelineMgr.On("RemoveReplicationStatus", mock.Anything).Return(nil).Times(2)

	time.Sleep(serializerSleepTime)
	go serializer.Delete("TestTopic")
	go serializer.Delete("TestTopic")
	time.Sleep(serializerSleepTime)
	assert.Equal(0, len(serializer.jobTopicMap))
	serializer.Stop()
	fmt.Println("============== Test case start: TestPipelineOpSerializerDeleteTwice =================")
}

func TestPipelineOpSerializerUpdateTwice(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPipelineOpSerializerUpdateTwice =================")
	serializer, pipelineMgr := setupBoilerPlateSerializer()
	pipelineMgr.On("Update", mock.Anything, mock.Anything).Return(nil).Times(2)

	time.Sleep(serializerSleepTime)
	go serializer.Update("TestTopic", nil)
	go serializer.Update("TestTopic", nil)
	time.Sleep(serializerSleepTime)
	assert.Equal(0, len(serializer.jobTopicMap))
	serializer.Stop()
	fmt.Println("============== Test case start: TestPipelineOpSerializerUpdateTwice =================")
}

func TestPipelineOpSerializerGetTwice(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPipelineOpSerializerGetTwice =================")
	serializer, pipelineMgr := setupBoilerPlateSerializer()
	repStatusPtr := &pipeline.ReplicationStatus{}
	pipelineMgr.On("GetOrCreateReplicationStatus", mock.Anything, mock.Anything).Return(repStatusPtr, nil).Times(2)

	time.Sleep(serializerSleepTime)

	repStatus, repStatusErr := serializer.GetOrCreateReplicationStatus("TestTopic", nil)
	assert.NotNil(repStatus)
	assert.Nil(repStatusErr)
	repStatus, repStatusErr = serializer.GetOrCreateReplicationStatus("TestTopic", nil)
	assert.NotNil(repStatus)
	assert.Nil(repStatusErr)
	assert.Equal(repStatus, repStatusPtr)

	time.Sleep(serializerSleepTime)
	assert.Equal(0, len(serializer.jobTopicMap))
	serializer.Stop()
	fmt.Println("============== Test case start: TestPipelineOpSerializerGetTwice =================")
}

func TestPipelineOpSerializerMix(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPipelineOpSerializerMix =================")
	serializer, pipelineMgr := setupBoilerPlateSerializer()
	pipelineMgr.On("Update", mock.Anything, mock.Anything).Return(nil).Times(1)
	pipelineMgr.On("RemoveReplicationStatus", mock.Anything).Return(nil).Times(2)

	time.Sleep(serializerSleepTime)
	go serializer.Update("TestTopic", nil)
	go serializer.Delete("TestTopic")
	go serializer.Delete("TestTopic")
	time.Sleep(serializerSleepTime)
	assert.Equal(0, len(serializer.jobTopicMap))
	serializer.Stop()
	fmt.Println("============== Test case start: TestPipelineOpSerializerMix =================")
}

func TestPipelineOpSerializerStopped(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPipelineOpSerializerStopped =================")
	serializer, _ := setupBoilerPlateSerializer()
	serializer.Stop()

	stoppedErr := serializer.Update("TestTopic", nil)
	assert.Equal(SerializerStoppedErr, stoppedErr)
	fmt.Println("============== Test case start: TestPipelineOpSerializerStopped =================")
}

func TestPipelineOpSerializerReinit(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPipelineOpSerializerReinit =================")
	serializer, pipelineMgr := setupBoilerPlateSerializer()
	pipelineMgr.On("CleanupPipeline", mock.Anything).Return(nil).Times(1)
	pipelineMgr.On("Update", mock.Anything, mock.Anything).Return(nil).Times(1)

	assert.Nil(serializer.ReInit("TestTopic"))
	time.Sleep(serializerSleepTime)
	assert.Equal(0, len(serializer.jobTopicMap))
	fmt.Println("============== Test case end: TestPipelineOpSerializerReinit =================")
}
