// +build !pcre

/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software
will be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package pipeline

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/stretchr/testify/assert"
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
	repStatus := NewReplicationStatus(specId, specGetter, testLogger)

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
