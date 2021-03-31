/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software
will be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadata

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecompositionReplId(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestDecompositionReplId =================")
	defer fmt.Println("============== Test case end: TestDecompositionReplId =================")

	replId := "136082a7c89ccdc9aed81cb04a97720f/B1/B2"

	deconstructed := DecomposeReplicationId(replId, nil)

	assert.Equal("136082a7c89ccdc9aed81cb04a97720f", deconstructed.TargetClusterUUID)
	assert.Equal("B1", deconstructed.SourceBucketName)
	assert.Equal("B2", deconstructed.TargetBucketName)
	assert.Equal("Main", deconstructed.PipelineType)

	replId = "backfill_136082a7c89ccdc9aed81cb04a97720f/B1/B2"
	deconstructed = DecomposeReplicationId(replId, deconstructed)
	assert.Equal("Backfill", deconstructed.PipelineType)
}
