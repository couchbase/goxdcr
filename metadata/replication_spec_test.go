/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadata

import (
	"fmt"
	"strings"
	"testing"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/stretchr/testify/assert"
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

func TestListSameAs(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestListSameAs =================")
	defer fmt.Println("============== Test case end: TestListSameAs =================")

	spec, err := NewReplicationSpecification("S1", "s1UUID", "tcUUID", "tb2", "tbUuid")
	assert.Nil(err)
	spec2 := spec.Clone()
	spec3 := spec.Clone()
	spec3a := spec.Clone()

	list1 := ReplSpecList{spec, spec2, spec3}
	list2 := list1.Clone()
	assert.True(list1.SameAs(list2))
	assert.True(list2.SameAs(list1))

	list3 := ReplSpecList{spec, spec2}
	assert.False(list1.SameAs(list3))
	assert.False(list3.SameAs(list1))

	spec3a.Settings.Active = false
	spec3a.Revision = "abcdef"
	assert.False(spec.Settings.Active == spec3a.Settings.Active)
	// Pretend revision changed as part of setting change
	list4 := ReplSpecList{spec, spec2, spec3a}
	assert.False(list4.SameAs(list1))
	assert.False(list1.SameAs(list4))

}

func TestListRedact(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestListSameAs =================")
	defer fmt.Println("============== Test case end: TestListSameAs =================")

	spec, err := NewReplicationSpecification("S1", "s1UUID", "tcUUID", "tb2", "tbUuid")
	assert.Nil(err)
	spec.Settings.Values[FilterExpressionKey] = "EXISTS(key)"
	spec2 := spec.Clone()
	spec3 := spec.Clone()

	list1 := ReplSpecList{spec, spec2, spec3}

	list1 = list1.Redact()
	assert.True(strings.Contains(list1[0].Settings.Values[FilterExpressionKey].(string), base.UdTagBegin))

}
