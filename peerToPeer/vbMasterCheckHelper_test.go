/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package peerToPeer

import (
	"fmt"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestVbMasterCheckHelper(t *testing.T) {
	fmt.Println("============== Test case start: TestVbMasterCheckHelper =================")
	defer fmt.Println("============== Test case end: TestVbMasterCheckHelper =================")
	assert := assert.New(t)
	helper := NewVBMasterCheckHelper()

	spec, _ := metadata.NewReplicationSpecification("srcBucketName", "", "", "", "")
	spec2, _ := metadata.NewReplicationSpecification("srcBucketName2", "", "", "", "")
	helper.HandleSpecCreation(spec)

	fullset := make(BucketVBMapType)
	fullset["srcBucketName"] = []uint16{0, 1, 2, 3}
	fullset["srcBucketName2"] = []uint16{0, 1, 2, 3, 4}
	_, err := helper.GetUnverifiedSubset(fullset)
	assert.NotNil(err)

	helper.HandleSpecCreation(spec2)
	checkSet, err := helper.GetUnverifiedSubset(fullset)
	assert.Nil(err)
	assert.Equal(fullset, checkSet)

	helper.bucketVbsMap["srcBucketName"].verifiedVbs[0] = true
	checkSet2, err := helper.GetUnverifiedSubset(fullset)
	assert.Nil(err)
	assert.NotEqual(fullset, checkSet2)
	assert.Equal([]uint16{1, 2, 3}, checkSet2["srcBucketName"])
}
