// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHeartbeatMetadataSameAs(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("================= Test case start: TestHeartbeatMetadataSameAs =================")

	spec1, _ := NewReplicationSpecification("sourceBucketName", "sourceBucketUUID", "targetClusterUUID", "targetBucketName", "targetBucketUUID")
	spec2, _ := NewReplicationSpecification("sourceBucketName", "sourceBucketUUID", "targetClusterUUID", "targetBucketName2", "targetBucketUUID2")
	spec3, _ := NewReplicationSpecification("sourceBucketName2", "sourceBucketUUID2", "targetClusterUUID2", "targetBucketName", "targetBucketUUID")

	a := HeartbeatMetadata{
		SourceClusterUUID: "SourceClusterUUID",
		SourceClusterName: "SourceClusterName",
		SourceSpecsList:   []*ReplicationSpecification{spec1, spec2, spec3},
		NodesList:         []string{"node1", "node2", "node3"},
		TTL:               time.Hour,
	}
	b, c, d, e, f := a, a, a, a, a

	assert.True(a.SameAs(&a))

	b.SourceClusterName = "b.SourceClusterName"
	assert.False(a.SameAs(&b))

	c.SourceClusterUUID = "c.SourceClusterUUID"
	assert.False(a.SameAs(&c))

	d.SourceSpecsList = []*ReplicationSpecification{spec1}
	assert.False(a.SameAs(&d))

	e.NodesList = []string{"node1", "node2", "node3", "node4"}
	assert.False(a.SameAs(&e))

	f.TTL = time.Minute
	assert.False(a.SameAs(&f))

	fmt.Println("================== Test case end: TestHeartbeatMetadataSameAs ==================")
}
