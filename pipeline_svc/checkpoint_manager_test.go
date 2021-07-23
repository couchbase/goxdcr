// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package pipeline_svc

import (
	"encoding/json"
	"fmt"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func TestCombineFailoverlogs(t *testing.T) {
	fmt.Println("============== Test case start: TestCombineFailoverLogs =================")
	defer fmt.Println("============== Test case end: TestCombineFailoverLogs =================")
	assert := assert.New(t)

	var goodVbUuid uint64 = 1
	var badVbUuid uint64 = 2
	var goodSeqno uint64 = 5
	failoverLog := &mcc.FailoverLog{[2]uint64{goodVbUuid, goodSeqno}}

	failoverLogMap := make(map[uint16]*mcc.FailoverLog)
	// 3 VBs from failoverlog
	failoverLogMap[0] = failoverLog
	failoverLogMap[1] = failoverLog
	failoverLogMap[2] = failoverLog

	goodRecord := &metadata.CheckpointRecord{
		Failover_uuid: goodVbUuid,
		Seqno:         goodSeqno,
	}
	badRecord := &metadata.CheckpointRecord{
		Failover_uuid: badVbUuid,
		Seqno:         goodSeqno,
	}

	goodDoc := &metadata.CheckpointsDoc{
		Checkpoint_records: []*metadata.CheckpointRecord{goodRecord},
	}

	badDoc := &metadata.CheckpointsDoc{
		Checkpoint_records: []*metadata.CheckpointRecord{badRecord},
	}

	mixedDoc := &metadata.CheckpointsDoc{
		Checkpoint_records: []*metadata.CheckpointRecord{goodRecord, badRecord},
	}

	checkMap := make(map[string]map[uint16]*metadata.CheckpointsDoc)
	nodeName := "node"
	checkMap[nodeName] = make(map[uint16]*metadata.CheckpointsDoc)
	checkMap[nodeName][0] = goodDoc
	checkMap[nodeName][1] = mixedDoc
	checkMap[nodeName][2] = badDoc
	checkMap[nodeName][3] = goodDoc // vb3 isn't included

	result := filterInvalidCkptsBasedOnSourceFailover(checkMap, failoverLogMap)
	assert.Len(result, 3)

	assert.Len(result[0].Checkpoint_records, 1)
	assert.Len(result[1].Checkpoint_records, 1)
	assert.Len(result[2].Checkpoint_records, 0)
}

func TestCombineFailoverlogsWithData(t *testing.T) {
	fmt.Println("============== Test case start: TestCombineFailoverLogsWithData =================")
	defer fmt.Println("============== Test case end: TestCombineFailoverLogsWithData =================")
	assert := assert.New(t)

	nodeVbCkptsMapSlice, err := ioutil.ReadFile("./unitTestdata/nodeVbCkptsMap.json")
	assert.Nil(err)
	srcFailoverLogsSlice, err := ioutil.ReadFile("./unitTestdata/srcFailoverLogs.json")
	assert.Nil(err)

	nodeVbCkptsMap := make(map[string]map[uint16]*metadata.CheckpointsDoc)
	assert.Nil(json.Unmarshal(nodeVbCkptsMapSlice, &nodeVbCkptsMap))
	srcFailoverLogs := make(map[uint16]*mcc.FailoverLog)
	assert.Nil(json.Unmarshal(srcFailoverLogsSlice, &srcFailoverLogs))

	for _, ckptMapDoc := range nodeVbCkptsMap {
		for vb, ckptDoc := range ckptMapDoc {
			assert.NotEqual(0, len(ckptDoc.Checkpoint_records))
			assert.NotNil(ckptDoc.Checkpoint_records[0])
			assert.NotNil(srcFailoverLogs[vb])
		}
	}

	filteredMap := filterInvalidCkptsBasedOnSourceFailover(nodeVbCkptsMap, srcFailoverLogs)
	for _, ckptDoc := range filteredMap {
		assert.NotEqual(0, len(ckptDoc.Checkpoint_records))
	}

	tgtFailoverLogsSlice, err := ioutil.ReadFile("./unitTestdata/tgtFailoverJson.json")
	assert.Nil(err)
	tgtFailoverLogs := make(map[uint16]*mcc.FailoverLog)
	assert.Nil(json.Unmarshal(tgtFailoverLogsSlice, &tgtFailoverLogs))
	filteredMapTgt := filterInvalidCkptsBasedOnTargetFailover(filteredMap, tgtFailoverLogs)
	for _, ckptDoc := range filteredMapTgt {
		assert.NotEqual(0, len(ckptDoc.Checkpoint_records))
	}
}
