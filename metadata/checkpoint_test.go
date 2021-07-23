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
	"encoding/json"
	"fmt"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func TestCheckpointDocMarshaller(t *testing.T) {
	fmt.Println("============== Test case start: TestCheckpointDocMarshaller =================")
	defer fmt.Println("============== Test case end: TestCheckpointDocMarshaller =================")
	assert := assert.New(t)

	vbUuidAndTimestamp := &TargetVBUuidAndTimestamp{
		Target_vb_uuid: "abc",
		Startup_time:   "012",
	}
	newCkptRecord := CheckpointRecord{
		Failover_uuid:                0,
		Seqno:                        1,
		Dcp_snapshot_seqno:           2,
		Dcp_snapshot_end_seqno:       3,
		Target_vb_opaque:             vbUuidAndTimestamp,
		Target_Seqno:                 4,
		Filtered_Items_Cnt:           5,
		Filtered_Failed_Cnt:          6,
		SourceManifestForDCP:         7,
		SourceManifestForBackfillMgr: 8,
		TargetManifest:               9,
		BrokenMappingSha256:          "",
		brokenMappings:               nil,
	}

	ckpt_doc := NewCheckpointsDoc("testInternalId")
	added, _ := ckpt_doc.AddRecord(&newCkptRecord)
	assert.True(added)

	marshalledData, err := json.Marshal(ckpt_doc)
	assert.Nil(err)

	var checkDoc CheckpointsDoc
	err = json.Unmarshal(marshalledData, &checkDoc)
	assert.Nil(err)

	assert.Equal(5, len(checkDoc.Checkpoint_records))
	assert.NotNil(checkDoc.Checkpoint_records[0])
	assert.True(checkDoc.Checkpoint_records[0].IsSame(&newCkptRecord))
	assert.Equal(newCkptRecord.SourceManifestForBackfillMgr, checkDoc.Checkpoint_records[0].SourceManifestForBackfillMgr)
}

func TestCheckpointSortBySeqno(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCheckpointSortBySeqno =================")
	defer fmt.Println("============== Test case end: TestCheckpointSortBySeqno =================")

	var unsortedList CheckpointRecordsList

	validFailoverLog := uint64(1234)
	validFailoverLog2 := uint64(12345)
	failoverLog := &mcc.FailoverLog{[2]uint64{validFailoverLog, 0}, [2]uint64{validFailoverLog2, 0}}
	//var invalidFailoverLog uint64 = "2345"

	earlySeqno := uint64(100)
	laterSeqno := uint64(200)
	latestSeqno := uint64(300)

	record := &CheckpointRecord{
		Failover_uuid:    validFailoverLog,
		Seqno:            earlySeqno,
		Target_vb_opaque: nil,
		Target_Seqno:     0,
	}
	record2 := &CheckpointRecord{
		Failover_uuid: validFailoverLog,
		Seqno:         laterSeqno,
	}
	record3 := &CheckpointRecord{
		Failover_uuid: validFailoverLog,
		Seqno:         latestSeqno,
	}

	unsortedList = append(unsortedList, record)
	unsortedList = append(unsortedList, record2)
	unsortedList = append(unsortedList, record3)

	toSortList := unsortedList.PrepareSortStructure(failoverLog, nil)
	sort.Sort(toSortList)
	outputList := toSortList.ToRegularList()
	for i := 0; i < len(outputList)-1; i++ {
		assert.True(outputList[i].Seqno > outputList[i+1].Seqno)
	}
}

func TestCheckpointSortByFailoverLog(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCheckpointSortByFailoverLog =================")
	defer fmt.Println("============== Test case end: TestCheckpointSortByFailoverLog =================")

	var unsortedList CheckpointRecordsList

	validFailoverLog := uint64(1234)
	validFailoverLog2 := uint64(12345)
	failoverLog := &mcc.FailoverLog{[2]uint64{validFailoverLog, 0}, [2]uint64{validFailoverLog2, 0}}

	recordA := &CheckpointRecord{Failover_uuid: validFailoverLog2}
	recordB := &CheckpointRecord{Failover_uuid: validFailoverLog}

	unsortedList = append(unsortedList, recordA)
	unsortedList = append(unsortedList, recordB)

	toSortList := unsortedList.PrepareSortStructure(failoverLog, nil)
	sort.Sort(toSortList)
	outputList := toSortList.ToRegularList()
	// Failover log that shows up first is higher priority
	assert.Equal(validFailoverLog, outputList[0].Failover_uuid)
	assert.Equal(validFailoverLog2, outputList[1].Failover_uuid)
}
