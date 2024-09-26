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
	"encoding/json"
	"fmt"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math"
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
		GuardrailResidentRatioCnt:    100,
		GuardrailDataSizeCnt:         200,
		GuardrailDiskSpaceCnt:        300,
		CasPoisonCnt:                 1,
	}

	brokenMap := make(CollectionNamespaceMapping)
	ns1, err := base.NewCollectionNamespaceFromString("s1.col1")
	assert.Nil(err)
	ns2, err := base.NewCollectionNamespaceFromString("s1.col2")
	assert.Nil(err)

	brokenMap.AddSingleMapping(&ns1, &ns2)
	ckptRecord2 := CheckpointRecord{
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
		brokenMappings:               brokenMap,
		GuardrailResidentRatioCnt:    50,
		GuardrailDataSizeCnt:         100,
		GuardrailDiskSpaceCnt:        200,
		CasPoisonCnt:                 2,
	}
	assert.Nil(ckptRecord2.PopulateBrokenMappingSha())

	ckpt_doc := NewCheckpointsDoc("testInternalId")
	added, _ := ckpt_doc.AddRecord(&newCkptRecord)
	assert.True(added)
	added, _ = ckpt_doc.AddRecord(&ckptRecord2)
	assert.True(added)

	marshalledData, err := json.Marshal(ckpt_doc)
	assert.Nil(err)

	ckptDocCompressed, shaMapCompressed, err := ckpt_doc.SnappyCompress()
	assert.Nil(err)

	var checkDoc CheckpointsDoc
	err = json.Unmarshal(marshalledData, &checkDoc)
	assert.Nil(err)
	shaToBrokenMap := make(ShaToCollectionNamespaceMap)
	brokenmapSha, err := brokenMap.Sha256()
	assert.Nil(err)
	shaToBrokenMap[fmt.Sprintf("%s", brokenmapSha)] = &brokenMap

	assert.Equal(5, len(checkDoc.Checkpoint_records))
	assert.NotNil(checkDoc.Checkpoint_records[1])
	assert.True(checkDoc.Checkpoint_records[1].SameAs(&newCkptRecord))
	assert.Equal(newCkptRecord.SourceManifestForBackfillMgr, checkDoc.Checkpoint_records[1].SourceManifestForBackfillMgr)
	assert.NotNil(checkDoc.Checkpoint_records[0])
	assert.True(checkDoc.Checkpoint_records[0].SameAs(&ckptRecord2))

	var decompressCheck CheckpointsDoc
	assert.Nil(decompressCheck.SnappyDecompress(ckptDocCompressed, shaMapCompressed))
	assert.Equal(5, len(decompressCheck.Checkpoint_records))
	assert.NotNil(decompressCheck.Checkpoint_records[1])
	assert.True(decompressCheck.Checkpoint_records[1].SameAs(&newCkptRecord))
	assert.Equal(newCkptRecord.SourceManifestForBackfillMgr, decompressCheck.Checkpoint_records[1].SourceManifestForBackfillMgr)
	assert.NotNil(decompressCheck.Checkpoint_records[0])
	assert.True(decompressCheck.Checkpoint_records[0].SameAs(&ckptRecord2))
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

func TestCheckpointSortByFailoverLogRealData(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCheckpointSortByFailoverLogRealData =================")
	defer fmt.Println("============== Test case end: TestCheckpointSortByFailoverLogRealData =================")

	testDataDir := "./testData/sortCkptData"
	srcFailoverLogFile := testDataDir + "/srcFailoverLogs.json"
	tgtFailoverLogFile := testDataDir + "/tgtFailoverLogs.json"
	ckptsFile := testDataDir + "/currDocs.json"
	peersFile := testDataDir + "/peerDocs.json"

	srcFailoverLogBytes, err := ioutil.ReadFile(srcFailoverLogFile)
	assert.Nil(err)
	tgtFailoverLogBytes, err := ioutil.ReadFile(tgtFailoverLogFile)
	assert.Nil(err)
	ckptBytes, err := ioutil.ReadFile(ckptsFile)
	assert.Nil(err)
	peerBytes, err := ioutil.ReadFile(peersFile)
	assert.Nil(err)

	srcFailoverLogs := make(map[uint16]*mcc.FailoverLog)
	tgtFailoverLogs := make(map[uint16]*mcc.FailoverLog)
	ckpts := make(map[uint16]*CheckpointsDoc)
	peerCkpts := make(map[uint16]*CheckpointsDoc)

	assert.Nil(json.Unmarshal(srcFailoverLogBytes, &srcFailoverLogs))
	assert.Nil(json.Unmarshal(tgtFailoverLogBytes, &tgtFailoverLogs))
	assert.Nil(json.Unmarshal(ckptBytes, &ckpts))
	assert.Nil(json.Unmarshal(peerBytes, &peerCkpts))

	var nonNilDocCounter int
	var nonConformedVBList []uint16
	// The tgtFailoverLog captured above is nil, but should still work
	for vb, ckptDocs := range ckpts {
		srcFailoverLog := srcFailoverLogs[vb]
		assert.NotNil(ckptDocs)
		assert.NotNil(srcFailoverLog)

		if ckptDocs == nil || ckptDocs.Checkpoint_records == nil {
			continue
		}

		curRecordToSort := ckptDocs.Checkpoint_records.PrepareSortStructure(srcFailoverLog, nil)
		if curRecordToSort == nil {
			continue
		}

		var combinedRecords CheckpointSortRecordsList
		combinedRecords = append(combinedRecords, curRecordToSort...)
		assert.NotNil(combinedRecords[0])

		peerRecord, exists := peerCkpts[vb]
		if exists && peerRecord.Checkpoint_records != nil {
			peerRecordToSort := peerRecord.Checkpoint_records.PrepareSortStructure(srcFailoverLog, nil)
			combinedRecords = append(combinedRecords, peerRecordToSort...)
		}

		sort.Sort(combinedRecords)
		regList := combinedRecords.ToRegularList()
		ckpts[vb].Checkpoint_records = regList
		var prevSeqno uint64 = math.MaxUint64
		for _, ckptRecord := range regList {
			if prevSeqno == math.MaxUint64 {
				prevSeqno = ckptRecord.Seqno
			} else {
				if ckptRecord.Seqno > prevSeqno {
					nonConformedVBList = append(nonConformedVBList, vb)
				}
			}
		}

		nonNilDocCounter++
	}

	assert.NotEqual(0, nonNilDocCounter)
	assert.Len(nonConformedVBList, 0)
}

func Test_compareFailoverLogPositionThenSeqnos(t *testing.T) {
	type args struct {
		aRecord *CheckpointSortRecord
		bRecord *CheckpointSortRecord
		source  bool
	}
	tests := []struct {
		name  string
		args  args
		want  bool
		want1 bool
	}{
		{
			name: "Traditional ckpt no failover, compare between seqno, aRecord newer than bRecord",
			args: args{
				aRecord: &CheckpointSortRecord{
					CheckpointRecord: &CheckpointRecord{
						Seqno: 20,
					},
				},
				bRecord: &CheckpointSortRecord{
					CheckpointRecord: &CheckpointRecord{
						Seqno: 10,
					},
				},
				source: true,
			},
			want:  true, // aRecord is more recent than bRecord
			want1: true, // validOp
		},
		{
			name: "Traditional ckpt no failover, compare between seqno, aRecord older than bRecord",
			args: args{
				aRecord: &CheckpointSortRecord{
					CheckpointRecord: &CheckpointRecord{
						Seqno: 10,
					},
				},
				bRecord: &CheckpointSortRecord{
					CheckpointRecord: &CheckpointRecord{
						Seqno: 20,
					},
				},
				source: true,
			},
			want:  false, // aRecord is not more recent than bRecord
			want1: true,  // valid op
		},
		{
			name: "Traditional ckpt failover, compare between failoverlogs even if seqno is lower, aRecord newer than bRecord",
			args: args{
				aRecord: &CheckpointSortRecord{
					CheckpointRecord: &CheckpointRecord{
						Seqno:         10,
						Failover_uuid: 3,
					},
					srcFailoverLog: &mcc.FailoverLog{
						{3, 0},
						{2, 0},
					},
				},
				bRecord: &CheckpointSortRecord{
					CheckpointRecord: &CheckpointRecord{
						Seqno:         20,
						Failover_uuid: 2,
					},
					srcFailoverLog: &mcc.FailoverLog{
						{3, 0},
						{2, 0},
					},
				},
				source: true,
			},
			want:  true, // aRecord is more recent than bRecord
			want1: true, // valid op
		},
		{
			name: "Traditional ckpt failover, compare between failoverlogs but aRecord does not exist in failover log. bRecord wins",
			args: args{
				aRecord: &CheckpointSortRecord{
					CheckpointRecord: &CheckpointRecord{
						Seqno:         10,
						Failover_uuid: 4,
					},
					srcFailoverLog: &mcc.FailoverLog{
						{3, 0},
						{2, 0},
					},
				},
				bRecord: &CheckpointSortRecord{
					CheckpointRecord: &CheckpointRecord{
						Seqno:         20,
						Failover_uuid: 2,
					},
					srcFailoverLog: &mcc.FailoverLog{
						{3, 0},
						{2, 0},
					},
				},
				source: true,
			},
			want:  false, // aRecord is more recent than bRecord
			want1: true,  // valid op
		},
		{
			name: "Traditional target ckpt no failover, compare between seqno, aRecord newer than bRecord",
			args: args{
				aRecord: &CheckpointSortRecord{
					CheckpointRecord: &CheckpointRecord{
						Target_Seqno:     20,
						Target_vb_opaque: &TargetVBUuid{0},
					},
				},
				bRecord: &CheckpointSortRecord{
					CheckpointRecord: &CheckpointRecord{
						Target_Seqno:     10,
						Target_vb_opaque: &TargetVBUuid{0},
					},
				},
				source: false,
			},
			want:  true, // aRecord is more recent than bRecord
			want1: true, // valid op
		},
		{
			name: "Traditional target ckpt no failover, compare between seqno, aRecord older than bRecord",
			args: args{
				aRecord: &CheckpointSortRecord{
					CheckpointRecord: &CheckpointRecord{
						Target_Seqno:     10,
						Target_vb_opaque: &TargetVBUuid{0},
					},
				},
				bRecord: &CheckpointSortRecord{
					CheckpointRecord: &CheckpointRecord{
						Target_Seqno:     20,
						Target_vb_opaque: &TargetVBUuid{0},
					},
				},
				source: false,
			},
			want:  false, // aRecord is not more recent than bRecord
			want1: true,  // valid op
		},
		{
			name: "Traditional target ckpt failover, compare between failoverlogs even if seqno is lower, aRecord newer than bRecord",
			args: args{
				aRecord: &CheckpointSortRecord{
					CheckpointRecord: &CheckpointRecord{
						Target_Seqno:     10,
						Target_vb_opaque: &TargetVBUuid{3},
					},
					tgtFailoverLog: &mcc.FailoverLog{
						{3, 0},
						{2, 0},
					},
				},
				bRecord: &CheckpointSortRecord{
					CheckpointRecord: &CheckpointRecord{
						Target_Seqno:     20,
						Target_vb_opaque: &TargetVBUuid{2},
					},
					tgtFailoverLog: &mcc.FailoverLog{
						{3, 0},
						{2, 0},
					},
				},
				source: false,
			},
			want:  true, // aRecord is more recent than bRecord
			want1: true, // valid op
		},
		{
			name: "Traditional target ckpt failover, compare between failoverlogs but aRecord does not exist in failover log. bRecord wins",
			args: args{
				aRecord: &CheckpointSortRecord{
					CheckpointRecord: &CheckpointRecord{
						Target_Seqno:     10,
						Target_vb_opaque: &TargetVBUuid{4},
					},
					tgtFailoverLog: &mcc.FailoverLog{
						{3, 0},
						{2, 0},
					},
				},
				bRecord: &CheckpointSortRecord{
					CheckpointRecord: &CheckpointRecord{
						Target_Seqno:     20,
						Target_vb_opaque: &TargetVBUuid{2},
					},
					tgtFailoverLog: &mcc.FailoverLog{
						{3, 0},
						{2, 0},
					},
				},
				source: false,
			},
			want:  false, // aRecord is more recent than bRecord
			want1: true,  // valid op
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := compareFailoverLogPositionThenSeqnos(tt.args.aRecord, tt.args.bRecord, tt.args.source)
			assert.Equalf(t, tt.want, got, "compareFailoverLogPositionThenSeqnos(%v, %v, %v)", tt.args.aRecord, tt.args.bRecord, tt.args.source)
			assert.Equalf(t, tt.want1, got1, "compareFailoverLogPositionThenSeqnos(%v, %v, %v)", tt.args.aRecord, tt.args.bRecord, tt.args.source)
		})
	}
}
