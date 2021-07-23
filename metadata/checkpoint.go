// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata

import (
	"encoding/json"
	"fmt"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"sync"
	"unsafe"
)

const (
	FailOverUUID                 string = "failover_uuid"
	Seqno                        string = "seqno"
	DcpSnapshotSeqno             string = "dcp_snapshot_seqno"
	DcpSnapshotEndSeqno          string = "dcp_snapshot_end_seqno"
	TargetVbOpaque               string = "target_vb_opaque"
	TargetSeqno                  string = "target_seqno"
	TargetVbUuid                 string = "target_vb_uuid"
	StartUpTime                  string = "startup_time"
	FilteredCnt                  string = "filtered_items_cnt"
	FilteredFailedCnt            string = "filtered_failed_cnt"
	SourceManifestForDCP         string = "source_manifest_dcp"
	SourceManifestForBackfillMgr string = "source_manifest_backfill_mgr"
	TargetManifest               string = "target_manifest"
	BrokenCollectionsMapSha      string = "brokenCollectionsMapSha256"
)

type CheckpointRecord struct {
	//source vbucket failover uuid
	Failover_uuid uint64 `json:"failover_uuid"`
	//source vbucket high sequence number
	Seqno uint64 `json:"seqno"`
	//source snapshot start sequence number
	Dcp_snapshot_seqno uint64 `json:"dcp_snapshot_seqno"`
	//source snapshot end sequence number
	Dcp_snapshot_end_seqno uint64 `json:"dcp_snapshot_end_seqno"`
	//target vb opaque
	Target_vb_opaque TargetVBOpaque `json:"target_vb_opaque"`
	//target vb high sequence number
	Target_Seqno uint64 `json:"target_seqno"`
	// Number of items filtered
	Filtered_Items_Cnt uint64 `json:"filtered_items_cnt"`
	// Number of items failed filter
	Filtered_Failed_Cnt uint64 `json:"filtered_failed_cnt"`
	// Manifests uid corresponding to this checkpoint
	SourceManifestForDCP         uint64 `json:"source_manifest_dcp"`
	SourceManifestForBackfillMgr uint64 `json:"source_manifest_backfill_mgr"`
	TargetManifest               uint64 `json:"target_manifest"`
	// BrokenMappingInfoType SHA256 string - Internally used by checkpoints Service to populate the actual BrokenMappingInfoType above
	BrokenMappingSha256 string `json:"brokenCollectionsMapSha256"`
	// Broken mapping (if any) associated with the checkpoint - this is populated automatically by checkpointsService
	brokenMappings    CollectionNamespaceMapping
	brokenMappingsMtx sync.RWMutex
}

func (c *CheckpointRecord) BrokenMappings() *CollectionNamespaceMapping {
	if c == nil {
		return nil
	}
	c.brokenMappingsMtx.RLock()
	defer c.brokenMappingsMtx.RUnlock()
	cloned := c.brokenMappings.Clone()
	return &(cloned)
}

func (c *CheckpointRecord) Size() int {
	if c == nil {
		return 0
	}

	var totalSize int
	totalSize += int(unsafe.Sizeof(*c))
	totalSize += len(c.BrokenMappingSha256)
	totalSize += c.Target_vb_opaque.Size()
	return totalSize
}

func NewCheckpointRecord(failoverUuid, seqno, dcpSnapSeqno, dcpSnapEnd, targetSeqno, filteredItems, filterFailed, srcManifestForDCP, srcManifestForBackfill, tgtManifest uint64, brokenMappings CollectionNamespaceMapping) (*CheckpointRecord, error) {
	record := &CheckpointRecord{
		Failover_uuid:                failoverUuid,
		Seqno:                        seqno,
		Dcp_snapshot_seqno:           dcpSnapSeqno,
		Dcp_snapshot_end_seqno:       dcpSnapEnd,
		Target_Seqno:                 targetSeqno,
		Filtered_Items_Cnt:           filteredItems,
		Filtered_Failed_Cnt:          filterFailed,
		SourceManifestForDCP:         srcManifestForDCP,
		SourceManifestForBackfillMgr: srcManifestForBackfill,
		TargetManifest:               tgtManifest,
		brokenMappings:               brokenMappings,
	}
	err := record.PopulateBrokenMappingSha()
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (ckptRecord *CheckpointRecord) PopulateBrokenMappingSha() error {
	ckptRecord.brokenMappingsMtx.RLock()
	if len(ckptRecord.brokenMappings) > 0 {
		sha, err := ckptRecord.brokenMappings.Sha256()
		ckptRecord.brokenMappingsMtx.RUnlock()
		if err != nil {
			return err
		}
		ckptRecord.BrokenMappingSha256 = fmt.Sprintf("%x", sha[:])
	} else {
		ckptRecord.brokenMappingsMtx.RUnlock()
		ckptRecord.BrokenMappingSha256 = ""
	}
	return nil
}

func (ckptRecord *CheckpointRecord) IsSame(new_record *CheckpointRecord) bool {
	if ckptRecord == nil && new_record != nil {
		return false
	} else if ckptRecord != nil && new_record == nil {
		return false
	} else if ckptRecord == nil && new_record == nil {
		return true
	} else if ckptRecord.Failover_uuid == new_record.Failover_uuid &&
		ckptRecord.Seqno == new_record.Seqno &&
		ckptRecord.Dcp_snapshot_seqno == new_record.Dcp_snapshot_seqno &&
		ckptRecord.Dcp_snapshot_end_seqno == new_record.Dcp_snapshot_end_seqno &&
		ckptRecord.Target_vb_opaque.IsSame(new_record.Target_vb_opaque) &&
		ckptRecord.Target_Seqno == new_record.Target_Seqno &&
		ckptRecord.Filtered_Failed_Cnt == new_record.Filtered_Failed_Cnt &&
		ckptRecord.Filtered_Items_Cnt == new_record.Filtered_Items_Cnt &&
		ckptRecord.SourceManifestForDCP == new_record.SourceManifestForDCP &&
		ckptRecord.SourceManifestForBackfillMgr == new_record.SourceManifestForBackfillMgr &&
		ckptRecord.TargetManifest == new_record.TargetManifest &&
		ckptRecord.BrokenMappingSha256 == new_record.BrokenMappingSha256 {
		return true
	} else {
		return false
	}
}

// Loads each value individually minus the opaque
func (ckptRecord *CheckpointRecord) Load(other *CheckpointRecord) {
	if ckptRecord == nil || other == nil {
		return
	}
	ckptRecord.Failover_uuid = other.Failover_uuid
	ckptRecord.Seqno = other.Seqno
	ckptRecord.Dcp_snapshot_seqno = other.Dcp_snapshot_seqno
	ckptRecord.Dcp_snapshot_end_seqno = other.Dcp_snapshot_end_seqno
	ckptRecord.Target_Seqno = other.Target_Seqno
	ckptRecord.Filtered_Items_Cnt = other.Filtered_Items_Cnt
	ckptRecord.Filtered_Failed_Cnt = other.Filtered_Failed_Cnt
	ckptRecord.SourceManifestForDCP = other.SourceManifestForDCP
	ckptRecord.SourceManifestForBackfillMgr = other.SourceManifestForBackfillMgr
	ckptRecord.TargetManifest = other.TargetManifest
	ckptRecord.LoadBrokenMapping(*other.BrokenMappings())
}

func (ckptRecord *CheckpointRecord) LoadBrokenMapping(other CollectionNamespaceMapping) error {
	if ckptRecord == nil {
		return fmt.Errorf("nil ckptRecord")
	}
	ckptRecord.brokenMappingsMtx.Lock()
	ckptRecord.brokenMappings = other
	ckptRecord.brokenMappingsMtx.Unlock()
	return ckptRecord.PopulateBrokenMappingSha()
}

func (ckptRecord *CheckpointRecord) UnmarshalJSON(data []byte) error {
	var fieldMap map[string]interface{}
	err := json.Unmarshal(data, &fieldMap)
	if err != nil {
		return err
	}

	failover_uuid, ok := fieldMap[FailOverUUID]
	if ok {
		ckptRecord.Failover_uuid = uint64(failover_uuid.(float64))
	}

	seqno, ok := fieldMap[Seqno]
	if ok {
		ckptRecord.Seqno = uint64(seqno.(float64))
	}

	dcp_snapshot_seqno, ok := fieldMap[DcpSnapshotSeqno]
	if ok {
		ckptRecord.Dcp_snapshot_seqno = uint64(dcp_snapshot_seqno.(float64))
	}

	dcp_snapshot_end_seqno, ok := fieldMap[DcpSnapshotEndSeqno]
	if ok {
		ckptRecord.Dcp_snapshot_end_seqno = uint64(dcp_snapshot_end_seqno.(float64))
	}

	target_seqno, ok := fieldMap[TargetSeqno]
	if ok {
		ckptRecord.Target_Seqno = uint64(target_seqno.(float64))
	}

	// this is the special logic where we unmarshal targetVBOpaque into different concrete types
	target_vb_opaque, ok := fieldMap[TargetVbOpaque]
	if ok {
		target_vb_opaque_obj, err := UnmarshalTargetVBOpaque(target_vb_opaque)
		if err != nil {
			return err
		}
		ckptRecord.Target_vb_opaque = target_vb_opaque_obj
	}

	filteredCnt, ok := fieldMap[FilteredCnt]
	if ok {
		ckptRecord.Filtered_Items_Cnt = uint64(filteredCnt.(float64))
	}

	filteredFailedCnt, ok := fieldMap[FilteredFailedCnt]
	if ok {
		ckptRecord.Filtered_Failed_Cnt = uint64(filteredFailedCnt.(float64))
	}

	srcManifest, ok := fieldMap[SourceManifestForDCP]
	if ok {
		ckptRecord.SourceManifestForDCP = uint64(srcManifest.(float64))
	}

	srcManifestForBackfill, ok := fieldMap[SourceManifestForBackfillMgr]
	if ok {
		ckptRecord.SourceManifestForBackfillMgr = uint64(srcManifestForBackfill.(float64))
	}

	tgtManifest, ok := fieldMap[TargetManifest]
	if ok {
		ckptRecord.TargetManifest = uint64(tgtManifest.(float64))
	}

	brokenMapSha, ok := fieldMap[BrokenCollectionsMapSha]
	if ok {
		ckptRecord.BrokenMappingSha256 = brokenMapSha.(string)
	}

	return nil
}

type TargetVBOpaque interface {
	Value() interface{}
	IsSame(targetVBOpaque TargetVBOpaque) bool
	Size() int
}

// older clusters have a single int vbuuid
type TargetVBUuid struct {
	Target_vb_uuid uint64 `json:"target_vb_uuid"`
}

func (targetVBUuid *TargetVBUuid) Size() int {
	if targetVBUuid == nil {
		return 0
	}
	return int(unsafe.Sizeof(targetVBUuid.Target_vb_uuid))
}

func (targetVBUuid *TargetVBUuid) Value() interface{} {
	return targetVBUuid.Target_vb_uuid
}

func (targetVBUuid *TargetVBUuid) IsSame(targetVBOpaque TargetVBOpaque) bool {
	if targetVBUuid == nil && targetVBOpaque == nil {
		return true
	} else if targetVBUuid == nil && targetVBOpaque != nil {
		return false
	} else if targetVBUuid != nil && targetVBOpaque == nil {
		return false
	} else {
		new_targetVBUuid, ok := targetVBOpaque.(*TargetVBUuid)
		if !ok {
			return false
		} else {
			return targetVBUuid.Target_vb_uuid == new_targetVBUuid.Target_vb_uuid
		}
	}
}

// elasticSearch clusters have a single string vbuuid
type TargetVBUuidStr struct {
	Target_vb_uuid string `json:"target_vb_uuid"`
}

func (targetVBUuid *TargetVBUuidStr) Size() int {
	return len(targetVBUuid.Target_vb_uuid)
}

func (targetVBUuid *TargetVBUuidStr) Value() interface{} {
	return targetVBUuid.Target_vb_uuid
}

func (targetVBUuid *TargetVBUuidStr) IsSame(targetVBOpaque TargetVBOpaque) bool {
	if targetVBUuid == nil && targetVBOpaque == nil {
		return true
	} else if targetVBUuid == nil && targetVBOpaque != nil {
		return false
	} else if targetVBUuid != nil && targetVBOpaque == nil {
		return false
	} else {
		new_targetVBUuid, ok := targetVBOpaque.(*TargetVBUuidStr)
		if !ok {
			return false
		} else {
			return targetVBUuid.Target_vb_uuid == new_targetVBUuid.Target_vb_uuid
		}
	}
}

// newer clusters have a pair of vbuuid and seqno
type TargetVBUuidAndTimestamp struct {
	Target_vb_uuid string `json:"target_vb_uuid"`
	Startup_time   string `json:"startup_time"`
}

func (t *TargetVBUuidAndTimestamp) Size() int {
	return len(t.Target_vb_uuid) + len(t.Startup_time)
}

func (targetVBUuidAndTimestamp *TargetVBUuidAndTimestamp) Value() interface{} {
	valueArr := make([]interface{}, 2)
	valueArr[0] = targetVBUuidAndTimestamp.Target_vb_uuid
	valueArr[1] = targetVBUuidAndTimestamp.Startup_time
	return valueArr
}

func (targetVBUuidAndTimestamp *TargetVBUuidAndTimestamp) IsSame(targetVBOpaque TargetVBOpaque) bool {
	if targetVBUuidAndTimestamp == nil && targetVBOpaque == nil {
		return true
	} else if targetVBUuidAndTimestamp == nil && targetVBOpaque != nil {
		return false
	} else if targetVBUuidAndTimestamp != nil && targetVBOpaque == nil {
		return false
	} else {
		new_targetVBUuidAndTimestamp, ok := targetVBOpaque.(*TargetVBUuidAndTimestamp)
		if !ok {
			return false
		} else {
			return targetVBUuidAndTimestamp.Target_vb_uuid == new_targetVBUuidAndTimestamp.Target_vb_uuid && targetVBUuidAndTimestamp.Startup_time == new_targetVBUuidAndTimestamp.Startup_time
		}
	}
}

func UnmarshalTargetVBOpaque(data interface{}) (TargetVBOpaque, error) {
	if data == nil {
		return nil, nil
	}

	fieldMap, ok := data.(map[string]interface{})
	if !ok {
		return nil, TargetVBOpaqueUnmarshalError(data)
	}

	if len(fieldMap) == 1 {
		// unmarshal TargetVBUuid
		target_vb_uuid, ok := fieldMap[TargetVbUuid]
		if !ok {
			return nil, TargetVBOpaqueUnmarshalError(data)
		}

		target_vb_uuid_float, ok := target_vb_uuid.(float64)
		if ok {
			return &TargetVBUuid{uint64(target_vb_uuid_float)}, nil
		}

		target_vb_uuid_string, ok := target_vb_uuid.(string)
		if ok {
			return &TargetVBUuidStr{target_vb_uuid_string}, nil
		}

		return nil, TargetVBOpaqueUnmarshalError(data)

	} else if len(fieldMap) == 2 {
		// unmarshal TargetVBUuidAndTimestamp
		target_vb_uuid, ok := fieldMap[TargetVbUuid]
		if !ok {
			return nil, TargetVBOpaqueUnmarshalError(data)
		}

		target_vb_uuid_string, ok := target_vb_uuid.(string)
		if !ok {
			return nil, TargetVBOpaqueUnmarshalError(data)
		}

		startup_time, ok := fieldMap[StartUpTime]
		if !ok {
			return nil, TargetVBOpaqueUnmarshalError(data)
		}

		startup_time_string, ok := startup_time.(string)
		if !ok {
			return nil, TargetVBOpaqueUnmarshalError(data)
		}

		return &TargetVBUuidAndTimestamp{target_vb_uuid_string, startup_time_string}, nil
	}

	return nil, TargetVBOpaqueUnmarshalError(data)
}

func TargetVBOpaqueUnmarshalError(data interface{}) error {
	return fmt.Errorf("error unmarshaling target vb opaque. data=%v", data)
}

func (ckpt_record *CheckpointRecord) String() string {
	ckpt_record.brokenMappingsMtx.RLock()
	defer ckpt_record.brokenMappingsMtx.RUnlock()
	return fmt.Sprintf("{Failover_uuid=%v; Seqno=%v; Dcp_snapshot_seqno=%v; Dcp_snapshot_end_seqno=%v; Target_vb_opaque=%v; Commitopaque=%v; SourceManifestForDCP=%v; SourceManifestForBackfillMgr=%v; TargetManifest=%v; BrokenMappingSha=%v; BrokenMappingInfoType=%v}",
		ckpt_record.Failover_uuid, ckpt_record.Seqno, ckpt_record.Dcp_snapshot_seqno, ckpt_record.Dcp_snapshot_end_seqno, ckpt_record.Target_vb_opaque,
		ckpt_record.Target_Seqno, ckpt_record.SourceManifestForDCP, ckpt_record.SourceManifestForBackfillMgr, ckpt_record.TargetManifest, ckpt_record.BrokenMappingSha256, ckpt_record.brokenMappings)
}

type CheckpointSortRecordsList []*CheckpointSortRecord
type CheckpointSortRecord struct {
	*CheckpointRecord
	srcFailoverLog *mcc.FailoverLog
	tgtFailoverLog *mcc.FailoverLog
}

func (c CheckpointSortRecordsList) Len() int      { return len(c) }
func (c CheckpointSortRecordsList) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

func (c CheckpointSortRecordsList) ToRegularList() CheckpointRecordsList {
	var outList CheckpointRecordsList
	for _, ckptSortRecord := range c {
		outList = append(outList, ckptSortRecord.CheckpointRecord)
	}
	return outList
}

func findIdxGivenRecordAndFailoverlog(vbUuid uint64, failoverLog *mcc.FailoverLog) (int, bool) {
	if failoverLog == nil {
		return -1, false
	}

	for idx, pair := range *failoverLog {
		// vbuuid is 0th element
		if pair[0] == vbUuid {
			return idx, true
		}
	}

	return -1, false
}

// Since XDCR resumes pipelines by reading ckpts from idx 0 and onwards,
// checkpoints is sorted in reverse chronological order. This means that a < b if a happens later than b
func (c CheckpointSortRecordsList) Less(i, j int) bool {
	aRecord := c[i]
	bRecord := c[j]

	// If failover records are there, use those first
	if aRecord.srcFailoverLog != nil && bRecord.srcFailoverLog != nil {
		// First compare failover position
		result, done := compareFailoverLogPositionThenSeqnos(aRecord, bRecord, true /*src*/)
		if done {
			return result
		}
	}

	// If one record has failoverlog and another record doesn't, pick the one that does
	if aRecord.srcFailoverLog != nil && bRecord.srcFailoverLog == nil {
		// Let A be prioritized (less than)
		return true
	} else if aRecord.srcFailoverLog == nil && bRecord.srcFailoverLog != nil {
		return false
	}

	// Failover records comparison doesn't apply here
	if aRecord.Seqno != bRecord.Seqno {
		// Let seqno dictate
		return aRecord.Seqno > bRecord.Seqno
	}

	// By this point, both records cannot match on source failover UUID
	// And both cannot match by source seqnos
	// Let target side dictate who wins
	if aRecord.tgtFailoverLog != nil && bRecord.tgtFailoverLog == nil {
		return true
	} else if aRecord.tgtFailoverLog == nil && bRecord.tgtFailoverLog != nil {
		return false
	} else if aRecord.tgtFailoverLog != nil && bRecord.tgtFailoverLog != nil {
		result, done := compareFailoverLogPositionThenSeqnos(aRecord, bRecord, false /*src*/)
		if done {
			return result
		}
	}

	// Last resort
	return aRecord.Target_Seqno > bRecord.Target_Seqno
}

func compareFailoverLogPositionThenSeqnos(aRecord *CheckpointSortRecord, bRecord *CheckpointSortRecord, source bool) (bool, bool) {
	var aFailoverLog *mcc.FailoverLog
	var aFailoverUuid uint64
	var aSeqno uint64

	var bFailoverLog *mcc.FailoverLog
	var bFailoverUuid uint64
	var bSeqno uint64

	if source {
		aFailoverLog = aRecord.srcFailoverLog
		bFailoverLog = bRecord.srcFailoverLog
		aFailoverUuid = aRecord.Failover_uuid
		aSeqno = aRecord.Seqno
	} else {
		aFailoverLog = aRecord.tgtFailoverLog
		bFailoverLog = bRecord.tgtFailoverLog
		aFailoverUuid = aRecord.Target_vb_opaque.Value().(uint64)
		aSeqno = aRecord.Target_Seqno
	}

	aFailoverPos, aFound := findIdxGivenRecordAndFailoverlog(aFailoverUuid, aFailoverLog)
	bFailoverPos, bFound := findIdxGivenRecordAndFailoverlog(bFailoverUuid, bFailoverLog)
	if aFound && !bFound {
		// aRecord has vbuuid, bRecord doesn't... aRecord should be considered "more valid", aka "more recent"
		return true, true
	} else if !aFound && bFound {
		// converse of above
		return false, true
	} else {
		if aFound && bFound && aFailoverPos != bFailoverPos {
			// Comparison of index is only valid if failoverPos are different
			// Failover logs are sent back in the order of recent -> oldest
			return aFailoverPos < bFailoverPos, true
		} else {
			if aRecord.Seqno != bRecord.Seqno {
				// If aSeqno is > than bSeqno, that means aRecord should be "less than" or "newer" than bRecord
				return aSeqno > bSeqno, true
			}
		}
	}
	return false, false
}

type CheckpointRecordsList []*CheckpointRecord

func (c *CheckpointRecordsList) PrepareSortStructure(srcFailoverlog, tgtFailoverlog *mcc.FailoverLog) CheckpointSortRecordsList {
	var sortRecordsList CheckpointSortRecordsList
	if c == nil {
		return sortRecordsList
	}
	for _, checkpointRecord := range *c {
		sortRecordsList = append(sortRecordsList, &CheckpointSortRecord{
			CheckpointRecord: checkpointRecord,
			srcFailoverLog:   srcFailoverlog,
			tgtFailoverLog:   tgtFailoverlog,
		})
	}
	return sortRecordsList
}

type CheckpointsDoc struct {
	//keep "MaxCheckpointsKept" checkpoint record - ordered by new to old, with 0th element being the newest
	Checkpoint_records CheckpointRecordsList `json:"checkpoints"`

	// internal id of repl spec - for detection of repl spec deletion and recreation event
	SpecInternalId string `json:"specInternalId"`

	//revision number
	Revision interface{}
}

func (c *CheckpointsDoc) CloneWithoutRecords() *CheckpointsDoc {
	return &CheckpointsDoc{
		SpecInternalId: c.SpecInternalId,
		Revision:       nil,
	}
}

func (c *CheckpointsDoc) Size() int {
	if c == nil {
		return 0
	}

	var totalSize int
	for _, j := range c.Checkpoint_records {
		totalSize += j.Size()
	}
	totalSize += len(c.SpecInternalId)
	return totalSize
}

func (ckpt *CheckpointRecord) ToMap() map[string]interface{} {
	ckpt_record_map := make(map[string]interface{})
	ckpt_record_map[FailOverUUID] = ckpt.Failover_uuid
	ckpt_record_map[Seqno] = ckpt.Seqno
	ckpt_record_map[DcpSnapshotSeqno] = ckpt.Dcp_snapshot_seqno
	ckpt_record_map[DcpSnapshotEndSeqno] = ckpt.Dcp_snapshot_end_seqno
	ckpt_record_map[TargetVbOpaque] = ckpt.Target_vb_opaque
	ckpt_record_map[TargetSeqno] = ckpt.Target_Seqno
	return ckpt_record_map
}

func NewCheckpointsDoc(specInternalId string) *CheckpointsDoc {
	ckpt_doc := &CheckpointsDoc{Checkpoint_records: []*CheckpointRecord{},
		SpecInternalId: specInternalId,
		Revision:       nil}

	for i := 0; i < base.MaxCheckpointRecordsToKeep; i++ {
		ckpt_doc.Checkpoint_records = append(ckpt_doc.Checkpoint_records, nil)
	}

	return ckpt_doc
}

//Not concurrency safe. It should be used by one goroutine only
func (ckptsDoc *CheckpointsDoc) AddRecord(record *CheckpointRecord) (added bool, removedRecords []*CheckpointRecord) {
	length := len(ckptsDoc.Checkpoint_records)
	if length > 0 {
		if !ckptsDoc.Checkpoint_records[0].IsSame(record) {
			if length > base.MaxCheckpointRecordsToKeep {
				for i := base.MaxCheckpointRecordsToKeep - 1; i < length; i++ {
					removedRecords = append(removedRecords, ckptsDoc.Checkpoint_records[i])
				}
				ckptsDoc.Checkpoint_records = ckptsDoc.Checkpoint_records[:base.MaxCheckpointRecordsToKeep]
			} else if length < base.MaxCheckpointRecordsToKeep {
				for i := length; i < base.MaxCheckpointRecordsToKeep; i++ {
					ckptsDoc.Checkpoint_records = append(ckptsDoc.Checkpoint_records, nil)
				}
			}
			for i := len(ckptsDoc.Checkpoint_records) - 2; i >= 0; i-- {
				if i+1 == len(ckptsDoc.Checkpoint_records)-1 {
					removedRecords = append(removedRecords, ckptsDoc.Checkpoint_records[i+1])
				}
				ckptsDoc.Checkpoint_records[i+1] = ckptsDoc.Checkpoint_records[i]
			}
			ckptsDoc.Checkpoint_records[0] = record
			added = true
			return
		} else {
			return
		}
	} else {
		ckptsDoc.Checkpoint_records = append(ckptsDoc.Checkpoint_records, record)
		added = true
		return
	}
}

// all access to ckptsDoc.Checkpoint_records should go through this method
// too bad that we cannot hide ckptsDoc.Checkpoint_records by renaming it to ckptsDoc.checkpoint_records
// since it would have disabled json marshaling
func (ckptsDoc *CheckpointsDoc) GetCheckpointRecords() []*CheckpointRecord {
	if len(ckptsDoc.Checkpoint_records) <= base.MaxCheckpointRecordsToRead {
		return ckptsDoc.Checkpoint_records
	} else {
		return ckptsDoc.Checkpoint_records[:base.MaxCheckpointRecordsToRead]
	}
}
