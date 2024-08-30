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
	"sync"
	"unsafe"

	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/golang/snappy"
)

const (
	FailOverUUID                       string = "failover_uuid"
	Seqno                              string = "seqno"
	DcpSnapshotSeqno                   string = "dcp_snapshot_seqno"
	DcpSnapshotEndSeqno                string = "dcp_snapshot_end_seqno"
	TargetVbOpaque                     string = "target_vb_opaque"
	TargetSeqno                        string = "target_seqno"
	TargetVbUuid                       string = "target_vb_uuid"
	StartUpTime                        string = "startup_time"
	FilteredCnt                        string = "filtered_items_cnt"
	FilteredFailedCnt                  string = "filtered_failed_cnt"
	FilteredItemsOnExpirationsCnt      string = "expirations_filtered_cnt"
	FilteredItemsOnDeletionsCnt        string = "deletions_filtered_cnt"
	FilteredItemsOnSetCnt              string = "set_filtered_cnt"
	FilteredItemsOnExpiryStrippedCnt   string = "expiry_stripped_cnt"
	FilteredItemsOnBinaryDocsCnt       string = "binary_docs_filtered_cnt"
	FilteredItemsOnATRDocsCnt          string = "ATR_docs_filtered_cnt"
	FilteredItemsOnClientTxnRecordsCnt string = "client_txn_records_filtered_cnt"
	FilteredItemsOnTxnXattrsDocsCnt    string = "docs_with_txn_xattrs_filtered_cnt"
	FilteredItemsOnMobileRecords       string = "mobile_records_filtered_cnt"
	FilteredItemsOnUserDefinedFilters  string = "docs_filtered_on_user_defined_filters_cnt"
	SourceManifestForDCP               string = "source_manifest_dcp"
	SourceManifestForBackfillMgr       string = "source_manifest_backfill_mgr"
	TargetManifest                     string = "target_manifest"
	BrokenCollectionsMapSha            string = "brokenCollectionsMapSha256"
	CreationTime                       string = "creationTime"
	GuardrailResidentRatioCnt          string = "guardrail_resident_ratio_cnt"
	GuardrailDataSizeCnt               string = "guardrail_data_size_cnt"
	GuardrailDiskSpaceCnt              string = "guardrail_disk_space_cnt"
	DocsSentWithSubdocSetCnt           string = "docs_sent_with_subdoc_set"
	DocsSentWithSubdocDeleteCnt        string = "docs_sent_with_subdoc_delete"
	DocsSentWithPoisonedCasErrorMode   string = "docs_sent_with_poisoned_cas_error_mode"
	DocsSentWithPoisonedCasReplaceMode string = "docs_sent_with_poisoned_cas_replace_mode"
	CasPoisonCnt                       string = "cas_poison_cnt"
	SrcConflictDocsWritten             string = "clog_src_docs_written"
	TgtConflictDocsWritten             string = "clog_tgt_docs_written"
	CRDConflictDocsWritten             string = "clog_crd_docs_written"
	TrueConflictsDetected              string = "true_conflicts_detected"
	FilteredConflictDocs               string = "clog_docs_filtered_cnt"
	CLogHibernatedCnt                  string = "clog_hibernated_cnt"
	GetDocsCasChangedCnt               string = "get_docs_cas_changed_cnt"
)

// SourceVBTimestamp defines the necessary items to start or resume a source DCP stream in a checkpoint context
type SourceVBTimestamp struct {
	//source vbucket failover uuid
	Failover_uuid uint64 `json:"failover_uuid"`
	//source vbucket high sequence number
	Seqno uint64 `json:"seqno"`
	//source snapshot start sequence number
	Dcp_snapshot_seqno uint64 `json:"dcp_snapshot_seqno"`
	//source snapshot end sequence number
	Dcp_snapshot_end_seqno uint64 `json:"dcp_snapshot_end_seqno"`

	SourceManifestForDCP         uint64 `json:"source_manifest_dcp"`
	SourceManifestForBackfillMgr uint64 `json:"source_manifest_backfill_mgr"`
}

// TargetVBTimestamp defines the necessary items to confirm that the checkpoint is valid for resuming
type TargetVBTimestamp struct {
	//target vb opaque / high seqno
	Target_vb_opaque TargetVBOpaque `json:"target_vb_opaque"`
	//target vb high sequence number
	Target_Seqno uint64 `json:"target_seqno"`
	// Manifests uid corresponding to this checkpoint
	TargetManifest uint64 `json:"target_manifest"`
}

// SourceFilteredCounters are item counts based on source VB streams and are restored to the source-related
// components when resuming from a checkpoint
type SourceFilteredCounters struct {
	// Number of items filtered
	Filtered_Items_Cnt uint64 `json:"filtered_items_cnt"`
	// Number of items failed filter
	Filtered_Failed_Cnt uint64 `json:"filtered_failed_cnt"`
	// Number of Expirations filtered
	FilteredItemsOnExpirationsCnt uint64 `json:"expirations_filtered_cnt"`
	// Number of Deletions that were filtered
	FilteredItemsOnDeletionsCnt uint64 `json:"deletions_filtered_cnt"`
	// Number of documents filtered that was of a DCP mutation
	FilteredItemsOnSetCnt uint64 `json:"set_filtered_cnt"`
	// Number of Document Mutations replicated that had the TTL changed to 0 before writing to Targe
	FilteredItemsOnExpiryStrippedCnt uint64 `json:"expiry_stripped_cnt"`
	// Number of binary documents filtered
	FilteredItemsOnBinaryDocsCnt uint64 `json:"binary_docs_filtered_cnt"`
	// Number of ATR transaction documents filtered
	FilteredItemsOnATRDocsCnt uint64 `json:"ATR_docs_filtered_cnt"`
	// Number of client transaction records filtered
	FilteredItemsOnClientTxnRecordsCnt uint64 `json:"client_txn_records_filtered_cnt"`
	// Number of documents filtered because of the presence of transaction xattrs
	FilteredItemsOnTxnXattrsDocsCnt uint64 `json:"docs_with_txn_xattrs_filtered_cnt"`
	// Number of mobile records filtered
	FilteredItemsOnMobileRecords uint64 `json:"mobile_records_filtered_cnt"`
	// Number of mobile records filtered
	FilteredItemsOnUserDefinedFilters uint64 `json:"docs_filtered_on_user_defined_filters_cnt"`
	// conflict logger stats
	FilteredConflictDocs   uint64 `json:"clog_docs_filtered_cnt"`
	SrcConflictDocsWritten uint64 `json:"clog_src_docs_written"`
	TgtConflictDocsWritten uint64 `json:"clog_tgt_docs_written"`
	CRDConflictDocsWritten uint64 `json:"clog_crd_docs_written"`
}

// TargetPerVBCounters contain a list of counters that are collected throughout a target VB's
// lifetime. Each one is stored and restored on a per target VB basis
type TargetPerVBCounters struct {
	// Guardrails
	GuardrailResidentRatioCnt uint64 `json:"guardrail_resident_ratio_cnt"`
	GuardrailDataSizeCnt      uint64 `json:"guardrail_data_size_cnt"`
	GuardrailDiskSpaceCnt     uint64 `json:"guardrail_disk_space_cnt"`
	// Number of subdoc commands used, instead of set_with_meta/delete_with_meta
	DocsSentWithSubdocSetCnt    uint64 `json:"docs_sent_with_subdoc_set"`
	DocsSentWithSubdocDeleteCnt uint64 `json:"docs_sent_with_subdoc_delete"`
	CasPoisonCnt                uint64 `json:"cas_poison_cnt"`
	// Docs sent with a poisoned CAS value
	DocsSentWithPoisonedCasErrorMode   uint64 `json:"docs_sent_with_poisoned_cas_error_mode"`
	DocsSentWithPoisonedCasReplaceMode uint64 `json:"docs_sent_with_poisoned_cas_replace_mode"`
	// conflict logger stats
	TrueConflictsDetected uint64 `json:"true_conflicts_detected"`
	CLogHibernatedCnt     uint64 `json:"clog_hibernated_cnt"`
	GetDocsCasChangedCnt  uint64 `json:"get_docs_cas_changed_cnt"`
}

type CheckpointRecord struct {
	// Epoch timestamp of when this record was created
	CreationTime uint64 `json:"creationTime"`

	SourceVBTimestamp
	SourceFilteredCounters

	// Traditional checkpoints will utilize one single TargetVBTimestamp and one single TargetPerVBCounters
	TargetVBTimestamp
	TargetPerVBCounters

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

func NewCheckpointRecord(failoverUuid, seqno, dcpSnapSeqno, dcpSnapEnd, targetSeqno uint64,
	incomingMetric base.VBCountMetric, srcManifestForDCP, srcManifestForBackfill,
	tgtManifest uint64, brokenMappings CollectionNamespaceMapping, creationTime uint64) (*CheckpointRecord, error) {

	var record *CheckpointRecord

	if incomingMetric.IsTraditional() {
		record = newTraditionalCheckpointRecord(failoverUuid, seqno, dcpSnapSeqno, dcpSnapEnd, targetSeqno, srcManifestForDCP, srcManifestForBackfill, tgtManifest, brokenMappings, creationTime, incomingMetric)
	} else {
		return nil, fmt.Errorf("TODO global checkpoint")
	}
	err := record.PopulateBrokenMappingSha()
	if err != nil {
		return nil, err
	}
	return record, nil
}

func newTraditionalCheckpointRecord(failoverUuid uint64, seqno uint64, dcpSnapSeqno uint64, dcpSnapEnd uint64, targetSeqno uint64, srcManifestForDCP uint64, srcManifestForBackfill uint64, tgtManifest uint64, brokenMappings CollectionNamespaceMapping, creationTime uint64, incomingMetric base.VBCountMetric) *CheckpointRecord {
	vbCountMetrics := incomingMetric.(*base.TraditionalVBMetrics).GetValue()
	filteredItems := uint64(vbCountMetrics[base.DocsFiltered])
	filterFailed := uint64(vbCountMetrics[base.DocsUnableToFilter])
	filteredExpiredItems := uint64(vbCountMetrics[base.ExpiryFiltered])
	filteredDelItems := uint64(vbCountMetrics[base.DeletionFiltered])
	filteredSetItems := uint64(vbCountMetrics[base.SetFiltered])
	filteredBinaryDocItems := uint64(vbCountMetrics[base.BinaryFiltered])
	filteredExpiryStripItems := uint64(vbCountMetrics[base.ExpiryStripped])
	filteredATRDocItems := uint64(vbCountMetrics[base.AtrTxnDocsFiltered])
	filteredClientTxnDocItems := uint64(vbCountMetrics[base.ClientTxnDocsFiltered])
	filteredTxnXattrsItems := uint64(vbCountMetrics[base.DocsFilteredOnTxnXattr])
	filteredMobileDocItems := uint64(vbCountMetrics[base.MobileDocsFiltered])
	filteredDocsOnUserDefinedFilters := uint64(vbCountMetrics[base.DocsFilteredOnUserDefinedFilter])
	guardrailResidentRatioItems := uint64(vbCountMetrics[base.GuardrailResidentRatio])
	guardrailDataSizeItems := uint64(vbCountMetrics[base.GuardrailDataSize])
	guardrailDiskSpaceItems := uint64(vbCountMetrics[base.GuardrailDiskSpace])
	subdocSetItems := uint64(vbCountMetrics[base.DocsSentWithSubdocSet])
	subdocDeleteItems := uint64(vbCountMetrics[base.DocsSentWithSubdocDelete])
	casPoisonItems := uint64(vbCountMetrics[base.DocsCasPoisoned])
	docsSentWithPoisonedCasErrorMode := uint64(vbCountMetrics[base.DocsSentWithPoisonedCasErrorMode])
	docsSentWithPoisonedCasReplaceMode := uint64(vbCountMetrics[base.DocsSentWithPoisonedCasReplaceMode])
	srcConflictDocsWritten := uint64(vbCountMetrics[base.SrcConflictDocsWritten])
	tgtConflictDocsWritten := uint64(vbCountMetrics[base.TgtConflictDocsWritten])
	crdConflictDocsWritten := uint64(vbCountMetrics[base.CRDConflictDocsWritten])
	trueConflictsDetected := uint64(vbCountMetrics[base.TrueConflictsDetected])
	conflictDocsFiltered := uint64(vbCountMetrics[base.ConflictDocsFiltered])
	clogHibernatedCnt := uint64(vbCountMetrics[base.CLogHibernatedCount])
	getDocsCasChangedCnt := uint64(vbCountMetrics[base.GetDocsCasChangedCount])

	record := &CheckpointRecord{
		SourceVBTimestamp: SourceVBTimestamp{
			Failover_uuid:                failoverUuid,
			Seqno:                        seqno,
			Dcp_snapshot_seqno:           dcpSnapSeqno,
			Dcp_snapshot_end_seqno:       dcpSnapEnd,
			SourceManifestForDCP:         srcManifestForDCP,
			SourceManifestForBackfillMgr: srcManifestForBackfill,
		},
		TargetVBTimestamp: TargetVBTimestamp{
			Target_Seqno:   targetSeqno,
			TargetManifest: tgtManifest,
		},
		SourceFilteredCounters: SourceFilteredCounters{
			Filtered_Items_Cnt:                 filteredItems,
			Filtered_Failed_Cnt:                filterFailed,
			FilteredItemsOnExpirationsCnt:      filteredExpiredItems,
			FilteredItemsOnDeletionsCnt:        filteredDelItems,
			FilteredItemsOnSetCnt:              filteredSetItems,
			FilteredItemsOnExpiryStrippedCnt:   filteredExpiryStripItems,
			FilteredItemsOnBinaryDocsCnt:       filteredBinaryDocItems,
			FilteredItemsOnATRDocsCnt:          filteredATRDocItems,
			FilteredItemsOnClientTxnRecordsCnt: filteredClientTxnDocItems,
			FilteredItemsOnTxnXattrsDocsCnt:    filteredTxnXattrsItems,
			FilteredItemsOnMobileRecords:       filteredMobileDocItems,
			FilteredItemsOnUserDefinedFilters:  filteredDocsOnUserDefinedFilters,
			FilteredConflictDocs:               conflictDocsFiltered,
			SrcConflictDocsWritten:             srcConflictDocsWritten,
			TgtConflictDocsWritten:             tgtConflictDocsWritten,
			CRDConflictDocsWritten:             crdConflictDocsWritten,
		},
		brokenMappings: brokenMappings,
		CreationTime:   creationTime,
		TargetPerVBCounters: TargetPerVBCounters{
			GuardrailResidentRatioCnt:          guardrailResidentRatioItems,
			GuardrailDataSizeCnt:               guardrailDataSizeItems,
			GuardrailDiskSpaceCnt:              guardrailDiskSpaceItems,
			DocsSentWithSubdocSetCnt:           subdocSetItems,
			DocsSentWithSubdocDeleteCnt:        subdocDeleteItems,
			CasPoisonCnt:                       casPoisonItems,
			DocsSentWithPoisonedCasErrorMode:   docsSentWithPoisonedCasErrorMode,
			DocsSentWithPoisonedCasReplaceMode: docsSentWithPoisonedCasReplaceMode,
			GetDocsCasChangedCnt:               getDocsCasChangedCnt,
			CLogHibernatedCnt:                  clogHibernatedCnt,
			TrueConflictsDetected:              trueConflictsDetected,
		},
	}
	return record
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

func (ckptRecord *CheckpointRecord) SameAs(newRecord *CheckpointRecord) bool {
	if ckptRecord == nil && newRecord != nil {
		return false
	} else if ckptRecord != nil && newRecord == nil {
		return false
	} else if ckptRecord == nil && newRecord == nil {
		return true
	} else if ckptRecord.Failover_uuid == newRecord.Failover_uuid &&
		ckptRecord.Seqno == newRecord.Seqno &&
		ckptRecord.Dcp_snapshot_seqno == newRecord.Dcp_snapshot_seqno &&
		ckptRecord.Dcp_snapshot_end_seqno == newRecord.Dcp_snapshot_end_seqno &&
		ckptRecord.Target_vb_opaque.IsSame(newRecord.Target_vb_opaque) &&
		ckptRecord.Target_Seqno == newRecord.Target_Seqno &&
		ckptRecord.Filtered_Failed_Cnt == newRecord.Filtered_Failed_Cnt &&
		ckptRecord.Filtered_Items_Cnt == newRecord.Filtered_Items_Cnt &&
		ckptRecord.FilteredItemsOnExpirationsCnt == newRecord.FilteredItemsOnExpirationsCnt &&
		ckptRecord.FilteredItemsOnDeletionsCnt == newRecord.FilteredItemsOnDeletionsCnt &&
		ckptRecord.FilteredItemsOnSetCnt == newRecord.FilteredItemsOnSetCnt &&
		ckptRecord.FilteredItemsOnExpiryStrippedCnt == newRecord.FilteredItemsOnExpiryStrippedCnt &&
		ckptRecord.FilteredItemsOnBinaryDocsCnt == newRecord.FilteredItemsOnBinaryDocsCnt &&
		ckptRecord.FilteredItemsOnATRDocsCnt == newRecord.FilteredItemsOnATRDocsCnt &&
		ckptRecord.FilteredItemsOnClientTxnRecordsCnt == newRecord.FilteredItemsOnClientTxnRecordsCnt &&
		ckptRecord.FilteredItemsOnTxnXattrsDocsCnt == newRecord.FilteredItemsOnTxnXattrsDocsCnt &&
		ckptRecord.FilteredItemsOnMobileRecords == newRecord.FilteredItemsOnMobileRecords &&
		ckptRecord.FilteredItemsOnUserDefinedFilters == newRecord.FilteredItemsOnUserDefinedFilters &&
		ckptRecord.SourceManifestForDCP == newRecord.SourceManifestForDCP &&
		ckptRecord.SourceManifestForBackfillMgr == newRecord.SourceManifestForBackfillMgr &&
		ckptRecord.TargetManifest == newRecord.TargetManifest &&
		ckptRecord.BrokenMappingSha256 == newRecord.BrokenMappingSha256 &&
		ckptRecord.CreationTime == newRecord.CreationTime &&
		ckptRecord.GuardrailDiskSpaceCnt == newRecord.GuardrailDiskSpaceCnt &&
		ckptRecord.GuardrailDataSizeCnt == newRecord.GuardrailDataSizeCnt &&
		ckptRecord.GuardrailResidentRatioCnt == newRecord.GuardrailResidentRatioCnt &&
		ckptRecord.DocsSentWithSubdocDeleteCnt == newRecord.DocsSentWithSubdocDeleteCnt &&
		ckptRecord.DocsSentWithSubdocSetCnt == newRecord.DocsSentWithSubdocSetCnt &&
		ckptRecord.CasPoisonCnt == newRecord.CasPoisonCnt &&
		ckptRecord.DocsSentWithPoisonedCasErrorMode == newRecord.DocsSentWithPoisonedCasErrorMode &&
		ckptRecord.DocsSentWithPoisonedCasReplaceMode == newRecord.DocsSentWithPoisonedCasReplaceMode &&
		ckptRecord.SrcConflictDocsWritten == newRecord.SrcConflictDocsWritten &&
		ckptRecord.TgtConflictDocsWritten == newRecord.TgtConflictDocsWritten &&
		ckptRecord.CRDConflictDocsWritten == newRecord.CRDConflictDocsWritten &&
		ckptRecord.TrueConflictsDetected == newRecord.TrueConflictsDetected &&
		ckptRecord.FilteredConflictDocs == newRecord.FilteredConflictDocs &&
		ckptRecord.CLogHibernatedCnt == newRecord.CLogHibernatedCnt &&
		ckptRecord.GetDocsCasChangedCnt == newRecord.GetDocsCasChangedCnt {
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
	ckptRecord.FilteredItemsOnExpirationsCnt = other.FilteredItemsOnExpirationsCnt
	ckptRecord.FilteredItemsOnDeletionsCnt = other.FilteredItemsOnDeletionsCnt
	ckptRecord.FilteredItemsOnSetCnt = other.FilteredItemsOnSetCnt
	ckptRecord.FilteredItemsOnExpiryStrippedCnt = other.FilteredItemsOnExpiryStrippedCnt
	ckptRecord.FilteredItemsOnBinaryDocsCnt = other.FilteredItemsOnBinaryDocsCnt
	ckptRecord.FilteredItemsOnATRDocsCnt = other.FilteredItemsOnATRDocsCnt
	ckptRecord.FilteredItemsOnClientTxnRecordsCnt = other.FilteredItemsOnClientTxnRecordsCnt
	ckptRecord.FilteredItemsOnTxnXattrsDocsCnt = other.FilteredItemsOnTxnXattrsDocsCnt
	ckptRecord.FilteredItemsOnMobileRecords = other.FilteredItemsOnMobileRecords
	ckptRecord.FilteredItemsOnUserDefinedFilters = other.FilteredItemsOnUserDefinedFilters
	ckptRecord.SourceManifestForDCP = other.SourceManifestForDCP
	ckptRecord.SourceManifestForBackfillMgr = other.SourceManifestForBackfillMgr
	ckptRecord.TargetManifest = other.TargetManifest
	ckptRecord.LoadBrokenMapping(*other.BrokenMappings())
	ckptRecord.CreationTime = other.CreationTime
	ckptRecord.GuardrailResidentRatioCnt = other.GuardrailResidentRatioCnt
	ckptRecord.GuardrailDataSizeCnt = other.GuardrailDataSizeCnt
	ckptRecord.GuardrailDiskSpaceCnt = other.GuardrailDiskSpaceCnt
	ckptRecord.DocsSentWithSubdocDeleteCnt = other.DocsSentWithSubdocDeleteCnt
	ckptRecord.DocsSentWithSubdocSetCnt = other.DocsSentWithSubdocSetCnt
	ckptRecord.CasPoisonCnt = other.CasPoisonCnt
	ckptRecord.DocsSentWithPoisonedCasErrorMode = other.DocsSentWithPoisonedCasErrorMode
	ckptRecord.DocsSentWithPoisonedCasReplaceMode = other.DocsSentWithPoisonedCasReplaceMode
	ckptRecord.SrcConflictDocsWritten = other.SrcConflictDocsWritten
	ckptRecord.TgtConflictDocsWritten = other.TgtConflictDocsWritten
	ckptRecord.CRDConflictDocsWritten = other.CRDConflictDocsWritten
	ckptRecord.TrueConflictsDetected = other.TrueConflictsDetected
	ckptRecord.FilteredConflictDocs = other.FilteredConflictDocs
	ckptRecord.CLogHibernatedCnt = other.CLogHibernatedCnt
	ckptRecord.GetDocsCasChangedCnt = other.GetDocsCasChangedCnt
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

	filteredItemsOnExpirationsCnt, ok := fieldMap[FilteredItemsOnExpirationsCnt]
	if ok {
		ckptRecord.FilteredItemsOnExpirationsCnt = uint64(filteredItemsOnExpirationsCnt.(float64))
	}

	filteredItemsOnDeletionsCnt, ok := fieldMap[FilteredItemsOnDeletionsCnt]
	if ok {
		ckptRecord.FilteredItemsOnDeletionsCnt = uint64(filteredItemsOnDeletionsCnt.(float64))
	}

	filteredItemsOnSetCnt, ok := fieldMap[FilteredItemsOnSetCnt]
	if ok {
		ckptRecord.FilteredItemsOnSetCnt = uint64(filteredItemsOnSetCnt.(float64))
	}

	filteredItemsOnExpiryStripCnt, ok := fieldMap[FilteredItemsOnExpiryStrippedCnt]
	if ok {
		ckptRecord.FilteredItemsOnExpiryStrippedCnt = uint64(filteredItemsOnExpiryStripCnt.(float64))
	}

	filteredItemsOnBinaryDocsCnt, ok := fieldMap[FilteredItemsOnBinaryDocsCnt]
	if ok {
		ckptRecord.FilteredItemsOnBinaryDocsCnt = uint64(filteredItemsOnBinaryDocsCnt.(float64))
	}

	filteredItemsOnATRDocsCnt, ok := fieldMap[FilteredItemsOnATRDocsCnt]
	if ok {
		ckptRecord.FilteredItemsOnATRDocsCnt = uint64(filteredItemsOnATRDocsCnt.(float64))
	}

	filteredItemsOnClientTxnRecordsCnt, ok := fieldMap[FilteredItemsOnClientTxnRecordsCnt]
	if ok {
		ckptRecord.FilteredItemsOnClientTxnRecordsCnt = uint64(filteredItemsOnClientTxnRecordsCnt.(float64))
	}

	filteredItemsOnTxnXattrsDocsCnt, ok := fieldMap[FilteredItemsOnTxnXattrsDocsCnt]
	if ok {
		ckptRecord.FilteredItemsOnTxnXattrsDocsCnt = uint64(filteredItemsOnTxnXattrsDocsCnt.(float64))
	}

	filteredItemsOnMobileRecords, ok := fieldMap[FilteredItemsOnMobileRecords]
	if ok {
		ckptRecord.FilteredItemsOnMobileRecords = uint64(filteredItemsOnMobileRecords.(float64))
	}

	filteredDocsOnUserDefinedFilters, ok := fieldMap[FilteredItemsOnUserDefinedFilters]
	if ok {
		ckptRecord.FilteredItemsOnUserDefinedFilters = uint64(filteredDocsOnUserDefinedFilters.(float64))
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

	creationTime, ok := fieldMap[CreationTime]
	if ok {
		ckptRecord.CreationTime = uint64(creationTime.(float64))
	}

	guardrailResidentRatio, ok := fieldMap[GuardrailResidentRatioCnt]
	if ok {
		ckptRecord.GuardrailResidentRatioCnt = uint64(guardrailResidentRatio.(float64))
	}

	guardrailDataSize, ok := fieldMap[GuardrailDataSizeCnt]
	if ok {
		ckptRecord.GuardrailDataSizeCnt = uint64(guardrailDataSize.(float64))
	}

	guardrailDiskSpace, ok := fieldMap[GuardrailDiskSpaceCnt]
	if ok {
		ckptRecord.GuardrailDiskSpaceCnt = uint64(guardrailDiskSpace.(float64))
	}

	docsSentWithSubdocSet, ok := fieldMap[DocsSentWithSubdocSetCnt]
	if ok {
		ckptRecord.DocsSentWithSubdocSetCnt = uint64(docsSentWithSubdocSet.(float64))
	}

	docsSentWithSubdocDelete, ok := fieldMap[DocsSentWithSubdocDeleteCnt]
	if ok {
		ckptRecord.DocsSentWithSubdocDeleteCnt = uint64(docsSentWithSubdocDelete.(float64))
	}

	casPoisonCnt, ok := fieldMap[CasPoisonCnt]
	if ok {
		ckptRecord.CasPoisonCnt = uint64(casPoisonCnt.(float64))
	}

	docsSentWithPoisonedCasError, ok := fieldMap[DocsSentWithPoisonedCasErrorMode]
	if ok {
		ckptRecord.DocsSentWithPoisonedCasErrorMode = uint64(docsSentWithPoisonedCasError.(float64))
	}

	docsSentWithPoisonedCasReplace, ok := fieldMap[DocsSentWithPoisonedCasReplaceMode]
	if ok {
		ckptRecord.DocsSentWithPoisonedCasReplaceMode = uint64(docsSentWithPoisonedCasReplace.(float64))
	}

	srcConflictDocsWritten, ok := fieldMap[SrcConflictDocsWritten]
	if ok {
		ckptRecord.SrcConflictDocsWritten = uint64(srcConflictDocsWritten.(float64))
	}

	tgtConflictDocsWritten, ok := fieldMap[TgtConflictDocsWritten]
	if ok {
		ckptRecord.TgtConflictDocsWritten = uint64(tgtConflictDocsWritten.(float64))
	}

	crdConflictDocsWritten, ok := fieldMap[CRDConflictDocsWritten]
	if ok {
		ckptRecord.CRDConflictDocsWritten = uint64(crdConflictDocsWritten.(float64))
	}

	trueConflictsDetected, ok := fieldMap[TrueConflictsDetected]
	if ok {
		ckptRecord.TrueConflictsDetected = uint64(trueConflictsDetected.(float64))
	}

	conflictDocsFiltered, ok := fieldMap[FilteredConflictDocs]
	if ok {
		ckptRecord.FilteredConflictDocs = uint64(conflictDocsFiltered.(float64))
	}

	cLogHibernatedCnt, ok := fieldMap[CLogHibernatedCnt]
	if ok {
		ckptRecord.CLogHibernatedCnt = uint64(cLogHibernatedCnt.(float64))
	}

	getDocsCasChangedCnt, ok := fieldMap[GetDocsCasChangedCnt]
	if ok {
		ckptRecord.GetDocsCasChangedCnt = uint64(getDocsCasChangedCnt.(float64))
	}

	return nil
}

type TargetVBOpaque interface {
	Value() interface{}
	IsSame(targetVBOpaque TargetVBOpaque) bool
	Size() int
}

// clusters have a single int vbuuid
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

// Returns:
// 1. True if aRecord is "more recent in time" than bRecord", false otherwise
// 2. True if a valid operation, false otherwise
func compareFailoverLogPositionThenSeqnos(aRecord *CheckpointSortRecord, bRecord *CheckpointSortRecord, source bool) (bool, bool) {
	var aFailoverLog *mcc.FailoverLog
	var aFailoverUuid uint64
	var aSeqno uint64

	var bFailoverLog *mcc.FailoverLog
	var bFailoverUuid uint64
	var bSeqno uint64

	if aRecord.CheckpointRecord == nil && bRecord.CheckpointRecord != nil {
		// bRecord is "less than", or should belong in front of aRecord
		return false, true
	} else if aRecord.CheckpointRecord != nil && bRecord.CheckpointRecord == nil {
		return true, false
	} else if aRecord.CheckpointRecord == nil && bRecord.CheckpointRecord == nil {
		// Just say yes
		return true, false
	}

	if source {
		aFailoverLog = aRecord.srcFailoverLog
		bFailoverLog = bRecord.srcFailoverLog
		aFailoverUuid = aRecord.Failover_uuid
		bFailoverUuid = bRecord.Failover_uuid
		aSeqno = aRecord.Seqno
		bSeqno = bRecord.Seqno
	} else {
		aFailoverLog = aRecord.tgtFailoverLog
		bFailoverLog = bRecord.tgtFailoverLog
		aFailoverUuid = aRecord.Target_vb_opaque.Value().(uint64)
		bFailoverUuid = bRecord.Target_vb_opaque.Value().(uint64)
		aSeqno = aRecord.Target_Seqno
		bSeqno = bRecord.Target_Seqno
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
			if aSeqno != bSeqno {
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
		if checkpointRecord == nil {
			continue
		}
		sortRecordsList = append(sortRecordsList, &CheckpointSortRecord{
			CheckpointRecord: checkpointRecord,
			srcFailoverLog:   srcFailoverlog,
			tgtFailoverLog:   tgtFailoverlog,
		})
	}
	return sortRecordsList
}

func (c *CheckpointRecordsList) SameAs(other CheckpointRecordsList) bool {
	if c == nil {
		return false
	}
	if len(*c) != len(other) {
		return false
	}

	for i, record := range *c {
		if !record.SameAs(other[i]) {
			return false
		}
	}
	return true
}

func (c *CheckpointRecordsList) Len() int {
	if c == nil {
		return 0
	}

	var count int
	for _, record := range *c {
		if record == nil {
			continue
		}
		count++
	}
	return count
}

func (c *CheckpointRecordsList) Clone() CheckpointRecordsList {
	if c == nil {
		return nil
	}

	var retList CheckpointRecordsList
	for _, ckptRecord := range *c {
		retList = append(retList, ckptRecord.Clone())
	}
	return retList
}

type VBsCkptsDocMap map[uint16]*CheckpointsDoc

func (v *VBsCkptsDocMap) InternalIdMatch(internalId string) bool {
	if v == nil || *v == nil {
		return false
	}

	for _, ckptDoc := range *v {
		if ckptDoc == nil {
			continue
		}
		if ckptDoc.SpecInternalId != internalId {
			return false
		}
	}
	return true
}

func (v *VBsCkptsDocMap) Clone() VBsCkptsDocMap {
	if v == nil || *v == nil {
		return nil
	}

	result := make(VBsCkptsDocMap)
	for vbno, ckptDoc := range *v {
		result[vbno] = ckptDoc.Clone()
	}
	return result
}

func (v *VBsCkptsDocMap) SameAs(other VBsCkptsDocMap) bool {
	if v == nil || *v == nil {
		if other == nil {
			return true
		}
		return false
	}

	if len(*v) != len(other) {
		return false
	}

	for vbno, ckptRecord := range *v {
		bRecord, found := other[vbno]
		if !found {
			return false
		}
		if !ckptRecord.SameAs(bRecord) {
			return false
		}
	}
	return true
}

// Outputs:
// 1. Compressed map of ckpts
// 2. A deduped map of brokenMapSha -> snappy compressed broken map referred by all ckpts in the docs
func (v *VBsCkptsDocMap) SnappyCompress() (VBsCkptsDocSnappyMap, ShaMappingCompressedMap, error) {
	if v == nil {
		return nil, nil, base.ErrorNilPtr
	}

	snapCkptMap := make(VBsCkptsDocSnappyMap)
	snapShaMap := make(ShaMappingCompressedMap)
	errorMap := make(base.ErrorMap)

	for vbno, ckptsDoc := range *v {
		if ckptsDoc == nil {
			continue
		}

		compressedCkptsDoc, oneSnapShaMap, compressErr := ckptsDoc.SnappyCompress()
		if compressErr != nil {
			errorMap[fmt.Sprintf("CkptDoc for %v marshalErr", vbno)] = compressErr
			continue
		}

		snapShaMap.Merge(oneSnapShaMap)
		snapCkptMap[vbno] = compressedCkptsDoc
	}

	if len(errorMap) > 0 {
		return nil, nil, fmt.Errorf(base.FlattenErrorMap(errorMap))
	}
	return snapCkptMap, snapShaMap, nil
}

// This merge and replace will merge if the VBs do not intersect
// or replace if the incoming map and the current map has intersect
func (v *VBsCkptsDocMap) MergeAndReplace(incoming VBsCkptsDocMap) {
	if v == nil || *v == nil || incoming == nil {
		return
	}
	for vbno, ckptDocs := range incoming {
		// replace
		(*v)[vbno] = ckptDocs
	}
}

type VBsCkptsDocSnappyMap map[uint16][]byte

func (v *VBsCkptsDocSnappyMap) SnappyDecompress(snappyShaMap ShaMappingCompressedMap) (VBsCkptsDocMap, error) {
	if v == nil || snappyShaMap == nil {
		return nil, base.ErrorNilPtr
	}

	errMap := make(base.ErrorMap)
	regularMap := make(VBsCkptsDocMap)
	for vbno, compresedBytes := range *v {
		if compresedBytes == nil {
			regularMap[vbno] = nil
		} else {
			ckptDoc, err := NewCheckpointsDocFromSnappy(compresedBytes, snappyShaMap)
			if err != nil {
				errMap[fmt.Sprintf("vbno: %v", vbno)] = err
				continue
			}
			regularMap[vbno] = ckptDoc
		}
	}

	if len(errMap) > 0 {
		return nil, fmt.Errorf(base.FlattenErrorMap(errMap))
	}
	return regularMap, nil
}

type ShaMappingCompressedMap map[string][]byte

func (s *ShaMappingCompressedMap) Merge(other ShaMappingCompressedMap) {
	if s == nil {
		return
	}

	for sha, snappyVal := range other {
		if _, exists := (*s)[sha]; !exists {
			(*s)[sha] = snappyVal
		}
	}
}

func (s *ShaMappingCompressedMap) Get(sha string) (*CollectionNamespaceMapping, error) {
	if s == nil {
		return nil, base.ErrorNilPtr
	}

	if _, exists := (*s)[sha]; !exists {
		return nil, base.ErrorNotFound
	}

	retMapping := &CollectionNamespaceMapping{}
	deCompressErr := retMapping.SnappyDecompress((*s)[sha])
	return retMapping, deCompressErr
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

func (c *CheckpointsDoc) Clone() *CheckpointsDoc {
	if c == nil {
		return nil
	}
	retVal := &CheckpointsDoc{
		Checkpoint_records: c.Checkpoint_records.Clone(),
		SpecInternalId:     c.SpecInternalId,
		Revision:           c.Revision,
	}
	return retVal
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

func (c *CheckpointRecord) Clone() *CheckpointRecord {
	if c == nil {
		return nil
	}

	retVal := &CheckpointRecord{
		BrokenMappingSha256: c.BrokenMappingSha256,
		brokenMappings:      c.brokenMappings.Clone(),
		brokenMappingsMtx:   sync.RWMutex{},
		CreationTime:        c.CreationTime,
		SourceVBTimestamp: SourceVBTimestamp{
			Failover_uuid:                c.Failover_uuid,
			Seqno:                        c.Seqno,
			Dcp_snapshot_seqno:           c.Dcp_snapshot_seqno,
			Dcp_snapshot_end_seqno:       c.Dcp_snapshot_end_seqno,
			SourceManifestForDCP:         c.SourceManifestForDCP,
			SourceManifestForBackfillMgr: c.SourceManifestForBackfillMgr,
		},
		TargetVBTimestamp: TargetVBTimestamp{
			Target_vb_opaque: c.Target_vb_opaque,
			Target_Seqno:     c.Target_Seqno,
			TargetManifest:   c.TargetManifest,
		},
		SourceFilteredCounters: SourceFilteredCounters{
			Filtered_Items_Cnt:                 c.Filtered_Items_Cnt,
			Filtered_Failed_Cnt:                c.Filtered_Failed_Cnt,
			FilteredItemsOnExpirationsCnt:      c.FilteredItemsOnExpirationsCnt,
			FilteredItemsOnDeletionsCnt:        c.FilteredItemsOnDeletionsCnt,
			FilteredItemsOnSetCnt:              c.FilteredItemsOnSetCnt,
			FilteredItemsOnExpiryStrippedCnt:   c.FilteredItemsOnExpiryStrippedCnt,
			FilteredItemsOnBinaryDocsCnt:       c.FilteredItemsOnBinaryDocsCnt,
			FilteredItemsOnATRDocsCnt:          c.FilteredItemsOnATRDocsCnt,
			FilteredItemsOnClientTxnRecordsCnt: c.FilteredItemsOnClientTxnRecordsCnt,
			FilteredItemsOnTxnXattrsDocsCnt:    c.FilteredItemsOnTxnXattrsDocsCnt,
			FilteredItemsOnMobileRecords:       c.FilteredItemsOnMobileRecords,
			FilteredItemsOnUserDefinedFilters:  c.FilteredItemsOnUserDefinedFilters,
			FilteredConflictDocs:               c.FilteredConflictDocs,
			SrcConflictDocsWritten:             c.SrcConflictDocsWritten,
			TgtConflictDocsWritten:             c.TgtConflictDocsWritten,
			CRDConflictDocsWritten:             c.CRDConflictDocsWritten,
		},
		TargetPerVBCounters: TargetPerVBCounters{
			GuardrailDiskSpaceCnt:              c.GuardrailDiskSpaceCnt,
			GuardrailDataSizeCnt:               c.GuardrailDataSizeCnt,
			GuardrailResidentRatioCnt:          c.GuardrailResidentRatioCnt,
			DocsSentWithSubdocSetCnt:           c.DocsSentWithSubdocSetCnt,
			DocsSentWithSubdocDeleteCnt:        c.DocsSentWithSubdocDeleteCnt,
			CasPoisonCnt:                       c.CasPoisonCnt,
			DocsSentWithPoisonedCasErrorMode:   c.DocsSentWithPoisonedCasErrorMode,
			DocsSentWithPoisonedCasReplaceMode: c.DocsSentWithPoisonedCasReplaceMode,
			TrueConflictsDetected:              c.TrueConflictsDetected,
			CLogHibernatedCnt:                  c.CLogHibernatedCnt,
			GetDocsCasChangedCnt:               c.GetDocsCasChangedCnt,
		},
	}
	return retVal
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

func NewCheckpointsDocFromSnappy(snappyBytes []byte, compressedMap ShaMappingCompressedMap) (*CheckpointsDoc, error) {
	ckptDoc := &CheckpointsDoc{}
	err := ckptDoc.SnappyDecompress(snappyBytes, compressedMap)

	if err != nil {
		return nil, err
	}

	return ckptDoc, nil
}

// Not concurrency safe. It should be used by one goroutine only
func (ckptsDoc *CheckpointsDoc) AddRecord(record *CheckpointRecord) (added bool, removedRecords []*CheckpointRecord) {
	length := len(ckptsDoc.Checkpoint_records)
	if length > 0 {
		if !ckptsDoc.Checkpoint_records[0].SameAs(record) {
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
	if ckptsDoc == nil {
		return nil
	}
	if len(ckptsDoc.Checkpoint_records) <= base.MaxCheckpointRecordsToRead {
		return ckptsDoc.Checkpoint_records
	} else {
		return ckptsDoc.Checkpoint_records[:base.MaxCheckpointRecordsToRead]
	}
}

func (c *CheckpointsDoc) SameAs(other *CheckpointsDoc) bool {
	if c == nil && other == nil {
		return true
	} else if c != nil && other == nil {
		return false
	} else if c == nil && other != nil {
		return false
	}
	return c.Checkpoint_records.SameAs(other.Checkpoint_records) && c.SpecInternalId == other.SpecInternalId
}

// Note - returns only valid non nil checkpoint records
func (c *CheckpointsDoc) Len() int {
	if c == nil {
		return 0
	}
	return c.Checkpoint_records.Len()
}

func (c *CheckpointsDoc) SnappyCompress() ([]byte, ShaMappingCompressedMap, error) {
	marshalledBytes, err := json.Marshal(c)
	if err != nil {
		return nil, nil, err
	}

	snapShaMap := make(ShaMappingCompressedMap)
	errorMap := make(base.ErrorMap)
	records := c.GetCheckpointRecords()
	for _, record := range records {
		if record == nil {
			continue
		}
		brokenMap := record.BrokenMappings()
		if brokenMap == nil {
			// Record has no broken map
			continue
		}
		brokenMapSha := record.BrokenMappingSha256

		_, dedupMapExists := snapShaMap[brokenMapSha]
		if !dedupMapExists {
			compressedBytes, compressErr := brokenMap.ToSnappyCompressed()
			if compressErr != nil {
				errorMap[fmt.Sprintf("BrokenMap: %v", brokenMap.String())] = fmt.Errorf("Unable to snappyCompress")
			}
			snapShaMap[brokenMapSha] = compressedBytes
		}
	}

	if len(errorMap) > 0 {
		return nil, nil, fmt.Errorf(base.FlattenErrorMap(errorMap))
	}

	return snappy.Encode(nil, marshalledBytes), snapShaMap, nil
}

func (c *CheckpointsDoc) SnappyDecompress(data []byte, shaCompressedMap ShaMappingCompressedMap) error {
	if c == nil || shaCompressedMap == nil || len(shaCompressedMap) == 0 {
		return base.ErrorNilPtr
	}

	uncompressedCkptDocBytes, err := snappy.Decode(nil, data)
	if err != nil {
		return fmt.Errorf("snappy.Decode ckptDoc data err %v", err)
	}

	err = json.Unmarshal(uncompressedCkptDocBytes, c)
	if err != nil {
		return err
	}

	errMap := make(base.ErrorMap)

	records := c.GetCheckpointRecords()
	for _, record := range records {
		if record == nil {
			continue
		}

		brokenMapSha := record.BrokenMappingSha256
		brokenMap, brokenMapLookupErr := shaCompressedMap.Get(brokenMapSha)
		if brokenMapLookupErr != nil {
			errMap[brokenMapSha] = brokenMapLookupErr
			continue
		}
		err = record.LoadBrokenMapping(*brokenMap)
		if err != nil {
			errMap[brokenMapSha] = err
			continue
		}
	}

	if len(errMap) > 0 {
		return fmt.Errorf(base.FlattenErrorMap(errMap))
	}
	return nil
}

func (c *CheckpointsDoc) IsTraditional() bool {
	// TODO global checkpoint
	return true
}
