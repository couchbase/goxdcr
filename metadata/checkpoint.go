// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
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
	GlobalTimestampStr                 string = "global_timestamp"
	GlobalTargetCountersStr            string = "global_target_counters"
	GlobalTimestampMapSha              string = "globalTimestampMapSha256"
	GlobalCountersMapSha               string = "globalCountersMapSha256"
	GlobalInfoTypeStr                  string = "global_info_type"
	GlobalInfoDataStr                  string = "global_info_data"
	SubdocCmdSkippedCnt                string = "subdoc_cmd_skipped_cnt"
	PipelineReinitHashCkptKey          string = "pipelineReinitHash"
	BackfillCollections                string = "bfillColIDs"
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

func (s *SourceVBTimestamp) Size() int {
	if s == nil {
		return 0
	}
	numFields := reflect.TypeOf(*s).NumField()
	return 8 * numFields
}

// indicates the type of GlobalInfo
type GlobalInfoType int

const (
	GlobalTimestampType     GlobalInfoType = iota
	GlobalTargetCounterType GlobalInfoType = iota
)

var (
	ErrorGlobalInfoWrongType   = errors.New("unable to unmarshal to GlobalInfo")
	ErrorInvalidGlobalInfoType = errors.New("invalid GlobalInfoType")
)

type GlobalInfoMetaObj struct {
	Type GlobalInfoType `json:"global_info_type"`
	Data GlobalInfo     `json:"global_info_data"`
}

func newGlobalInfoMetaObj(type_ GlobalInfoType, data GlobalInfo) *GlobalInfoMetaObj {
	return &GlobalInfoMetaObj{
		Type: type_,
		Data: data}
}

// SnappyDecompress decompresses data using Snappy and unmarshals into the receiver.
func (gmo *GlobalInfoMetaObj) SnappyDecompress(data []byte) error {
	if gmo == nil {
		return base.ErrorNilPtr
	}

	uncompressedData, err := snappy.Decode(nil, data)
	if err != nil {
		return fmt.Errorf("failed to decompress data. err=%v", err)
	}

	return gmo.UnmarshalJSON(uncompressedData)
}

// UnmarshalJSON is the custom unmarshaller for `GlobalInfoMetaObj`.
func (gmo *GlobalInfoMetaObj) UnmarshalJSON(data []byte) error {
	var serializedMap map[string]interface{}
	if err := json.Unmarshal(data, &serializedMap); err != nil {
		return fmt.Errorf("failed to unmarshal JSON. err=%v", err)
	}

	typeVal, ok := serializedMap[GlobalInfoTypeStr]
	dataVal, ok1 := serializedMap[GlobalInfoDataStr]
	if !ok || !ok1 { // If either of them is not present, then the underlying data must be of wrong type
		return ErrorGlobalInfoWrongType
	}

	// Validate and populate `Type` field
	typeFloat, ok := typeVal.(float64)
	if !ok {
		return fmt.Errorf("failed to unmarshal. err=On unmarshalling, GlobalInfoType should be of type float64 but is of type %T", typeVal)
	}
	gmo.Type = GlobalInfoType(typeFloat)

	// Validate and populate `Data` field
	genericMap, ok := dataVal.(map[string]interface{})
	if !ok {
		return fmt.Errorf("failed to unmarshal. err=GlobalInfoMetaObj.Data should be of type map[string]interface{} but got %T", dataVal)
	}
	return gmo.populateDataField(genericMap)
}

// populateDataField handles assigning the correct type to `Data`.
func (gmo *GlobalInfoMetaObj) populateDataField(genericMap map[string]interface{}) error {
	switch gmo.Type {
	case GlobalTimestampType:
		return gmo.handleGlobalTimestamp(genericMap)
	case GlobalTargetCounterType:
		return gmo.handleGlobalTargetCounters(genericMap)
	default:
		return ErrorInvalidGlobalInfoType
	}
}

func (gmo *GlobalInfoMetaObj) handleGlobalTimestamp(genericMap map[string]interface{}) error {
	actualMap := make(GlobalTimestamp)
	if err := actualMap.LoadUnmarshalled(genericMap); err != nil {
		return fmt.Errorf("failed to load GlobalTimestamp: err=%v", err)
	}
	gmo.Data = &actualMap
	return nil
}

func (gmo *GlobalInfoMetaObj) handleGlobalTargetCounters(genericMap map[string]interface{}) error {
	actualMap := make(GlobalTargetCounters)
	if err := actualMap.LoadUnmarshalled(genericMap); err != nil {
		return fmt.Errorf("failed to load GlobalTargetCounters: err=%v", err)
	}
	gmo.Data = &actualMap
	return nil
}

type GlobalVBTimestamp struct {
	TargetVBTimestamp
}

func (g *GlobalVBTimestamp) Clone() GlobalVBTimestamp {
	if g == nil {
		return GlobalVBTimestamp{}
	}
	return GlobalVBTimestamp{
		TargetVBTimestamp: g.TargetVBTimestamp.Clone(),
	}
}

func (g *GlobalVBTimestamp) IsTraditional() bool {
	return false
}

func (g *GlobalVBTimestamp) SameAs(other *GlobalVBTimestamp) bool {
	return g.TargetVBTimestamp.SameAs(&other.TargetVBTimestamp)
}

func (g *GlobalVBTimestamp) GetValue() interface{} {
	return g
}

func (g *GlobalVBTimestamp) BrokenMappingsClone() CollectionNamespaceMapping {
	g.brokenMappingsMtx.RLock()
	defer g.brokenMappingsMtx.RUnlock()
	return g.brokenMappings.Clone()
}

// TargetVBTimestamp defines the necessary items to confirm that the checkpoint is valid for resuming
type TargetVBTimestamp struct {
	//target vb opaque / high seqno
	Target_vb_opaque TargetVBOpaque `json:"target_vb_opaque"`
	//target vb high sequence number
	Target_Seqno uint64 `json:"target_seqno"`
	// Manifests uid corresponding to this checkpoint
	TargetManifest uint64 `json:"target_manifest"`
	// BrokenMappingInfoType SHA256 string - Internally used by checkpoints Service to populate the actual BrokenMappingInfoType above
	BrokenMappingSha256 string `json:"brokenCollectionsMapSha256"`
	// Broken mapping (if any) associated with the checkpoint - this is populated automatically by checkpointsService
	brokenMappings    CollectionNamespaceMapping
	brokenMappingsMtx sync.RWMutex // TODO - should be pointer?
}

func (t *TargetVBTimestamp) IsTraditional() bool {
	return true
}

func (t *TargetVBTimestamp) GetValue() interface{} {
	return t
}

func (t *TargetVBTimestamp) SameAs(other *TargetVBTimestamp) bool {
	if t == nil || other == nil {
		return t == nil && other == nil
	}

	targetVBOpaqueSame := t.Target_vb_opaque == nil && other.Target_vb_opaque == nil ||
		t.Target_vb_opaque != nil && other.Target_vb_opaque != nil && t.Target_vb_opaque.IsSame(other.Target_vb_opaque)

	return targetVBOpaqueSame &&
		t.Target_Seqno == other.Target_Seqno &&
		t.TargetManifest == other.TargetManifest &&
		t.BrokenMappingSha256 == other.BrokenMappingSha256
}

// SetBrokenMappingAsPartOfCreation should only be used as part of checkpointing operation
// since only the brokenMappingSHA ultimately will be persisted
func (t *TargetVBTimestamp) SetBrokenMappingAsPartOfCreation(brokenMap CollectionNamespaceMapping) error {
	if t == nil {
		return base.ErrorNilPtr
	}

	t.brokenMappingsMtx.Lock()
	defer t.brokenMappingsMtx.Unlock()
	t.brokenMappings = brokenMap
	return t.populateBrokenMappingShaNoLock()
}

func (t *TargetVBTimestamp) populateBrokenMappingShaNoLock() error {
	sha, err := t.brokenMappings.Sha256()
	if err != nil {
		return err
	}
	t.BrokenMappingSha256 = fmt.Sprintf("%x", sha[:])
	return nil
}

func (t *TargetVBTimestamp) PopulateBrokenMappingSha() error {
	// We are using RLock only because the brokenMappings themselves are not changing
	// It is simply reading the mappings
	t.brokenMappingsMtx.RLock()
	defer t.brokenMappingsMtx.RUnlock()

	if len(t.brokenMappings) > 0 {
		return t.populateBrokenMappingShaNoLock()
	} else {
		t.BrokenMappingSha256 = ""
	}
	return nil
}

func (t *TargetVBTimestamp) LoadUnmarshalled(v interface{}) error {
	if t == nil {
		return base.ErrorNilPtr
	}

	fieldMap, ok := v.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expecting map[string]interface{}, got %T", v)
	}

	target_seqno, ok := fieldMap[TargetSeqno]
	if ok {
		t.Target_Seqno = uint64(target_seqno.(float64))
	}

	// this is the special logic where we unmarshal targetVBOpaque into different concrete types
	target_vb_opaque, ok := fieldMap[TargetVbOpaque]
	if ok {
		target_vb_opaque_obj, err := UnmarshalTargetVBOpaque(target_vb_opaque)
		if err != nil {
			return err
		}
		t.Target_vb_opaque = target_vb_opaque_obj
	}

	tgtManifest, ok := fieldMap[TargetManifest]
	if ok {
		t.TargetManifest = uint64(tgtManifest.(float64))
	}

	brokenMapSha, ok := fieldMap[BrokenCollectionsMapSha]
	if ok {
		t.BrokenMappingSha256 = brokenMapSha.(string)
	}

	return nil
}

func (t *TargetVBTimestamp) Clone() TargetVBTimestamp {
	if t == nil {
		return TargetVBTimestamp{}
	}

	clonedVal := &TargetVBTimestamp{
		//Target_vb_opaque to be cloned later
		Target_Seqno:        t.Target_Seqno,
		TargetManifest:      t.TargetManifest,
		BrokenMappingSha256: t.BrokenMappingSha256,
		brokenMappings:      t.brokenMappings.Clone(),
		brokenMappingsMtx:   sync.RWMutex{},
	}
	if t.Target_vb_opaque != nil {
		clonedVal.Target_vb_opaque = t.Target_vb_opaque.Clone()
	}
	return *clonedVal
}

func (t *TargetVBTimestamp) Size() int {
	if t == nil {
		return 0
	}

	objCounter := reflect.TypeOf(*t)
	numFields := objCounter.NumField()
	if numFields > 4 {
		// Remove:
		// 1. internal brokenMappings
		// 2. brokenMapping mutex
		// 3. brokenmapping sha (counted separately)
		// target vb opaque
		numFields -= 4
	}
	sizeOfOpaque := 0
	if t.Target_vb_opaque != nil {
		sizeOfOpaque += t.Target_vb_opaque.Size()
	}
	return numFields*8 + len(t.BrokenMappingSha256) + sizeOfOpaque
}

type GlobalInfo interface {
	GetClone() GlobalInfo
	ToSnappyCompressable() SnappyCompressableVal
	Sha256() ([sha256.Size]byte, error)
}

type ShaToGlobalInfoMap map[string]GlobalInfo

func (s *ShaToGlobalInfoMap) Load(other ShaToGlobalInfoMap) {
	if s == nil {
		return
	}

	for k, v := range other {
		_, exists := (*s)[k]
		if !exists {
			gts := v.GetClone()
			(*s)[k] = gts
		}
	}
}

func (s *ShaToGlobalInfoMap) Clone() ShaToGlobalInfoMap {
	if s == nil {
		return nil
	}

	clonedMap := make(ShaToGlobalInfoMap)
	for k, v := range *s {
		clonedMap[k] = v.GetClone()
	}
	return clonedMap
}

func (s *ShaToGlobalInfoMap) ToSnappyCompressableMap() map[string]SnappyCompressableVal {
	if s == nil {
		return nil
	}
	retMap := make(map[string]SnappyCompressableVal)
	for k, v := range *s {
		retMap[k] = v.ToSnappyCompressable()
	}
	return retMap
}

// Also needs to inplement VBOpaque
type GlobalTimestamp map[uint16]*GlobalVBTimestamp

func (g *GlobalTimestamp) InitializeVb(vb uint16) {
	if g == nil {
		return
	}
	(*g)[vb] = &GlobalVBTimestamp{}
}

func (g *GlobalTimestamp) SetTargetOpaque(vbno uint16, targetVbOpaque TargetVBOpaque) {
	if g == nil {
		return
	}

	(*g)[vbno].Target_vb_opaque = targetVbOpaque
}

func (g *GlobalTimestamp) Sha256() (result [sha256.Size]byte, err error) {
	if g == nil {
		err = fmt.Errorf("Calling Sha256() on a nil GlobalTimestamp")
		return
	}

	marshalledBytes, err := json.Marshal(g)
	if err != nil {
		return
	}

	result = sha256.Sum256(marshalledBytes)
	return
}

func (g *GlobalTimestamp) ToSnappyCompressed() ([]byte, error) {
	marshalledJson, err := g.CustomJsonMarshaller()
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, marshalledJson), nil
}

// This function is only used for unit tests
func (g *GlobalTimestamp) SnappyDecompress(data []byte) error {
	gInfoMetaObj := &GlobalInfoMetaObj{}
	err := gInfoMetaObj.SnappyDecompress(data)
	if err != nil {
		return err
	}
	gts, ok := gInfoMetaObj.Data.(*GlobalTimestamp)
	if !ok {
		return fmt.Errorf("failed to snappyDecompress. err=GlobalInfoMetaObj.Data should be of type *GlobalTimestamp but got %T", gInfoMetaObj.Data)
	}
	*g = *gts
	return nil
}

func (g *GlobalTimestamp) CloneGlobalTimestamp() GlobalTimestamp {
	if g == nil {
		return nil
	}

	clonedMap := make(GlobalTimestamp)
	for k, v := range *g {
		clonedVal := v.Clone()
		clonedMap[k] = &clonedVal
	}
	return clonedMap
}

func (g *GlobalTimestamp) GetClone() GlobalInfo {
	globalTimestampClone := g.CloneGlobalTimestamp()
	return &globalTimestampClone
}
func (g *GlobalTimestamp) ToSnappyCompressable() SnappyCompressableVal {
	return g
}

func (g *GlobalTimestamp) IsSame(targetVBOpaque TargetVBOpaque) bool {
	otherGlobalTsMap, ok := targetVBOpaque.Value().(GlobalTimestamp)
	if !ok {
		return false
	}
	otherGlobalTs := GlobalTimestamp(otherGlobalTsMap)
	return g.SameAs(&otherGlobalTs)
}

func (g *GlobalTimestamp) Size() int {
	var numOfUint16s int
	var sizeOfAllTargetVBTimestamp int
	for _, oneGlobalVbTimestamp := range *g {
		sizeOfAllTargetVBTimestamp += oneGlobalVbTimestamp.Size()
		numOfUint16s++
	}
	return 2*numOfUint16s + sizeOfAllTargetVBTimestamp
}

func (g *GlobalTimestamp) Value() interface{} {
	if g == nil {
		return nil
	}
	return g.GetValue()
}

func (g *GlobalTimestamp) IsTraditional() bool {
	return false
}

// GlobalTimestamp can implement as TargetVBOpaque
// This is just a "cast" for readability
func (g *GlobalTimestamp) GetTargetOpaque() TargetVBOpaque {
	return g
}

// Value returns a map of all the target VBs and the VBUUIDs assocated with each
func (g *GlobalTimestamp) GetValue() interface{} {
	if g == nil {
		return nil
	}

	tgtVBTimestampMap := make(GlobalTimestamp)
	for k, v := range *g {
		if v == nil {
			v = &GlobalVBTimestamp{}
		}

		gVBTimestamp := &GlobalVBTimestamp{
			TargetVBTimestamp: TargetVBTimestamp{
				Target_Seqno:        v.Target_Seqno,
				TargetManifest:      v.TargetManifest,
				BrokenMappingSha256: v.BrokenMappingSha256,
				brokenMappings:      v.brokenMappings.Clone(),
			},
		}
		if v.Target_vb_opaque != nil {
			gVBTimestamp.Target_vb_opaque = v.Target_vb_opaque.Clone()
		}

		tgtVBTimestampMap[k] = gVBTimestamp
	}

	return tgtVBTimestampMap
}

func (g *GlobalTimestamp) Clone() TargetVBOpaque {
	if g == nil {
		return g
	}

	clonedVal := g.CloneGlobalTimestamp()
	return TargetVBOpaque(&clonedVal)
}

func (g *GlobalTimestamp) SameAs(other *GlobalTimestamp) bool {
	if g == nil || other == nil {
		return g == nil && other == nil
	}

	if len(*g) != len(*other) {
		return false
	}

	for k, v := range *g {
		otherV, found := (*other)[k]
		if !found {
			return false
		}
		if !v.SameAs(otherV) {
			return false
		}
	}
	return true
}

func (g *GlobalTimestamp) LoadUnmarshalled(generic map[string]interface{}) error {
	if g == nil {
		return base.ErrorNilPtr
	}

	for strKey, v := range generic {
		vbInt, err := strconv.Atoi(strKey)
		if err != nil {
			return err
		}
		vb := uint16(vbInt)

		newTargetTs := GlobalVBTimestamp{}
		err = newTargetTs.LoadUnmarshalled(v)
		if err != nil {
			return err
		}
		(*g)[vb] = &newTargetTs
	}
	return nil
}

func (g *GlobalTimestamp) CompressToShaCompresedMap(preExistMap ShaMappingCompressedMap) error {
	if g == nil {
		return base.ErrorNilPtr
	}

	if len(*g) == 0 {
		// Do nothing
		return nil
	}

	shaSlice, err := g.Sha256()
	if err != nil {
		return err
	}
	sha := fmt.Sprintf("%x", shaSlice[:])

	if _, exists := preExistMap[sha]; exists {
		// Already done
		return nil
	}
	compressedBytes, compressErr := g.ToSnappyCompressed()
	if compressErr != nil {
		return fmt.Errorf("Unable to snappyCompress %v", *g)
	}
	preExistMap[sha] = compressedBytes
	return nil
}

// A custom Json marshaller for GlobalTimestamp
func (g *GlobalTimestamp) CustomJsonMarshaller() ([]byte, error) {
	newGlobalInfoMetaObj := newGlobalInfoMetaObj(GlobalTimestampType, g)
	return json.Marshal(newGlobalInfoMetaObj)
}

// SourceVBCounters are item counts based on source VB streams and are restored to the source-related
// components when resuming from a checkpoint
type SourceVBCounters struct {
	// Number of items filtered
	FilteredItemsCnt uint64 `json:"filtered_items_cnt"`
	// Number of items failed filter
	FilteredFailedCnt uint64 `json:"filtered_failed_cnt"`
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
	CLogHibernatedCnt      uint64 `json:"clog_hibernated_cnt"`
	// Number of items that did not send due to source document CAS clock out of bounds
	CasPoisonCnt uint64 `json:"cas_poison_cnt"`
}

func (s *SourceVBCounters) Size() int {
	if s == nil {
		return 0
	}
	return 8 * reflect.TypeOf(*s).NumField()
}

func (s *SourceVBCounters) SameAs(other *SourceVBCounters) bool {
	if s == nil || other == nil {
		return s == nil && other == nil
	}

	return s.FilteredItemsCnt == other.FilteredItemsCnt &&
		s.FilteredFailedCnt == other.FilteredFailedCnt &&
		s.FilteredItemsOnExpirationsCnt == other.FilteredItemsOnExpirationsCnt &&
		s.FilteredItemsOnDeletionsCnt == other.FilteredItemsOnDeletionsCnt &&
		s.FilteredItemsOnSetCnt == other.FilteredItemsOnSetCnt &&
		s.FilteredItemsOnExpiryStrippedCnt == other.FilteredItemsOnExpiryStrippedCnt &&
		s.FilteredItemsOnBinaryDocsCnt == other.FilteredItemsOnBinaryDocsCnt &&
		s.FilteredItemsOnATRDocsCnt == other.FilteredItemsOnATRDocsCnt &&
		s.FilteredItemsOnClientTxnRecordsCnt == other.FilteredItemsOnClientTxnRecordsCnt &&
		s.FilteredItemsOnTxnXattrsDocsCnt == other.FilteredItemsOnTxnXattrsDocsCnt &&
		s.FilteredItemsOnMobileRecords == other.FilteredItemsOnMobileRecords &&
		s.FilteredItemsOnUserDefinedFilters == other.FilteredItemsOnUserDefinedFilters &&
		s.FilteredConflictDocs == other.FilteredConflictDocs &&
		s.SrcConflictDocsWritten == other.SrcConflictDocsWritten &&
		s.TgtConflictDocsWritten == other.TgtConflictDocsWritten &&
		s.CRDConflictDocsWritten == other.CRDConflictDocsWritten &&
		s.CasPoisonCnt == other.CasPoisonCnt &&
		s.CLogHibernatedCnt == other.CLogHibernatedCnt
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
	// Number of subdoc commands skipped due to maximum paths limit.
	SubdocCmdSkippedCnt uint64 `json:"subdoc_cmd_skipped_cnt"`
	// Docs sent with a poisoned CAS value
	DocsSentWithPoisonedCasErrorMode   uint64 `json:"docs_sent_with_poisoned_cas_error_mode"`
	DocsSentWithPoisonedCasReplaceMode uint64 `json:"docs_sent_with_poisoned_cas_replace_mode"`
	// conflict logger stats
	TrueConflictsDetected uint64 `json:"true_conflicts_detected"`
	GetDocsCasChangedCnt  uint64 `json:"get_docs_cas_changed_cnt"`
}

func (t *TargetPerVBCounters) Clone() TargetPerVBCounters {
	newClonedVal := &TargetPerVBCounters{}
	if t == nil {
		return *newClonedVal
	}

	*newClonedVal = *t
	return *newClonedVal
}

func (t *TargetPerVBCounters) Size() int {
	if t == nil {
		return 0
	}
	objCountType := reflect.TypeOf(*t)
	numOfUint64s := objCountType.NumField()
	return numOfUint64s * 8
}

func (t *TargetPerVBCounters) SameAs(other *TargetPerVBCounters) bool {
	if t == nil || other == nil {
		return t == nil && other == nil
	}

	return t.GuardrailDiskSpaceCnt == other.GuardrailDiskSpaceCnt &&
		t.GuardrailDataSizeCnt == other.GuardrailDataSizeCnt &&
		t.GuardrailResidentRatioCnt == other.GuardrailResidentRatioCnt &&
		t.DocsSentWithSubdocDeleteCnt == other.DocsSentWithSubdocDeleteCnt &&
		t.DocsSentWithSubdocSetCnt == other.DocsSentWithSubdocSetCnt &&
		t.DocsSentWithPoisonedCasErrorMode == other.DocsSentWithPoisonedCasErrorMode &&
		t.DocsSentWithPoisonedCasReplaceMode == other.DocsSentWithPoisonedCasReplaceMode &&
		t.TrueConflictsDetected == other.TrueConflictsDetected &&
		t.GetDocsCasChangedCnt == other.GetDocsCasChangedCnt &&
		t.SubdocCmdSkippedCnt == other.SubdocCmdSkippedCnt
}

func (t *TargetPerVBCounters) LoadUnmarshalled(v interface{}) error {
	if t == nil {
		return base.ErrorNilPtr
	}

	fieldMap, ok := v.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expecting map[string]interface{}, got %T", v)
	}

	guardrailResidentRatio, ok := fieldMap[GuardrailResidentRatioCnt]
	if ok {
		t.GuardrailResidentRatioCnt = uint64(guardrailResidentRatio.(float64))
	}

	guardrailDataSize, ok := fieldMap[GuardrailDataSizeCnt]
	if ok {
		t.GuardrailDataSizeCnt = uint64(guardrailDataSize.(float64))
	}

	guardrailDiskSpace, ok := fieldMap[GuardrailDiskSpaceCnt]
	if ok {
		t.GuardrailDiskSpaceCnt = uint64(guardrailDiskSpace.(float64))
	}

	docsSentWithSubdocSet, ok := fieldMap[DocsSentWithSubdocSetCnt]
	if ok {
		t.DocsSentWithSubdocSetCnt = uint64(docsSentWithSubdocSet.(float64))
	}

	docsSentWithSubdocDelete, ok := fieldMap[DocsSentWithSubdocDeleteCnt]
	if ok {
		t.DocsSentWithSubdocDeleteCnt = uint64(docsSentWithSubdocDelete.(float64))
	}

	docsSentWithPoisonedCasError, ok := fieldMap[DocsSentWithPoisonedCasErrorMode]
	if ok {
		t.DocsSentWithPoisonedCasErrorMode = uint64(docsSentWithPoisonedCasError.(float64))
	}
	docsSentWithPoisonedCasReplace, ok := fieldMap[DocsSentWithPoisonedCasReplaceMode]
	if ok {
		t.DocsSentWithPoisonedCasReplaceMode = uint64(docsSentWithPoisonedCasReplace.(float64))
	}
	trueConflictsDetected, ok := fieldMap[TrueConflictsDetected]
	if ok {
		t.TrueConflictsDetected = uint64(trueConflictsDetected.(float64))
	}
	getDocsCasChangedCnt, ok := fieldMap[GetDocsCasChangedCnt]
	if ok {
		t.GetDocsCasChangedCnt = uint64(getDocsCasChangedCnt.(float64))
	}
	subdocCmdSkippedCnt, ok := fieldMap[SubdocCmdSkippedCnt]
	if ok {
		t.SubdocCmdSkippedCnt = uint64(subdocCmdSkippedCnt.(float64))
	}

	return nil
}

type GlobalTargetCounters map[uint16]*TargetPerVBCounters

func (g *GlobalTargetCounters) Size() int {
	var totalSize int
	var numOfVBs int
	for _, perVbCounter := range *g {
		numOfVBs++
		totalSize += perVbCounter.Size()
	}

	return totalSize + numOfVBs*2

}

func (g *GlobalTargetCounters) SameAs(other *GlobalTargetCounters) bool {
	if g == nil || other == nil {
		return g == nil && other == nil
	}

	if len(*g) != len(*other) {
		return false
	}

	for k, v := range *g {
		otherV, found := (*other)[k]
		if !found {
			return false
		}
		if !v.SameAs(otherV) {
			return false
		}
	}
	return true
}

func (g *GlobalTargetCounters) LoadUnmarshalled(generic map[string]interface{}) error {
	if g == nil {
		return base.ErrorNilPtr
	}
	for strKey, v := range generic {
		vbInt, err := strconv.Atoi(strKey)
		if err != nil {
			return err
		}
		vb := uint16(vbInt)

		newTargetCounters := TargetPerVBCounters{}
		err = newTargetCounters.LoadUnmarshalled(v)
		if err != nil {
			return err
		}
		(*g)[vb] = &newTargetCounters
	}
	return nil
}

func (g *GlobalTargetCounters) Clone() GlobalTargetCounters {
	if g == nil {
		return GlobalTargetCounters{}
	}

	clonedMap := make(GlobalTargetCounters)
	for k, v := range *g {
		clonedV := v.Clone()
		clonedMap[k] = &clonedV
	}

	return clonedMap
}

func (g *GlobalTargetCounters) GetClone() GlobalInfo {
	globalCounterClone := g.Clone()
	return &globalCounterClone
}

func (g *GlobalTargetCounters) ToSnappyCompressable() SnappyCompressableVal {
	return g
}

func (g *GlobalTargetCounters) Sha256() (result [sha256.Size]byte, err error) {
	if g == nil {
		err = fmt.Errorf("Calling Sha256() on a nil GlobalTargetCounter")
		return
	}

	marshalledBytes, err := json.Marshal(g)
	if err != nil {
		return
	}

	result = sha256.Sum256(marshalledBytes)
	return
}

func (g *GlobalTargetCounters) ToSnappyCompressed() ([]byte, error) {
	marshalledJson, err := g.CustomJsonMarshaller()
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, marshalledJson), nil
}

func (g *GlobalTargetCounters) CustomJsonMarshaller() ([]byte, error) {
	newGlobalInfoMetaObj := newGlobalInfoMetaObj(GlobalTargetCounterType, g)
	return json.Marshal(newGlobalInfoMetaObj)
}

func (g *GlobalTargetCounters) CompressToShaCompresedMap(preExistMap ShaMappingCompressedMap) error {
	if g == nil {
		return base.ErrorNilPtr
	}

	if len(*g) == 0 {
		// Do nothing
		return nil
	}

	shaSlice, err := g.Sha256()
	if err != nil {
		return err
	}
	sha := fmt.Sprintf("%x", shaSlice[:])

	if _, exists := preExistMap[sha]; exists {
		// Already done
		return nil
	}
	compressedBytes, compressErr := g.ToSnappyCompressed()
	if compressErr != nil {
		return fmt.Errorf("unable to snappyCompress %v", *g)
	}
	preExistMap[sha] = compressedBytes
	return nil
}

// This function is used only by unit tests
func (g *GlobalTargetCounters) SnappyDecompress(data []byte) error {
	gInfoMetaObj := &GlobalInfoMetaObj{}
	err := gInfoMetaObj.SnappyDecompress(data)
	if err != nil {
		return err
	}
	globalCounters, ok := gInfoMetaObj.Data.(*GlobalTargetCounters)
	if !ok {
		return fmt.Errorf("failed to snappyDecompress. err=GlobalInfoMetaObj.Data should be of type *GlobalTargetCounters but got %T", gInfoMetaObj.Data)
	}
	*g = *globalCounters
	return nil
}

func (g *GlobalTargetCounters) InitializeVb(vb uint16) {
	if g == nil {
		return
	}
	(*g)[vb] = &TargetPerVBCounters{}
}

// TargetTimestamp is an interface used within checkpointing operation that can be either
// traditional or variable VB (global) timestamp
type TargetTimestamp interface {
	IsTraditional() bool
	GetValue() interface{}
}

type CheckpointRecord struct {
	// Epoch timestamp of when this record was created
	CreationTime uint64 `json:"creationTime"`

	// Stamped by the ckpt manager. Used to filter out stale ckpts (i.e. those created prior
	// to the current pipeline initialisation).
	PipelineReinitHash string `json:"pipelineReinitHash"`

	SourceVBTimestamp
	SourceVBCounters

	// Traditional checkpoints will utilize one single TargetVBTimestamp and one single
	// TargetPerVBCounters
	TargetVBTimestamp
	TargetPerVBCounters

	// Global checkpoints will utilize multiple TargetVBTimestamp and TargetVBCounters
	GlobalTimestamp       GlobalTimestamp      `json:"-"`
	GlobalCounters        GlobalTargetCounters `json:"-"`
	GlobalTimestampSha256 string               `json:"globalTimestampMapSha256,omitempty"`
	GlobalCountersSha256  string               `json:"globalCountersMapSha256,omitempty"`

	// If this checkpoint is for a backfill pipeline, we will use this collection IDs list
	// to ensure that another backfill pipeline resumes from this checkpoint based on if the
	// new backfill pipeline's collection filter is the same (or subset) of the following field.
	// This list will be sorted and will be nil for main pipeline.
	BackfillCollections []uint32 `json:"bfillColIDs"`
}

// Generally speaking, brokenMapping should move in tandem in a global checkpoint
// This means that say a broken map is empty at t0, and a new target collection is newly deleted at t1
// Traditionally, a single ckptRecord for a single VB may have an empty broken map recorded at t0, or a broken
// mapping at t1
// Now, with global timestamp, broken map state may be at t0, t1, or a future t2 state
// Thus, we need to return all the combination of brokenmappings in this record that has been deduped
func (c *CheckpointRecord) BrokenMappings() ShaToCollectionNamespaceMap {
	if c == nil {
		return nil
	}

	shaToBrokenMap := make(ShaToCollectionNamespaceMap)

	if c.IsTraditional() {
		c.brokenMappingsMtx.RLock()
		defer c.brokenMappingsMtx.RUnlock()
		if len(c.brokenMappings) > 0 {
			clonedMapping := c.brokenMappings.Clone()
			shaToBrokenMap[c.BrokenMappingSha256] = &clonedMapping
		}
	} else {
		for _, oneGlobalTs := range c.GlobalTimestamp {
			oneGlobalTs.brokenMappingsMtx.RLock()
			if oneGlobalTs.BrokenMappingSha256 != "" && len(oneGlobalTs.brokenMappings) > 0 {
				clonedMapping := oneGlobalTs.brokenMappings.Clone()
				shaToBrokenMap[oneGlobalTs.BrokenMappingSha256] = &clonedMapping
			}
			oneGlobalTs.brokenMappingsMtx.RUnlock()
		}
	}
	return shaToBrokenMap
}

func (c *CheckpointRecord) GetBrokenMappingShaCount() int {
	if c == nil {
		return 0
	}

	var count int
	if c.IsTraditional() {
		c.brokenMappingsMtx.RLock()
		if c.BrokenMappingSha256 != "" {
			count++
		}
		c.brokenMappingsMtx.RUnlock()
	} else {
		for _, oneGlobalTs := range c.GlobalTimestamp {
			oneGlobalTs.brokenMappingsMtx.RLock()
			if oneGlobalTs.BrokenMappingSha256 != "" {
				count++
			}
			oneGlobalTs.brokenMappingsMtx.RUnlock()
		}
	}
	return count
}

func (c *CheckpointRecord) GetBrokenMappingSha256s() []string {
	dedupMap := make(map[string]bool)
	if c.IsTraditional() {
		c.brokenMappingsMtx.RLock()
		defer c.brokenMappingsMtx.RUnlock()
		return []string{c.BrokenMappingSha256}
	} else {
		for _, oneGlobalTs := range c.GlobalTimestamp {
			oneGlobalTs.brokenMappingsMtx.RLock()
			if oneGlobalTs.BrokenMappingSha256 != "" {
				dedupMap[oneGlobalTs.BrokenMappingSha256] = true
			}
			oneGlobalTs.brokenMappingsMtx.RUnlock()
		}
	}
	retList := make([]string, 0, len(dedupMap))
	for k, _ := range dedupMap {
		retList = append(retList, k)
	}
	return retList
}

type ManifestIdAndBrokenMapPair struct {
	ManifestId uint64
	BrokenMap  *CollectionNamespaceMapping
}

func (c *CheckpointRecord) BrokenMappingsAndManifestIds() (ManifestIdAndBrokenMapPair, map[uint16]ManifestIdAndBrokenMapPair) {
	if c.IsTraditional() {
		c.brokenMappingsMtx.RLock()
		defer c.brokenMappingsMtx.RUnlock()
		clonedMapping := c.brokenMappings.Clone()
		var traditionalPair ManifestIdAndBrokenMapPair
		traditionalPair.BrokenMap = &clonedMapping
		traditionalPair.ManifestId = c.TargetVBTimestamp.TargetManifest
		return traditionalPair, nil
	} else {
		globalMap := make(map[uint16]ManifestIdAndBrokenMapPair)
		for tgtVb, oneGlobalTs := range c.GlobalTimestamp {
			var brokenMapToReturn *CollectionNamespaceMapping
			oneGlobalTs.brokenMappingsMtx.RLock()
			if oneGlobalTs.brokenMappings != nil && len(oneGlobalTs.brokenMappings) > 0 {
				clonedMapping := oneGlobalTs.brokenMappings.Clone()
				brokenMapToReturn = &clonedMapping
			}
			oneGlobalTs.brokenMappingsMtx.RUnlock()
			globalMap[tgtVb] = ManifestIdAndBrokenMapPair{
				BrokenMap:  brokenMapToReturn,
				ManifestId: oneGlobalTs.TargetManifest,
			}
		}
		return ManifestIdAndBrokenMapPair{}, globalMap
	}
}

func (c *CheckpointRecord) BrokenMappingsLen() int {
	var counter int
	if c == nil {
		return counter
	}

	if c.IsTraditional() {
		c.brokenMappingsMtx.RLock()
		defer c.brokenMappingsMtx.RUnlock()
		if c.BrokenMappingSha256 != "" {
			// One sha -> actual broken map "map"
			counter++
		}
	} else {
		for _, oneGlobalTs := range c.GlobalTimestamp {
			oneGlobalTs.brokenMappingsMtx.RLock()
			if oneGlobalTs.BrokenMappingSha256 != "" {
				counter++
			}
			oneGlobalTs.brokenMappingsMtx.RUnlock()
		}
	}
	return counter
}

// GetShaOnlyMap is used when checkpoint record is first loaded and the actual
// mapping themselves do not exist (but only the Shas)
// Callers will then fill in the actual mappings as needed
func (c *CheckpointRecord) GetShaOnlyMap() ShaToCollectionNamespaceMap {
	if c == nil {
		return nil
	}

	retMap := make(ShaToCollectionNamespaceMap)
	if c.IsTraditional() {
		if c.BrokenMappingSha256 != "" {
			retMap[c.BrokenMappingSha256] = nil
		}
	} else {
		for _, oneGlobalTs := range c.GlobalTimestamp {
			if oneGlobalTs.BrokenMappingSha256 != "" {
				retMap[oneGlobalTs.BrokenMappingSha256] = nil
			}
		}
	}
	return retMap
}

func (c *CheckpointRecord) GetGlobalInfoMap() ShaToGlobalInfoMap {
	if c == nil {
		return nil
	}

	retMap := make(ShaToGlobalInfoMap)
	if c.IsTraditional() {
		return retMap
	}

	// Load GlobalTimestamp sha
	if c.GlobalTimestampSha256 != "" {
		retMap[c.GlobalTimestampSha256] = nil
	}

	// Load GlobalCounters sha
	if c.GlobalCountersSha256 != "" {
		retMap[c.GlobalCountersSha256] = nil
	}
	return retMap
}

func (c *CheckpointRecord) IsTraditional() bool {
	if c == nil {
		return true
	}
	return c.Target_vb_opaque != nil
}

func (c *CheckpointRecord) Size() int {
	if c == nil {
		return 0
	}

	var totalSize int
	totalSize += 8 // CreationTime

	totalSize += len(c.PipelineReinitHash)
	totalSize += len(c.BackfillCollections) * 4

	// Either traditional or global share the following 2 embedded structs
	totalSize += c.SourceVBTimestamp.Size()
	totalSize += c.SourceVBCounters.Size()

	if c.IsTraditional() {
		totalSize += c.TargetVBTimestamp.Size()
		totalSize += c.TargetPerVBCounters.Size()
	} else {
		totalSize += c.GlobalTimestamp.Size()
		totalSize += c.GlobalCounters.Size()
		totalSize += len(c.GlobalTimestampSha256)
		totalSize += len(c.GlobalCountersSha256)
	}
	return totalSize
}

func NewCheckpointRecord(failoverUuid, seqno, dcpSnapSeqno, dcpSnapEnd uint64,
	tgtTimestamp TargetTimestamp, incomingMetric base.VBCountMetric, srcManifestForDCP, srcManifestForBackfill,
	creationTime uint64, pipelineReinitHash string, backfillCollections []uint32) (*CheckpointRecord, error) {

	var record *CheckpointRecord
	var err error

	if incomingMetric.IsTraditional() {
		if !tgtTimestamp.IsTraditional() {
			return nil, fmt.Errorf("coding error: tgtTimestamp and incoming metrics must be both traditional")
		}
		targetVbTimestamp, ok := tgtTimestamp.GetValue().(*TargetVBTimestamp)
		if !ok {
			return nil, fmt.Errorf("expecting *TargetVBTimestamp, but got %T", tgtTimestamp)
		}
		record = newTraditionalCheckpointRecord(failoverUuid, seqno, dcpSnapSeqno, dcpSnapEnd, targetVbTimestamp, srcManifestForDCP, srcManifestForBackfill, creationTime, incomingMetric, pipelineReinitHash, backfillCollections)
	} else {
		record, err = newGlobalCheckpointRecord(failoverUuid, seqno, dcpSnapSeqno, dcpSnapEnd, tgtTimestamp, srcManifestForDCP, srcManifestForBackfill, creationTime, incomingMetric, pipelineReinitHash, backfillCollections)
		if err != nil {
			return nil, err
		}
		err = record.PopulateShasForGlobalInfo()
		if err != nil {
			return nil, err
		}
	}
	err = record.PopulateBrokenMappingSha()
	if err != nil {
		return nil, err
	}
	return record, nil
}

func newTraditionalCheckpointRecord(failoverUuid uint64, seqno uint64, dcpSnapSeqno uint64, dcpSnapEnd uint64, tgtTimestamp *TargetVBTimestamp, srcManifestForDCP uint64,
	srcManifestForBackfill uint64, creationTime uint64, incomingMetric base.VBCountMetric, pipelineReinitHash string, backfillCollections []uint32) *CheckpointRecord {
	vbCountMetrics := incomingMetric.GetValue().(base.TraditionalVBMetrics)
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
	subdocCmdSkippedCnt := uint64(vbCountMetrics[base.SubdocCmdsSkippedCount])

	record := &CheckpointRecord{
		SourceVBTimestamp: SourceVBTimestamp{
			Failover_uuid:                failoverUuid,
			Seqno:                        seqno,
			Dcp_snapshot_seqno:           dcpSnapSeqno,
			Dcp_snapshot_end_seqno:       dcpSnapEnd,
			SourceManifestForDCP:         srcManifestForDCP,
			SourceManifestForBackfillMgr: srcManifestForBackfill,
		},
		TargetVBTimestamp: *tgtTimestamp,
		SourceVBCounters: SourceVBCounters{
			FilteredItemsCnt:                   filteredItems,
			FilteredFailedCnt:                  filterFailed,
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
			CLogHibernatedCnt:                  clogHibernatedCnt,
			CasPoisonCnt:                       casPoisonItems,
		},
		CreationTime:       creationTime,
		PipelineReinitHash: pipelineReinitHash,
		TargetPerVBCounters: TargetPerVBCounters{
			GuardrailResidentRatioCnt:          guardrailResidentRatioItems,
			GuardrailDataSizeCnt:               guardrailDataSizeItems,
			GuardrailDiskSpaceCnt:              guardrailDiskSpaceItems,
			DocsSentWithSubdocSetCnt:           subdocSetItems,
			DocsSentWithSubdocDeleteCnt:        subdocDeleteItems,
			DocsSentWithPoisonedCasErrorMode:   docsSentWithPoisonedCasErrorMode,
			DocsSentWithPoisonedCasReplaceMode: docsSentWithPoisonedCasReplaceMode,
			GetDocsCasChangedCnt:               getDocsCasChangedCnt,
			TrueConflictsDetected:              trueConflictsDetected,
			SubdocCmdSkippedCnt:                subdocCmdSkippedCnt,
		},
		BackfillCollections: backfillCollections,
	}
	return record
}

func newGlobalCheckpointRecord(failoverUuid uint64, seqno uint64, dcpSnapSeqno uint64, dcpSnapEnd uint64, targetTimestamp TargetTimestamp, srcManifestForDCP uint64, srcManifestForBackfill uint64,
	creationTime uint64, incomingMetric base.VBCountMetric, pipelineReinitHash string, backfillCollections []uint32) (*CheckpointRecord, error) {
	vbCountMetrics := incomingMetric.GetValue().(base.GlobalVBMetrics)

	var srcFilteredCntrs SourceVBCounters
	for _, routerKey := range base.RouterVBMetricKeys {
		vbCountMap := vbCountMetrics[routerKey]
		if len(vbCountMap) > 1 {
			return nil, fmt.Errorf("global stats %v should only have 1 vb", routerKey)
		}
		var vbKey uint16
		for vb := range vbCountMap {
			vbKey = vb
		}
		switch routerKey {
		case base.DocsFiltered:
			srcFilteredCntrs.FilteredItemsCnt = uint64(vbCountMap[vbKey])
		case base.DocsUnableToFilter:
			srcFilteredCntrs.FilteredFailedCnt = uint64(vbCountMap[vbKey])
		case base.ExpiryFiltered:
			srcFilteredCntrs.FilteredItemsOnExpirationsCnt = uint64(vbCountMap[vbKey])
		case base.DeletionFiltered:
			srcFilteredCntrs.FilteredItemsOnDeletionsCnt = uint64(vbCountMap[vbKey])
		case base.SetFiltered:
			srcFilteredCntrs.FilteredItemsOnSetCnt = uint64(vbCountMap[vbKey])
		case base.BinaryFiltered:
			srcFilteredCntrs.FilteredItemsOnBinaryDocsCnt = uint64(vbCountMap[vbKey])
		case base.ExpiryStripped:
			srcFilteredCntrs.FilteredItemsOnExpiryStrippedCnt = uint64(vbCountMap[vbKey])
		case base.AtrTxnDocsFiltered:
			srcFilteredCntrs.FilteredItemsOnATRDocsCnt = uint64(vbCountMap[vbKey])
		case base.ClientTxnDocsFiltered:
			srcFilteredCntrs.FilteredItemsOnTxnXattrsDocsCnt = uint64(vbCountMap[vbKey])
		case base.DocsFilteredOnTxnXattr:
			srcFilteredCntrs.FilteredItemsOnTxnXattrsDocsCnt = uint64(vbCountMap[vbKey])
		case base.MobileDocsFiltered:
			srcFilteredCntrs.FilteredItemsOnMobileRecords = uint64(vbCountMap[vbKey])
		case base.DocsFilteredOnUserDefinedFilter:
			srcFilteredCntrs.FilteredItemsOnUserDefinedFilters = uint64(vbCountMap[vbKey])
		case base.DocsCasPoisoned:
			srcFilteredCntrs.CasPoisonCnt = uint64(vbCountMap[vbKey])
		case base.ConflictDocsFiltered:
			srcFilteredCntrs.FilteredConflictDocs = uint64(vbCountMap[vbKey])
		}
	}

	for _, clogKey := range base.CLogVBMetricKeys {
		vbCountMap := vbCountMetrics[clogKey]
		if len(vbCountMap) > 1 {
			return nil, fmt.Errorf("global stats %v should only have 1 vb", clogKey)
		}
		var vbKey uint16
		for vb := range vbCountMap {
			vbKey = vb
		}
		switch clogKey {
		case base.SrcConflictDocsWritten:
			srcFilteredCntrs.SrcConflictDocsWritten = uint64(vbCountMap[vbKey])
		case base.TgtConflictDocsWritten:
			srcFilteredCntrs.TgtConflictDocsWritten = uint64(vbCountMap[vbKey])
		case base.CRDConflictDocsWritten:
			srcFilteredCntrs.CRDConflictDocsWritten = uint64(vbCountMap[vbKey])
		case base.CLogHibernatedCount:
			srcFilteredCntrs.CLogHibernatedCnt = uint64(vbCountMap[vbKey])
		}
	}

	globalCntrs := make(GlobalTargetCounters)
	for _, outNozzleKey := range base.OutNozzleVBMetricKeys {
		vbCountMap := vbCountMetrics[outNozzleKey]
		for vb, count := range vbCountMap {
			if globalCntrs[vb] == nil {
				globalCntrs[vb] = &TargetPerVBCounters{}
			}
			switch outNozzleKey {
			case base.GuardrailResidentRatio:
				globalCntrs[vb].GuardrailResidentRatioCnt = uint64(count)
			case base.GuardrailDataSize:
				globalCntrs[vb].GuardrailDataSizeCnt = uint64(count)
			case base.GuardrailDiskSpace:
				globalCntrs[vb].GuardrailDiskSpaceCnt = uint64(count)
			case base.DocsSentWithSubdocSet:
				globalCntrs[vb].DocsSentWithSubdocSetCnt = uint64(count)
			case base.DocsSentWithSubdocDelete:
				globalCntrs[vb].DocsSentWithSubdocDeleteCnt = uint64(count)
			case base.DocsSentWithPoisonedCasErrorMode:
				globalCntrs[vb].DocsSentWithPoisonedCasErrorMode = uint64(count)
			case base.DocsSentWithPoisonedCasReplaceMode:
				globalCntrs[vb].DocsSentWithPoisonedCasReplaceMode = uint64(count)
			case base.TrueConflictsDetected:
				globalCntrs[vb].TrueConflictsDetected = uint64(count)
			case base.GetDocsCasChangedCount:
				globalCntrs[vb].GetDocsCasChangedCnt = uint64(count)
			case base.SubdocCmdsSkippedCount:
				globalCntrs[vb].SubdocCmdSkippedCnt = uint64(count)
			}
		}
	}

	globalTs, ok := targetTimestamp.(*GlobalTimestamp)
	if !ok {
		return nil, fmt.Errorf("global timestamp passed in of type %T expecting *GlobalTimestamp", targetTimestamp)
	}
	if globalTs == nil || len(*globalTs) == 0 {
		return nil, fmt.Errorf("global timestamp passed in is nil or empty")
	}

	record := &CheckpointRecord{
		SourceVBTimestamp: SourceVBTimestamp{
			Failover_uuid:                failoverUuid,
			Seqno:                        seqno,
			Dcp_snapshot_seqno:           dcpSnapSeqno,
			Dcp_snapshot_end_seqno:       dcpSnapEnd,
			SourceManifestForDCP:         srcManifestForDCP,
			SourceManifestForBackfillMgr: srcManifestForBackfill,
		},
		SourceVBCounters:    srcFilteredCntrs,
		GlobalCounters:      globalCntrs,
		GlobalTimestamp:     *globalTs,
		CreationTime:        creationTime,
		PipelineReinitHash:  pipelineReinitHash,
		BackfillCollections: backfillCollections,
	}
	return record, nil
}

func (ckptRecord *CheckpointRecord) PopulateBrokenMappingSha() error {
	if ckptRecord.IsTraditional() {
		return ckptRecord.TargetVBTimestamp.PopulateBrokenMappingSha()
	} else {
		errMap := make(base.ErrorMap)
		for vbno, perVbTs := range ckptRecord.GlobalTimestamp {
			err := perVbTs.PopulateBrokenMappingSha()
			if err != nil {
				errMap[fmt.Sprintf("PopulateBrokenMappingSha vb(%v)", vbno)] = err
			}
		}
		if len(errMap) > 0 {
			return fmt.Errorf("%s", base.FlattenErrorMap(errMap))
		}
	}
	return nil
}

func (ckptRecord *CheckpointRecord) PopulateShasForGlobalInfo() error {
	if ckptRecord.IsTraditional() {
		return fmt.Errorf("PopulateShasForGlobalInfo called on a traditional checkpoint")
	} else {
		globalTimestampSha, err1 := ckptRecord.GlobalTimestamp.Sha256()
		globalCounterSha, err2 := ckptRecord.GlobalCounters.Sha256()
		if err1 != nil || err2 != nil {
			return fmt.Errorf("error occured while performing sha hash. globalTimestampErr=%v globalCounterSha=%v", err1, err2)
		}
		ckptRecord.GlobalTimestampSha256 = fmt.Sprintf("%x", globalTimestampSha[:])
		ckptRecord.GlobalCountersSha256 = fmt.Sprintf("%x", globalCounterSha[:])
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
		ckptRecord.TargetVBTimestamp.SameAs(&newRecord.TargetVBTimestamp) &&
		ckptRecord.SourceVBCounters.SameAs(&newRecord.SourceVBCounters) &&
		ckptRecord.SourceManifestForDCP == newRecord.SourceManifestForDCP &&
		ckptRecord.SourceManifestForBackfillMgr == newRecord.SourceManifestForBackfillMgr &&
		ckptRecord.BrokenMappingSha256 == newRecord.BrokenMappingSha256 &&
		ckptRecord.CreationTime == newRecord.CreationTime &&
		ckptRecord.GuardrailDiskSpaceCnt == newRecord.GuardrailDiskSpaceCnt &&
		ckptRecord.GuardrailDataSizeCnt == newRecord.GuardrailDataSizeCnt &&
		ckptRecord.GuardrailResidentRatioCnt == newRecord.GuardrailResidentRatioCnt &&
		ckptRecord.DocsSentWithSubdocDeleteCnt == newRecord.DocsSentWithSubdocDeleteCnt &&
		ckptRecord.DocsSentWithSubdocSetCnt == newRecord.DocsSentWithSubdocSetCnt &&
		ckptRecord.CasPoisonCnt == newRecord.CasPoisonCnt &&
		ckptRecord.SubdocCmdSkippedCnt == newRecord.SubdocCmdSkippedCnt &&
		ckptRecord.DocsSentWithPoisonedCasErrorMode == newRecord.DocsSentWithPoisonedCasErrorMode &&
		ckptRecord.DocsSentWithPoisonedCasReplaceMode == newRecord.DocsSentWithPoisonedCasReplaceMode &&
		ckptRecord.TrueConflictsDetected == newRecord.TrueConflictsDetected &&
		ckptRecord.CLogHibernatedCnt == newRecord.CLogHibernatedCnt &&
		ckptRecord.GetDocsCasChangedCnt == newRecord.GetDocsCasChangedCnt &&
		ckptRecord.TargetPerVBCounters.SameAs(&newRecord.TargetPerVBCounters) &&
		ckptRecord.GlobalTimestampSha256 == newRecord.GlobalTimestampSha256 &&
		ckptRecord.GlobalCountersSha256 == newRecord.GlobalCountersSha256 &&
		ckptRecord.PipelineReinitHash == newRecord.PipelineReinitHash &&
		base.Uint32List(ckptRecord.BackfillCollections).Equal(newRecord.BackfillCollections) {
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
	ckptRecord.TargetVBTimestamp = other.TargetVBTimestamp.Clone()
	ckptRecord.FilteredItemsCnt = other.FilteredItemsCnt
	ckptRecord.FilteredFailedCnt = other.FilteredFailedCnt
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
	ckptRecord.GlobalCounters = other.GlobalCounters.Clone()
	ckptRecord.GlobalTimestamp = other.GlobalTimestamp.CloneGlobalTimestamp()
	ckptRecord.GlobalTimestampSha256 = other.GlobalTimestampSha256
	ckptRecord.GlobalCountersSha256 = other.GlobalCountersSha256
	ckptRecord.SubdocCmdSkippedCnt = other.SubdocCmdSkippedCnt
	ckptRecord.PipelineReinitHash = other.PipelineReinitHash
	ckptRecord.BackfillCollections = base.Uint32List(other.BackfillCollections).Clone()
}

func (ckptRecord *CheckpointRecord) LoadBrokenMapping(allShaToBrokenMaps ShaToCollectionNamespaceMap) error {
	if ckptRecord == nil {
		return fmt.Errorf("nil ckptRecord")
	}

	if ckptRecord.IsTraditional() {
		for sha, mapping := range allShaToBrokenMaps {
			if mapping == nil || len(allShaToBrokenMaps) > 1 {
				return fmt.Errorf("LoadBrokenMapping should expect one non-nil mapping %v len: %v", allShaToBrokenMaps, len(allShaToBrokenMaps))
			}
			if ckptRecord.BrokenMappingSha256 != sha {
				continue
			}
			err := ckptRecord.TargetVBTimestamp.SetBrokenMappingAsPartOfCreation(*mapping)
			if err != nil {
				return err
			}
		}
		return ckptRecord.PopulateBrokenMappingSha()
	} else {
		for _, globalTs := range ckptRecord.GlobalTimestamp {
			if globalTs.BrokenMappingSha256 != "" {
				brokenMap, exists := allShaToBrokenMaps[globalTs.BrokenMappingSha256]
				if !exists || brokenMap == nil {
					continue
				}
				globalTs.brokenMappingsMtx.Lock()
				globalTs.brokenMappings = *brokenMap
				globalTs.brokenMappingsMtx.Unlock()
			}
		}
		return nil
	}
}

func (ckptRecord *CheckpointRecord) LoadGlobalInfoMapping(allShasToGInfo ShaToGlobalInfoMap) error {
	if ckptRecord == nil {
		return fmt.Errorf("checkpoint record is nil")
	}

	// Load Global Timestamp if available
	if ckptRecord.GlobalTimestampSha256 != "" {
		if globalTsIface, exists := allShasToGInfo[ckptRecord.GlobalTimestampSha256]; exists {
			if globalTs, ok := globalTsIface.(*GlobalTimestamp); ok {
				ckptRecord.GlobalTimestamp = *globalTs
			}
		}
	}

	// Load Global Counters if available
	if ckptRecord.GlobalCountersSha256 != "" {
		if globalCountersIface, exists := allShasToGInfo[ckptRecord.GlobalCountersSha256]; exists {
			if globalCounters, ok := globalCountersIface.(*GlobalTargetCounters); ok {
				ckptRecord.GlobalCounters = *globalCounters
			}
		}
	}
	return nil
}

// Initializes the global data structures i.e. GlobalTimestamp and GlobalCounters for the specified VB
func (ckptRecord *CheckpointRecord) InitializeVbForGlobalInfo(targetVB uint16) {
	if ckptRecord == nil {
		return
	}
	ckptRecord.GlobalTimestamp.InitializeVb(targetVB)
	ckptRecord.GlobalCounters.InitializeVb(targetVB)
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
		ckptRecord.FilteredItemsCnt = uint64(filteredCnt.(float64))
	}

	filteredFailedCnt, ok := fieldMap[FilteredFailedCnt]
	if ok {
		ckptRecord.FilteredFailedCnt = uint64(filteredFailedCnt.(float64))
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

	subdocCmdSkippedCnt, ok := fieldMap[SubdocCmdSkippedCnt]
	if ok {
		ckptRecord.SubdocCmdSkippedCnt = uint64(subdocCmdSkippedCnt.(float64))
	}

	globalTimestampRaw, ok := fieldMap[GlobalTimestampStr]
	if ok && globalTimestampRaw != nil {
		ckptRecord.GlobalTimestamp = make(GlobalTimestamp)
		err = ckptRecord.GlobalTimestamp.LoadUnmarshalled(globalTimestampRaw.(map[string]interface{}))
		if err != nil {
			return fmt.Errorf("ckptRecord.GlobalTimestamp.LoadUnmarshalled %v", err)
		}
	}

	globalTargetCounters, ok := fieldMap[GlobalTargetCountersStr]
	if ok && globalTargetCounters != nil {
		ckptRecord.GlobalCounters = make(GlobalTargetCounters)
		err = ckptRecord.GlobalCounters.LoadUnmarshalled(globalTargetCounters.(map[string]interface{}))
		if err != nil {
			return fmt.Errorf("ckptRecord.GlobalCOunters.LoadUnmarshalled %v", err)
		}
	}

	globalTimestampSha, ok := fieldMap[GlobalTimestampMapSha]
	if ok {
		ckptRecord.GlobalTimestampSha256 = globalTimestampSha.(string)
	}

	globalCountersSha, ok := fieldMap[GlobalCountersMapSha]
	if ok {
		ckptRecord.GlobalCountersSha256 = globalCountersSha.(string)
	}

	pipelineReinitHash, ok := fieldMap[PipelineReinitHashCkptKey]
	if ok {
		ckptRecord.PipelineReinitHash = pipelineReinitHash.(string)
	}

	backfillCollections, ok := fieldMap[BackfillCollections]
	if ok && backfillCollections != nil {
		backfillColIDsList := backfillCollections.([]interface{})
		ckptRecord.BackfillCollections = make([]uint32, 0)
		for _, colId := range backfillColIDsList {
			ckptRecord.BackfillCollections = append(ckptRecord.BackfillCollections, uint32(colId.(float64)))
		}
	}

	return nil
}

// For a backfill pipeline, we can resume from a ckptRecord, only if ckptRecord was taken for a backfill
// pipeline of a set of collections, which is same as or a superset of a set of collections for which
// the current backfill pipeline will be backfilling. In other words, currBackfillColIDs should be a subset
// or equal to ckptBackfillColIDs. Otherwise, there will be count mismatch for currBackfillColIDs - ckptBackfillColIds
// set of collections.
func (ckptRecord *CheckpointRecord) FilterBasedOnBackfillCollections(currBackfillColIDs []uint32) (skipCkptRec bool) {
	ckptBackfillColIDs := ckptRecord.BackfillCollections
	if ckptBackfillColIDs == nil {
		// This is probably because this ckptRecord is from a pre-fix version. We will not skip this ckptRecord to
		// ensure if there any long running backfill pipelines, it won't restart from 0.
		return
	}

	currentCollectionIDs := base.Uint32List(currBackfillColIDs)
	if !currentCollectionIDs.IsSubset(ckptBackfillColIDs) {
		skipCkptRec = true
		return
	}

	return
}

type TargetVBOpaque interface {
	Value() interface{}
	IsSame(targetVBOpaque TargetVBOpaque) bool
	Size() int
	Clone() TargetVBOpaque
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

func (targetVBUuid *TargetVBUuid) Clone() TargetVBOpaque {
	if targetVBUuid == nil {
		return nil
	}

	return &TargetVBUuid{Target_vb_uuid: targetVBUuid.Target_vb_uuid}
}

type GlobalTargetVbUuids map[uint16][]uint64

// Note this function includes the header size of the map and the value slice
func (g *GlobalTargetVbUuids) Size() int {
	if g == nil {
		return 0
	}
	mapSize := unsafe.Sizeof(*g) // size of the map header

	for key, value := range *g {
		mapSize += unsafe.Sizeof(key)      // Size of the key
		mapSize += unsafe.Sizeof(value)    // Size of the slice header
		mapSize += uintptr(len(value)) * 8 // Size of the underlying slice data
	}
	return int(mapSize)
}

func (g *GlobalTargetVbUuids) Value() interface{} {
	if g == nil {
		return nil
	}
	return *g
}

// IsSame essentially checks to make sure all VBUUIDs haven't changed
// Note that the input must be a type of global target opaque used in the context
// of a checkpointing logic
// This is needed to implement the TargetVBOpaque interface and is *not*
// meant to be a direct comparison between two GlobalTargetVbUuids objects
// Use SameAs() for direct comparisons
func (g *GlobalTargetVbUuids) IsSame(globalTargetOpaque TargetVBOpaque) bool {
	lastFetchedTargetOpaque, ok := globalTargetOpaque.Value().(GlobalTimestamp)
	if !ok {
		// We don't accept any other type of TargetVBOpaque for global checkpointing
		// But because interface is hard-coded, there's no other way to raise error and prefer not to
		// raise panic
		// So, need to watch out for testing to see if it's constantly restreaming from 0
		// due to mismatching target VBOpaque
		return false
	}

	// VBs need to match
	if len(*g) != len(lastFetchedTargetOpaque) {
		return false
	}

	for vb, latestTargetVBTimestamp := range lastFetchedTargetOpaque {
		latestTargetVbUuid := latestTargetVBTimestamp.Target_vb_opaque.Value().(uint64)

		currentKnownSeqnoAndVbuuid, ok := (*g)[vb]
		if !ok {
			return false
		}
		currentKnownVbUuid := currentKnownSeqnoAndVbuuid[1]

		if currentKnownVbUuid != latestTargetVbUuid {
			return false
		}
	}
	return true
}

// SameAs is the actual method to compare between two objects of the same type in this case
func (g *GlobalTargetVbUuids) SameAs(other *GlobalTargetVbUuids) bool {
	if g == nil || other == nil {
		return g == nil && other == nil
	}

	if len(*g) != len(*other) {
		return false
	}

	uuidMap := base.HighSeqnoAndVbUuidMap(*g)
	diffOutput := uuidMap.Diff(base.HighSeqnoAndVbUuidMap(*other))
	return len(diffOutput) == 0
}

func (g *GlobalTargetVbUuids) ToGlobalTimestamp() *GlobalTimestamp {
	if g == nil {
		return nil
	}

	gts := make(GlobalTimestamp)
	for vb, pair := range *g {
		gts[vb] = &GlobalVBTimestamp{
			TargetVBTimestamp: TargetVBTimestamp{
				Target_vb_opaque: &TargetVBUuid{pair[1]},
				Target_Seqno:     pair[0],
			},
		}
	}

	return &gts
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

func (targetVBUuid *TargetVBUuidStr) Clone() TargetVBOpaque {
	if targetVBUuid == nil {
		return nil
	}

	return &TargetVBUuidStr{Target_vb_uuid: targetVBUuid.Target_vb_uuid}
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

func (targetVBUuidAndTimestamp *TargetVBUuidAndTimestamp) Clone() TargetVBOpaque {
	if targetVBUuidAndTimestamp == nil {
		return nil
	}

	return &TargetVBUuidAndTimestamp{
		Target_vb_uuid: targetVBUuidAndTimestamp.Target_vb_uuid,
		Startup_time:   targetVBUuidAndTimestamp.Startup_time,
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

		// Note that if the UUID is > Maxuint32, it could be subjected to loss of precision
		// by doing this type of conversion.
		// We technically haven't seen a CBSE yet... but it could be an issue potentially...
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
	return fmt.Sprintf("{pipelineReinitHash=%v; Failover_uuid=%v; Seqno=%v; Dcp_snapshot_seqno=%v; Dcp_snapshot_end_seqno=%v; Target_vb_opaque=%v; Commitopaque=%v; SourceManifestForDCP=%v; SourceManifestForBackfillMgr=%v; TargetManifest=%v; BrokenMappingShaCnt=%v; BrokenMappingInfoType=%v; bfillCol=%v}",
		ckpt_record.PipelineReinitHash, ckpt_record.Failover_uuid, ckpt_record.Seqno, ckpt_record.Dcp_snapshot_seqno, ckpt_record.Dcp_snapshot_end_seqno, ckpt_record.Target_vb_opaque,
		ckpt_record.Target_Seqno, ckpt_record.SourceManifestForDCP, ckpt_record.SourceManifestForBackfillMgr, ckpt_record.TargetManifest, ckpt_record.GetBrokenMappingShaCount(),
		ckpt_record.brokenMappings, ckpt_record.BackfillCollections)
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
	// indicates that aRecord is the lastest checkpoint
	// Note - this is under the assumption that the wall clocks of all the nodes in a cluster
	// are synchronised using NTP
	return aRecord.CreationTime > bRecord.CreationTime
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
		// aRecord is "less than", or should belong in front of bRecord
		return true, true
	} else if aRecord.CheckpointRecord == nil && bRecord.CheckpointRecord == nil {
		// Just say yes
		return true, false
	}

	if !source && !aRecord.IsTraditional() {
		// For global target checkpoint there is no point to compare between target global timestamps
		// purely because they are all over the place... each source VB could have up to 1024 target highSeqno/VBUUID
		// pairs and it's not logical to find a way to sort that
		return false, false
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
//  1. Compressed map of ckpts
//  2. A deduped map consisted of elements of:
//     a. brokenMapSha -> snappy compressed broken map referred by all ckpts in the docs
//     b. globalTimestampSha -> snappy compressed global timestamp
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

// Given a set of VBs and each one's individual checkpoints, deduplicate and return
// a single compressed documented that has all the global timestamps compressed and the shas populated
func (v *VBsCkptsDocMap) GetGlobalInfoDoc() (*GlobalInfoCompressedDoc, error) {
	if v == nil {
		return nil, base.ErrorNilPtr
	}

	_, shaMap, err := v.SnappyCompress()
	// Darshan TODO - the shaMap here contains the broken mappings as well - Can save some bytes over the wire
	// Note - Currently brokenMappings are populated seperatly in the payload
	if err != nil {
		return nil, err
	}

	retDoc := &GlobalInfoCompressedDoc{}
	err = retDoc.LoadCompressedShaMap(shaMap)
	if err != nil {
		return nil, err
	}

	return retDoc, nil
}

type VBsCkptsDocSnappyMap map[uint16][]byte

// Decompress snappyShaMap containing both brokenMappings and globalInfoMap
func SnappyDecompressShaMap(snappyShaMap ShaMappingCompressedMap) (ShaToCollectionNamespaceMap, ShaToGlobalInfoMap, error) {
	if snappyShaMap == nil {
		return nil, nil, base.ErrorNilPtr
	}
	brokenMap, err := snappyShaMap.ToBrokenMapShaMap()
	if err != nil {
		return nil, nil, err
	}
	globalInfoMap, err := snappyShaMap.ToGlobalInfoMap()
	if err != nil {
		return nil, nil, err
	}
	return brokenMap, globalInfoMap, nil
}

func (v *VBsCkptsDocSnappyMap) SnappyDecompress(snappyShaMap ShaMappingCompressedMap) (VBsCkptsDocMap, error) {
	if v == nil || snappyShaMap == nil {
		return nil, base.ErrorNilPtr
	}
	brokenMap, globalInfoShaMap, err := SnappyDecompressShaMap(snappyShaMap)
	if err != nil {
		return nil, err
	}
	errMap := make(base.ErrorMap)
	regularMap := make(VBsCkptsDocMap)
	for vbno, compresedBytes := range *v {
		if compresedBytes == nil {
			regularMap[vbno] = nil
		} else {
			ckptDoc, err := NewCheckpointsDocFromSnappy(compresedBytes, brokenMap, globalInfoShaMap)
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

func (s *ShaMappingCompressedMap) ToBrokenMapShaMap() (ShaToCollectionNamespaceMap, error) {
	if s == nil {
		return nil, base.ErrorNilPtr
	}
	outputMap := make(ShaToCollectionNamespaceMap)
	for sha, _ := range *s {
		uncompresedMap, err := s.GetCollectionNsMapping(sha)
		if err != nil {
			if err == errorColNsMappingWrongType {
				// Could be different types of snappy sha-maps
				continue
			}
			return nil, err
		}
		outputMap[sha] = uncompresedMap
	}
	return outputMap, nil
}

func (s *ShaMappingCompressedMap) ToGlobalInfoMap() (ShaToGlobalInfoMap, error) {
	if s == nil {
		return nil, base.ErrorNilPtr
	}
	outputMap := make(ShaToGlobalInfoMap)
	for sha, _ := range *s {
		uncompresedMap, err := s.GetGlobalInfoMapping(sha)
		if err != nil {
			if err == ErrorGlobalInfoWrongType {
				// Could be different types of snappy sha-maps
				continue
			}
			return nil, err
		}
		outputMap[sha] = uncompresedMap
	}
	return outputMap, nil
}

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

func (s *ShaMappingCompressedMap) GetCollectionNsMapping(sha string) (*CollectionNamespaceMapping, error) {
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

func (s *ShaMappingCompressedMap) GetGlobalInfoMapping(sha string) (GlobalInfo, error) {
	if s == nil {
		return nil, base.ErrorNilPtr
	}

	if _, exists := (*s)[sha]; !exists {
		return nil, base.ErrorNotFound
	}

	gInfoMetaObj := &GlobalInfoMetaObj{}

	deCompressErr := gInfoMetaObj.SnappyDecompress((*s)[sha])
	if deCompressErr != nil {
		return nil, deCompressErr
	}
	return gInfoMetaObj.Data, nil

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

func (c *CheckpointsDoc) GetMaxCkptRecordsToKeep() int {
	if c.IsTraditional() {
		return base.MaxCheckpointRecordsToKeepTraditional
	} else {
		return base.MaxCheckpointRecordsToKeepVariableVB
	}
}

func (c *CheckpointsDoc) GetMaxCkptRecordsToRead() int {
	if c.IsTraditional() {
		return base.MaxCheckpointRecordsToReadTraditional
	} else {
		return base.MaxCheckpointRecordsToReadVariableVB
	}
}

func (ckpt *CheckpointRecord) ToMap() map[string]interface{} {
	ckpt_record_map := make(map[string]interface{})
	ckpt_record_map[PipelineReinitHashCkptKey] = ckpt.PipelineReinitHash
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
		SourceVBTimestamp: SourceVBTimestamp{
			Failover_uuid:                c.Failover_uuid,
			Seqno:                        c.Seqno,
			Dcp_snapshot_seqno:           c.Dcp_snapshot_seqno,
			Dcp_snapshot_end_seqno:       c.Dcp_snapshot_end_seqno,
			SourceManifestForDCP:         c.SourceManifestForDCP,
			SourceManifestForBackfillMgr: c.SourceManifestForBackfillMgr,
		},
		TargetVBTimestamp: TargetVBTimestamp{
			// Target_vb_opaque to be cloned later
			Target_Seqno:        c.Target_Seqno,
			TargetManifest:      c.TargetManifest,
			BrokenMappingSha256: c.BrokenMappingSha256,
			brokenMappings:      c.brokenMappings.Clone(),
			brokenMappingsMtx:   sync.RWMutex{},
		},
		SourceVBCounters: SourceVBCounters{
			FilteredItemsCnt:                   c.FilteredItemsCnt,
			FilteredFailedCnt:                  c.FilteredFailedCnt,
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
			CasPoisonCnt:                       c.CasPoisonCnt,
			CLogHibernatedCnt:                  c.CLogHibernatedCnt,
		},

		CreationTime:       c.CreationTime,
		PipelineReinitHash: c.PipelineReinitHash,
		TargetPerVBCounters: TargetPerVBCounters{
			GuardrailDiskSpaceCnt:              c.GuardrailDiskSpaceCnt,
			GuardrailDataSizeCnt:               c.GuardrailDataSizeCnt,
			GuardrailResidentRatioCnt:          c.GuardrailResidentRatioCnt,
			DocsSentWithSubdocSetCnt:           c.DocsSentWithSubdocSetCnt,
			DocsSentWithSubdocDeleteCnt:        c.DocsSentWithSubdocDeleteCnt,
			DocsSentWithPoisonedCasErrorMode:   c.DocsSentWithPoisonedCasErrorMode,
			DocsSentWithPoisonedCasReplaceMode: c.DocsSentWithPoisonedCasReplaceMode,
			TrueConflictsDetected:              c.TrueConflictsDetected,
			GetDocsCasChangedCnt:               c.GetDocsCasChangedCnt,
			SubdocCmdSkippedCnt:                c.SubdocCmdSkippedCnt,
		},

		GlobalTimestamp:       c.GlobalTimestamp.CloneGlobalTimestamp(),
		GlobalCounters:        c.GlobalCounters.Clone(),
		GlobalTimestampSha256: c.GlobalTimestampSha256,
		GlobalCountersSha256:  c.GlobalCountersSha256,
		BackfillCollections:   base.Uint32List(c.BackfillCollections).Clone(),
	}

	if c.Target_vb_opaque != nil {
		retVal.Target_vb_opaque = c.Target_vb_opaque.Clone()
	}
	return retVal
}

func (c *CheckpointRecord) IsValidGlobalTs(targetVBs []uint16) bool {
	if c == nil || c.IsTraditional() {
		return false
	}

	for _, tgtVB := range targetVBs {
		if c.GlobalTimestamp == nil || c.GlobalTimestamp[tgtVB] == nil {
			return false
		}
	}
	return true
}

func NewCheckpointsDoc(specInternalId string) *CheckpointsDoc {
	ckpt_doc := &CheckpointsDoc{Checkpoint_records: []*CheckpointRecord{},
		SpecInternalId: specInternalId,
		Revision:       nil}

	maxCkptsToKeep := ckpt_doc.GetMaxCkptRecordsToKeep()
	for i := 0; i < maxCkptsToKeep; i++ {
		ckpt_doc.Checkpoint_records = append(ckpt_doc.Checkpoint_records, nil)
	}

	return ckpt_doc
}

func NewCheckpointsDocFromSnappy(snappyBytes []byte, brokenMap ShaToCollectionNamespaceMap, globalInfoShaMap ShaToGlobalInfoMap) (*CheckpointsDoc, error) {
	ckptDoc := &CheckpointsDoc{}
	err := ckptDoc.SnappyDecompress(snappyBytes, brokenMap, globalInfoShaMap)

	if err != nil {
		return nil, err
	}

	return ckptDoc, nil
}

// Not concurrency safe. It should be used by one goroutine only
func (ckptsDoc *CheckpointsDoc) AddRecord(record *CheckpointRecord) (added bool, removedRecords []*CheckpointRecord) {
	length := len(ckptsDoc.Checkpoint_records)
	maxCkptsRecordsToKeep := ckptsDoc.GetMaxCkptRecordsToKeep()
	if length > 0 {
		if !ckptsDoc.Checkpoint_records[0].SameAs(record) {
			if length > maxCkptsRecordsToKeep {
				for i := maxCkptsRecordsToKeep - 1; i < length; i++ {
					removedRecords = append(removedRecords, ckptsDoc.Checkpoint_records[i])
				}
				ckptsDoc.Checkpoint_records = ckptsDoc.Checkpoint_records[:maxCkptsRecordsToKeep]
			} else if length < maxCkptsRecordsToKeep {
				for i := length; i < maxCkptsRecordsToKeep; i++ {
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
	if len(ckptsDoc.Checkpoint_records) <= ckptsDoc.GetMaxCkptRecordsToRead() {
		return ckptsDoc.Checkpoint_records
	} else {
		return ckptsDoc.Checkpoint_records[:ckptsDoc.GetMaxCkptRecordsToRead()]
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

// SnappyCompress will take all the records in the checkpointsDoc and, if there are things
// that can be deduped into sha -> compressed bytes, then extract them out
func (c *CheckpointsDoc) SnappyCompress() ([]byte, ShaMappingCompressedMap, error) {
	marshalledBytes, err := json.Marshal(c)
	if err != nil {
		return nil, nil, err
	}

	snapShaMap := make(ShaMappingCompressedMap)
	records := c.GetCheckpointRecords()
	for _, record := range records {
		if record == nil {
			continue
		}

		brokenMap := record.BrokenMappings()
		if len(brokenMap) > 0 {
			err = brokenMap.CompressToShaCompressedMap(snapShaMap)
			if err != nil {
				return nil, nil, err
			}
		}

		if !record.IsTraditional() {
			err1 := record.GlobalTimestamp.CompressToShaCompresedMap(snapShaMap)
			err2 := record.GlobalCounters.CompressToShaCompresedMap(snapShaMap)
			if err1 != nil || err2 != nil {
				return nil, nil, fmt.Errorf("Failed to Compress GlobalInfo. GlobalTimestampErr=%v GlobalTargetCounterErr=%v", err1, err2)
			}
		}
	}

	return snappy.Encode(nil, marshalledBytes), snapShaMap, nil
}

func (c *CheckpointsDoc) SnappyDecompress(data []byte, brokenMap ShaToCollectionNamespaceMap, globalInfoShaMap ShaToGlobalInfoMap) error {
	if c == nil {
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

	if len(brokenMap) > 0 {
		records := c.GetCheckpointRecords()
		for _, record := range records {
			if record == nil {
				continue
			}

			err = record.LoadBrokenMapping(brokenMap)
			if err != nil {
				errMap[fmt.Sprintf("record.LoadBrokenMapping created at: %v", record.CreationTime)] = err
				continue
			}
		}
	}

	errMap2 := c.LoadGlobalInfoFromShaMap(globalInfoShaMap)

	base.MergeErrorMaps(errMap, errMap2, false)
	if len(errMap) > 0 {
		return fmt.Errorf(base.FlattenErrorMap(errMap))
	}
	return nil
}

func (c *CheckpointsDoc) LoadGlobalInfoFromShaMap(globalInfoShaMap ShaToGlobalInfoMap) base.ErrorMap {
	errMap := make(base.ErrorMap)
	if len(globalInfoShaMap) > 0 {
		for _, record := range c.GetCheckpointRecords() {
			if record == nil || record.IsTraditional() {
				continue
			}
			// Hanlde GlobalTimestamp
			if record.GlobalTimestampSha256 != "" {
				if gts, found := globalInfoShaMap[record.GlobalTimestampSha256]; found {
					record.GlobalTimestamp = *gts.(*GlobalTimestamp)
				} else {
					errMap[fmt.Sprintf("Global Timestamp sha %v", record.GlobalTimestampSha256)] = base.ErrorNotFound
				}
			}
			// Hanlde GlobalCounters
			if record.GlobalCountersSha256 != "" {
				if gts, found := globalInfoShaMap[record.GlobalCountersSha256]; found {
					record.GlobalCounters = *gts.(*GlobalTargetCounters)
				} else {
					errMap[fmt.Sprintf("Global Target Counter sha %v", record.GlobalTimestampSha256)] = base.ErrorNotFound
				}
			}
		}
	}
	if len(errMap) > 0 {
		var debugKeys []string
		for k, _ := range globalInfoShaMap {
			debugKeys = append(debugKeys, k)
		}
		errMap["globalTsShaMap has the following shas:"] = fmt.Errorf("%s", strings.Join(debugKeys, " "))
	}
	return errMap
}

func (c *CheckpointsDoc) IsTraditional() bool {
	if c == nil || len(c.Checkpoint_records) == 0 {
		return true // for legacy use case
	}

	// Checkpoints should not intermix - thus one is enough to check
	return c.Checkpoint_records[0].IsTraditional()
}

// contains compressed mappings of both GlobalTimestamps and GlobalTargetCounters
type GlobalInfoCompressedDoc CompressedMappings

func (g *GlobalInfoCompressedDoc) SameAs(other *GlobalInfoCompressedDoc) bool {
	return (*CompressedMappings)(g).SameAs((*CompressedMappings)(other))
}

func (g *GlobalInfoCompressedDoc) ToShaMap() (ShaToGlobalInfoMap, error) {
	if g == nil {
		return nil, fmt.Errorf("calling ToShaMap() on a nil GlobalTimestampCompresedDoc")
	}

	errorMap := make(base.ErrorMap)
	shaMap := make(ShaToGlobalInfoMap)
	for _, oneRecord := range g.NsMappingRecords {
		if oneRecord == nil {
			continue
		}

		// Decompress and populate GlobalInfoMetaObj
		gInfoMetaObj := &GlobalInfoMetaObj{}
		err := gInfoMetaObj.SnappyDecompress(oneRecord.CompressedMapping)
		if err != nil {
			if err != ErrorGlobalInfoWrongType {
				errorMap[oneRecord.Sha256Digest] = err
			}
			continue
		}

		// Sanity check
		checkSha, err := gInfoMetaObj.Data.Sha256()
		if err != nil {
			errorMap[oneRecord.Sha256Digest] = fmt.Errorf("validing SHA failed %v", err)
			continue
		}
		checkShaString := fmt.Sprintf("%x", checkSha[:])
		if checkShaString != oneRecord.Sha256Digest {
			errorMap[oneRecord.Sha256Digest] = fmt.Errorf("SHA validation mismatch %v", checkShaString)
			continue
		}

		shaMap[oneRecord.Sha256Digest] = gInfoMetaObj.Data
	}

	var err error
	if len(errorMap) > 0 {
		errStr := base.FlattenErrorMap(errorMap)
		err = fmt.Errorf(errStr)
	}
	return shaMap, err

}

func (g *GlobalInfoCompressedDoc) LoadShaMap(shaMap ShaToGlobalInfoMap) error {
	if g == nil {
		return base.ErrorNilPtr
	}
	// MUST cast as a pointer otherwise the LoadShaMap below will be done on a copy
	compressedMapPtr := (*CompressedMappings)(g)
	return compressedMapPtr.LoadShaMap(shaMap.ToSnappyCompressableMap())
}

func (g *GlobalInfoCompressedDoc) LoadCompressedShaMap(shaMap ShaMappingCompressedMap) error {
	if g == nil {
		return base.ErrorNilPtr
	}
	// MUST cast as a pointer otherwise the LoadShaMap below will be done on a copy
	compressedMapPtr := (*CompressedMappings)(g)
	return compressedMapPtr.LoadCompressedShaMap(shaMap)
}

func (g *GlobalInfoCompressedDoc) UniqueAppend(other *GlobalInfoCompressedDoc) error {
	if g == nil {
		return base.ErrorNilPtr
	}
	// MUST cast as a pointer otherwise the action below will be done on a copy
	compressedMapPtr := (*CompressedMappings)(g)
	otherPtr := (*CompressedMappings)(other)
	return compressedMapPtr.UniqueAppend(otherPtr)
}

// For unit test
func GenerateGlobalVBsCkptDocMap(vbsList []uint16, brokenMappingShaToInsert string) VBsCkptsDocMap {
	retMap := make(VBsCkptsDocMap)
	for _, vb := range vbsList {
		oneGts := GlobalTimestamp{}
		oneGctrs := GlobalTargetCounters{}
		for i := 0; i < base.NumberOfVbs; i++ {
			oneGts[uint16(i)] = &GlobalVBTimestamp{
				TargetVBTimestamp{
					Target_vb_opaque: &TargetVBUuid{
						// Note that if the UUID is > Maxuint32, it could be subjected to loss of precision
						// by doing this type of conversion.
						// We technically haven't seen a CBSE yet... but it could be an issue potentially...
						// Do the same for all uint64's
						Target_vb_uuid: uint64(rand.Uint32()),
					},
					Target_Seqno:        uint64(rand.Uint32()),
					BrokenMappingSha256: brokenMappingShaToInsert,
				},
			}
			oneGctrs[uint16(i)] = &TargetPerVBCounters{}
		}

		oneRecord := &CheckpointRecord{
			CreationTime: uint64(rand.Uint32()),
			SourceVBTimestamp: SourceVBTimestamp{
				Seqno:         uint64(rand.Uint32()),
				Failover_uuid: rand.Uint64(),
			},
			GlobalTimestamp: oneGts,
			GlobalCounters:  oneGctrs,
		}
		oneRecord.PopulateShasForGlobalInfo()
		oneDoc := &CheckpointsDoc{
			Checkpoint_records: CheckpointRecordsList{oneRecord},
		}
		retMap[vb] = oneDoc
	}
	return retMap
}
