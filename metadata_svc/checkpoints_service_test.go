//go:build !pcre
// +build !pcre

/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadata_svc

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	service_def_real "github.com/couchbase/goxdcr/v8/service_def"
	service_def "github.com/couchbase/goxdcr/v8/service_def/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func setupCkptSvcBoilerPlate() (*service_def.MetadataSvc, *log.LoggerContext, *service_def.ReplicationSpecSvc) {
	metadataSvc := &service_def.MetadataSvc{}
	loggerCtx := &log.LoggerContext{
		Log_writers: nil,
		Log_level:   0,
	}
	replSpecSvc := &service_def.ReplicationSpecSvc{}
	return metadataSvc, loggerCtx, replSpecSvc
}

var timeAfterDelAllFromCatalog int64
var timeAfterGet int64

func setupCkptSvcMocks(metadataSvc *service_def.MetadataSvc, ctx *log.LoggerContext, metadataEntry []*service_def_real.MetadataEntry, brokenMappingMarshalled []byte, newMapMarshalled []byte, replSpecSvc *service_def.ReplicationSpecSvc, opMap map[string]error, delayMap map[string]time.Duration, globalTsDocMarshalled, globaTsNewUpsert []byte) {

	metadataSvc.On("GetAllMetadataFromCatalog", fmt.Sprintf("%v/%v", CheckpointsCatalogKeyPrefix, replId)).Return(metadataEntry, opMap["GetAllMetadataFromCatalog"])
	metadataSvc.On("DelAllFromCatalog", fmt.Sprintf("%v/%v", CheckpointsCatalogKeyPrefix, replId)).Run(func(arg mock.Arguments) {
		if delayTime, exists := delayMap["DelAllFromCatalog"]; exists {
			fmt.Printf("Sleeping %v to simulate slow DelAllFromCatalog\n", delayTime)
			time.Sleep(delayTime)
		}
		timeInUint := time.Now().UnixMicro()
		atomic.StoreInt64(&timeAfterDelAllFromCatalog, timeInUint)

	}).Return(opMap["DelAllFromCatalog"])
	metadataSvc.On("Get", fmt.Sprintf("%v/%v/%v", CheckpointsCatalogKeyPrefix, replId, BrokenMappingKey)).Run(func(args mock.Arguments) {
		timeInUint := time.Now().UnixMicro()
		atomic.StoreInt64(&timeAfterGet, timeInUint)
	}).Return(brokenMappingMarshalled, nil, opMap["Get"])
	metadataSvc.On("Set", fmt.Sprintf("%v/%v/%v", CheckpointsCatalogKeyPrefix, replId, BrokenMappingKey), newMapMarshalled, mock.Anything).Return(opMap["Set"])
	metadataSvc.On("Del", fmt.Sprintf("%v/%v/%v", CheckpointsCatalogKeyPrefix, replId, BrokenMappingKey), mock.Anything).Return(opMap["Del"])
	metadataSvc.On("Add", fmt.Sprintf("%v/%v/%v", CheckpointsCatalogKeyPrefix, replId, BrokenMappingKey), mock.Anything).Return(opMap["Add"])
	metadataSvc.On("Set", fmt.Sprintf("%v/%v/%v", CheckpointsCatalogKeyPrefix, replId, GlobalTimestampKey), newMapMarshalled, mock.Anything).Return(opMap["Set"])
	metadataSvc.On("Del", fmt.Sprintf("%v/%v/%v", CheckpointsCatalogKeyPrefix, replId, GlobalTimestampKey), mock.Anything).Return(opMap["Del"])
	metadataSvc.On("Add", fmt.Sprintf("%v/%v/%v", CheckpointsCatalogKeyPrefix, replId, GlobalTimestampKey), mock.Anything).Return(opMap["Add"])

	metadataSvc.On("Get", fmt.Sprintf("%v/%v/%v", CheckpointsCatalogKeyPrefix, replId, GlobalTimestampKey)).Run(func(args mock.Arguments) {
		timeInUint := time.Now().UnixMicro()
		atomic.StoreInt64(&timeAfterGet, timeInUint)
	}).Return(globalTsDocMarshalled, nil, opMap["Get"])
	metadataSvc.On("Set", fmt.Sprintf("%v/%v/%v", CheckpointsCatalogKeyPrefix, replId, GlobalTimestampKey), globaTsNewUpsert, mock.Anything).Return(opMap["Set"])

	dummyMap := make(map[string]*metadata.ReplicationSpecification)
	dummyRepl := &metadata.ReplicationSpecification{Id: replId, InternalId: internalId}
	dummyMap[replId] = dummyRepl
	replSpecSvc.On("AllReplicationSpecs").Return(dummyMap, nil)
	replSpecSvc.On("ReplicationSpec", replId).Return(dummyRepl, nil)
}

const replId = "testReplId"
const internalId = "testInternalId"

func TestCkptSvcRemoveSourceMapping(t *testing.T) {
	fmt.Println("============== Test case start: TestCkptSvcRemoveSourceMapping =================")
	defer fmt.Println("============== Test case end: TestCkptSvcRemoveSourceMapping =================")
	assert := assert.New(t)

	metadataSvc, loggerCtx, replSpecSvc := setupCkptSvcBoilerPlate()

	// This test will start with two mappings
	// S1:C1 -> S1T:C1T
	// S2:C2 -> S2T:C2T
	brokenMap := generateBrokenMap()
	shaSlice, err := brokenMap.Sha256()
	assert.Nil(err)
	brokenMapCompressedMap, err := brokenMap.ToSnappyCompressed()
	assert.Nil(err)

	ckptRecord := metadata.CheckpointRecord{
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			BrokenMappingSha256: fmt.Sprintf("%x", shaSlice[:]),
			Target_vb_opaque:    &metadata.TargetVBUuidAndTimestamp{},
		},
	}
	entries := getEntries(ckptRecord, err, assert)

	marshalledDoc := getCompressedMapUnmarshalledDoc(brokenMapCompressedMap, shaSlice, err, assert)

	gts := generateGlobalTimestampMap()
	shaSlice, err = gts.Sha256()
	assert.Nil(err)
	gtsCompressed, err := gts.ToSnappyCompressed()
	assert.Nil(err)
	gtsMarshalledDoc := getCompressedMapUnmarshalledDoc(gtsCompressed, shaSlice, err, assert)

	// New map should be just: SOURCE ||Scope: S2 Collection: C2|| -> TARGET(s) |Scope: S2T Collection: C2T|
	newMap := generateUpsertMap()
	_, _, upsertMappingDocSlice := getUpsertMap(newMap)
	newMapShaSlice, _ := newMap.Sha256()
	newMapSha := fmt.Sprintf("%x", newMapShaSlice)
	assert.Equal("67d24325ed5df4d1f04c425606b4d40575032663de206e49a76033ced5dc15ee", newMapSha)

	metadataSvcOpMap := make(map[string]error)
	metadataSvcOpMap["Del"] = nil
	metadataSvcOpMap["Get"] = nil
	metadataSvcOpMap["Set"] = nil
	metadataSvcOpMap["DelAllFromCatalog"] = nil
	metadataSvcOpMap["GetAllMetadataFromCatalog"] = nil

	metadataSvcDelayMap := make(map[string]time.Duration)

	// We feed it an "non-nil but empty" sha map but the global checkpoint is not in use
	// We should anticipate the sha ref counter service to strip all and store an empty doc
	emptyDoc := &metadata.CompressedMappings{}
	emptyDoc.SpecInternalId = internalId
	emptyDocMarshalled, err := json.Marshal(emptyDoc)
	assert.Nil(err)

	setupCkptSvcMocks(metadataSvc, loggerCtx, entries, marshalledDoc, upsertMappingDocSlice, replSpecSvc, metadataSvcOpMap, metadataSvcDelayMap, gtsMarshalledDoc, emptyDocMarshalled)

	ckptSvc, err := NewCheckpointsService(metadataSvc, loggerCtx, nil, replSpecSvc)
	assert.NotNil(ckptSvc)
	assert.Nil(err)

	docs, err := ckptSvc.CheckpointsDocs(replId, true)
	assert.Nil(err)
	assert.Equal(1, len(docs))

	for _, doc := range docs {
		assert.Equal(1, len(doc.Checkpoint_records))
		brokenMappingsFromRecord := doc.Checkpoint_records[0].BrokenMappings()
		assert.Len(brokenMappingsFromRecord, 1)
		var brokenMappingFromRecord *metadata.CollectionNamespaceMapping
		for _, v := range brokenMappingsFromRecord {
			brokenMappingFromRecord = v
		}
		assert.Equal(2, len(*brokenMappingFromRecord))
	}

	counter, exists := ckptSvc.brokenMapRefCountSvc.topicMaps[replId]
	assert.True(exists)
	assert.NotNil(counter)
	assert.Equal(1, len(counter.shaToMapping)) // representing the original 2-entries brokenmap
	var origSha string
	for _, v := range counter.shaToMapping {
		sha, _ := v.Sha256()
		origSha = fmt.Sprintf("%x", sha[:])
	}
	assert.NotEqual("", origSha)

	// Remove S1:C1
	collectionsMap := make(metadata.CollectionsMap)
	collectionsMap["C1"] = metadata.Collection{
		Uid:  1,
		Name: "C1",
	}
	scopeMap := make(metadata.ScopesMap)
	scopeMap["S1"] = metadata.Scope{
		Uid:         0,
		Name:        "S1",
		Collections: collectionsMap,
	}
	changed, err := ckptSvc.removeMappingFromCkptDocs(replId, internalId, scopeMap)
	assert.True(changed)
	assert.Nil(err)
}

func generateUpsertMap() metadata.CollectionNamespaceMapping {
	newUpsertingMap := make(metadata.CollectionNamespaceMapping)
	src2 := &base.CollectionNamespace{
		ScopeName:      "S2",
		CollectionName: "C2",
	}
	tgt2 := &base.CollectionNamespace{
		ScopeName:      "S2T",
		CollectionName: "C2T",
	}
	newUpsertingMap.AddSingleMapping(src2, tgt2)
	return newUpsertingMap
}

func getUpsertMap(newUpsertingMap metadata.CollectionNamespaceMapping) ([]byte, *metadata.CollectionNsMappingsDoc, []byte) {
	marshalledMap, _ := json.Marshal(&newUpsertingMap)
	shaSlice, _ := newUpsertingMap.Sha256()

	compressedMap, err := newUpsertingMap.ToSnappyCompressed()
	if err != nil {
		panic(err.Error())
	}
	mappingRecord := metadata.CompressedShaMapping{
		CompressedMapping: compressedMap,
		Sha256Digest:      fmt.Sprintf("%x", shaSlice[:]),
	}
	var mappingRecords metadata.CompressedShaMappingList
	if len(newUpsertingMap) > 0 {
		mappingRecords.SortedInsert(&mappingRecord)
	}
	mappingDoc := &metadata.CollectionNsMappingsDoc{
		NsMappingRecords: mappingRecords,
		SpecInternalId:   internalId,
	}
	mappingsDocMarshalled, err := json.Marshal(mappingDoc)
	if err != nil {
		panic(err.Error())
	}
	return marshalledMap, mappingDoc, mappingsDocMarshalled
}

func getCompressedMapUnmarshalledDoc(compressedMap []byte, shaSlice [32]byte, err error, assert *assert.Assertions) []byte {
	compressedNamespaceMapping := &metadata.CompressedShaMapping{
		CompressedMapping: compressedMap,
		Sha256Digest:      fmt.Sprintf("%x", shaSlice[:]),
	}
	var compressedList metadata.CompressedShaMappingList
	compressedList = append(compressedList, compressedNamespaceMapping)
	brokenMappingDoc := &metadata.CollectionNsMappingsDoc{
		SpecInternalId:   internalId,
		NsMappingRecords: compressedList,
	}
	marshalledDoc, err := json.Marshal(brokenMappingDoc)
	assert.Nil(err)
	return marshalledDoc
}

func getEntries(ckptRecord metadata.CheckpointRecord, err error, assert *assert.Assertions) []*service_def_real.MetadataEntry {
	var records []*metadata.CheckpointRecord
	var entries []*service_def_real.MetadataEntry
	records = append(records, &ckptRecord)
	vb12Doc := metadata.CheckpointsDoc{
		Checkpoint_records: records,
		Revision:           nil,
	}
	vb12Val, err := json.Marshal(&vb12Doc)
	assert.Nil(err)

	entry := service_def_real.MetadataEntry{
		Key:   "/12",
		Value: vb12Val,
		Rev:   nil,
	}
	entries = append(entries, &entry)
	return entries
}

func generateBrokenMap() metadata.CollectionNamespaceMapping {
	brokenMap := make(metadata.CollectionNamespaceMapping)
	src1 := &base.CollectionNamespace{
		ScopeName:      "S1",
		CollectionName: "C1",
	}
	src2 := &base.CollectionNamespace{
		ScopeName:      "S2",
		CollectionName: "C2",
	}
	tgt1 := &base.CollectionNamespace{
		ScopeName:      "S1T",
		CollectionName: "C1T",
	}
	tgt2 := &base.CollectionNamespace{
		ScopeName:      "S2T",
		CollectionName: "C2T",
	}
	brokenMap.AddSingleMapping(src1, tgt1)
	brokenMap.AddSingleMapping(src2, tgt2)
	return brokenMap
}

func generateGlobalTimestampMap() metadata.GlobalTimestamp {
	gts := make(metadata.GlobalTimestamp)
	return gts
}

func TestCkptSvcConcurrentRemAndCreate(t *testing.T) {
	fmt.Println("============== Test case start: TestCkptSvcConcurrentRemAndCreate =================")
	defer fmt.Println("============== Test case end: TestCkptSvcConcurrentRemAndCreate =================")
	assert := assert.New(t)

	metadataSvc, loggerCtx, replSpecSvc := setupCkptSvcBoilerPlate()

	// This test will start with two mappings
	// S1:C1 -> S1T:C1T
	// S2:C2 -> S2T:C2T
	brokenMap := generateBrokenMap()
	shaSlice, err := brokenMap.Sha256()
	assert.Nil(err)
	brokenMapCompressedMap, err := brokenMap.ToSnappyCompressed()
	assert.Nil(err)

	ckptRecord := metadata.CheckpointRecord{
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			BrokenMappingSha256: fmt.Sprintf("%x", shaSlice[:]),
			Target_vb_opaque:    &metadata.TargetVBUuidAndTimestamp{},
		},
	}
	entries := getEntries(ckptRecord, err, assert)

	marshalledDoc := getCompressedMapUnmarshalledDoc(brokenMapCompressedMap, shaSlice, err, assert)

	// New map should be just: SOURCE ||Scope: S2 Collection: C2|| -> TARGET(s) |Scope: S2T Collection: C2T|
	newMap := generateUpsertMap()
	_, _, upsertMappingDocSlice := getUpsertMap(newMap)
	newMapShaSlice, _ := newMap.Sha256()
	newMapSha := fmt.Sprintf("%x", newMapShaSlice)
	assert.Equal("67d24325ed5df4d1f04c425606b4d40575032663de206e49a76033ced5dc15ee", newMapSha)

	metadataSvcOpMap := make(map[string]error)
	metadataSvcOpMap["Del"] = nil
	metadataSvcOpMap["Get"] = nil
	metadataSvcOpMap["Set"] = nil
	metadataSvcOpMap["Add"] = nil
	metadataSvcOpMap["DelAllFromCatalog"] = nil
	metadataSvcOpMap["GetAllMetadataFromCatalog"] = nil

	metadataSvcDelayMap := make(map[string]time.Duration)
	metadataSvcDelayMap["DelAllFromCatalog"] = 3 * time.Second

	gts := generateGlobalTimestampMap()
	shaSlice, err = gts.Sha256()
	assert.Nil(err)
	gtsCompressed, err := gts.ToSnappyCompressed()
	assert.Nil(err)
	gtsMarshalledDoc := getCompressedMapUnmarshalledDoc(gtsCompressed, shaSlice, err, assert)

	// We feed it an "non-nil but empty" sha map but the global checkpoint is not in use
	// We should anticipate the sha ref counter service to strip all and store an empty doc
	emptyDoc := &metadata.CompressedMappings{}
	emptyDoc.SpecInternalId = internalId
	emptyDocMarshalled, err := json.Marshal(emptyDoc)
	assert.Nil(err)

	setupCkptSvcMocks(metadataSvc, loggerCtx, entries, marshalledDoc, upsertMappingDocSlice, replSpecSvc, metadataSvcOpMap, metadataSvcDelayMap, gtsMarshalledDoc, emptyDocMarshalled)

	ckptSvc, err := NewCheckpointsService(metadataSvc, loggerCtx, testUtils, replSpecSvc)
	assert.NotNil(ckptSvc)
	assert.Nil(err)

	// In CBSE, we see that DelCheckpointsDocs could potentially be delayed
	// DelCheckpointsDocs launches 2 go-routines, one on metadata_svc.DelAllFromCatalog
	// and the other is ckpt_svc.CleanupMapping
	// It is possible that the metadata_svc.DelAll is a very slow process, and that CleanupMapping finishes
	// and as it finishes, it removes the counter/topic from the shaRefCounterService's topicMaps
	// But, this call is still executing...
	// In the meantime, p2p merge is calling and it calls "loadBrokenMappingsInternal"
	//
	// vvv
	//  	alreadyExists := ckpt_svc.InitTopicShaCounterWithInternalId(replicationId, "")
	//
	//	mappingsDoc, err := ckpt_svc.GetMappingsDoc(replicationId, !alreadyExists /*initIfNotFound*/)
	//
	// This call will cause a new counter to be re-established
	// But, the metakv still hasn't deleted it yet
	// So, the GetMappingsDoc will *not* create a new mappings doc, and a counter will have been established
	// This breaks the assumption that "When a counter is first created, it should also create a new mappingdoc"

	// We now do two concurrent things - we have a checkpoint delete that happens with a delay
	// with a checkpoint push that happens soon after the delete
	var waitGrp sync.WaitGroup
	waitGrp.Add(1)
	go func() {
		assert.Nil(ckptSvc.DelCheckpointsDocs(replId))
		waitGrp.Done()
	}()

	time.Sleep(1 * time.Second)

	waitGrp.Add(1)
	go func() {
		// In the meantime, launch a load op to re-create the counter
		//ckptSvc.loadBrokenMappingsInternal(replId)
		ckptSvc.CheckpointsDocs(replId, true)
		waitGrp.Done()
	}()

	waitGrp.Wait()

	// The Checkpoint Delete should have blocked the loadBrokenMapping from recreating a counter
	// Very raw way to verify
	assert.True(timeAfterGet > timeAfterDelAllFromCatalog)
}

func TestCkptSvcLoadGlobalTimestamp(t *testing.T) {
	fmt.Println("============== Test case start: TestCkptSvcLoadGlobalTimestamp =================")
	defer fmt.Println("============== Test case end: TestCkptSvcLoadGlobalTimestamp =================")
	assert := assert.New(t)

	metadataSvc, loggerCtx, replSpecSvc := setupCkptSvcBoilerPlate()

	metadataSvcOpMap := make(map[string]error)
	metadataSvcOpMap["Del"] = nil
	metadataSvcOpMap["Get"] = nil
	metadataSvcOpMap["Set"] = nil
	metadataSvcOpMap["Add"] = nil
	metadataSvcOpMap["DelAllFromCatalog"] = nil
	metadataSvcOpMap["GetAllMetadataFromCatalog"] = nil

	metadataSvcDelayMap := make(map[string]time.Duration)
	metadataSvcDelayMap["DelAllFromCatalog"] = 3 * time.Second

	gts := generateGlobalTimestampMap()
	gts[0] = &metadata.GlobalVBTimestamp{
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			Target_Seqno:     1,
			Target_vb_opaque: &metadata.TargetVBUuid{345},
		},
	}
	gts[1] = &metadata.GlobalVBTimestamp{
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			Target_Seqno:     2,
			Target_vb_opaque: &metadata.TargetVBUuid{456},
		},
	}

	marshalOut, err := json.Marshal(gts)
	assert.Nil(err)

	unmarshalTest := generateGlobalTimestampMap()
	var fieldMap map[string]interface{}
	assert.Nil(json.Unmarshal(marshalOut, &fieldMap))
	assert.Nil(unmarshalTest.LoadUnmarshalled(fieldMap))

	shaSlice, err := gts.Sha256()
	assert.Nil(err)
	gtsCompressed, err := gts.ToSnappyCompressed()
	assert.Nil(err)
	gtsMarshalledDoc := getCompressedMapUnmarshalledDoc(gtsCompressed, shaSlice, err, assert)
	sha256String := fmt.Sprintf("%x", shaSlice[:])

	// The checkpoint record to return where the global sha is in use and its members are empty
	// The test should expect that the global timestamps are populated automatically upon reading both
	// the sha map entry and the checkpoint itself
	ckptRecord := metadata.CheckpointRecord{
		GlobalTimestampSha256: sha256String,
	}
	entries := getEntries(ckptRecord, err, assert)
	emptyBrokenMap := make(metadata.CollectionNamespaceMapping)
	marshalledBytes, _, newMapMarshalled := getUpsertMap(emptyBrokenMap)

	setupCkptSvcMocks(metadataSvc, loggerCtx, entries, marshalledBytes, newMapMarshalled, replSpecSvc, metadataSvcOpMap, metadataSvcDelayMap, gtsMarshalledDoc, gtsMarshalledDoc)

	ckptSvc, err := NewCheckpointsService(metadataSvc, loggerCtx, testUtils, replSpecSvc)
	assert.NotNil(ckptSvc)
	assert.Nil(err)

	// The service should load the ckpt docs
	vbCkptDocs, err := ckptSvc.CheckpointsDocs(replId, true)
	assert.Nil(err)

	// The metakv mocks here in this test will hard-code vb12 as the key to return
	vb12Docs := vbCkptDocs[12]
	assert.NotNil(vb12Docs)
	assert.False(vb12Docs.IsTraditional())
	assert.Len(vb12Docs.Checkpoint_records, 1)

	record := vb12Docs.Checkpoint_records[0]
	gtsCheck := record.GlobalTimestamp
	assert.Len(gts, 2)
	assert.Equal(uint64(1), gtsCheck[0].Target_Seqno)
	assert.Equal(uint64(2), gtsCheck[1].Target_Seqno)
}
