// +build !pcre

/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software
will be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadata_svc

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	service_def_real "github.com/couchbase/goxdcr/service_def"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
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

func setupCkptSvcMocks(metadataSvc *service_def.MetadataSvc, ctx *log.LoggerContext, metadataEntry []*service_def_real.MetadataEntry, brokenMappingMarshalled []byte, newMapMarshalled []byte, replSpecSvc *service_def.ReplicationSpecSvc) {
	metadataSvc.On("GetAllMetadataFromCatalog", fmt.Sprintf("%v/%v", CheckpointsCatalogKeyPrefix, replId)).Return(metadataEntry, nil)
	metadataSvc.On("Get", fmt.Sprintf("%v/%v/%v", CheckpointsCatalogKeyPrefix, replId, BrokenMappingKey)).Return(brokenMappingMarshalled, nil, nil)
	metadataSvc.On("Set", fmt.Sprintf("%v/%v/%v", CheckpointsCatalogKeyPrefix, replId, BrokenMappingKey), newMapMarshalled, mock.Anything).Return(nil)

	dummyMap := make(map[string]*metadata.ReplicationSpecification)
	dummyMap[replId] = nil
	replSpecSvc.On("AllReplicationSpecs").Return(dummyMap, nil)
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
		BrokenMappingSha256: fmt.Sprintf("%x", shaSlice[:]),
	}
	entries := getEntries(ckptRecord, err, assert)

	marshalledDoc := getbrokenMapUnmarshalledDoc(brokenMapCompressedMap, shaSlice, err, assert)

	// New map should be just: SOURCE ||Scope: S2 Collection: C2|| -> TARGET(s) |Scope: S2T Collection: C2T|
	newMap, _, _, upsertMappingDocSlice := getUpsertMap()
	newMapShaSlice, _ := newMap.Sha256()
	newMapSha := fmt.Sprintf("%x", newMapShaSlice)
	assert.Equal("67d24325ed5df4d1f04c425606b4d40575032663de206e49a76033ced5dc15ee", newMapSha)

	setupCkptSvcMocks(metadataSvc, loggerCtx, entries, marshalledDoc, upsertMappingDocSlice, replSpecSvc)

	ckptSvc, err := NewCheckpointsService(metadataSvc, loggerCtx, nil, replSpecSvc)
	assert.NotNil(ckptSvc)
	assert.Nil(err)

	docs, err := ckptSvc.CheckpointsDocs(replId, true)
	assert.Nil(err)
	assert.Equal(1, len(docs))

	for _, doc := range docs {
		assert.Equal(1, len(doc.Checkpoint_records))
		brokenMappingFromRecord := doc.Checkpoint_records[0].BrokenMappings()
		assert.Equal(2, len(*brokenMappingFromRecord))
	}

	counter, exists := ckptSvc.ShaRefCounterService.topicMaps[replId]
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

func getUpsertMap() (*metadata.CollectionNamespaceMapping, []byte, *metadata.CollectionNsMappingsDoc, []byte) {
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
	marshalledMap, _ := json.Marshal(&newUpsertingMap)
	shaSlice, _ := newUpsertingMap.Sha256()

	compressedMap, err := newUpsertingMap.ToSnappyCompressed()
	if err != nil {
		panic(err.Error())
	}
	mappingRecord := metadata.CompressedColNamespaceMapping{
		CompressedMapping: compressedMap,
		Sha256Digest:      fmt.Sprintf("%x", shaSlice[:]),
	}
	var mappingRecords metadata.CompressedColNamespaceMappingList
	mappingRecords.SortedInsert(&mappingRecord)
	mappingDoc := &metadata.CollectionNsMappingsDoc{
		NsMappingRecords: mappingRecords,
		SpecInternalId:   internalId,
	}
	mappingsDocMarshalled, err := json.Marshal(mappingDoc)
	if err != nil {
		panic(err.Error())
	}
	return &newUpsertingMap, marshalledMap, mappingDoc, mappingsDocMarshalled
}

func getbrokenMapUnmarshalledDoc(compressedMap []byte, shaSlice [32]byte, err error, assert *assert.Assertions) []byte {
	compressedNamespaceMapping := &metadata.CompressedColNamespaceMapping{
		CompressedMapping: compressedMap,
		Sha256Digest:      fmt.Sprintf("%x", shaSlice[:]),
	}
	var compressedList metadata.CompressedColNamespaceMappingList
	compressedList = append(compressedList, compressedNamespaceMapping)
	brokenMappingDoc := &metadata.CollectionNsMappingsDoc{
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
