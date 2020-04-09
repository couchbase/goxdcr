// Copyright (c) 2013-2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package metadata_svc

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	serviceDefReal "github.com/couchbase/goxdcr/service_def"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	utilsReal "github.com/couchbase/goxdcr/utils"
	utilsMock "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"testing"
)

func setupBoilerPlateBRS() (*service_def.UILogSvc,
	*service_def.MetadataSvc,
	*service_def.XDCRCompTopologySvc,
	*service_def.ClusterInfoSvc,
	*utilsMock.UtilsIface,
	*service_def.ReplicationSpecSvc) {

	uiLogSvcMock := &service_def.UILogSvc{}
	metadataSvcMock := &service_def.MetadataSvc{}
	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	clusterInfoSvcMock := &service_def.ClusterInfoSvc{}
	utilitiesMock := &utilsMock.UtilsIface{}
	replSpecSvcMock := &service_def.ReplicationSpecSvc{}

	return uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock, utilitiesMock, replSpecSvcMock
}

func setupMocksBRS(uiLogSvc *service_def.UILogSvc,
	metadataSvc *service_def.MetadataSvc,
	xdcrTopologySvc *service_def.XDCRCompTopologySvc,
	clusterInfoSvc *service_def.ClusterInfoSvc,
	utilsIn *utilsMock.UtilsIface,
	replSpecSvc *service_def.ReplicationSpecSvc,
	startupMetadataEntries GetAllMetadataFromCatalogMockFunc,
	replSpecMap map[string]*metadata.ReplicationSpecification /* key: specId to actual spec */) {

	metadataSvc.On("GetAllMetadataFromCatalog", mock.Anything).Return(startupMetadataEntries(), nil)
	// This is cool - it is actually returning the data we are feeding it back
	utilsIn.On("ExponentialBackoffExecutor", "GetAllMetadataFromCatalogBackfillReplicationSpec", mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Run(func(args mock.Arguments) { (args.Get(4)).(utilsReal.ExponentialOpFunc)() }).Return(nil)

	for specName, actualSpec := range replSpecMap {
		replSpecSvc.On("ReplicationSpec", specName).Return(actualSpec, nil)
	}

	oneNodeAllVBs := make(map[string][]uint16)
	var allVBs []uint16
	var i uint16
	for i = 0; i < 1024; i++ {
		allVBs = append(allVBs, i)
	}
	oneNodeAllVBs["localhost"] = allVBs
	clusterInfoSvc.On("GetLocalServerVBucketsMap", mock.Anything, mock.Anything).Return(oneNodeAllVBs, nil)
	var kvNodes []string
	kvNodes = append(kvNodes, "localhost")
	xdcrTopologySvc.On("MyKVNodes").Return(kvNodes, nil)
}

type GetAllMetadataFromCatalogMockFunc func() []*serviceDefReal.MetadataEntry

func NewBackfillReplTestSvc(uiLogSvc *service_def.UILogSvc,
	metadataSvc *service_def.MetadataSvc,
	utilsIn *utilsMock.UtilsIface,
	replSpecSvc *service_def.ReplicationSpecSvc,
	clusterInfoSvc *service_def.ClusterInfoSvc,
	xdcrTopologySvc *service_def.XDCRCompTopologySvc) *BackfillReplicationService {

	svc, _ := NewBackfillReplicationService(uiLogSvc, metadataSvc, log.DefaultLoggerContext,
		utilsIn, replSpecSvc, clusterInfoSvc, xdcrTopologySvc)

	return svc
}

func constructBackfillRevSlice(id, internalId string) []byte {
	backfillSpec := metadata.NewBackfillReplicationSpec(id, internalId, constructDummyTasksMap(), nil)
	slice, err := json.Marshal(backfillSpec)
	if err != nil {
		panic(fmt.Sprintf("%v", err.Error()))
	}
	return slice
}

func constructDummyTasksMap() metadata.VBTasksMapType {
	namespaceMapping := make(metadata.CollectionNamespaceMapping)
	defaultNamespace := &base.CollectionNamespace{base.DefaultScopeCollectionName, base.DefaultScopeCollectionName}
	namespaceMapping.AddSingleMapping(defaultNamespace, defaultNamespace)

	manifestsIdPair := base.CollectionsManifestIdPair{0, 0}
	ts0 := &metadata.BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 5000, 500, 500, manifestsIdPair},
	}

	vb0Task0 := metadata.NewBackfillTask(ts0, namespaceMapping)

	ts1 := &metadata.BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5005, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 15005, 500, 500, manifestsIdPair},
	}
	vb0Task1 := metadata.NewBackfillTask(ts1, namespaceMapping)

	var vb0Tasks metadata.BackfillTasks
	vb0Tasks = append(vb0Tasks, vb0Task0)
	vb0Tasks = append(vb0Tasks, vb0Task1)

	ts2 := &metadata.BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{1, 0, 5, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{1, 0, 5000, 500, 500, manifestsIdPair},
	}
	vb1Task0 := metadata.NewBackfillTask(ts2, namespaceMapping)

	var vb1Tasks metadata.BackfillTasks
	vb1Tasks = append(vb1Tasks, vb1Task0)

	vbTasksMap := make(map[uint16]*metadata.BackfillTasks)
	vbTasksMap[0] = &vb0Tasks
	vbTasksMap[1] = &vb1Tasks

	return vbTasksMap
}

func createValidateCollectionNsMappingsDocBytes(internalId string) []byte {
	// As part of adding, upsert should be done for all the VBtaskmaps
	dummyVBTaskMap := constructDummyTasksMap()
	shaToColMap := make(metadata.ShaToCollectionNamespaceMap)
	for _, tasks := range dummyVBTaskMap {
		oneMap := tasks.GetAllCollectionNamespaceMappings()
		for sha, mapping := range oneMap {
			shaToColMap[sha] = mapping
		}
	}
	validateDoc := &metadata.CollectionNsMappingsDoc{SpecInternalId: internalId}
	err := validateDoc.LoadShaMap(shaToColMap)
	if err != nil {
		panic("Coding validateDoc err")
	}
	validateDocDataBytes, err := json.Marshal(validateDoc)
	if err != nil {
		panic("Marshal validateDoc err")
	}
	return validateDocDataBytes
}

// Test Adding a brand new backfill repl service
func setupMetakvStartupLoadingExistingOne(metadataSvc *service_def.MetadataSvc, specId, internalId string) {
	docBytes := createValidateCollectionNsMappingsDocBytes(internalId)
	backfillMappingsKey := getBackfillMappingsDocKeyFunc(specId)
	metadataSvc.On("Get", backfillMappingsKey).Return(docBytes, nil, nil)
}

func TestBackfillReplSvc(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillReplSvc =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock, utilitiesMock, replSpecSvcMock := setupBoilerPlateBRS()

	specName := "testSpec"
	randInternalId, _ := base.GenerateRandomId(base.LengthOfRandomId, base.MaxRetryForRandomIdGeneration)
	metadataEntriesFunc := func() []*serviceDefReal.MetadataEntry {
		var entries []*serviceDefReal.MetadataEntry
		dummyEntry := &serviceDefReal.MetadataEntry{
			Key:   fmt.Sprintf("%v/%v", BackfillParentCatalogKey, specName),
			Value: constructBackfillRevSlice(specName, randInternalId),
		}
		entries = append(entries, dummyEntry)
		return entries
	}
	dummySpec := &metadata.ReplicationSpecification{Id: specName,
		InternalId: randInternalId,
		Settings:   metadata.DefaultReplicationSettings(),
	}
	specMap := make(map[string]*metadata.ReplicationSpecification)
	specMap[specName] = dummySpec

	setupMocksBRS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock, utilitiesMock, replSpecSvcMock, metadataEntriesFunc, specMap)
	setupMetakvStartupLoadingExistingOne(metadataSvcMock, specName, randInternalId)

	backfillReplSvc := NewBackfillReplTestSvc(uiLogSvcMock, metadataSvcMock, utilitiesMock, replSpecSvcMock, clusterInfoSvcMock, xdcrTopologyMock)
	assert.NotNil(backfillReplSvc)

	// check the backfillSpec
	backfillSpec := metadata.NewBackfillReplicationSpec(specName, randInternalId, constructDummyTasksMap(), dummySpec)
	checkSpec, err := backfillReplSvc.backfillSpec(specName)
	assert.Nil(err)
	assert.NotNil(checkSpec)
	assert.True(checkSpec.SameAs(backfillSpec))

	// Ensure that the backfillSpec loaded from service is not empty, thus validating the checkSpec.SameAs above
	assert.NotEqual(0, len(checkSpec.VBTasksMap))
	for _, backfillTasks := range checkSpec.VBTasksMap {
		for _, backfillTask := range *backfillTasks {
			assert.NotNil(backfillTask.Timestamps)
			assert.False(backfillTask.Timestamps.StartingTimestamp.SameAs(backfillTask.Timestamps.EndingTimestamp))
			assert.NotEqual(0, len(backfillTask.RequestedCollections()))
			for _, collectionNamespaceList := range backfillTask.RequestedCollections() {
				assert.NotEqual(0, collectionNamespaceList)
			}
		}
	}

	fmt.Println("============== Test case end: TestBackfillReplSvc =================")
}

// Test Adding a brand new backfill repl service
func setupMetakvInitialAdd(metadataSvc *service_def.MetadataSvc, specId, internalId string) {
	// Initially, there's no backfill mappings
	backfillMappingsKey := getBackfillMappingsDocKeyFunc(specId)
	metadataSvc.On("Get", backfillMappingsKey).Return(nil, nil, serviceDefReal.MetadataNotFoundErr).Times(1)

	// sharefcounting service should persist an empty doc
	emptyMappingsDoc := &metadata.CollectionNsMappingsDoc{}
	emptyMappingDocBytes, err := json.Marshal(emptyMappingsDoc)
	if err != nil {
		panic("Coding marshaller error")
	}
	metadataSvc.On("Add", backfillMappingsKey, emptyMappingDocBytes).Return(nil)

	// Second time, when Get is called, it's part of RMW of upsert
	metadataSvc.On("Get", backfillMappingsKey).Return(emptyMappingDocBytes, nil, nil).Times(1)

	validateDocBytes := createValidateCollectionNsMappingsDocBytes(internalId)
	// The add path should be consisted of the exact bytes as this validateDoc
	// And should be called on Set not Add because init should have been done already on empty doc
	metadataSvc.On("Set", backfillMappingsKey, validateDocBytes, mock.Anything).Return(nil)

	// For now no need to actually capture the metadatakv add op
	replKey := getBackfillReplicationDocKeyFunc(specId)
	metadataSvc.On("Add", replKey, mock.Anything).Return(nil)

	// Del ops are pass-through
	metadataSvc.On("Del", replKey, mock.Anything).Return(nil)
}

func TestBackfillReplSvcAddThenDel(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillReplSvcAddThenDel =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock, utilitiesMock, replSpecSvcMock := setupBoilerPlateBRS()

	specName := "testSpec"
	randInternalId, _ := base.GenerateRandomId(base.LengthOfRandomId, base.MaxRetryForRandomIdGeneration)
	metadataEntriesFunc := func() []*serviceDefReal.MetadataEntry {
		var entries []*serviceDefReal.MetadataEntry
		return entries
	}
	dummySpec := &metadata.ReplicationSpecification{Id: specName,
		InternalId: randInternalId,
		Settings:   metadata.DefaultReplicationSettings(),
	}
	specMap := make(map[string]*metadata.ReplicationSpecification)
	specMap[specName] = dummySpec

	setupMocksBRS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock, utilitiesMock, replSpecSvcMock, metadataEntriesFunc, specMap)
	setupMetakvInitialAdd(metadataSvcMock, specName, randInternalId)

	backfillReplSvc := NewBackfillReplTestSvc(uiLogSvcMock, metadataSvcMock, utilitiesMock, replSpecSvcMock, clusterInfoSvcMock, xdcrTopologyMock)
	assert.NotNil(backfillReplSvc)

	// check the backfillSpec - should not be there
	backfillSpec := metadata.NewBackfillReplicationSpec(specName, randInternalId, constructDummyTasksMap(), dummySpec)
	checkSpec, err := backfillReplSvc.backfillSpec(specName)
	assert.Equal(ReplNotFoundErr, err)
	assert.Nil(checkSpec)

	assert.Nil(backfillReplSvc.AddBackfillReplSpec(backfillSpec))

	checkSpec, err = backfillReplSvc.backfillSpec(specName)
	assert.Nil(err)
	assert.NotNil(checkSpec)
	assert.True(checkSpec.SameAs(backfillSpec))

	// Then do a delete
	oldSpec := dummySpec
	var newNilSpec *metadata.ReplicationSpecification
	assert.Nil(backfillReplSvc.ReplicationSpecChangeCallback(specName, oldSpec, newNilSpec))
	_, err = backfillReplSvc.backfillSpec(specName)
	assert.Equal(ReplNotFoundErr, err)

	// Random delete of another entity that does not have backfill replication associated should be nil error
	assert.Nil(backfillReplSvc.ReplicationSpecChangeCallback("randomIdName", oldSpec, newNilSpec))

	fmt.Println("============== Test case end: TestBackfillReplSvcAddThenDel =================")
}
