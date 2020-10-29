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

type TasksMapType int

const (
	FullSet TasksMapType = iota
	Subset  TasksMapType = iota
)

func constructDummyTasksMap() metadata.VBTasksMapType {
	return constructDummyTasksMapCustom(FullSet)
}

const CustomScopeName = "CustomScope"
const CustomCollectionName = "CustomCollection"

func constructDummyTasksMapCustom(set TasksMapType) metadata.VBTasksMapType {
	namespaceMapping := make(metadata.CollectionNamespaceMapping)
	defaultNamespace := &base.CollectionNamespace{base.DefaultScopeCollectionName, base.DefaultScopeCollectionName}
	namespaceMapping.AddSingleMapping(defaultNamespace, defaultNamespace)

	nonDefaultNamespaceMapping := make(metadata.CollectionNamespaceMapping)
	nonDefaultNamespace := &base.CollectionNamespace{CustomScopeName, CustomCollectionName}
	nonDefaultNamespaceMapping.AddSingleMapping(nonDefaultNamespace, nonDefaultNamespace)

	manifestsIdPair := base.CollectionsManifestIdPair{0, 0}
	ts0 := &metadata.BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 5000, 500, 500, manifestsIdPair},
	}

	vb0Task0 := metadata.NewBackfillTask(ts0, []metadata.CollectionNamespaceMapping{namespaceMapping})
	var vb0Task1 *metadata.BackfillTask

	if set == FullSet {
		ts1 := &metadata.BackfillVBTimestamps{
			StartingTimestamp: &base.VBTimestamp{0, 0, 5005, 10, 10, manifestsIdPair},
			EndingTimestamp:   &base.VBTimestamp{0, 0, 15005, 500, 500, manifestsIdPair},
		}
		vb0Task1 = metadata.NewBackfillTask(ts1, []metadata.CollectionNamespaceMapping{nonDefaultNamespaceMapping})
	}

	var vb0Tasks metadata.BackfillTasks
	vb0Tasks = append(vb0Tasks, vb0Task0)
	if set == FullSet {
		vb0Tasks = append(vb0Tasks, vb0Task1)
	}

	ts2 := &metadata.BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{1, 0, 5, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{1, 0, 5000, 500, 500, manifestsIdPair},
	}
	vb1Task0 := metadata.NewBackfillTask(ts2, []metadata.CollectionNamespaceMapping{namespaceMapping})

	var vb1Tasks metadata.BackfillTasks
	vb1Tasks = append(vb1Tasks, vb1Task0)

	vbTasksMap := make(map[uint16]*metadata.BackfillTasks)
	vbTasksMap[0] = &vb0Tasks
	vbTasksMap[1] = &vb1Tasks

	return vbTasksMap
}

func createValidateCollectionNsMappingsDocBytes(internalId string, setType TasksMapType) []byte {
	// As part of adding, upsert should be done for all the VBtaskmaps
	dummyVBTaskMap := constructDummyTasksMapCustom(setType)
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
	docBytes := createValidateCollectionNsMappingsDocBytes(internalId, FullSet)
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
			Key:   fmt.Sprintf("%v/%v/%v", BackfillParentCatalogKey, specName, SpecKey),
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

var randomSpecName string = "randomIdName"

// Test Adding a brand new backfill repl service
func setupMetakvInitialAddSetDel(metadataSvc *service_def.MetadataSvc, specId, internalId string, dummySpec *metadata.ReplicationSpecification) {
	// Initially, there's no backfill mappings
	backfillMappingsKey := getBackfillMappingsDocKeyFunc(specId)
	metadataSvc.On("Get", backfillMappingsKey).Return(nil, nil, serviceDefReal.MetadataNotFoundErr).Times(1)

	specGetKey := getBackfillReplicationDocKeyFunc(specId)

	// sharefcounting service should persist an empty doc
	emptyMappingsDoc := &metadata.CollectionNsMappingsDoc{}
	emptyMappingDocBytes, err := json.Marshal(emptyMappingsDoc)
	if err != nil {
		panic("Coding marshaller error")
	}
	// ADD PATH
	metadataSvc.On("Add", backfillMappingsKey, emptyMappingDocBytes).Return(nil)

	// When adding, a Get will be called  as part of getting the revision
	metadataSvc.On("Get", specGetKey).Return(emptyMappingDocBytes, nil, nil).Times(1)

	metadataSvc.On("Get", backfillMappingsKey).Return(emptyMappingDocBytes, nil, nil).Times(1)

	// Second time, when Get is called, it's part of RMW of upsert
	metadataSvc.On("Get", backfillMappingsKey).Return(emptyMappingDocBytes, nil, nil).Times(1)

	validateDocBytes := createValidateCollectionNsMappingsDocBytes(internalId, FullSet)
	// The add path should be consisted of the exact bytes as this validateDoc
	// And should be called on Set not Add because init should have been done already on empty doc
	metadataSvc.On("Set", backfillMappingsKey, validateDocBytes, mock.Anything).Return(nil)

	// For now no need to actually capture the metadatakv add op
	replKey := getBackfillReplicationDocKeyFunc(specId)
	metadataSvc.On("Add", replKey, mock.Anything).Return(nil)

	// SET OPS
	// When setting, it will ask for the the currently stored mappingdoc, which was previously "SET" as part of the ADD PATH
	// This is due to RMW of the sha service
	metadataSvc.On("Get", backfillMappingsKey).Return(validateDocBytes, nil, nil).Times(1)

	// The Write of the RMW should contain a smaller subset of doc due to setting of a subsetReplicationSpec
	subsetDocBytes := createValidateCollectionNsMappingsDocBytes(internalId, Subset)
	metadataSvc.On("Set", backfillMappingsKey, subsetDocBytes, mock.Anything).Return(nil)

	// Then it will try to set the new subspec
	backfillReplKey := getBackfillReplicationDocKeyFunc(specId)
	subsetBackfillSpec := metadata.NewBackfillReplicationSpec(specId, internalId, constructDummyTasksMapCustom(Subset), dummySpec)
	subsetDocVal, err := json.Marshal(subsetBackfillSpec)
	if err != nil {
		panic("Json marshal err")
	}
	metadataSvc.On("Set", backfillReplKey, subsetDocVal, mock.Anything).Return(nil).Times(1)

	// After set, will get revision to update spec
	dummyRev := "dummyRev"
	metadataSvc.On("Get", backfillReplKey).Return(subsetDocVal, dummyRev, nil).Times(1)

	// DEL OPS
	// Del ops are pass-through
	metadataSvc.On("Del", replKey, mock.Anything).Return(nil)
	metadataSvc.On("Del", backfillMappingsKey, mock.Anything).Return(nil)
	// Random delete of another entity that does not have backfill replication associated should be nil error
	// Should call Del anyway just to ensure cleanup of stray backfills lying around
	randomSpecKey := getBackfillReplicationDocKeyFunc(randomSpecName)
	randomSpecBackfillKey := getBackfillMappingsDocKeyFunc(randomSpecName)
	metadataSvc.On("Del", randomSpecKey, mock.Anything).Return(nil)
	metadataSvc.On("Del", randomSpecBackfillKey, mock.Anything).Return(nil)
}

func TestBackfillReplSvcAddSetDel(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillReplSvcAddSetDel =================")
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
	setupMetakvInitialAddSetDel(metadataSvcMock, specName, randInternalId, dummySpec)

	backfillReplSvc := NewBackfillReplTestSvc(uiLogSvcMock, metadataSvcMock, utilitiesMock, replSpecSvcMock, clusterInfoSvcMock, xdcrTopologyMock)
	assert.NotNil(backfillReplSvc)

	// check the backfillSpec - should not be there
	backfillSpec := metadata.NewBackfillReplicationSpec(specName, randInternalId, constructDummyTasksMap(), dummySpec)
	checkSpec, err := backfillReplSvc.backfillSpec(specName)
	assert.Equal(base.ReplNotFoundErr, err)
	assert.Nil(checkSpec)

	assert.Nil(backfillReplSvc.AddBackfillReplSpec(backfillSpec))

	checkSpec, err = backfillReplSvc.backfillSpec(specName)
	assert.Nil(err)
	assert.NotNil(checkSpec)
	assert.True(checkSpec.SameAs(backfillSpec))

	// Do a set with a smaller subset of tasks
	subsetBackfillSpec := metadata.NewBackfillReplicationSpec(specName, randInternalId, constructDummyTasksMapCustom(Subset), dummySpec)
	assert.Nil(backfillReplSvc.SetBackfillReplSpec(subsetBackfillSpec))
	checkSpec, err = backfillReplSvc.backfillSpec(specName)
	assert.Nil(err)
	assert.NotNil(checkSpec)
	assert.False(checkSpec.SameAs(backfillSpec))
	assert.True(checkSpec.SameAs(subsetBackfillSpec))

	// Then do a delete
	oldSpec := dummySpec
	var newNilSpec *metadata.ReplicationSpecification
	assert.Nil(backfillReplSvc.ReplicationSpecChangeCallback(specName, oldSpec, newNilSpec))
	_, err = backfillReplSvc.backfillSpec(specName)
	assert.Equal(base.ReplNotFoundErr, err)

	// Random delete of another entity that does not have backfill replication associated should be nil error
	assert.Nil(backfillReplSvc.ReplicationSpecChangeCallback(randomSpecName, oldSpec, newNilSpec))

	fmt.Println("============== Test case end: TestBackfillReplSvcAddSetDel =================")
}
