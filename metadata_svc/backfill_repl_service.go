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
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"strings"
	"sync"
)

const BackfillKey = "backfill"

const SpecKey = "spec"

var BackfillParentCatalogKey = CheckpointsCatalogKeyPrefix + base.KeyPartsDelimiter + BackfillKey

const BackfillMappingsKey = "backfillMappings"

// Get a unique key to access metakv for backfillMappings
func getBackfillMappingsDocKeyFunc(replicationId string) string {
	// ckpt/backfill/<replId>/backfillMappings
	return fmt.Sprintf("%v", BackfillParentCatalogKey+base.KeyPartsDelimiter+replicationId+base.KeyPartsDelimiter+BackfillMappingsKey)
}

// This is the actual replicationSpec that contains all the VBs and their tasks
// Each tasks depend on having the mapping information already loaded
func getBackfillReplicationDocKeyFunc(replicationId string) string {
	// ckpt/backfill/<replId>/spec
	return fmt.Sprintf("%v", BackfillParentCatalogKey+base.KeyPartsDelimiter+replicationId+base.KeyPartsDelimiter+SpecKey)
}

type BackfillReplicationService struct {
	*ShaRefCounterService
	metadataSvc     service_def.MetadataSvc
	uiLogSvc        service_def.UILogSvc
	replSpecSvc     service_def.ReplicationSpecSvc
	clusterInfoSvc  service_def.ClusterInfoSvc
	xdcrTopologySvc service_def.XDCRCompTopologySvc

	logger     *log.CommonLogger
	utils      utilities.UtilsIface
	cache      *MetadataCache
	cache_lock sync.Mutex

	metadataChangeCallbacks []base.MetadataChangeHandlerCallback
	metadataChangeCbMtx     sync.RWMutex
}

func NewBackfillReplicationService(uiLogSvc service_def.UILogSvc,
	metadataSvc service_def.MetadataSvc,
	loggerCtx *log.LoggerContext,
	utilsIn utilities.UtilsIface,
	replSpecSvc service_def.ReplicationSpecSvc,
	clusterInfoSvc service_def.ClusterInfoSvc,
	xdcrTopologySvc service_def.XDCRCompTopologySvc) (*BackfillReplicationService, error) {
	logger := log.NewLogger("BackfillReplSvc", loggerCtx)
	svc := &BackfillReplicationService{
		ShaRefCounterService: NewShaRefCounterService(getBackfillMappingsDocKeyFunc, metadataSvc),
		metadataSvc:          metadataSvc,
		logger:               logger,
		replSpecSvc:          replSpecSvc,
		utils:                utilsIn,
		clusterInfoSvc:       clusterInfoSvc,
		xdcrTopologySvc:      xdcrTopologySvc,
	}

	return svc, svc.initCacheFromMetaKV()
}

func (b *BackfillReplicationService) initCacheFromMetaKV() (err error) {
	b.cache_lock.Lock()
	defer b.cache_lock.Unlock()
	var KVsFromMetaKV []*service_def.MetadataEntry
	var KVsFromMetaKVErr error

	// Clears all from cache before reloading
	b.initCache()

	getAllKVsOpFunc := func() error {
		KVsFromMetaKV, KVsFromMetaKVErr = b.metadataSvc.GetAllMetadataFromCatalog(BackfillParentCatalogKey)
		return KVsFromMetaKVErr
	}
	err = b.utils.ExponentialBackoffExecutor("GetAllMetadataFromCatalogBackfillReplicationSpec", base.RetryIntervalMetakv,
		base.MaxNumOfMetakvRetries, base.MetaKvBackoffFactor, getAllKVsOpFunc)
	if err != nil {
		b.logger.Errorf("Unable to get all the KVs from metakv: %v", err)
		return
	}

	// One by one update the specs
	for _, KVentry := range KVsFromMetaKV {
		key := KVentry.Key
		marshalledSpec := KVentry.Value
		rev := KVentry.Rev

		replicationId := b.getReplicationIdFromKey(key)
		backfillSpec, err := b.constructBackfillSpec(marshalledSpec, rev, false /*lock*/)
		if err != nil {
			b.logger.Errorf("Unable to construct spec %v from metaKV's data. err: %v", key, err)
			continue
		}
		actualSpec, err := b.replSpecSvc.ReplicationSpec(replicationId)
		if err != nil {
			b.logger.Errorf("Unable to retrieve actual spec for id %v - %v", replicationId, err)
		}
		if backfillSpec.InternalId != actualSpec.InternalId {
			// Out of date
			b.logger.Warnf("Out of date backfill found with internal ID %v - skipping...", backfillSpec.InternalId)
			continue
		}

		backfillSpec.SetReplicationSpec(actualSpec)

		// Last step is to make sure that the sha mapping is retrieved
		b.InitTopicShaCounter(backfillSpec.Id)
		mappingsDoc, err := b.GetMappingsDoc(backfillSpec.Id, false /*initIfNotFound*/)
		if err != nil {
			// TODO - MB-38506
			b.logger.Errorf("Unable to retrieve mappingDoc for backfill replication %v - potential data loss", backfillSpec.Id)
			continue
		}
		shaMapping, err := b.GetShaToCollectionNsMap(backfillSpec.Id, mappingsDoc)
		if err != nil {
			// TODO - MB-38506
			b.logger.Errorf("Error - unable to get shaToCollectionsMap %v", err)
			continue
		}

		err = backfillSpec.VBTasksMap.LoadFromMappingsShaMap(shaMapping)
		if err != nil {
			// TODO - MB-38506
			b.logger.Errorf("Error - unable to get shaToCollectionsMap %v", err)
			continue
		}

		// Finally, done
		b.updateCacheInternal(replicationId, backfillSpec, false /*lock*/)
	}

	return nil
}

func (b *BackfillReplicationService) constructBackfillSpec(value []byte, rev interface{}, lock bool) (*metadata.BackfillReplicationSpec, error) {
	if value == nil {
		return nil, nil
	}

	// TODO - MB-38675 this will change
	spec := &metadata.BackfillReplicationSpec{}
	err := json.Unmarshal(value, spec)
	if err != nil {
		return nil, err
	}

	return spec, nil
}

func (b *BackfillReplicationService) updateCacheInternalNoLock(specId string, newSpec *metadata.BackfillReplicationSpec) (*metadata.BackfillReplicationSpec, bool, error) {
	oldSpec, err := b.backfillSpec(specId)
	if err != nil {
		oldSpec = nil
	}

	updated := false
	if newSpec == nil {
		if oldSpec != nil {
			// replication spec has been deleted
			b.removeSpecFromCache(specId)
			updated = true
		}
	} else {
		// replication spec has been created or updated

		// no need to update cache if newSpec is the same as the one already in cache
		if oldSpec == nil || !newSpec.SameAs(oldSpec) {
			err = b.cacheSpec(b.getCache(), specId, newSpec)
			if err == nil {
				specId = newSpec.Id
				updated = true
			} else {
				return oldSpec, updated, err
			}
		}

	}

	return oldSpec, updated, err
}

var ReplNotFoundErr = errors.New(ReplicationSpecNotFoundErrorMessage)

func (b *BackfillReplicationService) backfillSpec(replicationId string) (*metadata.BackfillReplicationSpec, error) {
	val, ok := b.getCache().Get(replicationId)
	if !ok {
		return nil, ReplNotFoundErr
	}
	replSpecVal, ok := val.(*ReplicationSpecVal)
	if !ok || replSpecVal == nil {
		return nil, ReplNotFoundErr
	}
	backfillReplSpec, ok := replSpecVal.spec.(*metadata.BackfillReplicationSpec)
	if !ok || backfillReplSpec == nil {
		return nil, ReplNotFoundErr
	}

	return backfillReplSpec, nil
}

func (b *BackfillReplicationService) removeSpecFromCache(specId string) error {
	//soft remove it from cache by setting SpecVal.spec = nil, but keep the key there
	//so that the derived object can still be retrieved and be acted on for cleaning-up.
	cache := b.getCache()
	val, ok := cache.Get(specId)
	if ok && val != nil {
		specVal, ok1 := val.(*ReplicationSpecVal)
		if ok1 {
			updatedCachedObj := &ReplicationSpecVal{
				spec:       nil,
				derivedObj: specVal.derivedObj,
				cas:        specVal.cas}
			return cache.Upsert(specId, updatedCachedObj)
		}
	}

	return nil
}

func (b *BackfillReplicationService) cacheSpec(cache *MetadataCache, specId string, spec *metadata.BackfillReplicationSpec) error {
	var cachedObj *ReplicationSpecVal = nil
	var updatedCachedObj *ReplicationSpecVal = nil
	var ok1 bool
	cachedVal, ok := cache.Get(specId)
	if ok && cachedVal != nil {
		cachedObj, ok1 = cachedVal.(*ReplicationSpecVal)
		if !ok1 || cachedObj == nil {
			panic("Object in ReplicationSpecServcie cache is not of type *replicationSpecVal")
		}
		updatedCachedObj = &ReplicationSpecVal{
			spec:       spec,
			derivedObj: cachedObj.derivedObj,
			cas:        cachedObj.cas}
	} else {
		//never being cached before
		updatedCachedObj = &ReplicationSpecVal{spec: spec}
	}
	return cache.Upsert(specId, updatedCachedObj)
}

func (b *BackfillReplicationService) getCache() *MetadataCache {
	if b.cache == nil {
		panic("Cache has not been initialized for ReplicationSpecService")
	}
	return b.cache
}

func (b *BackfillReplicationService) initCache() {
	b.cache = NewMetadataCache(b.logger)
	b.logger.Info("Cache has been initialized for BackfillReplicationService")
}

func (b *BackfillReplicationService) getReplicationIdFromKey(key string) string {
	prefix := BackfillParentCatalogKey + base.KeyPartsDelimiter
	if !strings.HasPrefix(key, prefix) {
		// should never get here.
		panic(fmt.Sprintf("Got unexpected key %v for backfill replication spec", key))
	}
	return key[len(prefix):]
}

func (b *BackfillReplicationService) BackfillReplSpec(replicationId string) (*metadata.BackfillReplicationSpec, error) {
	spec, err := b.backfillSpec(replicationId)
	if err != nil {
		return nil, err
	}

	return spec.Clone(), nil
}

func (b *BackfillReplicationService) GetMyVBs(sourceBucketName string) ([]uint16, error) {
	var vbList []uint16

	kv_vb_map, _, err := pipeline_utils.GetSourceVBMap(b.clusterInfoSvc, b.xdcrTopologySvc, sourceBucketName, b.logger)
	if err != nil {
		return vbList, err
	}

	for _, vbno := range kv_vb_map {
		vbList = append(vbList, vbno...)
	}

	return vbList, nil
}

func (b *BackfillReplicationService) AddBackfillReplSpec(spec *metadata.BackfillReplicationSpec) error {
	if spec == nil {
		return base.ErrorInvalidInput
	}

	// First, persist the collection mapping info for just the VBs this node owns
	alreadyExists := b.InitTopicShaCounterWithInternalId(spec.Id, spec.InternalId)
	if alreadyExists {
		return fmt.Errorf("Error - previous spec shouldn't exist")
	}

	err := b.persistMappingsForThisNode(spec)
	if err != nil {
		return err
	}
	b.logger.Infof("Adding backfill spec %v to metadata store", spec)

	value, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	// TODO - once consistent metakv is in play, there could be conflict when adding, so this
	// section would need to handle that and do RMW
	key := getBackfillReplicationDocKeyFunc(spec.Id)
	err = b.metadataSvc.Add(key, value)
	if err != nil {
		b.logger.Errorf("Add returned error: %v\n", err)
		return err
	}

	return b.updateCache(spec.Id, spec)
}

// Each node should only be responsible to update the centralized mapping file
// to ensure that the mappings that it needs are persisted. This way, it ensure that whichever VB is the most
// advanced on whatever node will get the chance to update metakv with the needed mappings, and every other node
// will benefit
// When consistent metakv is in play, this would lead to less conflicts
func (b *BackfillReplicationService) persistMappingsForThisNode(spec *metadata.BackfillReplicationSpec) error {
	mappingsDoc, err := b.GetMappingsDoc(spec.Id, true /*InitIfNotFound*/)
	if err != nil {
		return err
	}
	inflatedMapping, err := b.GetShaToCollectionNsMap(spec.Id, mappingsDoc)
	if err != nil {
		return err
	}
	err = b.InitCounterShaToActualMappings(spec.Id, spec.InternalId, inflatedMapping)
	if err != nil {
		return err
	}

	myVBs, err := b.GetMyVBs(spec.ReplicationSpec().SourceBucketName)
	if err != nil {
		return err
	}
	mySortedVBs := base.SortUint16List(myVBs)

	consolidatedMap := make(metadata.ShaToCollectionNamespaceMap)
	for vb, tasks := range spec.VBTasksMap {
		_, isMyVB := base.SearchVBInSortedList(vb, mySortedVBs)
		if !isMyVB {
			continue
		}
		innerConsolidatedMap := tasks.GetAllCollectionNamespaceMappings()
		for sha, nsMap := range innerConsolidatedMap {
			if _, exists := consolidatedMap[sha]; !exists {
				consolidatedMap[sha] = nsMap
			}
		}
	}

	incrementer, err := b.GetIncrementerFunc(spec.Id)
	if err != nil {
		return err
	}

	for sha, mapping := range consolidatedMap {
		incrementer(sha, mapping)
	}

	return b.UpsertMapping(spec.Id, spec.InternalId)
}

func (b *BackfillReplicationService) SetBackfillReplSpec(spec *metadata.BackfillReplicationSpec) error {
	if spec == nil {
		return base.ErrorInvalidInput
	}

	oldSpec, err := b.backfillSpec(spec.Id)
	if err != nil {
		return err
	}

	specValue, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	err = b.persistVBTasksMapDifferences(spec, oldSpec)
	if err != nil {
		return err
	}

	key := getBackfillReplicationDocKeyFunc(spec.Id)
	err = b.metadataSvc.Set(key, specValue, spec.Revision())
	if err != nil {
		return err
	}

	err = b.loadLatestMetakvRevisionIntoSpec(spec)
	if err != nil {
		return err
	}

	err = b.updateCache(spec.Id, spec)
	if err == nil {
		b.logger.Infof("BackfillReplication spec %s has been updated, rev=%v\n", spec.Id, spec.Revision)
	}

	return err
}

func (b *BackfillReplicationService) persistVBTasksMapDifferences(spec, oldSpec *metadata.BackfillReplicationSpec) error {
	vbsList, err := b.GetMyVBs(spec.ReplicationSpec().SourceBucketName)
	if err != nil {
		return err
	}
	sortedVbs := base.SortUint16List(vbsList)

	subsetBackfillTasks := make(metadata.VBTasksMapType)
	for vb, tasks := range spec.VBTasksMap {
		_, isInList := base.SearchVBInSortedList(vb, sortedVbs)
		if isInList {
			subsetBackfillTasks[vb] = tasks
		}
	}

	olderSubsetBackfillTasks := make(metadata.VBTasksMapType)
	for vb, tasks := range oldSpec.VBTasksMap {
		_, isInList := base.SearchVBInSortedList(vb, sortedVbs)
		if isInList {
			olderSubsetBackfillTasks[vb] = tasks
		}
	}

	newShaToColMapping := subsetBackfillTasks.GetAllCollectionNamespaceMappings()
	oldShaToColMapping := olderSubsetBackfillTasks.GetAllCollectionNamespaceMappings()

	added, removed := newShaToColMapping.Diff(oldShaToColMapping)

	if len(added) > 0 {
		increment, err := b.GetIncrementerFunc(spec.Id)
		if err != nil {
			return err
		}
		for k, v := range added {
			increment(k, v)
		}
	}

	if len(removed) > 0 {
		decrement, err := b.GetDecrementerFunc(spec.Id)
		if err != nil {
			return err
		}
		for k, _ := range removed {
			decrement(k)
		}
	}

	if len(added) > 0 || len(removed) > 0 {
		err = b.UpsertMapping(spec.Id, spec.InternalId)
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *BackfillReplicationService) DelBackfillReplSpec(replicationId string) (*metadata.BackfillReplicationSpec, error) {
	_, err := b.backfillSpec(replicationId)
	if err != nil {
		return nil, ReplNotFoundErr
	}

	key := getBackfillReplicationDocKeyFunc(replicationId)
	err = b.metadataSvc.Del(key, nil /*rev*/)
	if err != nil {
		b.logger.Errorf("Failed to delete backfill spec, key=%v, err=%v\n", key, err)
		return nil, err
	}

	// Once backfill spec has been deleted, safe to delete the backfill mappings
	// TODO - revisit once consistent metakv is in
	err = b.CleanupMapping(replicationId)
	if err != nil {
		b.logger.Errorf("Failed to cleanup backfill spec mapping err=%v\n", err)
		return nil, err
	}

	err = b.updateCache(replicationId, nil)
	if err != nil {
		b.logger.Errorf("Failed to delete backfill spec, key=%v, err=%v\n", key, err)
		return nil, err
	}

	return nil, nil
}

func (b *BackfillReplicationService) updateCache(specId string, newSpec *metadata.BackfillReplicationSpec) error {
	return b.updateCacheInternal(specId, newSpec, true /*lock*/)
}

func (b *BackfillReplicationService) updateCacheNoLock(specId string, newSpec *metadata.BackfillReplicationSpec) error {
	return b.updateCacheInternal(specId, newSpec, false /*lock*/)
}

func (b *BackfillReplicationService) updateCacheInternal(specId string, newSpec *metadata.BackfillReplicationSpec, lock bool) error {
	if lock {
		b.cache_lock.Lock()
		defer b.cache_lock.Unlock()
	}

	oldSpec, updated, err := b.updateCacheInternalNoLock(specId, newSpec)

	if updated && oldSpec != nil && newSpec == nil {
		// TODO MB-38331 - notify Backfill Manager
	}

	if updated && oldSpec == nil && newSpec != nil {
		// TODO MB-38331 - notify Backfill Manager
	}

	return err
}

func (b *BackfillReplicationService) ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}) error {
	oldSpec, ok := oldVal.(*metadata.ReplicationSpecification)
	if !ok {
		return base.ErrorInvalidInput
	}
	newSpec, ok := newVal.(*metadata.ReplicationSpecification)
	if !ok {
		return base.ErrorInvalidInput
	}

	if oldSpec == nil && newSpec != nil {
		// TODO MB-38331 - notify Backfill Manager??
	} else if oldSpec != nil && newSpec == nil {
		_, err := b.DelBackfillReplSpec(id)
		if err == ReplNotFoundErr {
			err = nil
		}
		return err
	}
	return nil
}

func (b *BackfillReplicationService) SetMetadataChangeHandlerCallback(call_back base.MetadataChangeHandlerCallback) {
	b.metadataChangeCbMtx.Lock()
	defer b.metadataChangeCbMtx.Unlock()
	b.metadataChangeCallbacks = append(b.metadataChangeCallbacks, call_back)
}

func (b *BackfillReplicationService) loadLatestMetakvRevisionIntoSpec(spec *metadata.BackfillReplicationSpec) error {
	key := getBackfillReplicationDocKeyFunc(spec.Id)
	_, rev, err := b.metadataSvc.Get(key)
	if err != nil {
		return err
	}
	spec.SetRevision(rev)
	return nil
}
