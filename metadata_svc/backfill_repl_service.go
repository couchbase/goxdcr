// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata_svc

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"
)

const BackfillKey = "backfill"
const SpecKey = "spec"
const BackfillMappingsKey = "backfillMappings"

var ReplicationIdSuffixes = []string{BackfillKey, SpecKey, BackfillMappingsKey}

var BackfillParentCatalogKey = CheckpointsCatalogKeyPrefix + base.KeyPartsDelimiter + BackfillKey

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

const BackfillReplSvcId = "BackfillReplicationService"

type BackfillReplicationService struct {
	*ShaRefCounterService
	metadataSvc     service_def.MetadataSvc
	uiLogSvc        service_def.UILogSvc
	replSpecSvc     service_def.ReplicationSpecSvc
	xdcrTopologySvc service_def.XDCRCompTopologySvc

	logger     *log.CommonLogger
	utils      utilities.UtilsIface
	cache      *MetadataCache
	cache_lock sync.Mutex

	metadataChangeCallbacks []base.MetadataChangeHandlerCallback
	metadataChangeCbMtx     sync.RWMutex

	unrecoverableBackfillIds [][2]string
	completeBackfillCb       func(string) error
	backfillCkptsCleanupCb   func(string) error
	recoveringBackfill       uint32

	bucketTopologySvc service_def.BucketTopologySvc
}

func NewBackfillReplicationService(uiLogSvc service_def.UILogSvc, metadataSvc service_def.MetadataSvc, loggerCtx *log.LoggerContext, utilsIn utilities.UtilsIface, replSpecSvc service_def.ReplicationSpecSvc, xdcrTopologySvc service_def.XDCRCompTopologySvc, bucketTopologySvc service_def.BucketTopologySvc) (*BackfillReplicationService, error) {
	logger := log.NewLogger("BackfillReplSvc", loggerCtx)
	shaRefSvc, err := NewShaRefCounterService(getBackfillMappingsDocKeyFunc, metadataSvc, logger, utilsIn)
	if err != nil {
		return nil, err
	}
	svc := &BackfillReplicationService{
		ShaRefCounterService: shaRefSvc,
		uiLogSvc:             uiLogSvc,
		metadataSvc:          metadataSvc,
		logger:               logger,
		replSpecSvc:          replSpecSvc,
		utils:                utilsIn,
		xdcrTopologySvc:      xdcrTopologySvc,
		bucketTopologySvc:    bucketTopologySvc,
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

	// Because "extras" below can cause double reads
	specProcessed := make(map[string]bool)

	// One by one update the specs
	for _, KVentry := range KVsFromMetaKV {
		key := KVentry.Key
		marshalledSpec := KVentry.Value
		rev := KVentry.Rev

		replicationIdPlusExtra := b.getReplicationIdFromKey(key)
		// replicationIdPlusExtra can be either
		// 1. 63a5f325205fe7f610a7ec19570054da/B1/B2/backfillMappings - backfill mappings
		// 2. 63a5f325205fe7f610a7ec19570054da/B1/B2/spec - actual backfill replication spec
		// 3. 63a5f325205fe7f610a7ec19570054da/B1/B2/globalInfo - deduped global info i.e globalTimestamps and globalTargetCounters
		// The replicationId should be:
		// 63a5f325205fe7f610a7ec19570054da/B1/B2
		var replicationId string
		for _, suffix := range ReplicationIdSuffixes {
			suffixStr := fmt.Sprintf("%v%v", "/", suffix)
			if strings.HasSuffix(replicationIdPlusExtra, suffixStr) {
				replicationId = strings.TrimSuffix(replicationIdPlusExtra, suffixStr)
				break
			}
		}
		if replicationId == "" {
			continue
		}
		if !strings.HasSuffix(key, fmt.Sprintf("/%v", SpecKey)) {
			continue
		}
		if specProcessed[replicationId] {
			// has already been processed
			continue
		}
		specProcessed[replicationId] = true

		backfillSpec, err := b.constructBackfillSpec(marshalledSpec, rev, false /*lock*/)
		if err != nil {
			b.logger.Errorf("Unable to construct spec %v from metaKV's data. err: %v", key, err)
			continue
		}

		actualSpec, err := b.replSpecSvc.ReplicationSpec(replicationId)
		if err != nil {
			if err != service_def.MetadataNotFoundErr {
				b.logger.Errorf("Unable to retrieve actual spec for id %v - %v", replicationId, err)
			}
			b.DelBackfillReplSpec(replicationId)
			continue
		}
		if backfillSpec.InternalId == "" && actualSpec.InternalId != "" {
			backfillSpec.InternalId = actualSpec.InternalId
		}
		if backfillSpec.Id == "" && replicationId != "" {
			backfillSpec.Id = replicationId
		}
		if backfillSpec.InternalId != actualSpec.InternalId {
			// Out of date
			b.logger.Warnf("Out of date backfill found with internal ID %v (expecting %v) - deleting it...",
				backfillSpec.InternalId, actualSpec.InternalId)
			b.DelBackfillReplSpec(replicationId)
			continue
		}

		backfillSpec.SetReplicationSpec(actualSpec)

		// Last step is to make sure that the sha mapping is retrieved and loaded

		// 1. initialise the topic with 0-values in SHA reference counter service.
		b.InitTopicShaCounterWithInternalId(backfillSpec.Id, backfillSpec.InternalId)

		// 2. perform a metakv get for previously persisted mapping doc
		var mappingsDoc *metadata.CompressedMappings
		getMappingDoc := func() error {
			doc, getErr := b.GetMappingsDoc(backfillSpec.Id, false /*initIfNotFound*/)
			if getErr != nil {
				b.logger.Warnf("Error while trying to get mapping doc for backfill replication %v, err=%v. May retry",
					backfillSpec.Id, err)
				return getErr
			}

			mappingsDoc = doc
			return nil
		}
		err = b.utils.ExponentialBackoffExecutor(fmt.Sprintf("getMappingDoc(%v)", backfillSpec.Id), base.RetryIntervalMetakv,
			base.MaxNumOfMetakvRetries, base.MetaKvBackoffFactor, getMappingDoc)
		if err != nil {
			b.logger.Errorf("Unable to retrieve mappingDoc for backfill replication %v. err=%v", backfillSpec.Id, err)
			b.appendUnrecoverableBackfillSpec(backfillSpec)
			continue
		}

		// 3. convert mappingDoc to SHA->CollectionNS mapping
		shaMapping, err := b.GetShaToCollectionNsMap(backfillSpec.Id, (*metadata.CollectionNsMappingsDoc)(mappingsDoc))
		if err != nil {
			b.logger.Errorf("Error - unable to get shaToCollectionsMap %v for %v", err, backfillSpec.Id)
			b.appendUnrecoverableBackfillSpec(backfillSpec)
			continue
		}

		// 4. load the backfill spec with SHA->CollectionNS mapping
		err = backfillSpec.VBTasksMap.LoadFromMappingsShaMap(shaMapping)
		if err != nil {
			b.logger.Errorf("Error - unable to load from shaToCollectionsMap %v for %v", err, backfillSpec.Id)
			b.appendUnrecoverableBackfillSpec(backfillSpec)
			continue
		}

		// 5. load the SHA reference counter initialised in step (1) with information
		// persisted previously in metaKV.
		_, _, err = b.loadSHARefMappings(backfillSpec, shaMapping, false)
		if err != nil {
			b.logger.Errorf("Error - unable to load SHA reference counter. err=%v for %v", err, backfillSpec.Id)
			b.appendUnrecoverableBackfillSpec(backfillSpec)
			continue
		}

		// 6. update the local cached backfill spec with the computed spec from steps 1-5.
		err = b.updateCacheInternal(replicationId, backfillSpec, false /*lock*/)
		if err != nil {
			// A clean cache should not have CAS mismatch error - don't panic but log a serious warning
			b.logger.Fatalf("updateCacheInternal failed with %v for %v", err, backfillSpec.Id)
		}
	}

	return nil
}

func (b *BackfillReplicationService) appendUnrecoverableBackfillSpec(backfillSpec *metadata.BackfillReplicationSpec) {
	var backfillPair [2]string
	backfillPair[0] = backfillSpec.Id
	backfillPair[1] = backfillSpec.InternalId
	b.unrecoverableBackfillIds = append(b.unrecoverableBackfillIds, backfillPair)
}

func (b *BackfillReplicationService) constructBackfillSpec(value []byte, rev interface{}, lock bool) (*metadata.BackfillReplicationSpec, error) {
	if value == nil {
		return nil, nil
	}

	spec := &metadata.BackfillReplicationSpec{}
	err := json.Unmarshal(value, spec)
	if err != nil {
		return nil, err
	}

	spec.PostUnmarshalInit()
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
		parentSpec := newSpec.GetReplicationSpec()
		if parentSpec != nil && parentSpec.Settings != nil && parentSpec.Settings.GetIntSettingValue(metadata.DevBackfillReplUpdateDelay) > 0 {
			msToSleep := parentSpec.Settings.GetIntSettingValue(metadata.DevBackfillReplUpdateDelay)
			time.Sleep(time.Duration(msToSleep) * time.Millisecond)
		}

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

func (b *BackfillReplicationService) backfillSpec(replicationId string) (*metadata.BackfillReplicationSpec, error) {
	val, ok := b.getCache().Get(replicationId)
	if !ok {
		return nil, base.ReplNotFoundErr
	}
	replSpecVal, ok := val.(*ReplicationSpecVal)
	if !ok || replSpecVal == nil {
		b.logger.Errorf("%v wrong type of object in backfill cache, ok=%v, replSpecVal=%v (%T)",
			replicationId, ok, replSpecVal, val)
		return nil, base.ReplNotFoundErr
	}
	backfillReplSpec, ok := replSpecVal.spec.(*metadata.BackfillReplicationSpec)
	if !ok || backfillReplSpec == nil {
		b.logger.Errorf("%v wrong type of object in replication spec, ok=%v, backfillReplSpec=%v (%T)",
			replicationId, ok, backfillReplSpec, replSpecVal.spec)
		return nil, base.ReplNotFoundErr
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

// Returns something like "89f6bd31bb8abff4b45714fdf06803a7/B1/B2/spec"
func (b *BackfillReplicationService) getReplicationIdFromKey(key string) string {
	prefix := BackfillParentCatalogKey + base.KeyPartsDelimiter
	if !strings.HasPrefix(key, prefix) {
		// should never get here.
		panic(fmt.Sprintf("Got unexpected key %v for backfill replication spec", key))
	}
	return key[len(prefix):]
}

// Caller should clone the spec themselves if modification will be done onto it
func (b *BackfillReplicationService) BackfillReplSpec(replicationId string) (*metadata.BackfillReplicationSpec, error) {
	spec, err := b.backfillSpec(replicationId)
	if err != nil {
		return nil, err
	}

	return spec, nil
}

var backfillReplSvcIteration uint32

func (b *BackfillReplicationService) GetMyVBs(replSpec *metadata.ReplicationSpecification) ([]uint16, error) {
	bucketSvcId := BackfillReplSvcId + base.GetIterationId(&backfillReplSvcIteration)
	notificationCh, err := b.bucketTopologySvc.SubscribeToLocalBucketFeed(replSpec, bucketSvcId)
	if err != nil {
		return nil, err
	}
	defer b.bucketTopologySvc.UnSubscribeLocalBucketFeed(replSpec, bucketSvcId)

	latestNotification := <-notificationCh
	kv_vb_map := latestNotification.GetSourceVBMapRO()
	defer latestNotification.Recycle()

	var vbList []uint16
	for _, vbnos := range kv_vb_map {
		vbList = append(vbList, vbnos...)
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

	err := b.persistMappingsForThisNode(spec, true /*addOp*/)
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

	err = b.loadLatestMetakvRevisionIntoSpec(spec)
	if err != nil {
		return err
	}

	return b.updateCache(spec.Id, spec)
}

// Initialises the data-structures for SHA reference counter service (reference counter and SHA mappings) in memory
// with the input shaToNsMapping, for the input backfill spec.
func (b *BackfillReplicationService) loadSHARefMappings(spec *metadata.BackfillReplicationSpec, shaToNsMapping metadata.ShaToCollectionNamespaceMap, addOp bool) (metadata.ShaToCollectionNamespaceMap, metadata.ShaToCollectionNamespaceMap, error) {
	var toDecrementMap metadata.ShaToCollectionNamespaceMap
	if addOp && len(shaToNsMapping) > 0 {
		b.logger.Warnf("Adding a new backfill spec %v should not have pre-existing mappings", spec.Id)
		toDecrementMap = shaToNsMapping.Clone()
	}

	err := b.InitCounterShaToActualMappings(spec.Id, spec.InternalId, shaToNsMapping)
	if err != nil {
		return nil, nil, err
	}

	consolidatedMap := make(metadata.ShaToCollectionNamespaceMap)
	spec.VBTasksMap.GetLock().RLock()
	for _, tasks := range spec.VBTasksMap.VBTasksMap {
		innerConsolidatedMap := tasks.GetAllCollectionNamespaceMappings()
		for sha, nsMap := range innerConsolidatedMap {
			if _, exists := consolidatedMap[sha]; !exists {
				consolidatedMap[sha] = nsMap
			}
		}
	}
	spec.VBTasksMap.GetLock().RUnlock()

	incrementer, err := b.GetIncrementerFunc(spec.Id)
	if err != nil {
		return nil, nil, err
	}

	for sha, mapping := range consolidatedMap {
		incrementer(sha, mapping)
	}

	if len(toDecrementMap) > 0 {
		decrementer, err := b.GetDecrementerFunc(spec.Id)
		if err != nil {
			b.logger.Errorf("Unable to get decrementerFunc for %v", spec.Id)
			return nil, nil, err
		}
		for sha := range toDecrementMap {
			decrementer(sha)
		}
	}

	return consolidatedMap, toDecrementMap, nil
}

// Each node is responsible for all VBs and all mappings now with P2P.
// This routine also initialises the in-memory data-structures (SHA reference counter and SHA to actual mappings)
// for the spec before persisting.
func (b *BackfillReplicationService) persistMappingsForThisNode(spec *metadata.BackfillReplicationSpec, addOp bool) error {
	mappingsDoc, err := b.GetMappingsDoc(spec.Id, addOp /*initIfNotFound*/)
	if err != nil {
		b.logger.Errorf("error while getting mapping doc for %v. err=%v (addOp=%v)",
			spec.Id, addOp, err)
		return err
	}

	inflatedMapping, err := b.GetShaToCollectionNsMap(spec.Id, (*metadata.CollectionNsMappingsDoc)(mappingsDoc))
	if err != nil {
		b.logger.Errorf("error while getting SHA to NS mapping for %v. err=%v (addOp=%v)",
			spec.Id, addOp, err)
		return err
	}

	incrementedMap, decrementedMap, err := b.loadSHARefMappings(spec, inflatedMapping, addOp)
	if err != nil {
		b.logger.Errorf("Error while calling loadSHARefMappings, err=%v, id=%v, add=%v", err, spec.Id, addOp)
		return err
	}

	err = b.UpsertMapping(spec.Id, spec.InternalId)
	if err != nil {
		b.logger.Errorf("BackfillReplService error upserting mapping for spec %v. addOp=%v, Inc=%v, Dec=%v",
			spec.Id, addOp, incrementedMap, decrementedMap)
	}
	return err
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

	err2 := b.setBackfillSpecUsingMarshalledData(spec, specValue)
	if err2 != nil {
		return err2
	}

	err = b.updateCache(spec.Id, spec)
	if err == nil {
		b.logger.Infof("BackfillReplication spec %s has been updated, rev=%v\n", spec.Id, spec.Revision)
	}

	return err
}

func (b *BackfillReplicationService) setBackfillSpecUsingMarshalledData(spec *metadata.BackfillReplicationSpec, specValue []byte) error {
	key := getBackfillReplicationDocKeyFunc(spec.Id)
	err := b.metadataSvc.Set(key, specValue, spec.Revision())
	if err != nil {
		return err
	}

	err = b.loadLatestMetakvRevisionIntoSpec(spec)
	if err != nil {
		return err
	}
	return nil
}

func (b *BackfillReplicationService) persistVBTasksMapDifferences(spec, oldSpec *metadata.BackfillReplicationSpec) error {
	currentVBTaskMapShaMappings := spec.VBTasksMap.GetAllCollectionNamespaceMappings()
	previousVBTaskMapShaMappings := oldSpec.VBTasksMap.GetAllCollectionNamespaceMappings()
	added, removed := currentVBTaskMapShaMappings.Diff(previousVBTaskMapShaMappings)
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
		err := b.UpsertMapping(spec.Id, spec.InternalId)
		if err != nil {
			return err
		}
	}
	return nil
}

// replicationId is the main replication ID
func (b *BackfillReplicationService) DelBackfillReplSpec(replicationId string) (*metadata.BackfillReplicationSpec, error) {
	_, err := b.backfillSpec(replicationId)
	backfillSpecCacheExists := err == nil

	stopFunc := b.utils.StartDiagStopwatch(fmt.Sprintf("DelBackfillReplSpec(%v)", replicationId), base.DiagInternalThreshold)
	defer stopFunc()

	key := getBackfillReplicationDocKeyFunc(replicationId)
	err = b.metadataSvc.Del(key, nil /*rev*/)
	if err != nil && err != service_def.MetadataNotFoundErr {
		b.logger.Errorf("Failed to delete backfill spec, key=%v, err=%v\n", key, err)
		return nil, err
	}

	// Once backfill spec has been deleted, safe to delete the backfill mappings
	err = b.CleanupMapping(replicationId, b.utils)
	if err != nil {
		b.logger.Errorf("Failed to cleanup backfill spec mapping err=%v\n", err)
		return nil, err
	}

	if backfillSpecCacheExists {
		err = b.updateCache(replicationId, nil)
		if err != nil {
			b.logger.Errorf("Failed to delete backfill spec, key=%v, err=%v\n", key, err)
			return nil, err
		}
	}

	b.logger.Infof("Backfill replication and spec mapping for %v cleaned up", replicationId)
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

	if updated {
		b.metadataChangeCbMtx.RLock()
		for _, cb := range b.metadataChangeCallbacks {
			cb(specId, oldSpec, newSpec)
		}
		b.metadataChangeCbMtx.RUnlock()
	}
	return err
}

func (b *BackfillReplicationService) ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}, wg *sync.WaitGroup) error {
	if wg != nil {
		defer wg.Done()
	}
	oldSpec, ok := oldVal.(*metadata.ReplicationSpecification)
	if !ok {
		return base.ErrorInvalidInput
	}
	newSpec, ok := newVal.(*metadata.ReplicationSpecification)
	if !ok {
		return base.ErrorInvalidInput
	}

	if oldSpec == nil && newSpec != nil {
		// Backfill Manager already watches this case
	} else if oldSpec != nil && newSpec == nil {
		_, err := b.DelBackfillReplSpec(id)
		if err == nil {
			b.logger.Infof("Deleted backfill replication %v", id)
		}
		if err == base.ReplNotFoundErr {
			err = nil
		}
		if err == nil {
			b.updateCache(id, nil)
		}
		return err
	} else {
		// It's possible that the changes have gone into the original(parent) replication spec.
		// This case is handled by the backfill manager
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

func cacheableMetadataObjToBackfillSpec(val CacheableMetadataObj) *metadata.BackfillReplicationSpec {
	rsv, ok := val.(*ReplicationSpecVal)
	if !ok {
		return nil
	}
	if rsv.spec == nil {
		return nil
	}
	genSpec, ok := rsv.spec.(metadata.GenericSpecification)
	if !ok {
		return nil
	}
	spec := genSpec.(*metadata.BackfillReplicationSpec)
	return spec
}

func (b *BackfillReplicationService) AllActiveBackfillSpecsReadOnly() (map[string]*metadata.BackfillReplicationSpec, error) {
	specs := make(map[string]*metadata.BackfillReplicationSpec)
	values_map := b.getCache().GetMap()
	for key, val := range values_map {
		spec := cacheableMetadataObjToBackfillSpec(val)
		if spec != nil && spec.ReplicationSpec() != nil && spec.ReplicationSpec().Settings.Active {
			specs[key] = spec
		}
	}
	return specs, nil
}

func (b *BackfillReplicationService) SetCompleteBackfillRaiser(backfillCallback func(specId string) error) error {
	b.completeBackfillCb = backfillCallback
	return nil
}

func (b *BackfillReplicationService) SetBackfillCkptsCleanupCb(backfillCallback func(specId string) error) error {
	b.backfillCkptsCleanupCb = backfillCallback
	return nil
}

func (b *BackfillReplicationService) handleUnrecoverableBackfillFatality(specId string) {
	msg := fmt.Sprintf("Backfill %v was not able to be loaded correctly. "+
		"A thorough backfill was tried, but was not possible. Please recreate the replication to ensure document count matches", specId)
	b.logger.Fatalf(msg)
	b.uiLogSvc.Write(msg)
}

// This method's job is to ensure that:
// 1. All the replications that need complete backfill have a cleanup: this happens synchronously.
// 2. Raise a complete backfill request for all such replications: this happens asynchronously.
func (b *BackfillReplicationService) RaiseUnrecoverableBackfillsIfNeeded() chan bool {
	initDoneCh := make(chan bool)
	if len(b.unrecoverableBackfillIds) == 0 {
		return initDoneCh
	}

	b.logger.Infof("Starting to handle unrecoverable backfills for %v", b.unrecoverableBackfillIds)
	atomic.StoreUint32(&b.recoveringBackfill, 1)

	// make sure the cleanup happens synchronously to avoid any races with future backfill requests
	// like P2P push, collection manifest diff backfill request or others.
	var replacementList [][2]string
	for _, backfillPair := range b.unrecoverableBackfillIds {
		backfillSpecId := backfillPair[0]
		internalId := backfillPair[1]

		backfillStateCleanup := func() error {
			err := b.CleanupMapping(backfillSpecId, b.utils)
			if err != nil {
				b.logger.Warnf("handleUnrecoverableBackfills received err %v when cleaning up mappings. Backfill may not raise correctly", backfillSpecId)
				return err
			}

			err = b.backfillCkptsCleanupCb(base.GetMainPipelineSpecIdFromBackfillId(backfillSpecId))
			if err != nil {
				b.logger.Warnf("Unable to clean up backfill pipeline checkpoint %v - %v", base.GetMainPipelineSpecIdFromBackfillId(backfillSpecId), err)
				return err
			}

			emptyBackfillSpec := metadata.NewBackfillReplicationSpec(backfillSpecId, internalId, metadata.NewVBTasksMap(), nil, 0)
			marshalledData, err := json.Marshal(emptyBackfillSpec)
			if err != nil {
				b.logger.Warnf("handleUnrecoverableBackfills received err %v when marshalling empty backfill spec. Backfill may not raise correctly", backfillSpecId)
				return err
			}

			err = b.setBackfillSpecUsingMarshalledData(emptyBackfillSpec, marshalledData)
			if err != nil {
				b.logger.Warnf("handleUnrecoverableBackfills received err %v when setting empty backfill spec into metakv. Backfill may not raise correctly", backfillSpecId)
				return err
			}

			return nil
		}

		err := b.utils.ExponentialBackoffExecutor(fmt.Sprintf("backfillStateCleanup(%v)", backfillSpecId), base.RetryIntervalMetakv,
			2*base.MaxNumOfMetakvRetries, base.MetaKvBackoffFactor, backfillStateCleanup)
		if err != nil {
			// we cannot go ahead with complete backfill for this replication anymore
			// since the cleanup was not successful.
			b.logger.Errorf("Unable to clear backfill state for backfill replication %v. err=%v", backfillSpecId, err)
			b.handleUnrecoverableBackfillFatality(backfillSpecId)
			continue
		}

		replacementList = append(replacementList, backfillPair)
	}

	b.unrecoverableBackfillIds = replacementList

	// now that we have successfully cleaned up the old backfill state,
	// we can asynchronously raise complete backfill requests.
	go b.raiseCompleteBackfillsReqsAfterCleanup(initDoneCh)
	return initDoneCh
}

// The key point here is to ensure that an empty backfill spec still exists,
// but the erroneous mapping files are all cleared
// The ensures that if at any point during this handling process XDCR is restarted, or if the backfill raise fails,
// then the init call will see that there's a backfill but the corresponding mapping file isn't there, and we
// will hit this RaiseUnrecoverableBackfillsIfNeeded method again.
// All backfill requests are serialised by the backfill request handler, so no real synchronisation needed
// to avoid race. The routine will keep retrying until all the replications that need complete backfill have
// completely persisted their requests in the backfill spec.
func (b *BackfillReplicationService) raiseCompleteBackfillsReqsAfterCleanup(initDoneCh chan bool) {
	// wait until b is fully initialised
	<-initDoneCh
	defer atomic.StoreUint32(&b.recoveringBackfill, 0)

	retryInterval := base.RetryIntervalMetakv
	backoffFactor := base.MetaKvBackoffFactor
	maxInterval := retryInterval * time.Duration(backoffFactor*2*base.MaxNumOfMetakvRetries)
	var injected bool

	for len(b.unrecoverableBackfillIds) > 0 {
		var shouldInjAndDelay bool

		var replacementList [][2]string
		// Once the mapping is cleared, then call the raiser to raise a complete backfill, which should be able to
		// properly write the spec and the corresponding mappings correctly
		for _, backfillPair := range b.unrecoverableBackfillIds {
			backfillSpecId := backfillPair[0]

			specToCheck, specCheckErr := b.replSpecSvc.ReplicationSpec(backfillSpecId)
			if specCheckErr == nil {
				shouldInjAndDelay = specToCheck.Settings.GetBoolSettingValue(metadata.DevBackfillUnrecoverableErrorInj)
				if shouldInjAndDelay {
					b.logger.Warnf("Dev inj spec has specified error injection %v", backfillSpecId)
				}
			}

			var err error
			if shouldInjAndDelay && !injected {
				// dev injection.
				injected = true
				err = fmt.Errorf("DevBackfillUnrecoverableErrorInj")
			} else {
				err = b.completeBackfillCb(backfillSpecId)
			}
			if err != nil {
				b.logger.Errorf("Unable to raise complete backfill for %v - %v. Will try again in %v", backfillSpecId, err, retryInterval)
				replacementList = append(replacementList, backfillPair)
			} else {
				msg := fmt.Sprintf("Backfill %v was not able to be loaded correctly. It has since been re-initialized and a thorough backfill has been raised to recover any missing data", backfillSpecId)
				b.logger.Infof(msg)
				b.uiLogSvc.Write(msg)
			}
		}

		b.unrecoverableBackfillIds = replacementList

		if shouldInjAndDelay && len(replacementList) > 0 {
			// dev injection.
			b.logger.Warnf("Dev inj sleeping for 90s for P2P push to kick in")
			time.Sleep(90 * time.Second)
		}

		time.Sleep(retryInterval)
		backoff := retryInterval * time.Duration(backoffFactor)
		if backoff < maxInterval {
			retryInterval = backoff
		}
	}
}

func (b *BackfillReplicationService) unitTestIsRecoveringBackfill() bool {
	return atomic.LoadUint32(&b.recoveringBackfill) == 1
}
