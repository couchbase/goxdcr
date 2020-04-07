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
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"strings"
	"sync"
)

const BackfillKey = "backfill"

var BackfillParentCatalogKey = CheckpointsCatalogKeyPrefix + base.KeyPartsDelimiter + BackfillKey

type BackfillReplicationService struct {
	metadataSvc service_def.MetadataSvc
	uiLogSvc    service_def.UILogSvc
	replSpecSvc service_def.ReplicationSpecSvc
	logger      *log.CommonLogger
	utils       utilities.UtilsIface
	cache       *MetadataCache
	cache_lock  sync.Mutex

	metadataChangeCallbacks []base.MetadataChangeHandlerCallback
	metadataChangeCbMtx     sync.RWMutex
}

func NewBackfillReplicationService(uiLogSvc service_def.UILogSvc,
	metadataSvc service_def.MetadataSvc,
	loggerCtx *log.LoggerContext,
	utilsIn utilities.UtilsIface,
	replSpecSvc service_def.ReplicationSpecSvc) (*BackfillReplicationService, error) {
	logger := log.NewLogger("BackfillReplSvc", loggerCtx)
	svc := &BackfillReplicationService{
		metadataSvc: metadataSvc,
		logger:      logger,
		replSpecSvc: replSpecSvc,
		utils:       utilsIn,
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

func (b *BackfillReplicationService) updateCacheInternal(specId string, newSpec *metadata.BackfillReplicationSpec, lock bool) error {
	if lock {
		b.cache_lock.Lock()
		defer b.cache_lock.Unlock()
	}

	oldSpec, updated, err := b.updateCacheInternalNoLock(specId, newSpec)

	var lastErr error
	b.metadataChangeCbMtx.RLock()
	if updated && len(b.metadataChangeCallbacks) > 0 {
		for _, cb := range b.metadataChangeCallbacks {
			err = cb(specId, oldSpec, newSpec)
			if err != nil {
				b.logger.Error(err.Error())
				lastErr = err
			}
		}
	}
	b.metadataChangeCbMtx.RUnlock()

	return lastErr
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
		if !newSpec.SameAs(oldSpec) {
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
