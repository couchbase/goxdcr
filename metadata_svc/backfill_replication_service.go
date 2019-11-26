// Copyright (c) 2013-2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// metadata service implementation leveraging gometa

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

const BackfillParentCatalogKey = "backfill"

// TODO - consolidate
//type ReplicationSpecVal struct {
//	spec       *metadata.BackfillReplicationSpec
//	derivedObj interface{}
//	cas        int64
//}
//
//func (rsv *ReplicationSpecVal) CAS(obj CacheableMetadataObj) bool {
//	if rsv == nil || obj == nil {
//		return true
//	} else if rsv2, ok := obj.(*ReplicationSpecVal); ok {
//		if rsv.cas == rsv2.cas {
//			if !rsv.spec.SameSpec(rsv2.spec) {
//				// increment cas only when the metadata portion of ReplicationSpecVal has been changed
//				// in other words, concurrent updates to the metadata portion is not allowed -- later write fails
//				// while concurrent updates to the runtime portion is allowed -- later write wins
//				rsv.cas++
//			}
//			return true
//		} else {
//			return false
//		}
//	} else {
//		return false
//	}
//}
//
//func (rsv *ReplicationSpecVal) Clone() CacheableMetadataObj {
//	if rsv != nil {
//		clonedRsv := &ReplicationSpecVal{}
//		*clonedRsv = *rsv
//		if rsv.spec != nil {
//			clonedRsv.spec = rsv.spec.Clone()
//		}
//		return clonedRsv
//	}
//	return rsv
//}
//
//func (rsv *ReplicationSpecVal) Redact() CacheableMetadataObj {
//	if rsv != nil && rsv.spec != nil {
//		rsv.spec.Redact()
//	}
//	return rsv
//}
//
//func (rsv *ReplicationSpecVal) CloneAndRedact() CacheableMetadataObj {
//	if rsv != nil {
//		return rsv.Clone().Redact()
//	}
//	return rsv
//}

type BackfillReplicationService struct {
	//	xdcrCompTopologySvc service_def.XDCRCompTopologySvc
	metadataSvc service_def.MetadataSvc
	uiLogSvc    service_def.UILogSvc
	//	remoteClusterSvc    service_def.RemoteClusterSvc
	//	clusterInfoSvc      service_def.ClusterInfoSvc
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
		//		xdcrCompTopologySvc: xdcrCompTopologySvc,
		metadataSvc: metadataSvc,
		//		uiLogSvc:            uiLogSvc,
		//		remoteClusterSvc:    remoteClusterSvc,
		//		clusterInfoSvc:      clusterInfoSvc,
		logger:      logger,
		replSpecSvc: replSpecSvc,
		utils:       utilsIn,
	}

	return svc, svc.initCacheFromMetaKV()
}

func (b *BackfillReplicationService) getKeyFromReplicationId(replicationId string) string {
	return BackfillParentCatalogKey + base.KeyPartsDelimiter + replicationId
}

func (b *BackfillReplicationService) getReplicationIdFromKey(key string) string {
	prefix := BackfillParentCatalogKey + base.KeyPartsDelimiter
	if !strings.HasPrefix(key, prefix) {
		// should never get here.
		panic(fmt.Sprintf("Got unexpected key %v for backfill replication spec", key))
	}
	return key[len(prefix):]
}

func (b *BackfillReplicationService) initCache() {
	b.cache = NewMetadataCache(b.logger)
	b.logger.Info("Cache has been initialized for BackfillReplicationService")
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
		replSpec, err := b.constructReplicationSpec(marshalledSpec, rev, false /*lock*/)
		if err != nil {
			b.logger.Errorf("Unable to construct spec %v from metaKV's data. err: %v", key, err)
			continue
		}
		b.updateCacheInternal(replicationId, replSpec, false /*lock*/)
	}

	return nil
}

func (b *BackfillReplicationService) updateCache(specId string, newSpec *metadata.BackfillReplicationSpec) error {
	return b.updateCacheInternal(specId, newSpec, true /*lock*/)
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

func (b *BackfillReplicationService) getCache() *MetadataCache {
	if b.cache == nil {
		panic("Cache has not been initialized for ReplicationSpecService")
	}
	return b.cache
}

func (b *BackfillReplicationService) ReplicationSpec(replicationId string) (*metadata.BackfillReplicationSpec, error) {
	return b.replicationSpec(replicationId)
}

func (b *BackfillReplicationService) replicationSpec(replicationId string) (*metadata.BackfillReplicationSpec, error) {
	val, ok := b.getCache().Get(replicationId)
	if !ok || val == nil || val.(*ReplicationSpecVal).spec.(*metadata.BackfillReplicationSpec) == nil {
		return nil, errors.New(ReplicationSpecNotFoundErrorMessage)
	}

	return val.(*ReplicationSpecVal).spec.(*metadata.BackfillReplicationSpec), nil
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

func (b *BackfillReplicationService) updateCacheInternalNoLock(specId string, newSpec *metadata.BackfillReplicationSpec) (*metadata.BackfillReplicationSpec, bool, error) {
	oldSpec, err := b.replicationSpec(specId)
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
		if !newSpec.SameSpec(oldSpec) {
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

func (b *BackfillReplicationService) constructReplicationSpec(value []byte, rev interface{}, lock bool) (*metadata.BackfillReplicationSpec, error) {
	if value == nil {
		return nil, nil
	}

	spec := &metadata.BackfillReplicationSpec{}
	err := json.Unmarshal(value, spec)
	if err != nil {
		return nil, err
	}

	return spec, nil
}

func (b *BackfillReplicationService) AddReplicationSpec(spec *metadata.BackfillReplicationSpec) error {
	if spec == nil {
		return base.ErrorInvalidInput
	}
	b.logger.Infof("Adding backfill spec %v to metadata store", spec)

	value, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	key := b.getKeyFromReplicationId(spec.Id)
	err = b.metadataSvc.Add(key, value)
	if err != nil {
		b.logger.Errorf("Add returned error: %v\n", err)
		return err
	}

	return b.updateCache(spec.Id, spec)
}

func (b *BackfillReplicationService) SetReplicationSpec(spec *metadata.BackfillReplicationSpec) error {
	if spec == nil {
		return base.ErrorInvalidInput
	}
	b.logger.Infof("Adding backfill spec %v to metadata store", spec)

	value, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	key := b.getKeyFromReplicationId(spec.Id)
	err = b.metadataSvc.Set(key, value, nil)
	if err != nil {
		b.logger.Errorf("Set returned error: %v\n", err)
		return err
	}

	return b.updateCache(spec.Id, spec)

}
