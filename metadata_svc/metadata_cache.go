package metadata_svc

import (
	"github.com/couchbase/goxdcr/log"
	"sync"
	"sync/atomic"
)

type CacheableMetadataObj interface {
}

type MetadataCache struct {
	cache      *atomic.Value
	cache_lock *sync.Mutex
	logger     *log.CommonLogger
}

func NewMetadataCache(logger *log.CommonLogger) *MetadataCache {
	metadata_cache := &MetadataCache{cache: &atomic.Value{},
		cache_lock: &sync.Mutex{},
		logger:     logger}
	metadata_cache.cache.Store(make(map[string]CacheableMetadataObj))
	return metadata_cache
}
func (cache *MetadataCache) Get(key string) (CacheableMetadataObj, bool) {
	val_map := cache.GetMap()
	val, ok := val_map[key]
	return val, ok
}

func (cache *MetadataCache) GetMap() map[string]CacheableMetadataObj {
	return cache.cache.Load().(map[string]CacheableMetadataObj)
}

func (cache *MetadataCache) Upsert(key string, val CacheableMetadataObj) {
	cache.cache_lock.Lock()
	defer cache.cache_lock.Unlock()

	current_val_map := cache.GetMap()
	new_val_map := make(map[string]CacheableMetadataObj)
	for k, v := range current_val_map {
		new_val_map[k] = v
	}

	new_val_map[key] = val
	cache.cache.Store(new_val_map)
	cache.logger.Debugf("Done with upserting key=%v, val=%v, cache val=%v\n", key, val, cache.cache)
}

func (cache *MetadataCache) Delete(key string) {
	cache.cache_lock.Lock()
	defer cache.cache_lock.Unlock()

	current_val_map := cache.GetMap()
	new_val_map := make(map[string]CacheableMetadataObj)
	for k, v := range current_val_map {
		if k != key {
			new_val_map[k] = v
		}
	}

	cache.cache.Store(new_val_map)
}
