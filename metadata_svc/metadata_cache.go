/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadata_svc

import (
	"errors"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"sync"
	"sync/atomic"
)

var CASMisMatchError = errors.New("CAS does not match")

type CacheableMetadataObj interface {
	// checks if the current object has the same CAS as that of the passed in object
	// if CAS is the same, increment CAS of current object and return true
	// otherwise, return false
	CAS(obj CacheableMetadataObj) bool
	Clone() CacheableMetadataObj
	Redact() CacheableMetadataObj
	CloneAndRedact() CacheableMetadataObj
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

func (cache *MetadataCache) Upsert(key string, val CacheableMetadataObj) error {
	cache.cache_lock.Lock()
	defer cache.cache_lock.Unlock()

	current_val_map := cache.GetMap()
	current_val := current_val_map[key]
	// check CAS
	if !val.CAS(current_val) {
		cache.logger.Errorf("CAS mismatch. cur_val=%v, val=%v\n", current_val, val)
		return CASMisMatchError
	}

	new_val_map := make(map[string]CacheableMetadataObj)
	for k, v := range current_val_map {
		new_val_map[k] = v
	}

	new_val_map[key] = val
	cache.cache.Store(new_val_map)

	if cache.logger.GetLogLevel() >= log.LogLevelDebug {
		cache.logger.Debugf("Done with upserting key=%v, val=%v, cache val=%v%v%v\n", key, val.CloneAndRedact(),
			base.UdTagBegin, new_val_map, base.UdTagEnd)
	}
	return nil
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
