package conflictlog

import (
	"sync"

	"github.com/couchbase/goxdcr/v8/metadata"
)

// NOTE: The cache implementation, although specific for colleciton manifest can be
// made generic to cache any value for a bucket. This is a todo for future.

// cachedObj is lock protected wrapper on the cached value
type cachedObj struct {
	man *metadata.CollectionsManifest
	rw  sync.RWMutex
}

// ManifestCache stores collection manifest for all buckets
// Reads on multiple buckets do not block each other
// Write to a bucket B1's manifest do not block other bucket's read or write. It will
// however, block reads of the B1's manifest
type ManifestCache struct {
	buckets map[string]*cachedObj
	rw      sync.RWMutex
}

func newManifestCache() *ManifestCache {
	return &ManifestCache{
		buckets: map[string]*cachedObj{},
		rw:      sync.RWMutex{},
	}
}

func (m *ManifestCache) GetOrCreateCachedObj(bucket string) *cachedObj {
	m.rw.RLock()
	obj, ok := m.buckets[bucket]
	m.rw.RUnlock()
	if ok {
		return obj
	}

	m.rw.Lock()
	obj, ok = m.buckets[bucket]
	if ok {
		m.rw.Unlock()
		return obj
	}

	obj = &cachedObj{
		rw: sync.RWMutex{},
	}
	m.buckets[bucket] = obj
	m.rw.Unlock()

	return obj
}

func (m *ManifestCache) GetCollectionId(bucket, scope, collection string) (id uint32, ok bool) {
	obj := m.GetOrCreateCachedObj(bucket)

	obj.rw.RLock()
	defer obj.rw.RUnlock()

	if obj.man == nil {
		return 0, false
	}

	id, err := obj.man.GetCollectionId(scope, collection)
	if err != nil {
		return 0, false
	}

	return id, true
}

func (m *ManifestCache) UpdateManifest(bucket string, man *metadata.CollectionsManifest) {
	if man == nil {
		return
	}

	obj := m.GetOrCreateCachedObj(bucket)

	obj.rw.Lock()
	obj.man = man
	obj.rw.Unlock()
}
