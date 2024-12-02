package conflictlog

import (
	"sync"
	"sync/atomic"

	"github.com/couchbase/goxdcr/v8/metadata"
)

// NOTE: The cache implementation, although specific for colleciton manifest can be
// made generic to cache any value for a bucket. This is a todo for future.

// cachedObj is lock protected wrapper on the cached value
type cachedObj struct {
	man *metadata.CollectionsManifest
	// bucketUUID, vbCount & genCount are expected to be updated together
	genCount   int64
	bucketUUID string
	vbCount    int

	rw sync.RWMutex
}

// ManifestCache stores collection manifest for all buckets
// Reads on multiple buckets do not block each other
// Write to a bucket B1's manifest do not block other bucket's read or write. It will
// however, block reads of the B1's manifest
type ManifestCache struct {
	buckets map[string]*cachedObj
	rw      sync.RWMutex
}

func NewManifestCache() *ManifestCache {
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

func (m *ManifestCache) GetBucketInfo(bucket string) (bucketUUID string, vbCount int, ok bool) {
	obj := m.GetOrCreateCachedObj(bucket)

	obj.rw.RLock()
	defer obj.rw.RUnlock()

	if obj.bucketUUID == "" || obj.vbCount == 0 {
		return
	}

	bucketUUID = obj.bucketUUID
	vbCount = obj.vbCount
	ok = true

	return
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

func (m *ManifestCache) UpdateBucketInfo(bucket, bucketUUID string, vbCount int) {
	obj := m.GetOrCreateCachedObj(bucket)

	obj.rw.Lock()
	obj.bucketUUID = bucketUUID
	obj.vbCount = vbCount
	atomic.AddInt64(&obj.genCount, 1)
	obj.rw.Unlock()
}

// UpdateBucketInfoByFn updates the bucketUUID and vbCount for a bucket using a function
// Since the function most likely to have a some sort of I/O, multiple threads will not call
// the function repeatedly. This is ensured by the genCount.
func (m *ManifestCache) UpdateBucketInfoByFn(bucket string, fn func(bucketName string) (string, int, error)) (err error) {
	obj := m.GetOrCreateCachedObj(bucket)

	genCount := atomic.LoadInt64(&obj.genCount)

	obj.rw.Lock()
	defer obj.rw.Unlock()
	// If unequal then it means some other thread has already updated the UUID and vbCount
	// so no need to call it again. If the uuid or the vb count is not set we also allow
	// the thread to continue
	if genCount != atomic.LoadInt64(&obj.genCount) || obj.bucketUUID == "" || obj.vbCount == 0 {
		return
	}

	// Reset bucketUUID. If the function 'fn' fails it means the need to update the UUID
	// is still valid. We invalidate the UUID so that when GetBucketInfo is called it will
	// return "Not Found" and the caller will be force to fetch it again.
	obj.bucketUUID = ""
	obj.vbCount = 0

	bucketUUID, vbCount, err := fn(bucket)
	if err != nil {
		return
	}

	obj.bucketUUID = bucketUUID
	obj.vbCount = vbCount
	atomic.AddInt64(&obj.genCount, 1)
	return
}

// Delete removes the specified bucket from cache
func (m *ManifestCache) DeleteByUUID(bucketUUID string) {
	var bucketName string

	m.rw.RLock()
	for b, obj := range m.buckets {
		if obj.bucketUUID == bucketUUID {
			bucketName = b
			break
		}
	}
	m.rw.RUnlock()

	m.rw.Lock()
	delete(m.buckets, bucketName)
	m.rw.Unlock()

}
