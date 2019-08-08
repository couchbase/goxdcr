package metadata_svc

import (
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"sync/atomic"
	"time"
)

type BucketManifestGetter struct {
	bucketName          string
	getterFunc          func(string) (*metadata.CollectionsManifest, error)
	lastQueryTime       time.Time
	lastStoredManifest  *metadata.CollectionsManifest
	firstCallerCh       chan bool
	checkInterval       time.Duration
	getterFuncOperating uint32
}

func NewBucketManifestGetter(bucketName string, manifestOps service_def.CollectionsManifestOps, checkInterval time.Duration) *BucketManifestGetter {
	getter := &BucketManifestGetter{
		bucketName:    bucketName,
		getterFunc:    manifestOps.CollectionManifestGetter,
		firstCallerCh: make(chan bool, 1),
		checkInterval: checkInterval,
	}
	// Allow one caller to call it
	getter.firstCallerCh <- true
	return getter
}

func (s *BucketManifestGetter) GetManifest() *metadata.CollectionsManifest {
	switch {
	case <-s.firstCallerCh:
		atomic.StoreUint32(&s.getterFuncOperating, 1)
		if time.Now().Sub(s.lastQueryTime) > (s.checkInterval) {
			// Prevent overwhelming the ns_server, only query every "checkInterval" seconds
			manifest, err := s.getterFunc(s.bucketName)
			if err == nil {
				s.lastStoredManifest = manifest
				s.lastQueryTime = time.Now()
			}
		}
		defer func() {
			atomic.StoreUint32(&s.getterFuncOperating, 0)
			s.firstCallerCh <- true
		}()
		return s.lastStoredManifest
	default:
		// Concurrent calls are really not common
		// so this rudimentary synchronization method should be ok
		// This is so that if a second caller calls after
		// the first caller, the second caller will be able to receive
		// the most up-to-date lastStoredManifest
		for atomic.LoadUint32(&s.getterFuncOperating) == 1 {
			time.Sleep(300 * time.Nanosecond)
		}
		return s.lastStoredManifest
	}
}
