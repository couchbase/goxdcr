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
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"sync"
	"time"
)

/**
 * The bucket manifest getter's purpose is to allow other service to specify a getter function
 * and then to do burst-control to prevent too many calls within a time period to overload
 * the manifest provider (ns_server). It will still release the latest pulled manifest
 * to every single caller. Because manifests are eventual consistent (i.e. KV can send down
 * events before ns_server has it for pulling), a little lag shouldn't hurt anyone
 */
type BucketManifestGetter struct {
	bucketName         string
	getterFunc         func(string) (*metadata.CollectionsManifest, error)
	lastQueryTime      time.Time
	lastStoredManifest *metadata.CollectionsManifest
	checkInterval      time.Duration

	// Handling of multiple getters
	callersMtx  sync.Mutex
	callersCv   *sync.Cond
	callersCnt  uint32
	getterState int
}

const (
	stateNone        int = iota
	stateRunning     int = iota
	stateDoneRunning int = iota
)

func NewBucketManifestGetter(bucketName string, manifestOps service_def.CollectionsManifestOps, checkInterval time.Duration) *BucketManifestGetter {
	getter := &BucketManifestGetter{
		bucketName:         bucketName,
		getterFunc:         manifestOps.CollectionManifestGetter,
		checkInterval:      checkInterval,
		lastStoredManifest: &defaultManifest,
	}
	getter.callersCv = &sync.Cond{L: &getter.callersMtx}

	return getter
}

func (s *BucketManifestGetter) runGetOp() {
	if time.Now().Sub(s.lastQueryTime) > (s.checkInterval) {
		// Prevent overwhelming the ns_server, only query every "checkInterval" seconds
		manifest, err := s.getterFunc(s.bucketName)
		if err == nil {
			s.lastStoredManifest = manifest
			s.lastQueryTime = time.Now()
		}
	}
	s.callersMtx.Lock()
	s.getterState = stateDoneRunning
	s.callersMtx.Unlock()
	s.callersCv.Broadcast()
	return
}

// Concurrent calls are really not common
// This is so that if a second caller calls after
// the first caller, the second caller will be able to receive
// the most up-to-date lastStoredManifest
func (s *BucketManifestGetter) GetManifest() *metadata.CollectionsManifest {
	s.callersMtx.Lock()
	defer s.callersMtx.Unlock()
	s.callersCnt++
	switch s.getterState {
	case stateNone:
		// First caller
		s.getterState = stateRunning
		go s.runGetOp()
		for s.getterState == stateRunning {
			s.callersCv.Wait()
		}
	case stateRunning:
		// A procedure is running
		// Wait for it to be done running and return the results
		for s.getterState == stateRunning {
			s.callersCv.Wait()
		}
	case stateDoneRunning:
		// Caught the last seat on the train
		// Do nothing and just return results
	}
	s.callersCnt--

	if s.callersCnt > 0 {
		// There are other callers that need to be woken up
		defer func() {
			s.callersCv.Broadcast()
		}()
	} else {
		// last guy to turn off the lights
		s.getterState = stateNone
	}
	return s.lastStoredManifest
}
