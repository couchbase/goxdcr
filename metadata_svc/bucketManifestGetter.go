// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata_svc

import (
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
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
	getterFunc         func(string, bool, uint64, *metadata.ReplicationSpecification) (*metadata.CollectionsManifest, error)
	lastQueryTime      time.Time
	lastStoredManifest *metadata.CollectionsManifest
	checkInterval      time.Duration
	spec               *metadata.ReplicationSpecification // Used to subscribe to bucketTopoSvc for bucketInfo

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

func NewBucketManifestGetter(bucketName string, manifestOps service_def.CollectionsManifestOps, checkInterval time.Duration,
	spec *metadata.ReplicationSpecification) *BucketManifestGetter {
	getter := &BucketManifestGetter{
		bucketName:         bucketName,
		getterFunc:         manifestOps.CollectionManifestGetter,
		checkInterval:      checkInterval,
		lastStoredManifest: &defaultManifest,
		spec:               spec,
	}
	getter.callersCv = &sync.Cond{L: &getter.callersMtx}

	return getter
}

func (s *BucketManifestGetter) runGetOp() {
	if time.Now().Sub(s.lastQueryTime) > (s.checkInterval) {
		// Prevent overwhelming the ns_server, only query every "checkInterval" seconds
		var storedManifestUid uint64
		var hasStoredManifest bool
		if s.lastStoredManifest != nil {
			storedManifestUid = s.lastStoredManifest.Uid()
			hasStoredManifest = true
		} else {
			hasStoredManifest = false
		}
		manifest, err := s.getterFunc(s.bucketName, hasStoredManifest, storedManifestUid, s.spec)
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
