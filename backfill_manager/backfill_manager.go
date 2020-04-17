// Copyright (c) 2013-2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package backfill_manager

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"sync"
)

// Checkpoint manager can only persist a manifest if and only if any broken maps have already been
// raised and cause backfills to occur.
// The backfill's manager's secondary job is to cover backfill situations that the broken map couldn't detect.
// This includes:
// 1. Collection on the target side gets deleted and recreated quickly and did not cause any broken-ness.
//    Backfill Manger can detect this if the manifest diff shows different collection IDs for the same collectionName.
//    This means that Backfill Manager should backfill from 0 to throughSeqno.
// 2. When a new target side collection is recreated and no new I/O was created (similar to 1). Backfill needs to be created.
// 3. If a backfill is currently under way and (1) occurs again, the backfill manager needs to restart backfill from 0 to throughSeqno
//
// Otherwise, if a backfill has already been requested by the router, and the collection ID hasn't changed since the requested
// target manifest ID, then it's a no-op.
//
// For example - at target manifest 5, router requests a backfill for target collection c1 of id 8
// Later, manifest changed to 7, and the target collection c1 of remains 8, it's a no-op.
// If the manifest changed to 9, and the target collection c1 changed to 12, and no brokenmap event raised, it is case (3) above.
type BackfillMgr struct {
	collectionsManifestSvc service_def.CollectionsManifestSvc
	replSpecSvc            service_def.ReplicationSpecSvc
	backfillReplSvc        service_def.BackfillReplSvc

	logger *log.CommonLogger

	// Collections Manifest Service will send down the latest changes from what it sees
	// Backfill Manager still needs to make decisions based on what it knows from last
	// time depending on source and target. These are what the caches are used for
	cacheMtx           sync.RWMutex
	cacheSpecSourceMap map[string]*metadata.CollectionsManifest
	cacheSpecTargetMap map[string]*metadata.CollectionsManifest
}

func NewBackfillManager(collectionsManifestSvc service_def.CollectionsManifestSvc,
	replSpecSvc service_def.ReplicationSpecSvc,
	backfillReplSvc service_def.BackfillReplSvc) *BackfillMgr {

	backfillMgr := &BackfillMgr{
		collectionsManifestSvc: collectionsManifestSvc,
		replSpecSvc:            replSpecSvc,
		backfillReplSvc:        backfillReplSvc,
		logger:                 log.NewLogger("BackfillMgr", log.DefaultLoggerContext),
		cacheSpecSourceMap:     make(map[string]*metadata.CollectionsManifest),
		cacheSpecTargetMap:     make(map[string]*metadata.CollectionsManifest),
	}

	return backfillMgr
}

func (b *BackfillMgr) Start() error {
	b.logger.Infof("BackfillMgr Starting...")

	b.collectionsManifestSvc.SetMetadataChangeHandlerCallback(b.collectionsManifestChangeCb)
	b.replSpecSvc.SetMetadataChangeHandlerCallback(b.replicationSpecChangeHandlerCallback)
	b.backfillReplSvc.SetMetadataChangeHandlerCallback(b.backfillReplSpecChangeHandlerCallback)

	err := b.initCache()
	if err != nil {
		return err
	}

	b.logger.Infof("BackfillMgr Started")
	return nil
}

func (b *BackfillMgr) Stop() {
	b.logger.Infof("BackfillMgr Stopping...")
	// TODO - stop handlers
	b.logger.Infof("BackfillMgr Stopped")
}

func (b *BackfillMgr) initCache() error {
	// Backfill Manager needs to "monitor" for every single ongoing replication's source and target ID
	// in additional to just the backfill replications
	// Such that it can launch backfill as needed
	replSpecMap, _ := b.replSpecSvc.AllReplicationSpecs()
	for replId, spec := range replSpecMap {
		err := b.retrieveLastPersistedManifest(spec)
		if err != nil {
			b.logger.Errorf("Retrieving manifest for spec %v returned %v", replId, err)
			// Fill it up with default manifest
			b.cacheMtx.Lock()
			defaultManifest := metadata.NewDefaultCollectionsManifest()
			b.cacheSpecSourceMap[replId] = &defaultManifest
			b.cacheSpecTargetMap[replId] = &defaultManifest
			b.cacheMtx.Unlock()
			// TODO - MB-38868
		}
	}
	return nil
}

func (b *BackfillMgr) retrieveLastPersistedManifest(spec *metadata.ReplicationSpecification) error {
	manifestPair, err := b.collectionsManifestSvc.GetLastPersistedManifests(spec)
	if err != nil {
		return err
	}
	b.cacheMtx.Lock()
	b.logger.Infof("Backfill Manager for replication %v received last persisted manifests of %v and %v",
		spec.Id, manifestPair.Source, manifestPair.Target)
	b.cacheSpecSourceMap[spec.Id] = manifestPair.Source
	b.cacheSpecTargetMap[spec.Id] = manifestPair.Target
	b.cacheMtx.Unlock()
	return nil
}

func (b *BackfillMgr) backfillReplSpecChangeHandlerCallback(changedSpecId string, oldSpecObj interface{}, newSpecObj interface{}) error {
	oldSpec, ok := oldSpecObj.(*metadata.BackfillReplicationSpec)
	newSpec, ok2 := newSpecObj.(*metadata.BackfillReplicationSpec)
	if !ok || !ok2 {
		return base.ErrorInvalidInput
	}
	b.logger.Infof("Backfill spec change callback for %v detected old %v new %v", changedSpecId, oldSpec, newSpec)
	// TODO - logics to determine what to do about current ongoing backfill tasks
	// i.e. combine? Optimize? start? stop? simply just queue?
	return nil
}

func (b *BackfillMgr) replicationSpecChangeHandlerCallback(changedSpecId string, oldSpecObj interface{}, newSpecObj interface{}) error {
	oldSpec, ok := oldSpecObj.(*metadata.ReplicationSpecification)
	newSpec, ok2 := newSpecObj.(*metadata.ReplicationSpecification)

	if !ok || !ok2 {
		b.logger.Errorf("BackfillMgr error converting oldSpec: %v newSpec: %v", ok, ok2)
		return base.ErrorInvalidInput
	}

	b.logger.Infof("BackfillMgr detected specId %v old %v new %v", changedSpecId, oldSpec, newSpec)

	// TODO - these should be handled by a different go routine as to not block
	if oldSpecObj.(*metadata.ReplicationSpecification) == nil &&
		newSpecObj.(*metadata.ReplicationSpecification) != nil {
		// TODO - need to create a backfill request handler
	} else if newSpecObj.(*metadata.ReplicationSpecification) == nil &&
		oldSpecObj.(*metadata.ReplicationSpecification) != nil {
		// Delete internal caches
		b.cacheMtx.Lock()
		delete(b.cacheSpecSourceMap, changedSpecId)
		delete(b.cacheSpecTargetMap, changedSpecId)
		b.cacheMtx.Unlock()

		// TODO - need to stop backfill request handler
		// Backfill Replication service will delete backfill specs if there is one
	}

	return nil
}

func (b *BackfillMgr) collectionsManifestChangeCb(replId string, oldVal, newVal interface{}) error {
	oldManifests, ok := oldVal.(*metadata.CollectionsManifestPair)
	if !ok {
		return base.ErrorInvalidInput
	}

	newManifests, ok := newVal.(*metadata.CollectionsManifestPair)
	if !ok {
		return base.ErrorInvalidInput
	}

	var srcErr error
	var tgtErr error

	// Handle source
	if oldManifests.Source == nil && newManifests.Source != nil {
		b.logger.Infof("Source manifest did not exist, now it has: %v\n", newManifests.Source.String())
	} else if oldManifests.Source != nil && newManifests.Source == nil {
		// Don't think it's possible...
		b.logger.Infof("Source manifest has been deleted")
		srcErr = base.ErrorInvalidInput
	}

	// Handle target
	if oldManifests.Target == nil && newManifests.Target != nil {
		b.logger.Infof("Target manifest did not exist, now it has: %v\n", newManifests.Target.String())
	} else if oldManifests.Target != nil && newManifests.Target == nil {
		// Don't think it's possible...
		b.logger.Infof("Target manifest has been deleted")
		tgtErr = base.ErrorInvalidInput
	} else {
		_, _, _, tgtErr = newManifests.Target.Diff(oldManifests.Target)
		if tgtErr != nil {
			b.logger.Errorf("Unable to diff between target manifests: %v", tgtErr.Error())
		}
	}

	if tgtErr != nil && srcErr != nil {
		if srcErr != nil {
			return srcErr
		} else {
			return tgtErr
		}
	}

	b.handleManifestsChanges(replId, oldManifests, newManifests)
	return nil
}

func (b *BackfillMgr) handleManifestsChanges(replId string, oldManifests, newManifests *metadata.CollectionsManifestPair) {
	if b == nil || oldManifests == nil || newManifests == nil {
		return
	}

	if newManifests.Source == nil && newManifests.Target == nil {
		// Nothing changed
		return
	} else if newManifests.Source != nil && newManifests.Target == nil {
		// Source changed but target did not
		b.handleSourceOnlyChange(replId, newManifests.Source)
	} else if newManifests.Source == nil && newManifests.Target != nil {
		// Source did not change but target did change
		b.handleTargetChanges(replId, nil /*source*/, oldManifests.Target, newManifests.Target)
	} else {
		// Both changed
		// First handle source change then target change
		b.handleTargetChanges(replId, newManifests.Source, oldManifests.Target, newManifests.Target)
	}
}

func (b *BackfillMgr) handleSourceOnlyChange(replId string, sourceManifest *metadata.CollectionsManifest) {
	// Nothing to do for now - TODO - mirroring policy handler will be here
	// Last step - update internal cache
	b.cacheMtx.Lock()
	b.cacheSpecSourceMap[replId] = sourceManifest
	b.cacheMtx.Unlock()
}

func (b *BackfillMgr) handleTargetChanges(replId string, sourceManifest, oldTargetManifest, newTargetManifest *metadata.CollectionsManifest) {
	// For added and modified since last time, need to launch backfill if they are mapped from source
	var sourceChanged bool = sourceManifest != nil

	if sourceManifest == nil {
		b.cacheMtx.RLock()
		sourceManifest = b.cacheSpecSourceMap[replId]
		b.cacheMtx.RUnlock()

		if sourceManifest == nil {
			panic("FIXME")
		}
	}

	if sourceManifest.Uid() == 0 {
		// Source manifest 0 means only default collection is being replicated
		// If it's implicit mapping, no-op
		// TODO - this section will change for explicit mapping and/or upgrade
		return
	}

	backfillMapping, err := newTargetManifest.ImplicitGetBackfillCollections(oldTargetManifest, sourceManifest)
	if err != nil {
		b.logger.Errorf("handleTargetChanges error: %v", err)
		return
	}

	// Update cache
	b.cacheMtx.Lock()
	if sourceChanged {
		b.cacheSpecSourceMap[replId] = sourceManifest
	}
	b.cacheSpecTargetMap[replId] = newTargetManifest
	b.cacheMtx.Unlock()

	b.logger.Infof("These collections need to backfill %v to seqno TBD", backfillMapping)
}

func (b *BackfillMgr) getThroughSeqno(replId string) (map[uint16]uint64, error) {
	// TODO
	return nil, nil
}
