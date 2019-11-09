// Copyright (c) 2013-2019 Couchbase, Inc.
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

type BackfillMgr struct {
	collectionsManifestSvc service_def.CollectionsManifestSvc
	exitFunc               base.ParentExitFunc
	replSpecSvc            service_def.ReplicationSpecSvc

	logger *log.CommonLogger

	childrenWaitgrp sync.WaitGroup
	cancelChan      chan struct{}

	cleanupOnce sync.Once

	cacheMtx           sync.RWMutex
	cacheSpecSourceMap map[string]*metadata.CollectionsManifest
	cacheSpecTargetMap map[string]*metadata.CollectionsManifest
}

func (b *BackfillMgr) backfillMgrExitFunc(force bool) {

	if !force {
		b.cleanup()
	}
}

func (b *BackfillMgr) cleanup() {
	b.cleanupOnce.Do(func() {
		b.logger.Infof("BackfillMgr exiting")
		b.Stop()
		b.cancelChan <- struct{}{}
		b.logger.Infof("BackfillMgr exited")
	})
}

func NewBackfillManager(collectionsManifestSvc service_def.CollectionsManifestSvc,
	parentExitFunc base.ParentExitFunc,
	replSpecSvc service_def.ReplicationSpecSvc) *BackfillMgr {

	backfillMgr := &BackfillMgr{
		collectionsManifestSvc: collectionsManifestSvc,
		replSpecSvc:            replSpecSvc,
		cancelChan:             make(chan struct{}, 1),
		logger:                 log.NewLogger("BackfillMgr", log.DefaultLoggerContext),
		cacheSpecSourceMap:     make(map[string]*metadata.CollectionsManifest),
		cacheSpecTargetMap:     make(map[string]*metadata.CollectionsManifest),
	}

	wrappedExitFunc := func(force bool) {
		backfillMgr.backfillMgrExitFunc(force)
		parentExitFunc(force)
	}

	// TODO - to be used by future metadata change monitors
	backfillMgr.exitFunc = wrappedExitFunc

	return backfillMgr
}

func (b *BackfillMgr) Start() {
	b.logger.Infof("BackfillMgr Starting...")
	b.initMetadataChangeMonitor()

	b.collectionsManifestSvc.SetMetadataChangeHandlerCallback(b.collectionsManifestChangeCb)
	b.replSpecSvc.SetMetadataChangeHandlerCallback(b.replicationSpecChangeHandlerCallback)

	b.initCache()
	b.logger.Infof("BackfillMgr Started")
}

func (b *BackfillMgr) Stop() {
	b.logger.Infof("BackfillMgr Stopping...")

	b.logger.Infof("BackfillMgr Stopped")
}

func (b *BackfillMgr) initMetadataChangeMonitor() {
	// TODO - future metadata change monitors
	//	mcm := base.NewMetadataChangeMonitor()
}

func (b *BackfillMgr) initCache() {
	// The following doesn't return a non-nil error for now
	replSpecMap, _ := b.replSpecSvc.AllReplicationSpecs()
	for replId, spec := range replSpecMap {
		if spec == nil {
			continue
		}
		manifestPair, err := b.collectionsManifestSvc.GetLastPersistedManifests(spec)
		if err != nil {
			b.logger.Errorf("Retrieving manifest for spec %v returned %v", replId, err)
			continue
		}
		b.cacheMtx.Lock()
		b.cacheSpecSourceMap[replId] = manifestPair.Source
		b.cacheSpecTargetMap[replId] = manifestPair.Target
		b.logger.Infof("NEIL DEBUG for replicationID: %v retrieved manifests for source: %v target: %v", replId, manifestPair.Source, manifestPair.Target)
		b.cacheMtx.Unlock()
	}
}

func (b *BackfillMgr) replicationSpecChangeHandlerCallback(changedSpecId string, oldSpecObj interface{}, newSpecObj interface{}) error {
	oldSpec, ok := oldSpecObj.(*metadata.ReplicationSpecification)
	newSpec, ok2 := newSpecObj.(*metadata.ReplicationSpecification)

	if !ok || !ok2 {
		b.logger.Errorf("BackfillMgr error converting oldSpec: %v newSpec: %v", ok, ok2)
		return base.ErrorInvalidInput
	}

	b.logger.Infof("BackfillMgr detected specId %v old %v new %v", changedSpecId, oldSpec, newSpec)

	b.collectionsManifestSvc.ReplicationSpecChangeCallback(changedSpecId, oldSpecObj, newSpecObj)

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
		//		b.logger.Infof(fmt.Sprintf("NEIL DEBUG Target Added or removed:\n%v\nRemoved:\n%v\n", addedOrModified.String(), removed.String()))
	}

	if tgtErr != nil && srcErr != nil {
		if srcErr != nil {
			return srcErr
		} else {
			return tgtErr
		}
	}

	if newManifests.Source == nil && newManifests.Target == nil {
		// Nothing changed
	} else if newManifests.Source != nil && newManifests.Target == nil {
		// Source changed but target did not
		// TODO
		b.logger.Infof("NEIL DEBUG source only change")

	} else if newManifests.Source == nil && newManifests.Target != nil {
		// Source did not change but target did change
		b.handleTargetOnlyChange(replId, oldManifests.Target, newManifests.Target)
	} else {
		// Both changed
		b.logger.Infof("NEIL DEBUG both change")
	}

	return nil
}

func (b *BackfillMgr) handleTargetOnlyChange(replId string, oldTargetManifest, newTargetManifest *metadata.CollectionsManifest) {
	// For added and modified since last time, need to launch backfill if they are mapped from source
	b.cacheMtx.RLock()
	sourceManifest, ok := b.cacheSpecSourceMap[replId]
	b.cacheMtx.RUnlock()
	if !ok {
		panic("TODO")
	}

	backfillIDs, err := newTargetManifest.GetBackfillCollectionIDs(oldTargetManifest, sourceManifest)
	if err != nil {
		b.logger.Errorf("handleTargetOnlyChange error: %v", err)
		return
	}

	b.logger.Infof("These collections Need to backfill %v", backfillIDs)

	// Store the target
	b.cacheMtx.Lock()
	b.cacheSpecTargetMap[replId] = newTargetManifest
	b.cacheMtx.Unlock()
}

func (b *BackfillMgr) handleSourceOnlyChange(replId string, oldSourceManifest, newSourceManifest *metadata.CollectionsManifest) {
	b.cacheMtx.RLock()
	targetManifest, ok := b.cacheSpecTargetMap[replId]
	b.cacheMtx.RUnlock()
	if !ok {
		panic("TODO")
	}

	_, _, origUnmappedTargetCols := oldSourceManifest.MapAsSourceToTargetByName(targetManifest)
	_, _, nowUnmappedTargetCols := newSourceManifest.MapAsSourceToTargetByName(targetManifest)

	_, missingFromOrig := nowUnmappedTargetCols.Diff(origUnmappedTargetCols)

	// Perhaps do not need backfill
	b.logger.Infof("These source->target collection are now mapped: %v\n", missingFromOrig)

	b.cacheMtx.Lock()
	b.cacheSpecSourceMap[replId] = newSourceManifest
	b.cacheMtx.Unlock()
}
