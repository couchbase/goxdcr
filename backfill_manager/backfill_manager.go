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
	"github.com/couchbase/goxdcr/pipeline"
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
	for replId, _ := range replSpecMap {
		err := b.refreshLatestManifest(replId)
		if err != nil {
			b.logger.Errorf("Retrieving manifest for spec %v returned %v", replId, err)
			continue
	}
}

// We call this "latestManifest" because it is used to determine the data that needs to be backfilled
// It is safer to backfill already backfilled-data than to have miss data at all
func (b *BackfillMgr) refreshLatestManifest(replId string) error {
	// CollectionsManifestService only cares about replication ID within the spec
	idOnlySpec := &metadata.ReplicationSpecification{Id: replId}
	manifestPair, err := b.collectionsManifestSvc.GetLastPersistedManifests(idOnlySpec)
	if err != nil {
		return err
	}
	b.cacheMtx.Lock()
	b.cacheSpecSourceMap[replId] = manifestPair.Source
	b.cacheSpecTargetMap[replId] = manifestPair.Target
	b.cacheMtx.Unlock()
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

	b.collectionsManifestSvc.ReplicationSpecChangeCallback(changedSpecId, oldSpecObj, newSpecObj)

	if oldSpecObj.(*metadata.ReplicationSpecification) == nil &&
		newSpecObj.(*metadata.ReplicationSpecification) != nil {
		// New spec
		err := b.refreshLatestManifest(changedSpecId)
		if err != nil {
			b.logger.Errorf("Unable to refresh manifest for new replication %v\n", changedSpecId)
			return err
		}
	} else if newSpecObj.(*metadata.ReplicationSpecification) == nil &&
		oldSpecObj.(*metadata.ReplicationSpecification) != nil {
		// Delete spec
		b.cacheMtx.Lock()
		delete(b.cacheSpecSourceMap, changedSpecId)
		delete(b.cacheSpecTargetMap, changedSpecId)
		b.cacheMtx.Unlock()
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

	if newManifests.Source == nil && newManifests.Target == nil {
		// Nothing changed
	} else if newManifests.Source != nil && newManifests.Target == nil {
		// Source changed but target did not
		b.handleSourceOnlyChange(replId, oldManifests.Source, newManifests.Source)
	} else if newManifests.Source == nil && newManifests.Target != nil {
		// Source did not change but target did change
		b.handleTargetOnlyChange(replId, oldManifests.Target, newManifests.Target)
	} else {
		// Both changed
		// First handle source change then target change
		b.handleSourceOnlyChange(replId, oldManifests.Source, newManifests.Source)
		b.handleTargetOnlyChange(replId, oldManifests.Target, newManifests.Target)
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

	if sourceManifest.Uid() == 0 {
		// Nothing has been persisted, backfill everything
		dummySpec := &metadata.ReplicationSpecification{Id: replId}
		var err error
		sourceManifest, _, err = b.collectionsManifestSvc.GetLatestManifests(dummySpec)
		if err != nil {
			return
		}
	}

	backfillIDs, err := newTargetManifest.GetBackfillCollectionIDs(oldTargetManifest, sourceManifest)
	if err != nil {
		b.logger.Errorf("handleTargetOnlyChange error: %v", err)
		return
	}

	// Store the target
	b.cacheMtx.Lock()
	b.cacheSpecTargetMap[replId] = newTargetManifest
	b.cacheMtx.Unlock()

	//TODO - move this
	replicationStatusRaw, err := b.replSpecSvc.GetDerivedObj(replId)
	if err != nil {
		return
	}
	replicationStatus, _ := replicationStatusRaw.(*pipeline.ReplicationStatus)
	throughSeqnos := replicationStatus.GetThroughSeqnos()
	var highestSeqnoAcrossVBs uint64
	for _, seqno := range throughSeqnos {
		if seqno > highestSeqnoAcrossVBs {
			highestSeqnoAcrossVBs = seqno
		}
	}
	b.logger.Infof("These collections Need to backfill %v to seqno: %v", backfillIDs, highestSeqnoAcrossVBs)
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
