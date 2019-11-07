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
	"fmt"
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
	oldManifest, ok := oldVal.(*metadata.CollectionsManifestPair)
	if !ok {
		return base.ErrorInvalidInput
	}
	newManifest, ok := newVal.(*metadata.CollectionsManifestPair)
	if !ok {
		return base.ErrorInvalidInput
	}

	var addedOrModified metadata.ScopesMap
	var removed metadata.ScopesMap
	var srcErr error
	var tgtErr error

	// Handle source
	if oldManifest.Source == nil && newManifest.Source != nil {
		b.logger.Infof("Source manifest did not exist, now it has: %v\n", newManifest.Source.String())
	} else if oldManifest.Source != nil && newManifest.Source == nil {
		// Don't think it's possible...
		b.logger.Infof("Source manifest has been deleted")
		srcErr = base.ErrorInvalidInput
	} else {
		addedOrModified, removed, srcErr = newManifest.Source.Diff(oldManifest.Source)
		if srcErr != nil {
			b.logger.Errorf("Unable to diff between source manifests: %v", srcErr.Error())
		}
		b.logger.Infof(fmt.Sprintf("NEIL DEBUG Source Added or removed:\n%v\nRemoved:\n%v\n", addedOrModified.String(), removed.String()))
	}

	// Handle target
	if oldManifest.Target == nil && newManifest.Target != nil {
		b.logger.Infof("Target manifest did not exist, now it has: %v\n", newManifest.Target.String())
	} else if oldManifest.Target != nil && newManifest.Target == nil {
		// Don't think it's possible...
		b.logger.Infof("Target manifest has been deleted")
		tgtErr = base.ErrorInvalidInput
	} else {
		addedOrModified, removed, tgtErr = newManifest.Target.Diff(oldManifest.Target)
		if tgtErr != nil {
			b.logger.Errorf("Unable to diff between target manifests: %v", tgtErr.Error())
		}
		b.logger.Infof(fmt.Sprintf("NEIL DEBUG Target Added or removed:\n%v\nRemoved:\n%v\n", addedOrModified.String(), removed.String()))
	}

	if tgtErr != nil && srcErr != nil {
		if srcErr != nil {
			return srcErr
		} else {
			return tgtErr
		}
	}

	return nil
}
