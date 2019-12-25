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
	backfillReplSvc        service_def.BackfillReplSvc

	logger *log.CommonLogger

	childrenWaitgrp sync.WaitGroup
	cancelChan      chan struct{}

	cleanupOnce sync.Once

	cacheMtx           sync.RWMutex
	cacheSpecSourceMap map[string]*metadata.CollectionsManifest
	cacheSpecTargetMap map[string]*metadata.CollectionsManifest

	specReqHandlersMtx  sync.RWMutex
	specToReqHandlerMap map[string]*BackfillRequestHandler
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
	replSpecSvc service_def.ReplicationSpecSvc,
	backfillReplSvc service_def.BackfillReplSvc) service_def.BackfillMgrIface {

	backfillMgr := &BackfillMgr{
		collectionsManifestSvc: collectionsManifestSvc,
		replSpecSvc:            replSpecSvc,
		backfillReplSvc:        backfillReplSvc,
		cancelChan:             make(chan struct{}, 1),
		logger:                 log.NewLogger("BackfillMgr", log.DefaultLoggerContext),
		cacheSpecSourceMap:     make(map[string]*metadata.CollectionsManifest),
		cacheSpecTargetMap:     make(map[string]*metadata.CollectionsManifest),
		specToReqHandlerMap:    make(map[string]*BackfillRequestHandler),
	}

	wrappedExitFunc := func(force bool) {
		backfillMgr.backfillMgrExitFunc(force)
		parentExitFunc(force)
	}

	// TODO - to be used by future metadata change monitors
	backfillMgr.exitFunc = wrappedExitFunc

	return backfillMgr
}

func (b *BackfillMgr) Start() error {
	b.logger.Infof("BackfillMgr Starting...")
	b.initMetadataChangeMonitor()

	b.collectionsManifestSvc.SetMetadataChangeHandlerCallback(b.collectionsManifestChangeCb)
	b.replSpecSvc.SetMetadataChangeHandlerCallback(b.replicationSpecChangeHandlerCallback)

	b.initCache()
	err := b.startHandlers()
	if err != nil {
		return err
	}
	b.logger.Infof("BackfillMgr Started")
	return nil
}

func (b *BackfillMgr) Stop() {
	b.logger.Infof("BackfillMgr Stopping...")
	errMap := b.stopHandlers()
	if len(errMap) > 0 {
		b.logger.Errorf("Stopping handlers returned: %v\n", errMap)
	}
	b.logger.Infof("BackfillMgr Stopped")
}

func (b *BackfillMgr) initMetadataChangeMonitor() {
	// TODO - future metadata change monitors
	//	mcm := base.NewMetadataChangeMonitor()
}

func (b *BackfillMgr) createNewBackfillReqHandler(replId string, startHandler bool, spec *metadata.ReplicationSpecification) error {
	b.specReqHandlersMtx.Lock()
	defer b.specReqHandlersMtx.Unlock()

	persistCb := func(info metadata.BackfillPersistInfo) error {
		return b.backfillRequestPersistCallback(replId, info)
	}
	handler := NewCollectionBackfillRequestHandler(b.logger, replId, persistCb, spec)
	b.specToReqHandlerMap[replId] = handler

	if startHandler {
		return handler.Start()
	}
	return nil
}

func (b *BackfillMgr) deleteBackfillReqHandler(replId string) {

}

func (b *BackfillMgr) initCache() {
	// The following doesn't return a non-nil error for now
	replSpecMap, _ := b.replSpecSvc.AllReplicationSpecs()
	for replId, spec := range replSpecMap {
		err := b.refreshLatestManifest(replId)
		if err != nil {
			b.logger.Errorf("Retrieving manifest for spec %v returned %v", replId, err)
		}

		b.createNewBackfillReqHandler(replId, false /*startHandler*/, spec)
	}
}

func (b *BackfillMgr) startHandlers() error {
	b.specReqHandlersMtx.RLock()
	defer b.specReqHandlersMtx.RUnlock()

	for _, handler := range b.specToReqHandlerMap {
		err := handler.Start()
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *BackfillMgr) stopHandlersInternalNoLock(handlers []*BackfillRequestHandler) base.ErrorMap {
	errCh := make(chan base.ComponentError, len(handlers))
	var errMap base.ErrorMap

	stopHandlersFunc := func() error {
		var stopChildWait sync.WaitGroup

		for _, handler := range handlers {
			stopChildWait.Add(1)
			go handler.Stop(&stopChildWait, errCh)
		}
		stopChildWait.Wait()

		if len(errCh) > 0 {
			errMap = base.FormatErrMsgWithUpperLimit(errCh, 10000 /* doesn't matter*/)
		}
		return nil
	}

	err := base.ExecWithTimeout(stopHandlersFunc, base.TimeoutPartsStop, b.logger)
	if err != nil {
		// if err is not nill, it is possible that stopHandlersFunc is still running and may still access errMap
		// return errMap1 instead of errMap to avoid race conditions
		errMap1 := make(base.ErrorMap)
		errMap1["backfillMgr.stopHandlers"] = err
		return errMap1
	}

	return errMap
}

func (b *BackfillMgr) stopHandlers() base.ErrorMap {
	b.specReqHandlersMtx.Lock()
	defer b.specReqHandlersMtx.Unlock()

	var handlerList []*BackfillRequestHandler

	for _, handler := range b.specToReqHandlerMap {
		handlerList = append(handlerList, handler)
	}

	return b.stopHandlersInternalNoLock(handlerList)
}

func (b *BackfillMgr) stopHandler(replId string) base.ErrorMap {
	b.specReqHandlersMtx.RLock()
	handler, ok := b.specToReqHandlerMap[replId]
	b.specReqHandlersMtx.RUnlock()

	if !ok {
		errMap := make(base.ErrorMap)
		errMap["stopHandler"] = base.ErrorInvalidInput
		return errMap
	}

	var handlerList []*BackfillRequestHandler
	handlerList = append(handlerList, handler)

	return b.stopHandlersInternalNoLock(handlerList)
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

	// TODO - these should be handled by a different go routine as to not block
	if oldSpecObj.(*metadata.ReplicationSpecification) == nil &&
		newSpecObj.(*metadata.ReplicationSpecification) != nil {
		spec := newSpecObj.(*metadata.ReplicationSpecification)
		// New spec
		err := b.refreshLatestManifest(changedSpecId)
		if err != nil {
			b.logger.Errorf("Unable to refresh manifest for new replication %v\n", changedSpecId)
			return err
		}
		b.createNewBackfillReqHandler(changedSpecId, true /*startHandler*/, spec)
	} else if newSpecObj.(*metadata.ReplicationSpecification) == nil &&
		oldSpecObj.(*metadata.ReplicationSpecification) != nil {
		// Delete spec
		b.cacheMtx.Lock()
		delete(b.cacheSpecSourceMap, changedSpecId)
		delete(b.cacheSpecTargetMap, changedSpecId)
		b.cacheMtx.Unlock()

		b.stopHandler(changedSpecId)
		b.specReqHandlersMtx.Lock()
		delete(b.specToReqHandlerMap, changedSpecId)
		b.specReqHandlersMtx.Unlock()
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

	backfillMapping, err := newTargetManifest.GetBackfillCollectionIDs(oldTargetManifest, sourceManifest)
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
	b.logger.Infof("These collections Need to backfill %v to seqno: %v", backfillMapping, highestSeqnoAcrossVBs)

	b.specReqHandlersMtx.RLock()
	handler, ok := b.specToReqHandlerMap[replId]
	if !ok {
		b.logger.Errorf("Unable to find spec handler for %v\n", replId)
	}
	b.specReqHandlersMtx.RUnlock()
	var manifestPair metadata.CollectionsManifestPair
	manifestPair.Source = sourceManifest
	manifestPair.Target = newTargetManifest
	request := metadata.NewCollectionBackfillRequest(manifestPair, backfillMapping, 0, highestSeqnoAcrossVBs)
	handler.HandleBackfillRequest(request)
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

func (b *BackfillMgr) backfillRequestPersistCallback(replId string, info metadata.BackfillPersistInfo) error {
	b.logger.Infof("NEIL DEBUG for replication %v received callback to store data", replId)

	// TODO - this needs burst control
	backfillReplSpec, err := b.backfillReplSvc.ReplicationSpec(replId)
	if err != nil {
		backfillReplSpec = metadata.NewBackfillReplicationSpec(replId, &info)
		err = b.backfillReplSvc.AddReplicationSpec(backfillReplSpec)
		b.logger.Infof("NEIL DEBUG Add backfill replication spec returned %v\n", err)
	} else {
		// Set operation
		// TODO - right now overwrite, but this is incorrect need to optimize and then write
		backfillReplSpec.BackfillTasks = &info
		err = b.backfillReplSvc.SetReplicationSpec(backfillReplSpec)
		b.logger.Infof("NEIL DEBUG Set backfill replication spec returned %v\n", err)
	}

	return err
}

// This is used by DCP nozzle to request a catch-up backfill for the whole bucket
// Note - this must persist the request
func (b *BackfillMgr) RequestIncrementalBucketBackfill(topic string, request metadata.VBucketBackfillMap) error {
	b.logger.Infof("NEIL DEBUG DCP requesting backfill for topic %v total %v mutations", topic, request.TotalMutations())
	b.specReqHandlersMtx.RLock()
	defer b.specReqHandlersMtx.RUnlock()

	incrementalReq := metadata.NewIncrementalBackfillRequest(request)
	for specId, handler := range b.specToReqHandlerMap {
		// specId is the same as topic
		if specId == topic {
			err := handler.HandleBackfillRequest(incrementalReq)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
