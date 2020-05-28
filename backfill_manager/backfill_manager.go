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
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/pipeline_manager"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	"reflect"
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
	clusterInfoSvc         service_def.ClusterInfoSvc
	xdcrTopologySvc        service_def.XDCRCompTopologySvc
	checkpointsSvc         service_def.CheckpointsService

	pipelineMgr pipeline_manager.PipelineMgrBackfillIface

	logger *log.CommonLogger

	// Collections Manifest Service will send down the latest changes from what it sees
	// Backfill Manager still needs to make decisions based on what it knows from last
	// time depending on source and target. These are what the caches are used for
	cacheMtx           sync.RWMutex
	cacheSpecSourceMap map[string]*metadata.CollectionsManifest
	cacheSpecTargetMap map[string]*metadata.CollectionsManifest

	// Request Handlers are responsible for handling a specific replication's backfill
	// request operations, like persisting, optimizing, stop/start, etc
	specReqHandlersMtx  sync.RWMutex
	specToReqHandlerMap map[string]*BackfillRequestHandler

	pipelineSvc  *pipelineSvcWrapper
	startStopMtx sync.Mutex
}

// Pipeline SvcWrapper is a way for backfill manager to pretend to be a pipeline service
// It implements the necessary functions to become a pipeline service
// And wraps the necessary pipeline-dependent calls into backfill manager
type pipelineSvcWrapper struct {
	backfillMgr *BackfillMgr
}

func (p *pipelineSvcWrapper) Attach(pipeline common.Pipeline) error {
	p.backfillMgr.specReqHandlersMtx.RLock()
	defer p.backfillMgr.specReqHandlersMtx.RUnlock()

	spec := pipeline.Specification().GetReplicationSpec()

	handler, ok := p.backfillMgr.specToReqHandlerMap[spec.Id]
	if !ok {
		return fmt.Errorf("Backfill Request Handler for Spec %v not found", spec.Id)
	}

	return handler.Attach(pipeline)
}

func (p *pipelineSvcWrapper) Start(metadata.ReplicationSettingsMap) error {
	// no op
	return nil
}

func (p *pipelineSvcWrapper) Stop() error {
	// no-op
	return nil
}

func (p *pipelineSvcWrapper) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	// no op
	return nil
}

func (p *pipelineSvcWrapper) IsSharable() bool {
	return false
}

func (p *pipelineSvcWrapper) Detach(pipeline common.Pipeline) error {
	return base.ErrorNotSupported
}

func NewBackfillManager(collectionsManifestSvc service_def.CollectionsManifestSvc,
	replSpecSvc service_def.ReplicationSpecSvc,
	backfillReplSvc service_def.BackfillReplSvc,
	pipelineMgr pipeline_manager.PipelineMgrBackfillIface,
	clusterInfoSvc service_def.ClusterInfoSvc,
	xdcrTopologySvc service_def.XDCRCompTopologySvc,
	checkpointsSvc service_def.CheckpointsService) *BackfillMgr {

	backfillMgr := &BackfillMgr{
		collectionsManifestSvc: collectionsManifestSvc,
		replSpecSvc:            replSpecSvc,
		backfillReplSvc:        backfillReplSvc,
		logger:                 log.NewLogger("BackfillMgr", log.DefaultLoggerContext),
		cacheSpecSourceMap:     make(map[string]*metadata.CollectionsManifest),
		cacheSpecTargetMap:     make(map[string]*metadata.CollectionsManifest),
		pipelineMgr:            pipelineMgr,
		specToReqHandlerMap:    make(map[string]*BackfillRequestHandler),
		clusterInfoSvc:         clusterInfoSvc,
		xdcrTopologySvc:        xdcrTopologySvc,
		pipelineSvc:            &pipelineSvcWrapper{},
		checkpointsSvc:         checkpointsSvc,
	}

	return backfillMgr
}

func (b *BackfillMgr) Start() error {
	b.startStopMtx.Lock()
	defer b.startStopMtx.Unlock()

	b.logger.Infof("BackfillMgr Starting...")

	b.pipelineSvc.backfillMgr = b

	b.collectionsManifestSvc.SetMetadataChangeHandlerCallback(b.collectionsManifestChangeCb)
	b.backfillReplSvc.SetMetadataChangeHandlerCallback(b.backfillReplSpecChangeHandlerCallback)

	err := b.initCache()
	if err != nil {
		return err
	}

	b.logger.Infof("BackfillMgr Started")
	return nil
}

func (b *BackfillMgr) Stop() {
	b.startStopMtx.Lock()
	defer b.startStopMtx.Unlock()

	b.logger.Infof("BackfillMgr Stopping...")
	errMap := b.stopHandlers()
	if len(errMap) > 0 {
		b.logger.Errorf("Stopping handlers returned: %v\n", errMap)
	}
	b.pipelineSvc.backfillMgr = nil
	b.logger.Infof("BackfillMgr Stopped")
}

func (b *BackfillMgr) stopHandlers() base.ErrorMap {
	var handlers []*BackfillRequestHandler
	b.specReqHandlersMtx.RLock()
	for _, handler := range b.specToReqHandlerMap {
		handlers = append(handlers, handler)
	}
	b.specReqHandlersMtx.RUnlock()

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
		b.createBackfillRequestHandler(spec)
	}
	return nil
}

func (b *BackfillMgr) createBackfillRequestHandler(spec *metadata.ReplicationSpecification) error {
	replId := spec.Id
	internalId := spec.InternalId

	seqnoGetter := func() (map[uint16]uint64, error) {
		return b.getThroughSeqno(replId)
	}
	vbsGetter := func() ([]uint16, error) {
		return b.GetMyVBs(spec.SourceBucketName)
	}
	var err error
	b.specReqHandlersMtx.Lock()
	if _, exists := b.specToReqHandlerMap[replId]; exists {
		err = fmt.Errorf("BackfillRequestHandler for spec %v already exists", spec)
		b.specReqHandlersMtx.Unlock()
		return err
	}
	reqHandler := NewCollectionBackfillRequestHandler(b.logger, replId,
		b.backfillReplSvc, spec, seqnoGetter, vbsGetter, spec.Settings.SourceNozzlePerNode*2)
	b.specToReqHandlerMap[replId] = reqHandler
	b.specReqHandlersMtx.Unlock()

	b.logger.Infof("Starting backfill request handler for spec %v internalId %v", replId, internalId)
	reqHandler.Start()
	return nil
}

func (b *BackfillMgr) deleteBackfillRequestHandler(replId, internalId string) error {
	b.specReqHandlersMtx.Lock()
	reqHandler, exists := b.specToReqHandlerMap[replId]
	defer b.specReqHandlersMtx.Unlock()

	if !exists {
		return base.ErrorNotFound
	}

	bgTask := func() {
		errCh := make(chan base.ComponentError, 1)
		var waitGrp sync.WaitGroup
		b.logger.Infof("Stopping backfill request handler for spec %v internalId %v in the background", replId, internalId)
		waitGrp.Add(1)
		go reqHandler.Stop(&waitGrp, errCh)
		waitGrp.Wait()
		if len(errCh) > 0 {
			err := <-errCh
			b.logger.Errorf("Stopped backfill request handler for spec %v internalId %v with err %v", replId, internalId, err.Err)
		} else {
			b.logger.Infof("Stopped backfill request handler for spec %v internalId %v", replId, internalId)
		}
	}

	// Don't care about waiting - a new replacement handler can be started in the meantime
	go bgTask()

	delete(b.specToReqHandlerMap, replId)
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
	if oldSpec == nil && newSpec != nil {
		err := b.pipelineMgr.RequestBackfill(changedSpecId)
		if err != nil {
			b.logger.Errorf("Unable to request backfill for %v", changedSpecId)
			return err
		}
	} else if oldSpec != nil && newSpec == nil {
		// When a spec is deleted, backfill pipeline will be automatically stopped as part of stopping main pipeline
		err := b.postDeleteBackfillRepl(changedSpecId, oldSpec.InternalId)
		if err != nil {
			b.logger.Errorf("Unable to run postDel backfill for %v", changedSpecId)
			return err
		}
	}
	return nil
}

func (b *BackfillMgr) ReplicationSpecChangeCallback(changedSpecId string, oldSpecObj interface{}, newSpecObj interface{}) error {
	oldSpec, ok := oldSpecObj.(*metadata.ReplicationSpecification)
	newSpec, ok2 := newSpecObj.(*metadata.ReplicationSpecification)

	if !ok || !ok2 {
		b.logger.Errorf("BackfillMgr error converting oldSpec: %v newSpec: %v", ok, ok2)
		return base.ErrorInvalidInput
	}

	b.logger.Infof("BackfillMgr detected specId %v old %v new %v", changedSpecId, oldSpec, newSpec)

	var err error
	if oldSpecObj.(*metadata.ReplicationSpecification) == nil &&
		newSpecObj.(*metadata.ReplicationSpecification) != nil {
		// As part of new replication created, pull the source and target manifest and store them as the starting point
		newSpec := newSpecObj.(*metadata.ReplicationSpecification)
		err = b.initNewReplStartingManifests(newSpec)
		if err != nil {
			return err
		}
		b.createBackfillRequestHandler(newSpec)
	} else if newSpecObj.(*metadata.ReplicationSpecification) == nil &&
		oldSpecObj.(*metadata.ReplicationSpecification) != nil {
		oldSpec := oldSpecObj.(*metadata.ReplicationSpecification)
		// Delete internal caches
		b.cacheMtx.Lock()
		delete(b.cacheSpecSourceMap, changedSpecId)
		delete(b.cacheSpecTargetMap, changedSpecId)
		b.cacheMtx.Unlock()

		err = b.postDeleteBackfillRepl(changedSpecId, oldSpec.InternalId)
	}

	return err
}

func (b *BackfillMgr) postDeleteBackfillRepl(specId, internalId string) error {
	err := b.deleteBackfillRequestHandler(specId, internalId)
	if err == base.ErrorNotFound {
		// No need to del checkpointdocs
		return nil
	}
	backfillSpecId := common.ComposeFullTopic(specId, common.BackfillPipeline)
	err = b.checkpointsSvc.DelCheckpointsDocs(backfillSpecId)
	if err != nil {
		b.logger.Errorf("Cleaning up backfill checkpoints for %v got err %v", backfillSpecId, err)
	}
	return err
}

func (b *BackfillMgr) initNewReplStartingManifests(spec *metadata.ReplicationSpecification) error {
	src, tgt, err := b.collectionsManifestSvc.GetLatestManifests(spec)
	if err != nil {
		b.logger.Errorf("Unable to retrieve manifests for new spec %v err %v", spec.Id, err)
		return err
	}

	b.cacheMtx.Lock()
	b.cacheSpecSourceMap[spec.Id] = src
	b.cacheSpecTargetMap[spec.Id] = tgt
	b.cacheMtx.Unlock()
	return nil
}

func (b *BackfillMgr) collectionsManifestChangeCb(replId string, oldVal, newVal interface{}) error {
	oldManifests, ok := oldVal.(*metadata.CollectionsManifestPair)
	if !ok {
		b.logger.Errorf("Oldval is not manifestpair, but %v", reflect.TypeOf(oldVal))
		return base.ErrorInvalidInput
	}

	newManifests, ok := newVal.(*metadata.CollectionsManifestPair)
	if !ok {
		b.logger.Errorf("Newval is not manifestpair, but %v", reflect.TypeOf(newVal))
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
			// Really odd error
			b.logger.Errorf("Repl %v Unable to find a baseline source manifest, and thus unable to figure out backfill situation", replId)
			return
		}
	}

	if sourceManifest.Uid() == 0 {
		// Source manifest 0 means only default collection is being replicated
		// If it's implicit mapping, no-op
		// TODO - this section will change for explicit mapping and/or upgrade
		b.logger.Infof("Repl %v shows default source manifest, thus no backfill would be created", replId)
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

	b.specReqHandlersMtx.RLock()
	handler := b.specToReqHandlerMap[replId]
	b.specReqHandlersMtx.RUnlock()
	if handler == nil {
		b.logger.Errorf("Unable to find handler for spec %v", replId)
		return
	}

	err = handler.HandleBackfillRequest(backfillMapping)
	if err != nil {
		b.logger.Errorf("Handler returned err %v", err)
	}
}

func (b *BackfillMgr) getThroughSeqno(replId string) (map[uint16]uint64, error) {
	return b.pipelineMgr.GetMainPipelineThroughSeqnos(replId)
}

func (b *BackfillMgr) GetMyVBs(sourceBucketName string) ([]uint16, error) {
	var vbList []uint16

	kv_vb_map, _, err := pipeline_utils.GetSourceVBMap(b.clusterInfoSvc, b.xdcrTopologySvc, sourceBucketName, b.logger)
	if err != nil {
		return vbList, err
	}

	for _, vbno := range kv_vb_map {
		vbList = append(vbList, vbno...)
	}

	return vbList, nil
}

func (b *BackfillMgr) GetPipelineSvc() common.PipelineService {
	return b.pipelineSvc
}
