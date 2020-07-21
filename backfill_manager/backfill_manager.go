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
	"github.com/couchbase/goxdcr/metadata_svc"
	"github.com/couchbase/goxdcr/pipeline_manager"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
	"reflect"
	"strings"
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

	utils utils.UtilsIface
}

// Pipeline SvcWrapper is a way for backfill manager to pretend to be a pipeline service
// It implements the necessary functions to become a pipeline service
// And wraps the necessary pipeline-dependent calls into backfill manager
type pipelineSvcWrapper struct {
	backfillMgr *BackfillMgr
}

func (p *pipelineSvcWrapper) Attach(pipeline common.Pipeline) error {
	p.backfillMgr.specReqHandlersMtx.RLock()
	spec := pipeline.Specification().GetReplicationSpec()
	handler, ok := p.backfillMgr.specToReqHandlerMap[spec.Id]
	p.backfillMgr.specReqHandlersMtx.RUnlock()

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
	return true
}

func (p *pipelineSvcWrapper) Detach(pipeline common.Pipeline) error {
	p.backfillMgr.specReqHandlersMtx.RLock()
	spec := pipeline.Specification().GetReplicationSpec()
	handler, ok := p.backfillMgr.specToReqHandlerMap[spec.Id]
	p.backfillMgr.specReqHandlersMtx.RUnlock()

	if !ok {
		return fmt.Errorf("Backfill Request Handler for Spec %v not found", spec.Id)
	}

	return handler.Detach(pipeline)
}

func (p *pipelineSvcWrapper) GetComponentEventListener(pipeline common.Pipeline) (common.ComponentEventListener, error) {
	var emptyEventListener common.ComponentEventListener
	if pipeline.Type() != common.BackfillPipeline {
		return emptyEventListener, base.ErrorInvalidInput
	}

	spec := pipeline.Specification().GetReplicationSpec()
	if spec == nil {
		err := fmt.Errorf("Pipeline %v has nil spec", pipeline.Topic())
		p.backfillMgr.logger.Errorf(err.Error())
		return emptyEventListener, err
	}

	p.backfillMgr.specReqHandlersMtx.RLock()
	defer p.backfillMgr.specReqHandlersMtx.RUnlock()
	handler, ok := p.backfillMgr.specToReqHandlerMap[spec.Id]
	if !ok {
		err := fmt.Errorf("Unable to find handler given spec ID %v", spec.Id)
		p.backfillMgr.logger.Errorf(err.Error())
		return emptyEventListener, err
	}
	return handler, nil
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
		utils:                  utils.NewUtilities(),
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
	vbsTasksDoneNotifier := func(startNewTask bool) {
		// When the first tasks for all VBs in VBTasksMap are done, this is the callback
		// (i.e. VBTasksMap 0th index of the VBTasksList for all VBs)
		// It will tell pipeline manager to stop the backfill pipeline (tear down)
		// and then start the backfill pipeline again (build a new one)
		b.logger.Infof("Backfill Request Handler %v has finished processing one task for all requested VBs", replId)
		err := b.pipelineMgr.HaltBackfill(replId)
		if err != nil {
			b.logger.Errorf("Unable to halt backfill pipeline %v - %v", replId, err)
		}
		err = b.pipelineMgr.CleanupBackfillCkpts(replId)
		if err != nil {
			b.logger.Errorf("Unable to clean up backfill pipeline checkpoint %v - %v", replId, err)
		}
		if startNewTask {
			err = b.pipelineMgr.RequestBackfill(replId)
			if err != nil {
				b.logger.Errorf("Unable to request backfill pipeline %v - %v", replId, err)
			}
		}
	}

	mainPipelineCkptSeqnosGetter := func() (map[uint16]uint64, error) {
		return b.getThroughSeqnosFromMainCkpts(replId, internalId)
	}

	restreamPipelineFatalFunc := func() {
		b.pipelineMgr.ReInitStreams(spec.Id)
	}

	var err error
	b.specReqHandlersMtx.Lock()
	if _, exists := b.specToReqHandlerMap[replId]; exists {
		err = fmt.Errorf("BackfillRequestHandler for spec %v already exists", spec)
		b.specReqHandlersMtx.Unlock()
		return err
	}
	reqHandler := NewCollectionBackfillRequestHandler(b.logger, replId, b.backfillReplSvc, spec, seqnoGetter, vbsGetter, base.BackfillPersistInterval, vbsTasksDoneNotifier, mainPipelineCkptSeqnosGetter, restreamPipelineFatalFunc)
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
			b.logger.Infof("Stopped backfill request handler for spec %v internalId %v with err %v", replId, internalId, err.Err)
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
		// Requesting a backfill pipeline means that a pipeline will start and for the top task in each VBTasksMap
		// will be sent to DCP to be run and backfilled
		// Once all the VB Task's top task is done, then the backfill pipeline will be considered finished
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
	} else {
		// If any replication spec changes occurred, and the backfill spec's pointer has changed
		// then the main replication pipeline will restart. When main pipeline restarts, the backfill pipeline
		// will also restart alongside with it, and backfill pipelines will restart with the new settings
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

		err := b.deleteBackfillRequestHandler(oldSpec.Id, oldSpec.InternalId)
		if err == base.ErrorNotFound {
			// No need to del checkpointdocs
			return nil
		}
		err = b.postDeleteBackfillRepl(changedSpecId, oldSpec.InternalId)
	} else {
		// metakv_change_listener will call GetExplicitMappingChangeHandler if needed
	}
	return err
}

func (b *BackfillMgr) GetExplicitMappingChangeHandler(specId string, internalSpecId string, oldSettings *metadata.ReplicationSettings, newSettings *metadata.ReplicationSettings) (base.StoppedPipelineCallback, base.StoppedPipelineErrCallback) {
	callback := func() error {
		err, upToDate := b.checkUpToDateSpec(specId, internalSpecId)
		if !upToDate {
			return err
		}

		oldCollectionMode := oldSettings.GetCollectionModes()
		oldRoutingRules := oldSettings.GetCollectionsRoutingRules()
		newCollectionMode := newSettings.GetCollectionModes()
		newRoutingRules := newSettings.GetCollectionsRoutingRules()

		if oldCollectionMode.IsExplicitMapping() != newCollectionMode.IsExplicitMapping() {
			// Any changes between implicit or explicit mapping means all checkpoints are deleted and everything starts over
			// This is handled by replication spec service
			return nil
		} else if !newCollectionMode.IsExplicitMapping() {
			// No need to worry about raising backfill for implicit mapping
			// This is handled by replication spec service
			return nil
		} else if !oldRoutingRules.SameAs(newRoutingRules) {
			// Explicit mapping and explicit mapping rules have changed
			specForId := metadata.ReplicationSpecification{Id: specId} /* only need the spec for the Id */
			srcMan, tgtMan, err := b.collectionsManifestSvc.GetLatestManifests(&specForId)
			if err != nil {
				b.logger.Errorf("error - Unable to retrieve manifests for spec %v due to %v - recommended to restream", specId, err)
				return err
			}
			manifestPair := metadata.CollectionsManifestPair{
				Source: srcMan,
				Target: tgtMan,
			}
			newMapping, err := metadata.NewCollectionNamespaceMappingFromRules(manifestPair, newCollectionMode, newRoutingRules)
			if err != nil {
				return err
			}
			oldMapping, err := metadata.NewCollectionNamespaceMappingFromRules(manifestPair, oldCollectionMode, oldRoutingRules)
			if err != nil {
				return err
			}
			added, removed := newMapping.Diff(oldMapping)
			err = b.handleExplicitMapChangeBackfillReq(specId, added, removed)
			if err != nil {
				return err
			}
		}
		return nil
	}

	errCallback := func(err error) {
		b.explicitMappingCbGenericErrHandler(err, specId)
	}

	return callback, errCallback
}

func (b *BackfillMgr) explicitMappingCbGenericErrHandler(err error, specId string) {
	if err != nil {
		b.logger.Fatalf("Unable to Handle backfilling data for explicit mapping change due to err %v. To prevent data loss, the whole replication %v must be restarted from seqno 0", err, specId)
		b.pipelineMgr.ReInitStreams(specId)
	}
}

func (b *BackfillMgr) checkUpToDateSpec(specId string, internalSpecId string) (error, bool) {
	// First, get most up-to-date spec to ensure this call is not out of date
	spec, err := b.replSpecSvc.ReplicationSpec(specId)
	if err != nil {
		if err == metadata_svc.ReplNotFoundErr {
			// This is ok - this callback was too late
			return nil, false
		} else {
			// shouldn't happen
			return err, false
		}
	}

	if spec.InternalId != internalSpecId {
		// Callback is too late - spec got recreated already, no op
		return nil, false
	}
	return nil, true
}

func (b *BackfillMgr) GetRouterMappingChangeHandler(specId, internalSpecId string, diff metadata.CollectionNamespaceMappingsDiffPair) (base.StoppedPipelineCallback, base.StoppedPipelineErrCallback) {
	callback := func() error {
		err, upToDate := b.checkUpToDateSpec(specId, internalSpecId)
		if !upToDate {
			return err
		}
		err = b.handleExplicitMapChangeBackfillReq(specId, diff.Added, diff.Removed)
		if err != nil {
			return err
		}
		return nil
	}

	errCb := func(err error) {
		b.explicitMappingCbGenericErrHandler(err, specId)
	}
	return callback, errCb
}

func (b *BackfillMgr) postDeleteBackfillRepl(specId, internalId string) error {
	backfillSpecId := common.ComposeFullTopic(specId, common.BackfillPipeline)
	err := b.checkpointsSvc.DelCheckpointsDocs(backfillSpecId)
	if err != nil {
		b.logger.Errorf("Cleaning up backfill checkpoints for %v got err %v", backfillSpecId, err)
	}
	return err
}

func (b *BackfillMgr) getThroughSeqnosFromMainCkpts(specId, internalId string) (map[uint16]uint64, error) {
	ckptDocs, err := b.checkpointsSvc.CheckpointsDocs(specId, false)
	if err != nil {
		return nil, err
	}
	maxSeqnoMap := make(map[uint16]uint64)

	for vb, ckptDoc := range ckptDocs {
		if ckptDoc.SpecInternalId != internalId {
			continue
		}
		var maxSeqno uint64
		for _, record := range ckptDoc.Checkpoint_records {
			if record == nil {
				continue
			}
			if record.Seqno > maxSeqno {
				maxSeqno = record.Seqno
			}
		}
		maxSeqnoMap[vb] = maxSeqno
	}
	return maxSeqnoMap, nil
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
	} else if oldManifests.Target != nil {
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
		b.handleSourceOnlyChange(replId, oldManifests.Source, newManifests.Source)
	} else if newManifests.Source == nil && newManifests.Target != nil {
		// Source did not change but target did change
		b.handleSrcAndTgtChanges(replId, nil, nil, oldManifests.Target, newManifests.Target)
	} else {
		// Both changed
		b.handleSrcAndTgtChanges(replId, oldManifests.Source, newManifests.Source, oldManifests.Target, newManifests.Target)
	}
}

func (b *BackfillMgr) handleSourceOnlyChange(replId string, oldSourceManifest, newSourceManifest *metadata.CollectionsManifest) {
	// update internal cache
	b.cacheMtx.Lock()
	b.cacheSpecSourceMap[replId] = newSourceManifest
	b.cacheMtx.Unlock()

	// TODO - mirroring policy handler will be here
	spec, err := b.replSpecSvc.ReplicationSpec(replId)
	if err != nil {
		b.logger.Errorf("Unable to find spec %v - %v", spec.Id, err)
		return
	}
	modes := spec.Settings.GetCollectionModes()
	if !modes.IsExplicitMapping() {
		// Nothing to do for implicit mapping
		return
	}

	b.cacheMtx.RLock()
	latestTgtManifestOrig := b.cacheSpecTargetMap[replId]
	if latestTgtManifestOrig == nil {
		b.logger.Warnf("%v Target has no manifest", replId)
		b.cacheMtx.RUnlock()
		return
	}
	latestTgtManifestObj := latestTgtManifestOrig.Clone()
	latestTgtManifest := &latestTgtManifestObj
	b.cacheMtx.RUnlock()

	if latestTgtManifest == nil {
		b.logger.Errorf("Unable to do explicit mapping diff for %v if target manifest is nil", spec.Id)
		return
	}

	diffPair, err := b.compileExplicitBackfillReq(spec, modes, oldSourceManifest, latestTgtManifest, newSourceManifest, latestTgtManifest)
	if err != nil {
		b.logger.Errorf("%v Error compiling explicit backfillReq: %v", spec.Id, err)
		return
	}
	b.raiseBackfillReq(replId, diffPair)
}

func (b *BackfillMgr) handleSrcAndTgtChanges(replId string, oldSourceManifest, newSourceManifest, oldTargetManifest, newTargetManifest *metadata.CollectionsManifest) {
	// For added and modified since last time, need to launch backfill if they are mapped from source
	var sourceChanged bool = newSourceManifest != nil

	if newSourceManifest == nil {
		b.cacheMtx.RLock()
		newSourceManifest = b.cacheSpecSourceMap[replId]
		b.cacheMtx.RUnlock()

		if newSourceManifest == nil {
			// Really odd error
			b.logger.Errorf("Repl %v Unable to find a baseline source manifest, and thus unable to figure out backfill situation", replId)
			return
		}
	}

	if newSourceManifest.Uid() == 0 {
		// Source manifest 0 means only default collection is being replicated
		// If it's implicit mapping, no-op
		// TODO MB-40588 - this section will change for explicit mapping and/or upgrade
		b.logger.Infof("Repl %v shows default source manifest, thus no backfill would be created", replId)
		return
	}

	// Update cache
	b.cacheMtx.Lock()
	if sourceChanged {
		b.cacheSpecSourceMap[replId] = newSourceManifest
	}
	b.cacheSpecTargetMap[replId] = newTargetManifest
	b.cacheMtx.Unlock()

	spec, err := b.replSpecSvc.ReplicationSpec(replId)
	if err != nil {
		b.logger.Errorf("Unable to retrieve repl spec %v to determine whether it is implicit or explicit mapping", replId)
		return
	}
	modes := spec.Settings.GetCollectionModes()

	var backfillReq interface{}
	if modes.IsExplicitMapping() {
		if newTargetManifest == nil {
			b.logger.Errorf("%v Unable to do explicit mapping if target manifest is nil", spec.Id)
			return
		}
		if oldSourceManifest != nil && oldTargetManifest != nil && newSourceManifest != nil && newTargetManifest != nil {
			// Both changed
			diffPair, err := b.compileExplicitBackfillReq(spec, modes, oldSourceManifest, oldTargetManifest, newSourceManifest, newTargetManifest)
			if err != nil {
				return
			}
			backfillReq = diffPair
		} else if oldTargetManifest != nil && newTargetManifest != nil {
			// Only target changed
			sourceManifest := b.getLatestSourceManifestClone(replId)
			if sourceManifest == nil {
				return
			}
			diffPair, err := b.compileExplicitBackfillReq(spec, modes, sourceManifest, oldTargetManifest, sourceManifest, newTargetManifest)
			if err != nil {
				return
			}
			backfillReq = diffPair
		} else if newTargetManifest != nil {
			// potentially brand new target manifest change - backfill everything
			sourceManifest := b.getLatestSourceManifestClone(replId)
			if sourceManifest == nil {
				return
			}
			manifestsPair := metadata.CollectionsManifestPair{
				Source: sourceManifest,
				Target: newTargetManifest,
			}
			explicitMapping, err := metadata.NewCollectionNamespaceMappingFromRules(manifestsPair, modes, spec.Settings.GetCollectionsRoutingRules())
			if err != nil {
				panic("FIME")
			}
			diffPair := metadata.CollectionNamespaceMappingsDiffPair{
				Added:   explicitMapping,
				Removed: nil,
			}
			backfillReq = diffPair
		} else {
			// Invalid function call
			panic("Invalid functionc call")
		}
	} else {
		backfillMapping, err := newTargetManifest.ImplicitGetBackfillCollections(oldTargetManifest, newSourceManifest)
		if err != nil {
			b.logger.Errorf("%v handleSrcAndTgtChanges error: %v", spec.Id, err)
			return
		}
		if len(backfillMapping) == 0 {
			// If no backfill is needed such as when collections get del' on the target, don't raise backfill request
			return
		}
		backfillReq = backfillMapping
	}

	b.raiseBackfillReq(replId, backfillReq)
}

func (b *BackfillMgr) getLatestSourceManifestClone(replId string) *metadata.CollectionsManifest {
	b.cacheMtx.RLock()
	sourceManifest := b.cacheSpecSourceMap[replId]
	if sourceManifest == nil {
		b.logger.Warnf("%v could not find source manifest", replId)
		b.cacheMtx.RUnlock()
		return nil
	}
	sourceManifestObj := sourceManifest.Clone()
	sourceManifest = &sourceManifestObj
	b.cacheMtx.RUnlock()
	return sourceManifest
}

func (b *BackfillMgr) compileExplicitBackfillReq(spec *metadata.ReplicationSpecification, modes base.CollectionsMgtType, oldSourceManifest, oldTargetManifest, newSourceManifest, newTargetManifest *metadata.CollectionsManifest) (metadata.CollectionNamespaceMappingsDiffPair, error) {
	pair := metadata.CollectionsManifestPair{
		Source: oldSourceManifest,
		Target: oldTargetManifest,
	}
	oldExplicitMap, err := metadata.NewCollectionNamespaceMappingFromRules(pair, modes, spec.Settings.GetCollectionsRoutingRules())
	if err != nil {
		b.logger.Errorf("%v Error compiling old explicit map: %v", spec.Id, err)
		panic("FIXME")
		return metadata.CollectionNamespaceMappingsDiffPair{}, err
	}
	pair.Source = newSourceManifest
	pair.Target = newTargetManifest
	newExplicitMap, err := metadata.NewCollectionNamespaceMappingFromRules(pair, modes, spec.Settings.GetCollectionsRoutingRules())
	if err != nil {
		b.logger.Errorf("%v Error compiling new explicit map: %v", spec.Id, err)
		panic("FIXME")
		return metadata.CollectionNamespaceMappingsDiffPair{}, err
	}
	added, removed := oldExplicitMap.Diff(newExplicitMap)
	diffPair := metadata.CollectionNamespaceMappingsDiffPair{
		Added:   added,
		Removed: removed,
	}
	return diffPair, nil
}

func (b *BackfillMgr) raiseBackfillReq(replId string, backfillReq interface{}) {
	b.specReqHandlersMtx.RLock()
	handler := b.specToReqHandlerMap[replId]
	b.specReqHandlersMtx.RUnlock()
	if handler == nil {
		b.logger.Errorf("Unable to find handler for spec %v", replId)
		b.logger.Fatalf(base.GetBackfillFatalDataLossError(replId).Error())
		b.pipelineMgr.ReInitStreams(replId)
		return
	}

	err := handler.HandleBackfillRequest(backfillReq)
	if err != nil {
		b.logger.Errorf("%v Backfill Handler returned err %v while raising backfill request", replId, err)
		b.logger.Fatalf(base.GetBackfillFatalDataLossError(replId).Error())
		b.pipelineMgr.ReInitStreams(replId)
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

func (b *BackfillMgr) GetComponentEventListener(pipeline common.Pipeline) (common.ComponentEventListener, error) {
	return b.pipelineSvc.GetComponentEventListener(pipeline)
}

func (b *BackfillMgr) handleExplicitMapChangeBackfillReq(replId string, added metadata.CollectionNamespaceMapping, removed metadata.CollectionNamespaceMapping) error {
	b.specReqHandlersMtx.RLock()
	handler := b.specToReqHandlerMap[replId]
	handlerFinCh := handler.finCh
	b.specReqHandlersMtx.RUnlock()
	if handler == nil {
		b.logger.Errorf("Unable to find handler for spec %v", replId)
		return base.ErrorNotFound
	}

	mapPair := metadata.CollectionNamespaceMappingsDiffPair{
		Added:   added,
		Removed: removed,
	}

	handleRequestWrapper := func(interface{}) (interface{}, error) {
		return nil, handler.HandleBackfillRequest(mapPair)
	}
	_, err := b.utils.ExponentialBackoffExecutorWithFinishSignal("explicitMapChange", base.RetryIntervalMetakv, base.MaxNumOfMetakvRetries, base.MetaKvBackoffFactor, handleRequestWrapper, nil, handlerFinCh)
	if err != nil {
		if strings.Contains(err.Error(), base.FinClosureStr) {
			// If finClosure, then this means that the handler has been told to stop, which means spec was deleted
			err = nil
		} else {
			b.logger.Errorf("%v Executing explicitMapChange with retry resulted in %v", replId, err)
		}
	}
	return err
}
