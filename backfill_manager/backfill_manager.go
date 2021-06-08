// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

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
	"github.com/couchbase/goxdcr/utils"
	"math"
	"reflect"
	"strings"
	"sync"
	"time"
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
	cacheMtx                          sync.RWMutex
	cacheSpecSourceMap                map[string]*metadata.CollectionsManifest
	cacheSpecLastSuccessfulManifestId map[string]uint64
	cacheSpecTargetMap                map[string]*metadata.CollectionsManifest

	// Request Handlers are responsible for handling a specific replication's backfill
	// request operations, like persisting, optimizing, stop/start, etc
	specReqHandlersMtx  sync.RWMutex
	specToReqHandlerMap map[string]*BackfillRequestHandler

	pipelineSvc  *pipelineSvcWrapper
	startStopMtx sync.Mutex

	utils utils.UtilsIface

	replSpecHandlerMtx sync.Mutex
	replSpecHandlerMap map[string]*replSpecHandler

	errorRetryQMtx  sync.RWMutex
	errorRetryQueue []BackfillRetryRequest
	finCh           chan bool

	retrySpecRemovalCh chan string
	retryTimerPeriod   time.Duration
}

type replSpecHandler struct {
	spec                         *metadata.ReplicationSpecification
	finCh                        chan bool
	initManifestsFunc            func(*metadata.ReplicationSpecification, chan bool) error
	createBackfillReqHandlerFunc func(*metadata.ReplicationSpecification) error
	cleanupFunc                  func(specId string)
}

// This must not fail - unless finCh hits
func (r *replSpecHandler) HandleNewSpec() {
	err := r.initManifestsFunc(r.spec, r.finCh)
	if err != nil {
		// Panic here and the normal startup should take over
		panic(fmt.Sprintf("Unable to establish starting manifests for %v", r.spec.Id))
	}
	r.createBackfillReqHandlerFunc(r.spec)
	go r.cleanupFunc(r.spec.Id)
}

func NewReplSpecHandler(b *BackfillMgr, newSpec *metadata.ReplicationSpecification) *replSpecHandler {
	return &replSpecHandler{
		spec:                         newSpec.Clone(),
		finCh:                        make(chan bool),
		initManifestsFunc:            b.initNewReplStartingManifests,
		createBackfillReqHandlerFunc: b.createBackfillRequestHandler,
		cleanupFunc:                  b.cleanupSpecHandlerCb,
	}
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
	errMap := make(base.ErrorMap)

	topic, exists := settings[base.NameKey].(string)
	_, exists2 := settings[metadata.CollectionsDelAllBackfillKey].(bool)
	if exists && exists2 {
		err := p.backfillMgr.DelAllBackfills(topic)
		if err != nil {
			errMap[metadata.CollectionsDelAllBackfillKey] = err
		}
	}

	topic, exists = settings[base.NameKey].(string)
	vbno, exists2 := settings[metadata.CollectionsDelVbBackfillKey].(int)
	if exists && exists2 && vbno >= 0 {
		err := p.backfillMgr.DelBackfillForVB(topic, uint16(vbno))
		if err != nil {
			errMap[metadata.CollectionsDelVbBackfillKey] = err
		}
	}

	backfillMapping, exists := settings[metadata.CollectionsManualBackfillKey].(metadata.CollectionNamespaceMapping)
	topic, exists2 = settings[base.NameKey].(string)
	if exists && exists2 {
		err := p.backfillMgr.RequestOnDemandBackfill(topic, backfillMapping)
		if err != nil {
			errMap[metadata.CollectionsManualBackfillKey] = err
		}
	}

	if len(errMap) > 0 {
		return fmt.Errorf(base.FlattenErrorMap(errMap))
	} else {
		return nil
	}
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

type BackfillRetryRequest struct {
	replId                     string
	req                        interface{}
	force                      bool
	correspondingSrcManifestId uint64
	handler                    *BackfillRequestHandler
}

func NewBackfillManager(collectionsManifestSvc service_def.CollectionsManifestSvc,
	replSpecSvc service_def.ReplicationSpecSvc,
	backfillReplSvc service_def.BackfillReplSvc,
	pipelineMgr pipeline_manager.PipelineMgrBackfillIface,
	clusterInfoSvc service_def.ClusterInfoSvc,
	xdcrTopologySvc service_def.XDCRCompTopologySvc,
	checkpointsSvc service_def.CheckpointsService) *BackfillMgr {

	backfillMgr := &BackfillMgr{
		collectionsManifestSvc:            collectionsManifestSvc,
		replSpecSvc:                       replSpecSvc,
		backfillReplSvc:                   backfillReplSvc,
		logger:                            log.NewLogger("BackfillMgr", log.DefaultLoggerContext),
		cacheSpecSourceMap:                make(map[string]*metadata.CollectionsManifest),
		cacheSpecLastSuccessfulManifestId: make(map[string]uint64),
		cacheSpecTargetMap:                make(map[string]*metadata.CollectionsManifest),
		pipelineMgr:                       pipelineMgr,
		specToReqHandlerMap:               make(map[string]*BackfillRequestHandler),
		clusterInfoSvc:                    clusterInfoSvc,
		xdcrTopologySvc:                   xdcrTopologySvc,
		pipelineSvc:                       &pipelineSvcWrapper{},
		checkpointsSvc:                    checkpointsSvc,
		utils:                             utils.NewUtilities(),
		replSpecHandlerMap:                make(map[string]*replSpecHandler),
		finCh:                             make(chan bool),
		retrySpecRemovalCh:                make(chan string, 5),
		retryTimerPeriod:                  10 * time.Second,
	}

	return backfillMgr
}

func (b *BackfillMgr) Start() error {
	b.startStopMtx.Lock()
	defer b.startStopMtx.Unlock()

	b.logger.Infof("BackfillMgr Starting...")

	b.pipelineSvc.backfillMgr = b

	// TODO - once consistent metakv is in, these should all be moved into a metadata_change_monitor
	b.collectionsManifestSvc.SetMetadataChangeHandlerCallback(b.collectionsManifestChangeCb)
	b.backfillReplSvc.SetMetadataChangeHandlerCallback(b.backfillReplSpecChangeHandlerCallback)
	b.backfillReplSvc.SetMetadataChangeHandlerCallback(b.checkpointsSvc.BackfillReplicationSpecChangeCallback)
	// CheckpointSvc performs checkpoint clean up when manifests changes, so this should be at the end
	// once all pipelines have restarted and stabilized
	b.collectionsManifestSvc.SetMetadataChangeHandlerCallback(b.checkpointsSvc.CollectionsManifestChangeCb)

	err := b.initCache()
	if err != nil {
		return err
	}

	b.backfillReplSvc.SetCompleteBackfillRaiser(b.RequestCompleteBackfill)

	go b.runRetryMonitor()

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
	close(b.finCh)
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
			// Create a map entry - setting this value depends on when pipeline starts
			b.cacheSpecLastSuccessfulManifestId[replId] = 0
			b.cacheSpecTargetMap[replId] = &defaultManifest
			b.cacheMtx.Unlock()
			// Default with default manifest in case of error
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

	specCheckFunc := func() bool {
		return b.validateReplIdExists(replId)
	}

	var err error
	b.specReqHandlersMtx.Lock()
	if _, exists := b.specToReqHandlerMap[replId]; exists {
		err = fmt.Errorf("BackfillRequestHandler for spec %v already exists", spec)
		b.specReqHandlersMtx.Unlock()
		return err
	}
	reqHandler := NewCollectionBackfillRequestHandler(b.logger, replId, b.backfillReplSvc, spec, seqnoGetter, vbsGetter, base.BackfillPersistInterval, vbsTasksDoneNotifier, mainPipelineCkptSeqnosGetter, restreamPipelineFatalFunc, specCheckFunc)
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
	b.cacheSpecLastSuccessfulManifestId[spec.Id] = 0
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
		if oldSpec.SameAs(newSpec) {
			// This means only the ptr to parent replicationSpec changed
			// If any replication spec changes occurred, and the backfill spec's pointer has changed
			// then the main replication pipeline will restart. When main pipeline restarts, the backfill pipeline
			// will also restart alongside with it, and backfill pipelines will restart with the new settings
		} else {
			// If the top task (aka the one that's supposed to be running) are different because the mappingNamespaces
			// has changed, and the pipeline is active, then restart the backfill pipeline
			// This occurs if:
			// 1. Explicit mapping changed and some backfill tasks mappings are no longer required
			// 2. Both Implicit and Explicit mapping: if source collections that were part of the backfill tasks have
			//    been deleted, then the top tasks' mappings will be modified
			oldTopTasks := oldSpec.VBTasksMap.GetTopTasksOnlyClone()
			newTopTasks := newSpec.VBTasksMap.GetTopTasksOnlyClone()
			oldNamespaceMappings := oldTopTasks.GetAllCollectionNamespaceMappings()
			newNamespaceMappings := newTopTasks.GetAllCollectionNamespaceMappings()
			if !oldNamespaceMappings.SameAs(newNamespaceMappings) && newSpec.ReplicationSpec().Settings.Active {
				removedScopesMap := b.populateRemovedScopesMap(newNamespaceMappings, oldNamespaceMappings)
				cb, errCb := b.checkpointsSvc.GetCkptsMappingsCleanupCallback(base.CompileBackfillPipelineSpecId(newSpec.Id), newSpec.InternalId, removedScopesMap)
				err := b.pipelineMgr.HaltBackfillWithCb(changedSpecId, cb, errCb)
				if err != nil {
					b.logger.Errorf("Unable to request backfill pipeline to stop for %v : %v - backfill pipeline may be executing out of date backfills", changedSpecId, err)
					return err
				}
				err = b.pipelineMgr.RequestBackfill(changedSpecId)
				if err != nil {
					b.logger.Errorf("Unable to request backfill pipeline to start for %v : %v - may require manual restart of pipeline", changedSpecId, err)
					return err
				}
			}
		}

	}
	return nil
}

func (b *BackfillMgr) populateRemovedScopesMap(newNamespaceMappings metadata.ShaToCollectionNamespaceMap, oldNamespaceMappings metadata.ShaToCollectionNamespaceMap) metadata.ScopesMap {
	removedScopesMap := make(metadata.ScopesMap)
	_, removedShaMappings := newNamespaceMappings.Diff(oldNamespaceMappings)
	for _, collectionNsMapping := range removedShaMappings {
		for src, _ := range *collectionNsMapping {
			removedScopesMap.AddNamespace(src.ScopeName, src.CollectionName)
		}
	}
	return removedScopesMap
}

func (b *BackfillMgr) ReplicationSpecChangeCallback(changedSpecId string, oldSpecObj, newSpecObj interface{}, wg *sync.WaitGroup) error {
	if wg != nil {
		defer wg.Done()
	}

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
		b.replSpecHandlerMtx.Lock()
		defer b.replSpecHandlerMtx.Unlock()
		_, exists := b.replSpecHandlerMap[newSpec.Id]
		if exists {
			errMsg := fmt.Sprintf("%v - A duplicate spec creation is under way", newSpec.Id)
			b.logger.Errorf(errMsg)
			return fmt.Errorf(errMsg)
		}
		b.replSpecHandlerMap[newSpec.Id] = NewReplSpecHandler(b, newSpec)
		b.replSpecHandlerMap[newSpec.Id].HandleNewSpec()
	} else if newSpecObj.(*metadata.ReplicationSpecification) == nil &&
		oldSpecObj.(*metadata.ReplicationSpecification) != nil {
		oldSpec := oldSpecObj.(*metadata.ReplicationSpecification)
		// Tell retry mechanism to stop retrying for this spec if any exists
		b.retrySpecRemovalCh <- oldSpec.Id

		// Delete internal caches
		b.replSpecHandlerMtx.Lock()
		handler, exists := b.replSpecHandlerMap[oldSpec.Id]
		if exists {
			close(handler.finCh)
		}
		b.replSpecHandlerMtx.Unlock()

		b.cacheMtx.Lock()
		delete(b.cacheSpecSourceMap, changedSpecId)
		delete(b.cacheSpecTargetMap, changedSpecId)
		delete(b.cacheSpecLastSuccessfulManifestId, changedSpecId)
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

		if oldSettings.NeedToRestreamPipelineEvenIfStoppedDueToCollectionModeChanges(newSettings) {
			// Any changes between implicit or explicit mapping means all checkpoints are deleted and everything starts over
			// This is handled by replication spec service
			return nil
		} else if newCollectionMode.IsImplicitMapping() {
			// No need to worry about raising backfill for implicit mapping
			// This is handled by replication spec service
			return nil
		} else if !oldRoutingRules.SameAs(newRoutingRules) {
			// Explicit mapping and explicit mapping rules have changed
			specForId := metadata.ReplicationSpecification{Id: specId} /* only need the spec for the Id */
			srcMan, tgtMan, err := b.collectionsManifestSvc.GetLatestManifests(&specForId, false)
			if err != nil {
				b.logger.Errorf("error - Unable to retrieve manifests for spec %v due to %v - recommended to restream", specId, err)
				return err
			}
			manifestPair := metadata.CollectionsManifestPair{
				Source: srcMan,
				Target: tgtMan,
			}
			newMapping, err := metadata.NewCollectionNamespaceMappingFromRules(manifestPair, newCollectionMode, newRoutingRules, false, false)
			if err != nil {
				return err
			}
			oldMapping, err := metadata.NewCollectionNamespaceMappingFromRules(manifestPair, oldCollectionMode, oldRoutingRules, false, false)
			if err != nil {
				return err
			}
			added, removed := oldMapping.Diff(newMapping)
			err = b.handleExplicitMapChangeBackfillReq(specId, added, removed)
			if err != nil {
				if !b.validateReplIdExists(specId) {
					return nil
				}
				if errMeansReqNeedsToBeRetried(err) {
					req := BackfillRetryRequest{
						replId:                     specId,
						req:                        specForId,
						force:                      false,
						correspondingSrcManifestId: srcMan.Uid(),
						handler:                    b.internalGetHandler(specId),
					}
					b.retryBackfillRequest(req)
				}
				return err
			}

			// If pipeline was active during this callback's lifetime, then it means the ckpts created by the pipeline has outdated mappings. clean those up
			if oldSettings.Active {
				removedScopesMap := make(metadata.ScopesMap)
				for src, _ := range removed {
					removedScopesMap.AddNamespace(src.ScopeName, src.CollectionName)
				}
				cb, errCb := b.checkpointsSvc.GetCkptsMappingsCleanupCallback(specId, internalSpecId, removedScopesMap)
				err = cb()
				if err != nil {
					errCb(err)
				}
			}
			if err != nil {
				return err
			}
		}
		return nil
	}

	errCallback := func(err error) {
		// Do nothing, because callback above will retry if needed
	}

	return callback, errCallback
}

func (b *BackfillMgr) explicitMappingCbGenericErrHandler(err error, specId string, diff metadata.CollectionNamespaceMappingsDiffPair) {
	if err != nil {
		if !b.validateReplIdExists(specId) {
			return
		}

		if errMeansReqNeedsToBeRetried(err) {
			req := BackfillRetryRequest{
				replId:                     specId,
				req:                        diff,
				force:                      false,
				correspondingSrcManifestId: diff.RouterLatestSrcManifestId,
				handler:                    b.internalGetHandler(specId),
			}
			b.retryBackfillRequest(req)
		}
	}
}

func (b *BackfillMgr) checkUpToDateSpec(specId string, internalSpecId string) (error, bool) {
	// First, get most up-to-date spec to ensure this call is not out of date
	spec, err := b.replSpecSvc.ReplicationSpec(specId)
	if err != nil {
		if err == base.ReplNotFoundErr {
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
		b.explicitMappingCbGenericErrHandler(err, specId, diff)
	}
	return callback, errCb
}

func (b *BackfillMgr) postDeleteBackfillRepl(specId, internalId string) error {
	backfillSpecId := common.ComposeFullTopic(specId, common.BackfillPipeline)
	stopFunc := b.utils.StartDiagStopwatch(fmt.Sprintf("postDeleteBackfillRepl(%v)", specId), base.DiagInternalThreshold)
	defer stopFunc()
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

func (b *BackfillMgr) initNewReplStartingManifests(spec *metadata.ReplicationSpecification, finCh chan bool) error {
	var err error
	var src *metadata.CollectionsManifest
	var tgt *metadata.CollectionsManifest

	retryOp := func(interface{}) (interface{}, error) {
		src, tgt, err = b.collectionsManifestSvc.GetLatestManifests(spec, false)
		if err != nil {
			if strings.Contains(err.Error(), base.ErrorTargetCollectionsNotSupported.Error()) {
				defaultManifest := metadata.NewDefaultCollectionsManifest()
				tgt = &defaultManifest
				err = nil
			} else {
				b.logger.Errorf("Unable to retrieve manifests for new spec %v err %v", spec.Id, err)
			}
		}
		return nil, err
	}
	_, err = b.utils.ExponentialBackoffExecutorWithFinishSignal("BackfillInit", base.DefaultHttpTimeoutWaitTime, base.DefaultHttpTimeoutMaxRetry, base.DefaultHttpTimeoutRetryFactor, retryOp, nil, finCh)
	if err != nil {
		return err
	}

	b.cacheMtx.Lock()
	b.cacheSpecSourceMap[spec.Id] = src
	b.cacheSpecLastSuccessfulManifestId[spec.Id] = src.Uid()
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

	if !b.validateReplIdExists(replId) {
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

	b.cacheMtx.RLock()
	latestTgtManifestOrig := b.cacheSpecTargetMap[replId]
	if latestTgtManifestOrig == nil {
		if !modes.IsImplicitMapping() {
			// If there is no target manifest, it means that the target cluster is a <7.0 cluster that does not
			// have collections support. That also means that implicit mode is the only mode this replication can be in
			// If it is explicit mode, then the above assumption is broken and we should show a warning
			b.logger.Warnf("%v Target has no manifest", replId)
		}
		b.cacheMtx.RUnlock()
		b.markNewSourceManifest(replId, newSourceManifest.Uid())
		return
	}
	latestTgtManifestObj := latestTgtManifestOrig.Clone()
	latestTgtManifest := &latestTgtManifestObj
	b.cacheMtx.RUnlock()

	if latestTgtManifest == nil {
		if !modes.IsImplicitMapping() {
			// If there is no target manifest, it means that the target cluster is a <7.0 cluster that does not
			// have collections support. That also means that implicit mode is the only mode this replication can be in
			// If it is explicit mode, then the above assumption is broken and we should show a warning
			b.logger.Errorf("Unable to do mapping diff for %v if target manifest is nil", spec.Id)
		}
		b.markNewSourceManifest(replId, newSourceManifest.Uid())
		return
	}

	if modes.IsImplicitMapping() {
		diffPair := metadata.CollectionNamespaceMappingsDiffPair{}

		oldSrcToTargetMapping, _, _ := oldSourceManifest.ImplicitMap(latestTgtManifest)
		srcToTargetMapping, _, _ := newSourceManifest.ImplicitMap(latestTgtManifest)

		added, removed := oldSrcToTargetMapping.Diff(srcToTargetMapping)
		diffPair.Added = added
		diffPair.Removed = removed

		b.notifyBackfillMappingStatusUpdateToEventMgr(replId, diffPair, []*metadata.CollectionsManifest{oldSourceManifest, newSourceManifest})
		b.markNewSourceManifest(replId, newSourceManifest.Uid())
		return
	}

	diffPair, err := b.compileExplicitBackfillReq(spec, modes, oldSourceManifest, latestTgtManifest, newSourceManifest, latestTgtManifest)
	if err != nil {
		b.logger.Errorf("%v Error compiling explicit backfillReq: %v", spec.Id, err)
		return
	}
	b.notifyBackfillMappingStatusUpdateToEventMgr(replId, diffPair, []*metadata.CollectionsManifest{oldSourceManifest, newSourceManifest})
	err = b.raiseBackfillReq(replId, diffPair, false, newSourceManifest.Uid())
	if errMeansReqNeedsToBeRetried(err) {
		req := BackfillRetryRequest{
			replId:                     replId,
			req:                        diffPair,
			force:                      false,
			correspondingSrcManifestId: newSourceManifest.Uid(),
			handler:                    b.internalGetHandler(replId),
		}
		b.retryBackfillRequest(req)
	} else if err == nil {
		b.markNewSourceManifest(replId, newSourceManifest.Uid())
	} else {
		// Either queued for retry or spec deleted (errorStopped)
	}
}

func (b *BackfillMgr) internalGetHandler(replId string) *BackfillRequestHandler {
	b.specReqHandlersMtx.RLock()
	handler := b.specToReqHandlerMap[replId]
	b.specReqHandlersMtx.RUnlock()
	if handler == nil {
		// This can happen in two cases:
		// 1. Replication spec is deleted
		// 2. Pipeline is currently stopped
		b.logger.Errorf("Unable to find handler for spec %v", replId)
	}
	return handler
}

func (b *BackfillMgr) markNewSourceManifest(replId string, newSourceManifestId uint64) {
	b.cacheMtx.Lock()
	manifestId, replExists := b.cacheSpecLastSuccessfulManifestId[replId]
	if replExists && newSourceManifestId > manifestId {
		b.cacheSpecLastSuccessfulManifestId[replId] = newSourceManifestId
	}
	b.cacheMtx.Unlock()
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

	if oldSourceManifest == nil {
		b.cacheMtx.RLock()
		oldSourceManifest = b.cacheSpecSourceMap[replId]
		b.cacheMtx.RUnlock()
	}

	// Update cache
	b.cacheMtx.Lock()
	if sourceChanged {
		b.cacheSpecSourceMap[replId] = newSourceManifest
	}
	b.cacheSpecTargetMap[replId] = newTargetManifest
	b.cacheMtx.Unlock()

	b.diffManifestsAndRaiseBackfill(replId, oldSourceManifest, newSourceManifest, oldTargetManifest, newTargetManifest)
}

func (b *BackfillMgr) diffManifestsAndRaiseBackfill(replId string, oldSourceManifest *metadata.CollectionsManifest, newSourceManifest *metadata.CollectionsManifest, oldTargetManifest *metadata.CollectionsManifest, newTargetManifest *metadata.CollectionsManifest) {
	spec, err := b.replSpecSvc.ReplicationSpec(replId)
	if err != nil {
		b.logger.Errorf("Unable to retrieve repl spec %v to determine whether it is implicit or explicit mapping", replId)
		return
	}
	modes := spec.Settings.GetCollectionModes()

	if newSourceManifest.Uid() == 0 && modes.IsImplicitMapping() {
		// Source manifest 0 means only default collection is being replicated
		b.logger.Infof("Repl %v shows default source manifest, and not under explicit nor migration mode, thus no backfill would be created", replId)
		return
	}

	var backfillReq interface{}
	var skipRaiseBackfillReq bool
	var diffPair metadata.CollectionNamespaceMappingsDiffPair
	if !modes.IsImplicitMapping() {
		if newTargetManifest == nil {
			b.logger.Errorf("%v Unable to do explicit mapping if target manifest is nil", spec.Id)
			return
		}
		backfillReq, skipRaiseBackfillReq = b.populateBackfillReqForExplicitMapping(replId, oldSourceManifest, newSourceManifest, oldTargetManifest, newTargetManifest, spec, modes, backfillReq)
		backfillReqDiffPair := backfillReq.(metadata.CollectionNamespaceMappingsDiffPair)
		b.notifyBackfillMappingStatusUpdateToEventMgr(replId, backfillReqDiffPair, []*metadata.CollectionsManifest{oldSourceManifest, newSourceManifest})
	} else {
		backfillReq, diffPair, skipRaiseBackfillReq = b.populateBackfillReqForImplicitMapping(newTargetManifest, oldTargetManifest, newSourceManifest, spec)
		b.notifyBackfillMappingStatusUpdateToEventMgr(replId, diffPair, []*metadata.CollectionsManifest{oldSourceManifest, newSourceManifest})
	}
	if !skipRaiseBackfillReq {
		err = b.raiseBackfillReq(replId, backfillReq, false, newSourceManifest.Uid())
	}

	if errMeansReqNeedsToBeRetried(err) {
		req := BackfillRetryRequest{
			replId:                     replId,
			req:                        backfillReq,
			force:                      false,
			correspondingSrcManifestId: newSourceManifest.Uid(),
			handler:                    b.internalGetHandler(replId),
		}
		b.retryBackfillRequest(req)
	} else if err == nil {
		// Only mark if successfull
		b.markNewSourceManifest(replId, newSourceManifest.Uid())
	} else {
		// Either it is in the retry queue, or the spec is deleted (errorStopped)
	}

	if modes.IsImplicitMapping() {
		// For implicit mapping, see if there are things that need to be cleaned up
		b.cleanupInvalidImplicitBackfillMappings(replId, oldSourceManifest, newSourceManifest)
	}
}

// The srcManifestsDiff is needed to detect any removed source namespaces that are not part of the implicit mapping
func (b *BackfillMgr) notifyBackfillMappingStatusUpdateToEventMgr(replId string, sentPair metadata.CollectionNamespaceMappingsDiffPair, srcManifestsDiff []*metadata.CollectionsManifest) error {
	err := b.pipelineMgr.BackfillMappingStatusUpdate(replId, &sentPair, srcManifestsDiff)
	if err != nil {
		// error could occur only if serializer is stopped which shouldn't happen in normal circumstances
		b.logger.Warnf("Unable to raise BackfillMappingStatusUpdate %v", err)
	}
	return err
}

func errMeansReqNeedsToBeRetried(err error) bool {
	return err != nil && err != QueuedForRetry && err != errorStopped
}

func (b *BackfillMgr) cleanupInvalidImplicitBackfillMappings(replId string, oldSourceManifest *metadata.CollectionsManifest, newSourceManifest *metadata.CollectionsManifest) bool {
	_, _, removed, err := newSourceManifest.Diff(oldSourceManifest)
	if err != nil {
		b.logger.Errorf("Unable to perform cleanup due to err %v", err)
		return true
	}
	if len(removed) > 0 {
		cleanupNamespace := metadata.CollectionNamespaceMapping{}
		for scopeName, scopeDetail := range removed {
			for collectionName, _ := range scopeDetail.Collections {
				implicitMappingNamespace := &base.CollectionNamespace{
					ScopeName:      scopeName,
					CollectionName: collectionName,
				}
				cleanupNamespace.AddSingleMapping(implicitMappingNamespace, implicitMappingNamespace)
			}
		}
		cleanupPair := metadata.CollectionNamespaceMappingsDiffPair{
			Removed: cleanupNamespace,
		}
		err = b.raiseBackfillReq(replId, cleanupPair, false, newSourceManifest.Uid())
		if errMeansReqNeedsToBeRetried(err) {
			req := BackfillRetryRequest{
				replId:                     replId,
				req:                        cleanupPair,
				force:                      false,
				correspondingSrcManifestId: newSourceManifest.Uid(),
				handler:                    b.internalGetHandler(replId),
			}
			b.retryBackfillRequest(req)
		}
	}
	return false
}

func (b *BackfillMgr) populateBackfillReqForImplicitMapping(newTargetManifest *metadata.CollectionsManifest, oldTargetManifest *metadata.CollectionsManifest, newSourceManifest *metadata.CollectionsManifest, spec *metadata.ReplicationSpecification) (metadata.CollectionNamespaceMapping, metadata.CollectionNamespaceMappingsDiffPair, bool) {
	backfillMapping, addedRemovedPair, err := newTargetManifest.ImplicitGetBackfillCollections(oldTargetManifest, newSourceManifest)
	if err != nil {
		b.logger.Errorf("%v handleSrcAndTgtChanges error: %v", spec.Id, err)
		return nil, addedRemovedPair, true
	}
	if len(backfillMapping) == 0 {
		// If no backfill is needed such as when collections get del' on the target, don't raise backfill request
		return nil, addedRemovedPair, true
	}
	return backfillMapping, addedRemovedPair, false
}

func (b *BackfillMgr) populateBackfillReqForExplicitMapping(replId string, oldSourceManifest *metadata.CollectionsManifest, newSourceManifest *metadata.CollectionsManifest, oldTargetManifest *metadata.CollectionsManifest, newTargetManifest *metadata.CollectionsManifest, spec *metadata.ReplicationSpecification, modes base.CollectionsMgtType, backfillReq interface{}) (interface{}, bool) {
	// TODO - revisit this logic - some of these may not be needed
	if oldSourceManifest != nil && oldTargetManifest != nil && newSourceManifest != nil && newTargetManifest != nil {
		// Both changed
		diffPair, err := b.compileExplicitBackfillReq(spec, modes, oldSourceManifest, oldTargetManifest, newSourceManifest, newTargetManifest)
		if err != nil {
			return nil, true
		}
		backfillReq = diffPair
	} else if oldTargetManifest != nil && newTargetManifest != nil {
		// Only target changed
		sourceManifest := b.getLatestSourceManifestClone(replId)
		if sourceManifest == nil {
			return nil, true
		}
		diffPair, err := b.compileExplicitBackfillReq(spec, modes, sourceManifest, oldTargetManifest, sourceManifest, newTargetManifest)
		if err != nil {
			return nil, true
		}
		backfillReq = diffPair
	} else if newTargetManifest != nil {
		// potentially brand new target manifest change - backfill everything
		sourceManifest := b.getLatestSourceManifestClone(replId)
		if sourceManifest == nil {
			return nil, true
		}
		manifestsPair := metadata.CollectionsManifestPair{
			Source: sourceManifest,
			Target: newTargetManifest,
		}
		explicitMapping, err := metadata.NewCollectionNamespaceMappingFromRules(manifestsPair, modes, spec.Settings.GetCollectionsRoutingRules(), false, false)
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
	return backfillReq, false
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
	oldExplicitMap, err := metadata.NewCollectionNamespaceMappingFromRules(pair, modes, spec.Settings.GetCollectionsRoutingRules(), false, true)
	if err != nil {
		b.logger.Errorf("%v Error compiling old explicit map: %v", spec.Id, err)
		panic("FIXME")
		return metadata.CollectionNamespaceMappingsDiffPair{}, err
	}
	pair.Source = newSourceManifest
	pair.Target = newTargetManifest
	newExplicitMap, err := metadata.NewCollectionNamespaceMappingFromRules(pair, modes, spec.Settings.GetCollectionsRoutingRules(), false, true)
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

var QueuedForRetry = fmt.Errorf("queued a job for retry")

func (b *BackfillMgr) raiseBackfillReq(replId string, backfillReq interface{}, overridePreviousBackfills bool, newSourceManifestId uint64) error {
	handler := b.internalGetHandler(replId)
	b.errorRetryQMtx.RLock()
	if handler == nil || len(b.errorRetryQueue) > 0 {
		// Don't jump ahead of queue
		b.errorRetryQMtx.RUnlock()
		b.errorRetryQMtx.Lock()
		defer b.errorRetryQMtx.Unlock()
		newRetryJob := BackfillRetryRequest{
			replId:                     replId,
			req:                        backfillReq,
			force:                      overridePreviousBackfills,
			correspondingSrcManifestId: newSourceManifestId,
			handler:                    handler,
		}
		b.errorRetryQueue = append(b.errorRetryQueue, newRetryJob)
		return QueuedForRetry
	} else {
		b.errorRetryQMtx.RUnlock()
		return handler.handleBackfillRequestWithArgs(backfillReq, overridePreviousBackfills)
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

func (b *BackfillMgr) cleanupSpecHandlerCb(specId string) {
	b.replSpecHandlerMtx.Lock()
	defer b.replSpecHandlerMtx.Unlock()

	delete(b.replSpecHandlerMap, specId)
}

func (b *BackfillMgr) RequestCompleteBackfill(specId string) error {
	backfillReq, err := b.onDemandBackfillGetCompleteRequest(specId, nil)
	if err != nil {
		return err
	}

	if backfillReq == nil {
		// This means nothing needs to be raised
		return nil
	}

	err = b.onDemandBackfillRaiseRequest(specId, backfillReq)
	return err
}

func (b *BackfillMgr) RequestOnDemandBackfill(specId string, pendingMappings metadata.CollectionNamespaceMapping) error {
	backfillReq, err := b.onDemandBackfillGetCompleteRequest(specId, pendingMappings)
	if err != nil {
		return err
	}

	if backfillReq == nil {
		// This means nothing needs to be raised
		return nil
	}

	// Given backfillReq contains everything that needs to be backfilled, we only want to care about the source namespace of pendingMappings
	backfillReq, isEmpty := b.filterBackfillReqBasedOnRequestedNamespaces(backfillReq, pendingMappings)

	if isEmpty {
		b.logger.Warnf("the given pending request %v does not translate into a valid backfillRequest given current rules and manifests", pendingMappings.String())
		return nil
	}

	err = b.onDemandBackfillRaiseRequest(specId, backfillReq)
	return err
}

func (b *BackfillMgr) onDemandBackfillRaiseRequest(specId string, backfillReq interface{}) error {
	err := b.raiseBackfillReq(specId, backfillReq, true, 0)
	if err == base.GetBackfillFatalDataLossError(specId) {
		// fatal error means restream is happening - everything is deleted, so let it pass
		err = nil
	}
	if err != nil {
		b.logger.Errorf("raising pending backfill - %v", err)
	}
	return err
}

// If pendingMappings is not nil, it's requesting a subset
// If pendingMappings is nil, it's requesting everything
func (b *BackfillMgr) onDemandBackfillGetCompleteRequest(specId string, pendingMappings metadata.CollectionNamespaceMapping) (interface{}, error) {
	b.specReqHandlersMtx.RLock()
	handler := b.specToReqHandlerMap[specId]
	b.specReqHandlersMtx.RUnlock()
	if handler == nil {
		b.logger.Errorf("Unable to find handler for spec %v", specId)
		return nil, base.ErrorNotFound
	}

	b.cacheMtx.RLock()
	currentCachedManifest, ok := b.cacheSpecSourceMap[specId]
	if !ok {
		// should not be the case
		b.logger.Errorf("Unable to find cached manifest for %v", specId)
		return nil, base.ErrorNotFound
	}
	clonedSourceManifest := currentCachedManifest.Clone()
	currentCachedManifest, ok = b.cacheSpecTargetMap[specId]
	if !ok {
		// should not be the case
		b.logger.Errorf("Unable to find cached target manifest for %v", specId)
		return nil, base.ErrorNotFound
	}
	clonedTargetManifest := currentCachedManifest.Clone()
	b.cacheMtx.RUnlock()

	spec, err := b.replSpecSvc.ReplicationSpec(specId)
	if err != nil {
		b.logger.Errorf("Unable to retrieve repl spec %v to determine whether it is implicit or explicit mapping", specId)
		return nil, base.ErrorNotFound
	}

	modes := spec.Settings.GetCollectionModes()

	if clonedSourceManifest.Uid() == 0 && modes.IsImplicitMapping() {
		// Source manifest 0 means only default collection is being replicated
		b.logger.Infof("Repl %v shows default source manifest, and not under explicit nor migration mode, thus no backfill would be created", specId)
		return nil, nil
	}

	requestingStr := "everything"
	if len(pendingMappings) > 0 {
		requestingStr = pendingMappings.String()
	}

	var backfillReq interface{}
	// Backfill everything means comparing to an empty/default manifest
	defaultManifest := metadata.NewDefaultCollectionsManifest()
	if modes.IsImplicitMapping() {
		b.logger.Infof("Forced backfill for implicit mapping, given source manifest version %v target manifest version %v, requesting: %v", clonedSourceManifest.Uid(), clonedTargetManifest.Uid(), requestingStr)
		backfillReq, _, _ = b.populateBackfillReqForImplicitMapping(&clonedTargetManifest, &defaultManifest /*oldTarget*/, &clonedSourceManifest, spec)
	} else {
		b.logger.Infof("Forced backfill for explicit mapping, given source manifest version %v target manifest version %v and rules: %v, requesting: %v", clonedSourceManifest.Uid(), clonedTargetManifest.Uid(), spec.Settings.GetCollectionsRoutingRules(), requestingStr)
		backfillReq, _ = b.populateBackfillReqForExplicitMapping(specId, &defaultManifest /*oldSrc*/, &clonedSourceManifest, &defaultManifest /*oldTgt*/, &clonedTargetManifest, spec, modes, backfillReq)
	}
	return backfillReq, err
}

// Given backfillReq, take out everything that is not based on the source namespace within the pendingMappings
func (b *BackfillMgr) filterBackfillReqBasedOnRequestedNamespaces(backfillReq interface{}, pendingMappings metadata.CollectionNamespaceMapping) (interface{}, bool) {
	if nsMapping, ok := backfillReq.(metadata.CollectionNamespaceMapping); ok {
		nsMapping = deleteNonPendingMappingsFromNsMapping(nsMapping, pendingMappings)
		return nsMapping, len(nsMapping) == 0
	} else if diffPair, ok := backfillReq.(metadata.CollectionNamespaceMappingsDiffPair); ok {
		// Only worry about added
		diffPair.Added = deleteNonPendingMappingsFromNsMapping(diffPair.Added, pendingMappings)
		return diffPair, len(diffPair.Added) == 0
	} else {
		b.logger.Errorf(fmt.Sprintf("invalid type: %v", reflect.TypeOf(backfillReq)))
		return backfillReq, true
	}
}

func deleteNonPendingMappingsFromNsMapping(nsMapping metadata.CollectionNamespaceMapping, pendingMappings metadata.CollectionNamespaceMapping) metadata.CollectionNamespaceMapping {
	subsetToDelete := make(metadata.CollectionNamespaceMapping)
	for srcNs, tgts := range nsMapping {
		_, _, _, exists := pendingMappings.Get(srcNs.CollectionNamespace, nil)
		if !exists {
			subsetToDelete[srcNs] = tgts
		}
	}

	if len(subsetToDelete) > 0 {
		nsMapping = nsMapping.Delete(subsetToDelete)
	}
	return nsMapping
}

func (b *BackfillMgr) GetLastSuccessfulSourceManifestId(specId string) (uint64, error) {
	b.cacheMtx.RLock()
	defer b.cacheMtx.RUnlock()

	id, found := b.cacheSpecLastSuccessfulManifestId[specId]
	if !found {
		return 0, base.ErrorNotFound
	}
	return id, nil
}

func (b *BackfillMgr) SetLastSuccessfulSourceManifestId(specId string, manifestId uint64, dcpRollbackScenario bool, finCh chan bool) error {
	// When backfill mgr receives a new replication spec, it goes off and creates the cache structures on its own go-routine
	// This method is called by checkpoint mgr when resuming a pipeline... and if somehow the pipeline checkpoint resumes
	// before the init process has been completed, then this needs to wait to ensure a proper diff
	setFunc := func(interface{}) (interface{}, error) {
		b.cacheMtx.Lock()

		_, found := b.cacheSpecLastSuccessfulManifestId[specId]
		if !found {
			// Means this spec hasn't been initialized yet
			b.cacheMtx.Unlock()
			return nil, base.ErrorNotFound
		}

		var updated bool
		if dcpRollbackScenario {
			// rollback scenario set only if lower
			if manifestId < b.cacheSpecLastSuccessfulManifestId[specId] {
				updated = true
				b.cacheSpecLastSuccessfulManifestId[specId] = manifestId
			}
		} else {
			// non rollback scenario set only if non-0
			updated = true
			b.cacheSpecLastSuccessfulManifestId[specId] = manifestId
		}
		currentSourceManifest := b.cacheSpecSourceMap[specId].Clone()
		currentTargetManifest := b.cacheSpecTargetMap[specId].Clone()
		b.cacheMtx.Unlock()

		if updated && manifestId < currentSourceManifest.Uid() {
			tempSpec := &metadata.ReplicationSpecification{
				Id: specId,
			}
			oldSourceManifest, err := b.collectionsManifestSvc.GetSpecificSourceManifest(tempSpec, manifestId)
			if err != nil {
				if manifestId > 0 {
					b.logger.Warnf("Unable to find old source manifest %v for version %v, diffing against default collection", manifestId)
				}
				defaultManifest := metadata.NewDefaultCollectionsManifest()
				oldSourceManifest = &defaultManifest
			}

			b.diffManifestsAndRaiseBackfill(specId, oldSourceManifest, &currentSourceManifest, &currentTargetManifest, &currentTargetManifest)
		}
		return nil, nil
	}

	_, err := b.utils.ExponentialBackoffExecutorWithFinishSignal("SetLastSuccessfulSourceManifestId", base.RetryIntervalMetakv, base.MaxNumOfMetakvRetries, base.MetaKvBackoffFactor, setFunc, nil, finCh)
	if err != nil && !strings.Contains(err.Error(), base.FinClosureStr) {
		panic("timeout trying to resume set source manifestID. May result in data loss")
	}
	return err
}

func (b *BackfillMgr) runRetryMonitor() {
	periodicRetryTimer := time.NewTicker(b.retryTimerPeriod)
	for {
		select {
		case <-b.finCh:
			periodicRetryTimer.Stop()
			return
		case removedSpecId := <-b.retrySpecRemovalCh:
			b.cleanupBackfillRetryQueue(removedSpecId)
		case <-periodicRetryTimer.C:
			b.errorRetryQMtx.RLock()
			if len(b.errorRetryQueue) == 0 {
				// No jobs to retry
				b.errorRetryQMtx.RUnlock()
			} else {
				b.errorRetryQMtx.RUnlock()
				b.errorRetryQMtx.Lock()
				// find the smallest manifestId to process
				var unableToBeProcessedQueue []BackfillRetryRequest
				var smallestJob uint64
				var lastRetryWasSuccessful = true

				for len(b.errorRetryQueue) > 0 {
					// Find the earliest job first to process, according to the srcManifestsID it is supposed to set
					minIdx, job := b.retryQueueFindSmallestManifestRequest()
					if lastRetryWasSuccessful {
						smallestJob = job.correspondingSrcManifestId
					} else {
						// Do not retry a job that has a higher source manifest ID than the last failed job
						if job.correspondingSrcManifestId > smallestJob {
							break
						}
					}
					// Unlock before doing metakv ops - in the meantime, queue may be appended but minIdx location should not be affected
					b.errorRetryQMtx.Unlock()
					lastRetryWasSuccessful = b.retryJob(job, &unableToBeProcessedQueue)
					b.errorRetryQMtx.Lock()
					// Regardless of success or failure, remove the job from errorRetryQueue
					b.errorRetryQueue = append(b.errorRetryQueue[:minIdx], b.errorRetryQueue[minIdx+1:]...)
				}
				// After cycling through the errorRetryQueue, re-add those jobs that weren't able to successfully run
				for _, unableToBeProcessedJob := range unableToBeProcessedQueue {
					b.errorRetryQueue = append(b.errorRetryQueue, unableToBeProcessedJob)
				}
				b.errorRetryQMtx.Unlock()
			}
		}
	}
}

// Used internally - retry a BackfillRetryRequest. In the case of error, put it in the unableToBeProcessedQueue
// Returns boolean to indicate whether or not the retry was successful
func (b *BackfillMgr) retryJob(job BackfillRetryRequest, unableToBeProcessedQueue *[]BackfillRetryRequest) bool {
	handler := job.handler
	var err error
	if handler == nil {
		handler = b.internalGetHandler(job.replId)
		if handler == nil {
			err = fmt.Errorf("unable to find backfill handler for %v", job.replId)
		}
	}
	if handler != nil {
		err = handler.handleBackfillRequestWithArgs(job.req, job.force)
	}
	if err != nil {
		// errorStopped by handler means that a repl spec has been deleted. If a spec hasn't been
		// deleted, even if it is just stopped, it is prob safer to put it in errorRetryQueue
		b.logger.Errorf("Retrying job %v failed due to %v - will try again next cycle", job.req, err)
		*unableToBeProcessedQueue = append(*unableToBeProcessedQueue, job)
		return false
	} else {
		// Successfully handled
		b.markNewSourceManifest(job.replId, job.correspondingSrcManifestId)
		return true
	}
}

// Used internally - lock must be held
func (b *BackfillMgr) retryQueueFindSmallestManifestRequest() (int, BackfillRetryRequest) {
	var minSourceManifestId uint64 = math.MaxUint64
	var minIdx int
	for i := 0; i < len(b.errorRetryQueue); i++ {
		if b.errorRetryQueue[i].correspondingSrcManifestId < minSourceManifestId {
			minIdx = i
			minSourceManifestId = b.errorRetryQueue[i].correspondingSrcManifestId
		}
	}
	job := b.errorRetryQueue[minIdx]
	return minIdx, job
}

func (b *BackfillMgr) retryBackfillRequest(req BackfillRetryRequest) {
	b.errorRetryQMtx.Lock()
	defer b.errorRetryQMtx.Unlock()

	b.logger.Infof("BackfillMgr queued up to retry backfill request for replId %v with req %v", req.replId, req.req)
	b.errorRetryQueue = append(b.errorRetryQueue, req)

}

func (b *BackfillMgr) DelAllBackfills(topic string) error {
	b.specReqHandlersMtx.RLock()
	handler := b.specToReqHandlerMap[topic]
	b.specReqHandlersMtx.RUnlock()

	if handler == nil {
		// This should not happen
		err := fmt.Errorf("Unable to find handler for spec %v", topic)
		b.logger.Errorf(err.Error())
		return err
	}

	return handler.DelAllBackfills()
}

func (b *BackfillMgr) DelBackfillForVB(topic string, vbno uint16) error {
	b.specReqHandlersMtx.RLock()
	handler := b.specToReqHandlerMap[topic]
	b.specReqHandlersMtx.RUnlock()

	if handler == nil {
		// This should not happen
		err := fmt.Errorf("Unable to find handler for spec %v", topic)
		b.logger.Errorf(err.Error())
		return err
	}

	cb, errCb := handler.GetDelVBSpecificBackfillCb(vbno)
	err := b.pipelineMgr.HaltBackfillWithCb(topic, cb, errCb)
	if err != nil {
		b.logger.Errorf("Unable to request backfill pipeline to stop for %v : %v - backfill for VB %v may occur", topic, err, vbno)
		return err
	}
	err = b.pipelineMgr.RequestBackfill(topic)
	if err != nil {
		b.logger.Errorf("Unable to request backfill pipeline to start for %v : %v - may require manual restart of pipeline", topic, err)
		return err
	}
	return nil
}

func (b *BackfillMgr) cleanupBackfillRetryQueue(removedSpecId string) {
	b.errorRetryQMtx.Lock()
	defer b.errorRetryQMtx.Unlock()

	var replacementQueue []BackfillRetryRequest

	for i := 0; i < len(b.errorRetryQueue); i++ {
		job := b.errorRetryQueue[i]
		if job.replId == removedSpecId {
			continue
		}
		replacementQueue = append(replacementQueue, job)
	}

	b.errorRetryQueue = replacementQueue
}

// Returns true if replId still exists
func (b *BackfillMgr) validateReplIdExists(replId string) bool {
	allSpecs, err := b.replSpecSvc.AllReplicationSpecIds()
	if err != nil {
		// Pretend it exists and let retry mechanism clean up
		return true
	}
	for _, specId := range allSpecs {
		if specId == replId {
			return true
		}
	}
	return false
}
