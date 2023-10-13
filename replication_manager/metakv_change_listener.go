// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package replication_manager

import (
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/pipeline_svc"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/metadata_svc"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/resource_manager"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
)

var SetTimeSyncRetryInterval = 10 * time.Second
var BucketSettingsChanSize = 100
var CallbackChanSize = 100

// generic listener for metadata stored in metakv
type MetakvChangeListener struct {
	id                         string
	dirpath                    string
	cancel_chan                chan struct{}
	number_of_retry            int
	children_waitgrp           *sync.WaitGroup
	metadata_service_call_back base.MetadataServiceCallback
	logger                     *log.CommonLogger
	utils                      utilities.UtilsIface
	cbSerializer               *callbackSerializer
}

type callbackSerializer struct {
	jobTopicMap map[string]chan metakv.KVEntry
	mapMtx      sync.RWMutex
	logger      *log.CommonLogger
	asyncCb     func(string, []byte, interface{})
}

func newCallbackSerializer(logger *log.CommonLogger, asyncCb func(path string, value []byte, rev interface{})) *callbackSerializer {
	return &callbackSerializer{
		jobTopicMap: map[string]chan metakv.KVEntry{},
		mapMtx:      sync.RWMutex{},
		logger:      logger,
		asyncCb:     asyncCb,
	}
}

func (c *callbackSerializer) distributeJob(kve metakv.KVEntry) {
	c.mapMtx.RLock()
	defer c.mapMtx.RUnlock()
	jobCh, ok := c.jobTopicMap[kve.Path]
	if ok {
		select {
		case jobCh <- kve:
		}
	} else {
		// job channel doesn't exist yet
		c.mapMtx.RUnlock()
		c.mapMtx.Lock()
		jobCh, ok = c.jobTopicMap[kve.Path]
		if ok {
			// jumped ahead
			select {
			case jobCh <- kve:
			}
		} else {
			c.jobTopicMap[kve.Path] = make(chan metakv.KVEntry, CallbackChanSize)
			c.jobTopicMap[kve.Path] <- kve
			go c.handleJobs(kve.Path)
		}
		c.mapMtx.Unlock()
		c.mapMtx.RLock()
	}

}

// One go instance of this method is running per path
// Once no more can be done, it'll exit
func (c *callbackSerializer) handleJobs(path string) {
	c.mapMtx.RLock()
	jobCh, ok := c.jobTopicMap[path]
	if !ok {
		c.logger.Errorf("Error: job multiplex channel for %v does not exist", path)
		c.mapMtx.RUnlock()
		return
	}
	c.mapMtx.RUnlock()

forloop:
	for {
		select {
		case job := <-jobCh:
			c.asyncCb(job.Path, job.Value, job.Rev)
		default:
			// no more jobs at this moment
			go c.cleanupJob(path)
			break forloop
		}
	}
}

func (c *callbackSerializer) cleanupJob(path string) {
	c.mapMtx.Lock()
	defer c.mapMtx.Unlock()

	_, ok := c.jobTopicMap[path]
	if ok {
		if len(c.jobTopicMap[path]) == 0 {
			delete(c.jobTopicMap, path)
		} else {
			// Someone else snuck in a job while we're supposed to clean up. Re-launch handler, which will finish
			// the job(s) and relaunch another cleanupJob() while this one returns
			go c.handleJobs(path)
		}
	}
}

func NewMetakvChangeListener(id, dirpath string, cancel_chan chan struct{},
	children_waitgrp *sync.WaitGroup,
	metadata_service_call_back base.MetadataServiceCallback,
	logger_ctx *log.LoggerContext,
	logger_name string,
	utilsIn utilities.UtilsIface) *MetakvChangeListener {
	logger := log.NewLogger(logger_name, logger_ctx)
	mcl := &MetakvChangeListener{
		id:                         id,
		dirpath:                    dirpath,
		cancel_chan:                cancel_chan,
		children_waitgrp:           children_waitgrp,
		metadata_service_call_back: metadata_service_call_back,
		logger:                     logger,
		utils:                      utilsIn,
	}
	mcl.cbSerializer = newCallbackSerializer(logger, mcl.metakvCallback_async)

	return mcl
}

func (mcl *MetakvChangeListener) Id() string {
	return mcl.id
}

func (mcl *MetakvChangeListener) Start() error {

	mcl.children_waitgrp.Add(1)
	go mcl.observeChildren()

	mcl.logger.Infof("Started MetakvChangeListener %v\n", mcl.Id())
	return nil
}

func (mcl *MetakvChangeListener) observeChildren() {
	defer mcl.children_waitgrp.Done()
	err := metakv.RunObserveChildren(mcl.dirpath, mcl.metakvCallback, mcl.cancel_chan)
	// call failure call back only when there are real errors
	// err may be nil when observeChildren is canceled, in which case there is no need to call failure call back
	mcl.failureCallback(err)
}

// Implement callback function for metakv
// Never returns err since we do not want RunObserveChildren to abort
func (mcl *MetakvChangeListener) metakvCallback(kve metakv.KVEntry) error {
	mcl.logger.Infof("metakvCallback called on listener %v with path = %v\n", mcl.Id(), kve.Path)

	mcl.cbSerializer.distributeJob(kve)
	return nil
}

// Implement callback function for metakv
func (mcl *MetakvChangeListener) metakvCallback_async(path string, value []byte, rev interface{}) {
	err := mcl.metadata_service_call_back(path, value, rev)
	if err != nil {
		mcl.logger.Errorf("Error calling metadata service call back for listener %v. err=%v\n", mcl.Id(), err)
	}

	return

}

// callback function for listener failure event
func (mcl *MetakvChangeListener) failureCallback(err error) {
	mcl.logger.Infof("metakv.RunObserveChildren failed, err=%v\n", err)
	if err == nil && !isReplicationManagerRunning() {
		//callback is cancelled and replication_mgr is exiting.
		//no-op
		return
	}
	if mcl.number_of_retry < base.MaxNumOfMetakvRetries {
		// Incremental backoff to wait for metakv server to be ready - and restart listener
		var timeToSleep = time.Duration(mcl.number_of_retry+1) * base.RetryIntervalMetakv
		// Once we've calculated the timeToSleep correctly, then increment the number_of_retry
		mcl.number_of_retry++
		mcl.logger.Infof("metakv.RunObserveChildren (%v) will retry in %v ...\n", mcl.Id(), timeToSleep)
		time.Sleep(timeToSleep)
		mcl.Start()
	} else {
		// exit process if max retry reached
		mcl.logger.Infof("metakv.RunObserveChildren (%v) failed after max retry %v\n", mcl.Id(), base.MaxNumOfMetakvRetries)
		exitProcess(false)
	}
}

// listener for replication spec
type ReplicationSpecChangeListener struct {
	*MetakvChangeListener
	resourceManager resource_manager.ResourceMgrIface
}

func NewReplicationSpecChangeListener(repl_spec_svc service_def.ReplicationSpecSvc,
	cancel_chan chan struct{},
	children_waitgrp *sync.WaitGroup,
	logger_ctx *log.LoggerContext,
	utilsIn utilities.UtilsIface,
	resourceManager resource_manager.ResourceMgrIface) *ReplicationSpecChangeListener {
	rscl := &ReplicationSpecChangeListener{
		NewMetakvChangeListener(base.ReplicationSpecChangeListener,
			metadata_svc.GetCatalogPathFromCatalogKey(metadata_svc.ReplicationSpecsCatalogKey),
			cancel_chan,
			children_waitgrp,
			repl_spec_svc.ReplicationSpecServiceCallback,
			logger_ctx,
			"ReplicationSpecChangeListener",
			utilsIn),
		resourceManager,
	}
	return rscl
}

// Handler callback for replication spec changed event
func (rscl *ReplicationSpecChangeListener) replicationSpecChangeHandlerCallback(changedSpecId string, oldSpecObj interface{}, newSpecObj interface{}, wg *sync.WaitGroup) error {
	if wg != nil {
		defer wg.Done()
	}
	topic := changedSpecId

	oldSpec, err := rscl.validateReplicationSpec(oldSpecObj)
	if err != nil {
		return err
	}
	newSpec, err := rscl.validateReplicationSpec(newSpecObj)
	if err != nil {
		return err
	}

	rscl.logger.Infof("specChangedCallback called on id = %v, oldSpec=%v, newSpec=%v\n", topic, oldSpec.CloneAndRedact(), newSpec.CloneAndRedact())
	if oldSpec != nil {
		rscl.logger.Infof("old spec settings=%v\n", oldSpec.Settings.CloneAndRedact())
	}
	if newSpec != nil {
		rscl.logger.Infof("new spec settings=%v\n", newSpec.Settings.CloneAndRedact())
	}

	if newSpec == nil {
		// Replication Spec is deleted.
		err = replication_mgr.pipelineMgr.DeletePipeline(topic)
		if err == nil {
			go replication_mgr.resourceMgr.HandlePipelineDeletion(topic)
		}
		return err
	}

	specActive := newSpec.Settings.Active
	//if the replication doesn't exit, it is treated the same as it exits, but it is paused
	specActiveOld := false
	var oldSettings *metadata.ReplicationSettings = nil
	if oldSpec != nil {
		oldSettings = oldSpec.Settings
		specActiveOld = oldSettings.Active
	}

	if specActiveOld && specActive {
		// if some critical settings have been changed, stop, reconstruct, and restart pipeline
		if needToReconstructPipeline(oldSettings, newSpec.Settings) {
			if needToRestreamPipeline(oldSettings, newSpec.Settings) {
				err := replication_mgr.pipelineMgr.ReInitStreams(topic)
				if err != nil {
					rscl.logger.Errorf("Unable to queue re-initialize streams job for pipeline %v to restart: %v", topic, err)
				}
				return err
			}
			callback, errCb, needCb := needSpecialCallbackUpdate(topic, newSpec.InternalId, oldSettings, newSpec.Settings)
			rscl.logger.Infof("Restarting pipeline %v since the changes to replication spec are critical: needSpecialCallback? %v", topic, needCb)
			if needCb {
				return replication_mgr.pipelineMgr.UpdatePipelineWithStoppedCb(topic, callback, errCb)
			} else {
				return replication_mgr.pipelineMgr.UpdatePipeline(topic, nil)
			}
		} else {
			// otherwise, perform live update to pipeline
			err := rscl.liveUpdatePipeline(topic, oldSettings, newSpec.Settings, newSpec.InternalId)
			if err != nil {
				rscl.logger.Errorf("Failed to perform live update on pipeline %v, err=%v\n", topic, err)
				return err
			} else {
				rscl.logger.Infof("Kept pipeline %v running since the changes to replication spec are not critical\n", topic)
				return nil
			}
		}

	} else if specActiveOld && !specActive {
		//stop replication
		rscl.logger.Infof("Stopping pipeline %v since the replication spec has been changed to inactive\n", topic)
		callback, errCb, needCb := needSpecialCallbackUpdate(topic, newSpec.InternalId, oldSettings, newSpec.Settings)
		var stopErr error
		if needCb {
			stopErr = replication_mgr.pipelineMgr.UpdatePipelineWithStoppedCb(topic, callback, errCb)
		} else {
			stopErr = replication_mgr.pipelineMgr.UpdatePipeline(topic, nil)
		}
		if needToRestreamPipelineEvenIfStopped(oldSettings, newSpec.Settings) {
			rscl.logger.Infof("Cleaning up pipeline %v because of setting change\n", topic)
			cleanErr := replication_mgr.pipelineMgr.ReInitStreams(topic)
			if cleanErr != nil {
				rscl.logger.Errorf("unable to cleanup %v due to %v - pipeline may be missing earlier data. Recommended manual delete and recreation", topic, cleanErr)
			}
			return cleanErr
		} else {
			return stopErr
		}

	} else if !specActiveOld && specActive {
		if needToRestreamPipelineEvenIfStopped(oldSettings, newSpec.Settings) {
			rscl.logger.Infof("Cleaning up pipeline %v because of setting change before starting\n", topic)
			cleanErr := replication_mgr.pipelineMgr.ReInitStreams(topic)
			if cleanErr != nil {
				rscl.logger.Errorf("unable to cleanup %v due to %v - pipeline may be missing earlier data. Recommended manual delete and recreation", topic, cleanErr)
			}
		}
		// start replication
		rscl.logger.Infof("Starting pipeline %v since the replication spec has been changed to active\n", topic)
		if oldSpec != nil {
			// We are resuming replication. Need to check if we need to raise backfill before start
			callback, errCb, needCb := needSpecialCallbackUpdate(topic, newSpec.InternalId, oldSettings, newSpec.Settings)
			if needCb {
				return replication_mgr.pipelineMgr.UpdatePipelineWithStoppedCb(topic, callback, errCb)
			} else {
				return replication_mgr.pipelineMgr.UpdatePipeline(topic, nil)
			}
		} else {
			return replication_mgr.pipelineMgr.UpdatePipeline(topic, nil)
		}
	} else {
		// this is the case where pipeline is not running and spec is not active.
		// Need to initiate the status if this is a newly created paused replication
		if oldSpec == nil {
			replication_mgr.pipelineMgr.InitiateRepStatus(newSpec.Id)
		} else {
			// is not a newly created spec
			if needToRestreamPipelineEvenIfStopped(oldSettings, newSpec.Settings) {
				cleanErr := replication_mgr.pipelineMgr.ReInitStreams(topic)
				if cleanErr != nil {
					rscl.logger.Errorf("unable to cleanup %v due to %v - pipeline may be missing earlier data. Recommended manual delete and recreation", topic, cleanErr)
				}
			}
			// Need to raise backfill even if stopped
			callback, errCb, needCb := needSpecialCallbackUpdate(topic, newSpec.InternalId, oldSettings, newSpec.Settings)
			if needCb {
				return replication_mgr.pipelineMgr.UpdatePipelineWithStoppedCb(topic, callback, errCb)
			}
		}
		return nil
	}
}

func needSpecialCallbackUpdate(topic, internalSpecId string, oldSettings, newSettings *metadata.ReplicationSettings) (callback base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback, needCallback bool) {
	if !oldSettings.GetCollectionsRoutingRules().SameAs(newSettings.GetCollectionsRoutingRules()) {
		needCallback = true
		handlerCb, handlerErrCb := replication_mgr.backfillMgr.GetExplicitMappingChangeHandler(topic, internalSpecId, oldSettings, newSettings)
		callback = func() error {
			// When explicit map rule changes, there's a chance src manifests and tgt
			// manifests did not change but the backfill raise is required because of
			// the mapping change as previously unmapped src and tgt collections
			// Each source node of the cluster will handle a subset of VBs and is
			// responsible for raising backfill tasks for subset of ownership
			// Normally when backfill is raised due to collections creation, it is
			// working in conjunction with the checkpoints and the last known manifest
			// recorded in the ckpt so that if a peer node takes over a VB
			// and resumes from a previously known manifest ID using an older ckpt,
			// another backfill can be raised when it detects that the resumed
			// manifest ID is less than the current manifest ID and diff to find the
			// differences
			// But with explicit mapping change, manifests may not change and this
			// backfill raise operation for the subset of VBs that this node owns
			// must not be lost as main pipeline will keep going. It is possible that
			// once the backfill tasks for this node's VBs are established, this
			// node can be rebalanced out of the cluster before a push occurs
			// (or if pipeline is paused)
			// For the sake of safety for this scenario, this callback will not only
			// raise backfill tasks, but also force a push to replicas so that the
			// work done here is not lost
			err := replication_mgr.utils.ExponentialBackoffExecutor("ExplicitMapChangeRetry",
				base.BucketInfoOpWaitTime, base.BucketInfoOpMaxRetry, base.BucketInfoOpRetryFactor,
				utilities.ExponentialOpFunc(handlerCb))
			if err != nil {
				return err
			}

			immediatePushRetry := func() error {
				return replication_mgr.p2pMgr.RequestImmediateCkptBkfillPush(topic)
			}
			return replication_mgr.utils.ExponentialBackoffExecutor("ExplicitMapChangePushRetry",
				base.BucketInfoOpWaitTime, base.BucketInfoOpMaxRetry, base.BucketInfoOpRetryFactor,
				immediatePushRetry)
		}
		errCb = handlerErrCb
	} else {
		callback = func() error { /* nothing */ return nil }
		errCb = func(error, bool) { /*nothing*/ }
	}
	return
}

func (rscl *ReplicationSpecChangeListener) validateReplicationSpec(specObj interface{}) (*metadata.ReplicationSpecification, error) {
	if specObj == nil {
		return nil, nil
	}

	spec, ok := specObj.(*metadata.ReplicationSpecification)
	if !ok {
		errMsg := fmt.Sprintf("Metadata, %v, is not of replication spec type\n", specObj)
		rscl.logger.Errorf(errMsg)
		return nil, errors.New(errMsg)
	}

	return spec, nil
}

// whether there are critical changes to the replication spec that require pipeline reconstruction
func needToReconstructPipeline(oldSettings, newSettings *metadata.ReplicationSettings) bool {

	// the following require reconstruction of pipeline
	repTypeChanged := !(oldSettings.RepType == newSettings.RepType)
	sourceNozzlePerNodeChanged := !(oldSettings.SourceNozzlePerNode == newSettings.SourceNozzlePerNode)
	targetNozzlePerNodeChanged := !(oldSettings.TargetNozzlePerNode == newSettings.TargetNozzlePerNode)
	compressionTypeChanged := base.GetCompressionType(oldSettings.CompressionType) != base.GetCompressionType(newSettings.CompressionType)
	filterChanged := !(oldSettings.FilterExpression == newSettings.FilterExpression)
	modesChanged := oldSettings.NeedToRestartPipelineDueToCollectionModeChanges(newSettings)
	rulesChanged := !oldSettings.GetCollectionsRoutingRules().SameAs(newSettings.GetCollectionsRoutingRules())

	// the following may qualify for live update in the future.
	// batchCount is tricky since the sizes of xmem data channels depend on it.
	// batchsize is easier to live update but it may not be intuitive to have different behaviors for batchCount and batchSize
	batchCountChanged := (oldSettings.BatchCount != newSettings.BatchCount)
	batchSizeChanged := (oldSettings.BatchSize != newSettings.BatchSize)

	return repTypeChanged || sourceNozzlePerNodeChanged || targetNozzlePerNodeChanged ||
		batchCountChanged || batchSizeChanged || compressionTypeChanged || filterChanged || modesChanged || rulesChanged
}

func needToRestreamPipeline(oldSettings *metadata.ReplicationSettings, newSettings *metadata.ReplicationSettings) bool {
	// Filter changed that require restart
	skip := false
	filterChanged := !(oldSettings.FilterExpression == newSettings.FilterExpression)

	if val, ok := newSettings.Values[metadata.FilterSkipRestreamKey]; ok {
		skip = val.(bool)
	}
	if !skip && filterChanged {
		return true
	}

	return needToRestreamPipelineEvenIfStopped(oldSettings, newSettings)
}

func needToRestreamPipelineEvenIfStopped(oldSettings, newSettings *metadata.ReplicationSettings) bool {
	if oldSettings == nil || newSettings == nil {
		return false
	}
	return oldSettings.NeedToRestreamPipelineEvenIfStoppedDueToCollectionModeChanges(newSettings)
}

func (rscl *ReplicationSpecChangeListener) liveUpdatePipeline(topic string, oldSettings *metadata.ReplicationSettings, newSettings *metadata.ReplicationSettings, newSpecInternalId string) error {
	// perform live update on pipeline if qualifying settings have been changed
	isOldReplHighPriority := rscl.resourceManager.IsReplHighPriority(topic, oldSettings.GetPriority())
	isNewReplHighPriority := rscl.resourceManager.IsReplHighPriority(topic, newSettings.GetPriority())
	oldMergeFuncMapping := oldSettings.GetMergeFunctionMapping()
	newMergeFuncMapping := newSettings.GetMergeFunctionMapping()
	if oldSettings.LogLevel != newSettings.LogLevel || oldSettings.CheckpointInterval != newSettings.CheckpointInterval ||
		oldSettings.StatsInterval != newSettings.StatsInterval ||
		oldSettings.OptimisticReplicationThreshold != newSettings.OptimisticReplicationThreshold ||
		oldSettings.BandwidthLimit != newSettings.BandwidthLimit ||
		isOldReplHighPriority != isNewReplHighPriority ||
		oldSettings.GetExpDelMode() != newSettings.GetExpDelMode() ||
		oldSettings.GetDevMainPipelineDelay() != newSettings.GetDevMainPipelineDelay() ||
		oldSettings.GetMobileCompatible() != newSettings.GetMobileCompatible() ||
		oldSettings.GetJsFunctionTimeoutMs() != newSettings.GetJsFunctionTimeoutMs() ||
		oldSettings.GetDevBackfillPipelineDelay() != newSettings.GetDevBackfillPipelineDelay() ||
		len(newMergeFuncMapping) > 0 && newMergeFuncMapping.SameAs(oldMergeFuncMapping) == false {

		newSettingsMap := newSettings.ToMap(false /*isDefaultSettings*/)

		if isOldReplHighPriority != isNewReplHighPriority {
			// if replication priority has changed, need to change isHighReplication setting accordingly
			isHighReplication := true
			if isOldReplHighPriority {
				// priority changed from high to low
				isHighReplication = false
			}
			newSettingsMap[parts.IsHighReplicationKey] = isHighReplication
		}

		rscl.logger.Infof("Updating pipeline %v with new settings=%v\n old settings=%v\n", topic, newSettingsMap.CloneAndRedact(), oldSettings.CloneAndRedact())

		go rscl.liveUpdatePipelineWithRetry(topic, newSettingsMap, newSpecInternalId)

		return nil
	}

	return nil
}

func (rscl *ReplicationSpecChangeListener) liveUpdatePipelineWithRetry(topic string, newSettingsMap metadata.ReplicationSettingsMap, specInternalId string) {
	numOfRetry := 0
	backoffTime := base.WaitTimeForLiveUpdatePipeline
	for {
		rs, err := replication_mgr.pipelineMgr.ReplicationStatus(topic)
		if err == nil {
			pipelines := rs.AllPipelines()
			for _, pipeline := range pipelines {
				if pipeline != nil {
					// check if the pipeline is associated with the correct repl spec to which the new settings belongs
					spec := rs.Spec()
					if spec != nil {
						curSpecInternalId := spec.InternalId
						if curSpecInternalId == specInternalId {
							err = pipeline.UpdateSettings(newSettingsMap)
							if err != nil {
								rscl.logger.Errorf("Live update on pipeline %v %v returned err = %v", pipeline.Type(), pipeline.Topic(), err)
							}
						} else {
							rscl.logger.Warnf("Abort live update on pipeline %v since replication spec has been recreated. oldSpecId=%v, newSpecId=%v", topic, specInternalId, curSpecInternalId)
						}
						return
					}
				}
			}
		}

		// It is possible this method invoked in a bg go-routine actually runs after a replication has been deleted
		// Check to see if spec still exists
		_, foundErr := replication_mgr.repl_spec_svc.ReplicationSpec(topic)
		if foundErr == base.ReplNotFoundErr {
			rscl.logger.Warnf("Abort live update on pipeline %v (internalID: %v) since replication spec has been deleted", topic, specInternalId)
			return
		}

		rscl.logger.Warnf("Error live updating pipeline %v. err=%v", topic, err)
		if numOfRetry < base.MaxRetryForLiveUpdatePipeline {
			numOfRetry++
			// exponential backoff
			rscl.logger.Warnf("Retrying live update on pipeline %v for %vth time after %v.", topic, numOfRetry, backoffTime)
			time.Sleep(backoffTime)
			backoffTime *= 2
		} else {
			rscl.logger.Errorf("Failed to perform live update on pipeline %v after %v retries.", topic, numOfRetry)
			return
		}
	}
}

// listener for remote clusters
type RemoteClusterChangeListener struct {
	*MetakvChangeListener
	repl_spec_svc      service_def.ReplicationSpecSvc
	remote_cluster_svc service_def.RemoteClusterSvc
}

func NewRemoteClusterChangeListener(remote_cluster_svc service_def.RemoteClusterSvc,
	repl_spec_svc service_def.ReplicationSpecSvc,
	cancel_chan chan struct{},
	children_waitgrp *sync.WaitGroup,
	logger_ctx *log.LoggerContext,
	utilsIn utilities.UtilsIface) *RemoteClusterChangeListener {
	rccl := &RemoteClusterChangeListener{
		NewMetakvChangeListener(base.RemoteClusterChangeListener,
			metadata_svc.GetCatalogPathFromCatalogKey(metadata_svc.RemoteClustersCatalogKey),
			cancel_chan,
			children_waitgrp,
			remote_cluster_svc.RemoteClusterServiceCallback,
			logger_ctx,
			"RemoteClusterChangeListener",
			utilsIn),
		repl_spec_svc,
		remote_cluster_svc,
	}
	return rccl
}

// Handler callback for remote cluster changed event
// Note - RemoteClusterService is calling this function with synchronization primatives. Rule of thumb is this fx should *not* call back into RCS to avoid deadlock.
func (rccl *RemoteClusterChangeListener) remoteClusterChangeHandlerCallback(remoteClusterRefId string, oldRemoteClusterRefObj interface{}, newRemoteClusterRefObj interface{}) error {
	// Default to nil
	var oldRemoteClusterRef *metadata.RemoteClusterReference
	var newRemoteClusterRef *metadata.RemoteClusterReference
	var err error

	// If emptyRef is passed in, it is essentially the same as nil
	if oldRemoteClusterRefObj != nil && !oldRemoteClusterRefObj.(*metadata.RemoteClusterReference).IsEmpty() {
		oldRemoteClusterRef, err = rccl.validateRemoteClusterRef(oldRemoteClusterRefObj)
		if err != nil {
			return err
		}
	}
	if newRemoteClusterRefObj != nil && !newRemoteClusterRefObj.(*metadata.RemoteClusterReference).IsEmpty() {
		newRemoteClusterRef, err = rccl.validateRemoteClusterRef(newRemoteClusterRefObj)
		if err != nil {
			return err
		}
	}

	rccl.logger.Infof("remoteClusterChangedCallback called on id = %v, oldRef=%v, newRef=%v\n", remoteClusterRefId, oldRemoteClusterRef.CloneAndRedact().String(), newRemoteClusterRef.CloneAndRedact().String())
	defer rccl.logger.Infof("Completed remoteClusterChangedCallback called on id = %v", remoteClusterRefId)

	if oldRemoteClusterRef == nil {
		// nothing to do if remote cluster has been created
		return nil
	}

	if newRemoteClusterRef == nil {
		// oldRemoteClusterRef has been deleted

		// if there are existing replications referencing the old cluster ref, there must have been a racing condition
		// between the replication creation and the cluster ref deletion. Delete the now orphaned replications to ensure consistency
		topics := replication_mgr.pipelineMgr.AllReplicationsForTargetCluster(oldRemoteClusterRef.Uuid())
		if len(topics) > 0 {
			rccl.logger.Infof("Deleting replications, %v, since the referenced remote cluster, %v, has been deleted\n", topics, oldRemoteClusterRef.Name)
			for _, topic := range topics {
				replication_mgr.pipelineMgr.DeletePipeline(topic)
			}
		} else {
			rccl.logger.Infof("Found no specs to delete for the deletion of remote cluster %v\n", oldRemoteClusterRef.Name)
		}
		return nil
	}

	if !oldRemoteClusterRef.AreUserSecurityCredentialsTheSame(newRemoteClusterRef) ||
		!oldRemoteClusterRef.AreSecuritySettingsTheSame(newRemoteClusterRef) {
		// TODO there may be less disruptive ways to handle the following updates without restarting the pipelines
		// restarting the pipelines seems to be acceptable considering the low frequency of such updates.
		specs := replication_mgr.pipelineMgr.AllReplicationSpecsForTargetCluster(oldRemoteClusterRef.Uuid())

		for _, spec := range specs {
			// if critical info in remote cluster reference, e.g., log info or certificate, is changed,
			// the existing connection pools to the corresponding target cluster all need to be reset to
			// take in the new changes. Mark these connection pools to be stale, so that they will be
			// removed and re-created once the replications are started or resumed.
			// Note that this needs to be done for paused replications as well.
			base.ConnPoolMgr().SetStaleForPoolsWithNamePrefix(spec.Id)

			if spec.Settings.Active {
				rccl.logger.Infof("Restarting pipelines %v since the referenced remote cluster %v has been changed\n", spec.Id, oldRemoteClusterRef.Name)
				replication_mgr.pipelineMgr.UpdatePipeline(spec.Id, nil)
			}
		}
	}

	// other updates to remote clusters do not require any actions

	return nil
}

func (rccl *RemoteClusterChangeListener) validateRemoteClusterRef(remoteClusterRefObj interface{}) (*metadata.RemoteClusterReference, error) {
	if remoteClusterRefObj == nil {
		return nil, nil
	}

	remoteClusterRef, ok := remoteClusterRefObj.(*metadata.RemoteClusterReference)
	if !ok {
		errMsg := fmt.Sprintf("Metadata, %v, is not of remote cluster type\n", remoteClusterRefObj)
		rccl.logger.Errorf(errMsg)
		return nil, errors.New(errMsg)
	}
	return remoteClusterRef, nil
}

//Process setting listeners

// listener for GOXDCR Process level setting changes.
type GlobalSettingChangeListener struct {
	*MetakvChangeListener
	resourceManager resource_manager.ResourceMgrIface
}

func NewGlobalSettingChangeListener(process_setting_svc service_def.GlobalSettingsSvc,
	cancel_chan chan struct{},
	children_waitgrp *sync.WaitGroup,
	logger_ctx *log.LoggerContext,
	utilsIn utilities.UtilsIface,
	resourceManager resource_manager.ResourceMgrIface) *GlobalSettingChangeListener {
	pscl := &GlobalSettingChangeListener{
		NewMetakvChangeListener(base.GlobalSettingChangeListener,
			metadata_svc.GetCatalogPathFromCatalogKey(metadata_svc.GlobalSettingCatalogKey),
			cancel_chan,
			children_waitgrp,
			process_setting_svc.GlobalSettingsServiceCallback,
			logger_ctx,
			"GlobalSettingChangeListener",
			utilsIn),
		resourceManager,
	}
	return pscl
}

func (pscl *GlobalSettingChangeListener) validateGlobalSetting(settingObj interface{}) (*metadata.GlobalSettings, error) {
	if settingObj == nil {
		return nil, nil
	}

	psettings, ok := settingObj.(*metadata.GlobalSettings)
	if !ok {
		errMsg := fmt.Sprintf("Metadata, %v, is not of GlobalSetting  type\n", settingObj)
		pscl.logger.Errorf(errMsg)
		return nil, errors.New(errMsg)
	}

	return psettings, nil
}

// Handler callback for prcoess setting changed event
// In case of globalsettings oldsetting object will be null as we dont cache the object.. so we dont have access to old value
func (pscl *GlobalSettingChangeListener) globalSettingChangeHandlerCallback(settingId string, oldSettingObj interface{}, newSettingObj interface{}) error {

	newSetting, err := pscl.validateGlobalSetting(newSettingObj)
	if err != nil {
		return err
	}
	pscl.logger.Infof("globalSettingChangeHandlerCallback called on id = %v\n", settingId)
	if newSetting != nil {
		pscl.logger.Infof("new Global settings=%v\n", newSetting.String())
	}
	if newSetting.GoMaxProcs > 0 {
		pscl.resourceManager.HandleGoMaxProcsChange(newSetting.GoMaxProcs)

		currentValue := runtime.GOMAXPROCS(0)
		if newSetting.GoMaxProcs != currentValue {
			runtime.GOMAXPROCS(newSetting.GoMaxProcs)
			pscl.logger.Infof("Successfully changed  Max Process setting from(old) %v to(New) %v\n", currentValue, newSetting.GoMaxProcs)
		}
	}

	if newSetting.GoGC == 0 {
		// This is possible only when we just upgraded from 4.1/4.5 to 4.6 and up, where GOGC does not exist in older version
		// Do not set GOGC in this case
		pscl.logger.Infof("GOGC in new global setting is 0, which is not a valid value and can only have come from upgrade. Skip the setting of GOGC runtime.")
	} else {
		// always sets gogc value since there is no way to check the current gogc value beforehand
		oldGoGCValue := debug.SetGCPercent(newSetting.GoGC)
		pscl.logger.Infof("Successfully changed  GOGC setting from(old) %v to(New) %v\n", oldGoGCValue, newSetting.GoGC)
	}

	return nil
}

// listener for GOXDCR internal setting changes.
type InternalSettingsChangeListener struct {
	*MetakvChangeListener
}

func NewInternalSettingsChangeListener(internal_setting_svc service_def.InternalSettingsSvc,
	cancel_chan chan struct{},
	children_waitgrp *sync.WaitGroup,
	logger_ctx *log.LoggerContext,
	utilsIn utilities.UtilsIface) *InternalSettingsChangeListener {
	iscl := &InternalSettingsChangeListener{
		NewMetakvChangeListener(base.InternalSettingsChangeListener,
			metadata_svc.GetCatalogPathFromCatalogKey(metadata_svc.InternalSettingsCatalogKey),
			cancel_chan,
			children_waitgrp,
			internal_setting_svc.InternalSettingsServiceCallback,
			logger_ctx,
			"InternalSettingChangeListener",
			utilsIn),
	}
	return iscl
}

func (iscl *InternalSettingsChangeListener) validateInternalSettings(settingsObj interface{}) (*metadata.InternalSettings, error) {
	if settingsObj == nil {
		return nil, nil
	}

	internal_settings, ok := settingsObj.(*metadata.InternalSettings)
	if !ok {
		errMsg := fmt.Sprintf("Metadata, %v, is not of InternalSettings  type\n", settingsObj)
		iscl.logger.Errorf(errMsg)
		return nil, errors.New(errMsg)
	}

	return internal_settings, nil
}

func (iscl *InternalSettingsChangeListener) internalSettingsChangeHandlerCallback(settingsId string, oldSettingsObj interface{}, newSettingsObj interface{}) error {

	oldSettings, err := iscl.validateInternalSettings(oldSettingsObj)
	if err != nil {
		return err
	}
	newSettings, err := iscl.validateInternalSettings(newSettingsObj)
	if err != nil {
		return err
	}
	iscl.logger.Infof("internalSettingsChangedCallback called on id = %v, oldSettings=%v, newSettings=%v\n", settingsId, oldSettings, newSettings)

	// Restart XDCR if internal settings have been changed
	if !newSettings.Equals(oldSettings) {
		iscl.logger.Infof("Checkpointing the pipelines and restarting XDCR process since internal settings have been changed\n")
		var waitGrp sync.WaitGroup
		for _, rep_status := range replication_mgr.pipelineMgr.ReplicationStatusMap() {
			pipeline := rep_status.Pipeline()
			if pipeline == nil {
				continue
			}

			ckpt_mgr := pipeline.RuntimeContext().Service(base.CHECKPOINT_MGR_SVC)
			if ckpt_mgr == nil {
				iscl.logger.Infof("CheckpointingManager has not been attached to pipeline %v", pipeline.Topic())
				continue
			}
			waitGrp.Add(1)
			go ckpt_mgr.(*pipeline_svc.CheckpointManager).CheckpointBeforeStopWithWait(&waitGrp)
		}
		waitGrp.Wait()
		exitProcess(false)
	}
	return nil
}
