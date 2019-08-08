// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package replication_manager

import (
	"encoding/binary"
	"errors"
	"fmt"
	mc "github.com/couchbase/gomemcached"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/metadata_svc"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/resource_manager"
	"github.com/couchbase/goxdcr/service_def"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

var SetTimeSyncRetryInterval = 10 * time.Second
var BucketSettingsChanSize = 100

// listener for replication spec
type ReplicationSpecChangeListener struct {
	*base.MetakvChangeListener
	resourceManager resource_manager.ResourceMgrIface
}

func NewReplicationSpecChangeListener(repl_spec_svc service_def.ReplicationSpecSvc,
	cancel_chan chan struct{},
	children_waitgrp *sync.WaitGroup,
	logger_ctx *log.LoggerContext,
	resourceManager resource_manager.ResourceMgrIface) *ReplicationSpecChangeListener {
	rscl := &ReplicationSpecChangeListener{
		base.NewMetakvChangeListener(base.ReplicationSpecChangeListener,
			metadata_svc.GetCatalogPathFromCatalogKey(metadata_svc.ReplicationSpecsCatalogKey),
			cancel_chan,
			children_waitgrp,
			repl_spec_svc.ReplicationSpecServiceCallback,
			logger_ctx,
			"ReplicationSpecChangeListener",
			isReplicationManagerRunning,
			ExitProcess),
		resourceManager,
	}
	return rscl
}

// Handler callback for replication spec changed event
func (rscl *ReplicationSpecChangeListener) replicationSpecChangeHandlerCallback(changedSpecId string, oldSpecObj interface{}, newSpecObj interface{}) error {
	topic := changedSpecId

	oldSpec, err := rscl.validateReplicationSpec(oldSpecObj)
	if err != nil {
		return err
	}
	newSpec, err := rscl.validateReplicationSpec(newSpecObj)
	if err != nil {
		return err
	}

	rscl.Logger().Infof("specChangedCallback called on id = %v, oldSpec=%v, newSpec=%v\n", topic, oldSpec.CloneAndRedact(), newSpec.CloneAndRedact())
	if oldSpec != nil {
		rscl.Logger().Infof("old spec settings=%v\n", oldSpec.Settings.CloneAndRedact())
	}
	if newSpec != nil {
		rscl.Logger().Infof("new spec settings=%v\n", newSpec.Settings.CloneAndRedact())
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
	specActive_old := false
	var oldSettings *metadata.ReplicationSettings = nil
	if oldSpec != nil {
		oldSettings = oldSpec.Settings
		specActive_old = oldSettings.Active
	}

	if specActive_old && specActive {
		// if some critical settings have been changed, stop, reconstruct, and restart pipeline
		if needToReconstructPipeline(oldSettings, newSpec.Settings) {
			if needToRestreamPipeline(oldSettings, newSpec.Settings) {
				err := replication_mgr.pipelineMgr.ReInitStreams(topic)
				if err != nil {
					rscl.Logger().Errorf("Unable to queue re-initialize streams job for pipeline %v to restart: %v", topic, err)
				}
				return err
			}
			rscl.Logger().Infof("Restarting pipeline %v since the changes to replication spec are critical\n", topic)
			return replication_mgr.pipelineMgr.UpdatePipeline(topic, nil)
		} else {
			// otherwise, perform live update to pipeline
			err := rscl.liveUpdatePipeline(topic, oldSettings, newSpec.Settings, newSpec.InternalId)
			if err != nil {
				rscl.Logger().Errorf("Failed to perform live update on pipeline %v, err=%v\n", topic, err)
				return err
			} else {
				rscl.Logger().Infof("Kept pipeline %v running since the changes to replication spec are not critical\n", topic)
				return nil
			}
		}

	} else if specActive_old && !specActive {
		//stop replication
		rscl.Logger().Infof("Stopping pipeline %v since the replication spec has been changed to inactive\n", topic)
		return replication_mgr.pipelineMgr.UpdatePipeline(topic, nil)

	} else if !specActive_old && specActive {
		// start replication
		rscl.Logger().Infof("Starting pipeline %v since the replication spec has been changed to active\n", topic)
		return replication_mgr.pipelineMgr.UpdatePipeline(topic, nil)

	} else {
		// this is the case where pipeline is not running and spec is not active.
		// nothing needs to be done
		return nil
	}
}

func (rscl *ReplicationSpecChangeListener) validateReplicationSpec(specObj interface{}) (*metadata.ReplicationSpecification, error) {
	if specObj == nil {
		return nil, nil
	}

	spec, ok := specObj.(*metadata.ReplicationSpecification)
	if !ok {
		errMsg := fmt.Sprintf("Metadata, %v, is not of replication spec type\n", specObj)
		rscl.Logger().Errorf(errMsg)
		return nil, errors.New(errMsg)
	}

	return spec, nil
}

// whether there are critical changes to the replication spec that require pipeline reconstruction
func needToReconstructPipeline(oldSettings *metadata.ReplicationSettings, newSettings *metadata.ReplicationSettings) bool {

	// the following require reconstruction of pipeline
	repTypeChanged := !(oldSettings.RepType == newSettings.RepType)
	sourceNozzlePerNodeChanged := !(oldSettings.SourceNozzlePerNode == newSettings.SourceNozzlePerNode)
	targetNozzlePerNodeChanged := !(oldSettings.TargetNozzlePerNode == newSettings.TargetNozzlePerNode)
	compressionTypeChanged := base.GetCompressionType(oldSettings.CompressionType) != base.GetCompressionType(newSettings.CompressionType)
	filterChanged := !(oldSettings.FilterExpression == newSettings.FilterExpression)

	// the following may qualify for live update in the future.
	// batchCount is tricky since the sizes of xmem data channels depend on it.
	// batchsize is easier to live update but it may not be intuitive to have different behaviors for batchCount and batchSize
	batchCountChanged := (oldSettings.BatchCount != newSettings.BatchCount)
	batchSizeChanged := (oldSettings.BatchSize != newSettings.BatchSize)

	return repTypeChanged || sourceNozzlePerNodeChanged || targetNozzlePerNodeChanged ||
		batchCountChanged || batchSizeChanged || compressionTypeChanged || filterChanged
}

func needToRestreamPipeline(oldSettings *metadata.ReplicationSettings, newSettings *metadata.ReplicationSettings) bool {
	skip := false
	filterChanged := !(oldSettings.FilterExpression == newSettings.FilterExpression)

	if val, ok := newSettings.Values[metadata.FilterSkipRestreamKey]; ok {
		skip = val.(bool)
	}
	return !skip && filterChanged
}

func (rscl *ReplicationSpecChangeListener) liveUpdatePipeline(topic string, oldSettings *metadata.ReplicationSettings, newSettings *metadata.ReplicationSettings, newSpecInternalId string) error {
	// perform live update on pipeline if qualifying settings have been changed
	isOldReplHighPriority := rscl.resourceManager.IsReplHighPriority(topic, oldSettings.GetPriority())
	isNewReplHighPriority := rscl.resourceManager.IsReplHighPriority(topic, newSettings.GetPriority())
	if oldSettings.LogLevel != newSettings.LogLevel || oldSettings.CheckpointInterval != newSettings.CheckpointInterval ||
		oldSettings.StatsInterval != newSettings.StatsInterval ||
		oldSettings.OptimisticReplicationThreshold != newSettings.OptimisticReplicationThreshold ||
		oldSettings.BandwidthLimit != newSettings.BandwidthLimit ||
		isOldReplHighPriority != isNewReplHighPriority ||
		oldSettings.GetExpDelMode() != newSettings.GetExpDelMode() {

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

		rscl.Logger().Infof("Updating pipeline %v with new settings=%v\n old settings=%v\n", topic, newSettingsMap.CloneAndRedact(), oldSettings.CloneAndRedact())

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
			pipeline := rs.Pipeline()
			if pipeline != nil {
				// check if the pipeline is associated with the correct repl spec to which the new settings belongs
				curSpecInternalId := rs.Spec().InternalId
				if curSpecInternalId == specInternalId {
					err = pipeline.UpdateSettings(newSettingsMap)
					if err != nil {
						rscl.Logger().Errorf("Live update on pipeline %v returned err = %v", topic, err)
					}
				} else {
					rscl.Logger().Warnf("Abort live update on pipeline %v since replication spec has been recreated. oldSpecId=%v, newSpecId=%v", topic, specInternalId, curSpecInternalId)
				}
				return
			} else {
				err = fmt.Errorf("Cannot find pipeline with topic %v", topic)
			}
		}

		rscl.Logger().Warnf("Error live updating pipeline %v. err=%v", topic, err)
		if numOfRetry < base.MaxRetryForLiveUpdatePipeline {
			numOfRetry++
			// exponential backoff
			rscl.Logger().Warnf("Retrying live update on pipeline %v for %vth time after %v.", topic, numOfRetry, backoffTime)
			time.Sleep(backoffTime)
			backoffTime *= 2
		} else {
			rscl.Logger().Errorf("Failed to perform live update on pipeline %v after %v retries.", topic, numOfRetry)
			return
		}
	}
}

// listener for remote clusters
type RemoteClusterChangeListener struct {
	*base.MetakvChangeListener
	repl_spec_svc      service_def.ReplicationSpecSvc
	remote_cluster_svc service_def.RemoteClusterSvc
}

func NewRemoteClusterChangeListener(remote_cluster_svc service_def.RemoteClusterSvc,
	repl_spec_svc service_def.ReplicationSpecSvc,
	cancel_chan chan struct{},
	children_waitgrp *sync.WaitGroup,
	logger_ctx *log.LoggerContext) *RemoteClusterChangeListener {
	rccl := &RemoteClusterChangeListener{
		base.NewMetakvChangeListener(base.RemoteClusterChangeListener,
			metadata_svc.GetCatalogPathFromCatalogKey(metadata_svc.RemoteClustersCatalogKey),
			cancel_chan,
			children_waitgrp,
			remote_cluster_svc.RemoteClusterServiceCallback,
			logger_ctx,
			"RemoteClusterChangeListener",
			isReplicationManagerRunning,
			ExitProcess),
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

	rccl.Logger().Infof("remoteClusterChangedCallback called on id = %v, oldRef=%v, newRef=%v\n", remoteClusterRefId, oldRemoteClusterRef.CloneAndRedact().String(), newRemoteClusterRef.CloneAndRedact().String())
	defer rccl.Logger().Infof("Completed remoteClusterChangedCallback called on id = %v", remoteClusterRefId)

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
			rccl.Logger().Infof("Deleting replications, %v, since the referenced remote cluster, %v, has been deleted\n", topics, oldRemoteClusterRef.Name)
			for _, topic := range topics {
				replication_mgr.pipelineMgr.DeletePipeline(topic)
			}
		} else {
			rccl.Logger().Infof("Found no specs to delete for the deletion of remote cluster %v\n", oldRemoteClusterRef.Name)
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
				rccl.Logger().Infof("Restarting pipelines %v since the referenced remote cluster %v has been changed\n", spec.Id, oldRemoteClusterRef.Name)
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
		rccl.Logger().Errorf(errMsg)
		return nil, errors.New(errMsg)
	}
	return remoteClusterRef, nil
}

//Process setting listeners

// listener for GOXDCR Process level setting changes.
type GlobalSettingChangeListener struct {
	*base.MetakvChangeListener
	resourceManager resource_manager.ResourceMgrIface
}

func NewGlobalSettingChangeListener(process_setting_svc service_def.GlobalSettingsSvc,
	cancel_chan chan struct{},
	children_waitgrp *sync.WaitGroup,
	logger_ctx *log.LoggerContext,
	resourceManager resource_manager.ResourceMgrIface) *GlobalSettingChangeListener {
	pscl := &GlobalSettingChangeListener{
		base.NewMetakvChangeListener(base.GlobalSettingChangeListener,
			metadata_svc.GetCatalogPathFromCatalogKey(metadata_svc.GlobalSettingCatalogKey),
			cancel_chan,
			children_waitgrp,
			process_setting_svc.GlobalSettingsServiceCallback,
			logger_ctx,
			"GlobalSettingChangeListener",
			isReplicationManagerRunning,
			ExitProcess),
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
		pscl.Logger().Errorf(errMsg)
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
	pscl.Logger().Infof("globalSettingChangeHandlerCallback called on id = %v\n", settingId)
	if newSetting != nil {
		pscl.Logger().Infof("new Global settings=%v\n", newSetting.String())
	}
	if newSetting.GoMaxProcs > 0 {
		pscl.resourceManager.HandleGoMaxProcsChange(newSetting.GoMaxProcs)

		currentValue := runtime.GOMAXPROCS(0)
		if newSetting.GoMaxProcs != currentValue {
			runtime.GOMAXPROCS(newSetting.GoMaxProcs)
			pscl.Logger().Infof("Successfully changed  Max Process setting from(old) %v to(New) %v\n", currentValue, newSetting.GoMaxProcs)
		}
	}

	if newSetting.GoGC == 0 {
		// This is possible only when we just upgraded from 4.1/4.5 to 4.6 and up, where GOGC does not exist in older version
		// Do not set GOGC in this case
		pscl.Logger().Infof("GOGC in new global setting is 0, which is not a valid value and can only have come from upgrade. Skip the setting of GOGC runtime.")
	} else {
		// always sets gogc value since there is no way to check the current gogc value beforehand
		oldGoGCValue := debug.SetGCPercent(newSetting.GoGC)
		pscl.Logger().Infof("Successfully changed  GOGC setting from(old) %v to(New) %v\n", oldGoGCValue, newSetting.GoGC)
	}

	return nil
}

// listener for GOXDCR internal setting changes.
type InternalSettingsChangeListener struct {
	*base.MetakvChangeListener
}

func NewInternalSettingsChangeListener(internal_setting_svc service_def.InternalSettingsSvc,
	cancel_chan chan struct{},
	children_waitgrp *sync.WaitGroup,
	logger_ctx *log.LoggerContext) *InternalSettingsChangeListener {
	iscl := &InternalSettingsChangeListener{
		base.NewMetakvChangeListener(base.InternalSettingsChangeListener,
			metadata_svc.GetCatalogPathFromCatalogKey(metadata_svc.InternalSettingsCatalogKey),
			cancel_chan,
			children_waitgrp,
			internal_setting_svc.InternalSettingsServiceCallback,
			logger_ctx,
			"InternalSettingChangeListener",
			isReplicationManagerRunning,
			ExitProcess),
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
		iscl.Logger().Errorf(errMsg)
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
	iscl.Logger().Infof("internalSettingsChangedCallback called on id = %v, oldSettings=%v, newSettings=%v\n", settingsId, oldSettings, newSettings)

	// Restart XDCR if internal settings have been changed
	if !newSettings.Equals(oldSettings) {
		iscl.Logger().Infof("Restarting XDCR process since internal settings have been changed\n")
		ExitProcess(false)
	}
	return nil
}

//Bucket settings listeners - not being used

//type BucketSettingsChangeListener struct {
//	*base.MetakvChangeListener
//	xdcr_topology_svc service_def.XDCRCompTopologySvc
//	cluster_info_svc  service_def.ClusterInfoSvc
//	changes_chan      chan *BucketSettingsChange
//}
//
//type BucketSettingsChange struct {
//	bucketUUID     string
//	bucketSettings *metadata.BucketSettings
//}
//
//func NewBucketSettingsChangeListener(bucket_settings_svc service_def.BucketSettingsSvc,
//	xdcr_topology_svc service_def.XDCRCompTopologySvc,
//	cluster_info_svc service_def.ClusterInfoSvc,
//	cancel_chan chan struct{},
//	children_waitgrp *sync.WaitGroup,
//	logger_ctx *log.LoggerContext) *BucketSettingsChangeListener {
//	return &BucketSettingsChangeListener{
//		MetakvChangeListener: base.NewMetakvChangeListener(base.BucketSettingsChangeListener,
//			metadata_svc.GetCatalogPathFromCatalogKey(metadata_svc.BucketSettingsCatalogKey),
//			cancel_chan,
//			children_waitgrp,
//			bucket_settings_svc.BucketSettingsServiceCallback,
//			logger_ctx,
//			"BucketSettingsChangeListener",
//			isReplicationManagerRunning,
//			ExitProcess),
//		xdcr_topology_svc: xdcr_topology_svc,
//		cluster_info_svc:  cluster_info_svc,
//		changes_chan:      make(chan *BucketSettingsChange, BucketSettingsChanSize),
//	}
//}
//
//func (bscl *BucketSettingsChangeListener) Start() error {
//
//	bscl.MetakvChangeListener.Start()
//	bscl.MetakvChangeListener.children_waitgrp.Add(1)
//	go bscl.processBucketSettingsChanges()
//
//	return nil
//}
//
//// Handler callback for bucket settings changed event
//// In case of bucket settings oldsetting object will be null as we dont cache the object.. so we dont have access to old value
//func (bscl *BucketSettingsChangeListener) bucketSettingsChangeHandlerCallback(bucketUUID string, oldSettingsObj interface{}, newSettingsObj interface{}) error {
//	newSettings, err := bscl.validateBucketSettings(newSettingsObj)
//	if err != nil {
//		return err
//	}
//
//	if newSettings == nil {
//		bscl.Logger().Infof("bucketSettingsChangeHandlerCallback. bucket settings for bucket with uuid=%v got deleted\n")
//		return nil
//	}
//
//	bscl.Logger().Infof("bucketSettingsChangeHandlerCallback called on bucket = %v. bucketSettings=%v\n", newSettings.BucketName, newSettings)
//
//	settingsChange := &BucketSettingsChange{bucketUUID, newSettings}
//	// this should not block since we have a big queue. Just in case that it blocks, do not want to hold up the callback
//	go bscl.addSettingsChangeToQueue(settingsChange)
//	return nil
//}
//
//func (bscl *BucketSettingsChangeListener) addSettingsChangeToQueue(settingsChange *BucketSettingsChange) {
//	bscl.changes_chan <- settingsChange
//}
//
//func (bscl *BucketSettingsChangeListener) processBucketSettingsChanges() {
//	defer bscl.MetakvChangeListener.children_waitgrp.Done()
//	fin_ch := bscl.MetakvChangeListener.cancel_chan
//	for {
//		select {
//		case <-fin_ch:
//			return
//		case settingsChange := <-bscl.changes_chan:
//			// this serializes the processing of bucket settings change
//			// processing of the next bucket settings won't begin until the processing of the previous bucket settings completes
//			bscl.processBucketSettingsChange(settingsChange)
//		}
//	}
//}
//
//func (bscl *BucketSettingsChangeListener) processBucketSettingsChange(settingsChange *BucketSettingsChange) error {
//	bucketUUID := settingsChange.bucketUUID
//	bucketName := settingsChange.bucketSettings.BucketName
//	bscl.Logger().Infof("Processing bucket settings change on bucket = %v. bucketSettings=%v\n", bucketName, settingsChange.bucketSettings)
//
//	// validate that bucketName and bucketUUID are still valid
//	connStr, err := bscl.xdcr_topology_svc.MyConnectionStr()
//	if err != nil {
//		bscl.Logger().Errorf("bucketSettingsChangeHandlerCallback on bucket = %v failed. err=%v\n", bucketName, err)
//		return err
//	}
//
//	curBucketUUID, err := bscl.utils.LocalBucketUUID(connStr, bucketName, bscl.logger)
//	if err != nil {
//		bscl.Logger().Infof("bucketSettingsChangeHandlerCallback on bucket = %v has been skipped since the bucket cannot be retrieved. The bucket may have been deleted. err=%v\n", bucketName, err)
//		return err
//	}
//
//	if curBucketUUID != bucketUUID {
//		bscl.Logger().Infof("bucketSettingsChangeHandlerCallback on bucket = %v has been skipped since bucket uuid does not match. The bucket may have been deleted and then recreated. old bucket uuid=%v, current bucket uuid=%v\n", bucketName, bucketUUID, curBucketUUID)
//		return nil
//	}
//
//	bscl.setTimeSyncOnBucketWithRetry(bucketName, settingsChange.bucketSettings.LWWEnabled)
//	return nil
//}
//
//func (bscl *BucketSettingsChangeListener) validateBucketSettings(settingsObj interface{}) (*metadata.BucketSettings, error) {
//	if settingsObj == nil {
//		return nil, nil
//	}
//
//	bucketSettings, ok := settingsObj.(*metadata.BucketSettings)
//	if !ok {
//		errMsg := fmt.Sprintf("Metadata, %v, is not of BucketSettings type\n", settingsObj)
//		bscl.Logger().Errorf(errMsg)
//		return nil, errors.New(errMsg)
//	}
//
//	return bucketSettings, nil
//}
//
//// keep retrying till set time sync succeeds
//// there are two major error scenarios:
//// 1. source kv is temporarily inaccessible
//// 2. source kv is undergoing rebalance
//// either way things may work next time we retry
//func (bscl *BucketSettingsChangeListener) setTimeSyncOnBucketWithRetry(bucketName string, enable bool) {
//	for {
//		err := bscl.setTimeSyncOnBucket(bucketName, enable)
//		if err == nil {
//			return
//		}
//		bscl.Logger().Errorf("Failed to set time sync to %v on bucket %v due to err=%v. Retrying after %v\n", enable, bucketName, err, SetTimeSyncRetryInterval)
//		time.Sleep(SetTimeSyncRetryInterval)
//	}
//}
//
//func (bscl *BucketSettingsChangeListener) setTimeSyncOnBucket(bucketName string, enable bool) error {
//	bscl.Logger().Infof("Setting time sync to %v on bucket %v\n", enable, bucketName)
//
//	var res *mc.MCResponse
//
//	hostAddr, err := bscl.xdcr_topology_svc.MyMemcachedAddr()
//	if err != nil {
//		return err
//	}
//
//	vbMap, _, err := pipeline_utils.GetSourceVBMap(bscl.cluster_info_svc, bscl.xdcr_topology_svc, bucketName, bscl.logger)
//	if err != nil {
//		return err
//	}
//
//	// local connection to memcached uses plain authentication
//	client, err := bscl.utils.GetMemcachedConnection(hostAddr, bucketName, "Goxdcr bucketSetting listener", 0 /*keepAlivePeriod*/, bscl.logger)
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//
//	seqnoMap := make(map[uint16][]uint64)
//
//	// send control message to en-engine to set drift counter and time sync settings for each vb managed by the current node
//	// TODO do we need to parallelize this? - parallization requires multiple clients and may not necessarily help. wait for performance results
//	for _, vbList := range vbMap {
//		for _, vb := range vbList {
//			time_sync_request := composeTimeSyncRequest(vb, enable)
//			conn := client.Hijack()
//			_, err = conn.Write(time_sync_request.Bytes())
//			if err != nil {
//				return fmt.Errorf("Received error writing time sync request for vb %v on bucket %v. err=%v\n", vb, bucketName, err)
//			}
//
//			if res, err = client.Receive(); err != nil {
//				return fmt.Errorf("Error receiving response for time sync request for vb %v on bucket %v. err=%v\n", vb, bucketName, err)
//			} else if res.Opcode != base.SET_TIME_SYNC {
//				return fmt.Errorf("Received unexpected #opcode %v for set time sync request for vb %v on bucket %v", res.Opcode, vb, bucketName)
//			} else if res.Status != mc.SUCCESS {
//				return fmt.Errorf("Received not ok response for set time sync request for vb %v on bucket %v. response status=%v", vb, bucketName, res.Status)
//			} else {
//				if len(res.Body) != 16 {
//					return fmt.Errorf("Response for time sync request for vb %v on bucket %v does not contain valid seqno. length of response body=%v\n", vb, bucketName, len(res.Body))
//				} else {
//					vb_arr := make([]uint64, 2)
//					// first 8 bytes is vb uuid
//					vb_arr[0] = binary.BigEndian.Uint64(res.Body[0:8])
//					// second 8 bytes is seqno
//					vb_arr[1] = binary.BigEndian.Uint64(res.Body[8:16])
//					seqnoMap[vb] = vb_arr
//				}
//			}
//		}
//	}
//
//	//TODO decide where to put the seqno map log
//	bscl.Logger().Infof("Starting seqnos after setting time sync to %v on bucket %v is %v\n", enable, bucketName, seqnoMap)
//	return nil
//}

func composeTimeSyncRequest(vb uint16, enable bool) *mc.MCRequest {
	req := &mc.MCRequest{VBucket: vb,
		Opcode: base.SET_TIME_SYNC}
	req.Extras = make([]byte, 9)
	// for the time being, set initial_drift to 0 since it is not being used
	binary.BigEndian.PutUint64(req.Extras[0:8], 0)
	// this is the critical operation - set time sync flag
	if enable {
		req.Extras[8] = 1
	} else {
		req.Extras[8] = 0
	}

	return req
}
