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
	"errors"
	"fmt"
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/metadata_svc"
	"github.com/couchbase/goxdcr/pipeline_manager"
	"github.com/couchbase/goxdcr/service_def"
	"sync"
)

// generic listener for metadata stored in metakv
type MetakvChangeListener struct {
	id                         string
	dirpath                    string
	cancel_chan                chan struct{}
	number_of_retry            int
	children_waitgrp           *sync.WaitGroup
	metadata_service_call_back base.MetadataServiceCallback
	logger                     *log.CommonLogger
}

func NewMetakvChangeListener(id, dirpath string, cancel_chan chan struct{},
	children_waitgrp *sync.WaitGroup,
	metadata_service_call_back base.MetadataServiceCallback,
	logger_ctx *log.LoggerContext,
	logger_name string) *MetakvChangeListener {
	return &MetakvChangeListener{
		id:                         id,
		dirpath:                    dirpath,
		cancel_chan:                cancel_chan,
		children_waitgrp:           children_waitgrp,
		metadata_service_call_back: metadata_service_call_back,
		logger: log.NewLogger(logger_name, logger_ctx),
	}
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
func (mcl *MetakvChangeListener) metakvCallback(path string, value []byte, rev interface{}) error {
	mcl.logger.Infof("metakvCallback called on listener %v with path = %v\n", mcl.Id(), path)

	go mcl.metakvCallback_async(path, value, rev)

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
		//callback is cannceled and replication_mgr is exiting.
		//no-op
		return
	}
	if mcl.number_of_retry < service_def.MaxNumOfRetries {
		//restart listener
		mcl.number_of_retry++
		mcl.Start()
	} else {
		// exit process if max retry reached
		exitProcess(false)
	}
}

// listener for replication spec
type ReplicationSpecChangeListener struct {
	*MetakvChangeListener
}

func NewReplicationSpecChangeListener(repl_spec_svc service_def.ReplicationSpecSvc,
	cancel_chan chan struct{},
	children_waitgrp *sync.WaitGroup,
	logger_ctx *log.LoggerContext) *ReplicationSpecChangeListener {
	rscl := &ReplicationSpecChangeListener{
		NewMetakvChangeListener(base.ReplicationSpecChangeListener,
			metadata_svc.GetCatalogPathFromCatalogKey(metadata_svc.ReplicationSpecsCatalogKey),
			cancel_chan,
			children_waitgrp,
			repl_spec_svc.ReplicationSpecServiceCallback,
			logger_ctx,
			"ReplicationSpecChangeListener"),
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

	rscl.logger.Infof("specChangedCallback called on id = %v, oldSpec=%v, newSpec=%v\n", topic, oldSpec, newSpec)
	if oldSpec != nil {
		rscl.logger.Infof("old spec settings=%v\n", oldSpec.Settings)
	}
	if newSpec != nil {
		rscl.logger.Infof("new spec settings=%v\n", newSpec.Settings)
	}

	if newSpec == nil {
		go onDeleteReplication(topic, rscl.logger)
		return nil
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
			rscl.logger.Infof("Restarting pipeline %v since the changes to replication spec are critical\n", topic)
			go rscl.launchPipelineUpdate(topic)
			return nil
		} else {
			// otherwise, perform live update to pipeline
			err := rscl.liveUpdatePipeline(topic, oldSettings, newSpec.Settings)
			if err != nil {
				rscl.logger.Errorf("Failed to perform live update on pipeline %v, err=%v\n", topic, err)
				return err
			} else {
				rscl.logger.Infof("Kept pipeline %v running since the changes to replication spec are not critical\n", topic)
				return nil
			}
		}

	} else if specActive_old && !specActive {
		//stop replication
		rscl.logger.Infof("Stopping pipeline %v since the replication spec has been changed to inactive\n", topic)

		go rscl.launchPipelineUpdate(topic)
		return nil

	} else if !specActive_old && specActive {
		// start replication
		rscl.logger.Infof("Starting pipeline %v since the replication spec has been changed to active\n", topic)

		go rscl.launchPipelineUpdate(topic)
		return nil

	} else {
		// this is the case where pipeline is not running and spec is not active.
		// nothing needs to be done
		return nil
	}
}

func (rscl *ReplicationSpecChangeListener) launchPipelineUpdate(topic string) {
	err := pipeline_manager.Update(topic, nil)
	if err != nil {
		rscl.logger.Error(err.Error())
	}

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
func needToReconstructPipeline(oldSettings *metadata.ReplicationSettings, newSettings *metadata.ReplicationSettings) bool {

	// the following require reconstuction of pipeline
	repTypeChanged := !(oldSettings.RepType == newSettings.RepType)
	sourceNozzlePerNodeChanged := !(oldSettings.SourceNozzlePerNode == newSettings.SourceNozzlePerNode)
	targetNozzlePerNodeChanged := !(oldSettings.TargetNozzlePerNode == newSettings.TargetNozzlePerNode)

	// the following may qualify for live update in the future.
	// batchCount is tricky since the sizes of xmem data channels depend on it.
	// batchsize is easier to live update but it may not be intuitive to have different behaviors for batchCount and batchSize
	batchCountChanged := (oldSettings.BatchCount != newSettings.BatchCount)
	batchSizeChanged := (oldSettings.BatchSize != newSettings.BatchSize)

	return repTypeChanged || sourceNozzlePerNodeChanged || targetNozzlePerNodeChanged ||
		batchCountChanged || batchSizeChanged
}

func (rscl *ReplicationSpecChangeListener) liveUpdatePipeline(topic string, oldSettings *metadata.ReplicationSettings, newSettings *metadata.ReplicationSettings) error {
	rscl.logger.Infof("Performing live update on pipeline %v \n", topic)

	// perform live update on pipeline if qualifying settings have been changed
	if oldSettings.LogLevel != newSettings.LogLevel || oldSettings.CheckpointInterval != newSettings.CheckpointInterval ||
		oldSettings.StatsInterval != newSettings.StatsInterval ||
		oldSettings.OptimisticReplicationThreshold != newSettings.OptimisticReplicationThreshold {

		rs := pipeline_manager.ReplicationStatus(topic)
		if rs == nil {
			return fmt.Errorf("Cannot find replication status for pipeline %v", topic)
		}

		pipeline := rs.Pipeline()
		if pipeline == nil {
			return fmt.Errorf("Cannot find pipeline with topic %v", topic)
		}

		return pipeline.UpdateSettings(newSettings.ToMap())
	}

	return nil
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
	logger_ctx *log.LoggerContext) *RemoteClusterChangeListener {
	rccl := &RemoteClusterChangeListener{
		NewMetakvChangeListener(base.RemoteClusterChangeListener,
			metadata_svc.GetCatalogPathFromCatalogKey(metadata_svc.RemoteClustersCatalogKey),
			cancel_chan,
			children_waitgrp,
			remote_cluster_svc.RemoteClusterServiceCallback,
			logger_ctx,
			"RemoteClusterChangeListener"),
		repl_spec_svc,
		remote_cluster_svc,
	}
	return rccl
}

// Handler callback for remote cluster changed event
func (rccl *RemoteClusterChangeListener) remoteClusterChangeHandlerCallback(remoteClusterRefId string, oldRemoteClusterRefObj interface{}, newRemoteClusterRefObj interface{}) error {
	oldRemoteClusterRef, err := rccl.validateRemoteClusterRef(oldRemoteClusterRefObj)
	if err != nil {
		return err
	}
	newRemoteClusterRef, err := rccl.validateRemoteClusterRef(newRemoteClusterRefObj)
	if err != nil {
		return err
	}

	rccl.logger.Infof("remoteClusterChangedCallback called on id = %v, oldRef=%v, newRef=%v\n", remoteClusterRefId, oldRemoteClusterRef.String(), newRemoteClusterRef.String())

	if oldRemoteClusterRef == nil {
		// nothing to do if remote cluster has been created
		return nil
	}

	if newRemoteClusterRef == nil {
		// oldRemoteClusterRef has been deleted

		// if there are existing replications referencing the old cluster ref, there must have been a racing condition
		// between the replication creation and the cluster ref deletion. Delete the now orphaned replications to ensure consistency
		topics := pipeline_manager.AllReplicationsForTargetCluster(oldRemoteClusterRef.Uuid)
		if len(topics) > 0 {
			rccl.logger.Infof("Deleting replications, %v, since the referenced remote cluster, %v, has been deleted\n", topics, oldRemoteClusterRef.Name)
			for _, topic := range topics {
				err = onDeleteReplication(topic, rccl.logger)
				if err != nil {
					rccl.logger.Errorf("Error deleting replication %v. err=%v", topic, err)
				}
			}
		}
		return nil
	}

	if oldRemoteClusterRef.DemandEncryption != newRemoteClusterRef.DemandEncryption ||
		// TODO there may be less disruptive ways to handle the following updates without restarting the pipelines
		// restarting the pipelines seems to be acceptable considering the low frequency of such updates.
		string(oldRemoteClusterRef.Certificate) != string(newRemoteClusterRef.Certificate) ||
		oldRemoteClusterRef.UserName != newRemoteClusterRef.UserName ||
		oldRemoteClusterRef.Password != newRemoteClusterRef.Password {
		specs := pipeline_manager.AllReplicationSpecsForTargetCluster(oldRemoteClusterRef.Uuid)
		for _, spec := range specs {
			if spec.Settings.Active {
				rccl.logger.Infof("Restarting pipeline %v since the referenced remote cluster has been changed\n", spec.Id)
				pipeline_manager.Update(spec.Id, nil)
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

func onDeleteReplication(topic string, logger *log.CommonLogger) error {
	err := pipeline_manager.RemoveReplicationStatus(topic)
	if err != nil {
		logger.Errorf("Error removing replication status for replication %v", topic)
		return err
	}

	//delete all checkpoint docs in an async fashion
	err = replication_mgr.checkpoint_svc.DelCheckpointsDocs(topic)
	if err != nil {
		logger.Errorf("Error deleting checkpoint docs for replication %v", topic)
	}

	//close the connection pool for the replication
	pools := base.ConnPoolMgr().FindPoolNamesByPrefix(topic)
	for _, poolName := range pools {
		base.ConnPoolMgr().RemovePool(poolName)
	}
	return nil

}
