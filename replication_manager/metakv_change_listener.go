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
	"github.com/couchbase/goxdcr/pipeline_manager"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/service_impl"
	"sync"
)

// Callback function, which typically is a method in metadata service, which translates metakv call back parameters into metadata objects
// The function may do something addtional that is specific to the metadata service, e.g., caching the new metadata
type MetadataServiceCallback func(path string, value []byte, rev interface{}) (metadataId string, oldMetadata interface{}, newMetadata interface{}, err error)

// Callback function for the handling of metadata changed event
type MetadataChangeHandlerCallback func(metadataId string, oldMetadata interface{}, newMetadata interface{}) error

// generic listener for metadata stored in metakv
type MetakvChangeListener struct {
	id                                string
	dirpath                           string
	cancel_chan                       chan struct{}
	number_of_retry                   int
	children_waitgrp                  *sync.WaitGroup
	metadata_service_call_back        MetadataServiceCallback
	metadata_change_handler_call_back MetadataChangeHandlerCallback
	logger                            *log.CommonLogger
}

func NewMetakvChangeListener(id, dirpath string, cancel_chan chan struct{},
	children_waitgrp *sync.WaitGroup,
	metadata_service_call_back MetadataServiceCallback,
	metadata_change_handler_call_back MetadataChangeHandlerCallback,
	logger_ctx *log.LoggerContext,
	logger_name string) *MetakvChangeListener {
	return &MetakvChangeListener{
		id:                                id,
		dirpath:                           dirpath,
		cancel_chan:                       cancel_chan,
		children_waitgrp:                  children_waitgrp,
		metadata_service_call_back:        metadata_service_call_back,
		metadata_change_handler_call_back: metadata_change_handler_call_back,
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

func (mcl *MetakvChangeListener) SetMetadataChangeHandlerCallBack(metadata_change_handler_call_back MetadataChangeHandlerCallback) {
	mcl.metadata_change_handler_call_back = metadata_change_handler_call_back
}

func (mcl *MetakvChangeListener) observeChildren() {
	defer mcl.children_waitgrp.Done()
	err := metakv.RunObserveChildren(mcl.dirpath, mcl.metakvCallback, mcl.cancel_chan)
	// call failure call back only when there are real errors
	// err may be nil when observeChildren is canceled, in which case there is no need to call failure call back
	mcl.failureCallback(err)
}

// Implement callback function for metakv
func (mcl *MetakvChangeListener) metakvCallback(path string, value []byte, rev interface{}) error {
	mcl.logger.Infof("metakvCallback called on listener %v with path = %v\n", mcl.Id(), path)

	metadataId, oldMetadata, newMetadata, err := mcl.metadata_service_call_back(path, value, rev)
	if err != nil {
		mcl.logger.Errorf("Error calling metadata service call back for listener %v. err=%v\n", mcl.Id(), err)
		return err
	}

	if mcl.metadata_change_handler_call_back != nil {
		err = mcl.metadata_change_handler_call_back(metadataId, oldMetadata, newMetadata)
		if err != nil {
			mcl.logger.Errorf("Error calling metadata change handler call back for listener %v. err=%v\n", mcl.Id(), err)
			// do not return err since we do not want RunObserveChildren to abort in this case
			return nil
		}
	}

	return nil

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
			service_impl.GetCatalogPathFromCatalogKey(service_impl.ReplicationSpecsCatalogKey),
			cancel_chan,
			children_waitgrp,
			repl_spec_svc.ReplicationSpecServiceCallback,
			nil,
			logger_ctx,
			"ReplicationSpecChangeListener"),
	}

	rscl.metadata_change_handler_call_back = rscl.replicationSpecChangeHandlerCallback
	return rscl
}

// Handler callback for replication spec changed event
// note that oldSpec is always nil since repl spec service has no way of retrieving it. it will be retrieved from pipeline_manager
func (rscl *ReplicationSpecChangeListener) replicationSpecChangeHandlerCallback(changedSpecId string, oldSpecObj interface{}, newSpecObj interface{}) error {
	rscl.logger.Infof("specChangedCallback called on id = %v\n", changedSpecId)

	topic := changedSpecId

	var newSpec *metadata.ReplicationSpecification
	if newSpecObj != nil {
		var ok bool
		newSpec, ok = newSpecObj.(*metadata.ReplicationSpecification)
		if !ok {
			errMsg := fmt.Sprintf("Metadata, %v, is not of replication spec type\n", newSpecObj)
			rscl.logger.Errorf(errMsg)
			return errors.New(errMsg)
		}
	}

	if newSpec == nil {
		// replication spec has been deleted
		pipeline_manager.RemoveReplicationStatus(topic)

		//delete all checkpoint docs in an async fashion
		replication_mgr.checkpoint_svc.DelCheckpointsDocs(topic)

		//close the connection pool for the replication
		pools := base.ConnPoolMgr().FindPoolNamesByPrefix(topic)
		for _, poolName := range pools {
			base.ConnPoolMgr().RemovePool(poolName)
		}
		return nil
	}

	specActive := newSpec.Settings.Active
	//if the replication doesn't exit, it is treated the same as it exits, but it is paused
	specActive_old := false
	var oldSettings *metadata.ReplicationSettings = nil
	if pipeline_manager.ReplicationStatus(topic) != nil {
		oldSettings = pipeline_manager.ReplicationStatus(topic).Settings()
		specActive_old = oldSettings.Active
	}

	if specActive_old && specActive {
		// if some critical settings have been changed, stop, reconstruct, and restart pipeline
		if needToReconstructPipeline(oldSettings, newSpec.Settings) {
			rscl.logger.Infof("Restarting pipeline %v since the changes to replication spec are critical\n", topic)
			return pipeline_manager.Update(topic, nil)

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

		return pipeline_manager.Update(topic, nil)

	} else if !specActive_old && specActive {
		// start replication
		rscl.logger.Infof("Starting pipeline %v since the replication spec has been changed to active\n", topic)

		return pipeline_manager.Update(topic, nil)

	} else {
		// this is the case where pipeline is not running and spec is not active.
		// nothing needs to be done
		return nil
	}
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
}

func NewRemoteClusterChangeListener(remote_cluster_svc service_def.RemoteClusterSvc,
	cancel_chan chan struct{},
	children_waitgrp *sync.WaitGroup,
	logger_ctx *log.LoggerContext) *RemoteClusterChangeListener {
	rccl := &RemoteClusterChangeListener{
		NewMetakvChangeListener(base.RemoteClusterChangeListener,
			service_impl.GetCatalogPathFromCatalogKey(service_impl.RemoteClustersCatalogKey),
			cancel_chan,
			children_waitgrp,
			remote_cluster_svc.RemoteClusterServiceCallback,
			nil,
			logger_ctx,
			"RemoteClusterChangeListener"),
	}

	rccl.metadata_change_handler_call_back = rccl.remoteClusterChangeHandlerCallback
	return rccl
}

// Handler callback for remote cluster changed event
func (rccl *RemoteClusterChangeListener) remoteClusterChangeHandlerCallback(remoteClusterRefId string, oldRemoteClusterRef interface{}, newRemoteClusterRef interface{}) error {
	rccl.logger.Infof("remoteClusterChangedCallback called on refId = %v\n", remoteClusterRefId)

	// TODO MB-9500 handle live update to remote clusters

	return nil
}
