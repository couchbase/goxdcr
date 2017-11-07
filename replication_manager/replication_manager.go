// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// replication manager.

package replication_manager

import (
	"bufio"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/factory"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/pipeline_manager"
	"github.com/couchbase/goxdcr/pipeline_svc"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/simple_utils"
	"github.com/couchbase/goxdcr/supervisor"
	utilities "github.com/couchbase/goxdcr/utils"
	"io"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var logger_rm *log.CommonLogger = log.NewLogger("ReplMgr", log.DefaultLoggerContext)
var StatsUpdateIntervalForPausedReplications = 60 * time.Second
var StatusCheckInterval = 15 * time.Second
var MemStatsLogInterval = 2 * time.Minute

var GoXDCROptions struct {
	SourceKVAdminPort    uint64 //source kv admin port
	XdcrRestPort         uint64 // port number of XDCR rest server
	SslProxyUpstreamPort uint64
	IsEnterprise         bool // whether couchbase is of enterprise edition
	IsConvert            bool // whether xdcr is running in conversion/upgrade mode

	// logging related parameters
	LogFileDir          string
	MaxLogFileSize      uint64
	MaxNumberOfLogFiles uint64
}

/************************************
/* struct ReplicationManager
*************************************/
type ReplicationManagerIf interface {
	initMetadataChangeMonitor()
	initPausedReplications()
	checkReplicationStatus(fin_chan chan bool)
	init(repl_spec_svc service_def.ReplicationSpecSvc,
		remote_cluster_svc service_def.RemoteClusterSvc,
		cluster_info_svc service_def.ClusterInfoSvc,
		xdcr_topology_svc service_def.XDCRCompTopologySvc,
		replication_settings_svc service_def.ReplicationSettingsSvc,
		checkpoint_svc service_def.CheckpointsService,
		capi_svc service_def.CAPIService,
		audit_svc service_def.AuditSvc,
		uilog_svc service_def.UILogSvc,
		global_setting_svc service_def.GlobalSettingsSvc,
		bucket_settings_svc service_def.BucketSettingsSvc,
		internal_settings_svc service_def.InternalSettingsSvc)
	createAndPersistReplicationSpec(justValidate bool, sourceBucket, targetCluster, targetBucket string, settings map[string]interface{}) (*metadata.ReplicationSpecification, map[string]error, error)
	OnError(s common.Supervisor, errMap map[string]error)
	upgradeRemoteClusterRefs()
	getPipelineFromPipelineSupevisor(s common.Supervisor) (common.Pipeline, error)
}

/************************************
/* struct ReplicationManager
*************************************/
type replicationManager struct {
	// supervises the livesness of adminport and pipelineMasterSupervisor
	supervisor.GenericSupervisor
	// supervises the liveness of all pipeline supervisors
	pipelineMasterSupervisor *supervisor.GenericSupervisor
	// Single instance of pipeline_mgr here instead of using a global
	pipelineMgr *pipeline_manager.PipelineManager

	//replication specification service handle
	repl_spec_svc service_def.ReplicationSpecSvc
	//remote cluster service handle
	remote_cluster_svc service_def.RemoteClusterSvc
	//cluster info service handle
	cluster_info_svc service_def.ClusterInfoSvc
	//xdcr topology service handle
	xdcr_topology_svc service_def.XDCRCompTopologySvc
	//replication settings service handle
	replication_settings_svc service_def.ReplicationSettingsSvc
	//checkpoint service handle
	checkpoint_svc service_def.CheckpointsService
	//capi service handle
	capi_svc service_def.CAPIService
	//audit service handle
	audit_svc service_def.AuditSvc
	//global setting service
	global_setting_svc service_def.GlobalSettingsSvc
	//bucket settings service
	bucket_settings_svc service_def.BucketSettingsSvc
	//internal settings service
	internal_settings_svc service_def.InternalSettingsSvc
	// Mockable utils object
	utils utilities.UtilsIface

	once sync.Once

	//finish channel for adminport
	adminport_finch chan bool

	metadata_change_callback_cancel_ch chan struct{}

	running      bool
	running_lock sync.RWMutex

	children_waitgrp *sync.WaitGroup

	status_logger_finch chan bool

	mem_stats_logger_finch chan bool

	refresh_remote_cluster_ref_finch chan bool
}

//singleton
var replication_mgr replicationManager

func StartReplicationManager(sourceKVHost string, xdcrRestPort uint16,
	repl_spec_svc service_def.ReplicationSpecSvc,
	remote_cluster_svc service_def.RemoteClusterSvc,
	cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	replication_settings_svc service_def.ReplicationSettingsSvc,
	checkpoint_svc service_def.CheckpointsService,
	capi_svc service_def.CAPIService,
	audit_svc service_def.AuditSvc,
	uilog_svc service_def.UILogSvc,
	global_setting_svc service_def.GlobalSettingsSvc,
	bucket_settings_svc service_def.BucketSettingsSvc,
	internal_settings_svc service_def.InternalSettingsSvc,
	utilitiesIn utilities.UtilsIface) {

	replication_mgr.once.Do(func() {
		// ns_server shutdown protocol: poll stdin and exit upon reciept of EOF
		go pollStdin()

		// initialize constants
		initConstants(xdcr_topology_svc, internal_settings_svc)

		// Take in utilities
		replication_mgr.utils = utilitiesIn

		// initializes replication manager
		replication_mgr.init(repl_spec_svc, remote_cluster_svc, cluster_info_svc, xdcr_topology_svc, replication_settings_svc, checkpoint_svc, capi_svc, audit_svc, uilog_svc, global_setting_svc, bucket_settings_svc, internal_settings_svc)

		// start pipeline master supervisor
		// TODO should we make heart beat settings configurable?
		replication_mgr.pipelineMasterSupervisor.Start(nil)
		logger_rm.Info("Master supervisor has started")

		// start replication manager supervisor
		// TODO should we make heart beat settings configurable?
		replication_mgr.GenericSupervisor.Start(nil)

		// set ReplicationStatus for paused replications
		replication_mgr.initPausedReplications()
		logger_rm.Info("initPausedReplications succeeded")

		replication_mgr.running = true
		replication_mgr.running_lock = sync.RWMutex{}

		replication_mgr.status_logger_finch = make(chan bool, 1)
		go replication_mgr.checkReplicationStatus(replication_mgr.status_logger_finch)

		// periodically log mem stats to facilitate debugging of memory issues
		replication_mgr.mem_stats_logger_finch = make(chan bool, 1)
		go logMemStats(replication_mgr.mem_stats_logger_finch)

		// periodically refresh remote cluster reference
		replication_mgr.refresh_remote_cluster_ref_finch = make(chan bool, 1)
		go refreshRemoteClusterRef(replication_mgr.refresh_remote_cluster_ref_finch)

		// upgrade remote cluster refs before initializing metadata change monitor
		// and starting adminport to reduce interference
		replication_mgr.upgradeRemoteClusterRefs()

		replication_mgr.initMetadataChangeMonitor()

		// start adminport
		adminport := NewAdminport(sourceKVHost, xdcrRestPort, replication_mgr.adminport_finch, replication_mgr.utils)
		go adminport.Start()
		logger_rm.Info("Admin port has been launched")
		// add adminport as children of replication manager supervisor
		replication_mgr.GenericSupervisor.AddChild(adminport)

		logger_rm.Info("ReplicationManager is running")

	})

}

func initConstants(xdcr_topology_svc service_def.XDCRCompTopologySvc, internal_settings_svc service_def.InternalSettingsSvc) {
	// get cluster version
	version, err := xdcr_topology_svc.MyClusterVersion()
	if err != nil {
		logger_rm.Errorf("Failed to get local cluster version. err=%v", err)
		// in the unlikely event of error, an empty version will be used
	}

	internal_settings := internal_settings_svc.GetInternalSettings()

	logger_rm.Infof("XDCR internal settings: %v\n", internal_settings.ToMap())

	base.InitConstants(time.Duration(internal_settings.Values[metadata.TopologyChangeCheckIntervalKey].(int))*time.Second,
		internal_settings.Values[metadata.MaxTopologyChangeCountBeforeRestartKey].(int),
		internal_settings.Values[metadata.MaxTopologyStableCountBeforeRestartKey].(int),
		internal_settings.Values[metadata.MaxWorkersForCheckpointingKey].(int),
		time.Duration(internal_settings.Values[metadata.TimeoutCheckpointBeforeStopKey].(int))*time.Second,
		internal_settings.Values[metadata.CapiDataChanSizeMultiplierKey].(int),
		time.Duration(internal_settings.Values[metadata.RefreshRemoteClusterRefIntervalKey].(int))*time.Second,
		version,
		internal_settings.Values[metadata.CapiMaxRetryBatchUpdateDocsKey].(int),
		time.Duration(internal_settings.Values[metadata.CapiBatchTimeoutKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.CapiWriteTimeoutKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.CapiReadTimeoutKey].(int))*time.Second,
		internal_settings.Values[metadata.MaxCheckpointRecordsToKeepKey].(int),
		internal_settings.Values[metadata.MaxCheckpointRecordsToReadKey].(int))
}

func (rm *replicationManager) initMetadataChangeMonitor() {
	mcm := NewMetadataChangeMonitor()

	// the sequence of the listener registration matters
	// for example, replicationSpecChangeListener will get all active replications started
	// this requires replication settings and remote cluster reference to be initialized
	// that is why it needs to be registered and started after the other listeners

	globalSettingChangeListener := NewGlobalSettingChangeListener(
		rm.global_setting_svc,
		rm.metadata_change_callback_cancel_ch,
		rm.children_waitgrp,
		log.DefaultLoggerContext,
		rm.utils)

	mcm.RegisterListener(globalSettingChangeListener)
	rm.global_setting_svc.SetMetadataChangeHandlerCallback(globalSettingChangeListener.globalSettingChangeHandlerCallback)

	internalSettingsChangeListener := NewInternalSettingsChangeListener(
		rm.internal_settings_svc,
		rm.metadata_change_callback_cancel_ch,
		rm.children_waitgrp,
		log.DefaultLoggerContext,
		rm.utils)

	mcm.RegisterListener(internalSettingsChangeListener)
	rm.internal_settings_svc.SetMetadataChangeHandlerCallback(internalSettingsChangeListener.internalSettingsChangeHandlerCallback)

	remoteClusterChangeListener := NewRemoteClusterChangeListener(
		rm.remote_cluster_svc,
		rm.repl_spec_svc,
		rm.metadata_change_callback_cancel_ch,
		rm.children_waitgrp,
		log.DefaultLoggerContext,
		rm.utils)

	mcm.RegisterListener(remoteClusterChangeListener)
	rm.remote_cluster_svc.SetMetadataChangeHandlerCallback(remoteClusterChangeListener.remoteClusterChangeHandlerCallback)

	replicationSpecChangeListener := NewReplicationSpecChangeListener(
		rm.repl_spec_svc,
		rm.metadata_change_callback_cancel_ch,
		rm.children_waitgrp,
		log.DefaultLoggerContext,
		rm.utils)
	mcm.RegisterListener(replicationSpecChangeListener)
	rm.repl_spec_svc.SetMetadataChangeHandlerCallback(replicationSpecChangeListener.replicationSpecChangeHandlerCallback)

	mcm.Start()
}

func (rm *replicationManager) initPausedReplications() {
	for i := 0; i < service_def.MaxNumOfRetries; i++ {
		// set ReplicationStatus for paused replications so that they will show up in task list
		specs, err := rm.repl_spec_svc.AllReplicationSpecs()
		if err != nil {
			logger_rm.Errorf("Failed to get all replication specs, err=%v, num_of_retry=%v\n", err, i)
			continue

		} else {
			for _, spec := range specs {
				if !spec.Settings.Active {
					rm.pipelineMgr.GetOrCreateReplicationStatus(spec.Id, nil)
				}
			}
			return
		}
	}

	logger_rm.Errorf("Failed to initPausedReplications after %v retries.", service_def.MaxNumOfRetries)
	exitProcess(false)
}

func (rm *replicationManager) checkReplicationStatus(fin_chan chan bool) {
	logger_rm.Infof("checkReplicationStatus started.")
	defer logger_rm.Infof("checkReplicationStatus exited")

	status_check_ticker := time.NewTicker(StatusCheckInterval)
	defer status_check_ticker.Stop()
	stats_update_ticker := time.NewTicker(StatsUpdateIntervalForPausedReplications)
	defer stats_update_ticker.Stop()

	kv_mem_clients := make(map[string]mcc.ClientIface)

	for {
		select {
		case <-fin_chan:
			return
		case <-status_check_ticker.C:
			rm.pipelineMgr.CheckPipelines()
		case <-stats_update_ticker.C:
			pipeline_svc.UpdateStats(ClusterInfoService(), XDCRCompTopologyService(), CheckpointService(), kv_mem_clients, logger_rm, rm.utils)
		}
	}
}

func (rm *replicationManager) init(
	repl_spec_svc service_def.ReplicationSpecSvc,
	remote_cluster_svc service_def.RemoteClusterSvc,
	cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	replication_settings_svc service_def.ReplicationSettingsSvc,
	checkpoint_svc service_def.CheckpointsService,
	capi_svc service_def.CAPIService,
	audit_svc service_def.AuditSvc,
	uilog_svc service_def.UILogSvc,
	global_setting_svc service_def.GlobalSettingsSvc,
	bucket_settings_svc service_def.BucketSettingsSvc,
	internal_settings_svc service_def.InternalSettingsSvc) {

	rm.GenericSupervisor = *supervisor.NewGenericSupervisor(base.ReplicationManagerSupervisorId, log.DefaultLoggerContext, rm, nil, rm.utils)
	rm.pipelineMasterSupervisor = supervisor.NewGenericSupervisor(base.PipelineMasterSupervisorId, log.DefaultLoggerContext, rm, &rm.GenericSupervisor, rm.utils)
	rm.repl_spec_svc = repl_spec_svc
	rm.remote_cluster_svc = remote_cluster_svc
	rm.cluster_info_svc = cluster_info_svc
	rm.xdcr_topology_svc = xdcr_topology_svc
	rm.replication_settings_svc = replication_settings_svc
	rm.checkpoint_svc = checkpoint_svc
	rm.capi_svc = capi_svc
	rm.audit_svc = audit_svc
	rm.adminport_finch = make(chan bool, 1)
	rm.children_waitgrp = &sync.WaitGroup{}
	rm.global_setting_svc = global_setting_svc
	rm.bucket_settings_svc = bucket_settings_svc
	rm.internal_settings_svc = internal_settings_svc
	fac := factory.NewXDCRFactory(repl_spec_svc, remote_cluster_svc, cluster_info_svc, xdcr_topology_svc, checkpoint_svc, capi_svc, uilog_svc, bucket_settings_svc, log.DefaultLoggerContext, log.DefaultLoggerContext, rm, rm.pipelineMasterSupervisor, rm.utils)

	rm.pipelineMgr = pipeline_manager.NewPipelineManager(fac, repl_spec_svc, xdcr_topology_svc, remote_cluster_svc, cluster_info_svc, checkpoint_svc, uilog_svc, log.DefaultLoggerContext, rm.utils)

	rm.metadata_change_callback_cancel_ch = make(chan struct{}, 1)

	logger_rm.Info("Replication manager is initialized")

}

func ReplicationSpecService() service_def.ReplicationSpecSvc {
	return replication_mgr.repl_spec_svc
}

func RemoteClusterService() service_def.RemoteClusterSvc {
	return replication_mgr.remote_cluster_svc
}

func ClusterInfoService() service_def.ClusterInfoSvc {
	return replication_mgr.cluster_info_svc
}

func XDCRCompTopologyService() service_def.XDCRCompTopologySvc {
	return replication_mgr.xdcr_topology_svc
}

func ReplicationSettingsService() service_def.ReplicationSettingsSvc {
	return replication_mgr.replication_settings_svc
}
func CheckpointService() service_def.CheckpointsService {
	return replication_mgr.checkpoint_svc
}

func AuditService() service_def.AuditSvc {
	return replication_mgr.audit_svc
}

func GlobalSettingsService() service_def.GlobalSettingsSvc {
	return replication_mgr.global_setting_svc
}

func BucketSettingsService() service_def.BucketSettingsSvc {
	return replication_mgr.bucket_settings_svc
}

func InternalSettingsService() service_def.InternalSettingsSvc {
	return replication_mgr.internal_settings_svc
}

//CreateReplication create the replication specification in metadata store
//and start the replication pipeline
func CreateReplication(justValidate bool, sourceBucket, targetCluster, targetBucket string, settings map[string]interface{}, realUserId *base.RealUserId) (string, map[string]error, error) {
	logger_rm.Infof("Creating replication - justValidate=%v, sourceBucket=%s, targetCluster=%s, targetBucket=%s, settings=%v\n",
		justValidate, sourceBucket, targetCluster, targetBucket, settings)

	var spec *metadata.ReplicationSpecification
	spec, errorsMap, err := replication_mgr.createAndPersistReplicationSpec(justValidate, sourceBucket, targetCluster, targetBucket, settings)
	if err != nil {
		logger_rm.Errorf("%v\n", err)
		return "", nil, err
	} else if len(errorsMap) != 0 {
		return "", errorsMap, nil
	}

	if justValidate {
		return spec.Id, nil, nil
	}

	go writeCreateReplicationEvent(spec, realUserId)

	logger_rm.Infof("Replication specification %s is created\n", spec.Id)

	return spec.Id, nil, nil
}

//DeleteReplication stops the running replication of given replicationId and
//delete the replication specification from the metadata store
func DeleteReplication(topic string, realUserId *base.RealUserId) error {
	logger_rm.Infof("Deleting replication %s\n", topic)

	// delete replication spec
	spec, err := ReplicationSpecService().DelReplicationSpec(topic)
	if err == nil {
		logger_rm.Infof("Replication specification %s is deleted\n", topic)
	} else {
		logger_rm.Errorf("%v\n", err)
		return err
	}

	go writeGenericReplicationEvent(base.CancelReplicationEventId, spec, realUserId)

	logger_rm.Infof("Pipeline %s is deleted\n", topic)

	return nil
}

func PipelineMasterSupervisor() *supervisor.GenericSupervisor {
	return replication_mgr.pipelineMasterSupervisor
}

//update the  replication settings and XDCR process setting
func UpdateDefaultSettings(settings map[string]interface{}, realUserId *base.RealUserId) (map[string]error, error) {

	// Validate process setting keys
	GlobalSettingsMap := metadata.ValidateGlobalSettingsKey(settings)
	//First update XDCR Process specific setting
	errorMap, err := UpdateGlobalSettings(GlobalSettingsMap, realUserId)
	if len(errorMap) != 0 {
		return errorMap, err
	}

	//validate replication settings
	replicationSettingMap := metadata.ValidateSettingsKey(settings)
	//Now update default replication setting
	errorMapRep, err := UpdateDefaultReplicationSettings(replicationSettingMap, realUserId)
	if len(errorMapRep) != 0 {
		return errorMapRep, err
	}
	logger_rm.Infof("Updated replication settings\n")

	return nil, nil
}

//update the process  settings
func UpdateGlobalSettings(settings map[string]interface{}, realUserId *base.RealUserId) (map[string]error, error) {
	defaultSettings, err := GlobalSettingsService().GetDefaultGlobalSettings()
	if err != nil {
		return nil, err
	}

	changedSettingsMap, errorMap := defaultSettings.UpdateSettingsFromMap(settings)
	if len(errorMap) != 0 {
		return errorMap, nil
	}

	if len(changedSettingsMap) != 0 {
		err = GlobalSettingsService().SetDefaultGlobalSettings(defaultSettings)
		if err != nil {
			return nil, err
		}
		logger_rm.Infof("Default Process settings saved succesfully\n")
	} else {
		logger_rm.Infof("Did not update process  settings since there are no real changes")
	}

	return nil, nil
}

//update the default replication settings
func UpdateDefaultReplicationSettings(settings map[string]interface{}, realUserId *base.RealUserId) (map[string]error, error) {
	defaultSettings, err := ReplicationSettingsService().GetDefaultReplicationSettings()
	if err != nil {
		return nil, err
	}

	changedSettingsMap, errorMap := defaultSettings.UpdateSettingsFromMap(settings)
	if len(errorMap) != 0 {
		return errorMap, nil
	}

	if len(changedSettingsMap) != 0 {
		err = ReplicationSettingsService().SetDefaultReplicationSettings(defaultSettings)
		if err != nil {
			return nil, err
		}
		logger_rm.Infof("Updated default replication settings\n")

		go writeUpdateDefaultReplicationSettingsEvent(&changedSettingsMap, realUserId)

	} else {
		logger_rm.Infof("Did not update default replication settings since there are no real changes")
	}

	return nil, nil
}

//update the per-replication settings
func UpdateReplicationSettings(topic string, settings map[string]interface{}, realUserId *base.RealUserId) (map[string]error, error) {
	logger_rm.Infof("Update replication settings for %v, settings=%v\n", topic, settings)
	// read replication spec with the specified replication id
	replSpec, err := ReplicationSpecService().ReplicationSpec(topic)
	if err != nil {
		return nil, err
	}

	oldFilterExpression := replSpec.Settings.FilterExpression

	// update replication spec with input settings
	changedSettingsMap, errorMap := replSpec.Settings.UpdateSettingsFromMap(settings)

	// enforce that filter expression cannot be changed
	newFilterExpression, ok := settings[FilterExpression]
	if ok {
		if newFilterExpression != oldFilterExpression {
			errorMap[FilterExpression] = errors.New("Filter expression cannot be changed after the replication is created")
		}
	}

	if len(errorMap) != 0 {
		return errorMap, nil
	}

	if len(changedSettingsMap) != 0 {
		err = ReplicationSpecService().SetReplicationSpec(replSpec)
		if err != nil {
			return nil, err
		}
		logger_rm.Infof("Updated replication settings for replication %v\n", topic)

		go writeUpdateReplicationSettingsEvent(replSpec, &changedSettingsMap, realUserId)

		// if the active flag has been changed, log Pause/ResumeReplication event
		active, ok := changedSettingsMap[metadata.Active]
		if ok {
			if active.(bool) {
				go writeGenericReplicationEvent(base.ResumeReplicationEventId, replSpec, realUserId)
			} else {
				go writeGenericReplicationEvent(base.PauseReplicationEventId, replSpec, realUserId)
			}
		}
		logger_rm.Infof("Done with replication settings auditing for replication %v\n", topic)

	} else {
		logger_rm.Infof("Did not update replication settings for replication %v since there are no real changes", topic)
	}

	return nil, nil
}

// get statistics for all running replications
//% returns a list of replication stats for the bucket. the format for each
//% item in the list is:
//% {ReplicationDocId,           & the settings doc id for this replication
//%    [{changes_left, Integer}, % amount of work remaining
//%     {docs_checked, Integer}, % total number of docs checked on target, survives restarts
//%     {docs_written, Integer}, % total number of docs written to target, survives restarts
//%     ...
//%    ]
//% }
func GetStatistics(bucket string) (*expvar.Map, error) {
	repIds := replication_mgr.pipelineMgr.AllReplicationsForBucket(bucket)
	logger_rm.Debugf("repIds=%v\n", repIds)

	stats := new(expvar.Map).Init()
	for _, repId := range repIds {
		statsForPipeline, err := pipeline_svc.GetStatisticsForPipeline(repId)
		if err == nil && statsForPipeline != nil {
			stats.Set(repId, statsForPipeline)
		}
	}
	logger_rm.Debugf("stats=%v\n", stats)

	return stats, nil
}

//create and persist the replication specification
func (rm *replicationManager) createAndPersistReplicationSpec(justValidate bool, sourceBucket, targetCluster, targetBucket string, settings map[string]interface{}) (*metadata.ReplicationSpecification, map[string]error, error) {
	logger_rm.Infof("Creating replication spec - justValidate=%v, sourceBucket=%s, targetCluster=%s, targetBucket=%s, settings=%v\n",
		justValidate, sourceBucket, targetCluster, targetBucket, settings)

	// validate that everything is alright with the replication configuration before actually creating it
	sourceBucketUUID, targetBucketUUID, targetClusterRef, errorMap, warning := replication_mgr.repl_spec_svc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings)
	if len(errorMap) > 0 {
		return nil, errorMap, nil
	}

	spec, err := metadata.NewReplicationSpecification(sourceBucket, sourceBucketUUID, targetClusterRef.Uuid, targetBucket, targetBucketUUID)
	if err != nil {
		return nil, nil, err
	}

	replSettings, err := ReplicationSettingsService().GetDefaultReplicationSettings()
	if err != nil {
		return nil, nil, err
	}

	// default isCapi to false if replication type is not explicitly specified in settings
	isCapi := false
	for key, value := range settings {
		if key == metadata.ReplicationType {
			isCapi = (value == metadata.ReplicationTypeCapi)
			break
		}
	}

	if isCapi {
		// for capi replication, ensure that bandwith limit is 0 regardless of the default setting
		replSettings.BandwidthLimit = 0
	}

	_, errorMap = replSettings.UpdateSettingsFromMap(settings)
	if len(errorMap) != 0 {
		return nil, errorMap, nil
	}
	spec.Settings = replSettings

	if justValidate {
		return spec, nil, nil
	}

	//persist it
	err = replication_mgr.repl_spec_svc.AddReplicationSpec(spec, warning)
	if err == nil {
		logger_rm.Infof("Success adding replication specification %s\n", spec.Id)
		return spec, nil, nil
	} else {
		logger_rm.Errorf("Error adding replication specification %s. err=%v\n", spec.Id, err)
		return spec, nil, err
	}
}

/**
 * Certain errors are still considered errors but should not be raised to the UI level.
 * Returns true if the error is to be hidden from the web GUI.
 */
func bypassUIErrorCodes(errStr string) bool {
	if errStr == base.ErrorNoSourceNozzle.Error() {
		return true
	} else if strings.Contains(errStr, base.ErrorMasterNegativeIndex.Error()) {
		return true
	}
	return false
}

// get info of all running replications - serves back to consumers who call the REST end point, i.e. UI
func GetReplicationInfos() ([]base.ReplicationInfo, error) {
	replInfos := make([]base.ReplicationInfo, 0)

	replIds := replication_mgr.pipelineMgr.AllReplications()

	for _, replId := range replIds {
		replInfo := base.ReplicationInfo{}
		replInfo.Id = replId
		replInfo.StatsMap = make(map[string]interface{})
		replInfo.ErrorList = make([]base.ErrorInfo, 0)

		rep_status, _ := replication_mgr.pipelineMgr.ReplicationStatus(replId)
		if rep_status != nil {
			// set stats map
			expvarMap, err := pipeline_svc.GetStatisticsForPipeline(replId)
			if err == nil && expvarMap != nil {
				replInfo.StatsMap = replication_mgr.utils.GetMapFromExpvarMap(expvarMap)
				validateStatsMap(replInfo.StatsMap)
			}

			// set error list
			errs := rep_status.Errors()
			if len(errs) > 0 {
				cur_node, err := XDCRCompTopologyService().MyHost()
				if err != nil {
					panic("cannot find current host")
				}

				for _, pipeline_error := range errs {
					if !bypassUIErrorCodes(pipeline_error.ErrMsg) {
						//prepend current node name to the error message to make it more helpful
						err_msg := cur_node + ":" + pipeline_error.ErrMsg
						errInfo := base.ErrorInfo{pipeline_error.Timestamp.UnixNano(), err_msg}
						replInfo.ErrorList = append(replInfo.ErrorList, errInfo)
					}
				}
			}
		}

		// set maxVBReps stats to 0 when replication has never been run or has been paused to ensure that ns_server gets the correct replication status
		if rep_status == nil || rep_status.RuntimeStatus(true) == pipeline.Paused {
			replInfo.StatsMap[base.MaxVBReps] = 0
		}

		replInfos = append(replInfos, replInfo)
	}
	return replInfos, nil
}

func validateStatsMap(statsMap map[string]interface{}) {
	missingStats := make([]string, 0)
	if _, ok := statsMap[pipeline_svc.CHANGES_LEFT_METRIC]; !ok {
		missingStats = append(missingStats, pipeline_svc.CHANGES_LEFT_METRIC)
	}
	if len(missingStats) > 0 {
		logger_rm.Errorf("Stats missing when constructing replication infos: %v", missingStats)
	}
}

//error handler
func (rm *replicationManager) OnError(s common.Supervisor, errMap map[string]error) {
	logger_rm.Infof("Supervisor %v of type %v reported errors %v\n", s.Id(), reflect.TypeOf(s), errMap)

	if s.Id() == base.ReplicationManagerSupervisorId {
		// the errors came from the replication manager supervisor because adminport or pipeline master supervisor is not longer alive.
		// there is nothing we can do except to abort xdcr. ns_server will restart xdcr while later
		exitProcess(false)
	} else if s.Id() == base.PipelineMasterSupervisorId {
		// the errors came from the pipeline master supervisor because some pipeline supervisors are not longer alive.
		for childId, err1 := range errMap {
			child, _ := s.Child(childId)
			// child could be null if the pipeline has been stopped so far. //TODO should we restart it here?
			if child != nil {
				pipeline, err := rm.getPipelineFromPipelineSupevisor(child.(common.Supervisor))
				if err == nil {
					// try to fix the pipeline
					rm.pipelineMgr.Update(pipeline.Topic(), err1)
				}
			}
		}
	} else {
		// the errors came from a pipeline supervisor because some parts are not longer alive.

		if len(errMap) == 0 {
			panic("errMap is empty")
		}
		pipeline, err := rm.getPipelineFromPipelineSupevisor(s)
		if err == nil {
			// try to fix the pipeline

			var errMsg string
			if len(errMap) > 1 {
				errMsg = fmt.Sprintf("%v", errMap)
			} else {
				// strip the "map[]" wapper when there is only one error in errMap
				for partId, partMsg := range errMap {
					errMsg = partId + ":" + partMsg.Error()
					break
				}
			}
			rm.pipelineMgr.Update(pipeline.Topic(), errors.New(errMsg))
		}
	}
}

//lauch the repairer for a pipeline
//in asynchronous fashion

func (rm *replicationManager) getPipelineFromPipelineSupevisor(s common.Supervisor) (common.Pipeline, error) {
	supervisorId := s.Id()
	if strings.HasPrefix(supervisorId, base.PipelineSupervisorIdPrefix) {
		pipelineId := supervisorId[len(base.PipelineSupervisorIdPrefix):]
		rep_status, _ := rm.pipelineMgr.ReplicationStatus(pipelineId)
		if rep_status != nil {
			pipeline := rep_status.Pipeline()
			if pipeline != nil {
				return pipeline, nil
			} else {
				// should never get here
				return nil, errors.New(fmt.Sprintf("Internal error. Pipeline, %v, is not found", pipelineId))
			}
		} else {
			logger_rm.Errorf("Replication %v no longer exists", pipelineId)
			return nil, errors.New(fmt.Sprintf("Internal error. Replication %v, is not found", pipelineId))
		}
	} else {
		// should never get here
		return nil, errors.New(fmt.Sprintf("Internal error. Supervisor, %v, is not a pipeline supervisor.", supervisorId))
	}
}

func validatePipelineExists(topic, action string, exist bool) error {
	_, err := replication_mgr.repl_spec_svc.ReplicationSpec(topic)
	pipelineExist := (err == nil)
	if pipelineExist != exist {
		state := "already exists"
		if exist {
			state = "does not exist"
		}
		return errors.New(fmt.Sprintf("Error %v replication with id, %v, since it %v.\n", action, topic, state))
	}
	return nil
}

// ns_server shutdown protocol: poll stdin and exit upon reciept of EOF
func pollStdin() {
	reader := bufio.NewReader(os.Stdin)
	logger_rm.Infof("pollEOF: About to start stdin polling")
	for {
		ch, err := reader.ReadByte()
		logger_rm.Infof("received byte %v\n", ch)
		if err == io.EOF {
			logger_rm.Infof("Received EOF; Exiting...")
			exitProcess(false)
		}
		if err != nil {
			logger_rm.Errorf("Unexpected error polling stdin: %v\n", err)
			exitProcess(true)
		}
		if ch == '\n' || ch == '\r' {
			logger_rm.Infof("Received EOL; Exiting...")
			exitProcess(false)
		}
	}
}

// periodically log mem stats to facilitate debugging of memory issues
func logMemStats(fin_chan chan bool) {
	logger_rm.Infof("logMemStats started.")
	defer logger_rm.Infof("logMemStats exited")

	mem_stats_ticker := time.NewTicker(MemStatsLogInterval)
	defer mem_stats_ticker.Stop()

	stats := new(runtime.MemStats)
	var bytes []byte

	for {
		select {
		case <-fin_chan:
			return
		case <-mem_stats_ticker.C:
			runtime.ReadMemStats(stats)
			bytes, _ = json.Marshal(stats)
			logger_rm.Infof("Mem stats = %v\n", string(bytes))
		}
	}
}

// periodically refresh remote cluster refs
func refreshRemoteClusterRef(fin_chan chan bool) {
	logger_rm.Infof("refreshRemoteClusterRef started.")
	defer logger_rm.Infof("refreshRemoteClusterRef exited")

	ticker := time.NewTicker(base.RefreshRemoteClusterRefInterval)
	defer ticker.Stop()

	remoteClusterSvc := RemoteClusterService()

	for {
		select {
		case <-fin_chan:
			return
		case <-ticker.C:
			_, err := remoteClusterSvc.RemoteClusters(true /*refresh*/)
			if err != nil {
				logger_rm.Warnf("Error refreshing remote cluster refs = %v\n", err)
			}
		}
	}
}

//gracefull stop
func cleanup() {
	if replication_mgr.running {

		replication_mgr.running = false

		//stop the generic supervisor
		replication_mgr.Stop()

		//stop pipelineMasterSupervisor
		replication_mgr.pipelineMasterSupervisor.Stop()

		// stop listening to spec changed events
		replication_mgr.metadata_change_callback_cancel_ch <- struct{}{}
		logger_rm.Infof("Sent cancel signal to metadata change listeners")

		// kill adminport to stop receiving new requests
		close(replication_mgr.adminport_finch)

		simple_utils.ExecWithTimeout(replication_mgr.pipelineMgr.OnExit, 1*time.Second, logger_rm)

		close(replication_mgr.status_logger_finch)
		close(replication_mgr.mem_stats_logger_finch)
		close(replication_mgr.refresh_remote_cluster_ref_finch)

		logger_rm.Infof("Replication manager exists")
	} else {
		logger_rm.Info("Replication manager is already in the processof stopping, no-op on this stop request")
	}
}

func isReplicationManagerRunning() bool {
	replication_mgr.running_lock.RLock()
	defer replication_mgr.running_lock.RUnlock()
	return replication_mgr.running
}

// CAS operation on running state. returns its old value before set
func checkAndSetRunningState() bool {
	replication_mgr.running_lock.Lock()
	defer replication_mgr.running_lock.Unlock()

	if replication_mgr.running {
		replication_mgr.running = false
		return true
	} else {
		return false
	}
}

//crash
func exitProcess(byForce bool) {
	wasRunning := checkAndSetRunningState()
	if wasRunning {
		logger_rm.Info("Replication manager is exiting...")
		exitProcess_once(byForce)
		os.Exit(0)
		logger_rm.Info("Replication manager exited")
	}
}

// this method is so named because it is called only once due to the CAS performed by the caller, exitProcess()
func exitProcess_once(byForce bool) {
	//clean up the connection pool
	defer base.ConnPoolMgr().Close()

	if !byForce {
		cleanup()
	}
}

func writeGenericReplicationEvent(eventId uint32, spec *metadata.ReplicationSpecification, realUserId *base.RealUserId) {
	event, err := constructGenericReplicationEvent(spec, realUserId)
	if err == nil {
		err = AuditService().Write(eventId, event)
	}

	logAuditErrors(err)
}

func writeCreateReplicationEvent(spec *metadata.ReplicationSpecification, realUserId *base.RealUserId) {
	genericReplicationEvent, err := constructGenericReplicationEvent(spec, realUserId)
	if err == nil {
		createReplicationEvent := &base.CreateReplicationEvent{
			GenericReplicationEvent: *genericReplicationEvent,
			FilterExpression:        spec.Settings.FilterExpression}

		err = AuditService().Write(base.CreateReplicationEventId, createReplicationEvent)
	}

	logAuditErrors(err)
}

func writeUpdateDefaultReplicationSettingsEvent(changedSettingsMap *map[string]interface{}, realUserId *base.RealUserId) {
	event, err := constructUpdateDefaultReplicationSettingsEvent(changedSettingsMap, realUserId)
	if err == nil {
		err = AuditService().Write(base.UpdateDefaultReplicationSettingsEventId, event)
	}
	logAuditErrors(err)
}

func writeUpdateReplicationSettingsEvent(spec *metadata.ReplicationSpecification, changedSettingsMap *map[string]interface{}, realUserId *base.RealUserId) {
	replicationSpecificFields, err := constructReplicationSpecificFieldsFromSpec(spec)
	if err == nil {
		var updateDefaultReplicationSettingsEvent *base.UpdateDefaultReplicationSettingsEvent
		updateDefaultReplicationSettingsEvent, err = constructUpdateDefaultReplicationSettingsEvent(changedSettingsMap, realUserId)
		if err == nil {
			updateReplicationSettingsEvent := &base.UpdateReplicationSettingsEvent{
				ReplicationSpecificFields:             *replicationSpecificFields,
				UpdateDefaultReplicationSettingsEvent: *updateDefaultReplicationSettingsEvent}
			err = AuditService().Write(base.UpdateReplicationSettingsEventId, updateReplicationSettingsEvent)
		}
	}
	logAuditErrors(err)
}

func writeUpdateBucketSettingsEvent(bucketName string, lwwEnabled bool, realUserId *base.RealUserId) {
	updateBucketSettingsEvent := constructUpdateBucketSettingsEvent(bucketName, lwwEnabled, realUserId)
	err := AuditService().Write(base.UpdateBucketSettingsEventId, updateBucketSettingsEvent)
	logAuditErrors(err)
}

func constructGenericReplicationFields(realUserId *base.RealUserId) (*base.GenericReplicationFields, error) {
	localClusterName, err := XDCRCompTopologyService().MyHostAddr()
	if err != nil {
		return nil, err
	}

	return &base.GenericReplicationFields{
		GenericFields:    base.GenericFields{log.FormatTimeWithMilliSecondPrecision(time.Now()), *realUserId},
		LocalClusterName: localClusterName}, nil
}

func constructReplicationSpecificFieldsFromSpec(spec *metadata.ReplicationSpecification) (*base.ReplicationSpecificFields, error) {
	remoteClusterName := RemoteClusterService().GetRemoteClusterNameFromClusterUuid(spec.TargetClusterUUID)

	return &base.ReplicationSpecificFields{
		SourceBucketName:  spec.SourceBucketName,
		RemoteClusterName: remoteClusterName,
		TargetBucketName:  spec.TargetBucketName}, nil
}

func constructGenericReplicationEvent(spec *metadata.ReplicationSpecification, realUserId *base.RealUserId) (*base.GenericReplicationEvent, error) {
	genericReplicationFields, err := constructGenericReplicationFields(realUserId)
	if err != nil {
		return nil, err
	}

	replicationSpecificFields, err := constructReplicationSpecificFieldsFromSpec(spec)
	if err != nil {
		return nil, err
	}

	return &base.GenericReplicationEvent{
		GenericReplicationFields:  *genericReplicationFields,
		ReplicationSpecificFields: *replicationSpecificFields}, nil
}

func constructUpdateDefaultReplicationSettingsEvent(changedSettingsMap *map[string]interface{}, realUserId *base.RealUserId) (*base.UpdateDefaultReplicationSettingsEvent, error) {
	logger_rm.Info("Start constructUpdateDefaultReplicationSettingsEvent....")
	genericReplicationFields, err := constructGenericReplicationFields(realUserId)
	if err != nil {
		return nil, err
	}

	// convert keys in changedSettingsMap from internal metadata keys to external facing rest api keys
	convertedSettingsMap := make(map[string]interface{})
	for key, value := range *changedSettingsMap {
		if key == metadata.Active {
			convertedSettingsMap[SettingsKeyToRestKeyMap[key]] = !(value.(bool))
		} else {
			convertedSettingsMap[SettingsKeyToRestKeyMap[key]] = value
		}
	}
	logger_rm.Info("Done constructUpdateDefaultReplicationSettingsEvent....")

	return &base.UpdateDefaultReplicationSettingsEvent{
		GenericReplicationFields: *genericReplicationFields,
		UpdatedSettings:          convertedSettingsMap}, nil
}

func constructUpdateBucketSettingsEvent(bucketName string, lwwEnabled bool, realUserId *base.RealUserId) *base.UpdateBucketSettingsEvent {
	logger_rm.Info("Start constructUpdateBucketSettingsEvent....")

	settingsMap := make(map[string]interface{})
	settingsMap[LWWEnabled] = lwwEnabled
	logger_rm.Info("Done constructUpdateBucketSettingsEvent....")

	return &base.UpdateBucketSettingsEvent{
		GenericFields:   base.GenericFields{log.FormatTimeWithMilliSecondPrecision(time.Now()), *realUserId},
		UpdatedSettings: settingsMap}

}

func logAuditErrors(err error) {
	if err != nil {
		err = replication_mgr.utils.NewEnhancedError(base.ErrorWritingAudit, err)
		logger_rm.Errorf(err.Error())
	}
}

//pull GoMaxProc from environment
func GoMaxProcs_env() int {
	max_procs_str := os.Getenv("GOXDCR_GOMAXPROCS")
	var max_procs int
	max_procs, err := strconv.Atoi(max_procs_str)
	if err != nil {
		max_procs = 4
		if runtime.NumCPU() < 4 {
			max_procs = runtime.NumCPU()
		}
	}
	logger_rm.Infof("GOMAXPROCS=%v\n", max_procs)
	return max_procs

}

func getBucketSettings(bucketName string) (map[string]interface{}, error) {
	bucketSettings, err := BucketSettingsService().BucketSettings(bucketName)
	if err != nil {
		return nil, err
	}
	return bucketSettings.ToMap(), nil
}

func setBucketSettings(bucketName string, lwwEnabled bool, realUserId *base.RealUserId) (map[string]interface{}, error) {
	bucketSettings := &metadata.BucketSettings{BucketName: bucketName, LWWEnabled: lwwEnabled}
	err := BucketSettingsService().SetBucketSettings(bucketName, bucketSettings)
	if err != nil {
		return nil, err
	}

	writeUpdateBucketSettingsEvent(bucketName, lwwEnabled, realUserId)

	// return new settings after set op
	return getBucketSettings(bucketName)
}

// when cluster is upgrade to 5.0, existing remote cluster refs do not have encryptionType field populated
// set encryptionType field for such refs
func (rm *replicationManager) upgradeRemoteClusterRefs() {
	logger_rm.Infof("upgradeRemoteClusterRefs started.")
	defer logger_rm.Infof("upgradeRemoteClusterRefs exited")

	remoteClusterSvc := rm.remote_cluster_svc
	remoteClusterRefs, err := remoteClusterSvc.RemoteClusters(false /*refresh*/)
	if err != nil {
		logger_rm.Warnf("Skipping upgradeRemoteClusterRefs because of err =%v", err)
		return
	}
	for _, remoteClusterRef := range remoteClusterRefs {
		if remoteClusterRef.IsEncryptionEnabled() && len(remoteClusterRef.EncryptionType) == 0 {
			remoteClusterRef.EncryptionType = metadata.EncryptionType_Full
			err = remoteClusterSvc.SetRemoteCluster(remoteClusterRef.Name, remoteClusterRef)
			if err != nil {
				logger_rm.Warnf("Skipping upgrading remote cluster ref %v because of err =%v", remoteClusterRef.Name, err)
				continue
			} else {
				logger_rm.Infof("Successfully upgraded remote cluster ref %v", remoteClusterRef.Name)
			}
		}
	}
}
