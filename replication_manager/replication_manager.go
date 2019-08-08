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
	"github.com/couchbase/goxdcr/resource_manager"
	"github.com/couchbase/goxdcr/service_def"
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
/* ReplicationManager Interface
*************************************/
type ReplicationManagerIf interface {
	OnError(s common.Supervisor, errMap base.ErrorMap)
}

/************************************
/* struct ReplicationManager
*************************************/
type replicationManager struct {
	// supervises the livesness of adminport
	supervisor.GenericSupervisor
	// Single instance of pipeline_mgr here instead of using a global
	pipelineMgr pipeline_manager.Pipeline_mgr_iface

	resourceMgr resource_manager.ResourceMgrIface

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
	// Collections Manifests service
	collectionsManifestSvc service_def.CollectionsManifestSvc

	once sync.Once

	//finish channel for adminport
	adminport_finch chan bool

	metadata_change_callback_cancel_ch chan struct{}

	running      bool
	running_lock sync.RWMutex

	children_waitgrp *sync.WaitGroup

	status_logger_finch chan bool

	mem_stats_logger_finch chan bool
}

//singleton
var replication_mgr replicationManager

func StartReplicationManager(sourceKVHost string,
	xdcrRestPort uint16, sourceKVAdminPort uint16,
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
	throughput_throttler_svc service_def.ThroughputThrottlerSvc,
	utilitiesIn utilities.UtilsIface,
	collectionsManifestSvc service_def.CollectionsManifestSvc) {

	replication_mgr.once.Do(func() {
		// ns_server shutdown protocol: poll stdin and exit upon reciept of EOF
		go pollStdin()

		// initialize constants
		initConstants(xdcr_topology_svc, internal_settings_svc)

		// Take in utilities
		replication_mgr.utils = utilitiesIn

		// initializes replication manager
		replication_mgr.init(repl_spec_svc, remote_cluster_svc, cluster_info_svc,
			xdcr_topology_svc, replication_settings_svc, checkpoint_svc, capi_svc, audit_svc,
			uilog_svc, global_setting_svc, bucket_settings_svc, internal_settings_svc,
			throughput_throttler_svc, collectionsManifestSvc)

		// start replication manager supervisor
		// TODO should we make heart beat settings configurable?
		replication_mgr.GenericSupervisor.Start(nil)

		// set ReplicationStatus for both paused and active replications
		replication_mgr.initReplications()
		logger_rm.Info("initReplications succeeded")

		replication_mgr.running = true
		replication_mgr.running_lock = sync.RWMutex{}

		replication_mgr.status_logger_finch = make(chan bool, 1)
		go replication_mgr.checkReplicationStatus(replication_mgr.status_logger_finch)

		// periodically log mem stats to facilitate debugging of memory issues
		replication_mgr.mem_stats_logger_finch = make(chan bool, 1)
		go logMemStats(replication_mgr.mem_stats_logger_finch)

		// upgrade remote cluster refs before initializing metadata change monitor
		// and starting adminport to reduce interference
		replication_mgr.upgradeRemoteClusterRefs()

		replication_mgr.initMetadataChangeMonitor()

		// start adminport
		adminport := NewAdminport(sourceKVHost, xdcrRestPort, sourceKVAdminPort, replication_mgr.adminport_finch, replication_mgr.utils)
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

	metadata.InitConstants(internal_settings.Values[metadata.XmemMaxIdleCountLowerBoundKey].(int),
		internal_settings.Values[metadata.XmemMaxIdleCountUpperBoundKey].(int))

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
		internal_settings.Values[metadata.MaxCheckpointRecordsToReadKey].(int),
		time.Duration(internal_settings.Values[metadata.DefaultHttpTimeoutKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.ShortHttpTimeoutKey].(int))*time.Second,
		internal_settings.Values[metadata.MaxRetryForLiveUpdatePipelineKey].(int),
		time.Duration(internal_settings.Values[metadata.WaitTimeForLiveUpdatePipelineKey].(int))*time.Millisecond,
		time.Duration(internal_settings.Values[metadata.ReplSpecCheckIntervalKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.MemStatsLogIntervalKey].(int))*time.Second,
		internal_settings.Values[metadata.MaxNumOfMetakvRetriesKey].(int),
		time.Duration(internal_settings.Values[metadata.RetryIntervalMetakvKey].(int))*time.Millisecond,
		internal_settings.Values[metadata.UprFeedDataChanLengthKey].(int),
		internal_settings.Values[metadata.UprFeedBufferSizeKey].(int),
		internal_settings.Values[metadata.XmemMaxRetryKey].(int),
		time.Duration(internal_settings.Values[metadata.XmemWriteTimeoutKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.XmemReadTimeoutKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.XmemMaxReadDownTimeKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.XmemBackoffWaitTimeKey].(int))*time.Millisecond,
		internal_settings.Values[metadata.XmemMaxBackoffFactorKey].(int),
		internal_settings.Values[metadata.XmemMaxRetryNewConnKey].(int),
		time.Duration(internal_settings.Values[metadata.XmemBackoffTimeNewConnKey].(int))*time.Millisecond,
		time.Duration(internal_settings.Values[metadata.XmemSelfMonitorIntervalKey].(int))*time.Second,
		internal_settings.Values[metadata.XmemMaxIdleCountKey].(int),
		internal_settings.Values[metadata.XmemMaxIdleCountLowerBoundKey].(int),
		internal_settings.Values[metadata.XmemMaxIdleCountUpperBoundKey].(int),
		internal_settings.Values[metadata.XmemMaxDataChanSizeKey].(int),
		internal_settings.Values[metadata.XmemMaxBatchSizeKey].(int),
		time.Duration(internal_settings.Values[metadata.CapiRetryIntervalKey].(int))*time.Millisecond,
		internal_settings.Values[metadata.MaxLengthSnapshotHistoryKey].(int),
		internal_settings.Values[metadata.MaxRetryTargetStatsKey].(int),
		time.Duration(internal_settings.Values[metadata.RetryIntervalTargetStatsKey].(int))*time.Millisecond,
		internal_settings.Values[metadata.NumberOfSlotsForBandwidthThrottlingKey].(int),
		internal_settings.Values[metadata.PercentageOfBytesToSendAsMinKey].(int),
		time.Duration(internal_settings.Values[metadata.AuditWriteTimeoutKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.AuditReadTimeoutKey].(int))*time.Second,
		internal_settings.Values[metadata.MaxRetryCapiServiceKey].(int),
		internal_settings.Values[metadata.MaxNumberOfAsyncListenersKey].(int),
		time.Duration(internal_settings.Values[metadata.XmemMaxRetryIntervalKey].(int))*time.Second,
		internal_settings.Values[metadata.XmemMaxRetryMutationLockedKey].(int),
		time.Duration(internal_settings.Values[metadata.XmemMaxRetryIntervalMutationLockedKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.HELOTimeoutKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.WaitTimeBetweenMetadataChangeListenersKey].(int))*time.Millisecond,
		time.Duration(internal_settings.Values[metadata.KeepAlivePeriodKey].(int))*time.Second,
		internal_settings.Values[metadata.ThresholdPercentageForEventChanSizeLoggingKey].(int),
		time.Duration(internal_settings.Values[metadata.ThresholdForThroughSeqnoComputationKey].(int))*time.Millisecond,
		time.Duration(internal_settings.Values[metadata.StatsLogIntervalKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.XmemDefaultRespTimeoutKey].(int))*time.Millisecond,
		internal_settings.Values[metadata.BypassSanInCertificateCheckKey].(int),
		internal_settings.Values[metadata.ReplicationSpecGCCntKey].(int),
		time.Duration(internal_settings.Values[metadata.TimeoutRuntimeContextStartKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.TimeoutRuntimeContextStopKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.TimeoutPartsStartKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.TimeoutPartsStopKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.TimeoutDcpCloseUprStreamsKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.TimeoutDcpCloseUprFeedKey].(int))*time.Second,
		time.Duration(internal_settings.Values[metadata.CpuCollectionIntervalKey].(int))*time.Millisecond,
		time.Duration(internal_settings.Values[metadata.ResourceManagementIntervalKey].(int))*time.Millisecond,
		time.Duration(internal_settings.Values[metadata.ResourceManagementStatsIntervalKey].(int))*time.Millisecond,
		internal_settings.Values[metadata.ChangesLeftThresholdForOngoingReplicationKey].(int),
		internal_settings.Values[metadata.ResourceManagementRatioBaseKey].(int),
		internal_settings.Values[metadata.ResourceManagementRatioUpperBoundKey].(int),
		internal_settings.Values[metadata.MaxCountBacklogForSetDcpPriorityKey].(int),
		internal_settings.Values[metadata.MaxCountNoBacklogForResetDcpPriorityKey].(int),
		internal_settings.Values[metadata.ExtraQuotaForUnderutilizedCPUKey].(int),
		time.Duration(internal_settings.Values[metadata.ThroughputThrottlerLogIntervalKey].(int))*time.Millisecond,
		time.Duration(internal_settings.Values[metadata.ThroughputThrottlerClearTokensIntervalKey].(int))*time.Millisecond,
		internal_settings.Values[metadata.NumberOfSlotsForThroughputThrottlingKey].(int),
		internal_settings.Values[metadata.IntervalForThrottlerCalibrationKey].(int),
		internal_settings.Values[metadata.ThroughputSampleSizeKey].(int),
		internal_settings.Values[metadata.ThroughputSampleAlphaKey].(int),
		internal_settings.Values[metadata.ThresholdRatioForProcessCpuKey].(int),
		internal_settings.Values[metadata.ThresholdRatioForTotalCpuKey].(int),
		internal_settings.Values[metadata.MaxCountCpuNotMaxedKey].(int),
		internal_settings.Values[metadata.MaxCountThroughputDropKey].(int),
		internal_settings.Values[metadata.FilteringInternalKey].(string),
		internal_settings.Values[metadata.FilteringInternalXattr].(string),
	)
}

func (rm *replicationManager) initMetadataChangeMonitor() {
	mcm := base.NewMetadataChangeMonitor()

	// the sequence of the listener registration matters
	// for example, replicationSpecChangeListener will get all active replications started
	// this requires replication settings and remote cluster reference to be initialized
	// that is why it needs to be registered and started after the other listeners

	globalSettingChangeListener := NewGlobalSettingChangeListener(
		rm.global_setting_svc,
		rm.metadata_change_callback_cancel_ch,
		rm.children_waitgrp,
		log.DefaultLoggerContext,
		rm.resourceMgr)

	mcm.RegisterListener(globalSettingChangeListener)
	rm.global_setting_svc.SetMetadataChangeHandlerCallback(globalSettingChangeListener.globalSettingChangeHandlerCallback)

	internalSettingsChangeListener := NewInternalSettingsChangeListener(
		rm.internal_settings_svc,
		rm.metadata_change_callback_cancel_ch,
		rm.children_waitgrp,
		log.DefaultLoggerContext)

	mcm.RegisterListener(internalSettingsChangeListener)
	rm.internal_settings_svc.SetMetadataChangeHandlerCallback(internalSettingsChangeListener.internalSettingsChangeHandlerCallback)

	remoteClusterChangeListener := NewRemoteClusterChangeListener(
		rm.remote_cluster_svc,
		rm.repl_spec_svc,
		rm.metadata_change_callback_cancel_ch,
		rm.children_waitgrp,
		log.DefaultLoggerContext)

	mcm.RegisterListener(remoteClusterChangeListener)
	rm.remote_cluster_svc.SetMetadataChangeHandlerCallback(remoteClusterChangeListener.remoteClusterChangeHandlerCallback)

	replicationSpecChangeListener := NewReplicationSpecChangeListener(
		rm.repl_spec_svc,
		rm.metadata_change_callback_cancel_ch,
		rm.children_waitgrp,
		log.DefaultLoggerContext,
		rm.resourceMgr)
	mcm.RegisterListener(replicationSpecChangeListener)
	rm.repl_spec_svc.SetMetadataChangeHandlerCallback(replicationSpecChangeListener.replicationSpecChangeHandlerCallback)

	mcm.Start()
}

func (rm *replicationManager) initReplications() {
	var specs map[string]*metadata.ReplicationSpecification
	var err error

	specs, err = rm.repl_spec_svc.AllReplicationSpecs()

	if err == nil {
		for _, spec := range specs {
			if spec.Settings.Active {
				logger_rm.Infof("Initializing active replication %v", spec.Id)
				rm.pipelineMgr.UpdatePipeline(spec.Id, nil)
			} else {
				logger_rm.Infof("Initializing paused replication %v", spec.Id)
				rm.pipelineMgr.InitiateRepStatus(spec.Id)
			}
		}
	} else {
		logger_rm.Errorf("Failed to initReplications - unable to retrieve specs from service cache.")
		ExitProcess(false)
	}
}

func (rm *replicationManager) checkReplicationStatus(fin_chan chan bool) {
	logger_rm.Infof("checkReplicationStatus started.")
	defer logger_rm.Infof("checkReplicationStatus exited")

	status_check_ticker := time.NewTicker(base.ReplSpecCheckInterval)
	defer status_check_ticker.Stop()
	stats_update_ticker := time.NewTicker(StatsUpdateIntervalForPausedReplications)
	defer stats_update_ticker.Stop()

	// key of outer map - bucket name
	// key of inner map - kv address
	// value - memcached client
	bucket_kv_mem_clients := make(map[string]map[string]mcc.ClientIface)

	for {
		select {
		case <-fin_chan:
			return
		case <-status_check_ticker.C:
			rm.pipelineMgr.CheckPipelines()
		case <-stats_update_ticker.C:
			pipeline_svc.UpdateStats(ClusterInfoService(), XDCRCompTopologyService(), CheckpointService(), bucket_kv_mem_clients, logger_rm, rm.utils)
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
	internal_settings_svc service_def.InternalSettingsSvc,
	throughput_throttler_svc service_def.ThroughputThrottlerSvc,
	collectionsManifestSvc service_def.CollectionsManifestSvc) {

	rm.GenericSupervisor = *supervisor.NewGenericSupervisor(base.ReplicationManagerSupervisorId, log.DefaultLoggerContext, rm, nil, rm.utils)
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
	rm.collectionsManifestSvc = collectionsManifestSvc

	fac := factory.NewXDCRFactory(repl_spec_svc, remote_cluster_svc, cluster_info_svc, xdcr_topology_svc,
		checkpoint_svc, capi_svc, uilog_svc, bucket_settings_svc, throughput_throttler_svc,
		log.DefaultLoggerContext, log.DefaultLoggerContext, rm, rm.utils, collectionsManifestSvc)

	rm.pipelineMgr = pipeline_manager.NewPipelineManager(fac, repl_spec_svc, xdcr_topology_svc, remote_cluster_svc, cluster_info_svc, checkpoint_svc, uilog_svc, log.DefaultLoggerContext, rm.utils)

	rm.resourceMgr = resource_manager.NewResourceManager(rm.pipelineMgr, repl_spec_svc, xdcr_topology_svc, remote_cluster_svc, cluster_info_svc, checkpoint_svc, uilog_svc, throughput_throttler_svc, log.DefaultLoggerContext, rm.utils)
	rm.resourceMgr.Start()

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
func CreateReplication(justValidate bool, sourceBucket, targetCluster, targetBucket string, settings metadata.ReplicationSettingsMap, realUserId *service_def.RealUserId) (string, map[string]error, error, []string) {
	logger_rm.Infof("Creating replication - justValidate=%v, sourceBucket=%s, targetCluster=%s, targetBucket=%s, settings=%v\n",
		justValidate, sourceBucket, targetCluster, targetBucket, settings.CloneAndRedact())

	var spec *metadata.ReplicationSpecification
	spec, errorsMap, err, warnings := replication_mgr.createAndPersistReplicationSpec(justValidate, sourceBucket, targetCluster, targetBucket, settings)
	if err != nil {
		logger_rm.Errorf("%v\n", err)
		return "", nil, err, nil
	} else if len(errorsMap) != 0 {
		return "", errorsMap, nil, nil
	}

	if justValidate {
		return spec.Id, nil, nil, warnings
	}

	go writeCreateReplicationEvent(spec, realUserId)

	logger_rm.Infof("Replication specification %s is created\n", spec.Id)

	return spec.Id, nil, nil, warnings
}

//DeleteReplication stops the running replication of given replicationId and
//delete the replication specification from the metadata store
func DeleteReplication(topic string, realUserId *service_def.RealUserId) error {
	logger_rm.Infof("Deleting replication %s\n", topic)

	// delete replication spec
	spec, err := ReplicationSpecService().DelReplicationSpec(topic)
	if err == nil {
		logger_rm.Infof("Replication specification %s is deleted\n", topic)
	} else {
		logger_rm.Errorf("%v\n", err)
		return err
	}

	go writeGenericReplicationEvent(service_def.CancelReplicationEventId, spec, realUserId)

	logger_rm.Infof("Pipeline %s is deleted\n", topic)

	return nil
}

//update the  replication settings and XDCR process setting
func UpdateDefaultSettings(settings metadata.ReplicationSettingsMap, realUserId *service_def.RealUserId) (map[string]error, error) {
	logger_rm.Infof("UpdateDefaultSettings called with settings=%v\n", settings)

	// Validate process setting keys
	globalSettingsMap := metadata.ValidateGlobalSettingsKey(settings)
	if len(globalSettingsMap) > 0 {
		//First update XDCR Process specific setting
		errorMap, err := GlobalSettingsService().UpdateGlobalSettings(globalSettingsMap)
		if len(errorMap) > 0 || err != nil {
			return errorMap, err
		}
		logger_rm.Infof("Updated global settings\n")
	} else {
		logger_rm.Infof("Did not update global settings since there are no real changes\n")
	}

	//validate replication settings
	replicationSettingMap := metadata.ValidateReplicationSettingsKey(settings)
	if len(replicationSettingMap) > 0 {
		//Now update default replication setting
		changedSettingsMap, errorMap, err := ReplicationSettingsService().UpdateDefaultReplicationSettings(replicationSettingMap)
		if len(errorMap) > 0 || err != nil {
			return errorMap, err
		}
		if len(changedSettingsMap) != 0 {
			go writeUpdateDefaultReplicationSettingsEvent(&changedSettingsMap, realUserId)
		}
		logger_rm.Infof("Updated default replication settings\n")
	} else {
		logger_rm.Infof("Did not update default replication settings since there are no real changes\n")
	}

	return nil, nil
}

func compressionSettingsChanged(changedSettingsMap metadata.ReplicationSettingsMap, oldCompressionType int) bool {
	if compressionType, ok := changedSettingsMap[metadata.CompressionTypeKey]; ok && (base.GetCompressionType(compressionType.(int)) != base.CompressionTypeNone) &&
		base.GetCompressionType(oldCompressionType) != base.GetCompressionType(compressionType.(int)) {
		return true
	}
	return false
}

func filterSettingsChanged(changedSettingsMap metadata.ReplicationSettingsMap, oldFilterExpression string) bool {
	if newFilterExpression, ok := changedSettingsMap[metadata.FilterExpressionKey]; ok {
		if newFilterExpression != oldFilterExpression {
			return true
		}
	}
	return false
}

//update the per-replication settings
func UpdateReplicationSettings(topic string, settings metadata.ReplicationSettingsMap, realUserId *service_def.RealUserId) (map[string]error, error) {
	logger_rm.Infof("Update replication settings for %v, settings=%v\n", topic, settings.CloneAndRedact())

	var internalChangesTookPlace bool

	// read replication spec with the specified replication id
	replSpec, err := ReplicationSpecService().ReplicationSpec(topic)
	if err != nil {
		return nil, err
	}

	// Validate the spec once more with the new settings
	replSpecificFields, replSpecificErr := constructReplicationSpecificFieldsFromSpec(replSpec)
	if replSpecificErr != nil {
		return nil, err
	}

	// Save some old values that we may need
	filterExpression := replSpec.Settings.Values[metadata.FilterExpressionKey].(string)
	oldCompressionType := replSpec.Settings.Values[metadata.CompressionTypeKey].(int)
	filterVersion := replSpec.Settings.Values[metadata.FilterVersionKey].(base.FilterVersionType)

	// update replication spec with input settings
	changedSettingsMap, errorMap := replSpec.Settings.UpdateSettingsFromMap(settings)

	// Only Re-evaluate Compression pre-requisites if it is turned on and actually switched algorithms to catch any cluster-wide compression changes
	compressionType, CompressionOk := changedSettingsMap[metadata.CompressionTypeKey]
	if CompressionOk && (base.GetCompressionType(compressionType.(int)) != base.CompressionTypeNone) &&
		base.GetCompressionType(oldCompressionType) != base.GetCompressionType(compressionType.(int)) {
		validateRoutineErrorMap, validateErr := ReplicationSpecService().ValidateReplicationSettings(replSpecificFields.SourceBucketName,
			replSpecificFields.RemoteClusterName, replSpecificFields.TargetBucketName, settings)
		if len(validateRoutineErrorMap) > 0 {
			return validateRoutineErrorMap, nil
		} else if validateErr != nil {
			return nil, validateErr
		}
	}
	// If compression is SNAPPY and is not changed, take this oppurtunity to change it to AUTO
	if oldCompressionType == base.CompressionTypeSnappy && !CompressionOk {
		changedSettingsMap[metadata.CompressionTypeKey] = base.CompressionTypeAuto
	}
	if len(errorMap) != 0 {
		return errorMap, nil
	}

	// If nonfilter-settings invoked this change, take this opportunity to fix stale infos if there is an existing expression present
	if !filterSettingsChanged(changedSettingsMap, filterExpression) && len(filterExpression) > 0 && filterVersion < base.FilterVersionAdvanced {
		settings[metadata.FilterVersionKey] = base.FilterVersionAdvanced

		_, errorMap = replSpec.Settings.UpdateSettingsFromMap(settings)
		if len(errorMap) != 0 {
			return errorMap, fmt.Errorf("Internal XDCR Error related to internal filter management: %v", errorMap)
		}
		internalChangesTookPlace = true
	}

	if len(changedSettingsMap) != 0 {
		err = ReplicationSpecService().SetReplicationSpec(replSpec)
		if err != nil {
			return nil, err
		}
		logger_rm.Infof("Updated replication settings for replication %v\n", topic)

		go writeUpdateReplicationSettingsEvent(replSpec, &changedSettingsMap, realUserId)

		// if the active flag has been changed, log Pause/ResumeReplication event
		active, ok := changedSettingsMap[metadata.ActiveKey]
		if ok {
			if active.(bool) {
				go writeGenericReplicationEvent(service_def.ResumeReplicationEventId, replSpec, realUserId)
			} else {
				go writeGenericReplicationEvent(service_def.PauseReplicationEventId, replSpec, realUserId)
			}
		}
		logger_rm.Infof("Done with replication settings auditing for replication %v\n", topic)

	} else if internalChangesTookPlace {
		err = ReplicationSpecService().SetReplicationSpec(replSpec)
		if err != nil {
			return nil, err
		}
		logger_rm.Infof("Internally updated replication settings for replication %v\n", topic)

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
func (rm *replicationManager) createAndPersistReplicationSpec(justValidate bool, sourceBucket, targetCluster, targetBucket string, settings metadata.ReplicationSettingsMap) (*metadata.ReplicationSpecification, map[string]error, error, []string) {
	logger_rm.Infof("Creating replication spec - justValidate=%v, sourceBucket=%s, targetCluster=%s, targetBucket=%s, settings=%v\n",
		justValidate, sourceBucket, targetCluster, targetBucket, settings.CloneAndRedact())
	// validate that everything is alright with the replication configuration before actually creating it
	sourceBucketUUID, targetBucketUUID, targetClusterRef, errorMap, err, warnings := replication_mgr.repl_spec_svc.ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket, settings)
	if err != nil || len(errorMap) > 0 {
		return nil, errorMap, err, nil
	}

	spec, err := metadata.NewReplicationSpecification(sourceBucket, sourceBucketUUID, targetClusterRef.Uuid(), targetBucket, targetBucketUUID)
	if err != nil {
		return nil, nil, err, nil
	}

	replSettings, err := ReplicationSettingsService().GetDefaultReplicationSettings()
	if err != nil {
		return nil, nil, err, nil
	}

	// default isCapi to false if replication type is not explicitly specified in settings
	isCapi := false
	for key, value := range settings {
		if key == metadata.ReplicationTypeKey {
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
		return nil, errorMap, nil, nil
	}
	spec.Settings = replSettings

	if justValidate {
		return spec, nil, nil, warnings
	}

	//persist it
	err = replication_mgr.repl_spec_svc.AddReplicationSpec(spec, base.FlattenStringArray(warnings))
	if err == nil {
		logger_rm.Infof("Success adding replication specification %s\n", spec.Id)
		return spec, nil, nil, warnings
	} else {
		logger_rm.Errorf("Error adding replication specification %s. err=%v\n", spec.Id, err)
		return nil, nil, err, nil
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

func processErrorMsgForUI(errStr string) string {
	// For pipeline start timeouts, return a more friendly error msg
	if (strings.Contains(errStr, pipeline.PipelineContextStart) || strings.Contains(errStr, pipeline.PipelinePartStart)) &&
		strings.Contains(errStr, base.ErrorExecutionTimedOut.Error()) {
		errStr = base.ErrorPipelineStartTimedOutUI.Error()
	}

	//prepend current node name, if not empty, to the error message to make it more helpful
	cur_node, err := XDCRCompTopologyService().MyHost()
	if err != nil {
		cur_node = ""
	}
	if cur_node != "" {
		errStr = cur_node + ":" + errStr
	}
	return errStr
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
				for _, pipeline_error := range errs {
					if !bypassUIErrorCodes(pipeline_error.ErrMsg) {
						err_msg := processErrorMsgForUI(pipeline_error.ErrMsg)
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
		// the errors came from the replication manager supervisor because adminport is not longer alive.
		// there is nothing we can do except to abort xdcr. ns_server will restart xdcr while later
		ExitProcess(false)
	} else {
		// the errors came from a pipeline supervisor because some parts are not longer alive.
		pipeline, err := rm.getPipelineFromPipelineSupevisor(s)
		if err == nil {
			// try to fix the pipeline
			var errMsg string
			if len(errMap) == 0 {
				// errMap is empty, which should not have happened. use predefined error message
				logger_rm.Warnf("Supervisor %v of type %v reported empty error map\n", s.Id(), reflect.TypeOf(s))
				errMsg = "pipeline failed"
			} else {
				errMsg = base.FlattenErrorMap(errMap)
			}
			// NOTE because we flatten the error map here, any error checked should not be exact match
			rm.pipelineMgr.UpdatePipeline(pipeline.Topic(), errors.New(errMsg))
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
			ExitProcess(false)
		}
		if err != nil {
			logger_rm.Errorf("Unexpected error polling stdin: %v\n", err)
			ExitProcess(true)
		}
		if ch == '\n' || ch == '\r' {
			logger_rm.Infof("Received EOL; Exiting...")
			ExitProcess(false)
		}
	}
}

// periodically log mem stats to facilitate debugging of memory issues
func logMemStats(fin_chan chan bool) {
	logger_rm.Infof("logMemStats started.")
	defer logger_rm.Infof("logMemStats exited")

	mem_stats_ticker := time.NewTicker(base.MemStatsLogInterval)
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

//gracefull stop
func cleanup() {
	if replication_mgr.running {

		replication_mgr.running = false

		//stop the generic supervisor
		replication_mgr.Stop()

		// stop listening to spec changed events
		replication_mgr.metadata_change_callback_cancel_ch <- struct{}{}
		logger_rm.Infof("Sent cancel signal to metadata change listeners")

		// kill adminport to stop receiving new requests
		close(replication_mgr.adminport_finch)

		base.ExecWithTimeout(replication_mgr.pipelineMgr.OnExit, 1*time.Second, logger_rm)

		close(replication_mgr.status_logger_finch)
		close(replication_mgr.mem_stats_logger_finch)

		replication_mgr.resourceMgr.Stop()

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
func ExitProcess(byForce bool) {
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

func writeGenericReplicationEvent(eventId uint32, spec *metadata.ReplicationSpecification, realUserId *service_def.RealUserId) {
	event, err := constructGenericReplicationEvent(spec, realUserId)
	if err == nil {
		err = AuditService().Write(eventId, event)
	}

	logAuditErrors(err)
}

func writeCreateReplicationEvent(spec *metadata.ReplicationSpecification, realUserId *service_def.RealUserId) {
	genericReplicationEvent, err := constructGenericReplicationEvent(spec, realUserId)
	if err == nil {
		createReplicationEvent := &service_def.CreateReplicationEvent{
			GenericReplicationEvent: *genericReplicationEvent,
			FilterExpression:        spec.Settings.FilterExpression}

		err = AuditService().Write(service_def.CreateReplicationEventId, createReplicationEvent)
	}

	logAuditErrors(err)
}

func writeUpdateDefaultReplicationSettingsEvent(changedSettingsMap *metadata.ReplicationSettingsMap, realUserId *service_def.RealUserId) {
	event, err := constructUpdateDefaultReplicationSettingsEvent(changedSettingsMap, realUserId)
	if err == nil {
		err = AuditService().Write(service_def.UpdateDefaultReplicationSettingsEventId, event)
	}
	logAuditErrors(err)
}

func writeUpdateReplicationSettingsEvent(spec *metadata.ReplicationSpecification, changedSettingsMap *metadata.ReplicationSettingsMap, realUserId *service_def.RealUserId) {
	replicationSpecificFields, err := constructReplicationSpecificFieldsFromSpec(spec)
	if err == nil {
		var updateDefaultReplicationSettingsEvent *service_def.UpdateDefaultReplicationSettingsEvent
		updateDefaultReplicationSettingsEvent, err = constructUpdateDefaultReplicationSettingsEvent(changedSettingsMap, realUserId)
		if err == nil {
			updateReplicationSettingsEvent := &service_def.UpdateReplicationSettingsEvent{
				ReplicationSpecificFields:             *replicationSpecificFields,
				UpdateDefaultReplicationSettingsEvent: *updateDefaultReplicationSettingsEvent}
			err = AuditService().Write(service_def.UpdateReplicationSettingsEventId, updateReplicationSettingsEvent)
		}
	}
	logAuditErrors(err)
}

func writeUpdateBucketSettingsEvent(bucketName string, lwwEnabled bool, realUserId *service_def.RealUserId) {
	updateBucketSettingsEvent := constructUpdateBucketSettingsEvent(bucketName, lwwEnabled, realUserId)
	err := AuditService().Write(service_def.UpdateBucketSettingsEventId, updateBucketSettingsEvent)
	logAuditErrors(err)
}

func constructGenericReplicationFields(realUserId *service_def.RealUserId) (*service_def.GenericReplicationFields, error) {
	localClusterName, err := XDCRCompTopologyService().MyHostAddr()
	if err != nil {
		return nil, err
	}

	return &service_def.GenericReplicationFields{
		GenericFields:    service_def.GenericFields{log.FormatTimeWithMilliSecondPrecision(time.Now()), *realUserId},
		LocalClusterName: localClusterName}, nil
}

func constructReplicationSpecificFieldsFromSpec(spec *metadata.ReplicationSpecification) (*service_def.ReplicationSpecificFields, error) {
	remoteClusterName := RemoteClusterService().GetRemoteClusterNameFromClusterUuid(spec.TargetClusterUUID)

	return &service_def.ReplicationSpecificFields{
		SourceBucketName:  spec.SourceBucketName,
		RemoteClusterName: remoteClusterName,
		TargetBucketName:  spec.TargetBucketName}, nil
}

func constructGenericReplicationEvent(spec *metadata.ReplicationSpecification, realUserId *service_def.RealUserId) (*service_def.GenericReplicationEvent, error) {
	genericReplicationFields, err := constructGenericReplicationFields(realUserId)
	if err != nil {
		return nil, err
	}

	replicationSpecificFields, err := constructReplicationSpecificFieldsFromSpec(spec)
	if err != nil {
		return nil, err
	}

	return &service_def.GenericReplicationEvent{
		GenericReplicationFields:  *genericReplicationFields,
		ReplicationSpecificFields: *replicationSpecificFields}, nil
}

func constructUpdateDefaultReplicationSettingsEvent(changedSettingsMap *metadata.ReplicationSettingsMap, realUserId *service_def.RealUserId) (*service_def.UpdateDefaultReplicationSettingsEvent, error) {
	logger_rm.Info("Start constructUpdateDefaultReplicationSettingsEvent....")
	genericReplicationFields, err := constructGenericReplicationFields(realUserId)
	if err != nil {
		return nil, err
	}

	// convert keys in changedSettingsMap from internal metadata keys to external facing rest api keys
	convertedSettingsMap := make(map[string]interface{})
	for key, value := range *changedSettingsMap {
		if key == metadata.ActiveKey {
			convertedSettingsMap[SettingsKeyToRestKeyMap[key]] = !(value.(bool))
		} else {
			convertedSettingsMap[SettingsKeyToRestKeyMap[key]] = value
		}
	}
	logger_rm.Info("Done constructUpdateDefaultReplicationSettingsEvent....")

	return &service_def.UpdateDefaultReplicationSettingsEvent{
		GenericReplicationFields: *genericReplicationFields,
		UpdatedSettings:          convertedSettingsMap}, nil
}

func constructUpdateBucketSettingsEvent(bucketName string, lwwEnabled bool, realUserId *service_def.RealUserId) *service_def.UpdateBucketSettingsEvent {
	logger_rm.Info("Start constructUpdateBucketSettingsEvent....")

	settingsMap := make(map[string]interface{})
	settingsMap[LWWEnabled] = lwwEnabled
	logger_rm.Info("Done constructUpdateBucketSettingsEvent....")

	return &service_def.UpdateBucketSettingsEvent{
		GenericFields:   service_def.GenericFields{log.FormatTimeWithMilliSecondPrecision(time.Now()), *realUserId},
		UpdatedSettings: settingsMap}

}

func logAuditErrors(err error) {
	if err != nil {
		err = replication_mgr.utils.NewEnhancedError(service_def.ErrorWritingAudit, err)
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

func setBucketSettings(bucketName string, lwwEnabled bool, realUserId *service_def.RealUserId) (map[string]interface{}, error) {
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
	remoteClusterRefs, err := remoteClusterSvc.RemoteClusters()
	if err != nil {
		logger_rm.Warnf("Skipping upgradeRemoteClusterRefs because of err =%v", err)
		return
	}
	for _, remoteClusterRef := range remoteClusterRefs {
		if remoteClusterRef.IsEncryptionEnabled() && len(remoteClusterRef.EncryptionType()) == 0 {
			remoteClusterRef.SetEncryptionType(metadata.EncryptionType_Full)
			err = remoteClusterSvc.SetRemoteCluster(remoteClusterRef.Name(), remoteClusterRef)
			if err != nil {
				logger_rm.Warnf("Skipping upgrading remote cluster ref %v because of err =%v", remoteClusterRef.Name(), err)
				continue
			} else {
				logger_rm.Infof("Successfully upgraded remote cluster ref %v", remoteClusterRef.Name())
			}
		}
	}
}
