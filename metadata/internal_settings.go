/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadata

import (
	"math"
	"time"

	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
)

var logger_is *log.CommonLogger = log.NewLogger("InternalSetting", log.DefaultLoggerContext)

const (
	InternalSettingsKey = "InternalSettings"

	// Following are keys representing valid internal settings

	// interval between topology checks (in seconds)
	TopologyChangeCheckIntervalKey = "TopologyChangeCheckInterval"
	// the maximum number of topology change checks to wait before pipeline is restarted
	MaxTopologyChangeCountBeforeRestartKey = "MaxTopologyChangeCountBeforeRestart"
	// the maximum number of consecutive stable topology seen before pipeline is restarted
	MaxTopologyStableCountBeforeRestartKey = "MaxTopologyStableCountBeforeRestart"
	// the max number of concurrent workers for checkpointing
	MaxWorkersForCheckpointingKey = "MaxWorkersForCheckpointing"
	// timeout for checkpointing attempt before pipeline is stopped (in seconds) -
	// to put an upper bound on the delay of pipeline stop/restart
	TimeoutCheckpointBeforeStopKey = "TimeoutCheckpointBeforeStop"
	// capi nozzle data chan size is defined as batchCount*CapiDataChanSizeMultiplier
	CapiDataChanSizeMultiplierKey = "CapiDataChanSizeMultiplier"
	// interval for refreshing remote cluster references
	RefreshRemoteClusterRefIntervalKey = "RefreshRemoteClusterRefInterval"
	// max retry for capi batchUpdateDocs operation
	CapiMaxRetryBatchUpdateDocsKey = "CapiMaxRetryBatchUpdateDocs"
	// timeout for batch processing in capi
	// 1. http timeout in revs_diff, i.e., batchGetMeta, call to target
	// 2. overall timeout for batchUpdateDocs operation
	CapiBatchTimeoutKey = "CapiBatchTimeout"
	// timeout for tcp write operation in capi
	CapiWriteTimeoutKey = "CapiWriteTimeout"
	// timeout for tcp read operation in capi
	CapiReadTimeoutKey = "CapiReadTimeout"
	// the maximum number of checkpoint records to write/keep in the checkpoint doc
	MaxCheckpointRecordsToKeepKey = "MaxCheckpointRecordsToKeep"
	// the maximum number of checkpoint records to read from the checkpoint doc
	MaxCheckpointRecordsToReadKey = "MaxCheckpointRecordsToRead"
	// default time out for outgoing http requests if it is not explicitly specified (seconds)
	DefaultHttpTimeoutKey = "DefaultHttpTimeout"
	// when we need to make a rest call when processing a XDCR rest request, the time out of the second rest call needs
	// to be shorter than that of the first one, which is currently 30 seconds. (seconds)
	ShortHttpTimeoutKey = "ShortHttpTimeout"
	// max retry for live updating of pipelines
	MaxRetryForLiveUpdatePipelineKey = "MaxRetryForLiveUpdatePipeline"
	// wait time between retries for live updating of pipelines (milliseconds)
	WaitTimeForLiveUpdatePipelineKey = "WaitTimeForLiveUpdatePipeline"
	// interval for replication spec validity check (seconds)
	ReplSpecCheckIntervalKey = "ReplSpecCheckInterval"
	// interval for mem stats logging (seconds)
	MemStatsLogIntervalKey = "MemStatsLogInterval"
	// max number of retries for metakv ops
	MaxNumOfMetakvRetriesKey = "MaxNumOfMetakvRetries"
	// interval between metakv retries
	RetryIntervalMetakvKey = "RetryIntervalMetakv"

	// In order for dcp flow control to work correctly, the number of mutations in dcp buffer
	// should be no larger than the size of the dcp data channel.
	// This way we can ensure that gomemcached is never blocked on writing to data channel,
	// and thus can always respond to dcp commands such as NOOP
	// In other words, the following three parameters should be selected such that
	// MinimumMutationSize * UprFeedDataChanLength >= UprFeedBufferSize
	// where MinimumMutationSize is the minimum size of a SetMeta/DelMeta mutation,
	// a DCP mutation has size 54 + key + body. 60 should be a safe value to use

	// length of data channel between dcp nozzle and gomemcached
	UprFeedDataChanLengthKey = "UprFeedDataChanLength"
	// dcp flow control buffer size (number of bytes)
	UprFeedBufferSizeKey = "UprFeedBufferSize"

	// max retry for xmem operations like batch send, resend, etc.
	XmemMaxRetryKey = "XmemMaxRetry"
	// xmem write time out for writing to network connection (seconds)
	XmemWriteTimeoutKey = "XmemWriteTimeout"
	// xmem read time out when reading from network connection (seconds)
	XmemReadTimeoutKey = "XmemReadTimeout"
	// network connection will be repaired if its down time (the time that it receives
	// continuous network error responses from read or write) exceeds max down time (seconds)
	XmemMaxReadDownTimeKey = "XmemMaxReadDownTime"
	//wait time between writes is backoff_factor*XmemBackoffWaitTime (milliseconds)
	XmemBackoffWaitTimeKey = "XmemBackoffWaitTime"
	//max backoff factor
	XmemMaxBackoffFactorKey = "XmemMaxBackoffFactor"
	// max retry for new xmem connection
	XmemMaxRetryNewConnKey = "XmemMaxRetryNewConn"
	// initial backoff time between retries for new xmem connection (milliseconds)
	XmemBackoffTimeNewConnKey = "XmemBackoffTimeNewConn"
	// interval for xmem self monitoring (seconds)
	XmemSelfMonitorIntervalKey = "XmemSelfMonitorInterval"
	// initial max idle count;
	// it is dynamically adjusted at runtime by factor = actual response wait time / previous response wait time
	// if xmem idle count exceeds this max, it will be declared to be stuck
	XmemMaxIdleCountKey = "XmemMaxIdleCount"
	// lower bound for xmem max idle count
	XmemMaxIdleCountLowerBoundKey = "XmemMaxIdleCountLowerBound"
	// upper bound for xmem max idle count
	XmemMaxIdleCountUpperBoundKey = "XmemMaxIdleCountUpperBound"
	//the maximum amount of data (in bytes) xmem data channel can hold
	XmemMaxDataChanSizeKey = "XmemMaxDataChanSize"
	// max batch size that can be sent in one writeToClient() op
	XmemMaxBatchSizeKey = "XmemMaxBatchSize"
	// interval between retries on batchUpdateDocs
	CapiRetryIntervalKey = "CapiRetryInterval"
	// maximum number of snapshot markers to store for each vb
	// once the maximum is reached, the oldest snapshot marker is dropped to make room for the new one
	MaxLengthSnapshotHistoryKey = "MaxLengthSnapshotHistory"
	// max retry for target stats retrieval.
	MaxRetryTargetStatsKey = "MaxRetryTargetStats"
	// base wait time between retries for target stats retrieval (milliseconds)
	RetryIntervalTargetStatsKey = "RetryIntervalTargetStats"
	// number of time slots [in one second] to track for bandwidth throttling computation
	NumberOfSlotsForBandwidthThrottlingKey = "NumberOfSlotsForBandwidthThrottling"
	// When doing bandwith throttling in xmem, set minNumberOfBytes = TotalNumberOfBytes * PercentageOfBytesToSendAsMin / 100
	PercentageOfBytesToSendAsMinKey = "PercentageOfBytesToSendAsMin"
	// write time out for audit service (seconds)
	AuditWriteTimeoutKey = "AuditWriteTimeout"
	// read time out for audit service (seconds)
	AuditReadTimeoutKey = "AuditReadTimeout"
	// number of retries for CAPI calls, e.g., pre_replicate and commit_for_checkpoint
	MaxRetryCapiServiceKey = "MaxRetryCapiService"
	// max number of async listeners [for an event type]
	MaxNumberOfAsyncListenersKey = "MaxNumberOfAsyncListeners"
	//max interval between retries when resending docs  (seconds)
	XmemMaxRetryIntervalKey = "XmemMaxRetryInterval"
	// max retry for xmem resend operation on mutation locked error
	XmemMaxRetryMutationLockedKey = "XmemMaxRetryMutationLocked"
	//max interval between retries when resending docs on mutation locked errors  (seconds)
	// the upper limit on lock period is as of now 30 seconds
	XmemMaxRetryIntervalMutationLockedKey = "XmemMaxRetryIntervalMutationLocked"
	// read/write timeout for helo command to memcached (seconds)
	HELOTimeoutKey = "HELOTimeout"
	// wait time between metadata change listeners (milliseconds)
	WaitTimeBetweenMetadataChangeListenersKey = "WaitTimeBetweenMetadataChangeListeners"
	// Keep alive period for tcp connections (seconds)
	KeepAlivePeriodKey = "KeepAlivePeriod"
	// actual size of data chan is logged when it exceeds ThresholdPercentageForEventChanSizeLogging * EventChanSize
	ThresholdPercentageForEventChanSizeLoggingKey = "ThresholdPercentageForEventChanSizeLogging"
	// if through seqno computation takes longer than the threshold, it will be logged (milliseconds)
	ThresholdForThroughSeqnoComputationKey = "ThresholdForThroughSeqnoComputation"
	// interval for printing replication runtime stats to log file (seconds)
	StatsLogIntervalKey = "StatsLogInterval"
	// default resp timeout, which is used as the interval for checkAndRepaitBufferMonitor (milliseconds)
	XmemDefaultRespTimeoutKey = "XmemDefaultRespTimeout"
	// when set to 1, bypass san in certificate check in ssl connections
	BypassSanInCertificateCheckKey = "BypassSanInCertificateCheck"
	// Number of times to verify bucket is missing before removing an invalid replicationSpec
	ReplicationSpecGCCntKey = "ReplicationSpecGCCnt"
	// Cooldown period for topology service before hitting ns_server REST endpoint again
	TopologySvcCooldownPeriodKey    = "TopologySvcCooldownPeriodSec"
	TopologySvcErrCooldownPeriodKey = "TopologySvcErrCooldownPeriodSec"

	TimeoutRuntimeContextStartKey = "TimeoutRuntimeContextStart"
	TimeoutRuntimeContextStopKey  = "TimeoutRuntimeContextStop"
	TimeoutPartsStartKey          = "TimeoutPartsStart"
	TimeoutPartsStopKey           = "TimeoutPartsStop"
	TimeoutDcpCloseUprStreamsKey  = "TimeoutDcpCloseUprStreams"
	TimeoutDcpCloseUprFeedKey     = "TimeoutDcpCloseUprFeed"
	TimeoutHttpsPortLookupKey     = "TimeoutHttpsPortLookup"

	/* --Resource menagement related settings ---*/

	// interval for cpu stats collection
	CpuCollectionIntervalKey = "CpuCollectionInterval"
	// interval for resource management actions
	ResourceManagementIntervalKey = "ResourceManagementInterval"
	// interval for logging resource management stats
	ResourceManagementStatsIntervalKey = "ResourceManagementStatsInterval"
	// once changes_left becomes smaller than this threshold, replication will be classified as ongoing replication
	ChangesLeftThresholdForOngoingReplicationKey = "ChangesLeftThresholdForOngoingReplication"
	// all the ratio related constants are defined as multiples of 1/ResourceManagementRatioBase
	// so that we can do integer arithmetic
	ResourceManagementRatioBaseKey = "ResourceManagementRatioBase"
	// upper bound for ratio of throughput of high priority replications to throughput of all replications
	// this is to ensure that low priority replications will not be completely starved
	ResourceManagementRatioUpperBoundKey = "ResourceManagementRatioUpperBound"
	// when the number of consecutive terms where there have been backlog reaches the threshold, set DCP priorities
	MaxCountBacklogForSetDcpPriorityKey = "MaxCountBacklogForSetDcpPriority"
	// when the number of consecutive terms where there have been no backlog reaches the threshold, reset DCP priorities to normal
	MaxCountNoBacklogForResetDcpPriorityKey = "MaxCountNoBacklogForResetDcpPriority"
	// extra quota given to replications when cpu is not yet maximized
	// max throughput is computed as currentThroughput * (1 + ExtraQuotaForUnderutilizedCPU/ResourceManagementRatioBase)
	ExtraQuotaForUnderutilizedCPUKey = "ExtraQuotaForUnderutilizedCPU"
	// interval for printing throughput throttler stats to log file
	ThroughputThrottlerLogIntervalKey = "ThroughputThrottlerLogInterval"
	// interval for clearing tokens in throughput throttler
	ThroughputThrottlerClearTokensIntervalKey = "ThroughputThrottlerClearTokensInterval"
	// number of time slots [per measurement interval] for throughput throttling
	NumberOfSlotsForThroughputThrottlingKey = "NumberOfSlotsForThroughputThrottling"
	// interval for throttler calibration, i.e., for stopping reassigning tokens to low priority replications
	IntervalForThrottlerCalibrationKey = "IntervalForThrottlerCalibration"
	// number of throughput samples to keep
	ThroughputSampleSizeKey = "ThroughputSampleSize"
	// alpha for exponential decay sampling
	ThroughputSampleAlphaKey = "ThroughputSampleAlpha"
	// when actual process cpu usage exceeds maxProcessCpu * ThresholdRatioForProcessCpu/100, process cpu is considered to have maxed out
	ThresholdRatioForProcessCpuKey = "ThresholdRatioForProcessCpu"
	// when actual total cpu usage exceeds totalCpu * ThresholdRatioForTotalCpu/100, total cpu is considered to have maxed out
	ThresholdRatioForTotalCpuKey = "ThresholdRatioForTotalCpu"
	// max count of consecutive terms where cpu has not been maxed out
	MaxCountCpuNotMaxedKey = "MaxCountCpuNotMaxed"
	// max count of consecutive terms where throughput dropped from previous high
	MaxCountThroughputDropKey = "MaxCountThroughputDrop"
	// When resource mgr starts up until a cluster is formed, how often it should try to check for KV service
	ResourceMgrKVDetectionRetryIntervalKey = "ResourceMgrKVDetectionRetryInterval"
	/* --End Resource menagement related settings ---*/

	// Internal keys to wrap around incoming document's key or xattributes for advanced filtering
	FilteringInternalKey   = "FilteringInternalKeyKey"
	FilteringInternalXattr = "FilteringInternalXattrKey"

	RemoteClusterAlternateAddrChangeKey = "RemoteClusterAlternateAddrChangeKey"

	// How often to pull manifests
	ManifestRefreshSrcIntervalKey = "ManifestRefreshSrcInterval"
	ManifestRefreshTgtIntervalKey = "ManifestRefreshTgtInterval"

	BackfillPersistIntervalKey = "BackfillPersistInterval"

	// Conflict Resolver related setting
	JSEngineWorkersKey = "JsNumWorkers"
	JSWorkerQuotaKey   = "JsWorkerMemQuotaBytes"

	MaxCountDCPStreamsInactiveKey = "MaxCountDCPStreamsInactive"

	UtilsStopwatchDiagInternalThresholdKey = "StopwatchDiagInternalThresholdMs"
	UtilsStopwatchDiagExternalThresholdKey = "StopwatchDiagExternalThresholdMs"

	ReplStatusExportBrokenMapTimeoutKey = "ReplStatusExportBrokenMapTimeoutSec"
	ReplStatusLoadBrokenMapTimeoutKey   = "ReplStatusLoadBrokenMapTimeoutSec"

	BucketTopologyGCScanTimeKey  = "BucketTopologyGCScanTimeMin"
	BucketTopologyGCPruneTimeKey = "BucketTopologyGCPruneTimeHour"

	P2PCommTimeoutKey                 = "P2PCommTimeoutSec"
	P2PMaxReceiveChLenKey             = "P2PMaxReceiveChLen"
	P2POpaqueTimeoutKey               = "P2POpaqueTimeoutMin"
	P2POpaqueCleanupIntervalKey       = "P2POpaqueCleanupIntervalSec"
	P2PVBRelatedGCIntervalKey         = "P2PVBRelatedGCIntervalHour"
	P2PReplicaReplicatorReloadSizeKey = "P2PReplicaReplicatorReloadSize"
	P2PRetryWaitTimeMilliSecKey       = "P2PRetryWaitTimeMillisec"
	P2PRetryFactorKey                 = "P2PRetryFactor"

	ThroughSeqnoBgScannerFreqKey    = "ThroughSeqnoBgScannerFreqSec"
	ThroughSeqnoBgScannerLogFreqKey = "ThroughSeqnoBgScannerLogFreqSec"

	PipelineTimeoutP2PProtocolKey = "PipelineTimeoutP2PProtocolSec"

	CkptCacheCtrlChLenKey = "CkptCacheCtrlChLen"
	CkptCacheReqChLenKey  = "CkptCacheReqChLen"

	HumanRecoveryThresholdKey = "humanRecoverySec"

	DnsSrvReBootstrapKey = "dnsSrvReBootstrap"

	ConnectionPreCheckGCTimeoutKey  = "ConnectionPreCheckGCTimeoutSec"
	ConnectionPreCheckRPCTimeoutKey = "ConnectionPreCheckRPCTimeoutSec"

	GlobalOSOConfigKey = "OSOModeOverride"

	ConnErrorsListMaxEntriesKey = "ConnectionErrorsListMaxEntries"

	PeerManifestsGetterSleepTimeKey = "PeerManifestsGetterSleepTimeSec"
	PeerManifestsGetterMaxRetryKey  = "PeersManifestsGetterMaxRetry"

	DatapoolLogFrequencyKey = "DatapoolLogFrequency"

	CapellaHostNameSuffixKey = "CapellaHostNameSuffix"

	NWLatencyToleranceMilliSecKey = "NWLatencyToleranceMilliSec"
)

var TopologyChangeCheckIntervalConfig = &SettingsConfig{10, &Range{1, 100}}
var MaxTopologyChangeCountBeforeRestartConfig = &SettingsConfig{30, &Range{1, 300}}
var MaxTopologyStableCountBeforeRestartConfig = &SettingsConfig{30, &Range{1, 300}}
var MaxWorkersForCheckpointingConfig = &SettingsConfig{5, &Range{1, 1000}}
var TimeoutCheckpointBeforeStopConfig = &SettingsConfig{180, &Range{10, 1800}}
var CapiDataChanSizeMultiplierConfig = &SettingsConfig{1, &Range{1, 100}}
var RefreshRemoteClusterRefIntervalConfig = &SettingsConfig{15, &Range{1, 3600}}
var CapiMaxRetryBatchUpdateDocsConfig = &SettingsConfig{6, &Range{1, 100}}
var CapiBatchTimeoutConfig = &SettingsConfig{180, &Range{10, 3600}}
var CapiWriteTimeoutConfig = &SettingsConfig{10, &Range{1, 3600}}
var CapiReadTimeoutConfig = &SettingsConfig{60, &Range{10, 3600}}
var MaxCheckpointRecordsToKeepConfig = &SettingsConfig{5, &Range{1, 100}}
var MaxCheckpointRecordsToReadConfig = &SettingsConfig{5, &Range{1, 100}}
var DefaultHttpTimeoutConfig = &SettingsConfig{180, &Range{10, 3600}}
var ShortHttpTimeoutConfig = &SettingsConfig{20, &Range{1, 3600}}
var MaxRetryForLiveUpdatePipelineConfig = &SettingsConfig{5, &Range{1, 100}}
var WaitTimeForLiveUpdatePipelineConfig = &SettingsConfig{2000, &Range{10, 60000}}
var ReplSpecCheckIntervalConfig = &SettingsConfig{15, &Range{1, 3600}}
var MemStatsLogIntervalConfig = &SettingsConfig{120, &Range{1, 3600}}
var MaxNumOfMetakvRetriesConfig = &SettingsConfig{4, &Range{0, 100}}
var RetryIntervalMetakvConfig = &SettingsConfig{500, &Range{1, 60000}}
var UprFeedDataChanLengthConfig = &SettingsConfig{20000, &Range{1, math.MaxInt32}}
var UprFeedBufferSizeConfig = &SettingsConfig{1024 * 1024, &Range{1, math.MaxInt32}}
var XmemMaxRetryConfig = &SettingsConfig{5, &Range{0, 1000}}
var XmemWriteTimeoutConfig = &SettingsConfig{120, &Range{1, 3600}}
var XmemReadTimeoutConfig = &SettingsConfig{120, &Range{1, 3600}}
var XmemMaxReadDownTimeConfig = &SettingsConfig{60, &Range{1, 3600}}
var XmemBackoffWaitTimeConfig = &SettingsConfig{10, &Range{1, 1000}}
var XmemMaxBackoffFactorConfig = &SettingsConfig{10, &Range{1, 1000}}
var XmemMaxRetryNewConnConfig = &SettingsConfig{10, &Range{0, 1000}}
var XmemBackoffTimeNewConnConfig = &SettingsConfig{1000, &Range{1, 60000}}
var XmemSelfMonitorIntervalConfig = &SettingsConfig{6, &Range{1, math.MaxInt32}}
var XmemMaxIdleCountConfig = &SettingsConfig{60, &Range{1, 3600}}
var XmemMaxIdleCountLowerBoundConfig = &SettingsConfig{10, &Range{1, base.XmemMaxIdleCountUpperBound}}
var XmemMaxIdleCountUpperBoundConfig = &SettingsConfig{120, &Range{base.XmemMaxIdleCountLowerBound, 3600}}
var XmemMaxDataChanSizeConfig = &SettingsConfig{10 * 1024 * 1024, &Range{1, math.MaxInt32}}
var XmemMaxBatchSizeConfig = &SettingsConfig{50, &Range{1, MaxBatchCount}}
var CapiRetryIntervalConfig = &SettingsConfig{500, &Range{1, 60000}}
var MaxLengthSnapshotHistoryConfig = &SettingsConfig{200, &Range{1, 100000}}
var MaxRetryTargetStatsConfig = &SettingsConfig{6, &Range{0, 100}}
var RetryIntervalTargetStatsConfig = &SettingsConfig{1000, &Range{1, 60000}}
var NumberOfSlotsForBandwidthThrottlingConfig = &SettingsConfig{10, &Range{1, 1000}}
var PercentageOfBytesToSendAsMinConfig = &SettingsConfig{30, &Range{1, 100}}
var AuditWriteTimeoutConfig = &SettingsConfig{1, &Range{1, 3600}}
var AuditReadTimeoutConfig = &SettingsConfig{1, &Range{1, 3600}}
var MaxRetryCapiServiceConfig = &SettingsConfig{5, &Range{0, 100}}
var MaxNumberOfAsyncListenersConfig = &SettingsConfig{4, &Range{1, 100}}
var XmemMaxRetryIntervalConfig = &SettingsConfig{300, &Range{1, 3600}}
var XmemMaxRetryMutationLockedConfig = &SettingsConfig{20, &Range{0, 1000}}
var XmemMaxRetryIntervalMutationLockedConfig = &SettingsConfig{30, &Range{1, 3600}}
var HELOTimeoutConfig = &SettingsConfig{120, &Range{1, 3600}}
var WaitTimeBetweenMetadataChangeListenersConfig = &SettingsConfig{int(base.WaitTimeBetweenMetadataChangeListeners / time.Millisecond), &Range{10, 60000}}
var KeepAlivePeriodConfig = &SettingsConfig{30, &Range{1, 3600}}
var ThresholdPercentageForEventChanSizeLoggingConfig = &SettingsConfig{90, &Range{1, 100}}
var ThresholdForThroughSeqnoComputationConfig = &SettingsConfig{100, &Range{1, 60000}}
var StatsLogIntervalConfig = &SettingsConfig{30, &Range{1, 36000}}
var XmemDefaultRespTimeoutConfig = &SettingsConfig{1000, &Range{1, 3600000}}
var BypassSanInCertificateCheckConfig = &SettingsConfig{0, &Range{0, 1}}
var ReplicationSpecGCCntConfig = &SettingsConfig{4, &Range{1, 100}}
var TimeoutRuntimeContextStartConfig = &SettingsConfig{int(base.TimeoutRuntimeContextStart / time.Second), &Range{1, 3600}}
var TimeoutRuntimeContextStopConfig = &SettingsConfig{int(base.TimeoutRuntimeContextStop / time.Second), &Range{1, 3600}}
var TimeoutPartsStartConfig = &SettingsConfig{int(base.TimeoutPartsStart / time.Second), &Range{1, 3600}}
var TimeoutPartsStopConfig = &SettingsConfig{int(base.TimeoutPartsStop / time.Second), &Range{1, 3600}}
var TimeoutDcpCloseUprStreamsConfig = &SettingsConfig{int(base.TimeoutDcpCloseUprStreams / time.Second), &Range{1, 3600}}
var TimeoutDcpCloseUprFeedConfig = &SettingsConfig{int(base.TimeoutDcpCloseUprFeed / time.Second), &Range{1, 3600}}
var TimeoutHttpsPortLookupConfig = &SettingsConfig{int(base.HttpsPortLookupTimeout / time.Second), &Range{1, 3600}}
var CpuCollectionIntervalConfig = &SettingsConfig{2000, &Range{10, 3600000}}
var ResourceManagementIntervalConfig = &SettingsConfig{1000, &Range{10, 3600000}}
var ResourceManagementStatsIntervalConfig = &SettingsConfig{10000, &Range{10, 3600000}}
var ChangesLeftThresholdForOngoingReplicationConfig = &SettingsConfig{200000, &Range{1, 200000000}}
var ResourceManagementRatioBaseConfig = &SettingsConfig{100, &Range{10, 10000}}
var ResourceManagementRatioUpperBoundConfig = &SettingsConfig{90, &Range{1, 10000}}
var MaxCountBacklogForSetDcpPriorityConfig = &SettingsConfig{5, &Range{1, 1000}}
var MaxCountNoBacklogForResetDcpPriorityConfig = &SettingsConfig{300, &Range{1, 10000}}
var ExtraQuotaForUnderutilizedCPUConfig = &SettingsConfig{10, &Range{0, 1000}}
var ThroughputThrottlerLogIntervalConfig = &SettingsConfig{10000, &Range{10, 3600000}}
var ThroughputThrottlerClearTokensIntervalConfig = &SettingsConfig{3000, &Range{10, 3600000}}
var NumberOfSlotsForThroughputThrottlingConfig = &SettingsConfig{10, &Range{1, 1000}}
var IntervalForThrottlerCalibrationConfig = &SettingsConfig{4, &Range{1, 1000}}
var ThroughputSampleSizeConfig = &SettingsConfig{1028, &Range{10, 1000000}}
var ThroughputSampleAlphaConfig = &SettingsConfig{15, &Range{1, 1000}}
var ThresholdRatioForProcessCpuConfig = &SettingsConfig{95, &Range{1, 1000}}
var ThresholdRatioForTotalCpuConfig = &SettingsConfig{95, &Range{1, 1000}}
var MaxCountCpuNotMaxedConfig = &SettingsConfig{3, &Range{1, 1000}}
var MaxCountThroughputDropConfig = &SettingsConfig{3, &Range{1, 1000}}
var FilteringInternalKeyConfig = &SettingsConfig{base.InternalKeyKey, nil}
var FilteringInternalXattrConfig = &SettingsConfig{base.InternalKeyXattr, nil}
var RemoteClusterAlternateAddrChangeConfig = &SettingsConfig{base.RemoteClusterAlternateAddrChangeCnt, &Range{1, 1000}}
var ManifestRefreshSrcIntervalConfig = &SettingsConfig{base.ManifestRefreshSrcInterval, &Range{1, 10000}}
var ManifestRefreshTgtIntervalConfig = &SettingsConfig{base.ManifestRefreshTgtInterval, &Range{1, 10000}}
var BackfillPersistIntervalConfig = &SettingsConfig{int64(base.BackfillPersistInterval / time.Millisecond), &Range{1, 10000}}
var JSEngineWorkersConfig = &SettingsConfig{base.JSEngineWorkers, &Range{1, 10000}}     // default and range same as GoMaxProcsConfig
var JSWorkerQuotaConfig = &SettingsConfig{base.JSWorkerQuota, &Range{1024, 1610612736}} // Max 1.5GB which is V8 heap limit
var MaxCountDCPStreamsInactiveConfig = &SettingsConfig{base.MaxCountStreamsInactive, &Range{1, 40}}
var ResourceMgrKVDetectionRetryIntervalConfig = &SettingsConfig{int(base.ResourceMgrKVDetectionRetryInterval / time.Second), &Range{1, 3600}}
var UtilsStopwatchDiagInternalThresholdConfig = &SettingsConfig{int(base.DiagInternalThreshold / time.Millisecond), &Range{10, 3600000}}
var UtilsStopwatchDiagExternalThresholdConfig = &SettingsConfig{int(base.DiagNetworkThreshold / time.Millisecond), &Range{10, 3600000}}
var ReplStatusLoadBrokenMapTimeoutConfig = &SettingsConfig{int(base.ReplStatusLoadBrokenMapTimeout / time.Second), &Range{1, int(base.AdminportReadTimeout / time.Second)}}
var ReplStatusExportBrokenMapTimeoutConfig = &SettingsConfig{int(base.ReplStatusExportBrokenMapTimeout / time.Second), &Range{1, int(base.AdminportReadTimeout / time.Second)}}
var TopologySvcCooldownConfig = &SettingsConfig{int(base.TopologySvcCoolDownPeriod / time.Second), &Range{1, 3600 /*1 hour*/}}
var TopologySvcErrCooldownConfig = &SettingsConfig{int(base.TopologySvcErrCoolDownPeriod / time.Second), &Range{1, 3600 /*1 hour*/}}
var BucketTopologyGCScanTimeConfig = &SettingsConfig{int(base.BucketTopologyGCScanTime / time.Minute), &Range{1, 360}}
var BucketTopologyGCPruneTimeConfig = &SettingsConfig{int(base.BucketTopologyGCPruneTime / time.Hour), &Range{1, 48}}
var P2PCommTimeoutConfig = &SettingsConfig{int(base.P2PCommTimeout / time.Second), &Range{1, 300}}
var P2PMaxReceiveChLenConfig = &SettingsConfig{base.MaxP2PReceiveChLen, &Range{500, 50000}}
var P2POpaqueCleanupIntervalConfig = &SettingsConfig{int(base.P2POpaqueCleanupInterval / time.Second), &Range{1, 600}}
var P2PVBRelatedGCIntervalConfig = &SettingsConfig{int(base.P2PVBRelatedGCInterval / time.Hour), &Range{1, 336 /*2 weeks*/}}
var P2PReplicaReplicatorReloadConfig = &SettingsConfig{int(base.P2PReplicaReplicatorReloadChSize), &Range{1, 1000}}
var P2PRetryWaitTimeMilliSecConfig = &SettingsConfig{int(base.PeerToPeerRetryWaitTime / time.Millisecond), &Range{1, 1000}}
var P2PRetryFactorConfig = &SettingsConfig{int(base.PeerToPeerRetryFactor), &Range{1, 5}}
var ThroughSeqnoBgScannerFreqConfig = &SettingsConfig{int(base.ThroughSeqnoBgScannerFreq / time.Second), &Range{1, 300}}
var ThroughSeqnoBgScannerLogFreqConfig = &SettingsConfig{int(base.ThroughSeqnoBgScannerLogFreq / time.Second), &Range{1, 300}}
var PipelineTimeoutP2PProtocolConfig = &SettingsConfig{int(base.TimeoutP2PProtocol / time.Second), &Range{10, 300}}
var CkptCacheCtrlChLenConfig = &SettingsConfig{int(base.CkptCacheCtrlChLen), &Range{1, 5000}}
var CkptCacheReqChLenConfig = &SettingsConfig{int(base.CkptCacheReqChLen), &Range{1, 5000}}
var HumanRecoveryThresholdConfig = &SettingsConfig{int(base.HumanRecoveryThreshold.Seconds()), &Range{10, 600}}
var DnsSrvReBootstrapConfig = &SettingsConfig{base.DNSSrvReBootstrap, nil}
var ConnectionPreCheckGCTimeoutConfig = &SettingsConfig{int(base.ConnectionPreCheckGCTimeout / time.Second), &Range{120, 600}}
var ConnectionPreCheckRPCTimeoutConfig = &SettingsConfig{int(base.ConnectionPreCheckRPCTimeout / time.Second), &Range{1, 30}}
var GlobalOSOConfig = &SettingsConfig{defaultValue: int(base.GlobalOSOSetting), Range: &Range{int(base.GlobalOSONoOp), int(base.GlobalOSOMax) - 1}}
var ConnErrsListMaxEntriesConfig = &SettingsConfig{defaultValue: base.ConnErrorsListMaxEntries, Range: &Range{1, 100}}
var PeerManifestsGetterSleepTimeConfig = &SettingsConfig{base.ManifestsGetterSleepTimeSecs, &Range{1, 60}}
var PeerManifestsGetterMaxRetryConfig = &SettingsConfig{base.ManifestsGetterMaxRetry, &Range{1, 60}}
var DatapoolLogFrequencyConfig = &SettingsConfig{base.DatapoolLogFrequency, &Range{0, 10000}}
var CapellaHostnameSuffixConfig = &SettingsConfig{base.CapellaHostnameSuffix, nil}
var NWLatencyToleranceMilliSecConfig = &SettingsConfig{int(base.NWLatencyToleranceMilliSec / time.Millisecond), &Range{0, 60000}}

var XDCRInternalSettingsConfigMap = map[string]*SettingsConfig{
	TopologyChangeCheckIntervalKey:                TopologyChangeCheckIntervalConfig,
	MaxTopologyChangeCountBeforeRestartKey:        MaxTopologyChangeCountBeforeRestartConfig,
	MaxTopologyStableCountBeforeRestartKey:        MaxTopologyStableCountBeforeRestartConfig,
	MaxWorkersForCheckpointingKey:                 MaxWorkersForCheckpointingConfig,
	TimeoutCheckpointBeforeStopKey:                TimeoutCheckpointBeforeStopConfig,
	CapiDataChanSizeMultiplierKey:                 CapiDataChanSizeMultiplierConfig,
	RefreshRemoteClusterRefIntervalKey:            RefreshRemoteClusterRefIntervalConfig,
	CapiMaxRetryBatchUpdateDocsKey:                CapiMaxRetryBatchUpdateDocsConfig,
	CapiBatchTimeoutKey:                           CapiBatchTimeoutConfig,
	CapiWriteTimeoutKey:                           CapiWriteTimeoutConfig,
	CapiReadTimeoutKey:                            CapiReadTimeoutConfig,
	MaxCheckpointRecordsToKeepKey:                 MaxCheckpointRecordsToKeepConfig,
	MaxCheckpointRecordsToReadKey:                 MaxCheckpointRecordsToReadConfig,
	DefaultHttpTimeoutKey:                         DefaultHttpTimeoutConfig,
	ShortHttpTimeoutKey:                           ShortHttpTimeoutConfig,
	MaxRetryForLiveUpdatePipelineKey:              MaxRetryForLiveUpdatePipelineConfig,
	WaitTimeForLiveUpdatePipelineKey:              WaitTimeForLiveUpdatePipelineConfig,
	ReplSpecCheckIntervalKey:                      ReplSpecCheckIntervalConfig,
	MemStatsLogIntervalKey:                        MemStatsLogIntervalConfig,
	MaxNumOfMetakvRetriesKey:                      MaxNumOfMetakvRetriesConfig,
	RetryIntervalMetakvKey:                        RetryIntervalMetakvConfig,
	UprFeedDataChanLengthKey:                      UprFeedDataChanLengthConfig,
	UprFeedBufferSizeKey:                          UprFeedBufferSizeConfig,
	XmemMaxRetryKey:                               XmemMaxRetryConfig,
	XmemWriteTimeoutKey:                           XmemWriteTimeoutConfig,
	XmemReadTimeoutKey:                            XmemReadTimeoutConfig,
	XmemMaxReadDownTimeKey:                        XmemMaxReadDownTimeConfig,
	XmemBackoffWaitTimeKey:                        XmemBackoffWaitTimeConfig,
	XmemMaxBackoffFactorKey:                       XmemMaxBackoffFactorConfig,
	XmemMaxRetryNewConnKey:                        XmemMaxRetryNewConnConfig,
	XmemBackoffTimeNewConnKey:                     XmemBackoffTimeNewConnConfig,
	XmemSelfMonitorIntervalKey:                    XmemSelfMonitorIntervalConfig,
	XmemMaxIdleCountKey:                           XmemMaxIdleCountConfig,
	XmemMaxIdleCountLowerBoundKey:                 XmemMaxIdleCountLowerBoundConfig,
	XmemMaxIdleCountUpperBoundKey:                 XmemMaxIdleCountUpperBoundConfig,
	XmemMaxDataChanSizeKey:                        XmemMaxDataChanSizeConfig,
	XmemMaxBatchSizeKey:                           XmemMaxBatchSizeConfig,
	CapiRetryIntervalKey:                          CapiRetryIntervalConfig,
	MaxLengthSnapshotHistoryKey:                   MaxLengthSnapshotHistoryConfig,
	MaxRetryTargetStatsKey:                        MaxRetryTargetStatsConfig,
	RetryIntervalTargetStatsKey:                   RetryIntervalTargetStatsConfig,
	NumberOfSlotsForBandwidthThrottlingKey:        NumberOfSlotsForBandwidthThrottlingConfig,
	PercentageOfBytesToSendAsMinKey:               PercentageOfBytesToSendAsMinConfig,
	AuditWriteTimeoutKey:                          AuditWriteTimeoutConfig,
	AuditReadTimeoutKey:                           AuditReadTimeoutConfig,
	MaxRetryCapiServiceKey:                        MaxRetryCapiServiceConfig,
	MaxNumberOfAsyncListenersKey:                  MaxNumberOfAsyncListenersConfig,
	XmemMaxRetryIntervalKey:                       XmemMaxRetryIntervalConfig,
	XmemMaxRetryMutationLockedKey:                 XmemMaxRetryMutationLockedConfig,
	XmemMaxRetryIntervalMutationLockedKey:         XmemMaxRetryIntervalMutationLockedConfig,
	HELOTimeoutKey:                                HELOTimeoutConfig,
	WaitTimeBetweenMetadataChangeListenersKey:     WaitTimeBetweenMetadataChangeListenersConfig,
	KeepAlivePeriodKey:                            KeepAlivePeriodConfig,
	ThresholdPercentageForEventChanSizeLoggingKey: ThresholdPercentageForEventChanSizeLoggingConfig,
	ThresholdForThroughSeqnoComputationKey:        ThresholdForThroughSeqnoComputationConfig,
	StatsLogIntervalKey:                           StatsLogIntervalConfig,
	XmemDefaultRespTimeoutKey:                     XmemDefaultRespTimeoutConfig,
	BypassSanInCertificateCheckKey:                BypassSanInCertificateCheckConfig,
	ReplicationSpecGCCntKey:                       ReplicationSpecGCCntConfig,
	TimeoutRuntimeContextStartKey:                 TimeoutRuntimeContextStartConfig,
	TimeoutRuntimeContextStopKey:                  TimeoutRuntimeContextStopConfig,
	TimeoutPartsStartKey:                          TimeoutPartsStartConfig,
	TimeoutPartsStopKey:                           TimeoutPartsStopConfig,
	TimeoutDcpCloseUprStreamsKey:                  TimeoutDcpCloseUprStreamsConfig,
	TimeoutDcpCloseUprFeedKey:                     TimeoutDcpCloseUprFeedConfig,
	TimeoutHttpsPortLookupKey:                     TimeoutHttpsPortLookupConfig,
	CpuCollectionIntervalKey:                      CpuCollectionIntervalConfig,
	ResourceManagementIntervalKey:                 ResourceManagementIntervalConfig,
	ResourceManagementStatsIntervalKey:            ResourceManagementStatsIntervalConfig,
	ChangesLeftThresholdForOngoingReplicationKey:  ChangesLeftThresholdForOngoingReplicationConfig,
	ResourceManagementRatioBaseKey:                ResourceManagementRatioBaseConfig,
	ResourceManagementRatioUpperBoundKey:          ResourceManagementRatioUpperBoundConfig,
	MaxCountBacklogForSetDcpPriorityKey:           MaxCountBacklogForSetDcpPriorityConfig,
	MaxCountNoBacklogForResetDcpPriorityKey:       MaxCountNoBacklogForResetDcpPriorityConfig,
	ExtraQuotaForUnderutilizedCPUKey:              ExtraQuotaForUnderutilizedCPUConfig,
	ThroughputThrottlerLogIntervalKey:             ThroughputThrottlerLogIntervalConfig,
	ThroughputThrottlerClearTokensIntervalKey:     ThroughputThrottlerClearTokensIntervalConfig,
	NumberOfSlotsForThroughputThrottlingKey:       NumberOfSlotsForThroughputThrottlingConfig,
	IntervalForThrottlerCalibrationKey:            IntervalForThrottlerCalibrationConfig,
	ThroughputSampleSizeKey:                       ThroughputSampleSizeConfig,
	ThroughputSampleAlphaKey:                      ThroughputSampleAlphaConfig,
	ThresholdRatioForProcessCpuKey:                ThresholdRatioForProcessCpuConfig,
	ThresholdRatioForTotalCpuKey:                  ThresholdRatioForTotalCpuConfig,
	MaxCountCpuNotMaxedKey:                        MaxCountCpuNotMaxedConfig,
	MaxCountThroughputDropKey:                     MaxCountThroughputDropConfig,
	FilteringInternalKey:                          FilteringInternalKeyConfig,
	FilteringInternalXattr:                        FilteringInternalXattrConfig,
	RemoteClusterAlternateAddrChangeKey:           RemoteClusterAlternateAddrChangeConfig,
	ManifestRefreshSrcIntervalKey:                 ManifestRefreshSrcIntervalConfig,
	ManifestRefreshTgtIntervalKey:                 ManifestRefreshTgtIntervalConfig,
	BackfillPersistIntervalKey:                    BackfillPersistIntervalConfig,
	JSEngineWorkersKey:                            JSEngineWorkersConfig,
	JSWorkerQuotaKey:                              JSWorkerQuotaConfig,
	MaxCountDCPStreamsInactiveKey:                 MaxCountDCPStreamsInactiveConfig,
	ResourceMgrKVDetectionRetryIntervalKey:        ResourceMgrKVDetectionRetryIntervalConfig,
	UtilsStopwatchDiagInternalThresholdKey:        UtilsStopwatchDiagInternalThresholdConfig,
	UtilsStopwatchDiagExternalThresholdKey:        UtilsStopwatchDiagExternalThresholdConfig,
	ReplStatusExportBrokenMapTimeoutKey:           ReplStatusExportBrokenMapTimeoutConfig,
	ReplStatusLoadBrokenMapTimeoutKey:             ReplStatusLoadBrokenMapTimeoutConfig,
	TopologySvcCooldownPeriodKey:                  TopologySvcCooldownConfig,
	TopologySvcErrCooldownPeriodKey:               TopologySvcErrCooldownConfig,
	BucketTopologyGCScanTimeKey:                   BucketTopologyGCScanTimeConfig,
	BucketTopologyGCPruneTimeKey:                  BucketTopologyGCPruneTimeConfig,
	P2PCommTimeoutKey:                             P2PCommTimeoutConfig,
	P2PMaxReceiveChLenKey:                         P2PMaxReceiveChLenConfig,
	P2POpaqueCleanupIntervalKey:                   P2POpaqueCleanupIntervalConfig,
	P2PVBRelatedGCIntervalKey:                     P2PVBRelatedGCIntervalConfig,
	P2PReplicaReplicatorReloadSizeKey:             P2PReplicaReplicatorReloadConfig,
	P2PRetryWaitTimeMilliSecKey:                   P2PRetryWaitTimeMilliSecConfig,
	P2PRetryFactorKey:                             P2PRetryFactorConfig,
	ThroughSeqnoBgScannerFreqKey:                  ThroughSeqnoBgScannerFreqConfig,
	ThroughSeqnoBgScannerLogFreqKey:               ThroughSeqnoBgScannerLogFreqConfig,
	PipelineTimeoutP2PProtocolKey:                 PipelineTimeoutP2PProtocolConfig,
	CkptCacheCtrlChLenKey:                         CkptCacheCtrlChLenConfig,
	CkptCacheReqChLenKey:                          CkptCacheReqChLenConfig,
	HumanRecoveryThresholdKey:                     HumanRecoveryThresholdConfig,
	DnsSrvReBootstrapKey:                          DnsSrvReBootstrapConfig,
	ConnectionPreCheckGCTimeoutKey:                ConnectionPreCheckGCTimeoutConfig,
	ConnectionPreCheckRPCTimeoutKey:               ConnectionPreCheckRPCTimeoutConfig,
	GlobalOSOConfigKey:                            GlobalOSOConfig,
	ConnErrorsListMaxEntriesKey:                   ConnErrsListMaxEntriesConfig,
	PeerManifestsGetterSleepTimeKey:               PeerManifestsGetterSleepTimeConfig,
	PeerManifestsGetterMaxRetryKey:                PeerManifestsGetterMaxRetryConfig,
	DatapoolLogFrequencyKey:                       DatapoolLogFrequencyConfig,
	CapellaHostNameSuffixKey:                      CapellaHostnameSuffixConfig,
	NWLatencyToleranceMilliSecKey:                 NWLatencyToleranceMilliSecConfig,
}

func InitConstants(xmemMaxIdleCountLowerBound int, xmemMaxIdleCountUpperBound int) {
	XmemMaxIdleCountLowerBoundConfig = &SettingsConfig{10, &Range{1, xmemMaxIdleCountUpperBound}}
	XmemMaxIdleCountUpperBoundConfig = &SettingsConfig{120, &Range{xmemMaxIdleCountLowerBound, 3600}}
	XDCRInternalSettingsConfigMap[XmemMaxIdleCountLowerBoundKey] = XmemMaxIdleCountLowerBoundConfig
	XDCRInternalSettingsConfigMap[XmemMaxIdleCountUpperBoundKey] = XmemMaxIdleCountUpperBoundConfig
}

type InternalSettings struct {
	*Settings
	// revision number to be used by metadata service. not included in json
	Revision interface{}
}

// config map retriever required by Settings
func GetInternalSettingsConfigMap() map[string]*SettingsConfig {
	return XDCRInternalSettingsConfigMap
}

func EmptyInternalSettings() *InternalSettings {
	return &InternalSettings{Settings: EmptySettings(GetInternalSettingsConfigMap)}
}

func DefaultInternalSettings() *InternalSettings {
	return &InternalSettings{Settings: DefaultSettings(GetInternalSettingsConfigMap)}
}

func (s *InternalSettings) Equals(s2 *InternalSettings) bool {
	if s == s2 {
		// this also covers the case where s = nil and s2 = nil
		return true
	}
	if (s == nil && s2 != nil) || (s != nil && s2 == nil) {
		return false
	}
	return s.Settings.Equals(s2.Settings)
}

func (s *InternalSettings) Clone() *InternalSettings {
	return &InternalSettings{Settings: s.Settings.Clone(),
		Revision: s.Revision,
	}
}

func (s *InternalSettings) PostProcessAfterUnmarshalling() {
	s.Settings.PostProcessAfterUnmarshalling(GetInternalSettingsConfigMap)
}

func ValidateAndConvertXDCRInternalSettingsValue(key, value string) (interface{}, error) {
	return ValidateAndConvertSettingsValue(key, value, XDCRInternalSettingsConfigMap)
}

// construct a new internal settings from an old internal settings object from pre-4.6 builds
func ConstructInternalSettingsFromV1Settings(v1Settings *V1InternalSettings) *InternalSettings {
	s := EmptyInternalSettings()

	s.Values[TopologyChangeCheckIntervalKey] = v1Settings.TopologyChangeCheckInterval
	s.Values[MaxTopologyChangeCountBeforeRestartKey] = v1Settings.MaxTopologyChangeCountBeforeRestart
	s.Values[MaxTopologyStableCountBeforeRestartKey] = v1Settings.MaxTopologyStableCountBeforeRestart
	s.Values[MaxWorkersForCheckpointingKey] = v1Settings.MaxWorkersForCheckpointing
	s.Values[TimeoutCheckpointBeforeStopKey] = v1Settings.TimeoutCheckpointBeforeStop
	s.Values[CapiDataChanSizeMultiplierKey] = v1Settings.CapiDataChanSizeMultiplier
	s.Values[RefreshRemoteClusterRefIntervalKey] = v1Settings.RefreshRemoteClusterRefInterval
	s.Values[CapiMaxRetryBatchUpdateDocsKey] = v1Settings.CapiMaxRetryBatchUpdateDocs
	s.Values[CapiBatchTimeoutKey] = v1Settings.CapiBatchTimeout
	s.Values[CapiWriteTimeoutKey] = v1Settings.CapiWriteTimeout
	s.Values[CapiReadTimeoutKey] = v1Settings.CapiReadTimeout
	s.Values[MaxCheckpointRecordsToKeepKey] = v1Settings.MaxCheckpointRecordsToKeep
	s.Values[MaxCheckpointRecordsToReadKey] = v1Settings.MaxCheckpointRecordsToRead

	return s
}

// old internal settings up until 4.6
// needed for the unmarshalling of json internal settings from 4.6 build and before
type V1InternalSettings struct {
	// interval between topology checks (in seconds)
	TopologyChangeCheckInterval int

	// the maximum number of topology change checks to wait before pipeline is restarted
	MaxTopologyChangeCountBeforeRestart int
	// the maximum number of consecutive stable topology seen before pipeline is restarted
	MaxTopologyStableCountBeforeRestart int
	// the max number of concurrent workers for checkpointing
	MaxWorkersForCheckpointing int

	// timeout for checkpointing attempt before pipeline is stopped (in seconds) - to put an upper bound on the delay of pipeline stop/restart
	TimeoutCheckpointBeforeStop int

	// capi nozzle data chan size is defined as batchCount*CapiDataChanSizeMultiplier
	CapiDataChanSizeMultiplier int

	// interval for refreshing remote cluster references (in seconds)
	RefreshRemoteClusterRefInterval int

	// max retry for capi batchUpdateDocs operation
	CapiMaxRetryBatchUpdateDocs int

	// timeout for batch processing in capi (in seconds)
	// 1. http timeout in revs_diff, i.e., batchGetMeta, call to target
	// 2. overall timeout for batchUpdateDocs operation
	CapiBatchTimeout int

	// timeout for tcp write operation in capi (in seconds)
	CapiWriteTimeout int

	// timeout for tcp read operation in capi (in seconds)
	CapiReadTimeout int

	// the maximum number of checkpoint records to write/keep in the checkpoint doc
	MaxCheckpointRecordsToKeep int

	// the maximum number of checkpoint records to read from the checkpoint doc
	MaxCheckpointRecordsToRead int

	// revision number to be used by metadata service. not included in json
	Revision interface{}
}

// after upgrade, old internal settings that did not exist in before-upgrade version will take 0 value
// these 0 values need to be replaced by defaule values
func (os *V1InternalSettings) HandleUpgrade() {
	if os.TopologyChangeCheckInterval == 0 {
		os.TopologyChangeCheckInterval = TopologyChangeCheckIntervalConfig.defaultValue.(int)
	}
	if os.MaxTopologyChangeCountBeforeRestart == 0 {
		os.MaxTopologyChangeCountBeforeRestart = MaxTopologyChangeCountBeforeRestartConfig.defaultValue.(int)
	}
	if os.MaxTopologyStableCountBeforeRestart == 0 {
		os.MaxTopologyStableCountBeforeRestart = MaxTopologyStableCountBeforeRestartConfig.defaultValue.(int)
	}
	if os.MaxWorkersForCheckpointing == 0 {
		os.MaxWorkersForCheckpointing = MaxWorkersForCheckpointingConfig.defaultValue.(int)
	}
	if os.TimeoutCheckpointBeforeStop == 0 {
		os.TimeoutCheckpointBeforeStop = TimeoutCheckpointBeforeStopConfig.defaultValue.(int)
	}
	if os.CapiDataChanSizeMultiplier == 0 {
		os.CapiDataChanSizeMultiplier = CapiDataChanSizeMultiplierConfig.defaultValue.(int)
	}
	if os.RefreshRemoteClusterRefInterval == 0 {
		os.RefreshRemoteClusterRefInterval = RefreshRemoteClusterRefIntervalConfig.defaultValue.(int)
	}
	if os.CapiMaxRetryBatchUpdateDocs == 0 {
		os.CapiMaxRetryBatchUpdateDocs = CapiMaxRetryBatchUpdateDocsConfig.defaultValue.(int)
	}
	if os.CapiBatchTimeout == 0 {
		os.CapiBatchTimeout = CapiBatchTimeoutConfig.defaultValue.(int)
	}
	if os.CapiWriteTimeout == 0 {
		os.CapiWriteTimeout = CapiWriteTimeoutConfig.defaultValue.(int)
	}
	if os.CapiReadTimeout == 0 {
		os.CapiReadTimeout = CapiReadTimeoutConfig.defaultValue.(int)
	}
	if os.MaxCheckpointRecordsToKeep == 0 {
		os.MaxCheckpointRecordsToKeep = MaxCheckpointRecordsToKeepConfig.defaultValue.(int)
	}
	if os.MaxCheckpointRecordsToRead == 0 {
		os.MaxCheckpointRecordsToRead = MaxCheckpointRecordsToReadConfig.defaultValue.(int)
	}
}
