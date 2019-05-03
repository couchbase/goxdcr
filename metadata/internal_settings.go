package metadata

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"math"
	"reflect"
	"strconv"
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
	//wait time between write is backoff_factor*XmemBackoffWaitTime (milliseconds)
	XmemBackoffWaitTimeKey = "XmemBackoffWaitTime"
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
	// Override setting to allow replication to be created on a source ephemeral bucket with NRU policy
	AllowSourceNRUKey = "AllowSourceNRUCreation"

	TimeoutRuntimeContextStartKey = "TimeoutRuntimeContextStart"
	TimeoutRuntimeContextStopKey  = "TimeoutRuntimeContextStop"
	TimeoutPartsStartKey          = "TimeoutPartsStart"
	TimeoutPartsStopKey           = "TimeoutPartsStop"
	TimeoutDcpCloseUprStreamsKey  = "TimeoutDcpCloseUprStreams"
	TimeoutDcpCloseUprFeedKey     = "TimeoutDcpCloseUprFeed"
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
var HELOTimeoutConfig = &SettingsConfig{120, &Range{1, 3600}}
var WaitTimeBetweenMetadataChangeListenersConfig = &SettingsConfig{1000, &Range{10, 60000}}
var KeepAlivePeriodConfig = &SettingsConfig{30, &Range{1, 3600}}
var ThresholdPercentageForEventChanSizeLoggingConfig = &SettingsConfig{90, &Range{1, 100}}
var ThresholdForThroughSeqnoComputationConfig = &SettingsConfig{100, &Range{1, 60000}}
var StatsLogIntervalConfig = &SettingsConfig{30, &Range{1, 36000}}
var XmemDefaultRespTimeoutConfig = &SettingsConfig{1000, &Range{1, 3600000}}
var BypassSanInCertificateCheckConfig = &SettingsConfig{0, &Range{0, 1}}
var ReplicationSpecGCCntConfig = &SettingsConfig{4, &Range{1, 100}}
var TimeoutRuntimeContextStartConfig = &SettingsConfig{30, &Range{1, 3600}}
var TimeoutRuntimeContextStopConfig = &SettingsConfig{5, &Range{1, 3600}}
var TimeoutPartsStartConfig = &SettingsConfig{30, &Range{1, 3600}}
var TimeoutPartsStopConfig = &SettingsConfig{10, &Range{1, 3600}}
var TimeoutDcpCloseUprStreamsConfig = &SettingsConfig{3, &Range{1, 3600}}
var TimeoutDcpCloseUprFeedConfig = &SettingsConfig{3, &Range{1, 3600}}
var AllowSourceNRUConfig = &SettingsConfig{false, nil}

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
	AllowSourceNRUKey:                             AllowSourceNRUConfig,
}

func InitConstants(xmemMaxIdleCountLowerBound int, xmemMaxIdleCountUpperBound int) {
	XmemMaxIdleCountLowerBoundConfig = &SettingsConfig{10, &Range{1, xmemMaxIdleCountUpperBound}}
	XmemMaxIdleCountUpperBoundConfig = &SettingsConfig{120, &Range{xmemMaxIdleCountLowerBound, 3600}}
	XDCRInternalSettingsConfigMap[XmemMaxIdleCountLowerBoundKey] = XmemMaxIdleCountLowerBoundConfig
	XDCRInternalSettingsConfigMap[XmemMaxIdleCountUpperBoundKey] = XmemMaxIdleCountUpperBoundConfig
}

type InternalSettings struct {
	// key - internalSettingsKey
	// value - value of internalSettings
	// Note, value is always a primitive type, int, bool, string, etc.
	Values map[string]interface{}

	// revision number to be used by metadata service. not included in json
	Revision interface{}
}

func DefaultInternalSettings() *InternalSettings {
	defaultSettings := &InternalSettings{
		Values: make(map[string]interface{}),
	}

	for settingsKey, settingsConfig := range XDCRInternalSettingsConfigMap {
		defaultSettings.Values[settingsKey] = settingsConfig.defaultValue
	}
	return defaultSettings
}

func (s *InternalSettings) Equals(s2 *InternalSettings) bool {
	if s == s2 {
		// this also covers the case where s = nil and s2 = nil
		return true
	}
	if (s == nil && s2 != nil) || (s != nil && s2 == nil) {
		return false
	}

	if len(s.Values) != len(s2.Values) {
		return false
	}

	for key, value := range s.Values {
		value2, ok := s2.Values[key]
		if !ok {
			return false
		}
		// use != operator on value, which is a primitive type
		if value != value2 {
			return false
		}
	}

	return true
}

func (s *InternalSettings) UpdateSettingsFromMap(settingsMap map[string]interface{}) (changed bool, errorMap map[string]error) {
	changed = false
	errorMap = make(map[string]error)

	for settingKey, settingValue := range settingsMap {
		settingConfig, ok := XDCRInternalSettingsConfigMap[settingKey]
		if !ok {
			// not a valid settings key
			errorMap[settingKey] = fmt.Errorf("Invalid key in map, %v", settingKey)
			continue
		}

		expectedType := reflect.TypeOf(settingConfig.defaultValue)
		actualType := reflect.TypeOf(settingValue)
		if expectedType != actualType {
			// type of the value does not match
			errorMap[settingKey] = fmt.Errorf("Invalid type of value in map for %v. expected=%v, actual=%v", settingKey, expectedType, actualType)
			continue
		}

		oldSettingValue, ok := s.Values[settingKey]
		if !ok || settingValue != oldSettingValue {
			s.Values[settingKey] = settingValue
			changed = true
		}
	}

	return
}

func ValidateAndConvertXDCRInternalSettingsValue(key, value string) (interface{}, error) {
	settingConfig, ok := XDCRInternalSettingsConfigMap[key]
	if !ok {
		return nil, errors.New("Not a valid internal setting")
	}

	valueTypeKind := reflect.TypeOf(settingConfig.defaultValue).Kind()
	switch valueTypeKind {
	case reflect.Int:
		return validateAndConvertIntValue(value, settingConfig)
	case reflect.Bool:
		return validateAndConvertBoolValue(value, settingConfig)
	case reflect.String:
		// no validation for string type
		return value, nil
	default:
		// should never get here
		return nil, fmt.Errorf("Value is of unsupported type, %v", valueTypeKind)
	}
}

func validateAndConvertIntValue(value string, settingConfig *SettingsConfig) (convertedValue interface{}, err error) {
	convertedValue, err = strconv.ParseInt(value, base.ParseIntBase, base.ParseIntBitSize)
	if err != nil {
		err = base.IncorrectValueTypeError("an integer")
		return
	}

	convertedValue = int(convertedValue.(int64))
	err = RangeCheck(convertedValue.(int), settingConfig)
	return
}

func validateAndConvertBoolValue(value string, settingConfig *SettingsConfig) (convertedValue interface{}, err error) {
	convertedValue, err = strconv.ParseBool(value)
	if err != nil {
		err = base.IncorrectValueTypeError("a boolean")
		return
	}
	return
}

func (s *InternalSettings) ToMap() map[string]interface{} {
	settingsMap := make(map[string]interface{})
	for key, value := range s.Values {
		settingsMap[key] = value
	}
	return settingsMap
}

func (s *InternalSettings) Clone() *InternalSettings {
	internal_settings := &InternalSettings{Values: make(map[string]interface{})}
	for key, value := range s.Values {
		internal_settings.Values[key] = value
	}
	// shallow copy. Revision is never modified
	internal_settings.Revision = s.Revision
	return internal_settings
}

// convert from an old internal settings object from pre-4.6 builds
// no need for type check since V1Settings are all of int type
func (s *InternalSettings) ConvertFromV1InternalSettings(v1Settings *V1InternalSettings) {
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
}

// since setting value is defined as interface{}, the actual data type of setting value may change
// after marshalling and unmarshalling
// for example, an "int" type value becomes "float64" after marshalling and unmarshalling
// it is necessary to convert such data type back
func (s *InternalSettings) HandleDataTypeConversionAfterUnmarshalling() {
	errorsMap := make(map[string]error)
	for settingKey, settingValue := range s.Values {
		settingConfig, ok := XDCRInternalSettingsConfigMap[settingKey]
		if !ok {
			// should never get here
			errorsMap[settingKey] = errors.New("not a valid internal setting")
			continue
		}
		valueTypeKind := reflect.TypeOf(settingConfig.defaultValue).Kind()
		switch valueTypeKind {
		case reflect.Int:
			intValue, err := handleIntTypeConversion(settingValue)
			if err != nil {
				// should never get here
				errorsMap[settingKey] = err
				continue
			}
			s.Values[settingKey] = intValue
		case reflect.Bool:
			// boolean type needs no conversion
			continue
		case reflect.String:
			// string type needs no conversion
			continue
		default:
			// should never get here
			errorsMap[settingKey] = fmt.Errorf("value is of unsupported data type, %v/n", valueTypeKind)
			continue
		}
	}

	if len(errorsMap) > 0 {
		logger_is.Warnf("Internal settings unmarshalled from metakv has the following issues: %v\n", errorsMap)

		// remove problematic key/value to avoid problems down the road
		// default values will be used for the removed keys
		for problematicSettingKey, _ := range errorsMap {
			delete(s.Values, problematicSettingKey)
		}
	}
}

func handleIntTypeConversion(settingValue interface{}) (int, error) {
	// if an integer type setting is unmarshalled from metakv, the value would be float64 type
	floatValue, ok := settingValue.(float64)
	if ok {
		return int(floatValue), nil
	}

	return 0, fmt.Errorf("value is of unexpected data type, %v", reflect.TypeOf(settingValue))
}

// after upgrade, internal settings that did not exist in before-upgrade version do not exist in s.Values
// add these settings to s.Values with default values
func (s *InternalSettings) HandleUpgrade() {
	for settingsKey, settingsConfig := range XDCRInternalSettingsConfigMap {
		if _, ok := s.Values[settingsKey]; !ok {
			s.Values[settingsKey] = settingsConfig.defaultValue
		}
	}
}

// old internal settings up until 4.6
// needed for the unmarshalling of json internal settings from pre-4.6 build
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
