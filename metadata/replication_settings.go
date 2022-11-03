// Copyright (c) 2013-2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package metadata

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"strconv"
)

// keys for replication settings
const (
	ReplicationTypeKey                = "replication_type"
	FilterExpressionKey               = "filter_expression"
	ActiveKey                         = "active"
	CheckpointIntervalKey             = "checkpoint_interval"
	BatchCountKey                     = "worker_batch_size"
	BatchSizeKey                      = "doc_batch_size_kb"
	FailureRestartIntervalKey         = "failure_restart_interval"
	OptimisticReplicationThresholdKey = "optimistic_replication_threshold"
	SourceNozzlePerNodeKey            = "source_nozzle_per_node"
	TargetNozzlePerNodeKey            = "target_nozzle_per_node"
	PipelineLogLevelKey               = "log_level"
	PipelineStatsIntervalKey          = "stats_interval"
	BandwidthLimitKey                 = "bandwidth_limit"
	CompressionTypeKey                = base.CompressionTypeKey
	FilterVersionKey                  = "filter_expression_version"
	FilterSkipRestreamKey             = "filter_skip_restream"
	PriorityKey                       = "priority"
	// threshold for deciding whether replication has backlog
	// defined as desired latency, i.e., changesLeft/throughput,
	// in other words, nnumber of mutations left to process/ number of mutations that can be processed per millisecond
	// the unit for backlogThreshold is millisecond
	// the default value is 1000 (millisecond)
	BacklogThresholdKey = "backlogThreshold"
	// FilterExpDelKey is a combination flag of the keys below it
	FilterExpDelKey = base.FilterExpDelKey
	// These keys are used for REST input/output into an internal flag of FilterExpDelKey
	FilterExpKey            = base.FilterExpKey
	FilterDelKey            = base.FilterDelKey
	BypassExpiryKey         = base.BypassExpiryKey
	BypassUncommittedTxnKey = base.BypassUncommittedTxnKey
)

// keys to facilitate redaction of replication settings map
const (
	XmemCertificate       = "certificate"
	XmemClientCertificate = "clientCertificate"
	XmemClientKey         = "clientKey"
)

// settings whose default values cannot be viewed or changed through rest apis
var ImmutableDefaultSettings = []string{ReplicationTypeKey, FilterExpressionKey, ActiveKey, FilterVersionKey}

// settings whose values cannot be changed after replication is created
var ImmutableSettings = []string{}

// settings that are internal and should be hidden from outside
var HiddenSettings = []string{FilterVersionKey, FilterSkipRestreamKey, FilterExpDelKey}

// settings that are externally multiple values, but internally single value
var MultiValueMap map[string]string = map[string]string{
	FilterExpKey:    FilterExpDelKey,
	FilterDelKey:    FilterExpDelKey,
	BypassExpiryKey: FilterExpDelKey,
	BypassUncommittedTxnKey:  FilterExpDelKey,
}

var MaxBatchCount = 10000

const (
	ReplicationTypeXmem = "xmem"
	ReplicationTypeCapi = "capi"
)

var ReplicationTypeConfig = &SettingsConfig{ReplicationTypeXmem, nil}
var FilterExpressionConfig = &SettingsConfig{"", nil}
var ActiveConfig = &SettingsConfig{true, nil}
var CheckpointIntervalConfig = &SettingsConfig{600, &Range{60, 14400}}
var BatchCountConfig = &SettingsConfig{500, &Range{10, MaxBatchCount}}
var BatchSizeConfig = &SettingsConfig{2048, &Range{10, 10000}}
var FailureRestartIntervalConfig = &SettingsConfig{10, &Range{1, 300}}
var OptimisticReplicationThresholdConfig = &SettingsConfig{256, &Range{0, 20 * 1024 * 1024}}
var SourceNozzlePerNodeConfig = &SettingsConfig{2, &Range{1, 100}}
var TargetNozzlePerNodeConfig = &SettingsConfig{2, &Range{1, 100}}
var PipelineLogLevelConfig = &SettingsConfig{log.LogLevelInfo, nil}
var PipelineStatsIntervalConfig = &SettingsConfig{1000, &Range{200, 600000}}
var BandwidthLimitConfig = &SettingsConfig{0, &Range{0, 1000000}}
// user can only configure compression to 1 (None) or 3(Auto). 2 (Snappy) is for internal use
var CompressionTypeConfig = &SettingsConfig{base.CompressionTypeAuto, &Range{base.CompressionTypeStartMarker + 1, base.CompressionTypeEndMarker - 1}}
var PriorityConfig = &SettingsConfig{base.PriorityTypeHigh, nil}
var BacklogThresholdConfig = &SettingsConfig{base.BacklogThresholdDefault, &Range{10, 10000000}}
var FilterExpDelConfig = &SettingsConfig{base.FilterExpDelNone, &Range{int(base.FilterExpDelNone), int(base.FilterExpDelMax)}}

// Set to keyOnly as default because prior to adv filtering, this config did not exist
var FilterVersionConfig = &SettingsConfig{base.FilterVersionKeyOnly, nil}
var FilterSkipRestreamConfig = &SettingsConfig{false, nil}

var ReplicationSettingsConfigMap = map[string]*SettingsConfig{
	ReplicationTypeKey:                ReplicationTypeConfig,
	FilterExpressionKey:               FilterExpressionConfig,
	ActiveKey:                         ActiveConfig,
	CheckpointIntervalKey:             CheckpointIntervalConfig,
	BatchCountKey:                     BatchCountConfig,
	BatchSizeKey:                      BatchSizeConfig,
	FailureRestartIntervalKey:         FailureRestartIntervalConfig,
	OptimisticReplicationThresholdKey: OptimisticReplicationThresholdConfig,
	SourceNozzlePerNodeKey:            SourceNozzlePerNodeConfig,
	TargetNozzlePerNodeKey:            TargetNozzlePerNodeConfig,
	PipelineLogLevelKey:               PipelineLogLevelConfig,
	PipelineStatsIntervalKey:          PipelineStatsIntervalConfig,
	BandwidthLimitKey:                 BandwidthLimitConfig,
	CompressionTypeKey:                CompressionTypeConfig,
	FilterVersionKey:                  FilterVersionConfig,
	FilterSkipRestreamKey:             FilterSkipRestreamConfig,
	PriorityKey:                       PriorityConfig,
	BacklogThresholdKey:               BacklogThresholdConfig,
	FilterExpDelKey:                   FilterExpDelConfig,
}

// Adding values in this struct is deprecated - use ReplicationSettings.Settings.Values instead
type ReplicationSettings struct {
	*Settings

	//type - XMEM or CAPI
	RepType string `json:"type"`

	//the filter expression - can be used for either version 0 (key-only regex) or version 1 (XDCR Advanced filtering)
	FilterExpression string `json:"filter_exp"`

	//if the replication is active
	//default is true
	Active bool `json:"active"`

	//the interval between two checkpoint
	//default: 600 s
	//range: 60-14400s
	CheckpointInterval int `json:"checkpoint_interval"`

	//the number of mutations in a batch
	//default: 500
	//range: 500-10000
	BatchCount int `json:"batch_count"`

	//the size (kb) of a batch
	//default: 2048
	//range: 10-10000
	BatchSize int `json:"batch_size"`

	//the number of seconds to wait after failure before restarting
	//default: 30
	//range: 1-300
	FailureRestartInterval int `json:"failure_restart_interval"`

	//if the document size (in bytes) <optimistic_replication_threshold, replicate optimistically; otherwise replicate pessimistically
	//default: 256
	//range: 0-20*1024*1024
	OptimisticReplicationThreshold int `json:"optimistic_replication_threshold"`

	//the number of nozzles can be used for this replication per source cluster node
	//This together with target_nozzle_per_node controls the parallism of the replication
	//default: 2
	//range: 1-10
	SourceNozzlePerNode int `json:"source_nozzle_per_node"`

	//the number of nozzles can be used for this replication per target cluster node
	//This together with source_nozzle_per_node controls the parallism of the replication
	//default: 2
	//range: 1-10
	TargetNozzlePerNode int `json:"target_nozzle_per_node"`

	//the max replication lag (in ms) that user can tolerant for this replication
	//
	//Note: if the actual replication lag is larger than this value, it is consider as timeout
	//default: 100ms
	MaxExpectedReplicationLag int `json:"max_expected_replication_lag"`

	// The max allowed timeout percentage. Exceed that limit, piepline would be
	// condisered as not healthy
	TimeoutPercentageCap int `json:"timeout_percentage_cap"`

	//log level
	//default:Error
	LogLevel log.LogLevel `json:"log_level"`

	//stats updating interval in milliseconds
	//default:5 second
	StatsInterval int `json:"stats_interval"`

	// bandwidth usage limit in MB/sec
	BandwidthLimit int `json:"bandwidth_limit"`

	// Compression type - 1: None, 2: Snappy, 3: Auto - REST will be inputting with string coming in
	CompressionType int `json:"compression_type"`

	// revision number to be used by metadata service. not included in json - not currently being used/set
	Revision interface{}
}

type ReplicationMultiValueHelper struct {
	activeConfig  map[string]interface{}
	flagKeyIssued map[string]bool
}

func NewMultiValueHelper() *ReplicationMultiValueHelper {
	return &ReplicationMultiValueHelper{activeConfig: make(map[string]interface{}),
		flagKeyIssued: make(map[string]bool),
	}
}

func (r *ReplicationMultiValueHelper) CheckAndConvertMultiValue(key string, valArr []string) (restKey string, outValArr []string, err error) {
	var settingsConfigKey string
	var newVal interface{}
	for k, v := range MultiValueMap {
		if key == k {
			settingsConfigKey = v
			break
		}
	}
	// This section here is to allow passthrough, in case MultiValueMap returns nothing, this function call
	// would have been transparent to the caller
	restKey = key
	outValArr = valArr
	if len(settingsConfigKey) == 0 {
		return
	}
	restKey = settingsConfigKey
	if _, ok := r.activeConfig[settingsConfigKey]; !ok {
		if config, ok := ReplicationSettingsConfigMap[settingsConfigKey]; ok {
			r.activeConfig[settingsConfigKey] = config.defaultValue
		} else {
			err = fmt.Errorf("CheckAndConvertMultiValue error - invalid activeConfigKey %v", settingsConfigKey)
			return
		}
	}

	switch restKey {
	case FilterExpDelKey:
		newVal, err = r.handleFilterExpDelKey(r.activeConfig[settingsConfigKey].(base.FilterExpDelType), key, valArr[0])
		// Need to set it using newVal to persist because we're passing interface references
		r.activeConfig[settingsConfigKey] = newVal
		outValArr[0] = newVal.(base.FilterExpDelType).String()
	default:
		err = base.ErrorInvalidInput
	}
	return
}

func (r *ReplicationMultiValueHelper) handleFilterExpDelKey(curConfig base.FilterExpDelType, key, val string) (retVal interface{}, err error) {
	boolVal, err := strconv.ParseBool(val)
	if err != nil {
		err = base.IncorrectValueTypeError("a boolean")
		return
	}
	r.flagKeyIssued[key] = boolVal
	switch key {
	case FilterExpKey:
		curConfig.SetSkipExpiration(boolVal)
	case FilterDelKey:
		curConfig.SetSkipDeletes(boolVal)
	case BypassExpiryKey:
		curConfig.SetStripExpiration(boolVal)
	case BypassUncommittedTxnKey:
		curConfig.SetSkipReplicateUncommittedTxn(boolVal)
	}
	retVal = curConfig
	return
}

func (r *ReplicationMultiValueHelper) handleFilterExpDelKeyImport(s *ReplicationSettings, sm ReplicationSettingsMap) {
	// Get the existing value
	var curVal base.FilterExpDelType
	var ok bool
	if curVal, ok = s.Values[FilterExpDelKey].(base.FilterExpDelType); !ok {
		curVal = FilterExpDelConfig.defaultValue.(base.FilterExpDelType)
	}

	if val, ok := r.flagKeyIssued[FilterExpKey]; ok {
		curVal.SetSkipExpiration(val)
	}
	if val, ok := r.flagKeyIssued[FilterDelKey]; ok {
		curVal.SetSkipDeletes(val)
	}
	if val, ok := r.flagKeyIssued[BypassExpiryKey]; ok {
		curVal.SetStripExpiration(val)
	}
	if val, ok := r.flagKeyIssued[BypassUncommittedTxnKey]; ok {
		curVal.SetSkipReplicateUncommittedTxn(val)
	}

	sm[FilterExpDelKey] = curVal
}

// When exporting, we just export the replicationMultiValueHelper, which ReplicationSetting will
// check for and make sure to do the right interaction combinations
func (r *ReplicationMultiValueHelper) ExportToSettingsMap(settings ReplicationSettingsMap) {
	for k, _ := range r.activeConfig {
		settings[k] = *r
	}

	for k, _ := range MultiValueMap {
		delete(settings, k)
	}
}

func (r *ReplicationMultiValueHelper) ImportToReplicationSettings(s *ReplicationSettings, sm ReplicationSettingsMap) {
	for k, _ := range r.activeConfig {
		if k == FilterExpDelKey {
			r.handleFilterExpDelKeyImport(s, sm)
		}
	}
	// Everything else, not implemented
	// Cleanup the older keys
	for k, _ := range MultiValueMap {
		delete(s.Values, k)
	}
}

// config map retriever required by Settings
func GetReplicationSettingsConfigMap() map[string]*SettingsConfig {
	return ReplicationSettingsConfigMap
}

func EmptyReplicationSettings() *ReplicationSettings {
	return &ReplicationSettings{Settings: EmptySettings(GetReplicationSettingsConfigMap)}
}

func DefaultReplicationSettings() *ReplicationSettings {
	defaultSettings := &ReplicationSettings{Settings: DefaultSettings(GetReplicationSettingsConfigMap)}
	defaultSettings.populateFieldsUsingMap()
	return defaultSettings
}

func ValidateReplicationSettingsKey(settingsMap map[string]interface{}) map[string]interface{} {
	return ValidateSettingsKey(settingsMap, ReplicationSettingsConfigMap)
}

func (s *ReplicationSettings) Clone() *ReplicationSettings {
	settings := &ReplicationSettings{Settings: s.Settings.Clone()}
	settings.populateFieldsUsingMap()
	return settings
}

func (s *ReplicationSettings) Redact() *ReplicationSettings {
	if s == nil {
		return s
	}

	if filterExpression, ok := s.Values[FilterExpressionKey]; ok {
		filterExpressionStr := filterExpression.(string)
		if len(filterExpressionStr) > 0 && !base.IsStringRedacted(filterExpressionStr) {
			s.Values[FilterExpressionKey] = base.TagUD(filterExpressionStr)
		}
	}

	if len(s.FilterExpression) > 0 && !base.IsStringRedacted(s.FilterExpression) {
		s.FilterExpression = base.TagUD(s.FilterExpression)
	}

	return s
}

func (s *ReplicationSettings) CloneAndRedact() *ReplicationSettings {
	if s != nil {
		return s.Clone().Redact()
	}
	return s
}

func (s *ReplicationSettings) ToMap(isDefaultSettings bool) ReplicationSettingsMap {
	return s.toMapInternal(isDefaultSettings, false /* hideInternals */)
}

func (s *ReplicationSettings) ToRESTMap(isDefaultSettings bool) ReplicationSettingsMap {
	settingsMap := s.toMapInternal(isDefaultSettings, true /* hideInternals */)
	if filter, ok := settingsMap[FilterExpressionKey]; ok {
		// For UI, we should not show internal XDCR Filtering key or xattributes
		settingsMap[FilterExpressionKey] = base.ReplaceKeyWordsForOutput(filter.(string))
	}
	return settingsMap
}

func (s *ReplicationSettings) toMapInternal(isDefaultSettings bool, hideInternals bool) ReplicationSettingsMap {
	s.exportFlagTypeValues()
	settingsMap := ReplicationSettingsMap(s.Settings.ToMap())
	if isDefaultSettings {
		// remove keys that do not belong to default settings
		for _, key := range ImmutableDefaultSettings {
			delete(settingsMap, key)
		}
	}

	if hideInternals {
		for _, key := range HiddenSettings {
			delete(settingsMap, key)
		}
	}

	// convert logLevel and Priority to string
	settingsMap[PipelineLogLevelKey] = s.Values[PipelineLogLevelKey].(log.LogLevel).String()
	settingsMap[PriorityKey] = s.Values[PriorityKey].(base.PriorityType).String()

	return settingsMap
}

func (s *ReplicationSettings) PostProcessAfterUnmarshalling() {
	if s.Settings == nil {
		// if s.Settings is nil, which could happen during/after upgrade, populate s.Settings using fields in s
		s.populateMapUsingFields()
		// In case the map values are different from fields, which is likely for compression, repopulate fields
		s.populateFieldsUsingMap()
	} else {
		s.Settings.PostProcessAfterUnmarshalling(GetReplicationSettingsConfigMap)

		// special handling
		logLevel := s.Values[PipelineLogLevelKey]
		if logLevel != nil {
			s.Values[PipelineLogLevelKey] = log.LogLevel(logLevel.(int))
		}

		filterVersion := s.Values[FilterVersionKey]
		if filterVersion != nil {
			s.Values[FilterVersionKey] = base.FilterVersionType(filterVersion.(int))
		}

		priority := s.Values[PriorityKey]
		if priority != nil {
			s.Values[PriorityKey] = base.PriorityType(priority.(int))
		}

		filterExpDelMode := s.Values[FilterExpDelKey]
		if filterExpDelMode != nil {
			s.Values[FilterExpDelKey] = base.FilterExpDelType(filterExpDelMode.(int))
		}

		// no need for populateFieldsUsingMap() since fields and map in metakv should already be consistent
	}
	//s.UpgradeFilterIfNeeded()
}

func (s *ReplicationSettings) exportFlagTypeValues() {
	expDelMode := s.GetExpDelMode()
	s.Values[BypassExpiryKey] = expDelMode.IsStripExpirationSet()
	s.Values[BypassUncommittedTxnKey] = expDelMode.IsSkipReplicateUncommittedTxnSet()
	s.Values[FilterExpKey] = expDelMode.IsSkipExpirationSet()
	s.Values[FilterDelKey] = expDelMode.IsSkipDeletesSet()
}

func (s *ReplicationSettings) PreprocessReplMultiValues(settingsMap map[string]interface{}) {
	var mvHelper ReplicationMultiValueHelper
	var found bool
	for _, v := range settingsMap {
		if mvHelper, found = v.(ReplicationMultiValueHelper); found {
			break
		}
	}
	// One issue of mvHelper is enough to restore all potential multiflag values
	mvHelper.ImportToReplicationSettings(s, settingsMap)
}

func (s *ReplicationSettings) UpdateSettingsFromMap(settingsMap map[string]interface{}) (changedSettingsMap ReplicationSettingsMap, errorMap map[string]error) {
	s.PreprocessReplMultiValues(settingsMap)
	changedSettingsMap, errorMap = s.Settings.UpdateSettingsFromMap(settingsMap)
	if len(errorMap) > 0 {
		return
	}
	s.populateFieldsUsingMap()
	return
}

// Modifies the in-memory version of the spec, does not persist changes onto metakv
// This should only be called once, after loading from metakv
func (s *ReplicationSettings) UpgradeFilterIfNeeded(keys []string) []string {
	if len(s.FilterExpression) == 0 {
		return keys
	}

	var versionKeyAppended bool
	if _, ok := s.Values[FilterVersionKey]; !ok {
		// This shouldn't happen... but for now, assume that the filter was input as a key version
		// since spec creation should have entered it as a valid value
		s.Values[FilterVersionKey] = base.FilterVersionKeyOnly
		keys = append(keys, FilterVersionKey)
		versionKeyAppended = true
	}

	if s.Values[FilterVersionKey] == base.FilterVersionKeyOnly {
		s.FilterExpression = base.UpgradeFilter(s.FilterExpression)
		s.Values[FilterExpressionKey] = s.FilterExpression
		s.Values[FilterVersionKey] = base.FilterVersionAdvanced
		if !versionKeyAppended {
			keys = append(keys, FilterVersionKey)
		}
		keys = append(keys, FilterExpressionKey)
	}
	return keys
}

// populate settings map using field values
// this is needed when we load pre-upgrade replication settings from metakv
func (s *ReplicationSettings) populateMapUsingFields() {
	s.Settings = EmptySettings(GetReplicationSettingsConfigMap)
	if s.RepType != ReplicationTypeConfig.defaultValue.(string) {
		s.Values[ReplicationTypeKey] = s.RepType
	}
	if s.FilterExpression != FilterExpressionConfig.defaultValue.(string) {
		s.Values[FilterExpressionKey] = s.FilterExpression
	}
	if s.Active != ActiveConfig.defaultValue.(bool) {
		s.Values[ActiveKey] = s.Active
	}
	if s.CheckpointInterval != CheckpointIntervalConfig.defaultValue.(int) {
		s.Values[CheckpointIntervalKey] = s.CheckpointInterval
	}
	if s.BatchCount != BatchCountConfig.defaultValue.(int) {
		s.Values[BatchCountKey] = s.BatchCount
	}
	if s.BatchSize != BatchSizeConfig.defaultValue.(int) {
		s.Values[BatchSizeKey] = s.BatchSize
	}
	if s.FailureRestartInterval != FailureRestartIntervalConfig.defaultValue.(int) {
		s.Values[FailureRestartIntervalKey] = s.FailureRestartInterval
	}
	if s.OptimisticReplicationThreshold != OptimisticReplicationThresholdConfig.defaultValue.(int) {
		s.Values[OptimisticReplicationThresholdKey] = s.OptimisticReplicationThreshold
	}
	if s.SourceNozzlePerNode != SourceNozzlePerNodeConfig.defaultValue.(int) {
		s.Values[SourceNozzlePerNodeKey] = s.SourceNozzlePerNode
	}
	if s.TargetNozzlePerNode != TargetNozzlePerNodeConfig.defaultValue.(int) {
		s.Values[TargetNozzlePerNodeKey] = s.TargetNozzlePerNode
	}
	if s.LogLevel != PipelineLogLevelConfig.defaultValue.(log.LogLevel) {
		s.Values[PipelineLogLevelKey] = s.LogLevel
	}
	if s.StatsInterval != PipelineStatsIntervalConfig.defaultValue.(int) {
		s.Values[PipelineStatsIntervalKey] = s.StatsInterval
	}
	if s.BandwidthLimit != BandwidthLimitConfig.defaultValue.(int) {
		s.Values[BandwidthLimitKey] = s.BandwidthLimit
	}
	if s.CompressionType != CompressionTypeConfig.defaultValue.(int) {
		s.Values[CompressionTypeKey] = s.GetCompressionType()
	}
}

// populate field values using settings map
// this needs to be done whenever the settings map is changed
// so as to ensure that field values and settings map are in sync
func (s *ReplicationSettings) populateFieldsUsingMap() {
	var value interface{}
	var ok bool
	if value, ok = s.Values[ReplicationTypeKey]; ok {
		s.RepType = value.(string)
	}
	if value, ok = s.Values[FilterExpressionKey]; ok {
		s.FilterExpression = value.(string)
	}
	if value, ok = s.Values[ActiveKey]; ok {
		s.Active = value.(bool)
	}
	if value, ok = s.Values[CheckpointIntervalKey]; ok {
		s.CheckpointInterval = value.(int)
	}
	if value, ok = s.Values[BatchCountKey]; ok {
		s.BatchCount = value.(int)
	}
	if value, ok = s.Values[BatchSizeKey]; ok {
		s.BatchSize = value.(int)
	}
	if value, ok = s.Values[FailureRestartIntervalKey]; ok {
		s.FailureRestartInterval = value.(int)
	}
	if value, ok = s.Values[OptimisticReplicationThresholdKey]; ok {
		s.OptimisticReplicationThreshold = value.(int)
	}
	if value, ok = s.Values[SourceNozzlePerNodeKey]; ok {
		s.SourceNozzlePerNode = value.(int)
	}
	if value, ok = s.Values[TargetNozzlePerNodeKey]; ok {
		s.TargetNozzlePerNode = value.(int)
	}
	if value, ok = s.Values[PipelineLogLevelKey]; ok {
		s.LogLevel = value.(log.LogLevel)
	}
	if value, ok = s.Values[PipelineStatsIntervalKey]; ok {
		s.StatsInterval = value.(int)
	}
	if value, ok = s.Values[BandwidthLimitKey]; ok {
		s.BandwidthLimit = value.(int)
	}
	if value, ok = s.Values[CompressionTypeKey]; ok {
		s.CompressionType = value.(int)
	}
}

func (s *ReplicationSettings) IsCapi() bool {
	return s.RepType == ReplicationTypeCapi
}

func (s *ReplicationSettings) GetPriority() base.PriorityType {
	priority, _ := s.GetSettingValueOrDefaultValue(PriorityKey)
	return priority.(base.PriorityType)
}

func (s *ReplicationSettings) GetDesiredLatencyMs() int {
	return s.GetIntSettingValue(BacklogThresholdKey)
}

func (s *ReplicationSettings) GetCompressionType() int {
	if s.CompressionType < CompressionTypeConfig.MinValue ||
		s.CompressionType > CompressionTypeConfig.MaxValue ||
		s.CompressionType == base.CompressionTypeSnappy {
		return CompressionTypeConfig.defaultValue.(int)
	} else {
		return s.CompressionType
	}
}

func (s *ReplicationSettings) GetExpDelMode() base.FilterExpDelType {
	expDel, _ := s.GetSettingValueOrDefaultValue(base.FilterExpDelKey)
	return expDel.(base.FilterExpDelType)
}

type ReplicationSettingsMap map[string]interface{}

type redactDictType int

const (
	redactDictString      redactDictType = iota
	redactDictBytes       redactDictType = iota
	redactDictStringClear redactDictType = iota // Should be cleared instead of tagged
	redactDictBytesClear  redactDictType = iota // Should be cleared instead of tagged
)

// The dictionary map is a kv pair of KeyNeedsRedacting -> RedactTypeAndOperation
var replicationSettingsMapRedactDict = map[string]redactDictType{FilterExpressionKey: redactDictString,
	XmemCertificate:       redactDictBytes,
	XmemClientKey:         redactDictBytesClear, // Clear the value instead of redaction
	XmemClientCertificate: redactDictBytes}

// Input - the key that is being redacted. Value - the value to be redacted
// The function will redact the value automatically if the key needs to be redacted, otherwise, it will do shallow clone
func (repMap ReplicationSettingsMap) repSettingsMapCloneAndRedactHelper(k string, v interface{}) {
	if redactOpType, ok := replicationSettingsMapRedactDict[k]; ok && v != nil {
		switch redactOpType {
		case redactDictBytes:
			if !base.IsByteSliceRedacted(v.([]byte)) {
				repMap[k] = base.DeepCopyByteArray(v.([]byte))
				repMap[k] = base.TagUDBytes(repMap[k].([]byte))
			} else {
				// Redacted already, so shallow copy already-redacted data
				repMap[k] = v
			}
		case redactDictString:
			if !base.IsStringRedacted(v.(string)) {
				repMap[k] = base.TagUD(v.(string))
			} else {
				repMap[k] = v
			}
		case redactDictBytesClear:
			if len(v.([]byte)) > 0 {
				repMap[k] = []byte{}
			} else {
				repMap[k] = v
			}
		case redactDictStringClear:
			repMap[k] = ""
		default:
			// Shallow copy since we don't know what to do with it
			repMap[k] = v
		}
	} else {
		// Not a key that needs redacting. Do shallow copy
		repMap[k] = v
	}
}

// NOTE: This is currently "cheating" and not cloning if there's nothing to be redacted
func (repMap ReplicationSettingsMap) CloneAndRedact() ReplicationSettingsMap {
	for keyNeedsRedacting, valueType := range replicationSettingsMapRedactDict {
		if setting, keyExistsInSetting := repMap[keyNeedsRedacting]; keyExistsInSetting && setting != nil {
			if ((valueType == redactDictBytes && len(setting.([]byte)) > 0) && !base.IsByteSliceRedacted(setting.([]byte))) ||
				(valueType == redactDictString && len(setting.(string)) > 0 && !base.IsStringRedacted(setting.(string))) ||
				(valueType == redactDictBytesClear && len(setting.([]byte)) > 0) ||
				(valueType == redactDictStringClear && len(setting.(string)) > 0) {
				clonedMap := make(ReplicationSettingsMap)
				// For now, duplicate ReplicationSettings.Redact() logic.
				// In the future, if more things need to be redacted, combine and use a single place to Redact()
				// to avoid having to maintain >1 places
				for k, v := range repMap {
					clonedMap.repSettingsMapCloneAndRedactHelper(k, v)
				}
				return clonedMap
			}
		}
	}
	return repMap
}

func ValidateAndConvertReplicationSettingsValue(key, value, errorKey string, isEnterprise bool, isCapi bool) (convertedValue interface{}, err error) {
	switch key {
	// special cases
	case ReplicationTypeKey:
		if value != ReplicationTypeXmem && value != ReplicationTypeCapi {
			err = base.GenericInvalidValueError(errorKey)
		} else {
			convertedValue = value
		}
	case PipelineLogLevelKey:
		if logLevel, err := log.LogLevelFromStr(value); err != nil {
			err = base.GenericInvalidValueError(errorKey)
		} else {
			convertedValue = logLevel
		}
	case FilterExpressionKey:
		// check that filter expression is a valid regular expression
		err = base.ValidateAdvFilter(value)
		if err != nil {
			return
		}
		convertedValue = value
	case FilterSkipRestreamKey:
		var skip bool
		skip, err = strconv.ParseBool(value)
		if err != nil {
			err = base.IncorrectValueTypeError("a boolean")
			return
		}
		convertedValue = skip
	case ActiveKey:
		var paused bool
		paused, err = strconv.ParseBool(value)
		if err != nil {
			err = base.IncorrectValueTypeError("a boolean")
			return
		}
		convertedValue = !paused
	case BandwidthLimitKey:
		convertedValue, err = ValidateAndConvertSettingsValue(key, value, ReplicationSettingsConfigMap)
		if err != nil {
			return
		}

		if err = enterpriseOnlyFeature(convertedValue.(int), 0, isEnterprise); err != nil {
			return
		}
		if err = nonCAPIOnlyFeature(convertedValue.(int), 0, isCapi); err != nil {
			return
		}
	case CompressionTypeKey:
		if convertedValue, err = base.CompressionStringToCompressionTypeConverter(value); err != nil {
			return
		}
		if err = enterpriseOnlyFeature(convertedValue, base.CompressionTypeNone, isEnterprise); err != nil {
			return
		}
		if err = nonCAPIOnlyFeature(convertedValue, base.CompressionTypeNone, isCapi); err != nil {
			return
		}
	case PriorityKey:
		if convertedValue, err = base.PriorityTypeFromStr(value); err != nil {
			err = base.GenericInvalidValueError(errorKey)
			return
		}
		if err = enterpriseOnlyFeature(convertedValue.(base.PriorityType), base.PriorityTypeHigh, isEnterprise); err != nil {
			return
		}
		if err = nonCAPIOnlyFeature(convertedValue.(base.PriorityType), base.PriorityTypeHigh, isCapi); err != nil {
			return
		}

	case BacklogThresholdKey:
		convertedValue, err = ValidateAndConvertSettingsValue(key, value, ReplicationSettingsConfigMap)
		if err != nil {
			return
		}
		if err = enterpriseOnlyFeature(convertedValue.(int), base.BacklogThresholdDefault, isEnterprise); err != nil {
			return
		}
		if err = nonCAPIOnlyFeature(convertedValue.(int), base.BacklogThresholdDefault, isCapi); err != nil {
			return
		}
	case FilterExpDelKey:
		convertedValue, err = ValidateAndConvertSettingsValue(key, value, ReplicationSettingsConfigMap)
		if err != nil {
			return
		}
		if err = enterpriseOnlyFeature(convertedValue.(base.FilterExpDelType), base.FilterExpDelNone, isEnterprise); err != nil {
			return
		}
		if err = nonCAPIOnlyFeature(convertedValue.(base.FilterExpDelType), base.FilterExpDelNone, isCapi); err != nil {
			return
		}
	default:
		// generic cases that can be handled by ValidateAndConvertSettingsValue
		convertedValue, err = ValidateAndConvertSettingsValue(key, value, ReplicationSettingsConfigMap)
	}

	return
}

// check if the default value of the specified settings can be changed through rest api
// it assumes that the key provided is a valid settings key
func IsSettingDefaultValueMutable(key string) bool {
	for _, setting := range ImmutableDefaultSettings {
		if setting == key {
			return false
		}
	}
	return true
}

// check if the value the specified settings can be changed after replication is created
// it assumes that the key provided is a valid settings key
func IsSettingValueMutable(key string) bool {
	for _, setting := range ImmutableSettings {
		if setting == key {
			return false
		}
	}
	return true
}
