// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package metadata

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"regexp"
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
)

// keys to facilitate redaction of replication settings map
const (
	XmemCertificate       = "certificate"
	XmemClientCertificate = "clientCertificate"
	XmemClientKey         = "clientKey"
)

// settings whose default values cannot be viewed or changed through rest apis
var ImmutableDefaultSettings = []string{ReplicationTypeKey, FilterExpressionKey, ActiveKey}

// settings whose values cannot be changed after replication is created
var ImmutableSettings = []string{FilterExpressionKey}

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
var CompressionTypeConfig = &SettingsConfig{base.CompressionTypeAuto, &Range{base.CompressionTypeStartMarker + 1, base.CompressionTypeEndMarker - 1}}

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
}

type ReplicationSettings struct {
	*Settings

	//type - XMEM or CAPI
	RepType string `json:"type"`

	//the filter expression
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

	// Compression type - 0: None, 1: Snappy - REST will be inputting with string coming in
	CompressionType int `json:"compression_type"`

	// revision number to be used by metadata service. not included in json - not currently being used/set
	Revision interface{}
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
	settingsMap := ReplicationSettingsMap(s.Settings.ToMap())
	if isDefaultSettings {
		// remove keys that do not belong to default settings
		for _, key := range ImmutableDefaultSettings {
			delete(settingsMap, key)
		}
	}

	// convert log level to string
	settingsMap[PipelineLogLevelKey] = s.Values[PipelineLogLevelKey].(log.LogLevel).String()

	return settingsMap
}

func (s *ReplicationSettings) PostProcessAfterUnmarshalling() {
	if s.Settings == nil {
		// if s.Settings is nil, which could happen during/after upgrade, populate s.Settings using fields in s
		s.populateMapUsingFields()
	} else {
		s.Settings.PostProcessAfterUnmarshalling(GetReplicationSettingsConfigMap)

		// special handling
		logLevel := s.Values[PipelineLogLevelKey]
		if logLevel != nil {
			s.Values[PipelineLogLevelKey] = log.LogLevel(logLevel.(int))
		}

		// no need for populateFieldsUsingMap() since fields and map in metakv should already be consistent
	}
	s.HandleUpgrade()
}

func (s *ReplicationSettings) UpdateSettingsFromMap(settingsMap map[string]interface{}) (changedSettingsMap ReplicationSettingsMap, errorMap map[string]error) {
	changedSettingsMap, errorMap = s.Settings.UpdateSettingsFromMap(settingsMap)
	if len(errorMap) > 0 {
		return
	}
	s.populateFieldsUsingMap()
	return
}

func (s *ReplicationSettings) SetCompressionType(compressionType int) {
	s.Values[CompressionTypeKey] = compressionType
	s.CompressionType = compressionType
}

// populate settings map using field values
// this is needed when we load pre-upgrade replication settings from metakv
func (s *ReplicationSettings) populateMapUsingFields() {
	s.Settings = EmptySettings(GetReplicationSettingsConfigMap)
	s.Values[ReplicationTypeKey] = s.RepType
	s.Values[FilterExpressionKey] = s.FilterExpression
	s.Values[ActiveKey] = s.Active
	s.Values[CheckpointIntervalKey] = s.CheckpointInterval
	s.Values[BatchCountKey] = s.BatchCount
	s.Values[BatchSizeKey] = s.BatchSize
	s.Values[FailureRestartIntervalKey] = s.FailureRestartInterval
	s.Values[OptimisticReplicationThresholdKey] = s.OptimisticReplicationThreshold
	s.Values[SourceNozzlePerNodeKey] = s.SourceNozzlePerNode
	s.Values[TargetNozzlePerNodeKey] = s.TargetNozzlePerNode
	s.Values[PipelineLogLevelKey] = s.LogLevel
	s.Values[PipelineStatsIntervalKey] = s.StatsInterval
	s.Values[BandwidthLimitKey] = s.BandwidthLimit
	s.Values[CompressionTypeKey] = s.CompressionType
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
		_, err = regexp.Compile(value)
		if err != nil {
			return
		}
		convertedValue = value
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
		if convertedValue, err = base.CompressionStringToConversionTypeConverter(value); err != nil {
			return
		}
		if err = enterpriseOnlyFeature(convertedValue, base.CompressionTypeNone, isEnterprise); err != nil {
			return
		}
		if err = nonCAPIOnlyFeature(convertedValue, base.CompressionTypeNone, isCapi); err != nil {
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
	mutable := true
	for _, setting := range ImmutableDefaultSettings {
		if setting == key {
			mutable = false
			break
		}
	}
	return mutable
}

// check if the value the specified settings can be changed after replication is created
// it assumes that the key provided is a valid settings key
func IsSettingValueMutable(key string) bool {
	mutable := true
	for _, setting := range ImmutableSettings {
		if setting == key {
			mutable = false
			break
		}
	}
	return mutable
}
