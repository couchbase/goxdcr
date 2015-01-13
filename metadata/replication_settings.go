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
	"errors"
	"fmt"
	"strconv"
	"regexp"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/utils"
)

const (
	ReplicationType                = "replication_type"
	FilterExpression               = "filter_expression"
	Active                         = "active"
	CheckpointInterval             = "checkpoint_interval"
	BatchCount                     = "worker_batch_size"
	BatchSize                      = "doc_batch_size_kb"
	FailureRestartInterval         = "failure_restart_interval"
	OptimisticReplicationThreshold = "optimistic_replication_threshold"
	SourceNozzlePerNode            = "source_nozzle_per_node"
	TargetNozzlePerNode            = "target_nozzle_per_node"
	MaxExpectedReplicationLag      = "max_expected_replication_lag"
	TimeoutPercentageCap           = "timeout_percentage_cap"
	PipelineLogLevel               = "log_level"
	PipelineStatsInterval          = "stats_interval"
)

const (
	ReplicationTypeXmem = "xmem"
	ReplicationTypeCapi = "capi"
)

type SettingsConfig struct {
	defaultValue  interface{}
	*Range
}

type Range struct {
	minValue   int
	maxValue   int
}

// TODO change to "capi"?
var ReplicationTypeConfig = &SettingsConfig{ReplicationTypeXmem, nil}
var FilterExpressionConfig = &SettingsConfig{"", nil}
var ActiveConfig = &SettingsConfig{true, nil}
var CheckpointIntervalConfig = &SettingsConfig{60, &Range{60, 14400}}
var BatchCountConfig = &SettingsConfig{500, &Range{500, 10000}}
var BatchSizeConfig = &SettingsConfig{2048, &Range{10, 10000}}
var FailureRestartIntervalConfig = &SettingsConfig{30, &Range{1, 300}}
var OptimisticReplicationThresholdConfig = &SettingsConfig{256, &Range{0, 20*1024*1024}}
var SourceNozzlePerNodeConfig = &SettingsConfig{2, &Range{1, 10}}
var TargetNozzlePerNodeConfig = &SettingsConfig{2, &Range{1, 10}}
var MaxExpectedReplicationLagConfig = &SettingsConfig{1000, nil}
var TimeoutPercentageCapConfig = &SettingsConfig{50, &Range{0, 100}}
var PipelineLogLevelConfig = &SettingsConfig{log.LogLevelInfo, nil}
var PipelineStatsIntervalConfig = &SettingsConfig{10000, nil}

var SettingsConfigMap = map[string]*SettingsConfig{
	ReplicationType: ReplicationTypeConfig,
	FilterExpression: FilterExpressionConfig,
	Active: ActiveConfig,
	CheckpointInterval: CheckpointIntervalConfig,
	BatchCount: BatchCountConfig,
	BatchSize: BatchSizeConfig,
	FailureRestartInterval: FailureRestartIntervalConfig,
	OptimisticReplicationThreshold: OptimisticReplicationThresholdConfig,
	SourceNozzlePerNode: SourceNozzlePerNodeConfig,
	TargetNozzlePerNode: TargetNozzlePerNodeConfig,
	MaxExpectedReplicationLag: MaxExpectedReplicationLagConfig,
	TimeoutPercentageCap: TimeoutPercentageCapConfig,
	PipelineLogLevel: PipelineLogLevelConfig,
	PipelineStatsInterval: PipelineStatsIntervalConfig,
}

/***********************************
/* struct ReplicationSettings
*************************************/
type ReplicationSettings struct {
	//type - XMEM or CAPI
	RepType string `json:"type"`

	//the filter expression
	FilterExpression string `json:"filter_exp"`

	//if the replication is active
	//default is true
	Active bool `json:"active"`

	//the interval between two checkpoint
	//default: 1800 s
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
	
    // revision number to be used by metadata service. not included in json
	Revision  interface{}
}

func DefaultSettings() *ReplicationSettings {
	return &ReplicationSettings{
		RepType: ReplicationTypeConfig.defaultValue.(string),
		FilterExpression: FilterExpressionConfig.defaultValue.(string),
		Active:                         ActiveConfig.defaultValue.(bool),
		CheckpointInterval:             CheckpointIntervalConfig.defaultValue.(int),
		BatchCount:                     BatchCountConfig.defaultValue.(int),
		BatchSize:                      BatchSizeConfig.defaultValue.(int),
		FailureRestartInterval:         FailureRestartIntervalConfig.defaultValue.(int),
		OptimisticReplicationThreshold: OptimisticReplicationThresholdConfig.defaultValue.(int),
		SourceNozzlePerNode:            SourceNozzlePerNodeConfig.defaultValue.(int),
		TargetNozzlePerNode:            TargetNozzlePerNodeConfig.defaultValue.(int),
		MaxExpectedReplicationLag:      MaxExpectedReplicationLagConfig.defaultValue.(int),
		TimeoutPercentageCap:           TimeoutPercentageCapConfig.defaultValue.(int),
		LogLevel:                       PipelineLogLevelConfig.defaultValue.(log.LogLevel),
		StatsInterval:                  PipelineStatsIntervalConfig.defaultValue.(int),
	}
}

func (s *ReplicationSettings) SetLogLevel(log_level string) error {
	l, err := log.LogLevelFromStr(log_level)
	if err == nil {
		s.LogLevel = l
	}
	return err
}

// returns true if real changes are done. false otherwise

// normally this method should return an empty errorsMap since the input settingsMap 
// is constructed internally and necessary checks should have been applied then
// I am leaving the error checks just in case.
func (s *ReplicationSettings) UpdateSettingsFromMap(settingsMap map[string]interface{}) (map[string]error, bool) {
	errorMap := make(map[string]error)
	changed := false
	for key, val := range settingsMap {		
		switch key {
		case ReplicationType:
			repType, ok := val.(string)
			if !ok {
				errorMap[key] = utils.IncorrectValueTypeInMapError(key, val, "string")
				continue
			}
			if s.RepType != repType {
				changed = true
				s.RepType = repType
			}
		case FilterExpression:
			filterExpression, ok := val.(string)
			if !ok {
				errorMap[key] = utils.IncorrectValueTypeInMapError(key, val, "string")
				continue
			}
			if s.FilterExpression != filterExpression {
				errorMap[key] = errors.New("Filter expression cannot be changed after the replication is created")
			}
		case Active:
			active, ok := val.(bool)
			if !ok {
				errorMap[key] = utils.IncorrectValueTypeInMapError(key, val, "bool")
				continue
			}
			if s.Active != active {
				changed = true
				s.Active = active
			}
		case CheckpointInterval:
			checkpointInterval, ok := val.(int)
			if !ok {
				errorMap[key] = utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.CheckpointInterval != checkpointInterval {
				changed = true
				s.CheckpointInterval = checkpointInterval
			}
			
		case BatchCount:
			batchCount, ok := val.(int)
			if !ok {
				errorMap[key] = utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.BatchCount != batchCount {
				changed = true
				s.BatchCount = batchCount
			}
		case BatchSize:
			batchSize, ok := val.(int)
			if !ok {
				errorMap[key] = utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.BatchSize != batchSize {
				changed = true
				s.BatchSize = batchSize
			}
		case FailureRestartInterval:
			failureRestartInterval, ok := val.(int)
			if !ok {
				errorMap[key] = utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.FailureRestartInterval != failureRestartInterval {
				changed = true
				s.FailureRestartInterval = failureRestartInterval
			}
		case OptimisticReplicationThreshold:
			optimisticReplicationThreshold, ok := val.(int)
			if !ok {
				errorMap[key] = utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.OptimisticReplicationThreshold != optimisticReplicationThreshold {
				changed = true
				s.OptimisticReplicationThreshold = optimisticReplicationThreshold
			}
		case SourceNozzlePerNode:
			sourceNozzlePerNode, ok := val.(int)
			if !ok {
				errorMap[key] = utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.SourceNozzlePerNode != sourceNozzlePerNode {
				changed = true
				s.SourceNozzlePerNode = sourceNozzlePerNode
			}
		case TargetNozzlePerNode:
			targetNozzlePerNode, ok := val.(int)
			if !ok {
				errorMap[key] = utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.TargetNozzlePerNode != targetNozzlePerNode {
				changed = true
				s.TargetNozzlePerNode = targetNozzlePerNode
			}		
		case MaxExpectedReplicationLag:
			maxExpectedReplicationLag, ok := val.(int)
			if !ok {
				errorMap[key] = utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.MaxExpectedReplicationLag != maxExpectedReplicationLag {
				changed = true
				s.MaxExpectedReplicationLag = maxExpectedReplicationLag
			}			
		case TimeoutPercentageCap:
			timeoutPercentageCap, ok := val.(int)
			if !ok {
				errorMap[key] = utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if 	s.TimeoutPercentageCap != timeoutPercentageCap {
				changed = true
				s.TimeoutPercentageCap = timeoutPercentageCap
			}	
		case PipelineLogLevel:
			l, ok := val.(string)
			if !ok {
				errorMap[key] = utils.IncorrectValueTypeInMapError(key, val, "string")
				continue
			}
			if 	s.LogLevel.String() != l {
				changed = true
				s.SetLogLevel(l)
			}			
		case PipelineStatsInterval:
			interval, ok := val.(int)
			if !ok {
				errorMap[key] = utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if 	s.StatsInterval != interval {
				changed = true
				s.StatsInterval = interval
			}
		default:
			errorMap[key] = errors.New(fmt.Sprintf("Invalid key in map, %v", key))

		}
	}

	return errorMap, changed
}

func (s *ReplicationSettings) ToMap() map[string]interface{} {
	settings_map := make(map[string]interface{})
	settings_map[ReplicationType] = s.RepType
	settings_map[FilterExpression] = s.FilterExpression
	settings_map[Active] = s.Active
	settings_map[CheckpointInterval] = s.CheckpointInterval
	settings_map[BatchCount] = s.BatchCount
	settings_map[BatchSize] = s.BatchSize
	settings_map[FailureRestartInterval] = s.FailureRestartInterval
	settings_map[OptimisticReplicationThreshold] = s.OptimisticReplicationThreshold
	settings_map[SourceNozzlePerNode] = s.SourceNozzlePerNode
	settings_map[TargetNozzlePerNode] = s.TargetNozzlePerNode
	settings_map[MaxExpectedReplicationLag] = s.MaxExpectedReplicationLag
	settings_map[TimeoutPercentageCap] = s.TimeoutPercentageCap
	settings_map[PipelineLogLevel] = s.LogLevel.String()
	settings_map[PipelineStatsInterval] = s.StatsInterval
	return settings_map
}

func ValidateAndConvertSettingsValue(key string, value string) (convertedValue interface{}, err error) {
	switch key {
		case ReplicationType, PipelineLogLevel:
			// string parameters need no conversion
			convertedValue = value
		case FilterExpression:
			// check that filter expression is a valid regular expression
			_, err = regexp.Compile(value)
			if err != nil {
				return
			}
			convertedValue = value
		case Active:
			var paused bool
			paused, err = strconv.ParseBool(value)
			if err != nil {
				err = utils.IncorrectValueTypeError("a boolean")
				return
			}
			convertedValue = !paused

		case CheckpointInterval, BatchCount, BatchSize, FailureRestartInterval, 
			 OptimisticReplicationThreshold, SourceNozzlePerNode, 
			 TargetNozzlePerNode,  MaxExpectedReplicationLag, TimeoutPercentageCap, 
			 PipelineStatsInterval	:
			convertedValue, err = strconv.ParseInt(value, base.ParseIntBase, base.ParseIntBitSize)
			if err != nil {
				err = utils.IncorrectValueTypeError("an integer")
				return
			}
			
			// convert it to int to make future processing easier
			convertedValue = int(convertedValue.(int64))
			
			// range check for int parameters
			settingsConfig, _ := SettingsConfigMap[key]
			if settingsConfig.Range != nil {
				intValue := convertedValue.(int)
				if intValue < settingsConfig.Range.minValue || intValue > settingsConfig.Range.maxValue {
					err =  utils.InvalidValueError("an integer", settingsConfig.Range.minValue, settingsConfig.Range.maxValue)
					return
				}
			}
		default:
			// a nil converted value indicates that the key is not a settings key
			convertedValue = nil
	}
		
	return		
}

