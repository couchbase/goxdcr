package metadata

import (
	"errors"
	"fmt"
	"github.com/Xiaomei-Zhang/goxdcr/log"
	"github.com/Xiaomei-Zhang/goxdcr_impl/utils"
)

const (
	default_checkpoint_interval              int          = 1800
	default_batch_count                      int          = 500
	default_batch_size                       int          = 2048
	default_failure_restart_interval         int          = 30
	default_optimistic_replication_threshold              = 256
	default_http_connection                               = 20
	default_source_nozzle_per_node                        = 2
	default_target_nozzle_per_node                        = 2
	default_max_expected_replication_lag                  = 1000
	default_timeout_percentage_cap                        = 80 // TODO is this ok?
	default_filter_expression                string       = ""
	default_replication_type                 string       = "capi"
	default_active                           bool         = true
	default_pipeline_log_level               log.LogLevel = log.LogLevelInfo
)

const (
	ReplicationType                = "replication_type"
	FilterExpression               = "filter_expression"
	Active                         = "active"
	CheckpointInterval             = "checkpoint_interval"
	BatchCount                     = "batch_count"
	BatchSize                      = "batch_size"
	FailureRestartInterval         = "failure_restart_interval"
	OptimisticReplicationThreshold = "optimistic_replication_threshold"
	HttpConnection                 = "http_connection"
	SourceNozzlePerNode            = "source_nozzle_per_node"
	TargetNozzlePerNode            = "target_nozzle_per_node"
	MaxExpectedReplicationLag      = "max_expected_replication_lag"
	TimeoutPercentageCap           = "timeout_percentage_cap"
	PipelineLogLevel               = "log_level"
)

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

	//if the document size (in kb) <optimistic_replication_threshold, replicate optimistically; otherwise replicate pessimistically
	//default: 256
	//range: 0-20*1024*1024
	OptimisticReplicationThreshold int `json:"optimistic_replication_threshold"`

	//number of maximum simultaneous HTTP connections used for REST protocol
	//default: 20
	//range: 1-100
	HttpConnection int `json:"http_connection"`

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
	LogLevel log.LogLevel  `json:"log_level"`
}

func DefaultSettings() *ReplicationSettings {
	return &ReplicationSettings{FilterExpression: default_filter_expression,
		Active:                           default_active,
		CheckpointInterval:              default_checkpoint_interval,
		BatchCount:                      default_batch_count,
		BatchSize:                       default_batch_size,
		FailureRestartInterval:         default_failure_restart_interval,
		OptimisticReplicationThreshold: default_optimistic_replication_threshold,
		HttpConnection:                  default_http_connection,
		SourceNozzlePerNode:           default_source_nozzle_per_node,
		TargetNozzlePerNode:           default_target_nozzle_per_node,
		MaxExpectedReplicationLag:     default_max_expected_replication_lag,
		TimeoutPercentageCap:           default_timeout_percentage_cap,
		LogLevel:                        default_pipeline_log_level}
}

func (s *ReplicationSettings) SetLogLevel(log_level string) error {
	l, err := log.LogLevelFromStr(log_level)
	if err == nil {
		s.LogLevel = l
	}
	return err
}

func SettingsFromMap(settingsMap map[string]interface{}) (*ReplicationSettings, error) {
	settings := DefaultSettings()

	err := settings.UpdateSettingsFromMap(settingsMap)
	if err == nil {
		return settings, nil
	} else {
		return nil, err
	}
}

func (s *ReplicationSettings) UpdateSettingsFromMap(settingsMap map[string]interface{}) error {
	for key, val := range settingsMap {
		switch key {
		case ReplicationType:
			repType, ok := val.(string)
			if !ok {
				return utils.IncorrectValueTypeInMapError(key, val, "string")
			}
			s.RepType = repType
		case FilterExpression:
			filterExpression, ok := val.(string)
			if !ok {
				return utils.IncorrectValueTypeInMapError(key, val, "string")
			}
			s.FilterExpression = filterExpression
		case Active:
			active, ok := val.(bool)
			if !ok {
				return utils.IncorrectValueTypeInMapError(key, val, "bool")
			}
			s.Active = active
		case CheckpointInterval:
			checkpointInterval, ok := val.(int)
			if !ok {
				return utils.IncorrectValueTypeInMapError(key, val, "int")
			}
			s.CheckpointInterval = checkpointInterval
		case BatchCount:
			batchCount, ok := val.(int)
			if !ok {
				return utils.IncorrectValueTypeInMapError(key, val, "int")
			}
			s.BatchCount = batchCount
		case BatchSize:
			batchSize, ok := val.(int)
			if !ok {
				return utils.IncorrectValueTypeInMapError(key, val, "int")
			}
			s.BatchSize = batchSize
		case FailureRestartInterval:
			failureRestartInterval, ok := val.(int)
			if !ok {
				return utils.IncorrectValueTypeInMapError(key, val, "int")
			}
			s.FailureRestartInterval = failureRestartInterval
		case OptimisticReplicationThreshold:
			optimisticReplicationThreshold, ok := val.(int)
			if !ok {
				return utils.IncorrectValueTypeInMapError(key, val, "int")
			}
			s.OptimisticReplicationThreshold = optimisticReplicationThreshold
		case HttpConnection:
			httpConnection, ok := val.(int)
			if !ok {
				return utils.IncorrectValueTypeInMapError(key, val, "int")
			}
			s.HttpConnection = httpConnection
		case SourceNozzlePerNode:
			sourceNozzlePerNode, ok := val.(int)
			if !ok {
				return utils.IncorrectValueTypeInMapError(key, val, "int")
			}
			s.SourceNozzlePerNode = sourceNozzlePerNode
		case TargetNozzlePerNode:
			targetNozzlePerNode, ok := val.(int)
			if !ok {
				return utils.IncorrectValueTypeInMapError(key, val, "int")
			}
			s.TargetNozzlePerNode = targetNozzlePerNode
		case MaxExpectedReplicationLag:
			maxExpectedReplicationLag, ok := val.(int)
			if !ok {
				return utils.IncorrectValueTypeInMapError(key, val, "int")
			}
			s.MaxExpectedReplicationLag = maxExpectedReplicationLag
		case TimeoutPercentageCap:
			timeoutPercentageCap, ok := val.(int)
			if !ok {
				return utils.IncorrectValueTypeInMapError(key, val, "int")
			}
			s.TimeoutPercentageCap = timeoutPercentageCap
		case PipelineLogLevel:
			l, ok := val.(string)
			if !ok {
				return utils.IncorrectValueTypeInMapError(key, val, "string")
			}
			s.SetLogLevel(l)
		default:
			return errors.New(fmt.Sprintf("Invalid key in map, %v", key))

		}
	}

	return nil
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
	settings_map[HttpConnection] = s.HttpConnection
	settings_map[SourceNozzlePerNode] = s.SourceNozzlePerNode
	settings_map[TargetNozzlePerNode] = s.TargetNozzlePerNode
	settings_map[MaxExpectedReplicationLag] = s.MaxExpectedReplicationLag
	settings_map[TimeoutPercentageCap] = s.TimeoutPercentageCap
	settings_map[PipelineLogLevel] = s.LogLevel.String()
	return settings_map
}
