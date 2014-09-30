package metadata

import (
	"errors"
	"fmt"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
	"reflect"
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
	default_max_expected_replication_lag                  = 100
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
	rep_type string `json:"type"`

	//the filter expression
	filter_expression string `json:"filter_exp"`

	//if the replication is active
	//default is true
	active bool `json:"active"`

	//the interval between two checkpoint
	//default: 1800 s
	//range: 60-14400s
	checkpoint_interval int `json:"checkpoint_interval"`

	//the number of mutations in a batch
	//default: 500
	//range: 500-10000
	batch_count int `json:"batch_count"`

	//the size (kb) of a batch
	//default: 2048
	//range: 10-10000
	batch_size int `json:"batch_size"`

	//the number of seconds to wait after failure before restarting
	//default: 30
	//range: 1-300
	failure_restart_interval int `json:"failure_restart_interval"`

	//if the document size (in kb) <optimistic_replication_threshold, replicate optimistically; otherwise replicate pessimistically
	//default: 256
	//range: 0-20*1024*1024
	optimistic_replication_threshold int `json:"optimistic_replication_threshold"`

	//number of maximum simultaneous HTTP connections used for REST protocol
	//default: 20
	//range: 1-100
	http_connection int `json:"http_connection"`

	//the number of nozzles can be used for this replication per source cluster node
	//This together with target_nozzle_per_node controls the parallism of the replication
	//default: 2
	//range: 1-10
	source_nozzle_per_node int `json:"source_nozzle_per_node"`

	//the number of nozzles can be used for this replication per target cluster node
	//This together with source_nozzle_per_node controls the parallism of the replication
	//default: 2
	//range: 1-10
	target_nozzle_per_node int `json:"target_nozzle_per_node"`

	//the max replication lag (in ms) that user can tolerant for this replication
	//
	//Note: if the actual replication lag is larger than this value, it is consider as timeout
	//default: 100ms
	max_expected_replication_lag int `json:"max_expected_replication_lag"`

	// The max allowed timeout percentage. Exceed that limit, piepline would be
	// condisered as not healthy
	timeout_percentage_cap int `json:"timeout_percentage_cap"`

	//log level
	log_level log.LogLevel
}

func DefaultSettings() *ReplicationSettings {
	return &ReplicationSettings{filter_expression: default_filter_expression,
		active:                           default_active,
		checkpoint_interval:              default_checkpoint_interval,
		batch_count:                      default_batch_count,
		batch_size:                       default_batch_size,
		failure_restart_interval:         default_failure_restart_interval,
		optimistic_replication_threshold: default_optimistic_replication_threshold,
		http_connection:                  default_http_connection,
		source_nozzle_per_node:           default_source_nozzle_per_node,
		target_nozzle_per_node:           default_target_nozzle_per_node,
		max_expected_replication_lag:     default_max_expected_replication_lag,
		timeout_percentage_cap:           default_timeout_percentage_cap,
		log_level:                        default_pipeline_log_level}
}

func (s *ReplicationSettings) Type() string {
	return s.rep_type
}

func (s *ReplicationSettings) SetType(repType string) {
	s.rep_type = repType
}

func (s *ReplicationSettings) CheckpointInterval() int {
	return s.checkpoint_interval
}

func (s *ReplicationSettings) SetCheckpointInterval(interval int) {
	s.checkpoint_interval = interval
}

func (s *ReplicationSettings) BatchCount() int {
	return s.batch_count
}

func (s *ReplicationSettings) SetBatchCount(batch_count int) {
	s.batch_count = batch_count
}

func (s *ReplicationSettings) BatchSize() int {
	return s.batch_size
}

func (s *ReplicationSettings) SetBatchSize(batch_size int) {
	s.batch_size = batch_size
}

func (s *ReplicationSettings) FailureRestartInterval() int {
	return s.failure_restart_interval
}

func (s *ReplicationSettings) SetFailureRestartInterval(interval int) {
	s.failure_restart_interval = interval
}

func (s *ReplicationSettings) OptimisticReplicationThreshold() int {
	return s.optimistic_replication_threshold
}

func (s *ReplicationSettings) SetOptimisticReplicationThreshold(threshold int) {
	s.optimistic_replication_threshold = threshold
}

func (s *ReplicationSettings) HttpConnection() int {
	return s.http_connection
}

func (s *ReplicationSettings) SetHttpConnection(httpConn int) {
	s.http_connection = httpConn
}

func (s *ReplicationSettings) SourceNozzlesPerNode() int {
	return s.source_nozzle_per_node
}

func (s *ReplicationSettings) SetSourceNozzlesPerNode(numOfNozzles int) {
	s.source_nozzle_per_node = numOfNozzles
}

func (s *ReplicationSettings) TargetNozzlesPerNode() int {
	return s.target_nozzle_per_node
}

func (s *ReplicationSettings) SetTargetNozzlesPerNode(numOfNozzles int) {
	s.target_nozzle_per_node = numOfNozzles
}

func (s *ReplicationSettings) MaxExpectedReplicationLag() int {
	return s.max_expected_replication_lag
}

func (s *ReplicationSettings) SetMaxExpectedReplicationLag(lag int) {
	s.max_expected_replication_lag = lag
}

func (s *ReplicationSettings) FilterExpression() string {
	return s.filter_expression
}

func (s *ReplicationSettings) SetFilterExpression(filterExp string) {
	s.filter_expression = filterExp
}

func (s *ReplicationSettings) TimeoutPercentageCap() int {
	return s.timeout_percentage_cap
}

func (s *ReplicationSettings) SetTimeoutPercentageCap(cap int) {
	s.timeout_percentage_cap = cap
}

func (s *ReplicationSettings) IsActive() bool {
	return s.active
}

func (s *ReplicationSettings) SetActive(active bool) {
	s.active = active
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
			replType, ok := val.(string)
			if !ok {
				return IncorrectValueTypeInMapError(key, val, "string")
			}
			s.SetType(replType)
		case FilterExpression:
			filterExpression, ok := val.(string)
			if !ok {
				return IncorrectValueTypeInMapError(key, val, "string")
			}
			s.SetFilterExpression(filterExpression)
		case Active:
			active, ok := val.(bool)
			if !ok {
				return IncorrectValueTypeInMapError(key, val, "bool")
			}
			s.SetActive(active)
		case CheckpointInterval:
			checkpointInterval, ok := val.(int)
			if !ok {
				return IncorrectValueTypeInMapError(key, val, "int")
			}
			s.SetCheckpointInterval(checkpointInterval)
		case BatchCount:
			batchCount, ok := val.(int)
			if !ok {
				return IncorrectValueTypeInMapError(key, val, "int")
			}
			s.SetBatchCount(batchCount)
		case BatchSize:
			batchSize, ok := val.(int)
			if !ok {
				return IncorrectValueTypeInMapError(key, val, "int")
			}
			s.SetBatchSize(batchSize)
		case FailureRestartInterval:
			failureRestartInterval, ok := val.(int)
			if !ok {
				return IncorrectValueTypeInMapError(key, val, "int")
			}
			s.SetFailureRestartInterval(failureRestartInterval)
		case OptimisticReplicationThreshold:
			optimisticReplicationThreshold, ok := val.(int)
			if !ok {
				return IncorrectValueTypeInMapError(key, val, "int")
			}
			s.SetOptimisticReplicationThreshold(optimisticReplicationThreshold)
		case HttpConnection:
			httpConnection, ok := val.(int)
			if !ok {
				return IncorrectValueTypeInMapError(key, val, "int")
			}
			s.SetHttpConnection(httpConnection)
		case SourceNozzlePerNode:
			sourceNozzlePerNode, ok := val.(int)
			if !ok {
				return IncorrectValueTypeInMapError(key, val, "int")
			}
			s.SetSourceNozzlesPerNode(sourceNozzlePerNode)
		case TargetNozzlePerNode:
			targetNozzlePerNode, ok := val.(int)
			if !ok {
				return IncorrectValueTypeInMapError(key, val, "int")
			}
			s.SetTargetNozzlesPerNode(targetNozzlePerNode)
		case MaxExpectedReplicationLag:
			maxExpectedReplicationLag, ok := val.(int)
			if !ok {
				return IncorrectValueTypeInMapError(key, val, "int")
			}
			s.SetMaxExpectedReplicationLag(maxExpectedReplicationLag)
		case TimeoutPercentageCap:
			timeoutPercentageCap, ok := val.(int)
			if !ok {
				return IncorrectValueTypeInMapError(key, val, "int")
			}
			s.SetTimeoutPercentageCap(timeoutPercentageCap)
		case PipelineLogLevel:
			l, ok := val.(log.LogLevel)
			if !ok {
				return IncorrectValueTypeInMapError(key, val, "log.LogLevel")
			}
			s.setLogLevel(l)
		default:
			return errors.New(fmt.Sprintf("Invalid key in map, %v", key))

		}
	}

	return nil
}

func (s *ReplicationSettings) LogLevel() log.LogLevel {
	return s.log_level
}

func (s *ReplicationSettings) SetLogLevel(log_level string) error {
	l, err := log.LogLevelFromStr(log_level)
	if err == nil {
		s.setLogLevel(l)
	}
	return err
}

func (s *ReplicationSettings) setLogLevel (l log.LogLevel)  {
		s.log_level = l
}

func (s *ReplicationSettings) ToMap() map[string]interface{} {
	settings_map := make(map[string]interface{})
	settings_map[ReplicationType] = s.Type()
	settings_map[FilterExpression] = s.FilterExpression()
	settings_map[Active] = s.IsActive()
	settings_map[CheckpointInterval] = s.CheckpointInterval()
	settings_map[BatchCount] = s.BatchCount()
	settings_map[BatchSize] = s.BatchSize()
	settings_map[FailureRestartInterval] = s.FailureRestartInterval()
	settings_map[OptimisticReplicationThreshold] = s.OptimisticReplicationThreshold()
	settings_map[HttpConnection] = s.HttpConnection()
	settings_map[SourceNozzlePerNode] = s.SourceNozzlesPerNode()
	settings_map[TargetNozzlePerNode] = s.TargetNozzlesPerNode()
	settings_map[MaxExpectedReplicationLag] = s.MaxExpectedReplicationLag()
	settings_map[TimeoutPercentageCap] = s.TimeoutPercentageCap()
	settings_map[PipelineLogLevel] = s.LogLevel()
	return settings_map
}

func IncorrectValueTypeInMapError(key string, val interface{}, expectedType string) error {
	return errors.New(fmt.Sprintf("Value, %v, for settings with key, %v, has incorrect data type. Expected type: %v. Actual type: %v", val, key, expectedType, reflect.TypeOf(val)))
}
