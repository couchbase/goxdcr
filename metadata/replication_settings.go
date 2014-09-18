package metadata

import ()

const (
	default_checkpoint_interval              int = 1800
	default_batch_count                      int = 500
	default_batch_size                       int = 2048
	default_failure_restart_interval         int = 30
	default_optimistic_replication_threshold     = 256
	default_http_connection                      = 20
	default_source_nozzle_per_node               = 2
	default_target_nozzle_per_node               = 2
	default_max_expected_replication_lag         = 100
	default_filter_expression string			 = ""
	default_active bool							 = true
)

/************************************
/* struct ReplicationSettings
*************************************/
type ReplicationSettings struct {

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
}

func DefaultSettings() *ReplicationSettings {
	return &ReplicationSettings{filter_expression : default_filter_expression,
		active: default_active,
		checkpoint_interval: default_checkpoint_interval,
		batch_count:                      default_batch_count,
		batch_size:                       default_batch_size,
		failure_restart_interval:         default_failure_restart_interval,
		optimistic_replication_threshold: default_optimistic_replication_threshold,
		http_connection:                  default_http_connection,
		source_nozzle_per_node:           default_source_nozzle_per_node,
		target_nozzle_per_node:           default_target_nozzle_per_node,
		max_expected_replication_lag:     default_max_expected_replication_lag}
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

func (s *ReplicationSettings) FilterExpression () string {
	return s.filter_expression
}

func (s *ReplicationSettings) SetFilterExpression (filterExp string) {
	s.filter_expression = filterExp
}

func (s *ReplicationSettings) IsActive() bool {
	return s.active
}

func (s *ReplicationSettings) SetActive (active bool) {
	s.active = active
}

func SettingsFromMap(settingsMap map[string]interface{}) *ReplicationSettings {
	settings := DefaultSettings()
	
	if val, ok := settingsMap["filter_expression"]; ok {
		settings.SetFilterExpression(val.(string))
	}
	
	if val, ok := settingsMap["active"]; ok {
		settings.SetActive (val.(bool))
	}
	
	if val, ok := settingsMap["checkpoint_interval"]; ok {
		settings.SetCheckpointInterval(val.(int))
	}
	
	if val, ok := settingsMap["batch_count"]; ok {
		settings.SetBatchCount(val.(int))
	}
	
	if val, ok := settingsMap["batch_size"]; ok {
		settings.SetBatchSize(val.(int))
	}
	
	if val, ok := settingsMap["failure_restart_interval"]; ok {
		settings.SetFailureRestartInterval(val.(int))
	}
	
	if val, ok := settingsMap["optimistic_replication_threshold"]; ok {
		settings.SetOptimisticReplicationThreshold(val.(int))
	}
	
	if val, ok := settingsMap["http_connection"]; ok {
		settings.SetHttpConnection(val.(int))
	}
	
	if val, ok := settingsMap["source_nozzle_per_node"]; ok {
		settings.SetSourceNozzlesPerNode(val.(int))
	}

	if val, ok := settingsMap["target_nozzle_per_node"]; ok {
		settings.SetTargetNozzlesPerNode(val.(int))
	}

	if val, ok := settingsMap["max_expected_replication_lag"]; ok {
		settings.SetMaxExpectedReplicationLag(val.(int))
	}
	
	return settings
}

func (s *ReplicationSettings) ToMap () map[string]interface{}{
	settings_map := make (map[string]interface{})
	settings_map["filter_expression"] = s.FilterExpression()
	settings_map["active"] = s.IsActive()
	settings_map["checkpoint_interval"] = s.CheckpointInterval()
	settings_map["batch_count"] = s.BatchCount()
	settings_map["batch_size"] = s.BatchSize()
	settings_map["failure_restart_interval"] = s.FailureRestartInterval()
	settings_map["optimistic_replication_threshold"] = s.OptimisticReplicationThreshold()
	settings_map["http_connection"] = s.HttpConnection()
	settings_map["source_nozzle_per_node"] = s.SourceNozzlesPerNode()
	settings_map["target_nozzle_per_node"] = s.TargetNozzlesPerNode()
	settings_map["max_expected_replication_lag"] = s.MaxExpectedReplicationLag ()
	
	return settings_map
}