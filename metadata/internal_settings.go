package metadata

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/simple_utils"
	"strconv"
)

var logger_is *log.CommonLogger = log.NewLogger("InternalSetting", log.DefaultLoggerContext)

const (
	InternalSettingsKey = "InternalSettings"

	TopologyChangeCheckIntervalKey         = "TopologyChangeCheckInterval"
	MaxTopologyChangeCountBeforeRestartKey = "MaxTopologyChangeCountBeforeRestart"
	MaxTopologyStableCountBeforeRestartKey = "MaxTopologyStableCountBeforeRestart"
	MaxWorkersForCheckpointingKey          = "MaxWorkersForCheckpointing"
	TopologyChangeCheckpointTimeoutKey     = "TopologyChangeCheckpointTimeout"
)

var TopologyChangeCheckIntervalConfig = &SettingsConfig{10, &Range{1, 100}}
var MaxTopologyChangeCountBeforeRestartConfig = &SettingsConfig{30, &Range{1, 300}}
var MaxTopologyStableCountBeforeRestartConfig = &SettingsConfig{30, &Range{1, 300}}
var MaxWorkersForCheckpointingConfig = &SettingsConfig{5, &Range{1, 1000}}
var TopologyChangeCheckpointTimeoutConfig = &SettingsConfig{10, &Range{1, 300}}

var XDCRInternalSettingsConfigMap = map[string]*SettingsConfig{
	TopologyChangeCheckIntervalKey:         TopologyChangeCheckIntervalConfig,
	MaxTopologyChangeCountBeforeRestartKey: MaxTopologyChangeCountBeforeRestartConfig,
	MaxTopologyStableCountBeforeRestartKey: MaxTopologyStableCountBeforeRestartConfig,
	MaxWorkersForCheckpointingKey:          MaxWorkersForCheckpointingConfig,
	TopologyChangeCheckpointTimeoutKey:     TopologyChangeCheckpointTimeoutConfig,
}

type InternalSettings struct {
	// interval between topology checks (in seconds)
	TopologyChangeCheckInterval int

	// the maximum number of topology change checks to wait before pipeline is restarted
	MaxTopologyChangeCountBeforeRestart int
	// the maximum number of consecutive stable topology seen before pipeline is restarted
	MaxTopologyStableCountBeforeRestart int
	// the max number of concurrent workers for checkpointing
	MaxWorkersForCheckpointing int

	// timeout for checkpointing attempt due to topology changes (in minutes) - to put an upper bound on the delay of pipeline restartx
	TopologyChangeCheckpointTimeout int

	// revision number to be used by metadata service. not included in json
	Revision interface{}
}

func DefaultInternalSettings() *InternalSettings {
	return &InternalSettings{
		TopologyChangeCheckInterval:         TopologyChangeCheckIntervalConfig.defaultValue.(int),
		MaxTopologyChangeCountBeforeRestart: MaxTopologyChangeCountBeforeRestartConfig.defaultValue.(int),
		MaxTopologyStableCountBeforeRestart: MaxTopologyStableCountBeforeRestartConfig.defaultValue.(int),
		MaxWorkersForCheckpointing:          MaxWorkersForCheckpointingConfig.defaultValue.(int),
		TopologyChangeCheckpointTimeout:     TopologyChangeCheckpointTimeoutConfig.defaultValue.(int)}
}

func (s *InternalSettings) Equals(s2 *InternalSettings) bool {
	if s == s2 {
		// this also covers the case where s = nil and s2 = nil
		return true
	}
	if (s == nil && s2 != nil) || (s != nil && s2 == nil) {
		return false
	}

	return s.TopologyChangeCheckInterval == s2.TopologyChangeCheckInterval &&
		s.MaxTopologyChangeCountBeforeRestart == s2.MaxTopologyChangeCountBeforeRestart &&
		s.MaxTopologyStableCountBeforeRestart == s2.MaxTopologyStableCountBeforeRestart &&
		s.MaxWorkersForCheckpointing == s2.MaxWorkersForCheckpointing &&
		s.TopologyChangeCheckpointTimeout == s2.TopologyChangeCheckpointTimeout
}

func (s *InternalSettings) UpdateSettingsFromMap(settingsMap map[string]interface{}) (changed bool, errorMap map[string]error) {
	changed = false
	errorMap = make(map[string]error)

	for key, val := range settingsMap {
		switch key {
		case TopologyChangeCheckIntervalKey:
			checkInterval, ok := val.(int)
			if !ok {
				errorMap[key] = simple_utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.TopologyChangeCheckInterval != checkInterval {
				s.TopologyChangeCheckInterval = checkInterval
				changed = true
			}
		case MaxTopologyChangeCountBeforeRestartKey:
			maxCount, ok := val.(int)
			if !ok {
				errorMap[key] = simple_utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.MaxTopologyChangeCountBeforeRestart != maxCount {
				s.MaxTopologyChangeCountBeforeRestart = maxCount
				changed = true
			}
		case MaxTopologyStableCountBeforeRestartKey:
			maxCount, ok := val.(int)
			if !ok {
				errorMap[key] = simple_utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.MaxTopologyStableCountBeforeRestart != maxCount {
				s.MaxTopologyStableCountBeforeRestart = maxCount
				changed = true
			}
		case MaxWorkersForCheckpointingKey:
			maxWorkers, ok := val.(int)
			if !ok {
				errorMap[key] = simple_utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.MaxWorkersForCheckpointing != maxWorkers {
				s.MaxWorkersForCheckpointing = maxWorkers
				changed = true
			}
		case TopologyChangeCheckpointTimeoutKey:
			timeout, ok := val.(int)
			if !ok {
				errorMap[key] = simple_utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.TopologyChangeCheckpointTimeout != timeout {
				s.TopologyChangeCheckpointTimeout = timeout
				changed = true
			}
		default:
			errorMap[key] = fmt.Errorf("Invalid key in map, %v", key)
		}
	}

	return
}

func ValidateAndConvertXDCRInternalSettingsValue(key, value string) (convertedValue interface{}, err error) {
	switch key {
	case TopologyChangeCheckIntervalKey, MaxTopologyChangeCountBeforeRestartKey, MaxTopologyStableCountBeforeRestartKey,
		MaxWorkersForCheckpointingKey, TopologyChangeCheckpointTimeoutKey:
		convertedValue, err = strconv.ParseInt(value, base.ParseIntBase, base.ParseIntBitSize)
		if err != nil {
			err = simple_utils.IncorrectValueTypeError("an integer")
			return
		}

		convertedValue = int(convertedValue.(int64))

		err = RangeCheck(convertedValue.(int), XDCRInternalSettingsConfigMap[key])
		return
	default:
		// a nil converted value indicates that the key is not a settings key
		convertedValue = nil
	}

	return
}

func (s *InternalSettings) ToMap() map[string]interface{} {
	settings_map := make(map[string]interface{})
	settings_map[TopologyChangeCheckIntervalKey] = s.TopologyChangeCheckInterval
	settings_map[MaxTopologyChangeCountBeforeRestartKey] = s.MaxTopologyChangeCountBeforeRestart
	settings_map[MaxTopologyStableCountBeforeRestartKey] = s.MaxTopologyStableCountBeforeRestart
	settings_map[MaxWorkersForCheckpointingKey] = s.MaxWorkersForCheckpointing
	settings_map[TopologyChangeCheckpointTimeoutKey] = s.TopologyChangeCheckpointTimeout
	return settings_map
}
