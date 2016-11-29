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
	TimeoutCheckpointBeforeStopKey         = "TimeoutCheckpointBeforeStop"
	CapiDataChanSizeMultiplierKey          = "CapiDataChanSizeMultiplier"
)

var TopologyChangeCheckIntervalConfig = &SettingsConfig{10, &Range{1, 100}}
var MaxTopologyChangeCountBeforeRestartConfig = &SettingsConfig{30, &Range{1, 300}}
var MaxTopologyStableCountBeforeRestartConfig = &SettingsConfig{30, &Range{1, 300}}
var MaxWorkersForCheckpointingConfig = &SettingsConfig{5, &Range{1, 1000}}
var TimeoutCheckpointBeforeStopConfig = &SettingsConfig{180, &Range{10, 1800}}
var CapiDataChanSizeMultiplierConfig = &SettingsConfig{1, &Range{1, 100}}

var XDCRInternalSettingsConfigMap = map[string]*SettingsConfig{
	TopologyChangeCheckIntervalKey:         TopologyChangeCheckIntervalConfig,
	MaxTopologyChangeCountBeforeRestartKey: MaxTopologyChangeCountBeforeRestartConfig,
	MaxTopologyStableCountBeforeRestartKey: MaxTopologyStableCountBeforeRestartConfig,
	MaxWorkersForCheckpointingKey:          MaxWorkersForCheckpointingConfig,
	TimeoutCheckpointBeforeStopKey:         TimeoutCheckpointBeforeStopConfig,
	CapiDataChanSizeMultiplierKey:          CapiDataChanSizeMultiplierConfig,
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

	// timeout for checkpointing attempt before pipeline is stopped (in seconds) - to put an upper bound on the delay of pipeline stop/restart
	TimeoutCheckpointBeforeStop int

	// capi nozzle data chan size is defined as batchCount*CapiDataChanSizeMultiplier
	CapiDataChanSizeMultiplier int

	// revision number to be used by metadata service. not included in json
	Revision interface{}
}

func DefaultInternalSettings() *InternalSettings {
	return &InternalSettings{
		TopologyChangeCheckInterval:         TopologyChangeCheckIntervalConfig.defaultValue.(int),
		MaxTopologyChangeCountBeforeRestart: MaxTopologyChangeCountBeforeRestartConfig.defaultValue.(int),
		MaxTopologyStableCountBeforeRestart: MaxTopologyStableCountBeforeRestartConfig.defaultValue.(int),
		MaxWorkersForCheckpointing:          MaxWorkersForCheckpointingConfig.defaultValue.(int),
		TimeoutCheckpointBeforeStop:         TimeoutCheckpointBeforeStopConfig.defaultValue.(int),
		CapiDataChanSizeMultiplier:          CapiDataChanSizeMultiplierConfig.defaultValue.(int)}
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
		s.TimeoutCheckpointBeforeStop == s2.TimeoutCheckpointBeforeStop &&
		s.CapiDataChanSizeMultiplier == s2.CapiDataChanSizeMultiplier
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
		case TimeoutCheckpointBeforeStopKey:
			timeout, ok := val.(int)
			if !ok {
				errorMap[key] = simple_utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.TimeoutCheckpointBeforeStop != timeout {
				s.TimeoutCheckpointBeforeStop = timeout
				changed = true
			}
		case CapiDataChanSizeMultiplierKey:
			mutiplier, ok := val.(int)
			if !ok {
				errorMap[key] = simple_utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.CapiDataChanSizeMultiplier != mutiplier {
				s.CapiDataChanSizeMultiplier = mutiplier
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
		MaxWorkersForCheckpointingKey, TimeoutCheckpointBeforeStopKey, CapiDataChanSizeMultiplierKey:
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
	settings_map[TimeoutCheckpointBeforeStopKey] = s.TimeoutCheckpointBeforeStop
	settings_map[CapiDataChanSizeMultiplierKey] = s.CapiDataChanSizeMultiplier
	return settings_map
}
