package metadata

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/simple_utils"
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

var XDCRInternalSettingsConfigMap = map[string]*SettingsConfig{
	TopologyChangeCheckIntervalKey:         TopologyChangeCheckIntervalConfig,
	MaxTopologyChangeCountBeforeRestartKey: MaxTopologyChangeCountBeforeRestartConfig,
	MaxTopologyStableCountBeforeRestartKey: MaxTopologyStableCountBeforeRestartConfig,
	MaxWorkersForCheckpointingKey:          MaxWorkersForCheckpointingConfig,
	TimeoutCheckpointBeforeStopKey:         TimeoutCheckpointBeforeStopConfig,
	CapiDataChanSizeMultiplierKey:          CapiDataChanSizeMultiplierConfig,
	RefreshRemoteClusterRefIntervalKey:     RefreshRemoteClusterRefIntervalConfig,
	CapiMaxRetryBatchUpdateDocsKey:         CapiMaxRetryBatchUpdateDocsConfig,
	CapiBatchTimeoutKey:                    CapiBatchTimeoutConfig,
	CapiWriteTimeoutKey:                    CapiWriteTimeoutConfig,
	CapiReadTimeoutKey:                     CapiReadTimeoutConfig,
	MaxCheckpointRecordsToKeepKey:          MaxCheckpointRecordsToKeepConfig,
	MaxCheckpointRecordsToReadKey:          MaxCheckpointRecordsToReadConfig,
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
		err = simple_utils.IncorrectValueTypeError("an integer")
		return
	}

	convertedValue = int(convertedValue.(int64))
	err = RangeCheck(convertedValue.(int), settingConfig)
	return
}

func validateAndConvertBoolValue(value string, settingConfig *SettingsConfig) (convertedValue interface{}, err error) {
	convertedValue, err = strconv.ParseBool(value)
	if err != nil {
		err = simple_utils.IncorrectValueTypeError("a boolean")
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
func (os V1InternalSettings) HandleUpgrade() {
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
