package metadata

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/simple_utils"
	"strconv"
)

var logger_ps *log.CommonLogger = log.NewLogger("GlobalSetting", log.DefaultLoggerContext)

/*
 *  global_setting will contain all the process level configuration that could be applied to all the
 *  replication running the system
 */

const (
	GoMaxProcs = "gomaxprocs"
	GoGC       = "gogc"
	//setting that would be applied at the GOXDCR Process level that would affect all replications
	DefaultGlobalSettingsKey = "GlobalSettings"
	GlobalConfigurationKey   = "GlobalConfiguration"
)

var GoMaxProcsConfig = &SettingsConfig{4, &Range{1, 10000}}

// -1 indicates that GC is disabled completely
var GoGCConfig = &SettingsConfig{100, &Range{-1, 10000}}

var GlobalSettingsConfigMap = map[string]*SettingsConfig{
	GoMaxProcs: GoMaxProcsConfig,
	GoGC:       GoGCConfig,
}

type GlobalSettings struct {

	//maxprocs setting for golang to use number of core in the system
	GoMaxProcs int `json:"goMaxProcs"`
	//gogc setting sets the initial garbage collection target percentage.
	//a collection is triggered when the ratio of freshly allocated data to
	//live data remaining after the previous collection reaches this percentage.
	GoGC int `json:"goGC"`
	// revision number to be used by metadata service. not included in json
	Revision interface{}
}

func DefaultGlobalSettings() *GlobalSettings {
	return &GlobalSettings{GoMaxProcs: GoMaxProcsConfig.defaultValue.(int),
		GoGC: GoGCConfig.defaultValue.(int)}
}

func ValidateGlobalSettingsKey(settingsMap map[string]interface{}) (globalSettingsMap map[string]interface{}) {
	globalSettingsMap = make(map[string]interface{})

	for key, val := range settingsMap {
		switch key {
		case GoMaxProcs:
			fallthrough
		case GoGC:
			globalSettingsMap[key] = val
		}
	}
	return
}

// returns a map of settings that have indeed been changed and their new values.
// returns a map of validation errors, which should normally be empty since the input settingsMap
// is constructed internally and necessary checks should have been applied before
// I am leaving the error checks just in case.
func (s *GlobalSettings) UpdateSettingsFromMap(settingsMap map[string]interface{}) (changedSettingsMap map[string]interface{}, errorMap map[string]error) {
	changedSettingsMap = make(map[string]interface{})
	errorMap = make(map[string]error)

	for key, val := range settingsMap {
		switch key {
		case GoMaxProcs:
			maxprocs, ok := val.(int)
			if !ok {
				errorMap[key] = simple_utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.GoMaxProcs != maxprocs {
				s.GoMaxProcs = maxprocs
				changedSettingsMap[key] = maxprocs
			}
		case GoGC:
			gogc, ok := val.(int)
			if !ok {
				errorMap[key] = simple_utils.IncorrectValueTypeInMapError(key, val, "int")
				continue
			}
			if s.GoGC != gogc {
				s.GoGC = gogc
				changedSettingsMap[key] = gogc
			}
		}
	}
	return
}

func ValidateAndConvertGlobalSettingsValue(key, value, errorKey string) (convertedValue interface{}, err error) {
	switch key {
	case GoMaxProcs:
		fallthrough
	case GoGC:
		convertedValue, err = strconv.ParseInt(value, base.ParseIntBase, base.ParseIntBitSize)
		if err != nil {
			err = simple_utils.IncorrectValueTypeError("an integer")
			return
		}
		// convert it to int to make future processing easier
		convertedValue = int(convertedValue.(int64))

		// range check for int parameters
		err = RangeCheck(convertedValue.(int), GlobalSettingsConfigMap[key])
	default:
		// a nil converted value indicates that the key is not a settings key
		convertedValue = nil
	}
	return
}

func (s *GlobalSettings) ToMap() map[string]interface{} {
	settings_map := make(map[string]interface{})
	settings_map[GoMaxProcs] = s.GoMaxProcs
	settings_map[GoGC] = s.GoGC
	return settings_map
}

func (s *GlobalSettings) Clone() *GlobalSettings {
	if s == nil {
		return nil
	}

	clone := &GlobalSettings{}
	clone.UpdateSettingsFromMap(s.ToMap())
	return clone
}

func (s *GlobalSettings) String() string {
	if s == nil {
		return "nil"
	}
	return fmt.Sprintf("GoMaxProcs:%v, GoGC:%v", s.GoMaxProcs, s.GoGC)
}
