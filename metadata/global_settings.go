/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadata

import (
	"fmt"

	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
)

var GoGCDefaultValue = 100
var logger_ps *log.CommonLogger = log.NewLogger("GlobalSetting", log.DefaultLoggerContext)

/*
 *  global_setting will contain all the process level configuration that could be applied to all the
 *  replications
 */

const (
	GoMaxProcsKey              = "gomaxprocs"
	GoGCKey                    = "gogc"
	GenericServicesLogLevelKey = "genericServicesLogLevel"

	DefaultGlobalSettingsKey = "GlobalSettings"
	GlobalConfigurationKey   = "GlobalConfiguration"
)

var GoMaxProcsConfig = &SettingsConfig{base.DefaultGoMaxProcs, &Range{1, 10000}}

// -1 indicates that GC is disabled completely
// note, 0 is not a valid value for GOGC, which will be checked separately from the range check
var GoGCConfig = &SettingsConfig{100, &Range{-1, 10000}}

const EmptyString = ""

var genericServicesLogLevelConfig = &SettingsConfig{EmptyString, nil}

var GlobalSettingsConfigMap = map[string]*SettingsConfig{
	GoMaxProcsKey:              GoMaxProcsConfig,
	GoGCKey:                    GoGCConfig,
	GenericServicesLogLevelKey: genericServicesLogLevelConfig,
}

// Adding values in this struct is deprecated - use GlobalSettings.Settings.Values instead
type GlobalSettings struct {
	*Settings
	//maxprocs setting for golang to use number of core in the system
	GoMaxProcs int `json:"goMaxProcs"`
	//gogc setting sets the initial garbage collection target percentage.
	//a collection is triggered when the ratio of freshly allocated data to
	//live data remaining after the previous collection reaches this percentage.
	GoGC int `json:"goGC"`
	// revision number to be used by metadata service. not included in json
	Revision interface{}
}

// config map retriever required by Settings
func GetGlobalSettingsConfigMap() map[string]*SettingsConfig {
	return GlobalSettingsConfigMap
}

func EmptyGlobalSettings() *GlobalSettings {
	return &GlobalSettings{Settings: EmptySettings(GetGlobalSettingsConfigMap)}
}

func DefaultGlobalSettings() *GlobalSettings {
	defaultSettings := &GlobalSettings{Settings: DefaultSettings(GetGlobalSettingsConfigMap)}
	defaultSettings.populateFieldsUsingMap()
	return defaultSettings
}

func (s *GlobalSettings) ToMap() map[string]interface{} {
	settingsMap := s.Settings.ToMap()

	if goGC, ok := settingsMap[GoGCKey]; ok {
		if goGC.(int) == 0 {
			// 0 value for GOGC can only have come from upgrade.
			// It is not used at runtime and should not be visible to users
			// Remove GoGC field and value from map in this case
			delete(settingsMap, GoGCKey)
		}
	}

	return settingsMap
}

// post processing after global settings is loaded from metakv
func (s *GlobalSettings) PostProcessAfterUnmarshalling() {
	if s.Settings == nil {
		// if s.Settings is nil, which could happen during/after upgrade, populate s.Settings using fields in s
		s.populateMapUsingFields()
	} else {
		s.Settings.PostProcessAfterUnmarshalling(GetGlobalSettingsConfigMap)

		// no need for populateFieldsUsingMap() since fields and map in metakv should already be consistent
	}
}

func (s *GlobalSettings) UpdateSettingsFromMap(settingsMap map[string]interface{}) (changedSettingsMap ReplicationSettingsMap, errorMap map[string]error) {
	changedSettingsMap, errorMap = s.Settings.UpdateSettingsFromMap(settingsMap)
	if len(errorMap) > 0 {
		return
	}
	s.populateFieldsUsingMap()
	return
}

// populate settings map using field values
// this is needed when we load pre-upgrade global settings from metakv
func (s *GlobalSettings) populateMapUsingFields() {
	s.Settings = EmptySettings(GetGlobalSettingsConfigMap)
	if s.GoMaxProcs != GoMaxProcsConfig.defaultValue.(int) {
		s.Values[GoMaxProcsKey] = s.GoMaxProcs
	}
	if s.GoGC != GoGCConfig.defaultValue.(int) {
		s.Values[GoGCKey] = s.GoGC
	}
}

// populate field values using settings map
// this needs to be done whenever the settings map is changed,
// so as to ensure that field values and settings map are in sync
func (s *GlobalSettings) populateFieldsUsingMap() {
	var value interface{}
	var ok bool
	if value, ok = s.Values[GoMaxProcsKey]; ok {
		s.GoMaxProcs = value.(int)
	}
	if value, ok = s.Values[GoGCKey]; ok {
		s.GoGC = value.(int)
	}
}

func ValidateGlobalSettingsKey(settingsMap map[string]interface{}) map[string]interface{} {
	return ValidateSettingsKey(settingsMap, GlobalSettingsConfigMap)
}

func ValidateAndConvertGlobalSettingsValue(key, value string) (convertedValue interface{}, err error) {
	convertedValue, err = ValidateAndConvertSettingsValue(key, value, GlobalSettingsConfigMap)
	if err != nil {
		return
	}

	// additional checks
	switch key {
	case GoGCKey:
		if convertedValue == 0 {
			err = fmt.Errorf("0 is not a valid value for GOGC")
		}
	}
	return
}
