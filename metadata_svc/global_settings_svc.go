/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadata_svc

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
)

const (
	// the key to the metadata that stores process setting
	GlobalSettingCatalogKey = metadata.GlobalConfigurationKey
)

type GlobalSettingsSvc struct {
	metadata_svc             service_def.MetadataSvc
	metadata_change_callback base.MetadataChangeHandlerCallback
	logger                   *log.CommonLogger
	globalSettingsMtx        sync.Mutex
}

func NewGlobalSettingsSvc(metadata_svc service_def.MetadataSvc, logger_ctx *log.LoggerContext) *GlobalSettingsSvc {
	return &GlobalSettingsSvc{
		metadata_svc: metadata_svc,
		logger:       log.NewLogger("GlobalSettSvc", logger_ctx),
	}
}

// Implement callback function for metakv
func (service *GlobalSettingsSvc) GlobalSettingsServiceCallback(path string, value []byte, rev interface{}) error {
	service.logger.Infof("GlobalSettingsServiceCallback called on path = %v\n", path)

	var globalSetting *metadata.GlobalSettings
	var err error
	if len(value) != 0 {
		globalSetting, err = service.constructGlobalSettingObject(value, rev)
		if err != nil {
			service.logger.Errorf("Error marshaling Global Setting Object. value=%v, err=%v\n", string(value), err)
			return err
		}
	}
	if service.metadata_change_callback != nil {
		err := service.metadata_change_callback(path, nil, globalSetting)
		if err != nil {
			service.logger.Error(err.Error())
		}
	}

	return nil
}

func (service *GlobalSettingsSvc) constructGlobalSettingObject(value []byte, rev interface{}) (*metadata.GlobalSettings, error) {
	settings := &metadata.GlobalSettings{}
	err := json.Unmarshal(value, settings)
	if err != nil {
		return nil, err
	}
	settings.Revision = rev

	settings.PostProcessAfterUnmarshalling()

	return settings, err
}

func (service *GlobalSettingsSvc) SetMetadataChangeHandlerCallback(call_back base.MetadataChangeHandlerCallback) {
	service.metadata_change_callback = call_back
}

func getGlobalSettingKey() string {
	return metadata.GlobalConfigurationKey + base.KeyPartsDelimiter + metadata.DefaultGlobalSettingsKey
}

// This method will pull  process setting setting
func (service *GlobalSettingsSvc) GetGlobalSettings() (*metadata.GlobalSettings, error) {
	service.globalSettingsMtx.Lock()
	defer service.globalSettingsMtx.Unlock()

	return service.getGlobalSettings(true /*populateDefault*/)
}

// getter for global settings
// if populateDefault is false, it simple returns the settings values in metakv, which could be empty
// this is nencessary when the retrieved global settings will be used as the base for update
// if populateDefault is true, it ensures that the values for all settings keys are populated
func (service *GlobalSettingsSvc) getGlobalSettings(populateDefault bool) (*metadata.GlobalSettings, error) {
	globalSettings, err := service.getGlobalSettingsFromMetakv()
	if err != nil {
		return nil, err
	}
	if populateDefault {
		globalSettings.PopulateDefault()
	}
	return globalSettings, nil
}

// retrieves global settings from metakv
// returns empty global settings if not found in metakv
func (service *GlobalSettingsSvc) getGlobalSettingsFromMetakv() (*metadata.GlobalSettings, error) {
	pKey := getGlobalSettingKey()
	service.logger.Infof("getDefaultGlobalSetting Processing = %v\n", pKey)

	var globalSettings *metadata.GlobalSettings
	bytes, rev, err := service.metadata_svc.Get(pKey)
	if err != nil {
		if err == service_def.MetadataNotFoundErr {
			service.logger.Info("Global settings not found in metakv")
			return metadata.EmptyGlobalSettings(), nil
		} else {
			service.logger.Errorf("Error retrieving global settings = %v\n", err)
			return nil, err
		}
	} else {
		globalSettings, err = service.constructGlobalSettingObject(bytes, rev)
		if err != nil {
			service.logger.Errorf("Error constructing global settings = %v\n", err)
			return nil, err
		}
	}
	return globalSettings, nil
}

func (service *GlobalSettingsSvc) UpdateGlobalSettings(settings metadata.ReplicationSettingsMap) (map[string]error, error) {
	service.globalSettingsMtx.Lock()
	defer service.globalSettingsMtx.Unlock()

	globalSettings, err := service.getGlobalSettings(false /*populateDefault*/)
	if err != nil {
		return nil, err
	}

	changedSettingsMap, errorMap := globalSettings.UpdateSettingsFromMap(settings)
	if len(errorMap) != 0 {
		return errorMap, nil
	}

	if len(changedSettingsMap) == 0 {
		service.logger.Infof("Skipping update of global settings since there are no real changes. settings=%v\n", settings)
		return nil, nil
	}

	if val, ok := changedSettingsMap[metadata.GenericServicesLogLevelKey]; ok {
		jsonStr, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("failed to update global settings.err=Invalid type %T for genericServicesLogLevel. Expected string", val)
		}
		updatedJsonStr, err := metadata.ValidateAndFillJson(jsonStr)
		if err != nil {
			return nil, fmt.Errorf("failed to update global settings. err=%v", err)
		}
		globalSettings.Settings.Values[metadata.GenericServicesLogLevelKey] = updatedJsonStr
	}
	return nil, service.setGlobalSettings(globalSettings)
}

func (service *GlobalSettingsSvc) setGlobalSettings(globalSettings *metadata.GlobalSettings) error {
	bytes, err := json.Marshal(globalSettings)
	if err != nil {
		return err
	}
	pKey := getGlobalSettingKey()
	if globalSettings.Revision != nil {
		err = service.metadata_svc.Set(pKey, bytes, globalSettings.Revision)
	} else {
		err = service.metadata_svc.Add(pKey, bytes)
	}
	if err != nil {
		service.logger.Infof("Failed to save Global settings to metakv. settings=%v\n", globalSettings)
		return err
	}
	service.logger.Infof("Global settings saved successfully to metakv. settings=%v\n", globalSettings)
	return nil
}
