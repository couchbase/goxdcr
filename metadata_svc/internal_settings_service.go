/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadata_svc

import (
	"encoding/json"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	"sync"
)

const (
	// catalog key for v1 internal settings with individual setting fields
	// have to use the same key as before so that we can load v1 settings before upgrade
	v1InternalSettingsCatalogKey = "InternalSettings"
	// catalog key for v2 internal settings with map
	v2InternalSettingsCatalogKey = "V2InternalSettings"

	// publically exposed catalog key for internal settings
	InternalSettingsCatalogKey = v2InternalSettingsCatalogKey
)

var v1InternalSettingsMetakvKey = v1InternalSettingsCatalogKey + base.KeyPartsDelimiter + metadata.InternalSettingsKey
var v2InternalSettingsMetakvKey = v2InternalSettingsCatalogKey + base.KeyPartsDelimiter + metadata.InternalSettingsKey

type InternalSettingsSvc struct {
	metadata_svc             service_def.MetadataSvc
	metadata_change_callback base.MetadataChangeHandlerCallback
	logger                   *log.CommonLogger
	// keeps a single copy of internal settings
	internal_settings   *metadata.InternalSettings
	internalSettingsMtx sync.RWMutex
}

func NewInternalSettingsSvc(metadata_svc service_def.MetadataSvc, logger_ctx *log.LoggerContext) *InternalSettingsSvc {
	service := &InternalSettingsSvc{
		metadata_svc: metadata_svc,
		logger:       log.NewLogger("IntSettSvc", logger_ctx),
	}

	service.initializeInternalSettings()
	return service
}

// initializes the cached internal_settings object, which can be used to determine if any change has been made to it later
func (service *InternalSettingsSvc) initializeInternalSettings() {
	service.internalSettingsMtx.Lock()
	defer service.internalSettingsMtx.Unlock()

	internal_settings, err := service.getInternalSettings(true /*populateDefault*/)
	if err == nil {
		service.internal_settings = internal_settings
	} else {
		service.logger.Warnf("%v. Use default values instead.", err)
		service.internal_settings = metadata.DefaultInternalSettings()
	}
}

// getter for internal settings
// if populateDefault is false, it simple returns the settings values in metakv, which could be empty
// this is nencessary when the retrieved internal settings will be used as the base for update
// if populateDefault is true, it ensures that the values for all settings keys are populated
func (service *InternalSettingsSvc) getInternalSettings(populateDefault bool) (*metadata.InternalSettings, error) {
	internal_settings, err := service.getInternalSettingsFromMetakv()
	if err != nil {
		return nil, err
	}
	if populateDefault {
		internal_settings.PopulateDefault()
	}
	return internal_settings, nil
}

// retrieves internal settings from metakv
// returns empty internal settings if not found in metakv
func (service *InternalSettingsSvc) getInternalSettingsFromMetakv() (*metadata.InternalSettings, error) {
	// first try to read new internal settings from metakv
	internal_settings, err := service.getV2InternalSettings()
	if err == nil {
		return internal_settings, nil
	} else if err != service_def.MetadataNotFoundErr {
		service.logger.Warnf("Error retrieving internal settings. err = %v.", err)
		return nil, err
	}

	// err == service_def.MetadataNotFoundErr
	// this could happen when customer has not updated internal settings in upgraded build
	// check if there are old internal settings prior to upgrade
	internal_settings, err = service.getV2InternalSettingsFromV1()
	if err == nil {
		return internal_settings, nil
	} else {
		if err == service_def.MetadataNotFoundErr {
			service.logger.Info("Internal settings not found in metakv")
			return metadata.EmptyInternalSettings(), nil
		} else {
			service.logger.Warnf("Error retrieving v1 internal settings. err = %v.", err)
			return nil, err
		}
	}
}

func (service *InternalSettingsSvc) GetInternalSettings() *metadata.InternalSettings {
	service.internalSettingsMtx.RLock()
	defer service.internalSettingsMtx.RUnlock()
	// simply return a copy of the cached value
	// if value in metakv is changed, goxdcr processed will get restarted
	// and the cached value will get updated after restart
	return service.internal_settings.Clone()
}

func (service *InternalSettingsSvc) getV2InternalSettings() (*metadata.InternalSettings, error) {
	var internal_settings *metadata.InternalSettings
	bytes, rev, err := service.metadata_svc.Get(v2InternalSettingsMetakvKey)
	if err != nil {
		return nil, err
	} else {
		internal_settings, err = service.constructV2InternalSettingsObject(bytes, rev)
		if err != nil {
			return nil, err
		}
	}
	return internal_settings, nil
}

func (service *InternalSettingsSvc) getV2InternalSettingsFromV1() (*metadata.InternalSettings, error) {
	var internal_settings *metadata.InternalSettings
	bytes, rev, err := service.metadata_svc.Get(v1InternalSettingsMetakvKey)
	if err != nil {
		return nil, err
	} else {
		internal_settings, err = service.constructV2InternalSettingsObjectFromV1(bytes, rev)
		if err != nil {
			return nil, err
		}
	}
	return internal_settings, nil
}

func (service *InternalSettingsSvc) UpdateInternalSettings(settingsMap map[string]interface{}) (*metadata.InternalSettings, map[string]error, error) {
	service.internalSettingsMtx.Lock()
	defer service.internalSettingsMtx.Unlock()
	// cannot use service.internal_settings as the base since it could have been default internal settings
	// retrieve from metakv to be sure that the base is correct
	internal_settings, err := service.getInternalSettings(false /*populateDefault*/)
	if err != nil {
		service.logger.Warnf("Skipped update to internal settings because of error retrieving current internal settings. err=%v", err)
		return nil, nil, err
	}

	changedSettingMap, errorsMap := internal_settings.UpdateSettingsFromMap(settingsMap)
	if len(errorsMap) > 0 {
		return nil, errorsMap, nil
	}

	if len(changedSettingMap) > 0 {
		err := service.setV2InternalSettings(internal_settings)
		if err != nil {
			return nil, nil, err
		}
		// note, the cached internal_settings is not updated here,
		// since the actual internal settings in use are not changed yet
		// we will receive metakv callback with the updated values shortly
		// and we will restart goxdcr process and update the cached internal settings after restart
	} else {
		service.logger.Infof("Skipped update to internal settings since there have been no real changes.")
	}

	// this is necessary for all settings values to be shown in rest api response
	internal_settings.PopulateDefault()
	return internal_settings, nil, nil
}

func (service *InternalSettingsSvc) setV2InternalSettings(internal_settings *metadata.InternalSettings) error {
	bytes, err := json.Marshal(internal_settings)
	if err != nil {
		return err
	}

	if internal_settings.Revision != nil {
		err = service.metadata_svc.Set(v2InternalSettingsMetakvKey, bytes, internal_settings.Revision)
	} else {
		err = service.metadata_svc.Add(v2InternalSettingsMetakvKey, bytes)
	}

	if err != nil {
		return err
	}

	service.logger.Infof("Successfully updated internal settings to %v", internal_settings)

	return nil
}

// construct new internal settings object (with map)
func (service *InternalSettingsSvc) constructV2InternalSettingsObject(value []byte, rev interface{}) (*metadata.InternalSettings, error) {
	v2_settings := metadata.EmptyInternalSettings()
	err := json.Unmarshal(value, v2_settings)
	if err != nil {
		return nil, err
	}

	v2_settings.Revision = rev
	v2_settings.PostProcessAfterUnmarshalling()

	return v2_settings, nil
}

// construct internal settings object base on old object in metakv (with individual setting fields)
func (service *InternalSettingsSvc) constructV2InternalSettingsObjectFromV1(value []byte, rev interface{}) (*metadata.InternalSettings, error) {
	v1_settings := &metadata.V1InternalSettings{}
	err := json.Unmarshal(value, v1_settings)
	if err != nil {
		return nil, err
	}
	v1_settings.HandleUpgrade()

	v2_settings := metadata.ConstructInternalSettingsFromV1Settings(v1_settings)
	service.logger.Infof("Converted v1 internal settings to v2 internal settings. v1=%v\n v2=%v\n", v1_settings, v2_settings)

	return v2_settings, nil
}

func (service *InternalSettingsSvc) SetMetadataChangeHandlerCallback(call_back base.MetadataChangeHandlerCallback) {
	service.metadata_change_callback = call_back
}

// Implement callback function for metakv
func (service *InternalSettingsSvc) InternalSettingsServiceCallback(path string, value []byte, rev interface{}) error {
	service.logger.Infof("InternalSettingsServiceCallback called on path = %v\n", path)

	var internal_settings *metadata.InternalSettings
	var err error
	if len(value) != 0 {
		internal_settings, err = service.constructV2InternalSettingsObject(value, rev)
		if err != nil {
			service.logger.Errorf("Error marshaling Internal Settings Object. value=%v, err=%v\n", string(value), err)
			return nil
		}
		// this is necessary for change detection to work correctly
		internal_settings.PopulateDefault()
	}

	if service.metadata_change_callback != nil {
		// use the cached service.internal_settings object as the old settings
		err := service.metadata_change_callback(path, service.internal_settings.Clone(), internal_settings)
		if err != nil {
			service.logger.Error(err.Error())
		}
	}

	return nil
}
