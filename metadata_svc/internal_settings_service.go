package metadata_svc

import (
	"encoding/json"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
)

const (
	InternalSettingsCatalogKey = "InternalSettings"
)

var InternalSettingsMetakvKey = InternalSettingsCatalogKey + base.KeyPartsDelimiter + metadata.InternalSettingsKey

type InternalSettingsSvc struct {
	metadata_svc             service_def.MetadataSvc
	metadata_change_callback base.MetadataChangeHandlerCallback
	logger                   *log.CommonLogger
	// keeps a single copy of internal settings and keeps it up to date
	internal_settings *metadata.InternalSettings
}

func NewInternalSettingsSvc(metadata_svc service_def.MetadataSvc, logger_ctx *log.LoggerContext) *InternalSettingsSvc {
	service := &InternalSettingsSvc{
		metadata_svc: metadata_svc,
		logger:       log.NewLogger("InternalSettingsService", logger_ctx),
	}

	// tracks the "initial" internal_settings object, which can be used to determine if any change has been made to it later
	service.internal_settings = service.GetInternalSettings()
	service.logger.Infof("Internal settings for the current XCR process: %v", service.internal_settings)
	return service
}

func (service *InternalSettingsSvc) GetInternalSettings() *metadata.InternalSettings {
	var internal_settings metadata.InternalSettings
	bytes, rev, err := service.metadata_svc.Get(InternalSettingsMetakvKey)
	if err != nil {
		if err == service_def.MetadataNotFoundErr {
			service.logger.Info("Internal settings spec not found. Using default values")
		} else {
			service.logger.Errorf("Error retrieving internal settings spec. err = %v. Using default values", err)
		}
		internal_settings = *(metadata.DefaultInternalSettings())
	} else {
		err = json.Unmarshal(bytes, &internal_settings)
		if err != nil {
			service.logger.Errorf("Error unmarshaling internal settings spec. err = %v. Using default values", err)
			internal_settings = *(metadata.DefaultInternalSettings())
		}
		internal_settings.Revision = rev
	}
	return &internal_settings
}

func (service *InternalSettingsSvc) UpdateInternalSettings(settingsMap map[string]interface{}) (*metadata.InternalSettings, map[string]error, error) {
	internal_settings := service.GetInternalSettings()
	changed, errorsMap := internal_settings.UpdateSettingsFromMap(settingsMap)
	if len(errorsMap) > 0 {
		return nil, errorsMap, nil
	}

	if changed {
		bytes, err := json.Marshal(internal_settings)
		if err != nil {
			return nil, nil, err
		}

		if internal_settings.Revision != nil {
			err = service.metadata_svc.Set(InternalSettingsMetakvKey, bytes, internal_settings.Revision)
		} else {
			err = service.metadata_svc.Add(InternalSettingsMetakvKey, bytes)
		}

		if err != nil {
			return nil, nil, err
		}

		service.logger.Infof("Successfully updated internal settings to %v", internal_settings)

		// note that service.internal_settings is not updated.
		// GOXDCR process needs to be restarted for the new value to become effective
	} else {
		service.logger.Infof("Skipped update to internal settings since there have been no real changes.")
	}

	return internal_settings, nil, nil
}

func (service *InternalSettingsSvc) constructInternalSettingsObject(value []byte, rev interface{}) (*metadata.InternalSettings, error) {
	settings := &metadata.InternalSettings{}
	err := json.Unmarshal(value, settings)
	if err != nil {
		return nil, err
	}
	settings.Revision = rev

	return settings, err
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
		internal_settings, err = service.constructInternalSettingsObject(value, rev)
		if err != nil {
			service.logger.Errorf("Error marshaling Internal Settings Object. value=%v, err=%v\n", string(value), err)
			return nil
		}
	}

	if service.metadata_change_callback != nil {
		// use the cached service.internal_settings object as the old settings
		err := service.metadata_change_callback(path, service.internal_settings, internal_settings)
		if err != nil {
			service.logger.Error(err.Error())
		}
	}

	return nil
}
