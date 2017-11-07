package metadata_svc

import (
	"encoding/json"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
)

const (
	// catalog key for v1 internal settings with individual setting fields
	// have to use the same key as before so that we can load v1 settings before upgrade
	V1InternalSettingsCatalogKey = "InternalSettings"
	// catalog key for v2 internal settings with map
	V2InternalSettingsCatalogKey = "V2InternalSettings"
)

var V1InternalSettingsMetakvKey = V1InternalSettingsCatalogKey + base.KeyPartsDelimiter + metadata.InternalSettingsKey
var V2InternalSettingsMetakvKey = V2InternalSettingsCatalogKey + base.KeyPartsDelimiter + metadata.InternalSettingsKey

type InternalSettingsSvc struct {
	metadata_svc             service_def.MetadataSvc
	metadata_change_callback base.MetadataChangeHandlerCallback
	logger                   *log.CommonLogger
	// keeps a single copy of internal settings
	internal_settings *metadata.InternalSettings
}

func NewInternalSettingsSvc(metadata_svc service_def.MetadataSvc, logger_ctx *log.LoggerContext) *InternalSettingsSvc {
	service := &InternalSettingsSvc{
		metadata_svc: metadata_svc,
		logger:       log.NewLogger("IntSettSvc", logger_ctx),
	}

	// initializes the cached internal_settings object, which can be used to determine if any change has been made to it later
	service.internal_settings, _ = service.getInternalSettingsFromMetakv(true /*useDefaultSettingsAtError*/)
	return service
}

// this method returns
// 1. (internal settings, nil) when it successfully retrieves internal settings from metakv,
// 2. (default internal settings, nil) when it cannot find internal settings in metakv
// 3. (default internal settings, nil) when it encounters a non-MetadataNotFoundErr error from metakv, and useDefaultSettingsAtError is true
// 4. (nil, err) when it encounters a non-MetadataNotFoundErr error from metakv, and useDefaultSettingsAtError is false
// this method never returns non-nil err when useDefaultSettingsAtError is true
func (service *InternalSettingsSvc) getInternalSettingsFromMetakv(useDefaultSettingsAtError bool) (*metadata.InternalSettings, error) {
	// first try to read new internal settings from metakv
	internal_settings, err := service.getV2InternalSettings()
	if err == nil {
		return internal_settings, nil
	} else if err != service_def.MetadataNotFoundErr {
		if useDefaultSettingsAtError {
			service.logger.Warnf("Error retrieving internal settings. err = %v. Using default values", err)
			return metadata.DefaultInternalSettings(), nil
		} else {
			service.logger.Warnf("Error retrieving internal settings. err = %v.", err)
			return nil, err
		}
	}

	// err == service_def.MetadataNotFoundErr
	// this could happen when customer has not updated internal settings in upgraded build
	// check if there are old internal settings prior to upgrade
	internal_settings, err = service.getV2InternalSettingsFromV1()
	if err == nil {
		return internal_settings, nil
	} else {
		if err == service_def.MetadataNotFoundErr {
			service.logger.Info("Internal settings not found. Using default values")
			return metadata.DefaultInternalSettings(), nil
		} else {
			if useDefaultSettingsAtError {
				service.logger.Warnf("Error retrieving V1 internal settings. err = %v. Using default values", err)
				return metadata.DefaultInternalSettings(), nil
			} else {
				service.logger.Warnf("Error retrieving internal settings. err = %v.", err)
				return nil, err
			}
		}
	}
}

func (service *InternalSettingsSvc) GetInternalSettings() *metadata.InternalSettings {
	// simply return a copy of the cached value
	// if value in metakv is changed, goxdcr processed will get restarted
	// and the cached value will get updated after restart
	return service.internal_settings.Clone()
}

func (service *InternalSettingsSvc) getV2InternalSettings() (*metadata.InternalSettings, error) {
	var internal_settings *metadata.InternalSettings
	bytes, rev, err := service.metadata_svc.Get(V2InternalSettingsMetakvKey)
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
	bytes, rev, err := service.metadata_svc.Get(V1InternalSettingsMetakvKey)
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
	// cannot use service.internal_settings as the base since it could have been default internal settings
	// retrieve from metakv to be sure that the base is correct
	internal_settings, err := service.getInternalSettingsFromMetakv(false /*useDefaultSettingsAtError*/)
	if err != nil {
		service.logger.Warnf("Skipping update to internal settings because of error retrieving current internal settings. err=%v", err)
		return nil, nil, err
	}

	changed, errorsMap := internal_settings.UpdateSettingsFromMap(settingsMap)
	if len(errorsMap) > 0 {
		return nil, errorsMap, nil
	}

	if changed {
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

	return internal_settings, nil, nil
}

func (service *InternalSettingsSvc) setV2InternalSettings(internal_settings *metadata.InternalSettings) error {
	bytes, err := json.Marshal(internal_settings)
	if err != nil {
		return err
	}

	if internal_settings.Revision != nil {
		err = service.metadata_svc.Set(V2InternalSettingsMetakvKey, bytes, internal_settings.Revision)
	} else {
		err = service.metadata_svc.Add(V2InternalSettingsMetakvKey, bytes)
	}

	if err != nil {
		return err
	}

	service.logger.Infof("Successfully updated internal settings to %v", internal_settings)

	return nil
}

// construct new internal settings object (with map)
func (service *InternalSettingsSvc) constructV2InternalSettingsObject(value []byte, rev interface{}) (*metadata.InternalSettings, error) {
	v2_settings := &metadata.InternalSettings{}
	err := json.Unmarshal(value, v2_settings)
	if err != nil {
		return nil, err
	}

	v2_settings.Revision = rev

	// handle data type change caused by marshalling and unmarshalling
	v2_settings.HandleDataTypeConversionAfterUnmarshalling()

	// handle v2 settings added after the v2 settings type is introduced
	v2_settings.HandleUpgrade()

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

	v2_settings := &metadata.InternalSettings{Values: make(map[string]interface{})}
	// convert old internal settings into internal settings
	v2_settings.ConvertFromV1InternalSettings(v1_settings)
	service.logger.Infof("Converted v1 internal settings to v2 internal settings. v1=%v\n v2=%v\n", v1_settings, v2_settings)

	// handle v2 settings added after the v2 settings type is introduced
	v2_settings.HandleUpgrade()
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
