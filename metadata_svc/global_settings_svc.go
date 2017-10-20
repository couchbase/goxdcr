package metadata_svc

import (
	"encoding/json"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"sync"
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
	ref := &metadata.GlobalSettings{}
	err := json.Unmarshal(value, ref)
	if err != nil {
		return nil, err
	}
	ref.Revision = rev

	return ref, err
}

func (service *GlobalSettingsSvc) SetMetadataChangeHandlerCallback(call_back base.MetadataChangeHandlerCallback) {
	service.metadata_change_callback = call_back
}

func getGlobalSettingKey() string {
	return metadata.GlobalConfigurationKey + base.KeyPartsDelimiter + metadata.DefaultGlobalSettingsKey
}

// This method will pull  process setting setting
func (service *GlobalSettingsSvc) GetDefaultGlobalSettings() (*metadata.GlobalSettings, error) {
	var defaultGlobalSettings metadata.GlobalSettings
	service.globalSettingsMtx.Lock()
	defer service.globalSettingsMtx.Unlock()

	pKey := getGlobalSettingKey()

	service.logger.Infof("getDefaultGlobalSetting Processing = %v\n", pKey)
	//Pull Global Setting if it does not exists than intialize it
	bytes, rev, err := service.metadata_svc.Get(pKey)

	if err != nil {
		if err == service_def.MetadataNotFoundErr {
			// initialize default process settings if it does not exist
			defaultGlobalSettings = *metadata.DefaultGlobalSettings()
		} else {
			return nil, err
		}
	} else {
		err = json.Unmarshal(bytes, &defaultGlobalSettings)
		if err != nil {
			return nil, err
		}
		// set rev number
		defaultGlobalSettings.Revision = rev
	}
	return &defaultGlobalSettings, nil
}

func (service *GlobalSettingsSvc) SetDefaultGlobalSettings(settings *metadata.GlobalSettings) error {
	service.globalSettingsMtx.Lock()
	defer service.globalSettingsMtx.Unlock()
	bytes, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	pKey := getGlobalSettingKey()
	service.logger.Infof("setDefaultGlobalSetting = %v\n", pKey)
	if settings.Revision != nil {
		return service.metadata_svc.Set(pKey, bytes, settings.Revision)
	} else {
		return service.metadata_svc.Add(pKey, bytes)
	}

	//update setting
	if service.metadata_change_callback != nil {
		err := service.metadata_change_callback(pKey, nil, settings)
		if err != nil {
			service.logger.Error(err.Error())
			return err
		}
	}
	return nil
}
