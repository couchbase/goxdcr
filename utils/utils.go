package utils

import (
	"errors"
	"fmt"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/util"
	base "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	"reflect"
)

var logger_utils *log.CommonLogger = log.NewLogger("Utils", log.LogLevelInfo)

func ValidateSettings(defs base.SettingDefinitions, settings map[string]interface{}) error {
	logger_utils.Infof("Start validate setting=%v, defs=%v", settings, defs)
	var err *base.SettingsError = nil
	for key, def := range defs {
		val, ok := settings[key]
		if !ok && def.Required {
			if err == nil {
				err = base.NewSettingsError()
			}
			err.Add(key, errors.New("required, but not supplied"))
		} else {
			if val != nil && def.Data_type != reflect.PtrTo(reflect.TypeOf(val)) {
				if err == nil {
					err = base.NewSettingsError()
				}
				err.Add(key, errors.New(fmt.Sprintf("expected type is %v, supplied type is %v",
					def.Data_type, reflect.TypeOf(val))))
			}
		}
	}
	if err != nil {
		logger_utils.Infof("setting validation result = %v", *err)
		return *err
	}
	return nil
}

func RecoverPanic(err *error) {
	if r := recover(); r != nil {
		*err = errors.New(fmt.Sprint(r))
	}
}
