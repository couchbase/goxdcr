package utils

import (
	"errors"
	"fmt"
	base "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	"reflect"
)

func ValidateSettings(defs base.SettingDefinitions, settings map[string]interface{}) error {
	var err *base.SettingsError = nil
	for key, def := range defs {
		val, ok := settings[key]
		if !ok && def.Required {
			if err == nil {
				err = base.NewSettingsError()
			}
			err.Add(key, errors.New("required, but not supplied"))
		} else {
			if !def.Data_type.AssignableTo(reflect.TypeOf(val)) {
				if err == nil {
					err = base.NewSettingsError()
				}
				err.Add(key, errors.New(fmt.Sprintf("expected type is %s, supplied type is %s",
					def.Data_type.Name(), reflect.TypeOf(val).Name())))
			}
		}
	}
	return *err
}

func RecoverPanic(err *error) {
	if r := recover(); r != nil {
		*err = errors.New(fmt.Sprint(r))
	}
}
