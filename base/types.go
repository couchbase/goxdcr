package base

import (
	"bytes"
	"fmt"
	"reflect"
)

type SettingDef struct {
	Data_type reflect.Type
	Required  bool
}

func NewSettingDef(data_type reflect.Type, bReq bool) *SettingDef {
	return &SettingDef{Data_type: data_type, Required: bReq}
}

type SettingDefinitions map[string]*SettingDef

type SettingsError struct {
	err_map map[string]error
}

func (se SettingsError) Error() string {
	var buffer bytes.Buffer
	for key, err := range se.err_map {
		errStr := fmt.Sprintf("etting=%s; err=%s\n", key, err.Error())
		buffer.WriteString(errStr)
	}
	return buffer.String()
}

func NewSettingsError() *SettingsError {
	return &SettingsError{make(map[string]error)}
}

func (se SettingsError) Add(key string, err error) {
	se.err_map[key] = err
}

type PipelineFailureHandler interface {
	OnError(topic string, partsError map[string]error)
}
