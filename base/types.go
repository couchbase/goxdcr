// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package base

import (
	"bytes"
	"fmt"
	"reflect"
	"time"
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

// timestamp for a specific vb
type VBTimestamp struct{
    Vbno  uint16
    Vbuuid uint64
    Seqno  uint64
    SnapshotStart  uint64
    SnapshotEnd  uint64
}

type ClusterConnectionInfoProvider interface {
	MyConnectionStr()  string
	MyUsername()  string
	MyPassword()  string
}

type ReplicationInfo struct {
	Id string
	StatsMap  map[string]string
	ErrorList []ErrorInfo
}

type ErrorInfo struct {
	Time time.Time
	ErrorMsg  string
}

