// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_impl

import (
	"encoding/json"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/log"
)

var DefaultReplicationSettingsKey = "DefaultReplicationSettings"

type ReplicationSettingsSvc struct {
	metadata_svc  service_def.MetadataSvc
	logger      *log.CommonLogger
}

func NewReplicationSettingsSvc(metadata_svc service_def.MetadataSvc, logger_ctx *log.LoggerContext) *ReplicationSettingsSvc {
	return &ReplicationSettingsSvc{
					metadata_svc:  metadata_svc, 
					logger:    log.NewLogger("ReplicationSettingsService", logger_ctx),
					}
}

func (repl_settings_svc *ReplicationSettingsSvc) GetDefaultReplicationSettings() (*metadata.ReplicationSettings, error) {
	var defaultSettings metadata.ReplicationSettings
	bytes, err := repl_settings_svc.metadata_svc.Get(DefaultReplicationSettingsKey)
	if err != nil {
		// initialize default settings if it does not exist
		defaultSettings = *metadata.DefaultSettings()
		repl_settings_svc.SetDefaultReplicationSettings(&defaultSettings)
	} else {
		err = json.Unmarshal(bytes, &defaultSettings)
		if err != nil {
			return nil, err
		}
	}
	return &defaultSettings, nil
}
	
func (repl_settings_svc *ReplicationSettingsSvc) SetDefaultReplicationSettings(settings *metadata.ReplicationSettings) error {
	bytes, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	return repl_settings_svc.metadata_svc.Set(DefaultReplicationSettingsKey, bytes)
}