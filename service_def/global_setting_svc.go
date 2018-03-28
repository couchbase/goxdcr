// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_def

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
)

type GlobalSettingsSvc interface {
	GetGlobalSettings() (*metadata.GlobalSettings, error)
	UpdateGlobalSettings(metadata.ReplicationSettingsMap) (map[string]error, error)

	// Service call back function for process changed event
	GlobalSettingsServiceCallback(path string, value []byte, rev interface{}) error

	// set the metadata change call back method
	// when the replication spec service makes changes, it needs to call the call back
	// explicitly, so that the actions can be taken immediately
	SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback)
}
