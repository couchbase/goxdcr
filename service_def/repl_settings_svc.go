// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import (
	metadata "github.com/couchbase/goxdcr/v8/metadata"
)

// this service is likely provided by exposing an erlang rest api in ns_server
type ReplicationSettingsSvc interface {
	GetDefaultReplicationSettings() (*metadata.ReplicationSettings, error)
	UpdateDefaultReplicationSettings(map[string]interface{}) (metadata.ReplicationSettingsMap, map[string]error, error)
}
