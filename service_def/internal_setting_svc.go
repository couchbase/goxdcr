// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
)

type InternalSettingsSvc interface {
	GetInternalSettings() *metadata.InternalSettings
	UpdateInternalSettings(settingsMap map[string]interface{}) (*metadata.InternalSettings, map[string]error, error)

	// Service call back function for internal settings changed event
	InternalSettingsServiceCallback(path string, value []byte, rev interface{}) error
	SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback)
}
