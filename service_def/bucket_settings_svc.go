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

type BucketSettingsSvc interface {
	BucketSettings(bucketName string) (*metadata.BucketSettings, error)
	SetBucketSettings(bucketName string, bucketSettings *metadata.BucketSettings) error

	// Service call back function for process changed event
	BucketSettingsServiceCallback(path string, value []byte, rev interface{}) error

	// set the metadata change call back method
	SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback)
}
