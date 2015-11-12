// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package metadata

import (
	"github.com/couchbase/goxdcr/log"
)

var logger_bs *log.CommonLogger = log.NewLogger("BucketSettings", log.DefaultLoggerContext)

var LWWEnabled = "lwwEnabled"

/*
 *  bucket_settings contains bucket level settings that are applicable only to XDCR, e.g., lwwEnabled
 */
type BucketSettings struct {
	// BucketSettings is keyed by bucket UUID to avoid confusion in the case where a bucket is deleted and then re-created.
	// BucketName needs to be stored so that it can be retrieved
	BucketName string `json:"bucketName"`
	LWWEnabled bool   `json:"lwwEnabled"`

	// revision number to be used by metadata service. not included in json
	Revision interface{}
}

// this creates a new bucket settings with default values, e.g., false for lwwEnabled
func NewBucketSettings(bucketName string) *BucketSettings {
	return &BucketSettings{BucketName: bucketName}
}

func (bucketSettings *BucketSettings) ToMap() map[string]interface{} {
	settings_map := make(map[string]interface{})
	settings_map[LWWEnabled] = bucketSettings.LWWEnabled
	return settings_map
}
