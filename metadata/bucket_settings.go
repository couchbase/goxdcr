// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata

import (
	"github.com/couchbase/goxdcr/v8/log"
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
