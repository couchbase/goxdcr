// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

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
