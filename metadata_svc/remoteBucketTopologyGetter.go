// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata_svc

import "github.com/couchbase/goxdcr/v8/service_def"

type BucketTopologyGetter struct {
	bucketName string
	// Returns bucketInfo, usesExternal, error
	bucketInfoGetter service_def.BucketInfoGetter
}

type MaxCasStatsGetter struct {
	bucketName        string
	maxCasStatsGetter service_def.MaxVBCasStatsGetter
}

func NewBucketTopologyGetter(bucketName string, bucketInfoGetter service_def.BucketInfoGetter) *BucketTopologyGetter {
	return &BucketTopologyGetter{
		bucketName:       bucketName,
		bucketInfoGetter: bucketInfoGetter,
	}
}

func NewMaxCasStatsGetter(bucketName string, maxCasStatsGetter service_def.MaxVBCasStatsGetter) *MaxCasStatsGetter {
	return &MaxCasStatsGetter{
		bucketName:        bucketName,
		maxCasStatsGetter: maxCasStatsGetter,
	}
}
