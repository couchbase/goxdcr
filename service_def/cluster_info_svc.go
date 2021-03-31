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
)

type ClusterInfoSvc interface {
	// This API should be called on source cluster only
	GetLocalServerVBucketsMap(clusterConnInfoProvider base.ClusterConnectionInfoProvider, Bucket string) (map[string][]uint16, error)
	IsClusterCompatible(clusterConnInfoProvider base.ClusterConnectionInfoProvider, version []int) (bool, error)
}
