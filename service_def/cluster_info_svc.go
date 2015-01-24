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
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/goxdcr/base"
)

type ClusterInfoSvc interface {
	GetMyActiveVBuckets(clusterConnInfoProvider base.ClusterConnectionInfoProvider, Bucket, NodeId string) ([]uint16, error)
	GetServerList(clusterConnInfoProvider base.ClusterConnectionInfoProvider, Bucket string) ([]string, error)
	GetNodes(clusterConnInfoProvider base.ClusterConnectionInfoProvider) ([]couchbase.Node, error)
	GetServerVBucketsMap(clusterConnInfoProvider base.ClusterConnectionInfoProvider, Bucket string) (map[string][]uint16, error)
	IsClusterCompatible(clusterConnInfoProvider base.ClusterConnectionInfoProvider, version []int) (bool, error)
	GetBucket(clusterConnInfoProvider base.ClusterConnectionInfoProvider, bucketName string) (*couchbase.Bucket, error)
}
