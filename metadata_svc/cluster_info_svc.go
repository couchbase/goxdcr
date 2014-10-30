// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package metadata_svc

import (
	"github.com/couchbaselabs/go-couchbase"
)

type ClusterInfoSvc interface {
	GetClusterConnectionStr(ClusterUUID string) (string, error)
	GetMyActiveVBuckets(ClusterUUID string, Bucket string, NodeId string) ([]uint16, error)
	GetServerList(ClusterUUID string, Bucket string) ([]string, error)
	GetServerVBucketsMap(ClusterUUID string, Bucket string) (map[string][]uint16, error)
	IsNodeCompatible(node string, version string) (bool, error)
	GetBucket(ClusterUUID, bucketName string) (*couchbase.Bucket, error)
}
