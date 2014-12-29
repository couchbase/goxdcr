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
	"github.com/couchbase/goxdcr/base"
	"strings"
)

const (
	// ids of xdcr replication specs are used as keys in gometa service.
	// the following prefix distinguishes the replication specs from other entries
	// and reduces the chance of naming conflicts
	ReplicationSpecKeyPrefix = "replicationSpec"
)

/************************************
/* struct ReplicationSpecification
*************************************/
type ReplicationSpecification struct {
	//id of the replication
	Id string `json:"id"`

	// Source Bucket Name
	SourceBucketName string `json:"sourceBucketName"`

	//Target Cluster UUID
	TargetClusterUUID string `json:"targetClusterUUID"`

	// Target Bucket Name
	TargetBucketName string `json:"targetBucketName"`

	Settings *ReplicationSettings `json:"replicationSettings"`
	
    // revision number to be used by metadata service. not included in json
	Revision  interface{}
}

func NewReplicationSpecification(sourceBucketName string, targetClusterUUID string, targetBucketName string) *ReplicationSpecification {
	return &ReplicationSpecification{Id: ReplicationId(sourceBucketName, targetClusterUUID, targetBucketName),
		SourceBucketName:  sourceBucketName,
		TargetClusterUUID: targetClusterUUID,
		TargetBucketName:  targetBucketName,
		Settings:          DefaultSettings()}
}

func ReplicationId(sourceBucketName string, targetClusterUUID string, targetBucketName string) string {
	parts := []string{ReplicationSpecKeyPrefix, targetClusterUUID, sourceBucketName, targetBucketName}
	return strings.Join(parts, base.KeyPartsDelimiter)
}

func IsReplicationIdForSourceBucket(replicationId string, sourceBucketName string) bool {
	parts := strings.Split(replicationId, base.KeyPartsDelimiter)

	if parts[2] == sourceBucketName {
		return true
	} else {
		return false
	}
}
