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
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"reflect"
	"strings"
)

/************************************
/* struct ReplicationSpecification
*************************************/
type ReplicationSpecification struct {
	//id of the replication
	Id string `json:"id"`

	// Source Bucket Name
	SourceBucketName string `json:"sourceBucketName"`

	//Source Bucket UUID
	SourceBucketUUID string `json:"sourceBucketUUID"`

	//Target Cluster UUID
	TargetClusterUUID string `json:"targetClusterUUID"`

	// Target Bucket Name
	TargetBucketName string `json:"targetBucketName"`

	TargetBucketUUID string `json:"targetBucketUUID"`

	Settings *ReplicationSettings `json:"replicationSettings"`

	// revision number to be used by metadata service. not included in json
	Revision interface{}
}

func NewReplicationSpecification(sourceBucketName string, sourceBucketUUID string, targetClusterUUID string, targetBucketName string, targetBucketUUID string) *ReplicationSpecification {
	return &ReplicationSpecification{Id: ReplicationId(sourceBucketName, targetClusterUUID, targetBucketName),
		SourceBucketName:  sourceBucketName,
		TargetClusterUUID: targetClusterUUID,
		TargetBucketName:  targetBucketName,
		Settings:          DefaultSettings()}
}

// checks if the passed in spec is the same as the current spec
// used to check if a spec in cache needs to be refreshed
func (spec *ReplicationSpecification) SameSpec(spec2 *ReplicationSpecification) bool {
	if spec == nil {
		return spec2 == nil
	}
	if spec2 == nil {
		return false
	}
	// note that settings in spec are not compared. The assumption is that if settings are different, Revision will have to be different
	return spec.Id == spec2.Id && spec.SourceBucketName == spec2.SourceBucketName &&
		spec.TargetClusterUUID == spec2.TargetClusterUUID && spec.TargetBucketName == spec2.TargetBucketName &&
		reflect.DeepEqual(spec.Revision, spec2.Revision)
}

func (spec *ReplicationSpecification) Clone() *ReplicationSpecification {
	if spec == nil {
		return nil
	}
	return &ReplicationSpecification{Id: spec.Id,
		SourceBucketName:  spec.SourceBucketName,
		TargetClusterUUID: spec.TargetClusterUUID,
		TargetBucketName:  spec.TargetBucketName,
		Settings:          spec.Settings.Clone()}
}

func ReplicationId(sourceBucketName string, targetClusterUUID string, targetBucketName string) string {
	parts := []string{targetClusterUUID, sourceBucketName, targetBucketName}
	return strings.Join(parts, base.KeyPartsDelimiter)
}

func IsReplicationIdForSourceBucket(replicationId string, sourceBucketName string) (bool, error) {
	replBucketName, err := GetSourceBucketNameFromReplicationId(replicationId)
	if err != nil {
		return false, err
	} else {
		return replBucketName == sourceBucketName, nil
	}
}

func GetSourceBucketNameFromReplicationId(replicationId string) (string, error) {
	parts := strings.Split(replicationId, base.KeyPartsDelimiter)
	if len(parts) == 3 {
		return parts[1], nil
	} else {
		return "", fmt.Errorf("Invalid replication id: %v", replicationId)
	}
}
