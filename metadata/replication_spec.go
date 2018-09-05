// Copyright (c) 2013-2019 Couchbase, Inc.
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

	// internal id, used to detect the case when replication spec has been deleted and recreated
	InternalId string `json:"internalId"`

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

func NewReplicationSpecification(sourceBucketName string, sourceBucketUUID string, targetClusterUUID string, targetBucketName string, targetBucketUUID string) (*ReplicationSpecification, error) {
	randId, err := base.GenerateRandomId(base.LengthOfRandomId, base.MaxRetryForRandomIdGeneration)
	if err != nil {
		return nil, err
	}
	return &ReplicationSpecification{Id: ReplicationId(sourceBucketName, targetClusterUUID, targetBucketName),
		InternalId:        randId,
		SourceBucketName:  sourceBucketName,
		SourceBucketUUID:  sourceBucketUUID,
		TargetClusterUUID: targetClusterUUID,
		TargetBucketName:  targetBucketName,
		TargetBucketUUID:  targetBucketUUID,
		Settings:          DefaultReplicationSettings()}, nil
}

func (spec *ReplicationSpecification) String() string {
	var specSettingsMap ReplicationSettingsMap
	if spec.Settings != nil {
		specSettingsMap = spec.Settings.CloneAndRedact().ToMap(false /*defaultSettings*/)
	}
	return fmt.Sprintf("Id: %v InternalId: %v SourceBucketName: %v SourceBucketUUID: %v TargetClusterUUID: %v TargetBucketName: %v TargetBucketUUID: %v Settings: %v",
		spec.Id, spec.InternalId, spec.SourceBucketName, spec.SourceBucketUUID, spec.TargetClusterUUID, spec.TargetBucketName, spec.TargetBucketUUID, specSettingsMap)
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
	return spec.Id == spec2.Id && spec.InternalId == spec2.InternalId &&
		spec.SourceBucketName == spec2.SourceBucketName &&
		spec.SourceBucketUUID == spec2.SourceBucketUUID &&
		spec.TargetClusterUUID == spec2.TargetClusterUUID && spec.TargetBucketName == spec2.TargetBucketName &&
		spec.TargetBucketUUID == spec2.TargetBucketUUID && reflect.DeepEqual(spec.Revision, spec2.Revision)
}

func (spec *ReplicationSpecification) Clone() *ReplicationSpecification {
	if spec == nil {
		return nil
	}
	return &ReplicationSpecification{Id: spec.Id,
		InternalId:        spec.InternalId,
		SourceBucketName:  spec.SourceBucketName,
		SourceBucketUUID:  spec.SourceBucketUUID,
		TargetClusterUUID: spec.TargetClusterUUID,
		TargetBucketName:  spec.TargetBucketName,
		TargetBucketUUID:  spec.TargetBucketUUID,
		Settings:          spec.Settings.Clone(),
		// !!! shallow copy of revision.
		// spec.Revision should only be passed along and should never be modified
		Revision: spec.Revision}
}

func (spec *ReplicationSpecification) Redact() *ReplicationSpecification {
	if spec != nil {
		// Currently only the Settings has user identifiable data in filtered expression
		spec.Settings.Redact()
	}
	return spec
}

func (spec *ReplicationSpecification) CloneAndRedact() *ReplicationSpecification {
	if spec != nil {
		return spec.Clone().Redact()
	}
	return spec
}

// Modifies the in-memory version of the spec, does not persist changes onto metakv
// This should only be called once, after loading from metakv
func (spec *ReplicationSpecification) UpgradeFilterIfNeeded() {
	if spec.Settings == nil || len(spec.Settings.FilterExpression) == 0 {
		return
	}

	if _, ok := spec.Settings.Values[FilterVersionKey]; !ok {
		// This shouldn't happen... but for now, assume that the filter was input as a key version
		// since spec creation should have entered it as a valid value
		spec.Settings.Values[FilterVersionKey] = base.FilterVersionKeyOnly
	}

	if spec.Settings.Values[FilterVersionKey] == base.FilterVersionKeyOnly {
		spec.Settings.FilterExpression = base.UpgradeFilter(spec.Settings.FilterExpression)
		spec.Settings.Values[FilterExpressionKey] = spec.Settings.FilterExpression
	}
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

func IsReplicationIdForTargetBucket(replicationId string, targetBucketName string) (bool, error) {
	replBucketName, err := GetTargetBucketNameFromReplicationId(replicationId)
	if err != nil {
		return false, err
	} else {
		return replBucketName == targetBucketName, nil
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

func GetTargetBucketNameFromReplicationId(replicationId string) (string, error) {
	parts := strings.Split(replicationId, base.KeyPartsDelimiter)
	if len(parts) == 3 {
		return parts[2], nil
	} else {
		return "", fmt.Errorf("Invalid replication id: %v", replicationId)
	}
}
