// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/couchbase/goxdcr/v8/base"
)

/*
***********************************
/* struct ReplicationSpecification
************************************
*/
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

func (spec *ReplicationSpecification) UniqueId() string {
	if spec == nil {
		return ""
	}

	return fmt.Sprintf("%s-%s", spec.Id, spec.InternalId)
}

func (spec *ReplicationSpecification) GetReplicationSpec() *ReplicationSpecification {
	return spec
}

func (spec *ReplicationSpecification) GetBackfillSpec() *BackfillReplicationSpec {
	return nil
}

func (spec *ReplicationSpecification) Type() ReplicationType {
	return MainReplication
}

func (spec *ReplicationSpecification) GetFullId() string {
	return spec.Id
}

func (spec *ReplicationSpecification) String() string {
	if spec == nil {
		return ""
	}
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

func (spec *ReplicationSpecification) SameSpecGeneric(other GenericSpecification) bool {
	return spec.SameSpec(other.(*ReplicationSpecification))
}

func (spec *ReplicationSpecification) CloneGeneric() GenericSpecification {
	return spec.Clone()
}

func (spec *ReplicationSpecification) RedactGeneric() GenericSpecification {
	return spec.Redact()
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

// Looks for both:
// 136082a7c89ccdc9aed81cb04a97720f/B1/B2
// and
// backfill_136082a7c89ccdc9aed81cb04a97720f/B1/B2
const ReplicationIdRegexpStr = "([a-z]+_)*([[:alnum:]]+)/([A-Za-z0-9._%-]+)/([A-Za-z0-9._%-]+)$"

var replicationRegexpGroupCount = 5

func IsAReplicationId(replicationId string) bool {
	// The replication ID could have "backfill_" in front of it, or not... so don't enforce a ^ at the beginning
	matched, _ := regexp.MatchString(ReplicationIdRegexpStr, replicationId)
	return matched
}

type ReplIdComposition struct {
	TargetClusterUUID string
	SourceBucketName  string
	TargetBucketName  string
	PipelineType      string
}

var decompositionRegexp = regexp.MustCompile(ReplicationIdRegexpStr)

func DecomposeReplicationId(replicationId string, reuseStruct *ReplIdComposition) *ReplIdComposition {
	if !IsAReplicationId(replicationId) {
		return reuseStruct
	}

	var compositionStruct = reuseStruct
	if compositionStruct == nil {
		compositionStruct = &ReplIdComposition{}
	}

	submatches := decompositionRegexp.FindStringSubmatch(replicationId)
	if len(submatches) != replicationRegexpGroupCount {
		panic("FIXME")
	}

	if len(submatches[1]) > 0 {
		// For now, only backfill
		compositionStruct.PipelineType = "Backfill"
	} else {
		compositionStruct.PipelineType = "Main"
	}
	compositionStruct.TargetClusterUUID = submatches[2]
	compositionStruct.SourceBucketName = submatches[3]
	compositionStruct.TargetBucketName = submatches[4]
	return compositionStruct
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

func ParseBackfillIntoSettingMap(incomingReq string, replSpec *ReplicationSpecification) (map[string]interface{}, error) {
	var err error

	// Validate the incoming request
	var sourceNamespace *SourceNamespace
	collectionMode := replSpec.Settings.GetCollectionModes()
	checkDefaultNs, defaultNsErr := base.NewCollectionNamespaceFromString(incomingReq)
	if !collectionMode.IsMigrationOn() || (defaultNsErr == nil && checkDefaultNs.IsDefault()) {
		// NonMigration means incoming request should be a specific namespace
		// OR Migration mode is on but specified default source collection, meaning IsExplicitMigrationRule() is true
		collectionNamespace, err := base.NewCollectionNamespaceFromString(incomingReq)
		if err != nil {
			return nil, fmt.Errorf("Unable to validate collection namespace: %v", err)
		}
		sourceNamespace = NewSourceCollectionNamespace(&collectionNamespace)
	} else {
		// incomingReq should be a rule
		var fakeDP base.FakeDataPool
		sourceNamespace, err = NewSourceMigrationNamespace(incomingReq, &fakeDP)
		if err != nil {
			return nil, fmt.Errorf("Unable to validate migration rule: %v", err)
		}
	}

	// Translate into a mapping where the manual backfill logic only cares about source namespace
	backfillMapping := make(CollectionNamespaceMapping)
	backfillMapping.AddSingleSourceNsMapping(sourceNamespace, &base.CollectionNamespace{})

	settingsMap := make(map[string]interface{})
	settingsMap[base.NameKey] = replSpec.Id
	settingsMap[CollectionsManualBackfillKey] = backfillMapping
	return settingsMap, nil
}

func ParseDelBackfillIntoSettingMap(replId string) map[string]interface{} {
	settingsMap := make(map[string]interface{})
	settingsMap[base.NameKey] = replId
	settingsMap[CollectionsDelAllBackfillKey] = true
	return settingsMap
}

type ReplSpecList []*ReplicationSpecification

func (r ReplSpecList) Clone() ReplSpecList {
	newList := make([]*ReplicationSpecification, len(r))
	for i, spec := range r {
		newList[i] = spec.Clone()
	}
	return newList
}

func (r ReplSpecList) SameAs(other ReplSpecList) bool {
	if len(r) != len(other) {
		return false
	}

	otherMap := make(map[string]*ReplicationSpecification, len(other))
	for _, spec := range other {
		otherMap[spec.Id] = spec
	}

	for _, rSpec := range r {
		otherSpec, exists := otherMap[rSpec.Id]
		if !exists || !rSpec.SameSpec(otherSpec) {
			return false
		}
	}
	return true
}

func (r ReplSpecList) String() any {
	var strBuilder strings.Builder
	for _, spec := range r {
		strBuilder.WriteString(fmt.Sprintf("Id: %s Settings: %s ", spec.Id, spec.Settings.ToMap(false)))
	}
	return strBuilder.String()
}

func (r ReplSpecList) Redact() ReplSpecList {
	if r == nil {
		return nil
	}

	for i, spec := range r {
		r[i] = spec.Redact()
	}
	return r
}
