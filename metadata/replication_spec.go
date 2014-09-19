package metadata

import (
	"errors"
	"strings"
)

/************************************
/* struct ReplicationSpecification
*************************************/
type ReplicationSpecification struct {
	//id of the replication
	id string `json:"id"`
	//<Source ClusterUUID>/<Source Bucket Name>
	source string `json:"source"`

	//<Target ClusterUUID>/<Target Bucket Name>
	target string `json:"target"`

	//the filter name, it is going to be part of the key
	//It will not change once the replication specification is created
	filter_name	string  `json:"filter_name"`
	
	settings *ReplicationSettings `json:"replicationSettings"`
}

func NewReplicationSpecification (sourceClusterUUID string, sourceBucketName string, targetClusterUUID string, targetBucketName string, filterName string) *ReplicationSpecification{
	return &ReplicationSpecification {id : replicationId (sourceClusterUUID, sourceBucketName, targetClusterUUID, targetBucketName, filterName),
	source : constructEndIdentifier (sourceClusterUUID, sourceBucketName),
	target : constructEndIdentifier (targetClusterUUID, targetBucketName),
	filter_name : filterName,
	settings: DefaultSettings()}
}

func (rep_spec *ReplicationSpecification) Id() string {
	return rep_spec.id
}

func (rep_spec *ReplicationSpecification) SourceClusterUUID() (string, error) {
	return parse(rep_spec.source, "Failed to get source cluster UUID", 0)
}

func (rep_spec *ReplicationSpecification) SourceBucketName() (string, error) {
	return parse(rep_spec.source, "Failed to get source bucket name", 1)
}

func (rep_spec *ReplicationSpecification) TargetClusterUUID() (string, error) {
	return parse(rep_spec.target, "Failed to get target cluster UUID", 0)
}

func (rep_spec *ReplicationSpecification) TargetBucketName() (string, error) {
	return parse(rep_spec.target, "Failed to get target cluster bucket", 1)
}

func (rep_spec *ReplicationSpecification) FilterName() string {
	return rep_spec.filter_name
}

func (rep_spec *ReplicationSpecification) Settings() *ReplicationSettings {
	return rep_spec.settings
}

func (rep_spec *ReplicationSpecification) SetSettings(settings *ReplicationSettings) {
	rep_spec.settings = settings
}

func parse(bucketStr string, errorStr string, segmentIndex int) (string, error) {
	parts := strings.Split(bucketStr, "/")
	if parts == nil && len(parts) != 2 {
		return "", errors.New(errorStr)
	}
	return parts[segmentIndex], nil

}

func replicationId (sourceClusterUUID string, sourceBucketName string, targetClusterUUID string, targetBucketName string, filterName string) string {
	parts := []string{sourceClusterUUID, sourceBucketName, targetClusterUUID, targetBucketName}
	if filterName != "" {
		parts = append (parts, filterName)
	}
	return strings.Join (parts, "_")
}

func constructEndIdentifier (clusterUUID string, bucketName string ) string {
	parts := []string{clusterUUID, bucketName}
	return strings.Join (parts, "/")
}
