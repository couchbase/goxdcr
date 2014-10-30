package metadata

import (
	"strings"
)

/************************************
/* struct ReplicationSpecification
*************************************/
type ReplicationSpecification struct {
	//id of the replication
	Id string `json:"id"`
	
	//Source Cluster UUID
	SourceClusterUUID string `json:"sourceClusterUUID"`

    // Source Bucket Name
    SourceBucketName string `json:"sourceBucketName"`

	//Target Cluster UUID
	TargetClusterUUID string `json:"targetClusterUUID"`

    // Target Bucket Name
    TargetBucketName string `json:"targetBucketName"`

	//the filter name, it is going to be part of the key
	//It will not change once the replication specification is created
	FilterName string `json:"filterName"`

	Settings *ReplicationSettings `json:"replicationSettings"`
}

func NewReplicationSpecification(sourceClusterUUID string, sourceBucketName string, targetClusterUUID string, targetBucketName string, filterName string) *ReplicationSpecification {
	return &ReplicationSpecification{Id: ReplicationId(sourceClusterUUID, sourceBucketName, targetClusterUUID, targetBucketName, filterName),
		SourceClusterUUID: sourceClusterUUID,
		SourceBucketName: sourceBucketName,
		TargetClusterUUID: targetClusterUUID,
		TargetBucketName: targetBucketName,
		FilterName: filterName,
		Settings:    DefaultSettings()}
}

func ReplicationId(sourceClusterUUID string, sourceBucketName string, targetClusterUUID string, targetBucketName string, filterName string) string {
	parts := []string{sourceClusterUUID, sourceBucketName, targetClusterUUID, targetBucketName}
	if filterName != "" {
		parts = append(parts, filterName)
	}
	return strings.Join(parts, "_")
}
