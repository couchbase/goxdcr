package metadata_svc

import (
	"github.com/Xiaomei-Zhang/goxdcr/metadata"
)

type MetadataSvc interface {
	ReplicationSpec(replicationId string) (*metadata.ReplicationSpecification, error)
	AddReplicationSpec(spec metadata.ReplicationSpecification) error
	SetReplicationSpec(spec metadata.ReplicationSpecification) error
	DelReplicationSpec(replicationId string) error
}
