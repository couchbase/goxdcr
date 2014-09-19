package metadata_svc

import (
	metadata "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/metadata"
)

type MetadataSvc interface {
	ReplicationSpec (replicationId string) (*metadata.ReplicationSpecification, error)
	AddReplicationSpec (spec metadata.ReplicationSpecification) error
	SetReplicationSpec (spec metadata.ReplicationSpecification) error
	DelReplicationSpec (replicationId string) error
}

