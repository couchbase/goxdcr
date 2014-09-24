package metadata_svc

import (
	metadata "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/metadata"
)

type InternalReplicationSettingsSvc interface {
	GetInternalReplicationSettings() (*metadata.ReplicationSettings, error)
	SetInternalReplicationSettings(*metadata.ReplicationSettings) error
}
