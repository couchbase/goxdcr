package metadata_svc

import (
	metadata "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/metadata"
)

// this service is likely provided by exposing an erlang rest api in ns_server
type ReplicationSettingsSvc interface {
	GetReplicationSettings() (*metadata.ReplicationSettings, error)
	SetReplicationSettings(*metadata.ReplicationSettings) error
}
