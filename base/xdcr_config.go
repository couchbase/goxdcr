package base

import (

)

// configuration of a particular replication
type XDCRConfig struct {
	SourceCluster string  // source cluster address
	SourceBucketn string   // source bucket name
	NumSourceConn  int  // number of connections per source KV node
	TargetCluster string  // target cluster address
	TargetBucketn string   // target bucket name
	NumTargetConn  int  // number of connections to target cluster
}

// XDCR deployment configuration
type XDCRDeployConfig struct {
	XDCRNodeMap map[string][]string  // XDCRNodeAddr -> List of KVNodeAddr that the XDCR node is responsible for
}

func NewXDCRConfig(SourceCluster string, SourceBucketn string, NumSourceConn int, 
	TargetCluster string, TargetBucketn string, NumTargetConn  int) *XDCRConfig {
	return &XDCRConfig {
	SourceCluster: SourceCluster,
	SourceBucketn: SourceBucketn,
	NumSourceConn: NumSourceConn,
	TargetCluster: TargetCluster, 
	TargetBucketn: TargetBucketn,
	NumTargetConn: NumTargetConn,
	}
}

func GetXDCRConfigFromMetadataStore(topic string) (*XDCRConfig, error) {
	// TODO get replication config for a particular topic from metadata store
	return nil, nil
}