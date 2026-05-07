// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata_svc

import (
	"strings"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
)

func PrepareHeartbeatPayload(clonedRef *metadata.RemoteClusterReference, topologySvc service_def.XDCRCompTopologySvc, specsReader service_def.ReplicationSpecReader) (*metadata.HeartbeatMetadata, error) {
	var sourceClusterUUID, sourceClusterName string
	var err error

	if sourceClusterUUID, err = topologySvc.MyClusterUUID(); err != nil {
		return nil, err
	}
	if sourceClusterName, err = topologySvc.MyClusterName(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(sourceClusterName) == "" {
		sourceClusterName = base.UnknownSourceClusterName
	}

	specs, err := specsReader.AllReplicationSpecsWithRemote(clonedRef)
	if err != nil {
		return nil, err
	}

	nodesList, err := topologySvc.PeerNodesAdminAddrs()
	if err != nil {
		return nil, err
	}

	// Add local node to the list only if it's a data node
	if isKV, err := topologySvc.IsKVNode(); err != nil {
		return nil, err
	} else if isKV {
		srcStr, err := topologySvc.MyHostAddr()
		if err != nil {
			return nil, err
		}
		nodesList = append(nodesList, srcStr)
	}

	hbMetadata := &metadata.HeartbeatMetadata{
		SourceClusterUUID: sourceClusterUUID,
		SourceClusterName: sourceClusterName,
		SourceSpecsList:   specs,
		NodesList:         nodesList,
		TTL:               time.Duration(base.SrcHeartbeatExpiryFactor) * base.SrcHeartbeatMaxInterval(),
	}

	return hbMetadata, nil
}
