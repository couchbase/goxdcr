/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package conflictlog

import (
	"github.com/couchbase/goxdcr/v8/base"
)

// BucketInfo, VBucketServerMap are the structs for the bucket topology json returned
// in the response body when memcached returns NOT_MY_VBUCKET.

type BucketInfo struct {
	VBucketServerMap VBucketServerMap   `json:"vBucketServerMap"`
	NodesExt         []NodesExtNodeInfo `json:"nodesExt"`
}

func (b *BucketInfo) GetAddrByVB(vbno uint16, replicaNum int) (idx int, hostname string, port, sslPort uint16, thisNode bool, err error) {
	idx = b.VBucketServerMap.VBucketMap[int(vbno)][replicaNum]
	nodesExtNodeInfo := &b.NodesExt[idx]
	origAddr := b.VBucketServerMap.ServerList[idx]

	hostname = base.GetHostName(origAddr)
	port = uint16(nodesExtNodeInfo.Services[base.KVPortKey])
	sslPort = uint16(nodesExtNodeInfo.Services[base.KVSSLPortKey])
	thisNode = nodesExtNodeInfo.ThisNode

	return
}

type NodesExtNodeInfo struct {
	Services map[string]int `json:"services"`
	ThisNode bool           `json:"thisNode"`
}

// VBucketServerMap is the a mapping of vbuckets to nodes.
type VBucketServerMap struct {
	NumReplicas int      `json:"numReplicas"`
	ServerList  []string `json:"serverList"`
	VBucketMap  [][]int  `json:"vBucketMap"`
}
