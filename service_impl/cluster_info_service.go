// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_impl

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/utils"
	"github.com/couchbase/go-couchbase"
)

type ClusterInfoSvc struct {
	logger *log.CommonLogger
}

func NewClusterInfoSvc(logger_ctx *log.LoggerContext) *ClusterInfoSvc {
	return &ClusterInfoSvc{
		logger: log.NewLogger("ClusterInfoService", logger_ctx),
	}
}

func (ci_svc *ClusterInfoSvc) GetMyActiveVBuckets(clusterConnInfoProvider base.ClusterConnectionInfoProvider, bucketName, NodeId string) ([]uint16, error) {
	bucket, err := ci_svc.GetBucket(clusterConnInfoProvider, bucketName)
	if err != nil {
		return nil, err
	}
	defer bucket.Close()

	// in test env, there should be only one kv in bucket server list
	kvaddr := bucket.VBServerMap().ServerList[0]

	m, err := bucket.GetVBmap([]string{kvaddr})
	if err != nil {
		return nil, err
	}

	vbList := m[kvaddr]

	return vbList, nil
}

func (ci_svc *ClusterInfoSvc) GetServerList(clusterConnInfoProvider base.ClusterConnectionInfoProvider, bucketName string) ([]string, error) {
	bucket, err := ci_svc.GetBucket(clusterConnInfoProvider, bucketName)
	if err != nil {
		return nil, err
	}
	defer bucket.Close()

	// in test env, there should be only one kv in bucket server list
	serverlist := bucket.VBServerMap().ServerList

	return serverlist, nil
}

func (ci_svc *ClusterInfoSvc) GetServerVBucketsMap(clusterConnInfoProvider base.ClusterConnectionInfoProvider, bucketName string) (map[string][]uint16, error) {
	bucket, err := ci_svc.GetBucket(clusterConnInfoProvider, bucketName)
	if err != nil {
		return nil, err
	} else if bucket == nil {
		return nil, fmt.Errorf("Failed to get bucket %v", bucketName)
	}

	defer func() {
		if bucket != nil {
			bucket.Close()
		}
	}()

	serverVBMap, err := bucket.GetVBmap(bucket.VBServerMap().ServerList)

	return serverVBMap, err
}

func (ci_svc *ClusterInfoSvc) IsClusterCompatible(clusterConnInfoProvider base.ClusterConnectionInfoProvider, version []int) (bool, error) {
	nodes, err := ci_svc.GetNodes(clusterConnInfoProvider)
	if err == nil && len(nodes) > 0 {
		clusterCompatibility := nodes[0].ClusterCompatibility
		effectiveVersion := ci_svc.encodeVersionToEffectiveVersion (version)
		compatible := ci_svc.isVersionCompatible(clusterCompatibility, effectiveVersion)
		return compatible, nil

	} else {
		//should not ever get here
		constr, _ := clusterConnInfoProvider.MyConnectionStr()
		return false, fmt.Errorf("Can't get nodes information for cluster %v, err=%v", constr, err)
	}
}

func (ci_svc *ClusterInfoSvc) encodeVersionToEffectiveVersion(version []int) int {
	majorVersion := 0
	minorVersion := 0
	if len(version) > 0 {
		majorVersion = version[0]
	}
	if len(version) > 1 {
		minorVersion = version[1]
	}

	effectiveVersion := majorVersion*0x10000 + minorVersion

	ci_svc.logger.Debugf("version=%v, effectiveVersion=%d\n", version, effectiveVersion)
	return effectiveVersion
}

func (ci_svc *ClusterInfoSvc) isVersionCompatible(clusterCompatibleVersion int, version int) bool {
	return clusterCompatibleVersion >= version
}

func (ci_svc *ClusterInfoSvc) GetBucket(clusterConnInfoProvider base.ClusterConnectionInfoProvider, bucketName string) (*couchbase.Bucket, error) {
	connStr, err := clusterConnInfoProvider.MyConnectionStr()
	if err != nil {
		return nil, err
	}
	var bucket *couchbase.Bucket = nil

	switch clusterConnInfoProvider.(type) {
	case *metadata.RemoteClusterReference:
		username, password, err1 := clusterConnInfoProvider.MyCredentials()
		if err1 != nil {
			return nil, err1
		}
		bucket, err = utils.RemoteBucket(connStr, bucketName, username, password)
	default:
		bucket, err = utils.LocalBucket(connStr, bucketName)
	}
	return bucket, err
}

func (ci_svc *ClusterInfoSvc) GetNodes(clusterConnInfoProvider base.ClusterConnectionInfoProvider) ([]couchbase.Node, error) {
	connStr, err := clusterConnInfoProvider.MyConnectionStr()
	if err != nil {
		return nil, err
	}

	var pool couchbase.Pool
	switch clusterConnInfoProvider.(type) {
	case *metadata.RemoteClusterReference:
		username, password, err := clusterConnInfoProvider.MyCredentials()
		if err != nil {
			return nil, err
		}
		pool, err = utils.RemotePool(connStr, username, password)
	default:
		pool, err = utils.LocalPool(connStr)
	}

	if err != nil {
		return nil, err
	}

	return pool.Nodes, nil

}

