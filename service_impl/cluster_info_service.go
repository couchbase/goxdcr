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
	"github.com/couchbase/goxdcr/simple_utils"
	"github.com/couchbase/goxdcr/utils"
)

type ClusterInfoSvc struct {
	logger *log.CommonLogger
}

func NewClusterInfoSvc(logger_ctx *log.LoggerContext) *ClusterInfoSvc {
	return &ClusterInfoSvc{
		logger: log.NewLogger("ClusterInfoService", logger_ctx),
	}
}

func (ci_svc *ClusterInfoSvc) getBucketInfo(clusterConnInfoProvider base.ClusterConnectionInfoProvider, bucketName string) (string, map[string]interface{}, error) {
	connStr, err := clusterConnInfoProvider.MyConnectionStr()
	if err != nil {
		return "", nil, err
	}
	userName, password, certificate, sanInCertificate, err := clusterConnInfoProvider.MyCredentials()
	if err != nil {
		return "", nil, err
	}
	bucketInfo, err := utils.GetBucketInfo(connStr, bucketName, userName, password, certificate, sanInCertificate, ci_svc.logger)

	return connStr, bucketInfo, err
}

func (ci_svc *ClusterInfoSvc) GetServerVBucketsMap(clusterConnInfoProvider base.ClusterConnectionInfoProvider, bucketName string) (map[string][]uint16, error) {
	connStr, bucketInfo, err := ci_svc.getBucketInfo(clusterConnInfoProvider, bucketName)
	if err != nil {
		return nil, err
	}

	return utils.GetServerVBucketsMap(connStr, bucketName, bucketInfo)

}

func (ci_svc *ClusterInfoSvc) GetBucketInfo(clusterConnInfoProvider base.ClusterConnectionInfoProvider, bucketName string) (string, int, map[string][]uint16, error) {
	connStr, bucketInfo, err := ci_svc.getBucketInfo(clusterConnInfoProvider, bucketName)
	if err != nil {
		return "", 0, nil, err
	}

	bucketUUID, err := utils.GetBucketUuidFromBucketInfo(bucketName, bucketInfo, ci_svc.logger)
	if err != nil {
		return "", 0, nil, err
	}

	clusterCompatibility, err := utils.GetClusterCompatibilityFromBucketInfo(bucketName, bucketInfo, ci_svc.logger)
	if err != nil {
		return "", 0, nil, err
	}

	serverVbMap, err := utils.GetServerVBucketsMap(connStr, bucketName, bucketInfo)

	return bucketUUID, clusterCompatibility, serverVbMap, err

}

func (ci_svc *ClusterInfoSvc) IsClusterCompatible(clusterConnInfoProvider base.ClusterConnectionInfoProvider, version []int) (bool, error) {

	connStr, err := clusterConnInfoProvider.MyConnectionStr()
	if err != nil {
		return false, err
	}

	username, password, certificate, sanInCertificate, err := clusterConnInfoProvider.MyCredentials()
	if err != nil {
		return false, err
	}

	// so far IsClusterCompatible is called only when the remote cluster reference is ssl enabled
	// which indicates that the target cluster is not an elastic search cluster
	// it should be safe to call GetNodeListWithFullInfo() to retrive full node info
	nodeList, err := utils.GetNodeListWithFullInfo(connStr, username, password, certificate, sanInCertificate, ci_svc.logger)
	if err == nil && len(nodeList) > 0 {
		clusterCompatibility, err := utils.GetClusterCompatibilityFromNodeList(nodeList)
		if err != nil {
			return false, err
		}
		return simple_utils.IsClusterCompatible(clusterCompatibility, version), nil
	} else {
		//should not ever get here
		return false, fmt.Errorf("Can't get nodes information for cluster %v, err=%v", connStr, err)
	}
}
