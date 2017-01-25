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
	"github.com/couchbase/goxdcr/utils"
	"reflect"
)

type ClusterInfoSvc struct {
	logger *log.CommonLogger
}

func NewClusterInfoSvc(logger_ctx *log.LoggerContext) *ClusterInfoSvc {
	return &ClusterInfoSvc{
		logger: log.NewLogger("ClusterInfoSvc", logger_ctx),
	}
}

func (ci_svc *ClusterInfoSvc) GetServerVBucketsMap(clusterConnInfoProvider base.ClusterConnectionInfoProvider, bucketName string) (map[string][]uint16, error) {
	connStr, err := clusterConnInfoProvider.MyConnectionStr()
	if err != nil {
		return nil, err
	}
	userName, password, certificate, sanInCertificate, err := clusterConnInfoProvider.MyCredentials()
	if err != nil {
		return nil, err
	}

	bucketInfo, err := utils.GetBucketInfo(connStr, bucketName, userName, password, certificate, sanInCertificate, ci_svc.logger)
	if err != nil {
		return nil, err
	}

	return utils.GetServerVBucketsMap(connStr, bucketName, bucketInfo)

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
		firstNode, ok := nodeList[0].(map[string]interface{})
		if !ok {
			return false, fmt.Errorf("node info is of wrong type for cluster %v. node info=%v", connStr, nodeList[0])
		}
		clusterCompatibility, ok := firstNode[base.ClusterCompatibilityKey]
		if !ok {
			return false, fmt.Errorf("Can't get cluster compatibility info for cluster %v", connStr)
		}
		clusterCompatibilityFloat, ok := clusterCompatibility.(float64)
		if !ok {
			return false, fmt.Errorf("cluster compatibility for cluster %v is not of int type. type=%v", connStr, reflect.TypeOf(clusterCompatibility))
		}

		effectiveVersion := ci_svc.encodeVersionToEffectiveVersion(version)
		compatible := ci_svc.isVersionCompatible(int(clusterCompatibilityFloat), effectiveVersion)
		return compatible, nil
	} else {
		//should not ever get here
		return false, fmt.Errorf("Can't get nodes information for cluster %v, err=%v", connStr, err)
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
