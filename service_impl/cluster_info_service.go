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
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
)

type ClusterInfoSvc struct {
	logger *log.CommonLogger
	utils  utilities.UtilsIface
	secSvc service_def.SecuritySvc
	isIpv6 bool
}

func NewClusterInfoSvc(logger_ctx *log.LoggerContext, utilsIn utilities.UtilsIface, sec service_def.SecuritySvc, isIpv6 bool) *ClusterInfoSvc {
	return &ClusterInfoSvc{
		logger: log.NewLogger("ClusterInfoSvc", logger_ctx),
		utils:  utilsIn,
		secSvc: sec,
		isIpv6: isIpv6,
	}
}

func (ci_svc *ClusterInfoSvc) getBucketInfo(clusterConnInfoProvider base.ClusterConnectionInfoProvider, bucketName string) (string, map[string]interface{}, error) {
	connStr, err := clusterConnInfoProvider.MyConnectionStr()
	if err != nil {
		return "", nil, err
	}
	userName, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := clusterConnInfoProvider.MyCredentials()
	if err != nil {
		return "", nil, err
	}
	bucketInfo, err := ci_svc.utils.GetBucketInfo(connStr, bucketName, userName, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, ci_svc.logger)

	return connStr, bucketInfo, err
}

func (ci_svc *ClusterInfoSvc) GetLocalServerVBucketsMap(clusterConnInfoProvider base.ClusterConnectionInfoProvider, bucketName string) (map[string][]uint16, error) {
	connStr, bucketInfo, err := ci_svc.getBucketInfo(clusterConnInfoProvider, bucketName)
	if err != nil {
		return nil, err
	}

	serverVBMap, err := ci_svc.utils.GetServerVBucketsMap(connStr, bucketName, bucketInfo)
	if err != nil {
		return nil, err
	}
	return ci_svc.updateLocalServerVBucketMapIfNeeded(serverVBMap, bucketInfo)
}

// When cluster uses strict encryption, we need to use loopback address for local server
// and set the key in serverVBMap accordingly
func (ci_svc *ClusterInfoSvc) updateLocalServerVBucketMapIfNeeded(serverVBMap map[string][]uint16, bucketInfo map[string]interface{}) (map[string][]uint16, error) {
	if ci_svc.secSvc.IsClusterEncryptionLevelStrict() == false {
		return serverVBMap, nil
	}
	loopback := base.LocalHostName
	if ci_svc.isIpv6 {
		loopback = base.LocalHostNameIpv6
	}
	currentHostAddr, err := ci_svc.utils.GetCurrentHostnameFromBucketInfo(bucketInfo)
	if err != nil {
		return nil, err
	}
	currentHostName := base.GetHostName(currentHostAddr)
	if currentHostName == loopback {
		return serverVBMap, nil
	}
	newServerVBMap := make(map[string][]uint16)
	for server, vbs := range serverVBMap {
		hostName := base.GetHostName(server)
		if hostName == currentHostName {
			// Change the map to use loopback
			port, err := base.GetPortNumber(server)
			if err != nil {
				return nil, err
			}
			newServerVBMap[base.GetHostAddr(loopback, port)] = vbs
		} else {
			newServerVBMap[server] = vbs
		}
	}
	return newServerVBMap, nil
}

func (ci *ClusterInfoSvc) IsClusterEncryptionLevelStrict() bool {
	return ci.secSvc.IsClusterEncryptionLevelStrict()
}
