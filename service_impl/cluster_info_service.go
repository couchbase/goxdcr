// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_impl

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	utilities "github.com/couchbase/goxdcr/utils"
)

type ClusterInfoSvc struct {
	logger *log.CommonLogger
	utils  utilities.UtilsIface
}

func NewClusterInfoSvc(logger_ctx *log.LoggerContext, utilsIn utilities.UtilsIface) *ClusterInfoSvc {
	return &ClusterInfoSvc{
		logger: log.NewLogger("ClusterInfoSvc", logger_ctx),
		utils:  utilsIn,
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

	return ci_svc.utils.GetServerVBucketsMap(connStr, bucketName, bucketInfo)

}
