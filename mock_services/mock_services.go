// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// mock services 
package mock_services

import (
	"fmt"
	"strings"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/utils"
	"github.com/couchbaselabs/go-couchbase"
	rm "github.com/couchbase/goxdcr/replication_manager"
)

var options struct {
	sourceClusterAddr      string //source cluster addr
	sourceKVHost      string //source kv host name
	username        string //username on source cluster
	password        string //password on source cluster
}

func SetTestOptions(sourceClusterAddr, sourceKVHost, username, password string) {
	options.sourceClusterAddr = sourceClusterAddr
	options.sourceKVHost = sourceKVHost
	options.username = username
	options.password = password
}

type MockClusterInfoSvc struct {
}

func (mock_ci_svc *MockClusterInfoSvc) GetClusterConnectionStr(ClusterUUID string) (string, error) {
	return ClusterUUID, nil
}

func (mock_ci_svc *MockClusterInfoSvc) GetMyActiveVBuckets(ClusterUUID string, bucketName string, NodeId string) ([]uint16, error) {
	sourceCluster, err := mock_ci_svc.GetClusterConnectionStr(ClusterUUID)
	if err != nil {
		return nil, err
	}
	b, err := utils.Bucket(sourceCluster, bucketName, options.username, options.password)
	if err != nil {
		return nil, err
	}

	// in test env, there should be only one kv in bucket server list
	kvaddr := b.VBServerMap().ServerList[0]

	m, err := b.GetVBmap([]string{kvaddr})
	if err != nil {
		return nil, err
	}

	vbList := m[kvaddr]

	return vbList, nil
}

func (mock_ci_svc *MockClusterInfoSvc) GetServerList(ClusterUUID string, bucketName string) ([]string, error) {
	cluster, err := mock_ci_svc.GetClusterConnectionStr(ClusterUUID)
	if err != nil {
		return nil, err
	}
	bucket, err := utils.Bucket(cluster, bucketName, options.username, options.password)
	if err != nil {
		return nil, err
	}

	// in test env, there should be only one kv in bucket server list
	serverlist := bucket.VBServerMap().ServerList

	return serverlist, nil
}

func (mock_ci_svc *MockClusterInfoSvc) GetServerVBucketsMap(ClusterUUID string, bucketName string) (map[string][]uint16, error) {
	cluster, err := mock_ci_svc.GetClusterConnectionStr(ClusterUUID)
	fmt.Printf("cluster=%s\n", cluster)
	if err != nil {
		return nil, err
	}
	bucket, err := utils.Bucket(cluster, bucketName, options.username, options.password)
	if err != nil {
		return nil, err
	}
	fmt.Printf("ServerList=%v\n", bucket.VBServerMap().ServerList)
	serverVBMap, err := bucket.GetVBmap(bucket.VBServerMap().ServerList)
	fmt.Printf("ServerVBMap=%v\n", serverVBMap)
	return serverVBMap, err
}

func (mock_ci_svc *MockClusterInfoSvc) IsNodeCompatible(node string, version string) (bool, error) {
	return true, nil
}

func (mock_ci_svc *MockClusterInfoSvc) GetBucket(clusterUUID, bucketName string) (*couchbase.Bucket, error) {
	clusterConnStr, err := mock_ci_svc.GetClusterConnectionStr(clusterUUID)
	if err != nil {
		return nil, err
	}
	return utils.Bucket(clusterConnStr, bucketName, options.username, options.password)
}


type MockXDCRTopologySvc struct {
}

func (mock_top_svc *MockXDCRTopologySvc) MyHost() (string, error) {
	return options.sourceKVHost, nil
}

func (mock_top_svc *MockXDCRTopologySvc) MyAdminPort() (uint16, error) {
	return uint16(base.AdminportNumber), nil
}

func (mock_top_svc *MockXDCRTopologySvc) MyKVNodes() ([]string, error) {
	// as of now each xdcr instance is responsible for only one kv node
	nodes := make([]string, 1)
	nodes[0] = options.sourceKVHost
	return nodes, nil
}

func (mock_top_svc *MockXDCRTopologySvc) XDCRTopology() (map[string]uint16, error) {
	retmap := make(map[string]uint16)
	sourceCluster, err := mock_top_svc.MyCluster()
		if err != nil {
		return nil, err
	}
	serverList, err := rm.ClusterInfoService().GetServerList(sourceCluster, "default")
	if err != nil {
		return nil, err
	}
	for _, server := range serverList {
		serverName := (strings.Split(server, ":"))[0]
		retmap[serverName] = uint16(base.AdminportNumber)
	}
	return retmap, nil
}

func (mock_top_svc *MockXDCRTopologySvc) XDCRCompToKVNodeMap() (map[string][]string, error) {
	retmap := make(map[string][]string)
	return retmap, nil
}

func (mock_top_svc *MockXDCRTopologySvc) MyCluster() (string, error) {
	return options.sourceClusterAddr, nil
}

type MockReplicationSettingsSvc struct {
}

func (mock_repl_settings_svc *MockReplicationSettingsSvc) GetReplicationSettings() (*metadata.ReplicationSettings, error) {
	return metadata.DefaultSettings(), nil
}
	
func (mock_repl_settings_svc *MockReplicationSettingsSvc) SetReplicationSettings(*metadata.ReplicationSettings) error {
	return nil
}