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
	"github.com/Xiaomei-Zhang/goxdcr/base"
	"github.com/Xiaomei-Zhang/goxdcr/metadata"
	"github.com/Xiaomei-Zhang/goxdcr/utils"
	"github.com/couchbaselabs/go-couchbase"
	rm "github.com/Xiaomei-Zhang/goxdcr/replication_manager"
)

var options struct {
	sourceBucket    string // source bucket
	targetBucket    string //target bucket
	sourceClusterAddr      string //source cluster addr
	targetClusterAddr      string //target cluster addr
	sourceKVHost      string //source kv host name
	numConnPerKV    int    // number of connections per source KV node
	numOutgoingConn int    // number of connections to target cluster
	username        string //username on source cluster
	password        string //password on source cluster
	maxVbno         int    // maximum number of vbuckets
}

func SetTestOptions(sourceBucket, targetBucket, sourceClusterAddr, targetClusterAddr, sourceKVHost, username, password string, numConnPerKV, numOutgoingConn int) {
	options.sourceBucket = sourceBucket
	options.targetBucket = targetBucket
	options.sourceClusterAddr = sourceClusterAddr
	options.targetClusterAddr = targetClusterAddr
	options.sourceKVHost = sourceKVHost
	options.username = username
	options.password = password
	options.numConnPerKV = numConnPerKV
	options.numOutgoingConn = numOutgoingConn 
}

type MockMetadataSvc struct {
	specs map[string]metadata.ReplicationSpecification
}

func NewMockMetadataSvc() *MockMetadataSvc {
	return &MockMetadataSvc{specs: make(map[string]metadata.ReplicationSpecification)}
}
func (mock_meta_svc *MockMetadataSvc) ReplicationSpec(replicationId string) (*metadata.ReplicationSpecification, error) {
	spec, ok := mock_meta_svc.specs[replicationId]
	if !ok {
		spec_ptr := metadata.NewReplicationSpecification(options.sourceClusterAddr, options.sourceBucket, options.targetClusterAddr, options.targetBucket, "")
		settings := spec_ptr.Settings
		settings.TargetNozzlePerNode = options.numOutgoingConn
		settings.SourceNozzlePerNode = options.numConnPerKV
		mock_meta_svc.specs[replicationId] = *spec_ptr
		return spec_ptr, nil
	}else {
		return &spec, nil
	}
}

func (mock_meta_svc *MockMetadataSvc) AddReplicationSpec(spec metadata.ReplicationSpecification) error {
	mock_meta_svc.specs[spec.Id] = spec
	return nil
}

func (mock_meta_svc *MockMetadataSvc) SetReplicationSpec(spec metadata.ReplicationSpecification) error {
	mock_meta_svc.specs[spec.Id] = spec
	return nil
}

func (mock_meta_svc *MockMetadataSvc) DelReplicationSpec(replicationId string) error {
	delete(mock_meta_svc.specs, replicationId)
	return nil
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