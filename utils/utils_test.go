/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"strings"
	"testing"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/metadata"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/base/filter"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcMetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// The test data's external node name is 10.10.10.10 instead of the regular 127.0.0.1
var localAddresses = []string{"127.0.0.1", "cb-example-0000.cb-example.default.svc"}

var externalAddresses = []string{"10.10.10.10", "11.11.11.11", "12.12.12.12",
	"cb-example-0000.cb-example.us-west1.spjmurray.co.uk", "cb-example-0001.cb-example.us-west1.spjmurray.co.uk", "cb-example-0002.cb-example.us-west1.spjmurray.co.uk"}

const testExternalDataDir = "testExternalData/"
const testInternalDataDir = "testInternalData/"
const testK8DataDir = "testK8ExternalData/"
const kvSSLDir = "alternateAddrKVSSL/"

var logger = log.NewLogger("testLogger", log.DefaultLoggerContext)
var testUtils = NewUtilities()
var connStr = "testConnStr"

func readJsonHelper(fileName string) (retMap map[string]interface{}, byteSlice []byte, err error) {
	byteSlice, err = ioutil.ReadFile(fileName)
	if err != nil {
		return
	}
	var unmarshalledIface interface{}
	err = json.Unmarshal(byteSlice, &unmarshalledIface)
	if err != nil {
		return
	}

	retMap = unmarshalledIface.(map[string]interface{})
	return
}

// Contacts remoteNode/pools
func getClusterInfoMock(external bool) (map[string]interface{}, error) {
	var fileName string
	if external {
		fileName = fmt.Sprintf("%v%v", testExternalDataDir, "pools_default.json")
	} else {
		fileName = fmt.Sprintf("%v%v", testInternalDataDir, "pools_default.json")
	}
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

func getClusterInfoMockK8() (map[string]interface{}, error) {
	fileName := fmt.Sprintf("%v%v", testK8DataDir, "pools_default.json")
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

func getNodeListWithMinInfoMock(external bool) ([]interface{}, error) {
	clusterInfo, err := getClusterInfoMock(external)
	if err != nil {
		return nil, err
	}
	return testUtils.GetNodeListFromInfoMap(clusterInfo, logger)
}

func getNodeListWithMinInfoMockK8() ([]interface{}, error) {
	clusterInfo, err := getClusterInfoMockK8()
	if err != nil {
		return nil, err
	}
	return testUtils.GetNodeListFromInfoMap(clusterInfo, logger)
}

func getBucketInfoWithReplicas() map[string]interface{} {
	fileName := fmt.Sprintf("%v%v", testInternalDataDir, "pools_default_buckets_4nodes_2replicas.json")
	retMap, _, err := readJsonHelper(fileName)
	if err != nil {
		panic(err)
	}
	return retMap
}

func getBucketInfoWithReplicasWithNoReplicas() map[string]interface{} {
	fileName := fmt.Sprintf("%v%v", testInternalDataDir, "pools_default_buckets_preRebalance.json")
	retMap, _, err := readJsonHelper(fileName)
	if err != nil {
		panic(err)
	}
	return retMap
}

func getBucketInfoWithReplicasAfterRebalance() map[string]interface{} {
	fileName := fmt.Sprintf("%v%v", testInternalDataDir, "pools_default_buckets_postRebalance.json")
	retMap, _, err := readJsonHelper(fileName)
	if err != nil {
		panic(err)
	}
	return retMap
}

func getBucketInfoWithManifestHexId() map[string]interface{} {
	fileName := fmt.Sprintf("%v%v", testInternalDataDir, "bucketInfoForCollection.json")
	retMap, _, err := readJsonHelper(fileName)
	if err != nil {
		panic(err)
	}
	return retMap
}

func getKvSSLFromBPath() map[string]interface{} {
	fileName := fmt.Sprintf("%v%v%v", testExternalDataDir, kvSSLDir, "pools_default_b_ssl.json")
	retMap, _, err := readJsonHelper(fileName)
	if err != nil {
		panic(err)
	}
	return retMap
}

func getNlbDefaultPool() map[string]interface{} {
	fileName := fmt.Sprintf("%v%v", testExternalDataDir, "pools_default_nlb.json")
	retMap, _, err := readJsonHelper(fileName)
	if err != nil {
		panic(err)
	}
	return retMap
}

type localNodeList []string

func (list localNodeList) check() bool {
	for _, node := range list {
		for _, externalAddress := range externalAddresses {
			if strings.Contains(node, externalAddress) {
				return false
			}
		}
	}
	return true
}

type externalNodeList []string

func (list externalNodeList) check() bool {
	for _, node := range list {
		for _, localAddress := range localAddresses {
			if strings.Contains(node, localAddress) {
				return false
			}
		}
	}
	return true
}

func getBucketInfoMock(external bool) (map[string]interface{}, error) {
	var fileName string
	var defaultPoolBucketsFile = "pools_default_buckets_b2.json"
	if external {
		fileName = fmt.Sprintf("%v%v", testExternalDataDir, defaultPoolBucketsFile)
	} else {
		fileName = fmt.Sprintf("%v%v", testInternalDataDir, defaultPoolBucketsFile)
	}
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

// Only has external
func getBucketInfoMockK8() (map[string]interface{}, error) {
	var fileName string
	var defaultPoolBucketsFile = "pools_default_buckets_default.json"
	fileName = fmt.Sprintf("%v%v", testK8DataDir, defaultPoolBucketsFile)
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

func getBucketDetailedInfoMock(external bool) (map[string]interface{}, error) {
	var fileName string
	var poolsBBFile = "pools_default_b_b2.json"
	if external {
		fileName = fmt.Sprintf("%v%v", testExternalDataDir, poolsBBFile)
	} else {
		fileName = fmt.Sprintf("%v%v", testInternalDataDir, poolsBBFile)
	}
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

func getBucketDetailedInfoMockK8() (map[string]interface{}, error) {
	var poolsBBFile = "pools_default_b_default.json"
	fileName := fmt.Sprintf("%v%v", testK8DataDir, poolsBBFile)
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

func getRemoteSSLPortMock(active bool) (map[string]interface{}, error) {
	var fileName string
	var activeFile = "nodes_self_xdcrSSLPorts2.json"
	var idleFile = "nodes_self_xdcrSSLPorts.json"
	if active {
		fileName = fmt.Sprintf("%v%v", testExternalDataDir, activeFile)
	} else {
		fileName = fmt.Sprintf("%v%v", testExternalDataDir, idleFile)
	}
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

func getPoolsDefaultBucketMock(external bool) (map[string]interface{}, error) {
	var fileName string
	var defaultPoolsBucketFile = "pools_default_buckets_b2.json"
	if external {
		fileName = fmt.Sprintf("%v%v", testExternalDataDir, defaultPoolsBucketFile)
	} else {
		fileName = fmt.Sprintf("%v%v", testInternalDataDir, defaultPoolsBucketFile)
	}
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

func getPoolsNodesMock(external bool) (map[string]interface{}, error) {
	var fileName string
	var poolsNodesFile = "pools_nodes.json"
	if external {
		fileName = fmt.Sprintf("%v%v", testExternalDataDir, poolsNodesFile)
	} else {
		fileName = fmt.Sprintf("%v%v", testInternalDataDir, poolsNodesFile)
	}
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err

}

func getTargetVBucketMockAlt() (map[string]interface{}, []byte, error) {
	fileName := fmt.Sprintf("%v%v", testExternalDataDir, "targetBucketInfo_alt.json")
	return readJsonHelper(fileName)
}

func getCloudTargetBucketInfo() (map[string]interface{}, []byte, error) {
	fileName := fmt.Sprintf("%v%v", testExternalDataDir, "cloudBucketInfo.json")
	return readJsonHelper(fileName)
}

func getCloudNodeList() ([]interface{}, error) {
	fileName := fmt.Sprintf("%v%v", testExternalDataDir, "cloudNodeList.json")
	byteSlice, err := ioutil.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	var unmarshalledIface interface{}
	err = json.Unmarshal(byteSlice, &unmarshalledIface)

	return unmarshalledIface.([]interface{}), nil
}

func getInitCloudNodeList() ([]interface{}, error) {
	fileName := fmt.Sprintf("%v%v", testExternalDataDir, "initCloudData.json")
	byteSlice, err := ioutil.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	var unmarshalledIface interface{}
	err = json.Unmarshal(byteSlice, &unmarshalledIface)

	return unmarshalledIface.([]interface{}), nil
}

func getDefaultPoolMixedCluster() (map[string]interface{}, error) {
	var fileName string
	var poolsNodesFile = "pools_default_incl_nonkv_node.json"
	fileName = fmt.Sprintf("%v%v", testInternalDataDir, poolsNodesFile)
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err

}

func TestGetNodeListWithMinInfo(t *testing.T) {
	fmt.Println("============== Test case start: TestGetNodeListWithMinInfo =================")
	assert := assert.New(t)
	_, err := getNodeListWithMinInfoMock(true /*external*/)
	assert.Nil(err)
	fmt.Println("============== Test case start: TestGetNodeListWithMinInfo =================")
}

func TestGetNodeNameListFromNodeListExternal(t *testing.T) {
	fmt.Println("============== Test case start: TestGetNodeAddressesListFromNodeListExternal =================")
	assert := assert.New(t)

	nodeList, _ := getNodeListWithMinInfoMock(true /*external*/)
	nodeAddressesList, err := testUtils.GetRemoteNodeAddressesListFromNodeList(nodeList, connStr, false, logger, true)
	assert.Nil(err)

	nodeNameList := nodeAddressesList.GetListOfFirstString()

	// This should be external nodes only
	assert.False((localNodeList)(nodeNameList).check())
	assert.True((externalNodeList)(nodeNameList).check())

	fmt.Println("============== Test case start: TestGetNodeAddressesListFromNodeListExternal =================")
}

func TestGetNodeNameListFromNodeListExternalK8(t *testing.T) {
	assert := assert.New(t)

	nodeList, _ := getNodeListWithMinInfoMockK8()
	nodeAddressList, err := testUtils.GetRemoteNodeAddressesListFromNodeList(nodeList, connStr, false, logger, true)
	assert.Nil(err)
	nodeNameList := nodeAddressList.GetListOfFirstString()

	// This should be external nodes only
	assert.False((localNodeList)(nodeNameList).check())
	assert.True((externalNodeList)(nodeNameList).check())

}

func TestGetNodeNameListFromNodeListInternal(t *testing.T) {
	fmt.Println("============== Test case start: TestGetNodeAddressesListFromNodeListInternal =================")
	assert := assert.New(t)

	nodeList, err := getNodeListWithMinInfoMock(false /*external*/)
	assert.Nil(err)
	nodeAddressesList, err := testUtils.GetRemoteNodeAddressesListFromNodeList(nodeList, connStr, false, logger, false)
	assert.Nil(err)

	nodeNameList := nodeAddressesList.GetListOfFirstString()

	// This should be internal nodes only
	assert.True((localNodeList)(nodeNameList).check())
	assert.False((externalNodeList)(nodeNameList).check())

	fmt.Println("============== Test case start: TestGetNodeAddressesListFromNodeListInternal =================")
}

func TestGetIntExtHostNameTranslationMap(t *testing.T) {
	fmt.Println("============== Test case start: TestGetIntExtHostNameTranslationMap =================")
	assert := assert.New(t)

	bucketInfoMap, err := getBucketInfoMock(true /*external*/)
	assert.Nil(err)

	translatedMap, err := testUtils.GetIntExtHostNameKVPortTranslationMap(bucketInfoMap)

	assert.Nil(err)
	assert.NotEqual(0, len(translatedMap))

	for internal, external := range translatedMap {
		if base.GetHostName(external) == externalAddresses[1] {
			// For 11.11.11.11 test case, use the internal host's port number
			internalPort, portErr := base.GetPortNumber(internal)
			assert.Nil(portErr)
			externalPort, portErr := base.GetPortNumber(external)
			assert.Equal(externalPort, internalPort)
		} else {
			// ports should be different
			internalPort, portErr := base.GetPortNumber(internal)
			assert.Nil(portErr)
			externalPort, portErr := base.GetPortNumber(external)
			assert.NotEqual(externalPort, internalPort)
		}
	}

	fmt.Println("============== Test case start: TestGetIntExtHostNameTranslationMap =================")
}

func TestGetIntExtHostNameTranslationMapK8(t *testing.T) {
	assert := assert.New(t)

	bucketInfoMap, err := getBucketInfoMockK8()
	assert.Nil(err)

	translatedMap, err := testUtils.GetIntExtHostNameKVPortTranslationMap(bucketInfoMap)

	assert.Nil(err)
	assert.NotEqual(0, len(translatedMap))

	for internal, external := range translatedMap {
		if base.GetHostName(external) == externalAddresses[1] || base.GetHostName(external) == externalAddresses[3] ||
			base.GetHostName(external) == externalAddresses[4] || base.GetHostName(external) == externalAddresses[5] {
			// For these test case, use the internal host's port number
			internalPort, portErr := base.GetPortNumber(internal)
			assert.Nil(portErr)
			externalPort, portErr := base.GetPortNumber(external)
			assert.Equal(externalPort, internalPort)
		} else {
			// ports should be different
			internalPort, portErr := base.GetPortNumber(internal)
			assert.Nil(portErr)
			externalPort, portErr := base.GetPortNumber(external)
			assert.NotEqual(externalPort, internalPort)
		}
	}
}

func TestGetIntExtHostNameTranslationMapInternal(t *testing.T) {
	fmt.Println("============== Test case start: TestGetIntExtHostNameTranslationMapInternal =================")
	assert := assert.New(t)

	bucketInfoMap, err := getBucketInfoMock(false /*external*/)
	assert.Nil(err)

	translatedMap, err := testUtils.GetIntExtHostNameKVPortTranslationMap(bucketInfoMap)

	assert.Equal(base.ErrorResourceDoesNotExist, err)
	assert.Equal(0, len(translatedMap))

	fmt.Println("============== Test case start: TestGetIntExtHostNameTranslationMapInternal =================")
}

func TestReplaceKVVBMapExternal(t *testing.T) {
	fmt.Println("============== Test case start: TestReplaceKVVBMapExternal =================")
	assert := assert.New(t)

	bucketInfoMap, err := getBucketInfoMock(true /*external*/)
	assert.Nil(err)

	translatedMap, err := testUtils.GetIntExtHostNameKVPortTranslationMap(bucketInfoMap)
	fmt.Printf("TranslatedMap: %v\n", translatedMap)

	assert.Nil(err)
	assert.NotEqual(0, len(translatedMap))

	kvVbMapPtr, err := testUtils.GetServerVBucketsMap("dummyConnStr", "b2", bucketInfoMap, nil, nil)
	assert.Nil(err)
	kvVbMap := *kvVbMapPtr

	// Before translate, should be internal only
	var nodeNameList []string
	for kv, _ := range kvVbMap {
		nodeNameList = append(nodeNameList, kv)
	}
	assert.True((localNodeList)(nodeNameList).check())
	assert.False((externalNodeList)(nodeNameList).check())

	// Translate, should be external only
	((base.BucketKVVbMap)(kvVbMap)).ReplaceInternalWithExternalHosts(translatedMap)

	nodeNameList = make([]string, len(nodeNameList), len(nodeNameList))
	for kv, _ := range kvVbMap {
		nodeNameList = append(nodeNameList, kv)
	}

	assert.False((localNodeList)(nodeNameList).check())
	assert.True((externalNodeList)(nodeNameList).check())

	fmt.Println("============== Test case start: TestReplaceKVVBMapExternal =================")
}

func TestReplaceKVVBMapExternalK8(t *testing.T) {
	assert := assert.New(t)

	bucketInfoMap, err := getBucketInfoMockK8()
	assert.Nil(err)

	translatedMap, err := testUtils.GetIntExtHostNameKVPortTranslationMap(bucketInfoMap)
	fmt.Printf("TranslatedMap: %v\n", translatedMap)

	assert.Nil(err)
	assert.NotEqual(0, len(translatedMap))

	kvVbMapPtr, err := testUtils.GetServerVBucketsMap("dummyConnStr", "b2", bucketInfoMap, nil, nil)
	assert.Nil(err)
	kvVbMap := *kvVbMapPtr

	// Before translate, should be internal only
	var nodeNameList []string
	for kv, _ := range kvVbMap {
		nodeNameList = append(nodeNameList, kv)
	}
	assert.True((localNodeList)(nodeNameList).check())
	assert.False((externalNodeList)(nodeNameList).check())

	// Translate, should be external only
	((base.BucketKVVbMap)(kvVbMap)).ReplaceInternalWithExternalHosts(translatedMap)

	nodeNameList = make([]string, len(nodeNameList), len(nodeNameList))
	for kv, _ := range kvVbMap {
		nodeNameList = append(nodeNameList, kv)
	}

	assert.False((localNodeList)(nodeNameList).check())
	assert.True((externalNodeList)(nodeNameList).check())
}

func TestGetMemcachedSSLPortMapExternal(t *testing.T) {
	fmt.Println("============== Test case start: TestGetMemcachedSSLPortMapExternal =================")
	assert := assert.New(t)

	bbucketInfo, err := getBucketDetailedInfoMock(true /*external*/)
	assert.Nil(err)

	nodesExt, ok := bbucketInfo[base.NodeExtKey]
	assert.True(ok)

	nodesExtArray, ok := nodesExt.([]interface{})
	assert.True(ok)

	for _, nodeExt := range nodesExtArray {
		nodeExtMap, ok := nodeExt.(map[string]interface{})
		assert.True(ok)

		externalHostAddr, _, _, externalSSLPort, externalSSLPortErr := testUtils.GetExternalAddressAndKvPortsFromNodeInfo(nodeExtMap)
		if externalHostAddr == externalAddresses[1] {
			// 11.11.11.11 has no SSL port
			assert.NotNil(externalSSLPortErr)
		} else if externalHostAddr == externalAddresses[2] {
			assert.Equal(3301, externalSSLPort)
			assert.Nil(externalSSLPortErr)
		}
	}
	fmt.Println("============== Test case start: TestGetMemcachedSSLPortMapExternal =================")
}

func TestGetMemcachedSSLPortMapExternalK8(t *testing.T) {
	assert := assert.New(t)

	bucketInfo, err := getBucketDetailedInfoMockK8()
	assert.Nil(err)

	nodesExt, ok := bucketInfo[base.NodeExtKey]
	assert.True(ok)

	nodesExtArray, ok := nodesExt.([]interface{})
	assert.True(ok)

	for _, nodeExt := range nodesExtArray {
		nodeExtMap, ok := nodeExt.(map[string]interface{})
		assert.True(ok)

		//		externalHostAddr, _, _, externalSSLPort, externalSSLPortErr := testUtils.GetExternalAddressAndKvPortsFromNodeInfo(nodeExtMap)
		externalHostAddr, _, _, _, externalSSLPortErr := testUtils.GetExternalAddressAndKvPortsFromNodeInfo(nodeExtMap)
		// all has no SSL port
		assert.NotNil(externalSSLPortErr)
		// Hostaddr should be returned
		assert.NotEqual("", externalHostAddr)
	}
}

func TestGetSSLMgmtPortActive(t *testing.T) {
	fmt.Println("============== Test case start: TestGetSSLMgmtPortActive =================")
	assert := assert.New(t)

	sslPortInfo, err := getRemoteSSLPortMock(true /*active*/)
	assert.Nil(err)

	portNumber, err := testUtils.getExternalSSLMgtPort(sslPortInfo)
	assert.Nil(err)
	assert.NotEqual(-1, portNumber)

	fmt.Println("============== Test case start: TestGetSSLMgmtPortActive =================")
}

func TestGetSSLMgmtPortInActive(t *testing.T) {
	fmt.Println("============== Test case start: TestGetSSLMgmtPortInActive =================")
	assert := assert.New(t)

	sslPortInfo, err := getRemoteSSLPortMock(false /*active*/)
	assert.Nil(err)

	portNumber, err := testUtils.getExternalSSLMgtPort(sslPortInfo)
	assert.NotNil(err)
	assert.Equal(-1, portNumber)
	fmt.Println("============== Test case start: TestGetSSLMgmtPortInActive =================")
}

func TestGetMgmtPortActive(t *testing.T) {
	fmt.Println("============== Test case start: TestGetMgmtPortActive =================")
	assert := assert.New(t)

	nodesList, err := getNodeListWithMinInfoMock(true /*external*/)
	assert.Nil(err)

	for _, nodeList := range nodesList {
		hostAddr, hostPort, err := testUtils.GetExternalMgtHostAndPort(nodeList.(map[string]interface{}), false /*isHttps*/)
		assert.Nil(err)
		assert.NotEqual(-1, hostPort)
		assert.True(len(hostAddr) > 0)
	}
	fmt.Println("============== Test case end: TestGetMgmtPortActive =================")
}

func TestGetMgmtPortInActive(t *testing.T) {
	fmt.Println("============== Test case start: TestGetMgmtPortInActive =================")
	assert := assert.New(t)

	nodesList, err := getNodeListWithMinInfoMock(false /*external*/)
	assert.Nil(err)

	for _, nodeList := range nodesList {
		hostAddr, hostPort, err := testUtils.GetExternalMgtHostAndPort(nodeList.(map[string]interface{}), false /*isHttps*/)
		assert.NotNil(err)
		assert.Equal(-1, hostPort)
		assert.True(len(hostAddr) == 0)
	}
	fmt.Println("============== Test case end: TestGetMgmtPortInActive =================")
}

func TestGetCapiPortsExternal(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestGetCapiPortsExternal =================")
	clusterInfo, err := getBucketInfoMock(true)
	assert.Nil(err)

	nodesList, nodesInfoOk := clusterInfo[base.NodesKey]
	assert.True(nodesInfoOk)

	for _, nodeList := range nodesList.([]interface{}) {
		host, capi, capiErr, capiSSL, capiSSLErr := testUtils.getExternalHostAndCapiPorts(nodeList.(map[string]interface{}))
		assert.NotEqual(0, len(host))
		assert.Nil(capiErr)
		assert.NotEqual(-1, capi)
		assert.Nil(capiSSLErr)
		assert.NotEqual(-1, capiSSL)

		testUrlString := "https://127.0.0.1:19502/b2%2B746a570d364cf609ac11572f8c8c2608"
		replaceString := testUtils.ReplaceCouchApiBaseObjWithExternals(testUrlString, nodeList.(map[string]interface{}))

		assert.False(strings.Contains(replaceString, "127.0.0.1"))
		assert.False(strings.Contains(replaceString, fmt.Sprintf("%v", capi)))
		assert.True(strings.Contains(replaceString, fmt.Sprintf("%v", capiSSL)))
	}

	for _, nodeList := range nodesList.([]interface{}) {
		host, capi, capiErr, capiSSL, capiSSLErr := testUtils.getExternalHostAndCapiPorts(nodeList.(map[string]interface{}))
		assert.NotEqual(0, len(host))
		assert.Nil(capiErr)
		assert.NotEqual(-1, capi)
		assert.Nil(capiSSLErr)
		assert.NotEqual(-1, capiSSL)

		testUrlString := "http://127.0.0.1:19502/b2%2B746a570d364cf609ac11572f8c8c2608"
		replaceString := testUtils.ReplaceCouchApiBaseObjWithExternals(testUrlString, nodeList.(map[string]interface{}))

		assert.False(strings.Contains(replaceString, "127.0.0.1"))
		assert.True(strings.Contains(replaceString, fmt.Sprintf("%v", capi)))
		assert.False(strings.Contains(replaceString, fmt.Sprintf("%v", capiSSL)))
	}

	fmt.Println("============== Test case end: TestGetCapiPortsExternal =================")
}

func TestGetCapiPortsInternal(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestGetCapiPortsInternal =================")
	clusterInfo, err := getBucketInfoMock(false /*external*/)
	assert.Nil(err)

	nodesList, nodesInfoOk := clusterInfo[base.NodesKey]
	assert.True(nodesInfoOk)

	for _, nodeList := range nodesList.([]interface{}) {
		host, capi, capiErr, capiSSL, capiSSLErr := testUtils.getExternalHostAndCapiPorts(nodeList.(map[string]interface{}))
		assert.Equal(0, len(host))
		assert.NotNil(capiErr)
		assert.Equal(-1, capi)
		assert.NotNil(capiSSLErr)
		assert.Equal(-1, capiSSL)

		testUrlString := "https://127.0.0.1:19502/b2%2B746a570d364cf609ac11572f8c8c2608"
		replaceString := testUtils.ReplaceCouchApiBaseObjWithExternals(testUrlString, nodeList.(map[string]interface{}))

		assert.Equal(testUrlString, replaceString)
	}

	fmt.Println("============== Test case end: TestGetCapiPortsInternal =================")
}

func TestTagAndGetUDBytes(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestTagAndGetUDBytes =================")
	testBytes := make([]byte, 5)
	rand.Read(testBytes)

	assert.False(bytes.HasPrefix(testBytes, base.UdTagBeginBytes))
	assert.False(bytes.HasSuffix(testBytes, base.UdTagEndBytes))
	assert.False(base.IsByteSliceRedacted(testBytes))

	testBytes = base.TagUDBytes(testBytes)

	assert.True(bytes.HasPrefix(testBytes, base.UdTagBeginBytes))
	assert.True(bytes.HasSuffix(testBytes, base.UdTagEndBytes))
	assert.True(base.IsByteSliceRedacted(testBytes))

	fmt.Println("============== Test case end: TestTagAndGetUDBytes =================")
}

func TestProcessNsServerDoc(t *testing.T) {
	fmt.Println("============== Test case start: TestProcessNsServerDoc =================")
	var docKey string = "TestDocKey"
	var testExpression string = fmt.Sprintf("META().xattrs.AnotherXattr = \"TestValueString\" AND META().xattrs.TestXattr = 30 AND META().id = \"%v\" AND REGEXP_CONTAINS(Key, \"^AA\")", docKey)
	assert := assert.New(t)
	fileName := "./testFilteringData/UIFilteringSampleDoc.json"
	data, err := ioutil.ReadFile(fileName)
	assert.Nil(err)

	docKVs := make(map[string]interface{})
	err = json.Unmarshal(data, &docKVs)
	assert.Nil(err)

	matcher, err := base.ValidateAndGetAdvFilter(testExpression)
	assert.Nil(err)

	matched, err := testUtils.processNsServerDocForFiltering(matcher, docKVs, docKey)
	assert.True(matched)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestProcessNsServerDoc =================")
}

// Tests when the doc coming back does not have a body
func TestProcessNsServerNilDoc(t *testing.T) {
	fmt.Println("============== Test case start: TestProcessNsServerDocNeg =================")
	var docKey string = "TestDocKey"
	var testExpression string = fmt.Sprintf("META().xattrs.AnotherXattr = \"TestValueString\" AND META().xattrs.TestXattr = 30 AND META().id = \"%v\"", docKey)
	assert := assert.New(t)
	fileName := "./testFilteringData/UIFilteringSampleDocNeg.json"
	data, err := ioutil.ReadFile(fileName)
	assert.Nil(err)

	docKVs := make(map[string]interface{})
	err = json.Unmarshal(data, &docKVs)
	assert.Nil(err)

	matcher, err := base.ValidateAndGetAdvFilter(testExpression)
	assert.Nil(err)

	matched, err := testUtils.processNsServerDocForFiltering(matcher, docKVs, docKey)
	assert.True(matched)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestProcessNsServerDocNeg =================")
}

func TestGetServerVBucketsMapAfterReplacingRefNode(t *testing.T) {
	fmt.Println("============== Test case start: TestGetServerVBucketsMapAfterReplacingRefNode =================")
	assert := assert.New(t)

	targetBucketInfo, _, err := getTargetVBucketMockAlt()

	assert.Nil(err)
	assert.NotNil(targetBucketInfo)

	serverName := "10.100.174.24:9001"

	kvVBMap, err := testUtils.GetRemoteServerVBucketsMap(serverName, "B2", targetBucketInfo, true /*useExternal*/)
	assert.Nil(err)
	assert.NotEqual(0, len(kvVBMap))

	fmt.Println("============== Test case end: TestGetServerVBucketsMapAfterReplacingRefNode =================")
}

func TestUtilsCloudBucketInfo(t *testing.T) {
	fmt.Println("============== Test case start: TestCloudBucketInfo =================")
	defer fmt.Println("============== Test case end: TestCloudBucketInfo=================")
	assert := assert.New(t)

	cloudBucketInfo, _, err := getCloudTargetBucketInfo()
	assert.Nil(err)
	assert.NotNil(cloudBucketInfo)

	retPortMap, err := testUtils.getMemcachedSSLPortMapInternal("", cloudBucketInfo, logger, false)
	assert.Nil(err)
	assert.NotNil(retPortMap)

	for _, port := range retPortMap {
		assert.Equal(uint16(11207), port)
	}

	// Try with external
	retPortMap, err = testUtils.getMemcachedSSLPortMapInternal("", cloudBucketInfo, logger, true)
	for _, port := range retPortMap {
		assert.Equal(uint16(11207), port)
	}

}

func TestUtilsCloudNodeInfo(t *testing.T) {
	fmt.Println("============== Test case start: TestCloudNodeInfo =================")
	defer fmt.Println("============== Test case end: TestCloudNodeInfo =================")
	assert := assert.New(t)

	cloudNodeList, _ := getCloudNodeList()

	for _, nodeInfo := range cloudNodeList {
		hostAddr, portNumber, portErr := testUtils.GetExternalMgtHostAndPort(nodeInfo.(map[string]interface{}), true)
		assert.Nil(portErr)
		assert.Equal(18091, portNumber)
		assert.NotEqual("", hostAddr)
	}

	cloudNodeList, _ = getInitCloudNodeList()
	for _, nodeInfo := range cloudNodeList {
		hostAddr, portNumber, portErr := testUtils.GetExternalMgtHostAndPort(nodeInfo.(map[string]interface{}), true)
		assert.Nil(portErr)
		assert.Equal(18091, portNumber)
		assert.NotEqual("", hostAddr)
	}

}

func TestUtilsMixedKVWithNonKVDefaultPoolsInfo(t *testing.T) {
	fmt.Println("============== Test case start: TestUtilsMixedKVWithNonKVDefaultPoolsInfo =================")
	defer fmt.Println("============== Test case end: TestUtilsMixedKVWithNonKVDefaultPoolsInfo =================")
	assert := assert.New(t)

	defaultPool, _ := getDefaultPoolMixedCluster()
	nodeList, err := testUtils.GetNodeListFromInfoMap(defaultPool, nil)
	assert.Nil(err)
	assert.Equal(1, len(nodeList))
}

func TestHeartbeatMap(t *testing.T) {
	fmt.Println("============== Test case start: TestHeartbeatMap =================")
	defer fmt.Println("============== Test case end: TestHeartbeatMap =================")
	assert := assert.New(t)

	nodeList, _ := getNodeListWithMinInfoMockK8()

	heartbeatMap, err := testUtils.GetClusterHeartbeatStatusFromNodeList(nodeList)
	assert.Nil(err)
	assert.Equal(3, len(heartbeatMap))
	for _, status := range heartbeatMap {
		assert.Equal(base.HeartbeatHealthy, status)
	}
}

func TestVBucketMapWithReplicas(t *testing.T) {
	fmt.Println("============== Test case start: TestVBucketMapWithReplicas =================")
	defer fmt.Println("============== Test case end: TestVBucketMapWithReplicas =================")
	assert := assert.New(t)

	bucketInfo := getBucketInfoWithReplicas()
	assert.NotNil(bucketInfo)

	replicaMap, translateMap, numOfReplicas, _, err := testUtils.GetReplicasInfo(bucketInfo, false, nil, nil, nil)
	assert.Nil(err)
	assert.Equal(2, numOfReplicas)

	// validate replica map
	serverVbMap, err := testUtils.GetServerVBucketsMap("", "B1", bucketInfo, nil, nil)
	assert.Nil(err)
	serverKey := "192.168.0.242:12000" // specific to the getBucketInfoWithReplicas() dataset
	vbsForThisNode := (*serverVbMap)[serverKey]

	// Replica map should only contain VBs owned by this node
	assert.Equal(len(vbsForThisNode), len(*replicaMap))
	// Total of 4 nodes
	assert.Len(*translateMap, 4)
}

func TestVBucketMapWithReplicasEarlyInit(t *testing.T) {
	fmt.Println("============== Test case start: TestVBucketMapWithReplicasEarlyInit =================")
	defer fmt.Println("============== Test case end: TestVBucketMapWithReplicasEarlyInit =================")
	assert := assert.New(t)

	bucketInfo := getBucketInfoWithReplicasWithNoReplicas()
	assert.NotNil(bucketInfo)

	replicaMap, translateMap, numOfReplicas, memberOfReplica, err := testUtils.GetReplicasInfo(bucketInfo, false, nil, nil, nil)
	assert.Nil(err)
	assert.Equal(1, numOfReplicas)
	assert.Len(*replicaMap, 0)
	assert.Len(*translateMap, 1)
	assert.Len(memberOfReplica, 0)

	bucketInfo = getBucketInfoWithReplicasAfterRebalance()
	assert.NotNil(bucketInfo)

	replicaMap, translateMap, numOfReplicas, memberOfReplica, err = testUtils.GetReplicasInfo(bucketInfo, false, nil, nil, nil)
	assert.Nil(err)
	assert.Equal(1, numOfReplicas)
	assert.NotEqual(len(*replicaMap), 0)
	assert.Len(*translateMap, 2)
	assert.NotEqual(0, len(memberOfReplica))
}

func TestGetCollectionManifestUidFromBucketInfo(t *testing.T) {
	fmt.Println("============== Test case start: TestVBucketMapWithReplicasEarlyInit =================")
	defer fmt.Println("============== Test case end: TestVBucketMapWithReplicasEarlyInit =================")
	assert := assert.New(t)

	bucketInfoWithManifest := getBucketInfoWithManifestHexId()
	id, err := testUtils.GetCollectionManifestUidFromBucketInfo(bucketInfoWithManifest)
	assert.Nil(err)
	assert.Equal(uint64(10), id)
}

func TestTransactionFilterWithPureArray(t *testing.T) {
	fmt.Println("============== Test case start: TestTransactionFilterWithPureArray =================")
	assert := assert.New(t)

	uprFile := "./testFilteringData/uprArrayOnly.bin"
	uprEvent, err := base.RetrieveUprJsonAndConvert(uprFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	fakedp := base.NewFakeDataPool()
	slicesToBeReleasedBuf := make([][]byte, 0, 2)
	_, _, _, _, err, errDesc, _, _ := testUtils.CheckForNecessarySystemXattrsInUprEvent(uprEvent.UprEvent, fakedp, &slicesToBeReleasedBuf, false)
	assert.Nil(err)
	assert.Equal("", errDesc)
	fmt.Println("============== Test case end: TestTransactionFilterWithPureArray =================")
}

func TestTransactionFilterWithInvalidJSON(t *testing.T) {
	fmt.Println("============== Test case start: TestTransactionFilterWithInvalidJSON =================")
	defer fmt.Println("============== Test case end: TestTransactionFilterWithInvalidJSON =================")
	assert := assert.New(t)

	uprFile := "./testFilteringData/uprArrayOnly.bin"
	uprEvent, err := base.RetrieveUprJsonAndConvert(uprFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	fakedp := base.NewFakeDataPool()
	slicesToBeReleasedBuf := make([][]byte, 0, 2)

	// Append a random char into uprevent's body
	out, err := snappy.Decode(nil, uprEvent.UprEvent.Value)
	assert.Nil(err)
	out = append(out, '-')
	uprEvent.UprEvent.Value = snappy.Encode(nil, out)

	_, _, _, _, err, errDesc, _, _ := testUtils.CheckForNecessarySystemXattrsInUprEvent(uprEvent.UprEvent, fakedp, &slicesToBeReleasedBuf, false)
	assert.NotNil(err)
	assert.True(strings.Contains(errDesc, filter.InvalidJSONMsg))
}

func TestKvSSLPortAlternateAddr(t *testing.T) {
	fmt.Println("============== Test case start: TestKvSSLPortAlternateAddr =================")
	defer fmt.Println("============== Test case end: TestKvSSLPortAlternateAddr =================")
	assert := assert.New(t)

	bucketInfo := getKvSSLFromBPath()
	assert.NotNil(bucketInfo)
	_, err := testUtils.getMemcachedSSLPortMapInternal("", bucketInfo, nil, false)
	assert.Nil(err)

	_, err = testUtils.getMemcachedSSLPortMapInternal("", bucketInfo, nil, true)
	assert.Nil(err)
}

func TestGetHostAddrFromNodeInfo(t *testing.T) {
	fmt.Println("============== Test case start: TestGetHostAddrFromNodeInfo =================")
	defer fmt.Println("============== Test case end: TestGetHostAddrFromNodeInfo =================")
	assert := assert.New(t)

	// Test that we will get hostname from alternateAddresses when
	// - target internal hostname is unresolvable and
	// - source cluster is ip family only

	// Pretend source is IPV4 only
	base.NetTCP = base.TCP4
	// Hostname in the alternateAddresses
	resolvableHostname := "couchbase.com"
	// hostname in the nodeInfo
	unresolvableHostname := "host1.clevy.local:8091" // This is unresolvable
	connStr := "test.com"

	// Test 1: nodeInfo with resolvable external hostname and unresolvable internal hostname
	portMap := map[string]interface{}{base.DirectPortKey: float64(11210), base.SSLPortKey: float64(18091)}
	external := map[string]interface{}{base.HostNameKey: resolvableHostname, // external hostname
		base.MgtPortKey: 8091, base.SSLMgtPortKey: 18091, base.KVPortKey: 11210,
		base.CapiPortKey: 8092, base.CapiSSLPortKey: 18092}
	altAddr := map[string]interface{}{base.ExternalKey: external}
	nodeInfoMap := map[string]interface{}{base.HostNameKey: unresolvableHostname, // internal hostname
		base.PortsKey: portMap, base.AlternateKey: altAddr}
	hostAddr, err := testUtils.GetHostAddrFromNodeInfo(connStr, nodeInfoMap, true, logger, true)
	// It should return external address
	assert.Nil(err)
	assert.Equal(resolvableHostname+":18091", hostAddr)

	// Test 2: nodeInfo with unresolvable external hostname and resolvable internal hostname
	portMap = map[string]interface{}{base.DirectPortKey: float64(11210), base.SSLPortKey: float64(18091)}
	external = map[string]interface{}{base.HostNameKey: unresolvableHostname, // external hostname
		base.MgtPortKey: 8091, base.SSLMgtPortKey: 18091, base.KVPortKey: 11210,
		base.CapiPortKey: 8092, base.CapiSSLPortKey: 18092}
	altAddr = map[string]interface{}{base.ExternalKey: external}
	nodeInfoMap = map[string]interface{}{base.HostNameKey: resolvableHostname, // internal hostname
		base.PortsKey: portMap, base.AlternateKey: altAddr}
	hostAddr, err = testUtils.GetHostAddrFromNodeInfo(connStr, nodeInfoMap, true, logger, true)
	// We will use external name if it is there. Since it is unresolvable, we get error
	assert.NotNil(err)

	// Test 3: nodeInfo with no external hostname and resolvable internal hostname
	nodeInfoMap = map[string]interface{}{base.HostNameKey: resolvableHostname, // internal hostname
		base.PortsKey: portMap}
	hostAddr, err = testUtils.GetHostAddrFromNodeInfo(connStr, nodeInfoMap, true, logger, true)
	// We should return internal hostname
	assert.Nil(err)
	assert.Equal(resolvableHostname+":18091", hostAddr)

	// Test 4: nodeInfo with no external hostname and unresolvable internal hostname
	nodeInfoMap = map[string]interface{}{base.HostNameKey: unresolvableHostname,
		base.PortsKey: portMap}
	hostAddr, err = testUtils.GetHostAddrFromNodeInfo(connStr, nodeInfoMap, true, logger, true)
	// We should return error
	assert.NotNil(err)

	// Test 5: NLB
	nlbRef, err := metadata.NewRemoteClusterReference("testuuid", "C2", "192.168.56.10:15022", "test",
		"test", metadata.HostnameMode_None, true, base.SecureTypeFull, nil, nil, nil, nil)
	assert.Nil(err)
	assert.NotNil(nlbRef)
	defaultPoolsInfo := getNlbDefaultPool()
	nodeList, err := testUtils.GetNodeListFromInfoMap(defaultPoolsInfo, logger)
	assert.Nil(err)
	nodeAddressesList, err := testUtils.GetRemoteNodeAddressesListFromNodeList(nodeList, nlbRef.HostName(), true, logger, true)
	for _, addressPair := range nodeAddressesList {
		assert.False(strings.Contains(addressPair.GetFirstString(), "18091"))
		assert.False(strings.Contains(addressPair.GetSecondString(), "18091"))
	}
}

func TestTargetHasSharedExternalHostname(t *testing.T) {
	fmt.Println("============== Test case start: TestTargetHasSharedExternalHostname =================")
	defer fmt.Println("============== Test case end: TestTargetHasSharedExternalHostname =================")
	assert := assert.New(t)

	// Case 1 : with alternate address setup + privateLinks
	fileName := fmt.Sprintf("%v%v", testExternalDataDir, "privateEndPoints.json")
	bucketInfo, _, err := readJsonHelper(fileName)
	assert.Nil(err)
	exists, err := testUtils.TargetHasSharedExternalHostnameAndMgmtPort(bucketInfo)
	assert.Nil(err)
	assert.Equal(true, exists)

	// Case 2 : no alternate addresses setup
	fileName = fmt.Sprintf("%v%v", testInternalDataDir, "pools_default_buckets_b2.json")
	bucketInfo, _, err = readJsonHelper(fileName)
	assert.Nil(err)
	exists, err = testUtils.TargetHasSharedExternalHostnameAndMgmtPort(bucketInfo)
	assert.Nil(err)
	assert.Equal(false, exists)

	//Case 3 : with alternate address setup and no Private Links
	fileName = fmt.Sprintf("%v%v", testExternalDataDir, "targetBucketInfo_alt.json")
	bucketInfo, _, err = readJsonHelper(fileName)
	assert.Nil(err)
	exists, err = testUtils.TargetHasSharedExternalHostnameAndMgmtPort(bucketInfo)
	assert.Nil(err)
	assert.Equal(false, exists)

	//Case 4 : Private Links setup but alternate address isn't setup on all the nodes
	fileName = fmt.Sprintf("%v%v", testExternalDataDir, "targetBucketInfo_alt.json")
	bucketInfo, _, err = readJsonHelper(fileName)
	assert.Nil(err)
	exists, err = testUtils.TargetHasSharedExternalHostnameAndMgmtPort(bucketInfo)
	assert.Nil(err)
	assert.Equal(false, exists)

	// Case 5: Network Load Balancer with Port Forwarding set up
	fileName = fmt.Sprintf("%v%v", testExternalDataDir, "nlb_pools_default_bucket_b2.json")
	bucketInfo, _, err = readJsonHelper(fileName)
	assert.Nil(err)
	exists, err = testUtils.TargetHasSharedExternalHostnameAndMgmtPort(bucketInfo)
	assert.Nil(err)
	assert.Equal(false, exists)
}

// TestGrpcCallSuccess tests grpcCall with successful RPC execution (nil error)
func TestGrpcCallSuccess(t *testing.T) {
	fmt.Println("============== Test case start: TestGrpcCallSuccess =================")
	defer fmt.Println("============== Test case end: TestGrpcCallSuccess =================")
	assert := assert.New(t)

	// Mock RPC function that succeeds
	mockRPC := func(ctx context.Context, request *internal_xdcr_v1.GetClusterInfoRequest, opts ...grpc.CallOption) (*internal_xdcr_v1.GetClusterInfoResponse, error) {
		return &internal_xdcr_v1.GetClusterInfoResponse{ClusterUuid: "1234567890"}, nil
	}

	response := grpcCall(&base.GrpcRequest[*internal_xdcr_v1.GetClusterInfoRequest]{Context: context.Background(), Request: &internal_xdcr_v1.GetClusterInfoRequest{}}, mockRPC)

	// When error is nil, status.FromError(nil) returns (nil, true), so Status field should be nil
	assert.Nil(response.Status)
	assert.Nil(response.Error)
	assert.Equal("1234567890", response.Response().ClusterUuid)
}

// TestGrpcCallWithGrpcError tests grpcCall with gRPC status error from RPC
func TestGrpcCallWithGrpcError(t *testing.T) {
	fmt.Println("============== Test case start: TestGrpcCallWithGrpcError =================")
	defer fmt.Println("============== Test case end: TestGrpcCallWithGrpcError =================")
	assert := assert.New(t)

	// Mock RPC function that returns a gRPC status error
	grpcErr := status.Error(codes.Unauthenticated, "unauthenticated")
	mockRPC := func(ctx context.Context, request *internal_xdcr_v1.GetClusterInfoRequest, opts ...grpc.CallOption) (*internal_xdcr_v1.GetClusterInfoResponse, error) {
		return nil, grpcErr
	}

	response := grpcCall(&base.GrpcRequest[*internal_xdcr_v1.GetClusterInfoRequest]{Context: context.Background(), Request: &internal_xdcr_v1.GetClusterInfoRequest{}}, mockRPC)

	// Verify response structure exists
	assert.Nil(response.Response())
	assert.NotNil(response.Status)
	assert.Equal(codes.Unauthenticated, response.Code())
	assert.Equal("unauthenticated", response.Message())
	assert.Equal(grpcErr, response.Error)
}

// TestGrpcCallWithRegularError tests grpcCall with regular non-gRPC error from RPC
func TestGrpcCallWithRegularError(t *testing.T) {
	fmt.Println("============== Test case start: TestGrpcCallWithRegularError =================")
	defer fmt.Println("============== Test case end: TestGrpcCallWithRegularError =================")
	assert := assert.New(t)

	// Mock RPC function that returns a regular error (not gRPC status error)
	regularErr := errors.New("regular error occurred")
	mockRPC := func(ctx context.Context, request *internal_xdcr_v1.GetClusterInfoRequest, opts ...grpc.CallOption) (*internal_xdcr_v1.GetClusterInfoResponse, error) {
		return nil, regularErr
	}

	// Call grpcCall
	response := grpcCall(&base.GrpcRequest[*internal_xdcr_v1.GetClusterInfoRequest]{Context: context.Background(), Request: &internal_xdcr_v1.GetClusterInfoRequest{}}, mockRPC)

	// Verify response structure
	assert.Nil(response.Response())
	assert.NotNil(response.Status)
	assert.Equal(codes.Unknown, response.Code())
	assert.Equal("regular error occurred", response.Message())
	assert.Equal(regularErr, response.Error)
}

// TestStatusFromErrorBehavior tests the behavior of status.FromError with different error types
// This test is not related to grpcCall function, but it is useful to understand the behavior of status.FromError
func TestStatusFromErrorBehavior(t *testing.T) {
	fmt.Println("============== Test case start: TestStatusFromErrorBehavior =================")
	defer fmt.Println("============== Test case end: TestStatusFromErrorBehavior =================")
	assert := assert.New(t)

	// Test with nil error (success case)
	st, ok := status.FromError(nil)
	assert.True(ok)
	assert.Nil(st) // status.FromError(nil) returns (nil, true)
	assert.Equal(codes.OK, st.Code())
	assert.Equal("", st.Message())

	// Test with regular error (not gRPC status)
	regularErr := errors.New("regular error")
	st, ok = status.FromError(regularErr)
	assert.False(ok) // Should be false for non-gRPC errors
	assert.NotNil(st)
	assert.Equal(codes.Unknown, st.Code())
	assert.Equal("regular error", st.Message())

	// Test with gRPC status error
	grpcErr := status.Error(codes.NotFound, "resource not found")
	st, ok = status.FromError(grpcErr)
	assert.True(ok) // Should be true for gRPC errors
	assert.NotNil(st)
	assert.Equal(codes.NotFound, st.Code())
	assert.Equal("resource not found", st.Message())

	// Test with permission denied gRPC error
	permissionErr := status.Error(codes.PermissionDenied, "access denied")
	st, ok = status.FromError(permissionErr)
	assert.True(ok)
	assert.NotNil(st)
	assert.Equal(codes.PermissionDenied, st.Code())
	assert.Equal("access denied", st.Message())
}

// MockStreamHandler implements GrpcStreamHandler using testify/mock
type MockStreamHandler struct {
	mock.Mock
}

func (m *MockStreamHandler) OnMessage(msg *internal_xdcr_v1.GetClusterInfoResponse) {
	m.Called(msg)
}

func (m *MockStreamHandler) OnError(err error) {
	m.Called(err)
}

func (m *MockStreamHandler) OnComplete() {
	m.Called()
}

// Simple mock streaming client for testing
type MockStreamClient struct {
	responses []internal_xdcr_v1.GetClusterInfoResponse
	errors    []error
	index     int
	headerErr error
	ctx       context.Context
}

func (m *MockStreamClient) Recv() (*internal_xdcr_v1.GetClusterInfoResponse, error) {
	if m.index < len(m.errors) && m.errors[m.index] != nil {
		err := m.errors[m.index]
		m.index++
		return nil, err
	}
	if m.index >= len(m.responses) {
		return nil, io.EOF
	}
	resp := m.responses[m.index]
	m.index++
	return &resp, nil
}

func (m *MockStreamClient) Header() (grpcMetadata.MD, error) {
	return grpcMetadata.MD{}, m.headerErr
}

func (m *MockStreamClient) Trailer() grpcMetadata.MD {
	return grpcMetadata.MD{}
}

func (m *MockStreamClient) CloseSend() error {
	return nil
}

func (m *MockStreamClient) Context() context.Context {
	return m.ctx
}

func (m *MockStreamClient) RecvMsg(message interface{}) error {
	// This method is required by grpc.ClientStream interface but not used in our tests
	return nil
}

func (m *MockStreamClient) SendMsg(message interface{}) error {
	// This method is required by grpc.ClientStream interface but not used in our tests
	return nil
}

// TestGrpcServerStreamCallSuccess tests successful streaming
func TestGrpcServerStreamCallSuccess(t *testing.T) {
	fmt.Println("============== Test case start: TestGrpcServerStreamCallSuccess =================")
	defer fmt.Println("============== Test case end: TestGrpcServerStreamCallSuccess =================")
	assert := assert.New(t)

	handler := &MockStreamHandler{}

	// Set up mock expectations
	resp1 := &internal_xdcr_v1.GetClusterInfoResponse{ClusterUuid: "uuid1"}
	resp2 := &internal_xdcr_v1.GetClusterInfoResponse{ClusterUuid: "uuid2"}
	resp3 := &internal_xdcr_v1.GetClusterInfoResponse{ClusterUuid: "uuid3"}

	handler.On("OnMessage", resp1).Return().Once()
	handler.On("OnMessage", resp2).Return().Once()
	handler.On("OnMessage", resp3).Return().Once()
	handler.On("OnComplete").Return().Once()

	// Mock stream client
	mockClient := &MockStreamClient{
		responses: []internal_xdcr_v1.GetClusterInfoResponse{*resp1, *resp2, *resp3},
		ctx:       context.Background(),
	}

	// Mock RPC function
	mockRPC := func(ctx context.Context, request *internal_xdcr_v1.GetClusterInfoRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[internal_xdcr_v1.GetClusterInfoResponse], error) {
		return mockClient, nil
	}

	request := &base.GrpcRequest[*internal_xdcr_v1.GetClusterInfoRequest]{
		Context: context.Background(),
		Request: &internal_xdcr_v1.GetClusterInfoRequest{},
	}

	response := grpcServerStreamCall(request, handler, mockRPC)

	// Verify response
	assert.Nil(response.Status)
	assert.Nil(response.Error)

	// Verify all mock expectations were met
	handler.AssertExpectations(t)
}

// TestGrpcServerStreamCallRpcFailure tests RPC failure
func TestGrpcServerStreamCallRpcFailure(t *testing.T) {
	fmt.Println("============== Test case start: TestGrpcServerStreamCallRpcFailure =================")
	defer fmt.Println("============== Test case end: TestGrpcServerStreamCallRpcFailure =================")
	assert := assert.New(t)

	handler := &MockStreamHandler{}

	// Set up mock expectations for error
	rpcErr := status.Error(codes.Unauthenticated, "authentication failed")
	handler.On("OnError", rpcErr).Return().Once()

	// Mock RPC function that fails
	mockRPC := func(ctx context.Context, request *internal_xdcr_v1.GetClusterInfoRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[internal_xdcr_v1.GetClusterInfoResponse], error) {
		return nil, rpcErr
	}

	request := &base.GrpcRequest[*internal_xdcr_v1.GetClusterInfoRequest]{
		Context: context.Background(),
		Request: &internal_xdcr_v1.GetClusterInfoRequest{},
	}

	response := grpcServerStreamCall(request, handler, mockRPC)

	// Verify response
	assert.NotNil(response.Status)
	assert.Equal(codes.Unauthenticated, response.Status.Code())
	assert.Equal("authentication failed", response.Status.Message())
	assert.Equal(rpcErr, response.Error)

	// Verify mock expectations
	handler.AssertExpectations(t)
}

// TestGrpcServerStreamCallHeaderFailure tests header failure
func TestGrpcServerStreamCallHeaderFailure(t *testing.T) {
	fmt.Println("============== Test case start: TestGrpcServerStreamCallHeaderFailure =================")
	defer fmt.Println("============== Test case end: TestGrpcServerStreamCallHeaderFailure =================")
	assert := assert.New(t)

	handler := &MockStreamHandler{}

	// Set up mock expectations for header error
	headerErr := status.Error(codes.Unknown, "xyz")
	handler.On("OnError", headerErr).Return().Once()

	// Mock stream client that fails on header
	mockClient := &MockStreamClient{
		headerErr: headerErr,
		ctx:       context.Background(),
	}

	// Mock RPC function
	mockRPC := func(ctx context.Context, request *internal_xdcr_v1.GetClusterInfoRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[internal_xdcr_v1.GetClusterInfoResponse], error) {
		return mockClient, nil
	}

	request := &base.GrpcRequest[*internal_xdcr_v1.GetClusterInfoRequest]{
		Context: context.Background(),
		Request: &internal_xdcr_v1.GetClusterInfoRequest{},
	}

	response := grpcServerStreamCall(request, handler, mockRPC)

	// Verify response
	assert.NotNil(response.Status)
	assert.Equal(codes.Unknown, response.Status.Code())
	assert.Equal("xyz", response.Status.Message())
	assert.Equal(headerErr, response.Error)

	// Verify mock expectations
	handler.AssertExpectations(t)
}

// TestGrpcServerStreamCallStreamError tests streaming error
func TestGrpcServerStreamCallStreamError(t *testing.T) {
	fmt.Println("============== Test case start: TestGrpcServerStreamCallStreamError =================")
	defer fmt.Println("============== Test case end: TestGrpcServerStreamCallStreamError =================")
	assert := assert.New(t)

	handler := &MockStreamHandler{}

	// Set up mock expectations for one message then error
	resp1 := &internal_xdcr_v1.GetClusterInfoResponse{ClusterUuid: "uuid1"}
	streamErr := status.Error(codes.Internal, "internal server error")

	handler.On("OnMessage", resp1).Return().Once()
	handler.On("OnError", streamErr).Return().Once()

	// Mock stream client that sends one message then fails
	mockClient := &MockStreamClient{
		responses: []internal_xdcr_v1.GetClusterInfoResponse{*resp1},
		errors:    []error{nil, streamErr}, // First message succeeds, second fails
		ctx:       context.Background(),
	}

	// Mock RPC function
	mockRPC := func(ctx context.Context, request *internal_xdcr_v1.GetClusterInfoRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[internal_xdcr_v1.GetClusterInfoResponse], error) {
		return mockClient, nil
	}

	request := &base.GrpcRequest[*internal_xdcr_v1.GetClusterInfoRequest]{
		Context: context.Background(),
		Request: &internal_xdcr_v1.GetClusterInfoRequest{},
	}

	response := grpcServerStreamCall(request, handler, mockRPC)

	// Verify response
	assert.NotNil(response.Status)
	assert.Equal(codes.Internal, response.Status.Code())
	assert.Equal("internal server error", response.Status.Message())
	assert.Equal(streamErr, response.Error)

	// Verify mock expectations
	handler.AssertExpectations(t)
}

// TestGrpcServerStreamCallContextCancellation tests context cancellation (non-user triggered)
func TestGrpcServerStreamCallContextCancellation(t *testing.T) {
	fmt.Println("============== Test case start: TestGrpcServerStreamCallContextCancellation =================")
	defer fmt.Println("============== Test case end: TestGrpcServerStreamCallContextCancellation =================")
	assert := assert.New(t)

	handler := &MockStreamHandler{}

	// Set up mock expectations for one message then cancellation
	resp1 := &internal_xdcr_v1.GetClusterInfoResponse{ClusterUuid: "uuid1"}

	handler.On("OnMessage", resp1).Return().Once()
	handler.On("OnError", context.Canceled).Return().Once()

	// Mock stream client that sends one message then gets canceled
	mockClient := &MockStreamClient{
		responses: []internal_xdcr_v1.GetClusterInfoResponse{*resp1},
		errors:    []error{nil, context.Canceled},
		ctx:       context.Background(), // No user cancellation cause
	}

	// Mock RPC function
	mockRPC := func(ctx context.Context, request *internal_xdcr_v1.GetClusterInfoRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[internal_xdcr_v1.GetClusterInfoResponse], error) {
		return mockClient, nil
	}

	request := &base.GrpcRequest[*internal_xdcr_v1.GetClusterInfoRequest]{
		Context: context.Background(),
		Request: &internal_xdcr_v1.GetClusterInfoRequest{},
	}

	response := grpcServerStreamCall(request, handler, mockRPC)

	// Verify response indicates error (non-user cancellation)
	assert.NotNil(response.Status)
	assert.Equal(codes.Unknown, response.Status.Code())
	assert.Equal("context canceled", response.Status.Message())
	assert.Equal(context.Canceled, response.Error)

	// Verify mock expectations
	handler.AssertExpectations(t)
}

// TestGrpcServerStreamCallContextCancellationUserTriggered tests context cancellation (user triggered)
func TestGrpcServerStreamCallContextCancellationUserTriggered(t *testing.T) {
	fmt.Println("============== Test case start: TestGrpcServerStreamCallContextCancellationUserTriggered =================")
	defer fmt.Println("============== Test case end: TestGrpcServerStreamCallContextCancellationUserTriggered =================")
	assert := assert.New(t)

	handler := &MockStreamHandler{}

	// Set up mock expectations for one message then cancellation
	resp1 := &internal_xdcr_v1.GetClusterInfoResponse{ClusterUuid: "uuid1"}

	handler.On("OnMessage", resp1).Return().Once()
	handler.On("OnComplete").Return().Once()

	ctx, cancelWithCause := context.WithCancelCause(context.Background())
	cancelWithCause(base.ErrorUserInitiatedStreamRpcCancellation)

	// Mock stream client that sends one message then gets canceled
	mockClient := &MockStreamClient{
		responses: []internal_xdcr_v1.GetClusterInfoResponse{*resp1},
		errors:    []error{nil, context.Canceled},
		ctx:       ctx, // User triggered cancellation
	}

	// Mock RPC function
	mockRPC := func(ctx context.Context, request *internal_xdcr_v1.GetClusterInfoRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[internal_xdcr_v1.GetClusterInfoResponse], error) {
		return mockClient, nil
	}

	request := &base.GrpcRequest[*internal_xdcr_v1.GetClusterInfoRequest]{
		Context: context.Background(),
		Request: &internal_xdcr_v1.GetClusterInfoRequest{},
	}

	response := grpcServerStreamCall(request, handler, mockRPC)

	// User triggered cancellation should be marked as successful
	assert.Nil(response.Status)
	assert.Nil(response.Error)

	// Verify mock expectations
	handler.AssertExpectations(t)
}

// TestIsUserTriggeredCancellation tests the helper function
func TestIsUserTriggeredCancellation(t *testing.T) {
	fmt.Println("============== Test case start: TestIsUserTriggeredCancellation =================")
	defer fmt.Println("============== Test case end: TestIsUserTriggeredCancellation =================")
	assert := assert.New(t)

	// Test with nil error
	result := isUserTriggeredCancellation(nil)
	assert.False(result)

	// Test with user-triggered cancellation error
	result = isUserTriggeredCancellation(base.ErrorUserInitiatedStreamRpcCancellation)
	assert.True(result)

	// Test with wrapped user-triggered cancellation error
	wrappedErr := fmt.Errorf("wrapper: %w", base.ErrorUserInitiatedStreamRpcCancellation)
	result = isUserTriggeredCancellation(wrappedErr)
	assert.True(result)

	// Test with other error
	otherErr := errors.New("some other error")
	result = isUserTriggeredCancellation(otherErr)
	assert.False(result)

	// Test with context.Canceled (not user-triggered)
	result = isUserTriggeredCancellation(context.Canceled)
	assert.False(result)
}
