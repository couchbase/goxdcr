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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/base/filter"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
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

func getNlbDefaultPool() map[string]interface{} {
	fileName := fmt.Sprintf("%v%v", testExternalDataDir, "pools_default_nlb.json")
	retMap, _, err := readJsonHelper(fileName)
	if err != nil {
		panic(err)
	}
	return retMap
}

func getGCPPrivateLinksBPath() map[string]interface{} {
	fileName := fmt.Sprintf("%v%v%v", testExternalDataDir, kvSSLDir, "pools_default_b_gcp_private_links.json")
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

func TestGetIntExtHostNameTranslationMapForGCPPrivateLinks(t *testing.T) {
	fmt.Println("============== Test case start: TestGetIntExtHostNameTranslationMapForGCPPrivateLinks =================")
	a := assert.New(t)

	bucketInfoMap := getGCPPrivateLinksBPath()

	translatedMap, err := testUtils.GetIntExtHostNameKVPortTranslationMap(bucketInfoMap)

	a.Equal(err, nil)
	a.Greater(len(translatedMap), 1)

	fmt.Println("============== Test case start: TestGetIntExtHostNameTranslationMap =================")
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

	retPortMap, err := testUtils.getMemcachedSSLPortMapInternal("", cloudBucketInfo, logger, false, false)
	assert.Nil(err)
	assert.NotNil(retPortMap)

	for _, port := range retPortMap {
		assert.Equal(uint16(11207), port)
	}

	// Try with external
	retPortMap, err = testUtils.getMemcachedSSLPortMapInternal("", cloudBucketInfo, logger, true, false)
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

func TestTransactionFilterWithPureArray(t *testing.T) {
	fmt.Println("============== Test case start: TestTransactionFilterWithPureArray =================")
	assert := assert.New(t)

	uprFile := "./testFilteringData/uprArrayOnly.bin"
	uprEvent, err := base.RetrieveUprJsonAndConvert(uprFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	fakedp := base.NewFakeDataPool()
	slicesToBeReleasedBuf := make([][]byte, 0, 2)
	_, _, _, err, errDesc, _, _ := testUtils.CheckForTransactionXattrsInUprEvent(uprEvent.UprEvent, fakedp, &slicesToBeReleasedBuf, false)
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

	_, _, _, err, errDesc, _, _ := testUtils.CheckForTransactionXattrsInUprEvent(uprEvent.UprEvent, fakedp, &slicesToBeReleasedBuf, false)
	assert.NotNil(err)
	assert.True(strings.Contains(errDesc, filter.InvalidJSONMsg))
}

// Local stub utilities for testing bucketValidationInfoInternal
type stubUtilsForBucketValidation struct {
	*Utilities
	mockBucketInfo      map[string]interface{}
	mockTerseBucketInfo map[string]interface{}
}

func (s *stubUtilsForBucketValidation) GetBucketInfo(hostAddr, bucketName, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (map[string]interface{}, error) {
	return s.mockBucketInfo, nil
}

func (s *stubUtilsForBucketValidation) GetTerseBucketInfo(hostAddr, bucketName, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCert []byte, clientKey []byte, logger *log.CommonLogger) (map[string]interface{}, error) {
	return s.mockTerseBucketInfo, nil
}

func TestBucketValidationInfoInternalWithGCPPrivateLinks(t *testing.T) {
	fmt.Println("============== Test case start: TestBucketValidationInfoInternalWithGCPPrivateLinks =================")
	defer fmt.Println("============== Test case end: TestBucketValidationInfoInternalWithGCPPrivateLinks =================")
	assert := assert.New(t)

	// Load the test data files as specified in requirements
	fileName := fmt.Sprintf("%v%v", testExternalDataDir, "pools_default_buckets_gcp_private_links.json")
	bucketInfo, _, err := readJsonHelper(fileName)
	assert.Nil(err)

	fileName = fmt.Sprintf("%v%v%v", testExternalDataDir, kvSSLDir, "pools_default_b_gcp_private_links.json")
	terseBucketInfo, _, err := readJsonHelper(fileName)
	assert.Nil(err)

	// Test parameters as specified in requirements
	hostAddr := "pe.vxzqmnl0syi3kpwn.aws-guardians.nonprod-project-avengers.com:1801"
	bucketName := "B1"
	username := "abc"
	password := "def"
	authMech := base.HttpAuthMechHttps
	certificates := []byte("blah")
	useExternal := true
	logger := log.NewLogger("test", log.DefaultLoggerContext)

	// Create a stub that will return our mock data
	stub := &stubUtilsForBucketValidation{
		Utilities:           testUtils,
		mockBucketInfo:      bucketInfo,
		mockTerseBucketInfo: terseBucketInfo,
	}

	// Manually test the core logic of bucketValidationInfoInternal
	// First call GetBucketInfo (should return our mock)
	bucketInfoResult, err := stub.GetBucketInfo(hostAddr, bucketName, username, password, authMech, certificates, false, nil, nil, logger)
	assert.Nil(err)
	assert.NotNil(bucketInfoResult)

	// Check if we should use terse info
	useTerseInfo, err := testUtils.ShouldUseTerseBucketInfo(bucketInfoResult, "", "", useExternal, authMech == base.HttpAuthMechHttps)
	assert.Nil(err)
	assert.True(useTerseInfo)

	// Call GetTerseBucketInfo (should return our mock)
	bucketInfoResult, err = stub.GetTerseBucketInfo(hostAddr, bucketName, username, password, authMech, certificates, false, nil, nil, logger)
	assert.Nil(err)
	assert.NotNil(bucketInfoResult)

	// Get VB map from the bucket info
	bucketKVVBMapPtr, err := testUtils.GetServerVBucketsMap(hostAddr, bucketName, bucketInfoResult, nil, nil)
	assert.Nil(err)
	bucketKVVBMap := *bucketKVVBMapPtr

	// Translate if using external
	testUtils.TranslateKvVbMap(bucketKVVBMap, bucketInfoResult)

	// Verify results
	assert.Equal(len(bucketKVVBMap), 3)

	// Verify that terse bucket info was used
	isTerse, err := testUtils.IsTerseBucketInfo(bucketInfoResult)
	assert.Equal(err, nil)
	assert.Equal(isTerse, true)

	// Also check for validity of ssl_port_map
	ssl_port_map, err := stub.getMemcachedSSLPortMapInternal(hostAddr, bucketInfoResult, logger, useExternal, true)
	assert.Nil(err)
	assert.Greater(len(ssl_port_map), 0)

	for k := range bucketKVVBMap {
		_, ok := ssl_port_map[k]
		assert.True(ok)
	}
}

func readJsonHelperForTest(fileName string) (retMap map[string]interface{}, err error) {
	byteSlice, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	var unmarshalledIface interface{}
	err = json.Unmarshal(byteSlice, &unmarshalledIface)
	if err != nil {
		return nil, err
	}

	// Check if the unmarshalled data is a map
	if mapVal, ok := unmarshalledIface.(map[string]interface{}); ok {
		retMap = mapVal
		return retMap, nil
	}

	// If not a map, return empty map (will be handled as empty bucketInfo)
	return make(map[string]interface{}), nil
}

func TestShouldUseTerseBucketInfo(t *testing.T) {
	fmt.Println("============== Test case start: TestShouldUseTerseBucketInfo =================")
	defer fmt.Println("============== Test case end: TestShouldUseTerseBucketInfo =================")
	assert := assert.New(t)

	// Test parameters needed to use terseInfo
	useExternal := true
	isHttps := true
	hostAddr := "pe.vxzqmnl0syi3kpwn.aws-guardians.nonprod-project-avengers.com:1801"
	bucketName := "B1"

	// 1. Positive case - pools_default_buckets_gcp_private_links.json (GCP private endpoints),
	// privateEndPoints.json & privateEndPoints1.json (AWS private endpoints)
	positiveFiles := []string{
		fmt.Sprintf("%v%v", testExternalDataDir, "pools_default_buckets_gcp_private_links.json"),
		fmt.Sprintf("%v%v", testExternalDataDir, "privateEndPoints.json"),
		fmt.Sprintf("%v%v", testExternalDataDir, "privateEndPoints1.json"),
	}

	for _, fileName := range positiveFiles {
		bucketInfo, err := readJsonHelperForTest(fileName)
		assert.Nil(err)

		result, err := testUtils.ShouldUseTerseBucketInfo(bucketInfo, hostAddr, bucketName, useExternal, isHttps)
		assert.Nil(err)
		assert.True(result)
	}

	// Read all JSON files from all folders in goxdcr root directory
	var jsonFiles []string
	goxdcrRoot := ".." // Assuming the test is in utils/ subdirectory
	err := filepath.Walk(goxdcrRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".json") {
			jsonFiles = append(jsonFiles, path)
		}
		return nil
	})
	assert.Nil(err)

	// 2. Negative cases - rest all of the valid bucketInfo files in the goxdcr repository.
	for _, fileName := range jsonFiles {
		var skip bool
		for _, positiveFile := range positiveFiles {
			if strings.Contains(fileName, positiveFile) || strings.Contains(fileName, "settings.json") {
				// positive case or env files
				skip = true
				break
			}
		}

		if skip {
			fmt.Printf("Skipping file %s as it is a positive test: %v\n", filepath.Base(fileName), err)
			continue
		}

		bucketInfo, err := readJsonHelperForTest(fileName)
		assert.Nil(err)

		if len(bucketInfo) == 0 {
			fmt.Printf("Skipping file %s as it is not a bucketInfo: %v\n", filepath.Base(fileName), err)
			continue
		}

		_, ok := bucketInfo["bucketType"]
		if !ok {
			fmt.Printf("Skipping file %s as it is not a bucketInfo or terseBucketInfo: %v\n", filepath.Base(fileName), err)
			continue
		}

		isTerse, err := testUtils.IsTerseBucketInfo(bucketInfo)
		assert.Nil(err)

		if isTerse {
			fmt.Printf("Skipping file %s as it terseBucketInfo: %v\n", filepath.Base(fileName), err)
			continue
		}

		result, err := testUtils.ShouldUseTerseBucketInfo(bucketInfo, hostAddr, bucketName, useExternal, isHttps)
		assert.Nil(err)

		assert.False(result)
	}
}

func TestGetServerVBucketsMapWithTerseInfo(t *testing.T) {
	fmt.Println("============== Test case start: TestGetServerVBucketsMapWithTerseInfo =================")
	assert := assert.New(t)

	bucketInfo := getGCPPrivateLinksBPath()
	assert.NotNil(bucketInfo)

	serverName := "pe.vxzqmnl0syi3kpwn.aws-guardians.nonprod-project-avengers.com:1801"
	kvVBMap, err := testUtils.GetRemoteServerVBucketsMap(serverName, "B1", bucketInfo, true /*useExternal*/)
	assert.Nil(err)
	assert.Equal(3, len(kvVBMap))

	fmt.Println("============== Test case end: TestGetServerVBucketsMapWithTerseInfo =================")
}
