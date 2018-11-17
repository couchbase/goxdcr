package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	base "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"strings"
	"testing"
)

// The test data's external node name is 10.10.10.10 instead of the regular 127.0.0.1
var localAddresses = []string{"127.0.0.1", "cb-example-0000.cb-example.default.svc"}

var externalAddresses = []string{"10.10.10.10", "11.11.11.11", "12.12.12.12",
	"cb-example-0000.cb-example.us-west1.spjmurray.co.uk", "cb-example-0001.cb-example.us-west1.spjmurray.co.uk", "cb-example-0002.cb-example.us-west1.spjmurray.co.uk"}

const testExternalDataDir = "testExternalData/"
const testInternalDataDir = "testInternalData/"
const testK8DataDir = "testK8ExternalData/"

var logger = log.NewLogger("testLogger", log.DefaultLoggerContext)
var testUtils = NewUtilities()
var connStr = "testConnStr"

func readJsonHelper(fileName string) (retMap map[string]interface{}, err error) {
	var byteSlice []byte
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
	return readJsonHelper(fileName)
}

func getClusterInfoMockK8() (map[string]interface{}, error) {
	fileName := fmt.Sprintf("%v%v", testK8DataDir, "pools_default.json")
	return readJsonHelper(fileName)
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
	return readJsonHelper(fileName)
}

// Only has external
func getBucketInfoMockK8() (map[string]interface{}, error) {
	var fileName string
	var defaultPoolBucketsFile = "pools_default_buckets_default.json"
	fileName = fmt.Sprintf("%v%v", testK8DataDir, defaultPoolBucketsFile)
	return readJsonHelper(fileName)
}

func getBucketDetailedInfoMock(external bool) (map[string]interface{}, error) {
	var fileName string
	var poolsBBFile = "pools_default_b_b2.json"
	if external {
		fileName = fmt.Sprintf("%v%v", testExternalDataDir, poolsBBFile)
	} else {
		fileName = fmt.Sprintf("%v%v", testInternalDataDir, poolsBBFile)
	}
	return readJsonHelper(fileName)
}

func getBucketDetailedInfoMockK8() (map[string]interface{}, error) {
	var poolsBBFile = "pools_default_b_default.json"
	fileName := fmt.Sprintf("%v%v", testK8DataDir, poolsBBFile)
	return readJsonHelper(fileName)
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
	return readJsonHelper(fileName)
}

func getPoolsDefaultBucketMock(external bool) (map[string]interface{}, error) {
	var fileName string
	var defaultPoolsBucketFile = "pools_default_buckets_b2.json"
	if external {
		fileName = fmt.Sprintf("%v%v", testExternalDataDir, defaultPoolsBucketFile)
	} else {
		fileName = fmt.Sprintf("%v%v", testInternalDataDir, defaultPoolsBucketFile)
	}
	return readJsonHelper(fileName)
}

func getPoolsNodesMock(external bool) (map[string]interface{}, error) {
	var fileName string
	var poolsNodesFile = "pools_nodes.json"
	if external {
		fileName = fmt.Sprintf("%v%v", testExternalDataDir, poolsNodesFile)
	} else {
		fileName = fmt.Sprintf("%v%v", testInternalDataDir, poolsNodesFile)
	}
	return readJsonHelper(fileName)

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
	nodeAddressesList, err := testUtils.GetRemoteNodeAddressesListFromNodeList(nodeList, connStr, false, logger)
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
	nodeAddressList, err := testUtils.GetRemoteNodeAddressesListFromNodeList(nodeList, connStr, false, logger)
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
	nodeAddressesList, err := testUtils.GetRemoteNodeAddressesListFromNodeList(nodeList, connStr, false, logger)
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

	kvVbMap, err := testUtils.GetServerVBucketsMap("dummyConnStr", "b2", bucketInfoMap)
	assert.Nil(err)

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

	kvVbMap, err := testUtils.GetServerVBucketsMap("dummyConnStr", "b2", bucketInfoMap)
	assert.Nil(err)

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

	bbucketInfo, err := getBucketDetailedInfoMockK8()
	assert.Nil(err)

	nodesExt, ok := bbucketInfo[base.NodeExtKey]
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
		hostAddr, hostPort, err := testUtils.getExternalMgtHostAndPort(nodeList.(map[string]interface{}), false /*isHttps*/)
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
		hostAddr, hostPort, err := testUtils.getExternalMgtHostAndPort(nodeList.(map[string]interface{}), false /*isHttps*/)
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
