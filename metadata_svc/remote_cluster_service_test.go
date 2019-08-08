// +build !pcre

package metadata_svc

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	utilsMock "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"io/ioutil"
	"net/http"
	"sync"
	"testing"
	"time"
)

var uuidField string = "dummyUUID"
var uuidField2 string = "dummyUUID2"
var hostname string = "dummyHostName1:9999"
var hostname2 string = "dummyHostName2:9999"
var hostname3 string = "dummyHostName3:9999"
var hostname4 string = "dummyHostName4:9999"
var dummyHostNameList = base.StringPairList{base.StringPair{hostname, ""}, base.StringPair{hostname2, ""}, base.StringPair{hostname3, ""}}
var dummyHostNameList2 = base.StringPairList{base.StringPair{hostname, ""}, base.StringPair{hostname2, ""}, base.StringPair{hostname3, ""}, base.StringPair{hostname4, ""}}
var localhost string = "localhost"
var emptyMap = map[string]interface{}{}
var nonEmptyMap = map[string]interface{}{"test": "test"}

var callBackCount int

// Note this callback here is meant for the metadata change callback, and NOT the metakv callback
func testCallbackIncrementCount(string, interface{}, interface{}) error {
	callBackCount++
	return nil
}

func getMetadataManifestRCS(fileName string) (*metadata.CollectionsManifest, error) {
	var testDir string = "testdata/"
	fullFileName := testDir + fileName
	data, err := ioutil.ReadFile(fullFileName)
	if err != nil {
		return nil, err
	}
	manifest, err := metadata.NewCollectionsManifestFromBytes(data)
	return &manifest, err
}

func setupBoilerPlateRCS() (*service_def.UILogSvc,
	*service_def.MetadataSvc,
	*service_def.XDCRCompTopologySvc,
	*service_def.ClusterInfoSvc,
	*utilsMock.UtilsIface,
	*RemoteClusterService,
	*metadata.CollectionsManifest) {

	uiLogSvcMock := &service_def.UILogSvc{}
	metadataSvcMock := &service_def.MetadataSvc{}
	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	clusterInfoSvcMock := &service_def.ClusterInfoSvc{}
	utilitiesMock := &utilsMock.UtilsIface{}
	utilitiesMock.On("ExponentialBackoffExecutor", "GetAllMetadataFromCatalogRemoteCluster", mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return(nil)

	remoteClusterSvc, _ := NewRemoteClusterService(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		clusterInfoSvcMock, log.DefaultLoggerContext, utilitiesMock)

	callBackCount = 0

	manifest, _ := getMetadataManifestRCS("remoteClusterManifest.json")

	return uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock, utilitiesMock, remoteClusterSvc, manifest
}

func jsonMarshalWrapper(ref *metadata.RemoteClusterReference) (key string, value []byte) {
	jsonKey := ref.Id()
	jsonBytes, _ := ref.Marshal()
	return jsonKey, jsonBytes
}

func setupMocksRCS(uiLogSvcMock *service_def.UILogSvc,
	metadataSvcMock *service_def.MetadataSvc,
	xdcrTopologyMock *service_def.XDCRCompTopologySvc,
	clusterInfoSvcMock *service_def.ClusterInfoSvc,
	utilitiesMock *utilsMock.UtilsIface,
	remoteClusterSvc *RemoteClusterService,
	remoteClusterRef *metadata.RemoteClusterReference,
	simulatedNetworkDelay time.Duration,
	manifest *metadata.CollectionsManifest) {

	// metakv mock
	setupMetaSvcMockGeneric(metadataSvcMock, remoteClusterRef)

	// utils mock
	setupUtilsMockGeneric(utilitiesMock, simulatedNetworkDelay)

	clusterInfo := make(map[string]interface{})
	dummyPools := make([]interface{}, 5, 5)
	clusterInfo[base.IsEnterprise] = true
	clusterInfo[base.Pools] = dummyPools
	clusterInfo[base.RemoteClusterUuid] = uuidField

	utilitiesMock.On("GetClusterInfoWStatusCode", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Run(func(args mock.Arguments) { time.Sleep(simulatedNetworkDelay) }).Return(clusterInfo, nil, http.StatusOK)

	utilitiesMock.On("GetCollectionsManifest", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(manifest, nil)

	// UI svc
	uiLogSvcMock.On("Write", mock.Anything).Return(nil)

}

func setupMetaSvcMockGeneric(metadataSvcMock *service_def.MetadataSvc, remoteClusterRef *metadata.RemoteClusterReference) {
	// Get json marshal and unmarshal for mocking
	jsonKey, jsonMarshalBytes := jsonMarshalWrapper(remoteClusterRef)
	revision := 1

	// metadatasvc mock
	metadataSvcMock.On("AddSensitiveWithCatalog", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	metadataSvcMock.On("Get", jsonKey).Return(jsonMarshalBytes, revision, nil)
	metadataSvcMock.On("DelWithCatalog", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	metadataSvcMock.On("SetSensitive", mock.Anything, mock.Anything, mock.Anything).Return(nil)
}

func setupUtilsMockGeneric(utilitiesMock *utilsMock.UtilsIface, simulatedNetworkDelay time.Duration) {
	emptyList := make([]interface{}, 5)
	hostnameList := append(emptyList, localhost)

	var totalCallsWDelay int64 = 3
	eachDelayNs := simulatedNetworkDelay.Nanoseconds() / totalCallsWDelay
	oneDelay := time.Duration(eachDelayNs) * time.Nanosecond

	utilitiesMock.On("GetNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) { time.Sleep(oneDelay) }).Return(emptyList, nil)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) { time.Sleep(oneDelay) }).Return(dummyHostNameList, nil)
	utilitiesMock.On("GetHostName", mock.Anything).Return(localhost)
	utilitiesMock.On("GetPortNumber", mock.Anything).Return(uint16(9999), nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uuidField, hostnameList, nil)
	utilitiesMock.On("GetClusterInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) { time.Sleep(oneDelay) }).Return(nil, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", mock.Anything, mock.Anything).Return(uuidField, hostnameList, nil)
}

func setupUtilsMockPre1Good3Bad(utilitiesMock *utilsMock.UtilsIface) {
	emptyList := make([]interface{}, 5)
	hostnameList := append(emptyList, localhost)
	utilitiesMock.On("GetNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyList, nil)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(dummyHostNameList2, nil)
	utilitiesMock.On("GetHostName", mock.Anything).Return(localhost)
	utilitiesMock.On("GetPortNumber", mock.Anything).Return(uint16(9999), nil)
	utilitiesMock.On("GetClusterInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uuidField, hostnameList, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", mock.Anything, mock.Anything).Return(uuidField, hostnameList, nil)
}

func setupUtilsOneNode(utilitiesMock *utilsMock.UtilsIface) {
	emptyList := make([]interface{}, 5)
	singleList := base.StringPairList{base.StringPair{hostname, ""}}
	hostnameList := append(emptyList, localhost)
	utilitiesMock.On("GetNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyList, nil)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(singleList, nil)
	utilitiesMock.On("GetHostName", mock.Anything).Return(localhost)
	utilitiesMock.On("GetPortNumber", mock.Anything).Return(uint16(9999), nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uuidField, hostnameList, nil)
	utilitiesMock.On("GetClusterInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", mock.Anything, mock.Anything).Return(uuidField, hostnameList, nil)
}

// emulates where node1 and 2 and 3 becomes cluster splitNames2 and rebalanced
// original cluster is now only consists of splitnames1 (hostname3)
func setupUtilsMock1Good3Bad(utilitiesMock *utilsMock.UtilsIface) base.StringPairList {
	emptyList := make([]interface{}, 5)
	hostnameList := append(emptyList, localhost)
	// the good one
	splitNames1 := base.StringPairList{base.StringPair{hostname4, ""}}
	// the bad ones
	splitNames2 := base.StringPairList{base.StringPair{hostname, ""}, base.StringPair{hostname2, ""}, base.StringPair{hostname3, ""}}
	utilitiesMock.On("GetNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyList, nil)
	utilitiesMock.On("GetHostName", mock.Anything).Return(localhost)
	utilitiesMock.On("GetPortNumber", mock.Anything).Return(uint16(9999), nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", hostname, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uuidField2, hostnameList, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", hostname2, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uuidField2, hostnameList, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", hostname3, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uuidField2, hostnameList, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", hostname4, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uuidField, hostnameList, nil)
	utilitiesMock.On("GetClusterInfo", hostname, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nonEmptyMap, nil)
	utilitiesMock.On("GetClusterInfo", hostname2, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nonEmptyMap, nil)
	utilitiesMock.On("GetClusterInfo", hostname3, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nonEmptyMap, nil)
	utilitiesMock.On("GetClusterInfo", hostname4, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyMap, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", nonEmptyMap, mock.Anything).Return(uuidField2, hostnameList, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", emptyMap, mock.Anything).Return(uuidField, emptyList, nil)

	// diff from generic above
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname, mock.Anything, mock.Anything).Return(splitNames2, nil)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname2, mock.Anything, mock.Anything).Return(splitNames2, nil)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname3, mock.Anything, mock.Anything).Return(splitNames2, nil)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname4, mock.Anything, mock.Anything).Return(splitNames1, nil)
	return splitNames1
}

// emulates where node1 and 2 becomes cluster splitNames2 and rebalanced
// original cluster is now only consists of splitnames1 (hostname3)
func setupUtilsMockListNode2Bad(utilitiesMock *utilsMock.UtilsIface) base.StringPairList {
	emptyList := make([]interface{}, 5)
	hostnameList := append(emptyList, localhost)
	// good one
	splitNames1 := base.StringPairList{base.StringPair{hostname3, ""}}
	// bad ones
	splitNames2 := base.StringPairList{base.StringPair{hostname, ""}, base.StringPair{hostname2, ""}}
	utilitiesMock.On("GetNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyList, nil)
	utilitiesMock.On("GetHostName", mock.Anything).Return(localhost)
	utilitiesMock.On("GetPortNumber", mock.Anything).Return(uint16(9999), nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", hostname3, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uuidField, hostnameList, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", hostname, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uuidField2, hostnameList, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", hostname2, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uuidField2, hostnameList, nil)
	utilitiesMock.On("GetClusterInfo", hostname3, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nonEmptyMap, nil)
	utilitiesMock.On("GetClusterInfo", hostname, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyMap, nil)
	utilitiesMock.On("GetClusterInfo", hostname2, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyMap, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", nonEmptyMap, mock.Anything).Return(uuidField, hostnameList, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", emptyMap, mock.Anything).Return(uuidField2, hostnameList, nil)

	// diff from generic above
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname, mock.Anything, mock.Anything).Return(splitNames2, nil)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname2, mock.Anything, mock.Anything).Return(splitNames2, nil)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname3, mock.Anything, mock.Anything).Return(splitNames1, nil)
	return splitNames1
}

// Emulates where first node is moved out and the rest of the nodes do not respond
func setupUtilsMockFirstNodeBad(utilitiesMock *utilsMock.UtilsIface) {
	emptyList := make([]interface{}, 5)
	var emptyStrList []string
	dummyErr := errors.New("Dummy error")
	hostnameList := append(emptyList, localhost)

	// the bad ones
	splitNames2 := base.StringPairList{base.StringPair{hostname, ""}, base.StringPair{hostname2, ""}, base.StringPair{hostname3, ""}}
	utilitiesMock.On("GetNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyList, nil)
	utilitiesMock.On("GetHostName", mock.Anything).Return(localhost)
	utilitiesMock.On("GetPortNumber", mock.Anything).Return(uint16(9999), nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", hostname, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uuidField2, hostnameList, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", hostname2, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", emptyList, dummyErr)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", hostname3, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", emptyList, dummyErr)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", hostname4, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", emptyList, dummyErr)
	utilitiesMock.On("GetClusterInfo", hostname, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nonEmptyMap, nil)
	utilitiesMock.On("GetClusterInfo", hostname2, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyMap, nil)
	utilitiesMock.On("GetClusterInfo", hostname3, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyMap, nil)
	utilitiesMock.On("GetClusterInfo", hostname4, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyMap, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", nonEmptyMap, mock.Anything).Return(uuidField2, hostnameList, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", emptyMap, mock.Anything).Return("", emptyList, dummyErr)

	// diff from generic above
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname, mock.Anything).Return(splitNames2, nil)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname2, mock.Anything, mock.Anything).Return(emptyStrList, dummyErr)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname3, mock.Anything, mock.Anything).Return(emptyStrList, dummyErr)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname4, mock.Anything, mock.Anything).Return(emptyStrList, dummyErr)
}

func createRemoteClusterReference(id string) *metadata.RemoteClusterReference {
	// Use name the same as ID for ease

	aRef, _ := metadata.NewRemoteClusterReference(uuidField, id, hostname, "", "", false, "", nil, nil, nil)
	aRef.SetId(id)
	return aRef
}

func TestAddDelRemoteClusterRef(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestAddRDelRemoteClusterRef =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, manifest := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, ref, 0 /* networkDelay*/, manifest)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.Equal(1, remoteClusterSvc.getNumberOfAgents())
	assert.NotNil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.True(remoteClusterSvc.agentCacheMapsAreSynced())
	remoteClusterSvc.agentMutex.RLock()
	assert.NotEqual(0, len(remoteClusterSvc.agentMap[idAndName].refNodesList))
	remoteClusterSvc.agentMutex.RUnlock()

	referencesMap, _ := remoteClusterSvc.RemoteClusters()
	assert.Equal(1, len(referencesMap))

	remoteClusterSvc.DelRemoteCluster(idAndName)
	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.True(remoteClusterSvc.agentCacheMapsAreSynced())

	fmt.Println("============== Test case end: TestAddDelRemoteClusterRef =================")
}

// Add cluster via callback
func TestAddSecondaryClusterRef(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestAddSecondaryClusterRef =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, manifest := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	_, jsonMarshalBytes := jsonMarshalWrapper(ref)
	revision := 1

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, ref, 0 /* networkDelay*/, manifest)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.RemoteClusterServiceCallback("", jsonMarshalBytes, revision))
	assert.Equal(1, remoteClusterSvc.getNumberOfAgents())
	assert.NotNil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.True(remoteClusterSvc.agentCacheMapsAreSynced())

	remoteClusterSvc.DelRemoteCluster(idAndName)
	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.True(remoteClusterSvc.agentCacheMapsAreSynced())

	fmt.Println("============== Test case end: TestAddSecondaryClusterRef =================")
}

func TestGetRefFromCachedMapNoRefresh(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestGetRefFromCachedMapNoRefresh =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, manifest := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, ref, 0 /* networkDelay*/, manifest)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.Equal(1, remoteClusterSvc.getNumberOfAgents())
	assert.NotNil(remoteClusterSvc.AddRemoteCluster(ref, true))

	refClone, err := remoteClusterSvc.RemoteClusterByRefId(idAndName, false)
	assert.Nil(err)
	assert.NotNil(refClone)
	assert.True(refClone.IsEssentiallySame(ref))

	refClone, err = remoteClusterSvc.RemoteClusterByRefName(idAndName, false)
	assert.Nil(err)
	assert.NotNil(refClone)
	assert.True(refClone.IsEssentiallySame(ref))

	refClone, err = remoteClusterSvc.RemoteClusterByUuid(uuidField, false)
	assert.Nil(err)
	assert.NotNil(refClone)
	assert.True(refClone.IsEssentiallySame(ref))
	fmt.Println("============== Test case end: TestGetRefFromCachedMapNoRefresh =================")
}

func TestAddThenSetRemoteClusterRef(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestAddThenSetRemoteClusterRef =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, manifest := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)
	ref.SetUserName("oldUserName")

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, ref, 0 /* networkDelay*/, manifest)

	// First set the callback before creating agents
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackIncrementCount)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.Equal(1, callBackCount)

	assert.Equal(1, remoteClusterSvc.getNumberOfAgents())
	assert.NotNil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.True(remoteClusterSvc.agentCacheMapsAreSynced())
	assert.Equal(1, callBackCount)

	// check before update
	agent, exists, _ := remoteClusterSvc.getOrStartNewAgent(ref, false, false)
	assert.Equal("oldUserName", agent.reference.UserName())

	// Update the remoteClusterSvc and agents with a new mock svc to return the actual reference
	newRef := ref.Clone()
	idAndName2 := "newName"
	newRef.SetName(idAndName2)
	newRef.SetUserName("newUserName")
	_, jsonMarshalBytes := jsonMarshalWrapper(newRef)
	revision := 2
	metadataSvcMock2 := &service_def.MetadataSvc{}
	metadataSvcMock2.On("Get", mock.Anything).Return(jsonMarshalBytes, revision, nil)
	metadataSvcMock2.On("SetSensitive", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	metadataSvcMock2.On("DelWithCatalog", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	remoteClusterSvc.metakv_svc = metadataSvcMock2
	remoteClusterSvc.agentMutex.Lock()
	remoteClusterSvc.agentMap[idAndName].metakvSvc = metadataSvcMock2
	remoteClusterSvc.agentMutex.Unlock()

	assert.Nil(remoteClusterSvc.SetRemoteCluster(idAndName, newRef))
	remoteClusterSvc.agentMutex.RLock()
	assert.Nil(remoteClusterSvc.agentCacheRefNameMap[idAndName])
	assert.NotNil(remoteClusterSvc.agentCacheRefNameMap[idAndName2])
	assert.NotEqual(0, len(remoteClusterSvc.agentMap[idAndName].refNodesList))
	remoteClusterSvc.agentMutex.RUnlock()

	// A set should call the callback
	assert.Equal(2, callBackCount)

	assert.True(remoteClusterSvc.agentCacheMapsAreSynced())
	agent, exists, _ = remoteClusterSvc.getOrStartNewAgent(newRef, false, false)
	assert.True(exists)
	assert.Equal("newUserName", agent.reference.UserName())

	remoteClusterSvc.DelRemoteCluster(idAndName2)
	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.True(remoteClusterSvc.agentCacheMapsAreSynced())

	fmt.Println("============== Test case end: TestAddThenSetRemoteClusterRef =================")
}
func TestAddThenSetCallback(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestAddThenSetCallback =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, manifest := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, ref, 0 /* networkDelay*/, manifest)

	// First set the callback before creating agents
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackReturnsNil)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.Equal(1, remoteClusterSvc.getNumberOfAgents())
	assert.NotNil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.True(remoteClusterSvc.agentCacheMapsAreSynced())
	assert.NotEqual(0, len(remoteClusterSvc.agentMap[idAndName].refNodesList))

	remoteClusterSvc.agentMutex.RLock()
	assert.Nil(remoteClusterSvc.agentMap[idAndName].metadataChangeCallback("", nil, nil))
	remoteClusterSvc.agentMutex.RUnlock()

	// After creating agent, update the remoteClusterSvc callback
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackReturnsError)

	// Should point to the second callback
	remoteClusterSvc.agentMutex.RLock()
	assert.NotNil(remoteClusterSvc.agentMap[idAndName].metadataChangeCallback("", nil, nil))
	remoteClusterSvc.agentMutex.RUnlock()

	remoteClusterSvc.DelRemoteCluster(idAndName)
	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.True(remoteClusterSvc.agentCacheMapsAreSynced())

	fmt.Println("============== Test case end: TestAddThenSetCallback =================")
}

func testCallbackReturnsNil(string, interface{}, interface{}) error {
	return nil
}

func testCallbackReturnsError(string, interface{}, interface{}) error {
	return errors.New("Error")
}

func TestAddThenRemoveSecondaryViaCallback(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestAddThenRemoveSecondaryViaCallback =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, manifest := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, ref, 0 /* networkDelay*/, manifest)

	// First set the callback before creating agents
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackReturnsNil)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.Equal(1, remoteClusterSvc.getNumberOfAgents())
	assert.NotNil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.True(remoteClusterSvc.agentCacheMapsAreSynced())
	assert.NotEqual(0, len(remoteClusterSvc.agentMap[idAndName].refNodesList))

	remoteClusterSvc.agentMutex.RLock()
	assert.Nil(remoteClusterSvc.agentMap[idAndName].metadataChangeCallback("", nil, nil))
	remoteClusterSvc.agentMutex.RUnlock()

	// After creating agent, update the remoteClusterSvc callback
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackReturnsError)

	// Should point to the second callback
	remoteClusterSvc.agentMutex.RLock()
	assert.NotNil(remoteClusterSvc.agentMap[idAndName].metadataChangeCallback("", nil, nil))
	remoteClusterSvc.agentMutex.RUnlock()

	remoteClusterSvc.DelRemoteCluster(idAndName)
	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.True(remoteClusterSvc.agentCacheMapsAreSynced())

	fmt.Println("============== Test case end: TestAddThenRemoveSecondaryViaCallback =================")
}

func TestAddThenSetThenDelClusterViaCallback(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestAddThenSetThenDelClusterViaCallback =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, manifest := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)
	ref2 := ref.Clone()

	_, jsonMarshalBytes := jsonMarshalWrapper(ref)
	revision := 1

	// First set the callback before creating agents
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackIncrementCount)

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, ref, 0 /* networkDelay*/, manifest)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.RemoteClusterServiceCallback("/test", jsonMarshalBytes, revision))
	assert.Equal(1, remoteClusterSvc.getNumberOfAgents())
	assert.NotNil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.True(remoteClusterSvc.agentCacheMapsAreSynced())

	// Adding a cluster via metakv callback should call the metakv update
	assert.Equal(1, callBackCount)

	// Set op via callback
	ref2.SetHostName("newHostName")
	ref2.SetUserName("newUserName")
	_, jsonMarshalBytes = jsonMarshalWrapper(ref2)
	revision = 2

	// Update the remoteClusterSvc and agents with a new mock svc to return the actual reference
	metadataSvcMock2 := &service_def.MetadataSvc{}
	metadataSvcMock2.On("Get", mock.Anything).Return(jsonMarshalBytes, revision, nil)
	metadataSvcMock2.On("SetSensitive", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	remoteClusterSvc.metakv_svc = metadataSvcMock2
	remoteClusterSvc.agentMutex.Lock()
	remoteClusterSvc.agentMap[idAndName].metakvSvc = metadataSvcMock2
	remoteClusterSvc.agentMutex.Unlock()

	assert.Nil(remoteClusterSvc.RemoteClusterServiceCallback("/test", jsonMarshalBytes, revision))
	assert.Equal(1, remoteClusterSvc.getNumberOfAgents())
	// A set from metakv should call the callback
	assert.Equal(2, callBackCount)
	remoteClusterSvc.agentMutex.RLock()
	assert.NotNil(remoteClusterSvc.agentMap[idAndName])
	assert.Equal(ref2.HostName(), remoteClusterSvc.agentMap[idAndName].reference.HostName())
	remoteClusterSvc.agentMutex.RUnlock()

	// Delete op via callback
	assert.Nil(remoteClusterSvc.RemoteClusterServiceCallback("/test", nil, nil))
	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.True(remoteClusterSvc.agentCacheMapsAreSynced())
	// A delete via metaCb already should should call the callback
	assert.Equal(3, callBackCount)

	// For completeness sake
	assert.NotNil(remoteClusterSvc.RemoteClusterServiceCallback("/test", nil, nil))

	fmt.Println("============== Test case end: TestAddThenSetThenDelClusterViaCallback =================")
}

func TestGetConnectionStringForRemoteCluster(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestGetConnectionStringForRemoteCluster =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, manifest := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, ref, 0 /* networkDelay*/, manifest)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.Equal(1, remoteClusterSvc.getNumberOfAgents())
	assert.NotNil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.True(remoteClusterSvc.agentCacheMapsAreSynced())
	remoteClusterSvc.agentMutex.RLock()
	assert.NotEqual(0, len(remoteClusterSvc.agentMap[idAndName].refNodesList))
	remoteClusterSvc.agentMutex.RUnlock()

	// Not Capi should return connectionStr
	hostStr, err := remoteClusterSvc.GetConnectionStringForRemoteCluster(ref, false)
	assert.Nil(err)
	actualConnStr, _ := ref.MyConnectionStr()
	assert.Equal(actualConnStr, hostStr)

	// No host in the list - but UpdateReferenceFrom would have pulled one
	hostStr, err = remoteClusterSvc.GetConnectionStringForRemoteCluster(ref, true)
	remoteClusterSvc.agentMutex.RLock()
	remoteClusterSvc.agentMap[idAndName].refMtx.RLock()
	assert.Equal(3, len(remoteClusterSvc.agentMap[idAndName].refNodesList))
	remoteClusterSvc.agentMap[idAndName].refMtx.RUnlock()
	remoteClusterSvc.agentMutex.RUnlock()
	assert.Nil(err)
	assert.Equal(hostname, hostStr)

	// Have a host in the list, should return sorted top one
	remoteClusterSvc.agentMutex.RLock()
	remoteClusterSvc.agentMap[idAndName].refMtx.Lock()
	remoteClusterSvc.agentMap[idAndName].refNodesList = append(remoteClusterSvc.agentMap[idAndName].refNodesList, base.StringPair{"dummyNewHost", ""})
	topHostName := "aaaaaHost"
	remoteClusterSvc.agentMap[idAndName].refNodesList = append(remoteClusterSvc.agentMap[idAndName].refNodesList, base.StringPair{topHostName, ""})
	remoteClusterSvc.agentMap[idAndName].refMtx.Unlock()
	remoteClusterSvc.agentMutex.RUnlock()
	hostStr, _ = remoteClusterSvc.GetConnectionStringForRemoteCluster(ref, true)
	assert.Equal(topHostName, hostStr)

	remoteClusterSvc.DelRemoteCluster(idAndName)
	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.True(remoteClusterSvc.agentCacheMapsAreSynced())

	fmt.Println("============== Test case end: TestGetConnectionStringForRemoteCluster =================")
}

func refreshCheckActiveHostNameHelper(agent *RemoteClusterAgent, nodeList base.StringPairList) bool {
	// find that it should be in here
	var findCheck bool
	for _, pair := range nodeList {
		if agent.reference.ActiveHostName() == pair.GetFirstString() {
			findCheck = true
			break
		}
	}
	return findCheck
}

var rcsSourceBucketName string = "B1"
var rcsSourceBucketUUID string = "B1_uuid"
var rcsTargetClusterUUID string = uuidField
var rcsTargetBucketName string = "B2"
var rcsTargetBucketUUID string = "B2_uuid"

func TestPositiveRefresh(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPositiveRefresh =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, manifest := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	spec, err := metadata.NewReplicationSpecification(rcsSourceBucketName, rcsSourceBucketUUID,
		rcsTargetClusterUUID, rcsTargetBucketName, rcsTargetBucketUUID)
	assert.Nil(err)

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, ref, 0 /* networkDelay*/, manifest)

	// First set the callback before creating agents
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackIncrementCount)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.Equal(1, callBackCount)

	agent, _, _ := remoteClusterSvc.getOrStartNewAgent(ref, false, false)
	agent.Refresh()
	assert.Equal(1, callBackCount) // refresh positive does not call callback
	assert.Equal(hostname, agent.reference.HostName())
	assert.True(refreshCheckActiveHostNameHelper(agent, dummyHostNameList))

	// Test remote cluster bucket book-keeping
	assert.Equal(0, len(agent.bucketManifestGetters))
	err = remoteClusterSvc.RequestRemoteMonitoring(spec)
	assert.Nil(err)
	assert.Equal(1, len(agent.bucketManifestGetters))
	assert.Equal(uint32(1), agent.bucketRefCnt[rcsTargetBucketName])

	// ensure manifest has been updated
	lastUpdated := make(map[string]time.Time)
	for k, _ := range agent.bucketRefCnt {
		lastUpdated[k] = time.Now()
	}
	assert.False(rcsCheckManifestUpdateTime(lastUpdated, agent))
	agent.Refresh()
	assert.True(rcsCheckManifestUpdateTime(lastUpdated, agent))
	manifestChk, err := agent.CollectionManifestGetter(rcsTargetBucketName)
	assert.Nil(err)
	assert.True(manifestChk.Equals(manifest))

	fmt.Println("============== Test case end: TestPositiveRefresh =================")
}

func rcsCheckManifestUpdateTime(lastUpdated map[string]time.Time, agent *RemoteClusterAgent) bool {
	agent.bucketMtx.RLock()
	defer agent.bucketMtx.RUnlock()

	var result bool = true
	for bucketName, manifestGetter := range agent.bucketManifestGetters {
		if manifestGetter.lastQueryTime != lastUpdated[bucketName] {
			if manifestGetter.lastQueryTime.Before(lastUpdated[bucketName]) {
				result = false
			}
		}
	}

	return result
}

func TestRefresh3Nodes2GoesBad(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestRefresh3Nodes2GoesBad =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, manifest := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, ref, 0 /* networkDelay*/, manifest)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.AddRemoteCluster(ref, true))

	// First make sure positive case is good - we have "dummyHostName" as the beginning
	agent, _, _ := remoteClusterSvc.getOrStartNewAgent(ref, false, false)
	agent.Refresh()
	assert.Equal(hostname, agent.reference.HostName())
	assert.True(refreshCheckActiveHostNameHelper(agent, dummyHostNameList))

	// After second refresh, the remoteClusteReference should not have "dummyHostName" as the actual hostname
	ref2 := createRemoteClusterReference(idAndName)
	ref2.SetHostName(hostname3)

	// hostName 1 and 3 have been moved
	// set things up for second refresh
	metadataSvc2 := &service_def.MetadataSvc{}
	setupMetaSvcMockGeneric(metadataSvc2, ref2)

	utilitiesMock2 := &utilsMock.UtilsIface{}
	newNodeList := setupUtilsMockListNode2Bad(utilitiesMock2)

	remoteClusterSvc.updateUtilities(utilitiesMock2)
	remoteClusterSvc.updateMetaSvc(metadataSvc2)

	// Second refresh
	agent.Refresh()
	assert.NotEqual(hostname, agent.reference.HostName())
	assert.True(refreshCheckActiveHostNameHelper(agent, newNodeList))

	fmt.Println("============== Test case end: TestRefresh3Nodes2GoesBad =================")
}

func TestRefresh4Nodes3GoesBad(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestRefresh4Nodes3GoesBad =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, manifest := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, ref, 0 /* networkDelay*/, manifest)

	utilitiesPreMock := &utilsMock.UtilsIface{}
	setupUtilsMockPre1Good3Bad(utilitiesPreMock)
	remoteClusterSvc.updateUtilities(utilitiesPreMock)

	// First set the callback before creating agents
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackIncrementCount)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.AddRemoteCluster(ref, true))

	// 1 callback for adding remote cluster
	assert.Equal(1, callBackCount)

	// First make sure positive case is good - we have "dummyHostName" as the beginning
	agent, _, _ := remoteClusterSvc.getOrStartNewAgent(ref, false, false)
	agent.Refresh()
	assert.Equal(hostname, agent.reference.HostName())
	assert.True(refreshCheckActiveHostNameHelper(agent, dummyHostNameList2))

	// First refresh should not have added to callback count because the reference boostrap node has not been changed
	assert.Equal(1, callBackCount)

	// After second refresh, the remoteClusteReference should not have "dummyHostName" as the actual hostname
	ref2 := createRemoteClusterReference(idAndName)
	ref2.SetHostName(hostname4)

	// hostName 1 and 2 and 3 have been moved
	// set things up for second refresh
	metadataSvc2 := &service_def.MetadataSvc{}
	setupMetaSvcMockGeneric(metadataSvc2, ref2)

	utilitiesMock2 := &utilsMock.UtilsIface{}
	newNodeList := setupUtilsMock1Good3Bad(utilitiesMock2)

	remoteClusterSvc.updateUtilities(utilitiesMock2)
	remoteClusterSvc.updateMetaSvc(metadataSvc2)

	// Second refresh
	agent.Refresh()
	assert.NotEqual(hostname, agent.reference.HostName())
	assert.True(refreshCheckActiveHostNameHelper(agent, newNodeList))

	// This second refresh should have changed the bootstrap node because node 1 was the bootstrap node and it has been declared "bad"
	assert.Equal(2, callBackCount)

	fmt.Println("============== Test case end: TestRefresh4Nodes3GoesBad =================")
}

func TestRefreshFirstNodeIsBad(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestRefreshFirstNodeIsBad =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, manifest := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, ref, 0 /* networkDelay*/, manifest)

	// First make sure positive case is good - we have "dummyHostName" as the beginning
	agent, _, _ := remoteClusterSvc.getOrStartNewAgent(ref, false, false)
	agent.Refresh()
	assert.Equal(hostname, agent.reference.HostName())
	assert.True(refreshCheckActiveHostNameHelper(agent, dummyHostNameList))

	// hostName 1 and 2 and 3 have been moved
	// set things up for second refresh
	//	metadataSvc2 := &service_def.MetadataSvc{}
	//	setupMetaSvcMockGeneric(metadataSvc2, ref2)

	utilitiesMock2 := &utilsMock.UtilsIface{}
	setupUtilsMockFirstNodeBad(utilitiesMock2)

	remoteClusterSvc.updateUtilities(utilitiesMock2)
	//	remoteClusterSvc.updateMetaSvc(metadataSvc2)

	// Second refresh - we should fail
	assert.NotNil(agent.Refresh())
	fmt.Println("============== Test case end: TestRefreshFirstNodeIsBad =================")
}

func runFuncGetTimeElapsed(timedFunc func(), waitGrp *sync.WaitGroup, elapsedTime *time.Duration) {
	defer waitGrp.Done()
	startTime := time.Now()
	timedFunc()
	*elapsedTime = time.Since(startTime)
}

func TestPositiveRefreshWDelay(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPositiveRefreshWDelay =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, manifest := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, ref, 2*time.Second /* networkDelay*/, manifest)

	// First set the callback before creating agents
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackIncrementCount)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.Equal(1, callBackCount)

	remoteClusterSvc.getOrStartNewAgent(ref, false, false)

	// Try agent refresh delay
	var waitGrp sync.WaitGroup
	var refreshTimeTaken time.Duration
	var refreshErr error
	var deleteTimeTaken time.Duration
	var deleteErr error
	waitGrp.Add(2)
	go runFuncGetTimeElapsed(func() { _, refreshErr = remoteClusterSvc.RemoteClusterByRefName(idAndName, true) }, &waitGrp, &refreshTimeTaken)
	time.Sleep(200 * time.Nanosecond)
	go runFuncGetTimeElapsed(func() { _, deleteErr = remoteClusterSvc.DelRemoteCluster(idAndName) }, &waitGrp, &deleteTimeTaken)
	waitGrp.Wait()

	assert.Nil(refreshErr)
	assert.Nil(deleteErr)
	assert.True(deleteTimeTaken < refreshTimeTaken)

	fmt.Println("============== Test case end: TestPositiveRefreshWDelay =================")
}

func TestNoWriteAfterDeletes(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestNoWriteAfterDeletes =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, manifest := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc, ref, 2*time.Second /* networkDelay*/, manifest)

	// First set the callback before creating agents
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackIncrementCount)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.Equal(1, callBackCount)
	agent, _, _ := remoteClusterSvc.getOrStartNewAgent(ref, false, false)
	assert.Nil(agent.deleteFromMetaKV())
	// Make minor changes
	ref.UserName_ = "changedUsername"
	assert.NotNil(agent.UpdateReferenceFrom(ref, true /*updateMetaKV*/))

	fmt.Println("============== Test case end: TestNoWriteAfterDeletes =================")
}
