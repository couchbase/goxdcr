// +build !pcre

// Copyright (c) 2013-2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package metadata_svc

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	base2 "github.com/couchbase/goxdcr/base/helpers"
	baseMock "github.com/couchbase/goxdcr/base/helpers/mocks"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	realUtils "github.com/couchbase/goxdcr/utils"
	utilsMock "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

var uuidField string = "5220aac3bdc2f7ac663ac288bb85ae27"
var uuidField2 string = "dummyUUID2"
var hostname string = "dummyHostName1:9999"
var dnsRemotenodeHostname string = "192.168.0.1:9001"
var dnsSrvHostname string = "xdcr.couchbase.target.local"
var hostnameSSL string = "dummyHostName1:19999"
var externalHostnameSSL string = "dummyHostName1:29999"
var hostname2 string = "dummyHostName2:9999"
var hostname3 string = "dummyHostName3:9999"
var hostname4 string = "dummyHostName4:9999"
var dummyHostNameList = base.StringPairList{base.StringPair{hostname, ""}, base.StringPair{hostname2, ""}, base.StringPair{hostname3, ""}}
var dummyHostNameList2 = base.StringPairList{base.StringPair{hostname, ""}, base.StringPair{hostname2, ""}, base.StringPair{hostname3, ""}, base.StringPair{hostname4, ""}}
var dnsValidHostNameList = base.StringPairList{base.StringPair{dnsRemotenodeHostname, ""}}
var localhost string = "localhost"
var emptyMap = map[string]interface{}{}
var nonEmptyMap = map[string]interface{}{"test": "test"}

var callBackCount int

// Note this callback here is meant for the metadata change callback, and NOT the metakv callback
func testCallbackIncrementCount(string, interface{}, interface{}) error {
	callBackCount++
	return nil
}

func setupBoilerPlateRCS() (*service_def.UILogSvc,
	*service_def.MetadataSvc,
	*service_def.XDCRCompTopologySvc,
	*service_def.ClusterInfoSvc,
	*utilsMock.UtilsIface,
	*RemoteClusterService) {

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

	return uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock, utilitiesMock, remoteClusterSvc
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
	remoteClusterSvc *RemoteClusterService,
	remoteClusterRef *metadata.RemoteClusterReference,
	utilsMockFunc func()) {

	// metakv mock
	setupMetaSvcMockGeneric(metadataSvcMock, remoteClusterRef)

	// utils mock
	utilsMockFunc()

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

var testUtils = realUtils.NewUtilities()

func setupUtilsMockGeneric(utilitiesMock *utilsMock.UtilsIface, simulatedNetworkDelay time.Duration) {
	setupUtilsMockSpecific(utilitiesMock, simulatedNetworkDelay, nil, "", 0, nil, true, false, false, clusterMadHatter, dummyHostNameList)
}

// This is used so when unit tests load actual JSON data, they can use the hostnames in the actual data to
// run positive or negative tests
var nodeListWithMinInfo []string

var externalNodeListWithMinInfo []string

var nodeListPairs base.StringPairList
var nodeListPairsInt base.StringPairList

func populateNodeListForTest(nodeList []interface{}) {
	nodeListWithMinInfo = []string{}
	externalNodeListWithMinInfo = []string{}
	for _, aNode := range nodeList {
		aNodeMap, ok := aNode.(map[string]interface{})
		if !ok {
			continue
		}

		aNodeName, ok := aNodeMap["hostname"].(string)
		if !ok {
			continue
		}
		nodeListWithMinInfo = append(nodeListWithMinInfo, aNodeName)

		alt, ok := aNodeMap["alternateAddresses"].(map[string]interface{})
		if !ok {
			continue
		}

		external, ok := alt["external"].(map[string]interface{})
		if !ok {
			continue
		}

		externalHostName, ok := external["hostname"].(string)
		if !ok {
			continue
		}
		externalNodeListWithMinInfo = append(externalNodeListWithMinInfo, externalHostName)
	}
}

type clusterVersion int

const (
	clusterMadHatter   clusterVersion = iota
	clusterCheshireCat clusterVersion = iota
)

func setupUtilsMockSpecific(utilitiesMock *utilsMock.UtilsIface, simulatedNetworkDelay time.Duration, getNodeListWithMinInfoErr error, extHost string, extPort int, extErr error, getExternalData bool, getExternalPort bool, accurateData bool, version clusterVersion, nonAccurateNodeAddressesList base.StringPairList) {
	var emptyList []interface{}
	localhostEntry := make(map[string]interface{})
	hostnameList := append(emptyList, localhostEntry)

	var totalCallsWDelay int64 = 3
	eachDelayNs := simulatedNetworkDelay.Nanoseconds() / totalCallsWDelay
	oneDelay := time.Duration(eachDelayNs) * time.Nanosecond

	nodeList, _ := getNodeListWithMinInfoMock(getExternalData, getExternalPort)
	populateNodeListForTest(nodeList)
	utilitiesMock.On("GetNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) { time.Sleep(oneDelay) }).Return(nodeList, getNodeListWithMinInfoErr)
	utilitiesMock.On("GetNodeListFromInfoMap", mock.Anything, mock.Anything).Run(func(args mock.Arguments) { time.Sleep(oneDelay) }).Return(nodeList, getNodeListWithMinInfoErr)
	utilitiesMock.On("GetExternalMgtHostAndPort", mock.Anything, mock.Anything).Return(extHost, extPort, extErr)

	if accurateData {
		// ExternalData
		nodeListPairs, _ = testUtils.GetRemoteNodeAddressesListFromNodeList(nodeList, "" /*connStr*/, false /*https*/, nil /*logger*/, true /*external*/)
		utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, mock.Anything, mock.Anything, mock.Anything, true /*external*/).Run(func(args mock.Arguments) { time.Sleep(oneDelay) }).Return(nodeListPairs, nil)
		// InternalData
		nodeListPairsInt, _ = testUtils.GetRemoteNodeAddressesListFromNodeList(nodeList, "" /*connStr*/, false /*https*/, nil /*logger*/, false /*external*/)
		utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, mock.Anything, mock.Anything, mock.Anything, false /*external*/).Run(func(args mock.Arguments) { time.Sleep(oneDelay) }).Return(nodeListPairsInt, nil)
	} else {
		utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) { time.Sleep(oneDelay) }).Return(nonAccurateNodeAddressesList, nil)
	}
	utilitiesMock.On("GetHostName", mock.Anything).Return(localhost)
	utilitiesMock.On("GetPortNumber", mock.Anything).Return(uint16(9999), nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uuidField, hostnameList, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", mock.Anything, mock.Anything).Return(uuidField, nodeList, nil)

	var clusterInfo map[string]interface{}
	var poolsInfo map[string]interface{}

	switch version {
	case clusterMadHatter:
		clusterInfo, _ = getPoolsDefaultPre70()
		poolsInfo, _ = getPoolsPre70()
	case clusterCheshireCat:
		clusterInfo, _ = getPoolsDefault70()
		poolsInfo, _ = getPools70()
	default:
		panic("Not implemented")
	}
	utilitiesMock.On("GetClusterInfoWStatusCode", mock.Anything, base.PoolsPath, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Run(func(args mock.Arguments) { time.Sleep(simulatedNetworkDelay) }).Return(poolsInfo, nil, http.StatusOK)
	utilitiesMock.On("GetClusterInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Run(func(args mock.Arguments) { time.Sleep(oneDelay) }).Return(clusterInfo, getNodeListWithMinInfoErr)
}

func setupUtilsSSL(utilitiesMock *utilsMock.UtilsIface) {
	// This is to simulate when user entered :8091 and tried to use SSL - it will fail
	utilitiesMock.On("GetDefaultPoolInfoUsingHttps", hostname, hostname, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("HttpsUsingdefault will fail"))
	utilitiesMock.On("HttpsRemoteHostAddr", hostname, mock.Anything).Return(hostnameSSL, externalHostnameSSL, nil)
	// When using hostname instead of hostnameSSL (i.e. 8091), fail
	utilitiesMock.On("GetSecuritySettingsAndDefaultPoolInfo", hostname, hostname, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false /*refSanincertificate*/, base.HttpAuthMechHttps, nil /*defaultPoolInfo*/, fmt.Errorf("WrongPort") /*err*/)
	utilitiesMock.On("GetSecuritySettingsAndDefaultPoolInfo", hostname, hostnameSSL, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false /*refSanincertificate*/, base.HttpAuthMechHttps, nil /*defaultPoolInfo*/, nil /*err*/)
	utilitiesMock.On("GetNodeListFromInfoMap", mock.Anything, mock.Anything).Return(nil, nil)
	var sslPairList base.StringPairList
	onePair := base.StringPair{hostname, hostnameSSL}
	sslPairList = append(sslPairList, onePair)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname, mock.Anything, mock.Anything, mock.Anything).Return(sslPairList, nil)
}

func setupUtilsSSL2(utilitiesMock *utilsMock.UtilsIface) {

	defaultPool, _ := getClusterInfoMockExtDummy()

	// This is to simulate when user entered :8091 and tried to use SSL - it will fail
	utilitiesMock.On("GetDefaultPoolInfoUsingHttps", hostname, "", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("HttpsUsingdefault will fail"))
	utilitiesMock.On("HttpsRemoteHostAddr", hostname, mock.Anything).Return(hostnameSSL, externalHostnameSSL, nil)
	// When using hostname instead of hostnameSSL (i.e. 8091), fail
	utilitiesMock.On("GetSecuritySettingsAndDefaultPoolInfo", hostname, hostname, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false /*refSanincertificate*/, base.HttpAuthMechHttps, nil /*defaultPoolInfo*/, fmt.Errorf("WrongPort") /*err*/)
	// Pretend that the target internal host and port is closed
	utilitiesMock.On("GetSecuritySettingsAndDefaultPoolInfo", hostname, hostnameSSL, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false /*refSanincertificate*/, base.HttpAuthMechHttps, nil /*defaultPoolInfo*/, fmt.Errorf("dummy") /*err*/)
	// Only if external port is used then it's ok
	utilitiesMock.On("GetSecuritySettingsAndDefaultPoolInfo", hostname, externalHostnameSSL, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false /*refSanincertificate*/, base.HttpAuthMechHttps, defaultPool /*defaultPoolInfo*/, nil /*err*/)

	nodeList, _ := testUtils.GetNodeListFromInfoMap(defaultPool, nil)
	// The 0th element of the node List is the dummy one we want
	nodeInfoMap := nodeList[0].(map[string]interface{})

	utilitiesMock.On("GetNodeListFromInfoMap", mock.Anything, mock.Anything).Return(nodeList, nil)
	var sslPairList base.StringPairList
	onePair := base.StringPair{hostname, externalHostnameSSL}
	sslPairList = append(sslPairList, onePair)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname, mock.Anything, mock.Anything, true /*shouldUseExternal*/).Return(sslPairList, nil)

	extHostStd, extPortStd, extErrStd := testUtils.GetExternalMgtHostAndPort(nodeInfoMap, false /*isHttps*/)
	utilitiesMock.On("GetExternalMgtHostAndPort", nodeInfoMap, false /*isHttps*/).Return(extHostStd, extPortStd, extErrStd)

	extHostSSL, extPortSSL, extErrSSL := testUtils.GetExternalMgtHostAndPort(nodeInfoMap, true /*isHttps*/)
	utilitiesMock.On("GetExternalMgtHostAndPort", nodeInfoMap, true /*isHttps*/).Return(extHostSSL, extPortSSL, extErrSSL)
}

func setupUtilsMockPre1Good3Bad(utilitiesMock *utilsMock.UtilsIface) {
	var emptyList []interface{}
	defaultPoolMap := make(map[string]interface{})
	hostnameList := append(emptyList, defaultPoolMap)

	nodeList, _ := getNodeListWithMinInfoMock(true /*external*/, false /*given port*/)
	utilitiesMock.On("GetNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nodeList, nil)
	utilitiesMock.On("GetNodeListFromInfoMap", mock.Anything, mock.Anything).Return(nodeList, nil)
	utilitiesMock.On("GetExternalMgtHostAndPort", mock.Anything, mock.Anything).Return("", -1, base.ErrorResourceDoesNotExist)

	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(dummyHostNameList2, nil)
	utilitiesMock.On("GetHostName", mock.Anything).Return(localhost)
	utilitiesMock.On("GetPortNumber", mock.Anything).Return(uint16(9999), nil)
	utilitiesMock.On("GetClusterInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyMap, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uuidField, hostnameList, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", mock.Anything, mock.Anything).Return(uuidField, hostnameList, nil)
}

func setupUtilsOneNode(utilitiesMock *utilsMock.UtilsIface) {
	var emptyList []interface{}
	singleList := base.StringPairList{base.StringPair{hostname, ""}}
	hostnameList := append(emptyList, localhost)

	nodeList, _ := getNodeListWithMinInfoMock(true /*external*/, false /*given port*/)
	nodeList = nodeList[:0]
	utilitiesMock.On("GetNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nodeList, nil)
	utilitiesMock.On("GetNodeListFromInfoMap", mock.Anything, mock.Anything).Return(nodeList, nil)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(singleList, nil)
	utilitiesMock.On("GetHostName", mock.Anything).Return(localhost)
	utilitiesMock.On("GetPortNumber", mock.Anything).Return(uint16(9999), nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uuidField, hostnameList, nil)
	utilitiesMock.On("GetClusterInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", mock.Anything, mock.Anything).Return(uuidField, hostnameList, nil)
}

// emulates where node1 and 2 and 3 becomes cluster splitNames2 and rebalanced
// original cluster is now only consists of splitnames1 (hostname3)
func setupUtilsMock1Good3Bad(utilitiesMock *utilsMock.UtilsIface) base.StringPairList {
	var emptyList []interface{}
	hostnameList := append(emptyList, localhost)
	// the good one
	splitNames1 := base.StringPairList{base.StringPair{hostname4, ""}}
	// the bad ones
	splitNames2 := base.StringPairList{base.StringPair{hostname, ""}, base.StringPair{hostname2, ""}, base.StringPair{hostname3, ""}}
	nodeList, _ := getNodeListWithMinInfoMock(true /*external*/, false /*given port*/)
	nodeList = nodeList[:0]
	utilitiesMock.On("GetNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nodeList, nil)
	utilitiesMock.On("GetNodeListFromInfoMap", mock.Anything, mock.Anything).Return(nodeList, nil)
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
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname, mock.Anything, mock.Anything, mock.Anything).Return(splitNames2, nil)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname2, mock.Anything, mock.Anything, mock.Anything).Return(splitNames2, nil)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname3, mock.Anything, mock.Anything, mock.Anything).Return(splitNames2, nil)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname4, mock.Anything, mock.Anything, mock.Anything).Return(splitNames1, nil)
	return splitNames1
}

//func setupUtilsGetExternalMgtPort(utilitiesMock *utilsMock.UtilsIface, retHost string, retPort int, retErr error) {
//	utilitiesMock.On("GetExternalMgtHostAndPort", mock.Anything, mock.Anything).Return(retHost, retPort, retErr)
//}

// emulates where node1 and 2 becomes cluster splitNames2 and rebalanced
// original cluster is now only consists of splitnames1 (hostname3)
func setupUtilsMockListNode2Bad(utilitiesMock *utilsMock.UtilsIface) base.StringPairList {
	var emptyList []interface{}
	defaultPoolMap := make(map[string]interface{})
	hostnameList := append(emptyList, defaultPoolMap)
	// good one
	splitNames1 := base.StringPairList{base.StringPair{hostname3, ""}}
	// bad ones
	splitNames2 := base.StringPairList{base.StringPair{hostname, ""}, base.StringPair{hostname2, ""}}
	nodeList, _ := getNodeListWithMinInfoMock(true /*external*/, false /*given port*/)
	utilitiesMock.On("GetNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nodeList, nil)
	utilitiesMock.On("GetNodeListFromInfoMap", mock.Anything, mock.Anything).Return(nodeList, nil)
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
	utilitiesMock.On("GetExternalMgtHostAndPort", mock.Anything, mock.Anything).Return("", -1, base.ErrorResourceDoesNotExist)

	// diff from generic above
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname, mock.Anything, mock.Anything, mock.Anything).Return(splitNames2, nil)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname2, mock.Anything, mock.Anything, mock.Anything).Return(splitNames2, nil)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname3, mock.Anything, mock.Anything, mock.Anything).Return(splitNames1, nil)
	return splitNames1
}

// Emulates where first node is moved out and the rest of the nodes do not respond
func setupUtilsMockFirstNodeBad(utilitiesMock *utilsMock.UtilsIface) {
	var emptyList []interface{}
	var emptyStrList []string
	dummyErr := errors.New("Dummy error")
	hostnameList := append(emptyList, localhost)

	// the bad ones
	splitNames2 := base.StringPairList{base.StringPair{hostname, ""}, base.StringPair{hostname2, ""}, base.StringPair{hostname3, ""}}
	utilitiesMock.On("GetNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyList, nil)
	utilitiesMock.On("GetNodeListFromInfoMap", mock.Anything, mock.Anything).Return(emptyList, nil)
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
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname, mock.Anything, mock.Anything, mock.Anything).Return(splitNames2, nil)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname2, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyStrList, dummyErr)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname3, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyStrList, dummyErr)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname4, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyStrList, dummyErr)
}

func createRemoteClusterReference(id string) *metadata.RemoteClusterReference {
	// Use name the same as ID for ease

	aRef, _ := metadata.NewRemoteClusterReference(uuidField, id, hostname, "", "", "", false, "", nil, nil, nil, nil)
	aRef.SetId(id)
	return aRef
}

func createRemoteClusterReferenceExtOnly(id string) *metadata.RemoteClusterReference {
	aRef, _ := metadata.NewRemoteClusterReference(uuidField, id, hostname, "", "", metadata.HostnameMode_External, false, "", nil, nil, nil, nil)
	aRef.SetId(id)
	return aRef
}

func createRemoteClusterReferenceIntOnly(id string) *metadata.RemoteClusterReference {
	aRef, _ := metadata.NewRemoteClusterReference(uuidField, id, hostname, "", "", metadata.HostnameMode_Internal, false, "", nil, nil, nil, nil)
	aRef.SetId(id)
	return aRef
}

func createRemoteClusterReferenceDNSSRV(id string, helper base2.DnsSrvHelperIface) *metadata.RemoteClusterReference {
	aRef, _ := metadata.NewRemoteClusterReference(uuidField, id, dnsSrvHostname, "", "", metadata.HostnameMode_Internal, false, "", nil, nil,
		nil, helper)
	aRef.SetId(id)
	return aRef
}

func TestAddDelRemoteClusterRef(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestAddRDelRemoteClusterRef =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.AddRemoteCluster(ref, true))

	agent, _, _ := remoteClusterSvc.getOrStartNewAgent(ref, false, false)
	assert.NotEqual(agent.addressPreference, Uninitialized)
	agent.Refresh()
	assert.Equal(hostname, agent.reference.HostName())
	assert.NotNil(agent.currentCapability)

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
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	_, jsonMarshalBytes := jsonMarshalWrapper(ref)
	revision := 1

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.RemoteClusterServiceCallback("", jsonMarshalBytes, revision))
	assert.Equal(1, remoteClusterSvc.getNumberOfAgents())
	remoteClusterSvc.waitForRefreshEnabled(ref)
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
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

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
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)
	ref.SetUserName("oldUserName")

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

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
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

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
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

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
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)
	ref2 := ref.Clone()

	_, jsonMarshalBytes := jsonMarshalWrapper(ref)
	revision := 1

	// First set the callback before creating agents
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackIncrementCount)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)
	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.RemoteClusterServiceCallback("/test", jsonMarshalBytes, revision))
	assert.Equal(1, remoteClusterSvc.getNumberOfAgents())
	assert.NotNil(remoteClusterSvc.AddRemoteCluster(ref, true))
	remoteClusterSvc.waitForRefreshEnabled(ref)
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
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

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

func TestPositiveRefresh(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPositiveRefresh =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

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

	fmt.Println("============== Test case end: TestPositiveRefresh =================")
}

func TestPositiveRefreshMultiples(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPositiveRefreshMultiples =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

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

	fmt.Println("Executing multiple parallel refreshes for 10 times. If it is stuck here, something is wrong...")
	for i := 0; i < 10; i++ {
		var spawnWaitGrp sync.WaitGroup
		spawnWaitGrp.Add(3)
		for j := 0; j < 3; j++ {
			go func() {
				defer spawnWaitGrp.Done()
				agent.Refresh()
			}()
		}
		spawnWaitGrp.Wait()
	}

	fmt.Println("============== Test case end: TestPositiveRefreshMultiples =================")
}

func TestRefresh3Nodes2GoesBad(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestRefresh3Nodes2GoesBad =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.AddRemoteCluster(ref, true))

	// First make sure positive case is good - we have "dummyHostName" as the beginning
	agent, _, _ := remoteClusterSvc.getOrStartNewAgent(ref, false, false)
	agent.Refresh()
	assert.Equal(hostname, agent.reference.HostName())
	assert.True(refreshCheckActiveHostNameHelper(agent, dummyHostNameList))

	// Currently nodes are: [dummyHostName1:9999 ] [dummyHostName2:9999 ] [dummyHostName3:9999 ]

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
	assert.Nil(agent.Refresh())
	assert.NotEqual(hostname, agent.reference.HostName())
	assert.True(refreshCheckActiveHostNameHelper(agent, newNodeList))

	fmt.Println("============== Test case end: TestRefresh3Nodes2GoesBad =================")
}

func TestRefresh4Nodes3GoesBad(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestRefresh4Nodes3GoesBad =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

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

	// Launch two refresh's in a row - both should soak up to one single call
	var refreshErr1 error
	var refreshErr2 error
	var refreshTime1 time.Duration
	var refreshTime2 time.Duration
	var waitGrp sync.WaitGroup
	waitGrp.Add(2)
	go runFuncGetTimeElapsed(func() { refreshErr1 = agent.Refresh() }, &waitGrp, &refreshTime1)
	go runFuncGetTimeElapsed(func() { refreshErr2 = agent.Refresh() }, &waitGrp, &refreshTime2)
	agent.Refresh()
	waitGrp.Wait()

	assert.Nil(refreshErr1)
	assert.Nil(refreshErr2)
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
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// First make sure positive case is good - we have "dummyHostName" as the beginning
	agent, _, _ := remoteClusterSvc.getOrStartNewAgent(ref, false, false)
	agent.waitForRefreshEnabled()
	agent.Refresh()
	assert.Equal(hostname, agent.reference.HostName())
	assert.True(refreshCheckActiveHostNameHelper(agent, dummyHostNameList))

	// hostName 1 and 2 and 3 have been moved
	// set things up for second refresh
	utilitiesMock2 := &utilsMock.UtilsIface{}
	setupUtilsMockFirstNodeBad(utilitiesMock2)

	remoteClusterSvc.updateUtilities(utilitiesMock2)

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
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 1*time.Second /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

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
	// Give it some time for goroutine to fire
	time.Sleep(50 * time.Millisecond)
	go runFuncGetTimeElapsed(func() { _, deleteErr = remoteClusterSvc.DelRemoteCluster(idAndName) }, &waitGrp, &deleteTimeTaken)
	waitGrp.Wait()

	assert.Nil(refreshErr)
	assert.Nil(deleteErr)
	assert.True(deleteTimeTaken < refreshTimeTaken)

	fmt.Println("============== Test case end: TestPositiveRefreshWDelay =================")
}

func TestPositiveRefreshWDelayAndAbort(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPositiveRefreshWDelayAndAbort =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 1*time.Second /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// First set the callback before creating agents
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackIncrementCount)

	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.AddRemoteCluster(ref, true))
	assert.Equal(1, callBackCount)

	agent, exists, _ := remoteClusterSvc.getOrStartNewAgent(ref, false, false)
	assert.True(exists)
	agent.waitForRefreshEnabled()

	// Try agent refresh delay
	var waitGrp sync.WaitGroup
	var refreshTimeTaken time.Duration
	var refreshErr error
	waitGrp.Add(1)
	go runFuncGetTimeElapsed(func() { refreshErr = agent.Refresh() }, &waitGrp, &refreshTimeTaken)
	time.Sleep(100 * time.Nanosecond)
	agent.waitForRefreshOngoing()
	agent.AbortAnyOngoingRefresh()
	waitGrp.Wait()

	assert.Equal(RefreshAborted, refreshErr)
	fmt.Println("============== Test case end: TestPositiveRefreshWDelayAndAbort =================")
}

func TestAddThenSetConcurrent(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestAddThenSetConcurrent =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)
	// Add op can modify the ref above
	setRef := ref.Clone()

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0*time.Second /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// First set the callback before creating agents
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackIncrementCount)

	var waitGrp sync.WaitGroup
	var addTimeTaken time.Duration
	var addErr error
	var setTimeTaken time.Duration
	var setErr error
	waitGrp.Add(2)
	go runFuncGetTimeElapsed(func() { addErr = remoteClusterSvc.AddRemoteCluster(ref, true) }, &waitGrp, &addTimeTaken)
	time.Sleep(200 * time.Nanosecond)
	go runFuncGetTimeElapsed(func() { setErr = remoteClusterSvc.SetRemoteCluster(idAndName, setRef) }, &waitGrp, &setTimeTaken)
	waitGrp.Wait()

	assert.Nil(addErr)
	assert.Equal(SetDisabledUntilInit, setErr)
	assert.True(setTimeTaken < addTimeTaken)

	fmt.Println("============== Test case end: TestAddThenSetConcurrent =================")
}

func TestRefreshAndOverride(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestRefreshAndOverride =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)
	setRef := ref.Clone()
	var testcaseUserName string = "RefreshFailUserName"
	setRef.UserName_ = testcaseUserName

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 1*time.Second /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// First set the callback before creating agents
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackIncrementCount)

	agent, exists, err := remoteClusterSvc.getOrStartNewAgent(ref, true /*UserInitiated*/, false /*updateFromRef*/)
	assert.Nil(err)
	assert.NotNil(agent)
	assert.False(exists)
	agent.unitTestBypassMetaKV = true
	agent.waitForRefreshEnabled()

	var waitGrp sync.WaitGroup
	var refreshTimeTaken time.Duration
	var refreshErr error
	waitGrp.Add(1)
	go runFuncGetTimeElapsed(func() { refreshErr = agent.Refresh() }, &waitGrp, &refreshTimeTaken)
	time.Sleep(200 * time.Nanosecond)
	agent.waitForRefreshOngoing()

	// Once refresh is underway, do a set that overrides the reset
	setErr := agent.UpdateReferenceFrom(setRef, true /*updateMetakv*/)
	assert.Nil(setErr)

	waitGrp.Wait()
	assert.Equal(RefreshAborted, refreshErr)

	assert.Equal(testcaseUserName, agent.reference.UserName())

	fmt.Println("============== Test case end: TestRefreshAndOverride =================")
}

func TestSetDisablesRefresh(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestSetDisablesRefresh =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)
	setRef := ref.Clone()
	var testcaseUserName string = "RefreshFailUserName"
	setRef.UserName_ = testcaseUserName

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 1*time.Second /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// First set the callback before creating agents
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackIncrementCount)

	agent, exists, err := remoteClusterSvc.getOrStartNewAgent(ref, true /*UserInitiated*/, false /*updateFromRef*/)
	assert.Nil(err)
	assert.NotNil(agent)
	assert.False(exists)
	agent.unitTestBypassMetaKV = true
	agent.waitForRefreshEnabled()

	var waitGrp sync.WaitGroup
	var setTimeTaken time.Duration
	var setErr error
	waitGrp.Add(1)
	go runFuncGetTimeElapsed(func() { setErr = remoteClusterSvc.SetRemoteCluster(ref.Name(), setRef) }, &waitGrp, &setTimeTaken)
	// Watch for set to start, which is when pendingRef is not empty
	var pendingRefIsEmpty bool = true
	for pendingRefIsEmpty {
		time.Sleep(100 * time.Nanosecond)
		agent.refMtx.RLock()
		pendingRefIsEmpty = agent.pendingRef.IsEmpty()
		agent.refMtx.RUnlock()
	}

	// Once set is occurring, Refresh should be disabled
	refreshErr := agent.Refresh()
	assert.Equal(refreshErr, SetInProgress)

	waitGrp.Wait()
	assert.Equal(nil, setErr)
	assert.Equal(testcaseUserName, agent.reference.UserName())

	// Once set finished, refresh should be re-enabled
	refreshErr = agent.Refresh()
	assert.Equal(refreshErr, nil)

	fmt.Println("============== Test case end: TestSetDisablesRefresh =================")
}

func TestNoWriteAfterDeletes(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestNoWriteAfterDeletes =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 1*time.Second /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

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

// Copied over from utils test
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

var testK8DataDir = "../utils/testK8ExternalData/"
var testExtDataDir = "../utils/testExternalData/"
var testIntDataDir = "../utils/testInternalData/"

func getClusterInfoMockK8() (map[string]interface{}, error) {
	// External address exists but no ports given
	fileName := fmt.Sprintf("%v%v", testK8DataDir, "pools_default.json")
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

func getClusterInfoMockExt() (map[string]interface{}, error) {
	fileName := fmt.Sprintf("%v%v", testExtDataDir, "pools_default.json")
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

func getClusterInfoMockExtDummy() (map[string]interface{}, error) {
	fileName := fmt.Sprintf("%v%v", testExtDataDir, "pools_default_dummy.json")
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

func getClusterInfoMockInt() (map[string]interface{}, error) {
	fileName := fmt.Sprintf("%v%v", testIntDataDir, "pools_default.json")
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

func getNodeListWithMinInfoMock(external bool, portGiven bool) ([]interface{}, error) {
	if external {
		if portGiven {
			clusterInfo, err := getClusterInfoMockExt()
			if err != nil {
				return nil, err
			}
			return testUtils.GetNodeListFromInfoMap(clusterInfo, nil)
		} else {
			clusterInfo, err := getClusterInfoMockK8()
			if err != nil {
				return nil, err
			}
			return testUtils.GetNodeListFromInfoMap(clusterInfo, nil)
		}
	} else {
		clusterInfo, err := getClusterInfoMockInt()
		if err != nil {
			return nil, err
		}
		return testUtils.GetNodeListFromInfoMap(clusterInfo, nil)
	}
}

func getPoolsDefaultPre70() (map[string]interface{}, error) {
	return getClusterInfoMockInt()
}

func getPoolsDefault70() (map[string]interface{}, error) {
	fileName := fmt.Sprintf("%v%v", testIntDataDir, "pools_default_7.0.json")
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

func getPoolsPre70() (map[string]interface{}, error) {
	fileName := fmt.Sprintf("%v%v", testIntDataDir, "pools.json")
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

func getPools70() (map[string]interface{}, error) {
	fileName := fmt.Sprintf("%v%v", testIntDataDir, "pools_7.0.json")
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

func TestAddressPreference(t *testing.T) {
	fmt.Println("============== Test case start: TestAddressPreference =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"

	ref := createRemoteClusterReference(idAndName)
	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0*time.Second /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// test1 - specified no port and alterate address did not give ports
	ref.HostName_ = "cb-example-0000.cb-example.us-west1.spjmurray.co.uk"
	nodeList, err := getNodeListWithMinInfoMock(true /*external*/, false /*given port*/)
	assert.Nil(err)
	agent := remoteClusterSvc.NewRemoteClusterAgent()
	agent.utils = testUtils
	assert.Equal(agent.pendingAddressPreference, Uninitialized)
	agent.stageNewReferenceNoLock(ref, false /*user initiated*/)
	assert.Nil(agent.initAddressPreference(nodeList, ref.HostName(), ref.IsHttps(), ref.HostnameMode()))
	assert.Equal(agent.addressPreference, External)

	// test2 - specified no port and alternate address gives port
	ref.HostName_ = "11.11.11.11"
	nodeList, err = getNodeListWithMinInfoMock(true /*external*/, true /*given port*/)
	assert.Nil(err)
	agent = remoteClusterSvc.NewRemoteClusterAgent()
	agent.utils = testUtils
	assert.Equal(agent.pendingAddressPreference, Uninitialized)
	agent.stageNewReferenceNoLock(ref, false /*user initiated*/)
	assert.Nil(agent.initAddressPreference(nodeList, ref.HostName(), ref.IsHttps(), ref.HostnameMode()))
	assert.Equal(agent.addressPreference, Internal)

	// test3 - specified port and alternate address gives port
	ref.HostName_ = "11.11.11.11:1100"
	nodeList, err = getNodeListWithMinInfoMock(true /*external*/, true /*given port*/)
	assert.Nil(err)
	agent = remoteClusterSvc.NewRemoteClusterAgent()
	agent.utils = testUtils
	assert.Equal(agent.pendingAddressPreference, Uninitialized)
	agent.stageNewReferenceNoLock(ref, false /*user initiated*/)
	assert.Nil(agent.initAddressPreference(nodeList, ref.HostName(), ref.IsHttps(), ref.HostnameMode()))
	assert.Equal(agent.addressPreference, External)

	usesExt, err := agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.True(usesExt)

	fmt.Println("============== Test case end: TestAddressPreference =================")
}

func TestAddressPreferenceInitFail(t *testing.T) {
	fmt.Println("============== Test case start: TestAddressPrefrenceInitFail =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"

	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, base.ErrorUnauthorized, "", 0, nil, true, false, false, clusterMadHatter, dummyHostNameList)
	}

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// When there's an error contacting the bootstrap node, agent should still be created and started
	agent, alreadyExists, err := remoteClusterSvc.getOrStartNewAgent(ref, false /*userInitiated*/, true /*updateFromRef*/)
	assert.Nil(err)
	assert.False(alreadyExists)

	agent.waitForRefreshEnabled()

	// However, agent should return error when bootstrap had issues
	_, err = agent.UsesAlternateAddress()
	assert.Equal(base.ErrorRemoteClusterUninit, err)

	// Agent should also not get the list of refNodes yet
	assert.Equal(0, len(agent.refNodesList))

	// Pretend the second pull now will return ok
	_, _, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
	setupUtilsMockGeneric(utilitiesMock, 0*time.Second)
	remoteClusterSvc.updateUtilities(utilitiesMock)

	// now a refresh should populate node list and also the user's intent
	assert.Nil(agent.Refresh())
	assert.NotEqual(0, len(agent.refNodesList))
	_, err = agent.UsesAlternateAddress()
	assert.Nil(err)

	fmt.Println("============== Test case end: TestAddressPrefrenceInitFail =================")
}

func TestAddressPreferenceForceExt(t *testing.T) {
	fmt.Println("============== Test case start: TestAddressPreferenceForceExt =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"

	ref := createRemoteClusterReferenceExtOnly(idAndName)
	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0*time.Second /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// test1 - specified no port and alterate address did not give ports
	ref.HostName_ = "cb-example-0000.cb-example.us-west1.spjmurray.co.uk"
	nodeList, err := getNodeListWithMinInfoMock(true /*external*/, false /*given port*/)
	assert.Nil(err)
	agent := remoteClusterSvc.NewRemoteClusterAgent()
	agent.utils = testUtils
	assert.Equal(agent.pendingAddressPreference, Uninitialized)
	agent.stageNewReferenceNoLock(ref, false /*user initiated*/)
	assert.Nil(agent.initAddressPreference(nodeList, ref.HostName(), ref.IsHttps(), ref.HostnameMode()))
	assert.Equal(agent.addressPreference, External)

	// test2 - specified no port and alternate address gives port
	ref.HostName_ = "11.11.11.11"
	nodeList, err = getNodeListWithMinInfoMock(true /*external*/, true /*given port*/)
	assert.Nil(err)
	agent = remoteClusterSvc.NewRemoteClusterAgent()
	agent.utils = testUtils
	assert.Equal(agent.pendingAddressPreference, Uninitialized)
	agent.stageNewReferenceNoLock(ref, false /*user initiated*/)
	assert.Nil(agent.initAddressPreference(nodeList, ref.HostName(), ref.IsHttps(), ref.HostnameMode()))
	assert.Equal(agent.addressPreference, External)

	// test3 - specified port and alternate address gives port
	ref.HostName_ = "11.11.11.11:1100"
	nodeList, err = getNodeListWithMinInfoMock(true /*external*/, true /*given port*/)
	assert.Nil(err)
	agent = remoteClusterSvc.NewRemoteClusterAgent()
	agent.utils = testUtils
	assert.Equal(agent.pendingAddressPreference, Uninitialized)
	agent.stageNewReferenceNoLock(ref, false /*user initiated*/)
	assert.Nil(agent.initAddressPreference(nodeList, ref.HostName(), ref.IsHttps(), ref.HostnameMode()))
	assert.Equal(agent.addressPreference, External)

	usesExt, err := agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.True(usesExt)

	fmt.Println("============== Test case end: TestAddressPreferenceForceExt =================")
}

func TestAddressPreferenceChange(t *testing.T) {
	fmt.Println("============== Test case start: TestAddressPreferenceChange =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", 0, nil, true, false, true, clusterMadHatter, dummyHostNameList)
	}

	idAndName := "test"

	ref := createRemoteClusterReference(idAndName)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// This is to grab the external node list info populated from actual mock data
	assert.NotEqual(0, len(nodeListWithMinInfo))
	assert.NotEqual(0, len(externalNodeListWithMinInfo))
	ref.HostName_ = externalNodeListWithMinInfo[0]

	// resetup utilitiesMock knowing this info
	_, _, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
	setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, ref.HostName_, -1, base.ErrorNoPortNumber, true, false, true, clusterMadHatter, dummyHostNameList)
	remoteClusterSvc.updateUtilities(utilitiesMock)

	// First bootstrap agent with a reference that refers to the external hostname
	agent, alreadyExists, err := remoteClusterSvc.getOrStartNewAgent(ref, false /*userInitiated*/, true /*updateFromRef*/)
	assert.Nil(err)
	assert.False(alreadyExists)
	agent.unitTestBypassMetaKV = true

	// Due to the async nature, alternate address should have error
	_, err = agent.UsesAlternateAddress()
	assert.NotNil(err)

	// This is because the getOrStart will launch updateReferenceFrom asynchronously
	// Ensure refresh returns an error
	err = agent.Refresh()
	assert.Equal(RefreshNotEnabledYet, err)

	agent.waitForRefreshEnabled()
	useAlternate, err := agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.True(useAlternate)
	assert.Equal(0, agent.pendingAddressPrefCnt)

	// Now, all the nodes will return internal
	_, _, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
	setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", -1, base.ErrorResourceDoesNotExist, false, false, true, clusterMadHatter, dummyHostNameList)
	remoteClusterSvc.updateUtilities(utilitiesMock)

	for i := 0; i <= base.RemoteClusterAlternateAddrChangeCnt-1; i++ {
		assert.Nil(agent.Refresh())
	}

	useAlternate, err = agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.False(useAlternate)

	// The next refresh will update
	assert.Nil(agent.Refresh())
	useAlternate, err = agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.False(useAlternate)

	// flag should have been raised and a reference returned
	refList, _ := remoteClusterSvc.GetRefListForRestartAndClearState()
	assert.NotEqual(0, len(refList))

	// second call should have NOT returned anything
	refList2, _ := remoteClusterSvc.GetRefListForRestartAndClearState()
	assert.Equal(0, len(refList2))

	fmt.Println("============== Test case end: TestAddressPreferenceChange =================")
}

func TestAddressPreferenceChangeForceExt(t *testing.T) {
	fmt.Println("============== Test case start: TestAddressPreferenceChangeForceExt =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", 0, nil, true, false, true, clusterMadHatter, dummyHostNameList)
	}

	idAndName := "test"

	ref := createRemoteClusterReference(idAndName)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// This is to grab the external node list info populated from actual mock data
	assert.NotEqual(0, len(nodeListWithMinInfo))
	assert.NotEqual(0, len(externalNodeListWithMinInfo))
	ref.HostName_ = externalNodeListWithMinInfo[0]

	// resetup utilitiesMock knowing this info
	_, _, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
	setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, ref.HostName_, -1, base.ErrorNoPortNumber, true, false, true, clusterMadHatter, dummyHostNameList)
	remoteClusterSvc.updateUtilities(utilitiesMock)

	// First bootstrap agent with a reference that refers to the external hostname
	agent, alreadyExists, err := remoteClusterSvc.getOrStartNewAgent(ref, false /*userInitiated*/, true /*updateFromRef*/)
	assert.Nil(err)
	assert.False(alreadyExists)
	agent.unitTestBypassMetaKV = true

	// Due to the async nature, alternate address should have error
	_, err = agent.UsesAlternateAddress()
	assert.NotNil(err)

	// This is because the getOrStart will launch updateReferenceFrom asynchronously
	// Ensure refresh returns an error
	err = agent.Refresh()
	assert.Equal(RefreshNotEnabledYet, err)

	agent.waitForRefreshEnabled()
	useAlternate, err := agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.True(useAlternate)
	assert.Equal(0, agent.pendingAddressPrefCnt)

	// Now, all the nodes will return internal
	_, _, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
	setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", -1, base.ErrorResourceDoesNotExist, false, false, true, clusterMadHatter, dummyHostNameList)
	remoteClusterSvc.updateUtilities(utilitiesMock)

	for i := 0; i <= base.RemoteClusterAlternateAddrChangeCnt-1; i++ {
		assert.Nil(agent.Refresh())
	}

	useAlternate, err = agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.False(useAlternate)

	// The next refresh will update
	assert.Nil(agent.Refresh())
	useAlternate, err = agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.False(useAlternate)

	// flag should have been raised and a reference returned
	refList, _ := remoteClusterSvc.GetRefListForRestartAndClearState()
	assert.NotEqual(0, len(refList))

	// second call should have NOT returned anything
	refList2, _ := remoteClusterSvc.GetRefListForRestartAndClearState()
	assert.Equal(0, len(refList2))

	fmt.Println("============== Test case start: TestAddressPreferenceChangeForceExt =================")
}

func TestAddressPreferenceChangeFlipFlop(t *testing.T) {
	fmt.Println("============== Test case start: TestAddressPreferenceChangeFlipFlop =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", 0, nil, true, false, true, clusterMadHatter, dummyHostNameList)
	}

	idAndName := "test"

	ref := createRemoteClusterReference(idAndName)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// This is to grab the external node list info populated from actual mock data
	assert.NotEqual(0, len(nodeListWithMinInfo))
	assert.NotEqual(0, len(externalNodeListWithMinInfo))
	externalHostName := externalNodeListWithMinInfo[0]
	ref.HostName_ = externalHostName

	// resetup utilitiesMock knowing this info
	_, _, _, _, utilitiesMockExt, _ := setupBoilerPlateRCS()
	setupUtilsMockSpecific(utilitiesMockExt, 0*time.Second, nil, ref.HostName_, -1, base.ErrorNoPortNumber, true, false, true, clusterMadHatter, dummyHostNameList)
	remoteClusterSvc.updateUtilities(utilitiesMockExt)

	// First bootstrap agent with a reference that refers to the external hostname
	agent, alreadyExists, err := remoteClusterSvc.getOrStartNewAgent(ref, false /*userInitiated*/, true /*updateFromRef*/)
	assert.Nil(err)
	assert.False(alreadyExists)
	agent.unitTestBypassMetaKV = true

	// Due to the async nature, alternate address should have error
	_, err = agent.UsesAlternateAddress()
	assert.NotNil(err)

	// This is because the getOrStart will launch updateReferenceFrom asynchronously
	// Ensure refresh returns an error
	err = agent.Refresh()
	assert.Equal(RefreshNotEnabledYet, err)

	agent.waitForRefreshEnabled()
	useAlternate, err := agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.True(useAlternate)
	assert.Equal(0, agent.pendingAddressPrefCnt)

	// Now, all the nodes will return internal
	_, _, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
	setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", -1, base.ErrorResourceDoesNotExist, false, false, true, clusterMadHatter, dummyHostNameList)
	remoteClusterSvc.updateUtilities(utilitiesMock)

	for i := 0; i <= base.RemoteClusterAlternateAddrChangeCnt-2; i++ {
		assert.Nil(agent.Refresh())
	}

	// Now pretend user changed his mind and now future pulls will go back go external
	remoteClusterSvc.updateUtilities(utilitiesMockExt)
	// Force the refname to match external - because unit test
	agent.reference.HostName_ = externalHostName
	assert.Nil(agent.Refresh())

	// Since currently it is running external, and flipflopped back to external, there should not be any need for updates
	assert.Equal(0, agent.pendingAddressPrefCnt)

	fmt.Println("============== Test case start: TestAddressPreferenceChangeFlipFlop =================")
}

// Tests to make sure the https address and port are parsed correctly
// This test will simulate user entering host:nonSSLPort
// And will only return pool info if host:SSL port is used
func TestSetHostNamesAndSecuritySettings(t *testing.T) {
	fmt.Println("============== Test case start: TestSetHostNamesAndSecuritySettings =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	utilsMockFunc := func() {
		setupUtilsSSL(utilitiesMock)
	}

	idAndName := "test"

	ref := createRemoteClusterReference(idAndName)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	ref.EncryptionType_ = metadata.EncryptionType_Full
	ref.DemandEncryption_ = true
	assert.Equal(0, len(ref.HttpsHostName()))

	err := remoteClusterSvc.setHostNamesAndSecuritySettings(ref)
	assert.Nil(err)
	assert.Equal(hostnameSSL, ref.HttpsHostName())
	fmt.Println("============== Test case end: TestSetHostNamesAndSecuritySettings =================")
}

// Test when only external SSL port is successful
// User enters dummyHostName1:9999 where both dummyHostName1 and 9999 are alternate hostname and
// alternate stanard ns_server port
// Ensure that dummyHostName:sslPort fails and only dummyHostName:AltSSLPort succeeds
func TestSetHostNamesAndSecuritySettings2(t *testing.T) {
	fmt.Println("============== Test case start: TestSetHostNamesAndSecuritySettings2 =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	utilsMockFunc := func() {
		setupUtilsSSL2(utilitiesMock)
	}

	idAndName := "test"

	ref := createRemoteClusterReference(idAndName)

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	ref.EncryptionType_ = metadata.EncryptionType_Full
	ref.DemandEncryption_ = true
	assert.Equal(0, len(ref.HttpsHostName()))

	err := remoteClusterSvc.setHostNamesAndSecuritySettings(ref)
	assert.Nil(err)
	assert.Equal(externalHostnameSSL, ref.HttpsHostName())
	fmt.Println("============== Test case end: TestSetHostNamesAndSecuritySettings2 =================")
}

func TestCapabilityStruct(t *testing.T) {
	fmt.Println("============== Test case start: TestCapabilityStruct =================")
	assert := assert.New(t)

	var capability metadata.Capability
	pre70poolsDefault, err := getPoolsDefaultPre70()
	assert.Nil(err)
	err = capability.LoadFromDefaultPoolInfo(pre70poolsDefault, nil /* logger*/)
	assert.Nil(err)
	assert.False(capability.Collection)

	cloneTest := capability.Clone()
	assert.False(cloneTest.Collection)

	on70poolsDefault, err := getPoolsDefault70()
	assert.Nil(err)
	err = capability.LoadFromDefaultPoolInfo(on70poolsDefault, nil /* logger*/)
	assert.Nil(err)
	assert.True(capability.Collection)

	cloneTest = capability.Clone()
	assert.True(cloneTest.Collection)

	fmt.Println("============== Test case end: TestCapabilityStruct =================")
}

func TestCapabilityChange(t *testing.T) {
	fmt.Println("============== Test case start: TestCapabilityChange =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", 0, nil, true, false, true, clusterMadHatter, dummyHostNameList)
	}

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	err := remoteClusterSvc.AddRemoteCluster(ref, true /*skipConnectivity*/)
	assert.Nil(err)
	capability, err := remoteClusterSvc.GetCapability(ref)
	assert.Nil(err)
	// Madhatter does not have collections support
	assert.False(capability.Collection)
	refListChanged, err := remoteClusterSvc.GetRefListForRestartAndClearState()
	assert.Nil(err)
	assert.Equal(0, len(refListChanged))

	agent, _, _ := remoteClusterSvc.getOrStartNewAgent(ref, false /*userInitiated*/, false /*updateFromRef*/)
	assert.NotNil(agent)
	agent.unitTestBypassMetaKV = true

	// Now pretend target cluster upgraded to Cheshire Cat
	_, _, _, _, utilitiesMockNew, _ := setupBoilerPlateRCS()
	setupUtilsMockSpecific(utilitiesMockNew, 0*time.Second, nil, "", 0, nil, true, false, true, clusterCheshireCat, dummyHostNameList)

	// Force a manual refresh to pick up the cheshire cat version
	remoteClusterSvc.updateUtilities(utilitiesMockNew)
	agent, alreadyExists, err := remoteClusterSvc.getOrStartNewAgent(ref, false /*userInitiated*/, false /*updateFromRef*/)
	assert.True(alreadyExists)
	assert.Nil(err)
	assert.NotNil(agent)
	assert.Nil(agent.Refresh())

	capability, err = remoteClusterSvc.GetCapability(ref)
	assert.Nil(err)
	// CheshireCat does have collections support
	assert.True(capability.Collection)
	refListChanged, err = remoteClusterSvc.GetRefListForRestartAndClearState()
	assert.Nil(err)
	assert.Equal(1, len(refListChanged))
	fmt.Println("============== Test case start: TestCapabilityChange =================")
}

func TestInitializeWithExternalModeAndRefreshPullsExtNodeList(t *testing.T) {
	fmt.Println("============== Test case start: TestInitializeWithExternalModeAndRefreshPullsExtNodeList =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReferenceExtOnly(idAndName)

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", 0, nil, true, true, true, clusterMadHatter, dummyHostNameList)
	}

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	agent, alreadyExists, err := remoteClusterSvc.getOrStartNewAgent(ref, true /*userInitiated*/, false /*updateFromRef*/)
	assert.False(alreadyExists)
	assert.Nil(err)
	assert.NotNil(agent)

	// Check to ensure that given hostname mode external and hostname entered non external, returns
	// external node list and external hostname

	// hostname is not the node list but uses external
	assert.True(agent.reference.HostName() != "11.11.11.11:1100" && agent.reference.HostName() != "12.12.12.12:1100")
	useExternal, err := agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.True(useExternal)
	for _, node := range agent.refNodesList {
		assert.True(node[0] == "11.11.11.11:1100" || node[0] == "12.12.12.12:1100")
	}

	// When refreshing, as long as refresh context pulls external list, we should be fine
	// Commenting it out as this is used to check and because the Refresh() below encapsulates this logic
	//	rctx, _, err := agent.initializeNewRefreshContext()
	//	assert.Nil(err)
	//	useExternal, err = rctx.getAddressPreference()
	//	assert.Nil(err)
	//	assert.True(useExternal)

	agent.unitTestBypassMetaKV = true
	assert.Nil(agent.Refresh())

	assert.True(agent.reference.HostName() == "11.11.11.11:1100" || agent.reference.HostName() == "12.12.12.12:1100")
	useExternal, err = agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.True(useExternal)
	for _, node := range agent.refNodesList {
		assert.True(node[0] == "11.11.11.11:1100" || node[0] == "12.12.12.12:1100")
	}

	fmt.Println("============== Test case end: TestInitializeWithExternalModeAndRefreshPullsExtNodeList =================")
}

// Note this is the "Inverse" test of the above - ie original behavior without the external flag set
func TestInitializeWithoutExternalModeAndRefreshPullsExtNodeList(t *testing.T) {
	fmt.Println("============== Test case start: TestInitializeWithoutExternalModeAndRefreshPullsExtNodeList =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", 0, nil, true, true, true, clusterMadHatter, dummyHostNameList)
	}

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	agent, alreadyExists, err := remoteClusterSvc.getOrStartNewAgent(ref, true /*userInitiated*/, false /*updateFromRef*/)
	assert.False(alreadyExists)
	assert.Nil(err)
	assert.NotNil(agent)

	// hostname is not the node list and does not use external
	assert.True(agent.reference.HostName() != "11.11.11.11:1100" && agent.reference.HostName() != "12.12.12.12:1100")
	useExternal, err := agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.False(useExternal)
	for _, node := range agent.refNodesList {
		assert.True(node[0] != "11.11.11.11:1100" && node[0] != "12.12.12.12:1100")
	}

	agent.unitTestBypassMetaKV = true
	assert.Nil(agent.Refresh())

	assert.False(agent.reference.HostName() == "11.11.11.11:1100" || agent.reference.HostName() == "12.12.12.12:1100")
	useExternal, err = agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.False(useExternal)
	for _, node := range agent.refNodesList {
		assert.False(node[0] == "11.11.11.11:1100" || node[0] == "12.12.12.12:1100")
	}

	fmt.Println("============== Test case end: TestInitializeWithoutExternalModeAndRefreshPullsExtNodeList =================")
}

// This is a clone of TestAddressPreferenceChange but forcing a ref to be created with internal
// This is to test to ensure that a ref created with internal only cannot change preference
func TestAddressPreferenceChangeWithDefaultMode(t *testing.T) {
	fmt.Println("============== Test case start: TestAddressPreferenceChangeWithDefaultMode =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", 0, nil, true, false, true, clusterMadHatter, dummyHostNameList)
	}

	idAndName := "test"

	ref := createRemoteClusterReferenceIntOnly(idAndName)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// This is to grab the external node list info populated from actual mock data
	assert.NotEqual(0, len(nodeListWithMinInfo))
	assert.NotEqual(0, len(externalNodeListWithMinInfo))
	ref.HostName_ = externalNodeListWithMinInfo[0]

	// resetup utilitiesMock knowing this info
	_, _, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
	setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, ref.HostName_, -1, base.ErrorNoPortNumber, true, false, true, clusterMadHatter, dummyHostNameList)
	remoteClusterSvc.updateUtilities(utilitiesMock)

	// First bootstrap agent with a reference that refers to the external hostname
	agent, alreadyExists, err := remoteClusterSvc.getOrStartNewAgent(ref, false /*userInitiated*/, true /*updateFromRef*/)
	assert.Nil(err)
	assert.False(alreadyExists)
	agent.unitTestBypassMetaKV = true

	// Due to the async nature, alternate address should have error
	_, err = agent.UsesAlternateAddress()
	assert.NotNil(err)

	// This is because the getOrStart will launch updateReferenceFrom asynchronously
	// Ensure refresh returns an error
	err = agent.Refresh()
	assert.Equal(RefreshNotEnabledYet, err)

	agent.waitForRefreshEnabled()
	useAlternate, err := agent.UsesAlternateAddress()
	assert.Nil(err)
	// Should be false for use alternate because of forced internal
	assert.False(useAlternate)
	assert.Equal(0, agent.pendingAddressPrefCnt)

	// Now, all the nodes will return internal
	_, _, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
	setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", -1, base.ErrorResourceDoesNotExist, false, false, true, clusterMadHatter, dummyHostNameList)
	remoteClusterSvc.updateUtilities(utilitiesMock)

	for i := 0; i <= base.RemoteClusterAlternateAddrChangeCnt-1; i++ {
		assert.Nil(agent.Refresh())
	}

	useAlternate, err = agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.False(useAlternate)

	// The next refresh will update
	assert.Nil(agent.Refresh())
	useAlternate, err = agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.False(useAlternate)

	// flag should not have been raised and a reference returned
	refList, _ := remoteClusterSvc.GetRefListForRestartAndClearState()
	assert.Equal(0, len(refList))
	fmt.Println("============== Test case start: TestAddressPreferenceChangeWithDefaultMode =================")
}

func TestDNSSrv(t *testing.T) {
	fmt.Println("============== Test case start: TestDNSSrv =================")

	// Test to see if above set up has been done correctly
	cname, addrs, err := net.LookupSRV("", "", dnsSrvHostname)
	if err != nil {
		fmt.Printf("Local DNS look up failed for %v, skipping DNSSRV unit test\n", dnsSrvHostname)
		return
	}
	fmt.Printf("ND: Lookup cname: %v, addrs: %v, err: %v\n", cname, addrs, err)
	for _, addr := range addrs {
		fmt.Printf("Address: %v port: %v, priority: %v weight: %v\n", addr.Target, addr.Port, addr.Priority, addr.Weight)
	}

	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", 0, nil, true, false,
			false, clusterMadHatter, dnsValidHostNameList)
	}

	srvHelper := &baseMock.DnsSrvHelperIface{}
	srvEntry := &net.SRV{
		Target: "192.168.0.1.",
		Port:   9001,
	}
	var srvList []*net.SRV
	srvList = append(srvList, srvEntry)

	srvHelper.On("DnsSrvLookup", dnsSrvHostname).Return(srvList, nil)

	idAndName := "test"
	ref := createRemoteClusterReferenceDNSSRV(idAndName, srvHelper)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, clusterInfoSvcMock,
		remoteClusterSvc, ref, utilsMockFunc)

	agent, alreadyExists, err := remoteClusterSvc.getOrStartNewAgent(ref, false /*userInitiated*/, true /*updateFromRef*/)
	assert.Nil(err)
	assert.False(alreadyExists)
	agent.unitTestBypassMetaKV = true

	agent.waitForRefreshEnabled()
	agent.refMtx.RLock()
	assert.True(agent.reference.IsDnsSRV())
	srvHostNames := agent.reference.GetSRVHostNames()
	agent.refMtx.RUnlock()
	var found bool
	for _, host := range srvHostNames {
		if host == dnsRemotenodeHostname {
			found = true
			break
		}
	}
	assert.True(found)
	assert.Equal(dnsSrvHostname, ref.HostName())
	agent.waitForRefreshEnabled()
	agent.refMtx.RLock()
	assert.Equal(dnsSrvHostname, agent.reference.HostName())
	assert.True(agent.reference.IsDnsSRV())
	agent.refMtx.RUnlock()

	// After refresh, hostname should remain the same
	assert.Nil(agent.Refresh())
	agent.refMtx.RLock()
	assert.Equal(dnsSrvHostname, agent.reference.HostName())
	oldHostName := agent.reference.HostName()
	agent.refMtx.RUnlock()

	// Now pretend DNS SRV is turned off/broken
	badSRVHelper := &baseMock.DnsSrvHelperIface{}
	var emptyList []*net.SRV
	badSRVHelper.On("DnsSrvLookup", mock.Anything).Return(emptyList, fmt.Errorf("Dummy"))
	agent.refMtx.Lock()
	agent.reference.UnitTestSetSRVHelper(badSRVHelper)
	agent.refMtx.Unlock()

	// Host name should be replaced
	assert.Nil(agent.Refresh())
	agent.refMtx.RLock()
	assert.NotEqual(oldHostName, agent.reference.HostName())
	agent.refMtx.RUnlock()
	fmt.Println("============== Test case end: TestDNSSrv =================")
}
