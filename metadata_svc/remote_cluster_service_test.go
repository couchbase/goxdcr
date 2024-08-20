//go:build !pcre
// +build !pcre

// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata_svc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	base2 "github.com/couchbase/goxdcr/v8/base/helpers"
	baseMock "github.com/couchbase/goxdcr/v8/base/helpers/mocks"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	service_def "github.com/couchbase/goxdcr/v8/service_def/mocks"
	realUtils "github.com/couchbase/goxdcr/v8/utils"
	utilsMock "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
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
	*utilsMock.UtilsIface,
	*RemoteClusterService) {

	uiLogSvcMock := &service_def.UILogSvc{}
	metadataSvcMock := &service_def.MetadataSvc{}
	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	utilitiesMock := &utilsMock.UtilsIface{}
	utilitiesMock.On("ExponentialBackoffExecutor", "GetAllMetadataFromCatalogRemoteCluster", mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return(nil)

	remoteClusterSvc, _ := NewRemoteClusterService(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		log.DefaultLoggerContext, utilitiesMock)

	callBackCount = 0

	return uiLogSvcMock, metadataSvcMock, xdcrTopologyMock, utilitiesMock, remoteClusterSvc
}

func jsonMarshalWrapper(ref *metadata.RemoteClusterReference) (key string, value []byte) {
	jsonKey := ref.Id()
	jsonBytes, _ := ref.Marshal()
	return jsonKey, jsonBytes
}

func setupMocksRCS(uiLogSvcMock *service_def.UILogSvc,
	metadataSvcMock *service_def.MetadataSvc,
	xdcrTopologyMock *service_def.XDCRCompTopologySvc,
	remoteClusterSvc *RemoteClusterService,
	remoteClusterRef *metadata.RemoteClusterReference,
	utilsMockFunc func()) {

	// metakv mock
	setupMetaSvcMockGeneric(metadataSvcMock, remoteClusterRef)

	setupXDCRTopologyMock(xdcrTopologyMock)

	// utils mock
	utilsMockFunc()

	// UI svc
	uiLogSvcMock.On("Write", mock.Anything).Return(nil)

}

func setupXDCRTopologyMock(topologyMock *service_def.XDCRCompTopologySvc) {
	topologyMock.On("IsMyClusterEnterprise").Return(true, nil)
	topologyMock.On("IsMyClusterEncryptionLevelStrict").Return(false)
}

func setupMetaSvcMockGeneric(metadataSvcMock *service_def.MetadataSvc, remoteClusterRef *metadata.RemoteClusterReference) {
	// Get json marshal and unmarshal for mocking
	jsonKey, jsonMarshalBytes := jsonMarshalWrapper(remoteClusterRef)
	revision := []byte{1}

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
	utilitiesMock.On("GetClusterUUID", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uuidField, nil)

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

	utilitiesMock.On("GetClusterInfoWStatusCode", mock.Anything, base.DefaultPoolPath, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) { time.Sleep(oneDelay) }).Return(clusterInfo, getNodeListWithMinInfoErr, http.StatusOK)
	utilitiesMock.On("GetClusterInfoWStatusCode", mock.Anything, base.PoolsPath, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) { time.Sleep(oneDelay) }).Return(poolsInfo, getNodeListWithMinInfoErr, http.StatusOK)
	utilitiesMock.On("GetClusterInfo", mock.Anything, base.DefaultPoolPath, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Run(func(args mock.Arguments) { time.Sleep(oneDelay) }).Return(clusterInfo, getNodeListWithMinInfoErr)
	utilitiesMock.On("GetClusterInfo", mock.Anything, base.PoolsPath, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Run(func(args mock.Arguments) { time.Sleep(oneDelay) }).Return(poolsInfo, getNodeListWithMinInfoErr)
	utilitiesMock.On("GetClusterHeartbeatStatusFromNodeList", mock.Anything).Return(nil, nil)
	utilitiesMock.On("StartDiagStopwatch", mock.Anything, mock.Anything).Return(func() time.Duration { return 0 })
}

func setupUtilsSSL(utilitiesMock *utilsMock.UtilsIface) {
	// This is to simulate when user entered :8091 and tried to use SSL - it will fail
	utilitiesMock.On("GetDefaultPoolInfoUsingHttps", hostname, hostname, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, http.StatusUnauthorized, fmt.Errorf("HttpsUsingdefault will fail"))
	utilitiesMock.On("HttpsRemoteHostAddr", hostname, mock.Anything).Return(hostnameSSL, externalHostnameSSL, nil)
	// When using hostname instead of hostnameSSL (i.e. 8091), fail
	utilitiesMock.On("GetSecuritySettingsAndDefaultPoolInfo", hostname, hostname, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(base.HttpAuthMechHttps, nil /*defaultPoolInfo*/, http.StatusUnauthorized, fmt.Errorf("WrongPort") /*err*/)
	utilitiesMock.On("GetSecuritySettingsAndDefaultPoolInfo", hostname, hostnameSSL, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(base.HttpAuthMechHttps, nil /*defaultPoolInfo*/, http.StatusOK, nil /*err*/)
	utilitiesMock.On("GetNodeListFromInfoMap", mock.Anything, mock.Anything).Return(nil, nil)
	var sslPairList base.StringPairList
	onePair := base.StringPair{hostname, hostnameSSL}
	sslPairList = append(sslPairList, onePair)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", mock.Anything, hostname, mock.Anything, mock.Anything, mock.Anything).Return(sslPairList, nil)
	utilitiesMock.On("GetClusterHeartbeatStatusFromNodeList", mock.Anything).Return(nil, nil)
	utilitiesMock.On("StartDiagStopwatch", mock.Anything, mock.Anything).Return(func() time.Duration { return 0 })
}

func setupUtilsSSL2(utilitiesMock *utilsMock.UtilsIface) {

	defaultPool, _ := getClusterInfoMockExtDummy()

	// This is to simulate when user entered :8091 and tried to use SSL - it will fail
	utilitiesMock.On("GetDefaultPoolInfoUsingHttps", hostname, "", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("HttpsUsingdefault will fail"))
	utilitiesMock.On("HttpsRemoteHostAddr", hostname, mock.Anything).Return(hostnameSSL, externalHostnameSSL, nil)
	// When using hostname instead of hostnameSSL (i.e. 8091), fail
	utilitiesMock.On("GetSecuritySettingsAndDefaultPoolInfo", hostname, hostname, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(base.HttpAuthMechHttps, nil /*defaultPoolInfo*/, http.StatusUnauthorized, fmt.Errorf("WrongPort") /*err*/)
	// Pretend that the target internal host and port is closed
	utilitiesMock.On("GetSecuritySettingsAndDefaultPoolInfo", hostname, hostnameSSL, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(base.HttpAuthMechHttps, nil /*defaultPoolInfo*/, http.StatusUnauthorized, fmt.Errorf("dummy") /*err*/)
	// Only if external port is used then it's ok
	utilitiesMock.On("GetSecuritySettingsAndDefaultPoolInfo", hostname, externalHostnameSSL, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(base.HttpAuthMechHttps, defaultPool /*defaultPoolInfo*/, http.StatusOK, nil /*err*/)

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
	utilitiesMock.On("GetClusterHeartbeatStatusFromNodeList", mock.Anything).Return(nil, nil)
	utilitiesMock.On("StartDiagStopwatch", mock.Anything, mock.Anything).Return(func() time.Duration { return 0 })
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
	utilitiesMock.On("GetClusterInfoWStatusCode", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nonEmptyMap, nil, http.StatusOK)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uuidField, hostnameList, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", mock.Anything, mock.Anything).Return(uuidField, hostnameList, nil)
	utilitiesMock.On("GetClusterHeartbeatStatusFromNodeList", mock.Anything).Return(nil, nil)
	utilitiesMock.On("StartDiagStopwatch", mock.Anything, mock.Anything).Return(func() time.Duration { return 0 })
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
	utilitiesMock.On("GetClusterHeartbeatStatusFromNodeList", mock.Anything).Return(nil, nil)
	utilitiesMock.On("StartDiagStopwatch", mock.Anything, mock.Anything).Return(func() time.Duration { return 0 })
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
	utilitiesMock.On("GetClusterInfoWStatusCode", hostname, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nonEmptyMap, nil, http.StatusOK)
	utilitiesMock.On("GetClusterInfoWStatusCode", hostname2, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nonEmptyMap, nil, http.StatusOK)
	utilitiesMock.On("GetClusterInfoWStatusCode", hostname3, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nonEmptyMap, nil, http.StatusOK)
	utilitiesMock.On("GetClusterInfoWStatusCode", hostname4, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyMap, nil, http.StatusOK)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", nonEmptyMap, mock.Anything).Return(uuidField2, hostnameList, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", emptyMap, mock.Anything).Return(uuidField, emptyList, nil)
	utilitiesMock.On("GetClusterHeartbeatStatusFromNodeList", mock.Anything).Return(nil, nil)
	utilitiesMock.On("StartDiagStopwatch", mock.Anything, mock.Anything).Return(func() time.Duration { return 0 })

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
	utilitiesMock.On("GetClusterInfo", hostname3, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Return(nonEmptyMap, nil)
	utilitiesMock.On("GetClusterInfo", hostname, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Return(emptyMap, nil)
	utilitiesMock.On("GetClusterInfo", hostname2, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Return(emptyMap, nil)
	utilitiesMock.On("GetClusterInfoWStatusCode", hostname3, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nonEmptyMap, nil, http.StatusOK)
	utilitiesMock.On("GetClusterInfoWStatusCode", hostname, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyMap, nil, http.StatusOK)
	utilitiesMock.On("GetClusterInfoWStatusCode", hostname2, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyMap, nil, http.StatusOK)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", nonEmptyMap, mock.Anything).Return(uuidField, hostnameList, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", emptyMap, mock.Anything).Return(uuidField2, hostnameList, nil)
	utilitiesMock.On("GetExternalMgtHostAndPort", mock.Anything, mock.Anything).Return("", -1, base.ErrorResourceDoesNotExist)
	utilitiesMock.On("GetClusterHeartbeatStatusFromNodeList", mock.Anything).Return(nil, nil)
	utilitiesMock.On("StartDiagStopwatch", mock.Anything, mock.Anything).Return(func() time.Duration { return 0 })

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
	utilitiesMock.On("GetClusterInfoWStatusCode", hostname, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nonEmptyMap, nil, http.StatusOK)
	utilitiesMock.On("GetClusterInfoWStatusCode", hostname2, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyMap, nil, http.StatusOK)
	utilitiesMock.On("GetClusterInfoWStatusCode", hostname3, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyMap, nil, http.StatusOK)
	utilitiesMock.On("GetClusterInfoWStatusCode", hostname4, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(emptyMap, nil, http.StatusOK)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", nonEmptyMap, mock.Anything).Return(uuidField2, hostnameList, nil)
	utilitiesMock.On("GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo", emptyMap, mock.Anything).Return("", emptyList, dummyErr)
	utilitiesMock.On("GetClusterHeartbeatStatusFromNodeList", mock.Anything).Return(nil, nil)
	utilitiesMock.On("StartDiagStopwatch", mock.Anything, mock.Anything).Return(func() time.Duration { return 0 })

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

func createRemoteClusterReferenceWithRev(id string, rev interface{}) *metadata.RemoteClusterReference {
	aRef := createRemoteClusterReference(id)
	aRef.SetRevision(rev)
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

var dummySelfSignedCert = []byte("-----BEGIN CERTIFICATE-----\nMIIB/TCCAWagAwIBAgIIFi5FqJcUd6AwDQYJKoZIhvcNAQELBQAwJDEiMCAGA1UE\nAxMZQ291Y2hiYXNlIFNlcnZlciAxN2ViMGZjZDAeFw0xMzAxMDEwMDAwMDBaFw00\nOTEyMzEyMzU5NTlaMCQxIjAgBgNVBAMTGUNvdWNoYmFzZSBTZXJ2ZXIgMTdlYjBm\nY2QwgZ8wDQYJKoZIhvcNAQEBBQADgY0AMIGJAoGBAL2wCiFGsxdn4Uw95KxlcCJt\nfdzl5N8NNRbDTmeONJjb7nmYIsGQgyBJAEpRFkGqbCaPi1MrQ5KTAWnA7OE5Puhu\npHHUaSFg74Ns3+5L05Rwj4pemQkFlNeX37b3uB8S6T4XoXi12bGWScE+s1twKM2p\nC8BUYyjP/o8NwH6/eEk3AgMBAAGjODA2MA4GA1UdDwEB/wQEAwICpDATBgNVHSUE\nDDAKBggrBgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4GB\nACZJN5HlJBimKNbD/Mzfy3vuEWRf9R3vA7QSSUlvckGcTh8v0HjdkxP6PO8+rulY\n1356Mi3S7Nw0DAXu/uRHMXA8MUMfsS3Lco1if2pjc9XjY7KeMDk7J13Xy3VoMSg5\nqpUM/4CL4gK0f4gbkV2vQC2+b8PRx3dhn8twGWW378YY\n-----END CERTIFICATE-----\n")

func createSSLRemoteClusterRef(id string, helper base2.DnsSrvHelperIface) *metadata.RemoteClusterReference {
	aRef, _ := metadata.NewRemoteClusterReference(uuidField, id, hostname, "username", "passworD", metadata.HostnameMode_Internal, true, metadata.EncryptionType_Full, dummySelfSignedCert, nil, nil, helper)
	aRef.SetId(id)
	return aRef
}

func createRemoteClusterRefWithHost(id, host string, helper base2.DnsSrvHelperIface, assert *assert.Assertions) *metadata.RemoteClusterReference {
	hostAddr, err := base.ValidateHostAddr(host)
	assert.Nil(err)
	aRef, _ := metadata.NewRemoteClusterReference(uuidField, id, hostAddr, "", "", "", false, "", nil, nil, nil, helper)
	aRef.SetId(id)
	return aRef
}

func TestAddDelRemoteClusterRef(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestAddDelRemoteClusterRef =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	_, jsonMarshalBytes := jsonMarshalWrapper(ref)
	revision := []byte{1}

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)
	ref.SetUserName("oldUserName")

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)
	ref2 := ref.Clone()

	_, jsonMarshalBytes := jsonMarshalWrapper(ref)
	revision := []byte{1}

	// First set the callback before creating agents
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackIncrementCount)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	revision = []byte{2}

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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 1*time.Second /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 1*time.Second /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)
	// Add op can modify the ref above
	setRef := ref.Clone()

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0*time.Second /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	setErrPass := setErr != nil && (setErr == RefreshNotEnabledYet || strings.Contains(setErr.Error(), UnknownRemoteClusterErrorMessage))
	assert.True(setErrPass)
	assert.True(setTimeTaken < addTimeTaken)

	fmt.Println("============== Test case end: TestAddThenSetConcurrent =================")
}

func TestRefreshAndOverride(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestRefreshAndOverride =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)
	setRef := ref.Clone()
	var testcaseUserName string = "RefreshFailUserName"
	setRef.UserName_ = testcaseUserName

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 1*time.Second /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)
	setRef := ref.Clone()
	var testcaseUserName string = "RefreshFailUserName"
	setRef.UserName_ = testcaseUserName

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 1*time.Second /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 1*time.Second /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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

func getCloudNodeList() ([]interface{}, error) {
	fileName := fmt.Sprintf("%v%v", testExtDataDir, "cloudNodeList.json")
	byteSlice, err := ioutil.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	var unmarshalledIface interface{}
	err = json.Unmarshal(byteSlice, &unmarshalledIface)

	return unmarshalledIface.([]interface{}), nil
}

func TestAddressPreference(t *testing.T) {
	fmt.Println("============== Test case start: TestAddressPreference =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"

	ref := createRemoteClusterReference(idAndName)
	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0*time.Second /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// test1 - specified no port and alterate address did not give ports
	ref.HostName_ = "cb-example-0000.cb-example.us-west1.spjmurray.co.uk"
	nodeList, err := getNodeListWithMinInfoMock(true /*external*/, false /*given port*/)
	assert.Nil(err)
	agent := remoteClusterSvc.NewRemoteClusterAgent()
	agent.utils = testUtils
	assert.Equal(agent.pendingAddressPreference, Uninitialized)
	agent.stageNewReferenceNoLock(ref, false /*user initiated*/)
	assert.Nil(agent.initAddressPreference(nodeList, ref.HostName(), ref.IsHttps(), ref.HostnameMode(), nil))
	assert.Equal(agent.addressPreference, External)

	// test2 - specified no port and alternate address gives port
	ref.HostName_ = "11.11.11.11"
	nodeList, err = getNodeListWithMinInfoMock(true /*external*/, true /*given port*/)
	assert.Nil(err)
	agent = remoteClusterSvc.NewRemoteClusterAgent()
	agent.utils = testUtils
	assert.Equal(agent.pendingAddressPreference, Uninitialized)
	agent.stageNewReferenceNoLock(ref, false /*user initiated*/)
	assert.Nil(agent.initAddressPreference(nodeList, ref.HostName(), ref.IsHttps(), ref.HostnameMode(), nil))
	assert.Equal(agent.addressPreference, Internal)

	// test3 - specified port and alternate address gives port
	ref.HostName_ = "11.11.11.11:1100"
	nodeList, err = getNodeListWithMinInfoMock(true /*external*/, true /*given port*/)
	assert.Nil(err)
	agent = remoteClusterSvc.NewRemoteClusterAgent()
	agent.utils = testUtils
	assert.Equal(agent.pendingAddressPreference, Uninitialized)
	agent.stageNewReferenceNoLock(ref, false /*user initiated*/)
	assert.Nil(agent.initAddressPreference(nodeList, ref.HostName(), ref.IsHttps(), ref.HostnameMode(), nil))
	assert.Equal(agent.addressPreference, External)

	usesExt, err := agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.True(usesExt)

	fmt.Println("============== Test case end: TestAddressPreference =================")
}

func TestAddressPreferenceInitFail(t *testing.T) {
	fmt.Println("============== Test case start: TestAddressPrefrenceInitFail =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"

	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, base.ErrorUnauthorized, "", 0, nil, true, false, false, clusterMadHatter, dummyHostNameList)
	}

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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
	_, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
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

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"

	ref := createRemoteClusterReferenceExtOnly(idAndName)
	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0*time.Second /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// test1 - specified no port and alterate address did not give ports
	ref.HostName_ = "cb-example-0000.cb-example.us-west1.spjmurray.co.uk"
	nodeList, err := getNodeListWithMinInfoMock(true /*external*/, false /*given port*/)
	assert.Nil(err)
	agent := remoteClusterSvc.NewRemoteClusterAgent()
	agent.utils = testUtils
	assert.Equal(agent.pendingAddressPreference, Uninitialized)
	agent.stageNewReferenceNoLock(ref, false /*user initiated*/)
	assert.Nil(agent.initAddressPreference(nodeList, ref.HostName(), ref.IsHttps(), ref.HostnameMode(), nil))
	assert.Equal(agent.addressPreference, External)

	// test2 - specified no port and alternate address gives port
	ref.HostName_ = "11.11.11.11"
	nodeList, err = getNodeListWithMinInfoMock(true /*external*/, true /*given port*/)
	assert.Nil(err)
	agent = remoteClusterSvc.NewRemoteClusterAgent()
	agent.utils = testUtils
	assert.Equal(agent.pendingAddressPreference, Uninitialized)
	agent.stageNewReferenceNoLock(ref, false /*user initiated*/)
	assert.Nil(agent.initAddressPreference(nodeList, ref.HostName(), ref.IsHttps(), ref.HostnameMode(), nil))
	assert.Equal(agent.addressPreference, External)

	// test3 - specified port and alternate address gives port
	ref.HostName_ = "11.11.11.11:1100"
	nodeList, err = getNodeListWithMinInfoMock(true /*external*/, true /*given port*/)
	assert.Nil(err)
	agent = remoteClusterSvc.NewRemoteClusterAgent()
	agent.utils = testUtils
	assert.Equal(agent.pendingAddressPreference, Uninitialized)
	agent.stageNewReferenceNoLock(ref, false /*user initiated*/)
	assert.Nil(agent.initAddressPreference(nodeList, ref.HostName(), ref.IsHttps(), ref.HostnameMode(), nil))
	assert.Equal(agent.addressPreference, External)

	usesExt, err := agent.UsesAlternateAddress()
	assert.Nil(err)
	assert.True(usesExt)

	fmt.Println("============== Test case end: TestAddressPreferenceForceExt =================")
}

func TestAddressPreferenceChange(t *testing.T) {
	fmt.Println("============== Test case start: TestAddressPreferenceChange =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", 0, nil, true, false, true, clusterMadHatter, dummyHostNameList)
	}

	idAndName := "test"

	ref := createRemoteClusterReference(idAndName)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// This is to grab the external node list info populated from actual mock data
	assert.NotEqual(0, len(nodeListWithMinInfo))
	assert.NotEqual(0, len(externalNodeListWithMinInfo))
	ref.HostName_ = externalNodeListWithMinInfo[0]

	// resetup utilitiesMock knowing this info
	_, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
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
	_, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
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

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", 0, nil, true, false, true, clusterMadHatter, dummyHostNameList)
	}

	idAndName := "test"

	ref := createRemoteClusterReference(idAndName)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// This is to grab the external node list info populated from actual mock data
	assert.NotEqual(0, len(nodeListWithMinInfo))
	assert.NotEqual(0, len(externalNodeListWithMinInfo))
	ref.HostName_ = externalNodeListWithMinInfo[0]

	// resetup utilitiesMock knowing this info
	_, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
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
	_, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
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

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", 0, nil, true, false, true, clusterMadHatter, dummyHostNameList)
	}

	idAndName := "test"

	ref := createRemoteClusterReference(idAndName)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// This is to grab the external node list info populated from actual mock data
	assert.NotEqual(0, len(nodeListWithMinInfo))
	assert.NotEqual(0, len(externalNodeListWithMinInfo))
	externalHostName := externalNodeListWithMinInfo[0]
	ref.HostName_ = externalHostName

	// resetup utilitiesMock knowing this info
	_, _, _, utilitiesMockExt, _ := setupBoilerPlateRCS()
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
	_, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
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

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	utilsMockFunc := func() {
		setupUtilsSSL(utilitiesMock)
	}

	idAndName := "test"

	ref := createRemoteClusterReference(idAndName)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)

	ref.EncryptionType_ = metadata.EncryptionType_Full
	ref.DemandEncryption_ = true
	assert.Equal(0, len(ref.HttpsHostName()))
	logger := remoteClusterSvc.logger
	err := setHostNamesAndSecuritySettings(logger, remoteClusterSvc.utils, ref, xdcrTopologyMock.IsMyClusterEncryptionLevelStrict())
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

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	utilsMockFunc := func() {
		setupUtilsSSL2(utilitiesMock)
	}

	idAndName := "test"

	ref := createRemoteClusterReference(idAndName)

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)

	ref.EncryptionType_ = metadata.EncryptionType_Full
	ref.DemandEncryption_ = true
	assert.Equal(0, len(ref.HttpsHostName()))
	logger := remoteClusterSvc.logger

	err := setHostNamesAndSecuritySettings(logger, remoteClusterSvc.utils, ref, xdcrTopologyMock.IsMyClusterEncryptionLevelStrict())
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
	assert.False(capability.HasCollectionSupport())

	cloneTest := capability.Clone()
	assert.False(cloneTest.HasCollectionSupport())

	on70poolsDefault, err := getPoolsDefault70()
	assert.Nil(err)
	err = capability.LoadFromDefaultPoolInfo(on70poolsDefault, nil /* logger*/)
	assert.Nil(err)
	assert.True(capability.HasCollectionSupport())

	cloneTest = capability.Clone()
	assert.True(cloneTest.HasCollectionSupport())

	fmt.Println("============== Test case end: TestCapabilityStruct =================")
}

func TestCapabilityChange(t *testing.T) {
	fmt.Println("============== Test case start: TestCapabilityChange =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", 0, nil, true, false, true, clusterMadHatter, dummyHostNameList)
	}

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)

	err := remoteClusterSvc.AddRemoteCluster(ref, true /*skipConnectivity*/)
	assert.Nil(err)
	capability, err := remoteClusterSvc.GetCapability(ref)
	assert.Nil(err)
	// Madhatter does not have collections support
	assert.False(capability.HasCollectionSupport())
	refListChanged, err := remoteClusterSvc.GetRefListForRestartAndClearState()
	assert.Nil(err)
	assert.Equal(0, len(refListChanged))

	agent, _, _ := remoteClusterSvc.getOrStartNewAgent(ref, false /*userInitiated*/, false /*updateFromRef*/)
	assert.NotNil(agent)
	agent.unitTestBypassMetaKV = true

	// Now pretend target cluster upgraded to Cheshire Cat
	_, _, _, utilitiesMockNew, _ := setupBoilerPlateRCS()
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
	assert.True(capability.HasCollectionSupport())
	refListChanged, err = remoteClusterSvc.GetRefListForRestartAndClearState()
	assert.Nil(err)
	assert.Equal(1, len(refListChanged))
	fmt.Println("============== Test case start: TestCapabilityChange =================")
}

func TestInitializeWithExternalModeAndRefreshPullsExtNodeList(t *testing.T) {
	fmt.Println("============== Test case start: TestInitializeWithExternalModeAndRefreshPullsExtNodeList =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReferenceExtOnly(idAndName)

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", 0, nil, true, true, true, clusterMadHatter, dummyHostNameList)
	}

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", 0, nil, true, true, true, clusterMadHatter, dummyHostNameList)
	}

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", 0, nil, true, false, true, clusterMadHatter, dummyHostNameList)
	}

	idAndName := "test"

	ref := createRemoteClusterReferenceIntOnly(idAndName)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// This is to grab the external node list info populated from actual mock data
	assert.NotEqual(0, len(nodeListWithMinInfo))
	assert.NotEqual(0, len(externalNodeListWithMinInfo))
	ref.HostName_ = externalNodeListWithMinInfo[0]

	// resetup utilitiesMock knowing this info
	_, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
	setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, ref.HostName_, -1, base.ErrorNoPortNumber, true, false, true,
		clusterMadHatter, dummyHostNameList)
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
	_, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
	setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", -1, base.ErrorResourceDoesNotExist, false, false, true,
		clusterMadHatter, dummyHostNameList)
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

	// flag should not have been raised and no reference returned
	refList, _ := remoteClusterSvc.GetRefListForRestartAndClearState()
	assert.Equal(0, len(refList))
	fmt.Println("============== Test case end: TestAddressPreferenceChangeWithDefaultMode =================")
}

func TestDNSSrv(t *testing.T) {
	fmt.Println("============== Test case start: TestDNSSrv =================")

	// Test to see if above set up has been done correctly
	cname, addrs, err := net.LookupSRV("", "", dnsSrvHostname)
	if err != nil {
		fmt.Printf("Local DNS look up failed for %v, skipping DNSSRV unit test\n", dnsSrvHostname)
		return
	}
	fmt.Printf("Lookup cname: %v, addrs: %v, err: %v\n", cname, addrs, err)
	for _, addr := range addrs {
		fmt.Printf("Address: %v port: %v, priority: %v weight: %v\n", addr.Target, addr.Port, addr.Priority, addr.Weight)
	}

	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
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

	srvHelper.On("DnsSrvLookup", dnsSrvHostname).Return(srvList, base2.SrvRecordsNonSecure, nil)

	idAndName := "test"
	ref := createRemoteClusterReferenceDNSSRV(idAndName, srvHelper)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)

	agent, alreadyExists, err := remoteClusterSvc.getOrStartNewAgent(ref, false /*userInitiated*/, true /*updateFromRef*/)
	assert.Nil(err)
	assert.False(alreadyExists)
	agent.unitTestBypassMetaKV = true

	agent.waitForRefreshEnabled()
	agent.refMtx.RLock()
	assert.True(agent.reference.IsDnsSRV())
	srvHostNames := agent.reference.GetSRVHostNames()
	// GetSrvHostName will always use standard port for now. See GetTargetConnectionString()
	dnsRemotenodeHostname = "192.168.0.1:8091"
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
	badSRVHelper.On("DnsSrvLookup", mock.Anything).Return(emptyList, base2.SrvRecordsInvalid, fmt.Errorf("Dummy"))
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

func TestConnHelper(t *testing.T) {
	fmt.Println("============== Test case start: TestConnHelper =================")
	assert := assert.New(t)

	helper := NewConnectivityHelper(100 * time.Millisecond)
	nodeName := "testNode0"
	dummyHeartbeatMap := make(map[string]base.HeartbeatStatus)
	dummyHeartbeatMap[nodeName] = base.HeartbeatHealthy
	helper.MarkNodeHeartbeatStatus(nodeName, dummyHeartbeatMap)
	helper.MarkNode("testNode0", metadata.ConnValid)
	helper.MarkNode("testNode1", metadata.ConnValid)
	assert.Equal(metadata.ConnValid, helper.GetOverallStatus())

	time.Sleep(120 * time.Millisecond)
	dummyHeartbeatMap[nodeName] = base.HeartbeatUnhealthy
	helper.MarkNodeHeartbeatStatus(nodeName, dummyHeartbeatMap)
	assert.Equal(metadata.ConnDegraded, helper.GetOverallStatus())
	time.Sleep(120 * time.Millisecond)

	helper.MarkNode("testNode1", metadata.ConnError)
	assert.Equal(metadata.ConnDegraded, helper.GetOverallStatus())

	helper.MarkNode("testNode0", metadata.ConnAuthErr)
	assert.Equal(metadata.ConnAuthErr, helper.GetOverallStatus())

	// Test that if one node is good, one node has connection error, overall state is degraded
	helper.MarkNode("testNode0", metadata.ConnValid)
	helper.MarkNode("testNode1", metadata.ConnValid)
	assert.Equal(metadata.ConnValid, helper.GetOverallStatus())
	helper.MarkNode("testNode1", metadata.ConnError)
	helper.SyncWithValidList(base.StringPairList{base.StringPair{"testNode0", ""}, base.StringPair{"testNode1", ""}})
	assert.Equal(metadata.ConnDegraded, helper.GetOverallStatus())

	fmt.Println("============== Test case end: TestConnHelper =================")
}

func TestHeartbeatCleanup(t *testing.T) {
	fmt.Println("============== Test case start: TestHeartbeatCleanup =================")
	defer fmt.Println("============== Test case end: TestHeartbeatCleanup =================")
	assert := assert.New(t)

	// See if after 1 second, the map is cleaned up
	helper := NewConnectivityHelper(100 * time.Millisecond)
	nodeName := "dummyNode"
	dummyHeartbeatMap := make(map[string]base.HeartbeatStatus)
	dummyHeartbeatMap[nodeName] = base.HeartbeatHealthy
	helper.MarkNodeHeartbeatStatus(nodeName, dummyHeartbeatMap)
	helper.mtx.RLock()
	assert.Equal(1, len(helper.nodeHeartbeatStatus))
	assert.Equal(1, len(helper.nodeHeartbeatCleanupMap))
	helper.mtx.RUnlock()
	time.Sleep(200 * time.Millisecond)

	helper.mtx.RLock()
	assert.Equal(0, len(helper.nodeHeartbeatStatus))
	assert.Equal(0, len(helper.nodeHeartbeatCleanupMap))
	helper.mtx.RUnlock()
}

func setupUtilsAdminBlockedAndSSLIsOK(utilitiesMock *utilsMock.UtilsIface) {
	// Simulate time it takes for the network to respond saying it's blocked
	blockedTime := 500 * time.Millisecond
	utilitiesMock.On("GetClusterInfo", hostname, base.DefaultPoolPath, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Run(func(args mock.Arguments) { time.Sleep(blockedTime) }).Return(nil, fmt.Errorf("AdminPortBlocked"))
	utilitiesMock.On("GetExternalMgtHostAndPort", mock.Anything, mock.Anything).Return("", -1, base.ErrorNoPortNumber)

	// Default port blocks TLS handshake but can return internal SSL port
	utilitiesMock.On("HttpsRemoteHostAddr", hostname, mock.Anything).Return(hostnameSSL, "", nil)

	defaultPoolInfo, _ := getClusterInfoMockInt()
	// SSL port is ok to get it
	utilitiesMock.On("GetSecuritySettingsAndDefaultPoolInfo", hostname, hostnameSSL, mock.Anything, mock.Anything, dummySelfSignedCert, mock.Anything, mock.Anything, false, mock.Anything).Return(base.HttpAuthMechHttps, defaultPoolInfo, http.StatusOK, nil)
	// Non-SSL port on security default pool, block it
	utilitiesMock.On("GetSecuritySettingsAndDefaultPoolInfo", hostname, hostname, mock.Anything, mock.Anything, dummySelfSignedCert, mock.Anything, mock.Anything, false, mock.Anything).Run(func(arguments mock.Arguments) { time.Sleep(blockedTime) }).Return(base.HttpAuthMechHttps, nil, http.StatusOK, fmt.Errorf("Blocked"))

	// These are proxy calls to actual real utils
	nodeList, _ := testUtils.GetNodeListFromInfoMap(defaultPoolInfo, nil)
	utilitiesMock.On("GetNodeListFromInfoMap", mock.Anything, mock.Anything).Return(nodeList, nil)
	nodeAddressesList, _ := testUtils.GetRemoteNodeAddressesListFromNodeList(nodeList, hostname, true, nil, false)
	utilitiesMock.On("GetRemoteNodeAddressesListFromNodeList", nodeList, hostname, true, mock.Anything, false).Return(nodeAddressesList, nil)
	clusterInfo, _ := getPools70()
	utilitiesMock.On("GetClusterInfoWStatusCode", hostnameSSL, base.PoolsPath, mock.Anything, mock.Anything, base.HttpAuthMechHttps, dummySelfSignedCert, true, mock.Anything, mock.Anything, mock.Anything).Return(clusterInfo, nil, http.StatusOK)
	utilitiesMock.On("StartDiagStopwatch", mock.Anything, mock.Anything).Return(func() time.Duration { return 0 })
}

func TestParallelSecureGet(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestParallelSecureGet =================")
	defer fmt.Println("============== Test case done: TestParallelSecureGet =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	// 8091 is blocked but 18091 is ok
	utilsMockFunc := func() {
		setupUtilsAdminBlockedAndSSLIsOK(utilitiesMock)
	}

	idAndName := "test"

	ref := createSSLRemoteClusterRef(idAndName, nil)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)

	assert.Nil(remoteClusterSvc.validateAddRemoteCluster(ref, false))
}

func setupUtilsBothAdminPortsBlocked(utilitiesMock *utilsMock.UtilsIface) {
	// Simulate time it takes for the network to respond saying it's blocked
	blockedTime := 500 * time.Millisecond
	utilitiesMock.On("GetClusterInfo", hostname, base.DefaultPoolPath, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Run(func(args mock.Arguments) { time.Sleep(blockedTime) }).Return(nil, fmt.Errorf("AdminPortBlocked"))
	utilitiesMock.On("GetExternalMgtHostAndPort", mock.Anything, mock.Anything).Return("", -1, base.ErrorNoPortNumber)

	// Default port blocks TLS handshake but can return internal SSL port
	utilitiesMock.On("HttpsRemoteHostAddr", hostname, mock.Anything).Return(hostnameSSL, "", nil)

	// Block both
	utilitiesMock.On("GetSecuritySettingsAndDefaultPoolInfo", hostname, hostnameSSL, mock.Anything, mock.Anything, dummySelfSignedCert, mock.Anything, mock.Anything, false, mock.Anything).Return(base.HttpAuthMechHttps, nil, http.StatusOK, fmt.Errorf("Blocked"))
	utilitiesMock.On("GetSecuritySettingsAndDefaultPoolInfo", hostname, hostname, mock.Anything, mock.Anything, dummySelfSignedCert, mock.Anything, mock.Anything, false, mock.Anything).Run(func(arguments mock.Arguments) { time.Sleep(blockedTime) }).Return(base.HttpAuthMechHttps, nil, http.StatusOK, fmt.Errorf("Blocked"))
	utilitiesMock.On("StartDiagStopwatch", mock.Anything, mock.Anything).Return(func() time.Duration { return 0 })
}

func TestParallelSecureGetFail(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestParallelSecureGetFail =================")
	defer fmt.Println("============== Test case done: TestParallelSecureGetFail =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	// 8091 and 18091 both blocked
	utilsMockFunc := func() {
		setupUtilsBothAdminPortsBlocked(utilitiesMock)
	}

	idAndName := "test"

	ref := createSSLRemoteClusterRef(idAndName, nil)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)

	assert.NotNil(remoteClusterSvc.validateAddRemoteCluster(ref, false))

	agent, _, err := remoteClusterSvc.getOrStartNewAgent(ref, true, false)
	assert.Nil(err)
	assert.NotNil(agent)

	nodeList, err := getCloudNodeList()
	assert.Nil(err)
	assert.NotNil(nodeList)

	agent.utils = testUtils
	hostname := "cb-0000.ead75783-adbe-42ac-8937-e187da9abf80.dp.cloud.couchbase.com"
	isExternal, err := agent.checkIfHostnameIsAlternate(nodeList, hostname, true, nil)
	assert.Nil(err)
	assert.True(isExternal)

	hostname = "cb-0000.ead75783-adbe-42ac-8937-e187da9abf80.dp.cloud.couchbase.com:8091"
	isExternal, err = agent.checkIfHostnameIsAlternate(nodeList, hostname, true, nil)
	assert.Nil(err)
	assert.True(isExternal)
}

// Given a DNS SRV with a list of SRV entries, retrieve external intent
func TestDNSSRVReturnAlternate(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestDNSSRVReturnAlternate =================")
	defer fmt.Println("============== Test case done: TestDNSSRVReturnAlternate =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	// 8091 and 18091 both blocked
	utilsMockFunc := func() {
		setupUtilsBothAdminPortsBlocked(utilitiesMock)
	}

	idAndName := "test"

	ref := createSSLRemoteClusterRef(idAndName, nil)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)

	assert.NotNil(remoteClusterSvc.validateAddRemoteCluster(ref, false))

	agent, _, err := remoteClusterSvc.getOrStartNewAgent(ref, true, false)
	assert.Nil(err)
	assert.NotNil(agent)

	// agent to use a real utils for the following test
	agent.utils = testUtils

	nodeList, err := getCloudNodeList()
	assert.Nil(err)
	assert.NotNil(nodeList)

	// Note 8091 is tagged on automatically
	srvName := "ead75783-adbe-42ac-8937-e187da9abf80.dp.cloud.couchbase.com:8091"
	srvNodeList := []string{
		"cb-0000.ead75783-adbe-42ac-8937-e187da9abf80.dp.cloud.couchbase.com:18091",
		"cb-0001.ead75783-adbe-42ac-8937-e187da9abf80.dp.cloud.couchbase.com:18091",
		"cb-0002.ead75783-adbe-42ac-8937-e187da9abf80.dp.cloud.couchbase.com:18091",
	}
	isExteranal, err := agent.checkIfHostnameIsAlternate(nodeList, srvName, true, srvNodeList)
	assert.Nil(err)
	assert.True(isExteranal)
}

func newConnectivityHelperConnStatus(status metadata.ConnectivityStatus) *service_def.ConnectivityHelperSvc {
	connectivityHelperMock := &service_def.ConnectivityHelperSvc{}
	connectivityHelperMock.On("GetOverallStatus").Return(status)
	connectivityHelperMock.On("SyncWithValidList", mock.Anything).Return()
	return connectivityHelperMock
}

func TestAgentGetConnectivityStatusForPipelinePause(t *testing.T) {
	fmt.Println("============== Test case start: TestAgentGetConnectivityStatusForPipelinePause =================")
	defer fmt.Println("============== Test case end: TestAgentGetConnectivityStatusForPipelinePause =================")
	assert := assert.New(t)

	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	utilsMockFunc := func() {
		setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, "", 0, nil, true, false, true, clusterMadHatter, dummyHostNameList)
	}

	idAndName := "test"

	ref := createRemoteClusterReferenceIntOnly(idAndName)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)

	// This is to grab the external node list info populated from actual mock data
	assert.NotEqual(0, len(nodeListWithMinInfo))
	assert.NotEqual(0, len(externalNodeListWithMinInfo))
	ref.HostName_ = externalNodeListWithMinInfo[0]

	// resetup utilitiesMock knowing this info
	_, _, _, utilitiesMock, _ = setupBoilerPlateRCS()
	setupUtilsMockSpecific(utilitiesMock, 0*time.Second, nil, ref.HostName_, -1, base.ErrorNoPortNumber, true, false, true,
		clusterMadHatter, dummyHostNameList)
	remoteClusterSvc.updateUtilities(utilitiesMock)

	// First bootstrap agent with a reference that refers to the external hostname
	agent, alreadyExists, err := remoteClusterSvc.getOrStartNewAgent(ref, false /*userInitiated*/, true /*updateFromRef*/)
	assert.Nil(err)
	assert.False(alreadyExists)
	agent.unitTestBypassMetaKV = true

	// Helper first returns everything healthy
	agent.connectivityHelper = newConnectivityHelperConnStatus(metadata.ConnValid)

	// Should not report unhealthy
	assert.False(agent.GetUnreportedAuthError())

	// Now cluster auth is bad
	agent.connectivityHelper = newConnectivityHelperConnStatus(metadata.ConnAuthErr)

	// Should report unhealthy for one time only
	assert.True(agent.GetUnreportedAuthError())
	assert.False(agent.GetUnreportedAuthError())

	// Now cluster auth is good again
	agent.connectivityHelper = newConnectivityHelperConnStatus(metadata.ConnValid)

	// should not report
	assert.False(agent.GetUnreportedAuthError())

}

// When a non-user initiated node receives a remote cluster add from a user-initiated node, it executes refresh
// in the bg. In the meantime, any querying on this non-user initiated node should return valid info
func TestPassiveAddAndImmediateGet(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPassiveAddAndImmediateGet =================")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	ref := createRemoteClusterReference(idAndName)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 200*time.Millisecond /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)

	agent, existed, err := remoteClusterSvc.getOrStartNewAgent(ref, false /*userInitiated*/, true /*updateFromRef*/)
	assert.False(existed)
	assert.Nil(err)
	assert.NotNil(agent)

	rcMap, err := remoteClusterSvc.RemoteClusters()
	assert.Nil(err)
	assert.False(agent.InitDone())
	for _, v := range rcMap {
		outputMap := v.ToMap()
		connectivityCheck := outputMap[base.ConnectivityStatus].(string)
		assert.NotEqual(metadata.ConnError.String(), connectivityCheck)
		assert.Equal(metadata.ConnIniting.String(), connectivityCheck)
		rcNameCheck := outputMap[base.RemoteClusterName].(string)
		assert.Equal(idAndName, rcNameCheck)
	}

	// After init done, status should be valid
	time.Sleep(1 * time.Second)

	assert.True(agent.InitDone())
	rcMap, err = remoteClusterSvc.RemoteClusters()
	assert.Nil(err)
	for _, v := range rcMap {
		outputMap := v.ToMap()
		connectivityCheck := outputMap[base.ConnectivityStatus].(string)
		assert.Equal(metadata.ConnValid.String(), connectivityCheck)
		rcNameCheck := outputMap[base.RemoteClusterName].(string)
		assert.Equal(idAndName, rcNameCheck)
	}
	fmt.Println("============== Test case end: TestPassiveAddAndImmediateGet =================")
}

func TestCreateRemoteWithIpFamilyV4Blocked(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCreateRemoteWithIpFamilyV4Blocked ===============")
	defer fmt.Println("============== Test case End: TestCreateRemoteWithIpFamilyV4Blocked ===============")

	base.NetTCP = base.TCP6 // This blocks IPV4
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	srvHelper := &baseMock.DnsSrvHelperIface{}

	var emptyList []*net.SRV
	srvHelper.On("DnsSrvLookup", mock.Anything).Return(emptyList, base2.SrvRecordsInvalid, nil)

	// Test when ipv4 is blocked, create remote with ipv4 address 127.0.0.1:9000 is not allowed
	idAndName := "test"
	ref := createRemoteClusterRefWithHost(idAndName, "127.0.0.1:9000", srvHelper, assert)
	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)
	err := remoteClusterSvc.validateAddRemoteCluster(ref, false)
	assert.NotNil(err)
	if err != nil {
		assert.Contains(err.Error(), "not allowed")
	}

	// Test when ipv4 is blocked, create remote with ipv4 address 127.0.0.1 is not allowed
	ref = createRemoteClusterRefWithHost(idAndName, "127.0.0.1", srvHelper, assert)
	utilsMockFunc = func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)
	err = remoteClusterSvc.validateAddRemoteCluster(ref, false)
	assert.NotNil(err)
	if err != nil {
		assert.Contains(err.Error(), "not allowed")
	}

	// Test when ipv4 is blocked, create remote with hostname which maps to ipv4 is not allowed
	// Comment out this part since it assume that hostname can be resolved to a ipv4 and ipv4 only
	//hostname, err := os.Hostname()
	//if err != nil {
	//	fmt.Printf("os.Hostname() failed with error %v. Skiping create remote ref with hostname test\n", err)
	//} else {
	//	ref = createRemoteClusterRefWithHost(idAndName, hostname, srvHelper, assert)
	//	utilsMockFunc = func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }
	//	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
	//		remoteClusterSvc, ref, utilsMockFunc)
	//	err = remoteClusterSvc.validateAddRemoteCluster(ref, false)
	//	assert.NotNil(err)
	//	if err != nil {
	//		assert.Contains(err.Error(), "Cannot find address in the ip family")
	//	}
	//}
	base.NetTCP = base.TCP // This restores to support both IPV4/IPV6
}

func TestCreateRemoteWithIpFamilyV6Blocked(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCreateRemoteWithIpFamilyV6Blocked ===============")
	defer fmt.Println("============== Test case End: TestCreateRemoteWithIpFamilyV6Blocked ===============")
	base.NetTCP = base.TCP4 // This blocks IPV6
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	srvHelper := &baseMock.DnsSrvHelperIface{}

	var emptyList []*net.SRV
	srvHelper.On("DnsSrvLookup", mock.Anything).Return(emptyList, base2.SrvRecordsInvalid, nil)

	// Test when ipv6 is blocked, create remote with ipv6 address [::FFFF:C0A8:1] is not allowed
	idAndName := "test"
	ref := createRemoteClusterRefWithHost(idAndName, "[::FFFF:C0A8:1]", srvHelper, assert)
	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)
	err := remoteClusterSvc.validateAddRemoteCluster(ref, false)
	assert.NotNil(err)
	if err != nil {
		assert.Contains(err.Error(), "is not allowed")
	}
	base.NetTCP = base.TCP // This restores to support both IPV4/IPV6
}

func TestStalledAddClusterRefFromCallback(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestStalledAddClusterRefFromCallback =================")
	defer fmt.Println("============== Test case End: TestStalledAddClusterRefFromCallback ===============")
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	idAndName := "test"
	revision := []byte{1}
	ref := createRemoteClusterReferenceWithRev(idAndName, revision)
	_, jsonMarshalBytes := jsonMarshalWrapper(ref)

	// First set the callback before creating agents
	remoteClusterSvc.SetMetadataChangeHandlerCallback(testCallbackIncrementCount)

	utilsMockFunc := func() { setupUtilsMockGeneric(utilitiesMock, 0 /*networkDelay*/) }

	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)
	assert.Equal(0, remoteClusterSvc.getNumberOfAgents())
	assert.Nil(remoteClusterSvc.RemoteClusterServiceCallback("/test", jsonMarshalBytes, revision))
	assert.Equal(1, remoteClusterSvc.getNumberOfAgents())

	agent, _, _ := remoteClusterSvc.getOrStartNewAgent(ref, false, false)

	ref2 := ref.Clone()
	// Set op via callback
	ref2.SetHostName("newHostName")
	ref2.SetUserName("newUserName")
	revision = []byte{2}
	ref2.SetRevision(revision)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	// The following goroutine represents the stalled goroutine
	// hence the sleep
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Second)
		ret := agent.UpdateReferenceFrom(ref2, false)
		assert.Error(ret)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Second)
		ret := agent.UpdateReferenceFrom(ref2, false)
		assert.Error(ret)
	}()

	wg.Wait()
}

// When an SRV entry is used and none of the A records are accessible
// and the setting to re-bootstrap is used, then the the agent should re-query
// the SRV record again for new A-records
func TestRefreshSRVRebootstrap(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCreateRemoteWithIpFamilyV6Blocked ===============")
	defer fmt.Println("============== Test case End: TestCreateRemoteWithIpFamilyV6Blocked ===============")

	var unitTestRefreshInterval = 1 * time.Second

	// Let refresh temporarily be a shorter period
	oldRefreshInt := base.RefreshRemoteClusterRefInterval
	base.RefreshRemoteClusterRefInterval = unitTestRefreshInterval
	// force bootstrap to be true
	oldBootstrap := base.DNSSrvReBootstrap
	base.DNSSrvReBootstrap = true
	defer func() {
		base.RefreshRemoteClusterRefInterval = oldRefreshInt
		base.DNSSrvReBootstrap = oldBootstrap
	}()

	// First use a valid bootstrap for DNS SRV
	uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		utilitiesMock, remoteClusterSvc := setupBoilerPlateRCS()

	// Force rebootStrap to be true
	//remoteClusterSvc.dnsSrvReBootstrap = true

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

	srvHelper.On("DnsSrvLookup", dnsSrvHostname).Return(srvList, base2.SrvRecordsNonSecure, nil)

	idAndName := "test"
	ref := createRemoteClusterReferenceDNSSRV(idAndName, srvHelper)
	setupMocksRCS(uiLogSvcMock, metadataSvcMock, xdcrTopologyMock,
		remoteClusterSvc, ref, utilsMockFunc)

	agent, alreadyExists, err := remoteClusterSvc.getOrStartNewAgent(ref, false /*userInitiated*/, true /*updateFromRef*/)
	assert.Nil(err)
	assert.False(alreadyExists)
	agent.unitTestBypassMetaKV = true

	agent.waitForRefreshEnabled()
	agent.refMtx.RLock()
	assert.True(agent.reference.IsDnsSRV())
	srvHostNames := agent.reference.GetSRVHostNames()
	// GetSrvHostName will always use standard port for now. See GetTargetConnectionString()
	dnsRemotenodeHostname = "192.168.0.1:8091"
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

	// Now let's pretend the SRV record points to a completely different set of A records
	srvHelper2 := &baseMock.DnsSrvHelperIface{}
	srvEntry2 := &net.SRV{
		Target: "192.168.0.2.",
		Port:   9002,
	}
	var srvList2 []*net.SRV
	srvList2 = append(srvList2, srvEntry2)
	srvHelper2.On("DnsSrvLookup", dnsSrvHostname).Return(srvList2, base2.SrvRecordsNonSecure, nil)

	agent.refMtx.Lock()
	agent.reference.UnitTestSetSRVHelper(srvHelper2)
	agent.pendingRef.UnitTestSetSRVHelper(srvHelper2)
	agent.refMtx.Unlock()

	_, _, _, utilitiesMockErr, _ := setupBoilerPlateRCS()
	forcedErr := fmt.Errorf("Unit test force error when refreshing")
	setupUtilsMockSpecific(utilitiesMockErr, 0*time.Second, forcedErr, "", 0, nil, true, false,
		false, clusterMadHatter, dnsValidHostNameList)
	remoteClusterSvc.updateUtilities(utilitiesMockErr)

	// Wait until the periodic refresh hits and utils returns an error
	// Then it should trigger a re-bootstrap of SRV entries
	agent.reference.PopulateDnsSrvIfNeeded(nil)
	time.Sleep(unitTestRefreshInterval + 100*time.Millisecond)

	agent.refMtx.RLock()
	assert.True(agent.reference.IsDnsSRV())
	srvHostNames = agent.reference.GetSRVHostNames()
	// GetSrvHostName will always use standard port for now. See GetTargetConnectionString()
	dnsRemotenodeHostname = "192.168.0.2:8091"
	agent.refMtx.RUnlock()
	found = false
	for _, host := range srvHostNames {
		if host == dnsRemotenodeHostname {
			found = true
			break
		}
	}
	assert.True(found)
	assert.Equal(dnsSrvHostname, ref.HostName())
	assert.Equal(dnsSrvHostname, agent.reference.HostName())
	assert.Equal("", agent.reference.ActiveHostName())

	// Restore a valid utils
	remoteClusterSvc.updateUtilities(utilitiesMock)
	// Wait for a valid refresh
	time.Sleep(unitTestRefreshInterval + 100*time.Millisecond)
	// Ensure activeHostname is repopulated
	assert.NotEqual("", agent.reference.ActiveHostName())
}

func TestConnectivityHelper_SyncWithValidList(t *testing.T) {
	tests := []struct {
		name     string
		c        *ConnectivityHelper
		nodeList base.StringPairList
	}{
		{
			name: "completely changed target topology",
			nodeList: base.StringPairList{
				{"node1", ""},
				{"node2", ""},
				{"node3", ""},
			},
			c: &ConnectivityHelper{
				nodeStatus: map[string]metadata.ConnectivityStatus{
					"node4": 1,
					"node5": 1,
					"node6": 1,
				},
			},
		},
		{
			name: "new nodes added to target topology",
			nodeList: base.StringPairList{
				{"node4", ""},
				{"node5", ""},
				{"node6", ""},
				{"node1", ""},
				{"node2", ""},
			},
			c: &ConnectivityHelper{
				nodeStatus: map[string]metadata.ConnectivityStatus{
					"node4": 1,
					"node5": 1,
					"node6": 1,
				},
			},
		},
		{
			name: "some nodes removed from target topology",
			nodeList: base.StringPairList{
				{"node6", ""},
			},
			c: &ConnectivityHelper{
				nodeStatus: map[string]metadata.ConnectivityStatus{
					"node4": 1,
					"node5": 1,
					"node6": 1,
				},
			},
		},
		{
			name: "some nodes added and some removed from target topology",
			nodeList: base.StringPairList{
				{"node6", ""},
				{"node7", ""},
				{"node8", ""},
			},
			c: &ConnectivityHelper{
				nodeStatus: map[string]metadata.ConnectivityStatus{
					"node4": 1,
					"node5": 1,
					"node6": 1,
				},
			},
		},
		{
			name: "no change target topology",
			nodeList: base.StringPairList{
				{"node4", ""},
				{"node5", ""},
				{"node6", ""},
			},
			c: &ConnectivityHelper{
				nodeStatus: map[string]metadata.ConnectivityStatus{
					"node4": 1,
					"node5": 1,
					"node6": 1,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			tt.c.SyncWithValidList(tt.nodeList)

			var list1, list2 []string
			for _, node := range tt.nodeList {
				list1 = append(list1, node.GetFirstString())
			}

			for node := range tt.c.nodeStatus {
				list2 = append(list2, node)
			}

			sort.Strings(list1)
			sort.Strings(list2)

			assert.Equal(t, len(list1), len(list2))
			for i := 0; i < len(list1) && i < len(list2); i++ {
				assert.Equal(t, list1[i], list2[i])
			}

		})
	}
}
