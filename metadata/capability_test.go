/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadata

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testExtDataDir = "../utils/testExternalData/"

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

func getDefaultPoolInfoFromFile(filename string) (map[string]interface{}, error) {
	fileName := fmt.Sprintf("%v%v", testExtDataDir, filename)
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

func Test651Capability(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: Test651Capability =================")
	defer fmt.Println("============== Test case done: Test651Capability =================")

	cloudDefaultPool, err := getDefaultPoolInfoFromFile("cloudDefaultPoolInfo.json")
	if err != nil {
		panic(err)
	}
	assert.NotNil(cloudDefaultPool)

	var capability Capability
	capability.LoadFromDefaultPoolInfo(cloudDefaultPool, nil)

	assert.False(capability.HasCollectionSupport())
}

func TestIsClusterEnterpriseForAllCommunity(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestIsClusterEnterpriseForAllCommunity =================")
	defer fmt.Println("============== Test case done: TestIsClusterEnterpriseForAllCommunity =================")

	defaultPoolInfo, err := getDefaultPoolInfoFromFile("cloudDefaultPoolInfoCommunityVersion.json")
	assert.Nil(err)
	assert.NotNil(defaultPoolInfo)

	var cap Capability
	err = cap.LoadFromDefaultPoolInfo(defaultPoolInfo, nil)
	assert.Nil(err)
	assert.True(cap.HasInitialized())
	assert.False(cap.IsClusterEnterprise(), "all-CE cluster should not be treated as Enterprise")
}

func TestIsClusterEnterpriseForAllEnterprise(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestIsClusterEnterpriseForAllEnterprise =================")
	defer fmt.Println("============== Test case done: TestIsClusterEnterpriseForAllEnterprise =================")

	defaultPoolInfo, err := getDefaultPoolInfoFromFile("cloudDefaultPoolInfo.json")
	assert.Nil(err)
	assert.NotNil(defaultPoolInfo)

	var cap Capability
	err = cap.LoadFromDefaultPoolInfo(defaultPoolInfo, nil)
	assert.Nil(err)
	assert.True(cap.HasInitialized())
	assert.True(cap.IsClusterEnterprise(), "all-EE cluster should be treated as Enterprise")
}

func TestIsClusterEnterpriseForMixedCluster(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestIsClusterEnterpriseForMixedCluster =================")
	defer fmt.Println("============== Test case done: TestIsClusterEnterpriseForMixedCluster =================")

	defaultPoolInfo, err := getDefaultPoolInfoFromFile("cloudDefaultPoolInfoMixedVersion.json")
	assert.Nil(err)
	assert.NotNil(defaultPoolInfo)

	var cap Capability
	err = cap.LoadFromDefaultPoolInfo(defaultPoolInfo, nil)
	assert.Nil(err)
	assert.True(cap.HasInitialized())
	assert.False(cap.IsClusterEnterprise(), "mixed CE/EE cluster should be treated as CE")
}

func TestIsClusterEnterpriseForNoNodesResponse(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestIsClusterEnterpriseForNoNodesResponse =================")
	defer fmt.Println("============== Test case done: TestIsClusterEnterpriseForNoNodesResponse =================")

	defaultPoolInfo, err := getDefaultPoolInfoFromFile("cloudDefaultPoolInfoNoNodes.json")
	assert.Nil(err)
	assert.NotNil(defaultPoolInfo)

	var cap Capability
	err = cap.LoadFromDefaultPoolInfo(defaultPoolInfo, nil)
	assert.NotNil(err, "parsing a defaultPoolInfo map with empty nodes-list should throw an error")
	assert.False(cap.HasInitialized())
	assert.False(cap.IsClusterEnterprise())
}
