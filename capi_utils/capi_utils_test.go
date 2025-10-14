/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package capi_utils

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/utils"
	"github.com/stretchr/testify/assert"
)

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

func TestPreReplicateMapWithTerseBucketInfo(t *testing.T) {
	fmt.Println("============== Test case start: TestPreReplicateMapWithTerseBucketInfo =================")
	defer fmt.Println("============== Test case end: TestPreReplicateMapWithTerseBucketInfo =================")
	assert := assert.New(t)

	// Test parameters needed to use terseInfo
	useExternal := true
	hostAddr := "pe.vxzqmnl0syi3kpwn.aws-guardians.nonprod-project-avengers.com:1801"
	bucketName := "B1"
	testUtils := utils.NewUtilities()
	ref := metadata.RemoteClusterReference{
		HostName_:     hostAddr,
		HttpAuthMech_: base.HttpAuthMechHttps,
	}

	// 1. Positive case - pools_default_b_gcp_private_links.json (GCP private endpoints)
	testExternalDataDir := "./../utils/testExternalData/"
	positiveFiles := []string{
		fmt.Sprintf("%v%v", testExternalDataDir, "alternateAddrKVSSL/pools_default_b_gcp_private_links.json"),
	}

	for _, fileName := range positiveFiles {
		bucketInfo, err := readJsonHelperForTest(fileName)
		assert.Nil(err)

		isTerse, err := testUtils.IsTerseBucketInfo(bucketInfo)
		assert.Nil(err)
		assert.True(isTerse)

		kvVBMap, err := testUtils.GetRemoteServerVBucketsMap(hostAddr, bucketName, bucketInfo, useExternal)
		assert.Nil(err)

		preReplicateMap, err := ConstructCapiServiceEndPointMap(bucketName, bucketInfo, &ref, testUtils, false, useExternal)
		assert.Nil(err)

		assert.Equal(len(kvVBMap), len(preReplicateMap))
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

	// 2. Negative cases - rest all of the valid non-terse bucketInfo files in the goxdcr repository.
	privateLinksSetupFiles := []string{
		fmt.Sprintf("%v%v", testExternalDataDir, "pools_default_buckets_gcp_private_links.json"),
		fmt.Sprintf("%v%v", testExternalDataDir, "privateEndPoints.json"),
		fmt.Sprintf("%v%v", testExternalDataDir, "privateEndPoints1.json"),
	}

	for _, fileName := range jsonFiles {
		var skip bool
		for _, privateLinksSetupFile := range privateLinksSetupFiles {
			if strings.Contains(fileName, privateLinksSetupFile) {
				// private links case which is already tested.
				skip = true
				break
			}
		}

		if skip {
			fmt.Printf("Skipping file %s as it is a negative test: %v\n", filepath.Base(fileName), err)
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

		if !isTerse {
			fmt.Printf("Skipping file %s as it not terseBucketInfo: %v\n", filepath.Base(fileName), err)
			continue
		}

		kvVBMap, err := testUtils.GetRemoteServerVBucketsMap(hostAddr, bucketName, bucketInfo, useExternal)
		assert.Nil(err)

		preReplicateMap, err := ConstructCapiServiceEndPointMap(bucketName, bucketInfo, &ref, testUtils, false, useExternal)
		assert.Nil(err)

		assert.Equal(len(kvVBMap), len(preReplicateMap))
	}
}
