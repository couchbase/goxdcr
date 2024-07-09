// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package peerToPeer

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/goxdcr/v8/metadata"
	service_def "github.com/couchbase/goxdcr/v8/service_def/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"io/ioutil"
	"testing"
	"time"
)

const tgtBucketName = "tgtBucket"
const testInternalDataDir = "../utils/testInternalData/"

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

func getBucketInfoWithReplicas() map[string]interface{} {
	fileName := fmt.Sprintf("%v%v", testInternalDataDir, "pools_default_buckets_4nodes_2replicas.json")
	retMap, _, err := readJsonHelper(fileName)
	if err != nil {
		panic(err)
	}
	return retMap
}

func TestNewReplicaCache(t *testing.T) {
	fmt.Println("============== Test case start: TestNewReplicaCache =================")
	defer fmt.Println("============== Test case end: TestNewReplicaCache =================")
	assert := assert.New(t)

	spec, _ := metadata.NewReplicationSpecification(srcBucketName, "", "", tgtBucketName, "")
	specList := []*metadata.ReplicationSpecification{spec}

	xdcrComp, utilsMock, bucketTopSvc, replSvc, utilsReal, queryResultErrs, queryResultsStatusCode, peerNodes, myHostAddr, srcCh, ckptSvc, backfillSpecSvc, colManifestSvc, securitySvc, _, remClusterSvc := setupBoilerPlate()
	setupMocks(utilsMock, utilsReal, xdcrComp, peerNodes, myHostAddr, specList, replSvc, queryResultErrs, queryResultsStatusCode, srcCh, nil, bucketTopSvc, ckptSvc, backfillSpecSvc, colManifestSvc, securitySvc, remClusterSvc)

	cache := NewReplicaCache(bucketTopSvc, nil)
	assert.NotNil(cache)

	cache.HandleSpecCreation(spec)
	assert.NotNil(cache.specsMonitorMap[spec.Id])

	srcNotification := &service_def.SourceNotification{}
	bucketInfo := getBucketInfoWithReplicas()
	replicasMap, translateMap, cnt, members, _ := utilsReal.GetReplicasInfo(bucketInfo, false, nil, nil, nil)

	srcNotification.On("GetReplicasInfo", mock.Anything).Return(cnt, replicasMap, translateMap, members)
	srcNotification.On("Recycle").Return(nil)
	srcNotification.On("Clone", mock.Anything).Return(srcNotification)
	srcCh <- srcNotification

	time.Sleep(10 * time.Millisecond)

	validateCnt, validateMap, validateTrMap, unlockFunc, err := cache.GetReplicaInfo(spec)
	assert.Nil(err)
	assert.Equal(cnt, validateCnt)
	assert.Equal(replicasMap, validateMap)
	assert.Equal(validateTrMap, translateMap)
	unlockFunc()

	_, _, _, unlockFunc, err = cache.GetReplicaInfo(spec)
	assert.Nil(err)
	unlockFunc()

	cache.HandleSpecDeletion(spec)
	_, _, _, _, err = cache.GetReplicaInfo(spec)
	assert.NotNil(err)
}
