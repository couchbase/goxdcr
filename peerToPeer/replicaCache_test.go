package peerToPeer

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/goxdcr/metadata"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/stretchr/testify/assert"
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

	xdcrComp, utilsMock, bucketTopSvc, replSvc, utilsReal, queryResultErrs, queryResultsStatusCode, peerNodes, myHostAddr, srcCh := setupBoilerPlate()
	setupMocks(utilsMock, utilsReal, xdcrComp, peerNodes, myHostAddr, specList, replSvc, queryResultErrs, queryResultsStatusCode, srcCh, bucketTopSvc)

	cache := NewReplicaCache(bucketTopSvc, nil)
	assert.NotNil(cache)

	cache.HandleSpecCreation(spec)
	assert.NotNil(cache.specsMonitorMap[spec.Id])

	srcNotification := &service_def.SourceNotification{}
	bucketInfo := getBucketInfoWithReplicas()
	replicasMap, translateMap, cnt, members, _ := utilsReal.GetReplicasInfo(bucketInfo)

	srcNotification.On("GetReplicasInfo").Return(cnt, replicasMap, translateMap, members)
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
