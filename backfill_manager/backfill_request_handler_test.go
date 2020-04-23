package backfill_manager

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func setupBRHBoilerPlate() (*log.CommonLogger, *service_def.BackfillReplSvc) {
	logger := log.NewLogger("BackfillReqHandler", log.DefaultLoggerContext)
	backfillReplSvc := &service_def.BackfillReplSvc{}
	return logger, backfillReplSvc
}

func createSeqnoGetterFunc(topSeqno uint64) LatestSeqnoGetter {
	highMap := make(map[uint16]uint64)
	var vb uint16
	for vb = 0; vb < 1024; vb++ {
		highMap[vb] = topSeqno
	}

	getterFunc := func() (map[uint16]uint64, error) {
		return highMap, nil
	}
	return getterFunc
}

const SourceBucketName = "sourceBucketName"
const SourceBucketUUID = "sourceBucketUUID"
const TargetClusterUUID = "targetClusterUUID"
const TargetBucketName = "targetBucket"
const TargetBucketUUID = "targetBucketUUID"
const specId = "testSpec"
const specInternalId = "testSpecInternal"

func createTestSpec() *metadata.ReplicationSpecification {
	return &metadata.ReplicationSpecification{Id: specId, InternalId: specInternalId,
		SourceBucketName:  SourceBucketName,
		SourceBucketUUID:  SourceBucketUUID,
		TargetClusterUUID: TargetClusterUUID,
		TargetBucketName:  TargetBucketName,
		TargetBucketUUID:  TargetBucketUUID,
		Settings:          metadata.DefaultReplicationSettings(),
	}
}

func TestBackfillReqHandler(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillReqHandler =================")
	logger, backfillReplSvc := setupBRHBoilerPlate()
	rh := NewCollectionBackfillRequestHandler(logger, specId, backfillReplSvc, createTestSpec(), createSeqnoGetterFunc(100))

	assert.NotNil(rh)
	assert.Nil(rh.Start())

	// Make a dummy namespacemapping
	collectionNs := &base.CollectionNamespace{base.DefaultScopeCollectionName, base.DefaultScopeCollectionName}
	requestMapping := make(metadata.CollectionNamespaceMapping)
	requestMapping.AddSingleMapping(collectionNs, collectionNs)

	assert.Nil(rh.HandleBackfillRequest(requestMapping))

	time.Sleep(100 * time.Millisecond)

	componentErr := make(chan base.ComponentError, 1)
	var waitGrp sync.WaitGroup

	// Stopping
	waitGrp.Add(1)
	rh.Stop(&waitGrp, componentErr)

	retVal := <-componentErr
	assert.NotNil(retVal)
	assert.Equal(specId, retVal.ComponentId)
	assert.Nil(retVal.Err)

	fmt.Println("============== Test case end: TestBackfillReqHandler =================")
}
