package backfill_manager

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
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

func createVBsGetter() MyVBsGetter {
	var allVBs []uint16
	var vb uint16
	for vb = 0; vb < 1024; vb++ {
		allVBs = append(allVBs, vb)
	}
	getterFunc := func() ([]uint16, error) {
		return allVBs, nil
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

func TestBackfillReqHandlerStartStop(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillReqHandlerStartStop =================")
	logger, backfillReplSvc := setupBRHBoilerPlate()
	rh := NewCollectionBackfillRequestHandler(logger, specId, backfillReplSvc, createTestSpec(), createSeqnoGetterFunc(100), createVBsGetter())

	assert.NotNil(rh)
	assert.Nil(rh.Start())

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

	fmt.Println("============== Test case end: TestBackfillReqHandlerStartStop =================")
}

func TestBackfillReqHandlerCreateReq(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillReqHandlerCreateReq =================")
	logger, _ := setupBRHBoilerPlate()
	spec := createTestSpec()
	vbsGetter := createVBsGetter()
	seqnoGetter := createSeqnoGetterFunc(100)
	var addCount int
	var setCount int
	// Make a dummy namespacemapping
	collectionNs := &base.CollectionNamespace{base.DefaultScopeCollectionName, base.DefaultScopeCollectionName}
	requestMapping := make(metadata.CollectionNamespaceMapping)
	requestMapping.AddSingleMapping(collectionNs, collectionNs)

	backfillReplSvc := &service_def.BackfillReplSvc{}
	backfillReplSvc.On("AddBackfillReplSpec", mock.Anything).Run(func(args mock.Arguments) { (addCount)++ }).Return(nil)
	backfillReplSvc.On("SetBackfillReplSpec", mock.Anything).Run(func(args mock.Arguments) { (setCount)++ }).Return(nil)

	rh := NewCollectionBackfillRequestHandler(logger, specId, backfillReplSvc, spec, seqnoGetter, vbsGetter)

	assert.NotNil(rh)
	assert.Nil(rh.Start())

	// Wait for the go-routine to start
	time.Sleep(10 * time.Millisecond)

	// Calling twice in a row with the same result will result in a single add
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	go func() {
		assert.Nil(rh.HandleBackfillRequest(requestMapping))
		waitGroup.Done()
	}()
	go func() {
		assert.Nil(rh.HandleBackfillRequest(requestMapping))
		waitGroup.Done()
	}()
	waitGroup.Wait()

	fmt.Printf("Done with AddRequest\n")

	time.Sleep(100 * time.Millisecond)

	// Doing another handle will result in a set
	assert.Nil(rh.HandleBackfillRequest(requestMapping))

	time.Sleep(100 * time.Millisecond)

	// Two bursty, concurrent add requests results in a single add
	assert.Equal(1, addCount)
	// One later set request should result in a single set
	assert.Equal(1, setCount)
	fmt.Println("============== Test case stop: TestBackfillReqHandlerCreateReq =================")
}
