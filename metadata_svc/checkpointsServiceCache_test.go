// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata_svc

import (
	"fmt"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCheckpointsServiceCacheImpl_Run(t *testing.T) {
	fmt.Println("============== Test case start: TestCheckpointsServiceCacheImpl_Run =================")
	defer fmt.Println("============== Test case end: TestCheckpointsServiceCacheImpl_Run =================")
	assert := assert.New(t)

	ckptCache := NewCheckpointsServiceCache(nil, "")
	go ckptCache.Run()

	testSpec, _ := metadata.NewReplicationSpecification("srcBucket", "bucketUUID", "targetCluster", "targetBucket", "tgtBucketUuid")
	ckptCache.SpecChangeCb(testSpec, nil)
	select {
	case <-ckptCache.finCh:
		break
	default:
		assert.True(false)
	}

}

func TestCheckpointsServiceCacheImplGetAndSet(t *testing.T) {
	fmt.Println("============== Test case start: TestCheckpointsServiceCacheImplGetAndSet =================")
	defer fmt.Println("============== Test case end: TestCheckpointsServiceCacheImplGetAndSet =================")
	assert := assert.New(t)

	ckptCache := NewCheckpointsServiceCache(nil, "")
	ckptCache.cacheEnabled = true
	go ckptCache.Run()

	// spec
	spec, _ := metadata.NewReplicationSpecification("sourceBucket", "sourceBucketUuid",
		"targetClusterUuid", "targetBucketName", "targetBucketUuid")
	ckptCache.SpecChangeCb(nil, spec)

	docs, err := ckptCache.GetLatestDocs()
	assert.Nil(docs)
	assert.Equal(service_def.MetadataNotFoundErr, err)

	// that's it
	record := &metadata.CheckpointRecord{
		Seqno:            12345,
		Target_vb_opaque: &metadata.TargetVBUuid{Target_vb_uuid: 123},
	}
	var recordsList metadata.CheckpointRecordsList
	recordsList = append(recordsList, record)
	setDoc := &metadata.CheckpointsDoc{
		Checkpoint_records: recordsList,
		SpecInternalId:     spec.InternalId,
		Revision:           nil,
	}
	docs = make(map[uint16]*metadata.CheckpointsDoc)
	docs[0] = setDoc
	assert.Nil(ckptCache.StoreLatestDocs(docs))

	// Let Run have some time to validate cache
	time.Sleep(100 * time.Millisecond)

	getDoc, err := ckptCache.GetLatestDocs()
	assert.Nil(err)
	assert.True(getDoc.SameAs(docs))

	time.Sleep(100 * time.Millisecond)
	assert.Nil(ckptCache.StoreOneVbDoc(1, setDoc, spec.InternalId))

	time.Sleep(100 * time.Millisecond)
	// The storeOneVbDoc will start a session so getting latest docs should trigger error
	_, err = ckptCache.GetLatestDocs()
	assert.Equal(service_def.MetadataNotFoundErr, err)

	ckptCache.ValidateCache("fakeInternalId") // should not work
	time.Sleep(100 * time.Millisecond)
	_, err = ckptCache.GetLatestDocs()
	assert.Equal(service_def.MetadataNotFoundErr, err)

	// Once all ckpts have been uploaded, cache is valid again
	ckptCache.ValidateCache(spec.InternalId)
	time.Sleep(100 * time.Millisecond)
	getDoc, _ = ckptCache.GetLatestDocs()
	assert.NotNil(getDoc[1])
}
