// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata_svc

import (
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCheckpointsServiceCacheImpl_Run(t *testing.T) {
	assert := assert.New(t)

	ckptCache := NewCheckpointsServiceCache(nil)
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
	assert := assert.New(t)

	ckptCache := NewCheckpointsServiceCache(nil)
	ckptCache.cacheEnabled = true
	go ckptCache.Run()

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
		SpecInternalId:     "testId",
		Revision:           nil,
	}
	docs = make(map[uint16]*metadata.CheckpointsDoc)
	docs[0] = setDoc
	assert.Nil(ckptCache.StoreLatestDocs(docs))

	getDoc, err := ckptCache.GetLatestDocs()
	assert.Nil(err)
	assert.True(getDoc.SameAs(docs))

	assert.Nil(ckptCache.StoreOneVbDoc(1, setDoc))
	getDoc, _ = ckptCache.GetLatestDocs()
	assert.NotNil(getDoc[1])
}
