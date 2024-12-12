/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package peerToPeer

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	service_def2 "github.com/couchbase/goxdcr/v8/service_def"
	service_def "github.com/couchbase/goxdcr/v8/service_def/mocks"
	utilsMock "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func setupVBCHBoilerPlate() (*service_def.BucketTopologySvc, *service_def.CheckpointsService, *log.CommonLogger, *service_def.CollectionsManifestSvc, *service_def.BackfillReplSvc, *utilsMock.UtilsIface, *service_def.ReplicationSpecSvc) {
	bucketTopologySvc := &service_def.BucketTopologySvc{}
	ckptSvc := &service_def.CheckpointsService{}
	logger := log.NewLogger("test", nil)
	colManifestSvc := &service_def.CollectionsManifestSvc{}
	backfillReplSvc := &service_def.BackfillReplSvc{}
	utils := &utilsMock.UtilsIface{}
	replSpecSvc := &service_def.ReplicationSpecSvc{}

	return bucketTopologySvc, ckptSvc, logger, colManifestSvc, backfillReplSvc, utils, replSpecSvc
}

func setupMocks2(ckptSvc *service_def.CheckpointsService, ckptData map[uint16]*metadata.CheckpointsDoc, bucketTopologySvc *service_def.BucketTopologySvc, vbsList []uint16, colManifestSvc *service_def.CollectionsManifestSvc, backfillReplSvc *service_def.BackfillReplSvc, backfillSpec *metadata.BackfillReplicationSpec, utils *utilsMock.UtilsIface, replSpecSvc *service_def.ReplicationSpecSvc, spec *metadata.ReplicationSpecification) {
	ckptSvc.On("CheckpointsDocs", replId, mock.Anything).Return(ckptData, nil)
	nsMappingDoc := &metadata.CollectionNsMappingsDoc{}
	globalTsDoc := &metadata.GlobalInfoCompressedDoc{}
	ckptSvc.On("LoadAllShaMappings", replId).Return(nsMappingDoc, globalTsDoc, nil)

	// For backfill, just return the same thing
	ckptSvc.On("CheckpointsDocs", common.ComposeFullTopic(replId, common.BackfillPipeline), mock.Anything).Return(ckptData, nil)
	ckptSvc.On("LoadAllShaMappings", common.ComposeFullTopic(replId, common.BackfillPipeline)).Return(nsMappingDoc, globalTsDoc, nil)

	notificationCh := make(chan service_def2.SourceNotification, 1)
	bucketTopologySvc.On("SubscribeToLocalBucketFeed", mock.Anything, mock.Anything).Return(notificationCh, nil)
	bucketTopologySvc.On("UnSubscribeLocalBucketFeed", mock.Anything, mock.Anything).Return(nil)

	notificationMock := &service_def.SourceNotification{}
	retMap := make(base.KvVBMapType)
	retMap["hostname"] = vbsList
	notificationMock.On("GetSourceVBMapRO").Return(retMap, nil)
	notificationMock.On("Recycle").Return(nil)
	notificationCh <- notificationMock

	manifestCache := make(map[uint64]*metadata.CollectionsManifest)
	defaultManifest := metadata.NewDefaultCollectionsManifest()
	manifestCache[0] = &defaultManifest
	colManifestSvc.On("GetAllCachedManifests", mock.Anything).Return(manifestCache, manifestCache, nil)

	backfillReplSvc.On("BackfillReplSpec", replId).Return(backfillSpec, nil)

	utils.On("StartDiagStopwatch", mock.Anything, mock.Anything).Return(func() time.Duration { return 0 })
	utils.On("ExponentialBackoffExecutor", "VBMasterCheckHandler.GetSpecDelNotification", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	replSpecSvc.On("ReplicationSpecReadOnly", mock.Anything).Return(spec, nil)
}

var srcBucketName = "bucket"
var replId = "TBD"

func TestVBMasterHandler(t *testing.T) {
	fmt.Println("============== Test case start: TestVBMasterHandler =================")
	defer fmt.Println("============== Test case end: TestVBMasterHandler =================")
	assert := assert.New(t)

	bucketTopologySvc, ckptSvc, logger, colManifestSvc, backfillReplSvc, utils, replSpecSvc := setupVBCHBoilerPlate()

	vbList := []uint16{0, 1}
	vbsListNonIntersect := []uint16{2, 3}
	ckptData := make(map[uint16]*metadata.CheckpointsDoc)
	for _, vb := range vbList {
		ckptData[vb] = &metadata.CheckpointsDoc{SpecInternalId: "dummyId"}
	}

	emptySpec, tasks0 := getTaskForVB0()

	vbTaskMap := metadata.NewVBTasksMap()
	vbTaskMap.VBTasksMap[0] = tasks0
	replSpec, _ := metadata.NewReplicationSpecification(srcBucketName, tgtBucketName, "test", "test", "test")
	replId = replSpec.Id
	backfillSpec := metadata.NewBackfillReplicationSpec(replId, replSpec.InternalId, vbTaskMap, emptySpec, 0)

	setupMocks2(ckptSvc, ckptData, bucketTopologySvc, vbsListNonIntersect, colManifestSvc, backfillReplSvc, backfillSpec, utils, replSpecSvc, replSpec)

	reqCh := make(chan interface{}, 100)
	respCh := make(chan interface{}, 100)
	handler := NewVBMasterCheckHandler([]chan interface{}{reqCh, respCh}, logger, "", 100*time.Millisecond, bucketTopologySvc, ckptSvc, colManifestSvc, backfillReplSvc, utils, replSpecSvc)
	handler.HandleSpecCreation(replSpec)

	var waitGrp sync.WaitGroup
	assert.Nil(handler.Start())
	req := NewVBMasterCheckReq(RequestCommon{
		Magic:             ReqMagic,
		ReqType:           ReqVBMasterChk,
		Sender:            "self",
		TargetAddr:        "self2",
		Opaque:            0,
		LocalLifeCycleId:  "",
		RemoteLifeCycleId: "",
		responseCb: func(resp Response) (HandlerResult, error) {
			var respInterface interface{} = resp
			respActual := respInterface.(*VBMasterCheckResp)
			validateResponse(respActual, assert)

			serializedResp, err := resp.Serialize()
			assert.Nil(err)
			testResp := &VBMasterCheckResp{}
			err = testResp.DeSerialize(serializedResp)
			assert.Nil(err)

			validateResponse(testResp, assert)

			waitGrp.Done()
			return nil, nil
		},
	})

	req.SourceBucketName = srcBucketName
	req.ReplicationId = replId
	req.InternalSpecId = replSpec.InternalId
	req.bucketVBMap = make(BucketVBMapType)
	req.bucketVBMap[srcBucketName] = vbList

	waitGrp.Add(1)
	handler.receiveReqCh <- req
	waitGrp.Wait()
}

func validateResponse(respActual *VBMasterCheckResp, assert *assert.Assertions) {
	bucketMapResp, unlockFunc := respActual.GetReponse()
	assert.NotNil(bucketMapResp)
	assert.Equal("", respActual.ErrorString)
	payload := (*bucketMapResp)[srcBucketName]
	ckptMap, err := payload.GetAllCheckpoints(common.MainPipeline)
	assert.Nil(err)
	assert.NotEqual(0, len(ckptMap))
	assert.NotNil(payload.BackfillMappingDoc)
	backfillMapping := payload.GetBackfillMappingDoc()
	assert.NotNil(backfillMapping)
	shaMap, err := backfillMapping.ToShaMap()
	assert.Nil(err)
	assert.NotNil(shaMap)
	assert.NotEqual(0, len(backfillMapping.NsMappingRecords))
	assert.NotEqual(0, backfillMapping.NsMappingRecords.Size())
	unlockFunc()

	// Test subset - NotMyVBs have 2 VBs
	subsetResp := respActual.GetSubsetBasedOnVBs([]uint16{0})
	assert.False((*subsetResp.payload)[srcBucketName].NotMyVBs.IsEmpty())
	ptr := (*subsetResp.payload)[srcBucketName].NotMyVBs
	assert.Len(*ptr, 1)

	for _, notMyVBPayload := range *ptr {
		assert.NotNil(notMyVBPayload.BackfillCkptDoc)
		assert.NotNil(notMyVBPayload.CheckpointsDoc)
	}
	return
}

func getTaskForVB0() (*metadata.ReplicationSpecification, *metadata.BackfillTasks) {
	collectionNs := make(metadata.CollectionNamespaceMapping)
	ns1 := &base.CollectionNamespace{
		ScopeName:      "s1",
		CollectionName: "col1",
	}
	collectionNs.AddSingleMapping(ns1, ns1)

	emptySpec, _ := metadata.NewReplicationSpecification(srcBucketName, "", "", "", "")
	ts0 := &metadata.BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{
			Vbno:  0,
			Seqno: 0,
		},
		EndingTimestamp: &base.VBTimestamp{
			Vbno:  0,
			Seqno: 1000,
		},
	}
	task0 := &metadata.BackfillTask{
		Timestamps: ts0,
	}
	task0.AddCollectionNamespaceMappingNoLock(collectionNs)
	tasks0 := &metadata.BackfillTasks{}
	tasks0.List = append(tasks0.List, task0)
	return emptySpec, tasks0
}
