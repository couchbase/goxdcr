package peerToPeer

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	service_def2 "github.com/couchbase/goxdcr/service_def"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"sync"
	"testing"
	"time"
)

func setupVBCHBoilerPlate() (*service_def.BucketTopologySvc, *service_def.CheckpointsService, *log.CommonLogger, *service_def.CollectionsManifestSvc, *service_def.BackfillReplSvc) {
	bucketTopologySvc := &service_def.BucketTopologySvc{}
	ckptSvc := &service_def.CheckpointsService{}
	logger := log.NewLogger("test", nil)
	colManifestSvc := &service_def.CollectionsManifestSvc{}
	backfillReplSvc := &service_def.BackfillReplSvc{}

	return bucketTopologySvc, ckptSvc, logger, colManifestSvc, backfillReplSvc
}

func setupMocks2(ckptSvc *service_def.CheckpointsService, ckptData map[uint16]*metadata.CheckpointsDoc, bucketTopologySvc *service_def.BucketTopologySvc, vbsList []uint16,
	colManifestSvc *service_def.CollectionsManifestSvc, backfillReplSvc *service_def.BackfillReplSvc, backfillSpec *metadata.BackfillReplicationSpec) {
	ckptSvc.On("CheckpointsDocs", replId, mock.Anything).Return(ckptData, nil)
	nsMappingDoc := &metadata.CollectionNsMappingsDoc{}
	ckptSvc.On("LoadBrokenMappings", replId).Return(nil, nsMappingDoc, nil, false, nil)

	notificationCh := make(chan service_def2.SourceNotification, 1)
	bucketTopologySvc.On("SubscribeToLocalBucketFeed", mock.Anything, mock.Anything).Return(notificationCh, nil)
	bucketTopologySvc.On("UnSubscribeLocalBucketFeed", mock.Anything, mock.Anything).Return(nil)

	notificationMock := &service_def.SourceNotification{}
	retMap := make(map[string][]uint16)
	retMap["hostname"] = vbsList
	notificationMock.On("GetSourceVBMapRO").Return(retMap, nil)
	notificationCh <- notificationMock

	manifestCache := make(map[uint64]*metadata.CollectionsManifest)
	defaultManifest := metadata.NewDefaultCollectionsManifest()
	manifestCache[0] = &defaultManifest
	colManifestSvc.On("GetAllCachedManifests", mock.Anything).Return(manifestCache, manifestCache, nil)

	backfillReplSvc.On("BackfillReplSpec", replId).Return(backfillSpec, nil)
}

var srcBucketName = "bucket"
var replId = "replId"

func TestVBMasterHandler(t *testing.T) {
	fmt.Println("============== Test case start: TestVBMasterHandler =================")
	defer fmt.Println("============== Test case end: TestVBMasterHandler =================")
	assert := assert.New(t)

	bucketTopologySvc, ckptSvc, logger, colManifestSvc, backfillReplSvc := setupVBCHBoilerPlate()

	vbList := []uint16{0, 1}
	vbsListNonIntersect := []uint16{2, 3}
	ckptData := make(map[uint16]*metadata.CheckpointsDoc)
	for _, vb := range vbList {
		ckptData[vb] = &metadata.CheckpointsDoc{SpecInternalId: "dummyId"}
	}

	emptySpec, tasks0 := getTaskForVB0()

	vbTaskMap := metadata.NewVBTasksMap()
	vbTaskMap.VBTasksMap[0] = tasks0
	backfillSpec := metadata.NewBackfillReplicationSpec(replId, "", vbTaskMap, emptySpec)

	setupMocks2(ckptSvc, ckptData, bucketTopologySvc, vbsListNonIntersect, colManifestSvc, backfillReplSvc, backfillSpec)

	reqCh := make(chan interface{}, 100)
	handler := NewVBMasterCheckHandler(reqCh, logger, "", 100*time.Millisecond, bucketTopologySvc, ckptSvc, colManifestSvc, backfillReplSvc)

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
	req.bucketVBMap = make(BucketVBMapType)
	req.bucketVBMap[srcBucketName] = vbList

	waitGrp.Add(1)
	handler.receiveCh <- req
	waitGrp.Wait()
}

func validateResponse(respActual *VBMasterCheckResp, assert *assert.Assertions) {
	bucketMapResp := respActual.GetReponse()
	assert.NotNil(bucketMapResp)
	assert.Equal("", respActual.ErrorMsg)
	payload := (*bucketMapResp)[srcBucketName]
	ckptMap := payload.GetAllCheckpoints()
	assert.NotEqual(0, len(ckptMap))
	assert.NotNil(payload.BackfillMappingDoc)
	backfillMapping := payload.GetBackfillMappingDoc()
	assert.NotNil(backfillMapping)
	shaMap, err := backfillMapping.ToShaMap()
	assert.Nil(err)
	assert.NotNil(shaMap)
	assert.NotEqual(0, len(backfillMapping.NsMappingRecords))
	assert.NotEqual(0, backfillMapping.NsMappingRecords.Size())
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
