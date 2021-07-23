package peerToPeer

import (
	"fmt"
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

func setupVBCHBoilerPlate() (*service_def.BucketTopologySvc, *service_def.CheckpointsService, *log.CommonLogger) {
	bucketTopologySvc := &service_def.BucketTopologySvc{}
	ckptSvc := &service_def.CheckpointsService{}
	logger := log.NewLogger("test", nil)

	return bucketTopologySvc, ckptSvc, logger
}

func setupMocks2(ckptSvc *service_def.CheckpointsService, ckptData map[uint16]*metadata.CheckpointsDoc, bucketTopologySvc *service_def.BucketTopologySvc, vbsList []uint16) {
	ckptSvc.On("CheckpointsDocs", replId, false).Return(ckptData, nil)

	notificationCh := make(chan service_def2.SourceNotification, 1)
	bucketTopologySvc.On("SubscribeToLocalBucketFeed", mock.Anything, mock.Anything).Return(notificationCh, nil)
	bucketTopologySvc.On("UnSubscribeLocalBucketFeed", mock.Anything, mock.Anything).Return(nil)

	notificationMock := &service_def.SourceNotification{}
	retMap := make(map[string][]uint16)
	retMap["hostname"] = vbsList
	notificationMock.On("GetSourceVBMapRO").Return(retMap, nil)
	notificationCh <- notificationMock

}

var srcBucketName = "bucket"
var replId = "replId"

func TestVBMasterHandler(t *testing.T) {
	fmt.Println("============== Test case start: TestVBMasterHandler =================")
	defer fmt.Println("============== Test case end: TestVBMasterHandler =================")
	assert := assert.New(t)

	bucketTopologySvc, ckptSvc, logger := setupVBCHBoilerPlate()

	vbList := []uint16{0, 1}
	vbsListNonIntersect := []uint16{2, 3}
	ckptData := make(map[uint16]*metadata.CheckpointsDoc)
	for _, vb := range vbList {
		ckptData[vb] = &metadata.CheckpointsDoc{SpecInternalId: "dummyId"}
	}

	setupMocks2(ckptSvc, ckptData, bucketTopologySvc, vbsListNonIntersect)

	reqCh := make(chan interface{}, 100)
	handler := NewVBMasterCheckHandler(reqCh, logger, "", 100*time.Millisecond, bucketTopologySvc, ckptSvc)

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
			fmt.Printf("ND: %v\n", respActual)
			bucketMap := respActual.GetReponse()
			payload := (*bucketMap)[srcBucketName]
			ckptMap := payload.GetAllCheckpoints()
			assert.NotEqual(0, len(ckptMap))
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
