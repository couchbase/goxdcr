// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package peerToPeer

import (
	"fmt"
	"github.com/couchbase/goxdcr/metadata"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/couchbase/goxdcr/utils"
	utilsMock2 "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"net/http"
	"testing"
	"time"
)

func setupBoilerPlate() (*service_def.XDCRCompTopologySvc, *utilsMock2.UtilsIface, *service_def.BucketTopologySvc, *service_def.ReplicationSpecSvc, *utils.Utilities) {
	xdcrComp := &service_def.XDCRCompTopologySvc{}
	utilsMock := &utilsMock2.UtilsIface{}
	bucketTopSvc := &service_def.BucketTopologySvc{}
	replSpecSvc := &service_def.ReplicationSpecSvc{}
	utilsReal := utils.NewUtilities()

	return xdcrComp, utilsMock, bucketTopSvc, replSpecSvc, utilsReal
}

func setupMocks(utilsMock *utilsMock2.UtilsIface, utilsReal *utils.Utilities, xdcrComp *service_def.XDCRCompTopologySvc, peerNodes []string, myAddr string,
	specList []*metadata.ReplicationSpecification, replSpecSvc *service_def.ReplicationSpecSvc, queryErrs []error, queryStatuses []int) {
	utilsMock.On("ExponentialBackoffExecutor", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		utilsReal.ExponentialBackoffExecutor(args.Get(0).(string), args.Get(1).(time.Duration), args.Get(2).(int), args.Get(3).(int), args.Get(4).(utils.ExponentialOpFunc))
	}).Return(nil)

	for i, peerNodeAddr := range peerNodes {
		utilsMock.On("QueryRestApiWithAuth", peerNodeAddr, mock.Anything, mock.Anything, mock.Anything,
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
			mock.Anything).Return(queryErrs[i], queryStatuses[i])
	}

	xdcrComp.On("PeerNodesAdminAddrs").Return(peerNodes, nil)
	xdcrComp.On("MyHostAddr").Return(myAddr, nil)

	retMap := make(map[string]*metadata.ReplicationSpecification)
	for _, spec := range specList {
		retMap[spec.Id] = spec
	}
	replSpecSvc.On("AllReplicationSpecs").Return(retMap, nil)
}

func TestPeerToPeerMgrSendVBCheck(t *testing.T) {
	fmt.Println("============== Test case start: TestPeerToPeerMgtSendVBCheck =================")
	defer fmt.Println("============== Test case end: TestPeerToPeerMgtSendVBCheck =================")
	assert := assert.New(t)

	bucketName := "bucketName"
	spec, _ := metadata.NewReplicationSpecification(bucketName, "", "", "", "")
	specList := []*metadata.ReplicationSpecification{spec}

	peerNodes := []string{"10.1.1.1:8091", "10.2.2.2:8091"}
	myHostAddr := "127.0.0.1:8091"

	queryResultErrs := []error{nil, nil}
	queryResultsStatusCode := []int{http.StatusOK, http.StatusOK}

	xdcrComp, utilsMock, bucket, replSvc, utilsReal := setupBoilerPlate()
	setupMocks(utilsMock, utilsReal, xdcrComp, peerNodes, myHostAddr, specList, replSvc, queryResultErrs, queryResultsStatusCode)

	mgr, err := NewPeerToPeerMgr(nil, xdcrComp, utilsMock, bucket, replSvc, 100*time.Millisecond, nil)
	assert.Nil(err)
	assert.NotNil(mgr)
	commAPI, err := mgr.Start()
	assert.NotNil(commAPI)
	assert.Nil(err)

	//var nilPtr *metadata.ReplicationSpecification
	//err = mgr.ReplicationSpecChangeCallback("", nilPtr, spec, nil)
	//assert.Nil(err)

	//type BucketVBMapType map[string][]uint16
	bucketMap := make(BucketVBMapType)
	bucketMap[bucketName] = []uint16{0, 1}

	filteredSubsets, err := mgr.vbMasterCheckHelper.GetUnverifiedSubset(bucketMap)
	assert.Nil(err)

	getReqFunc := func(src, tgt string) Request {
		var opaque uint32
		if tgt == peerNodes[0] {
			opaque = uint32(0)
		} else if tgt == peerNodes[1] {
			opaque = uint32(1)
		} else {
			panic("Invalid func")
		}
		common := NewRequestCommon(src, tgt, "", "", opaque)
		vbCheckReq := NewVBMasterCheckReq(common)
		vbCheckReq.SetBucketVBMap(filteredSubsets)
		return vbCheckReq
	}

	var responses []*VBMasterCheckResp
	for _, peerNode := range peerNodes {
		req := getReqFunc(myHostAddr, peerNode)
		var reqIface interface{} = req
		vbMasterCheckReq := reqIface.(*VBMasterCheckReq)
		resp := vbMasterCheckReq.GenerateResponse().(*VBMasterCheckResp)
		newMap := make(BucketVBMPayloadType)
		resp.responsePayload = &newMap
		(*resp.responsePayload)[bucketName] = &VBMasterPayload{
			OverallPayloadErr: "",
			NotMyVBs:          NewVBsPayload([]uint16{0, 1}),
			ConflictingVBs:    nil,
		}
		responses = append(responses, resp)
	}

	opts := NewSendOpts(true)
	err = mgr.sendToEachPeerOnce(ReqVBMasterChk, getReqFunc, opts)
	assert.Nil(err)

	// Now find the opaques
	handler, found := mgr.receiveHandlers[ReqVBMasterChk]
	assert.True(found)
	assert.NotNil(handler)

	// recast
	var handlerIface interface{} = handler
	vbMasterCheckHandler, ok := handlerIface.(*VBMasterCheckHandler)
	assert.True(ok)
	assert.NotNil(vbMasterCheckHandler)

	vbMasterCheckHandler.opaqueMapMtx.RLock()
	assert.Len(vbMasterCheckHandler.opaqueReqRespCbMap, 2)
	assert.Len(vbMasterCheckHandler.opaqueMap, 2)
	assert.Len(vbMasterCheckHandler.opaqueReqMap, 2)
	vbMasterCheckHandler.opaqueMapMtx.RUnlock()

	for _, resp := range responses {
		vbMasterCheckHandler.receiveCh <- resp
	}

	results := opts.GetResults()
	tgt1Result, found := results[peerNodes[0]]
	tgt2Result, found2 := results[peerNodes[0]]
	assert.True(found)
	assert.True(found2)
	assert.NotNil(tgt1Result.ReqPtr)
	assert.NotNil(tgt1Result.RespPtr)
	assert.NotNil(tgt2Result.ReqPtr)
	assert.NotNil(tgt2Result.RespPtr)
	checkResp := tgt1Result.RespPtr.(*VBMasterCheckResp)
	assert.Len((*checkResp.responsePayload), 1)
	notMyVbs := (*checkResp.responsePayload)[bucketName].NotMyVBs
	assert.Len(*notMyVbs, 2)
	checkResp = tgt2Result.RespPtr.(*VBMasterCheckResp)
	assert.Len((*checkResp.responsePayload), 1)
	notMyVbs = (*checkResp.responsePayload)[bucketName].NotMyVBs
	assert.Len(*notMyVbs, 2)

	time.Sleep(150 * time.Millisecond)
	vbMasterCheckHandler.opaqueMapMtx.RLock()
	assert.Len(vbMasterCheckHandler.opaqueReqRespCbMap, 0)
	assert.Len(vbMasterCheckHandler.opaqueMap, 0)
	assert.Len(vbMasterCheckHandler.opaqueReqMap, 0)
	vbMasterCheckHandler.opaqueMapMtx.RUnlock()

}
