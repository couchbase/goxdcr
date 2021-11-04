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
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/metadata"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/couchbase/goxdcr/utils"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func getManifest() metadata.CollectionsManifest {
	file := "../metadata/testData/provisionedManifest.json"
	data, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}
	provisionedManifest, _ := metadata.NewCollectionsManifestFromBytes(data)

	return provisionedManifest
}

func getPushFile1() []byte {
	file := "./unitTestData/periodicPush1.json"
	data, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}
	return data
}

func getPrePushFile() []byte {
	file := "./unitTestData/p2pReplicaAgentReqWBackfill.json"
	data, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}
	return data
}

func TestVBMasterCheckResp(t *testing.T) {
	fmt.Println("============== Test case start: TestVBMasterCheckResp =================")
	defer fmt.Println("============== Test case end: TestVBMasterCheckResp =================")
	assert := assert.New(t)

	bucketName := "bucketName"
	vbList := []uint16{0, 1}
	specInternalId := "testId"
	ckptDocs := make(map[uint16]*metadata.CheckpointsDoc)
	testDoc := &metadata.CheckpointsDoc{SpecInternalId: specInternalId}
	for _, vb := range vbList {
		ckptDocs[vb] = testDoc
	}
	reqCommon := NewRequestCommon("sender", "target", "", "", uint32(3))
	req := NewVBMasterCheckReq(reqCommon)
	req.SourceBucketName = bucketName

	bucketMap := make(BucketVBMapType)
	bucketMap[bucketName] = vbList
	req.SetBucketVBMap(bucketMap)

	resp := req.GenerateResponse().(*VBMasterCheckResp)
	resp.Init()
	resp.InitBucket(bucketName)
	(*resp.payload)[bucketName] = &VBMasterPayload{
		OverallPayloadErr: "",
		NotMyVBs:          NewVBsPayload(nil),
		ConflictingVBs:    nil,
	}
	(*resp.payload)[bucketName].RegisterNotMyVBs(vbList)

	assert.NotNil(resp)
	lenCheck := len(vbList)
	notMyVBs := (*resp.payload)[bucketName].NotMyVBs
	assert.Len(*notMyVBs, lenCheck)

	for _, vb := range vbList {
		notMyVbs := *(*resp.payload)[bucketName].NotMyVBs
		assert.NotEqual(testDoc, notMyVbs[vb].CheckpointsDoc)
	}

	assert.Nil(resp.LoadPipelineCkpts(ckptDocs, bucketName))

	for _, vb := range vbList {
		notMyVbs := *(*resp.payload)[bucketName].NotMyVBs
		assert.Equal(testDoc, notMyVbs[vb].CheckpointsDoc)
	}

	// Test out manifest
	manifest := getManifest()
	srcMap := make(metadata.ManifestsCache)
	tgtMap := make(metadata.ManifestsCache)
	srcMap[manifest.Uid()] = &manifest
	tgtMap[manifest.Uid()] = &manifest
	(*resp.payload)[bucketName].SrcManifests = &srcMap
	(*resp.payload)[bucketName].TgtManifests = &tgtMap

	marshalBytes, err := resp.Serialize()
	assert.Nil(err)

	newResp := &VBMasterCheckResp{}
	assert.Nil(newResp.DeSerialize(marshalBytes))

	payload, unlockFunc := newResp.GetReponse()
	assert.NotNil((*payload)[bucketName])
	ckptDocsValidate := (*payload)[bucketName].GetAllCheckpoints()
	for _, vb := range vbList {
		assert.Equal(specInternalId, ckptDocsValidate[vb].SpecInternalId)
	}
	unlockFunc()

	srcManifests, tgtManifests := (*payload)[bucketName].GetAllManifests()
	assert.Nil(err)
	assert.Len(*srcManifests, 1)
	assert.Len(*tgtManifests, 1)

	var respCast ReqRespCommon
	respCast = newResp
	var regIfaceCast interface{}
	regIfaceCast = respCast

	var castBack *VBMasterCheckResp
	castBack = regIfaceCast.(*VBMasterCheckResp)
	assert.Equal(castBack.payload, newResp.payload)
}

func TestVBMasterPayloadMap(t *testing.T) {
	fmt.Println("============== Test case start: TestVBMasterPayloadMap =================")
	defer fmt.Println("============== Test case end: TestVBMasterPayloadMap =================")
	assert := assert.New(t)

	file := "./unitTestData/bucketVBMPayload.json"
	vbMasterPayload := &VBMasterPayload{}
	data, err := ioutil.ReadFile(file)
	assert.Nil(err)
	err = json.Unmarshal(data, &vbMasterPayload)
	assert.Nil(err)

	ckpts := vbMasterPayload.GetAllCheckpoints()
	assert.NotEqual(0, len(ckpts))
	for _, ckptDoc := range ckpts {
		assert.NotEqual(0, len(ckptDoc.Checkpoint_records))
		assert.NotNil(ckptDoc.Checkpoint_records[0])
	}
}

func TestManifestLoadTest(t *testing.T) {
	fmt.Println("============== Test case start: TestManifestLoadTest =================")
	defer fmt.Println("============== Test case end: TestManifestLoadTest =================")
	assert := assert.New(t)

	file := "./unitTestData/postSerializeData.json"
	vbMasterPayload := &VBMasterPayload{}
	data, err := ioutil.ReadFile(file)
	assert.Nil(err)
	err = json.Unmarshal(data, &vbMasterPayload)
	assert.Nil(err)

	src, tgt := vbMasterPayload.GetAllManifests()
	fmt.Printf("%v - %v - %v\n", src, tgt, err)
}

func TestPeriodicPush(t *testing.T) {
	fmt.Println("============== Test case start: TestPeriodicPush =================")
	defer fmt.Println("============== Test case end: TestPeriodicPush =================")
	assert := assert.New(t)

	reqCommon := NewRequestCommon("sender", "target", "", "", uint32(3))
	pushReq := NewPeerVBPeriodicPushReq(reqCommon)

	specId := "testSpecId"
	specInternalId := "testSpecIdInternal"
	mainReplPayload := NewReplicationPayload(specId, specInternalId, common.MainPipeline, "")
	backfillPayload := NewReplicationPayload(specId, specInternalId, common.BackfillPipeline, "")

	bucketName := "bucketName"
	bucketVBMPayload := make(BucketVBMPayloadType)
	vbList := []uint16{0, 1}
	vbMasterPayload := &VBMasterPayload{
		PushVBs: NewVBsPayload(vbList),
	}
	bucketVBMPayload[bucketName] = vbMasterPayload

	mainReplPayload.payload = &bucketVBMPayload
	backfillPayload.payload = &bucketVBMPayload

	vbPeriodicReq := &VBPeriodicReplicateReq{
		MainReplication:     &mainReplPayload,
		BackfillReplication: &backfillPayload,
	}

	var list VBPeriodicReplicateReqList
	list = append(list, vbPeriodicReq)

	pushReq.PushRequests = &list

	serializedBytes, err := pushReq.Serialize()
	assert.Nil(err)
	assert.NotNil(serializedBytes)

	checkReq := &PeerVBPeriodicPushReq{}
	err = checkReq.DeSerialize(serializedBytes)
	assert.Nil(err)

	assert.Equal(ReqPeriodicPush, checkReq.ReqType)
	assert.Len(*checkReq.PushRequests, 1)

	assert.True(checkReq.SameAs(pushReq))
}

func TestPeriodicPushSendPkt(t *testing.T) {
	fmt.Println("============== Test case start: TestPeriodicPushSendPkt =================")
	defer fmt.Println("============== Test case end: TestPeriodicPushSendPkt =================")
	assert := assert.New(t)

	utilsReal := utils.NewUtilities()

	prePushData := getPrePushFile()
	prePush := VBPeriodicReplicateReq{}
	assert.Nil(json.Unmarshal(prePushData, &prePush))
	assert.Nil(prePush.PostSerialize())
	assert.NotNil(prePush.MainReplication)
	assert.NotNil(prePush.BackfillReplication)
	var atLeastOneBackfill bool
	for i := uint16(0); i < 512; i++ {
		if (*(*prePush.BackfillReplication.payload)[prePush.BackfillReplication.SourceBucketName].PushVBs)[i] != nil &&
			(*(*prePush.BackfillReplication.payload)[prePush.BackfillReplication.SourceBucketName].PushVBs)[i].BackfillTsks != nil {
			atLeastOneBackfill = true
		}
	}
	assert.True(atLeastOneBackfill)

	securitySvcMock := &service_def.SecuritySvc{}

	data1 := getPushFile1()
	var reqCommon RequestCommon
	err := json.Unmarshal(data1, &reqCommon)
	assert.Nil(err)
	reqRaw, err := generateRequest(utilsReal, reqCommon, data1, securitySvcMock)
	assert.Nil(err)
	req, ok := reqRaw.(*PeerVBPeriodicPushReq)
	assert.True(ok)
	assert.NotNil(req)

	for _, request := range *(req.PushRequests) {
		for i := uint16(0); i < 512; i++ {
			assert.NotNil((*(*request.MainReplication.payload)[request.MainReplication.SourceBucketName].PushVBs)[i].CheckpointsDoc)
		}
		var atLeastOneBackfill bool
		for i := uint16(0); i < 512; i++ {
			assert.NotNil((*(*request.MainReplication.payload)[request.MainReplication.SourceBucketName].PushVBs)[i].CheckpointsDoc)
			if (*(*request.BackfillReplication.payload)[request.BackfillReplication.SourceBucketName].PushVBs)[i].BackfillTsks != nil {
				atLeastOneBackfill = true
			}
		}
		assert.True(atLeastOneBackfill)
	}
}

func TestPeriodicPushSendPktCorners(t *testing.T) {
	fmt.Println("============== Test case start: TestPeriodicPushSendPktCorners =================")
	defer fmt.Println("============== Test case end: TestPeriodicPushSendPktCorners =================")
	assert := assert.New(t)

	//func (v *VBPeriodicReplicateReq) PreSerlialize() error {
	nilPreReq := &VBPeriodicReplicateReq{}
	assert.Nil(nilPreReq.PreSerlialize())
	nilPreReq.PostSerialize()
	data, err := json.Marshal(nilPreReq)
	assert.Nil(err)

	checkReq := &VBPeriodicReplicateReq{}
	assert.Nil(json.Unmarshal(data, &checkReq))

	requestCommon := NewRequestCommon("src", "tgt", "lifecycle", "", 0)
	peerSendReq := NewPeerVBPeriodicPushReq(requestCommon)
	nilList := &VBPeriodicReplicateReqList{}
	peerSendReq.PushRequests = nilList

	var nilReq *PeerVBPeriodicPushReq
	data, err = json.Marshal(nilReq)
	assert.Nil(err)

	assert.Nil(json.Unmarshal(data, &checkReq))
}
