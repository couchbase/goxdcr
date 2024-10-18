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
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"reflect"
	"strings"
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

func getPushWithManifestIdFile() []byte {
	file := "./unitTestData/pushWithManifestId.json"
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

	assert.Nil(resp.LoadMainPipelineCkpt(ckptDocs, bucketName))

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
	ckptDocsValidate, err := (*payload)[bucketName].GetAllCheckpoints(common.MainPipeline)
	assert.Nil(err)
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

	ckpts, err := vbMasterPayload.GetAllCheckpoints(common.MainPipeline)
	assert.Nil(err)
	assert.NotEqual(0, len(ckpts))
	for _, ckptDoc := range ckpts {
		assert.NotEqual(0, len(ckptDoc.Checkpoint_records))
		assert.NotNil(ckptDoc.Checkpoint_records[0])
	}
}

func TestVBMasterRespGlobalPayloadMap(t *testing.T) {
	fmt.Println("============== Test case start: TestVBMasterRespGlobalPayloadMap =================")
	defer fmt.Println("============== Test case end: TestVBMasterRespGlobalPayloadMap =================")
	assert := assert.New(t)

	brokenMap := make(metadata.CollectionNamespaceMapping)
	s1c1, err := base.NewCollectionNamespaceFromString("S1.c1")
	assert.Nil(err)
	brokenMap.AddSingleMapping(&s1c1, &s1c1)
	shaSlice, err := brokenMap.Sha256()
	assert.Nil(err)
	brokenMapSha := fmt.Sprintf("%x", shaSlice[:])

	brokenMapShaToColNsMap := make(metadata.ShaToCollectionNamespaceMap)
	brokenMapShaToColNsMap[brokenMapSha] = &brokenMap
	colNsMappingDoc := &metadata.CollectionNsMappingsDoc{}
	assert.Nil(colNsMappingDoc.LoadShaMap(brokenMapShaToColNsMap))

	vbs := []uint16{0, 1}
	notMyVBs, globalCkptDocMap := GenerateNotMyVBsPayload(vbs, brokenMapSha) // VBsCkptsDocMap type

	_, _, err = globalCkptDocMap.SnappyCompress()
	assert.Nil(err)

	// We need CompressedMappings of all global timestamp shas
	dedupShaBytesMap := make(map[string][]byte)
	for _, oneCkptDoc := range globalCkptDocMap {
		for _, record := range oneCkptDoc.Checkpoint_records {
			if record == nil {
				continue
			}
			assert.False(record.IsTraditional())
			gtsCompressedBytes, err := record.GlobalTimestamp.ToSnappyCompressed()
			assert.Nil(err)

			chkSha, err := record.GlobalTimestamp.Sha256()
			assert.Nil(err)
			gtsBytesChkStr := fmt.Sprintf("%x", chkSha)
			assert.Equal(record.GlobalTimestampSha256, gtsBytesChkStr)

			gtsMarshalled, err := json.Marshal(record.GlobalTimestamp)
			assert.Nil(err)
			checkBytes, err := snappy.Decode(nil, gtsCompressedBytes)
			assert.Nil(err)
			assert.True(reflect.DeepEqual(gtsMarshalled, checkBytes))

			// To double check
			assert.NotEqual("", record.GlobalTimestampSha256)
			chkGts := &metadata.GlobalTimestamp{}
			err = chkGts.SnappyDecompress(gtsCompressedBytes)
			assert.Nil(err)

			assert.True(chkGts.SameAs(&record.GlobalTimestamp))
			dedupShaBytesMap[record.GlobalTimestampSha256] = gtsCompressedBytes
		}
	}
	var compressedMappingDoc metadata.CompressedMappings
	for shaStr, snappyBytes := range dedupShaBytesMap {
		compressedMappingDoc.NsMappingRecords = append(compressedMappingDoc.NsMappingRecords, &metadata.CompressedShaMapping{
			CompressedMapping: snappyBytes,
			Sha256Digest:      shaStr,
		})
	}

	gtsCompressedDoc := (*metadata.GlobalTimestampCompressedDoc)(&compressedMappingDoc)

	vbMasterpayload := GenerateVBMasterPayload(&notMyVBs, colNsMappingDoc, gtsCompressedDoc)
	vbMasterCheckResp := &VBMasterCheckResp{
		ReplicationPayload: ReplicationPayload{
			payload: &BucketVBMPayloadType{
				"B0": vbMasterpayload,
			},
		},
	}

	payloadMap, unlockFunc := vbMasterCheckResp.GetReponse()
	assert.NotNil(payloadMap)
	var b0Found bool
	for k, _ := range *payloadMap {
		if k == "B0" {
			b0Found = true
		}
	}
	unlockFunc()
	assert.True(b0Found)
	payload := (*payloadMap)["B0"]
	assert.NotNil(payload)
	assert.NotNil(payload.GlobalTimestampDoc)

	oneNodeVbsCkptMap, err := payload.GetAllCheckpoints(common.MainPipeline)
	assert.Nil(err)
	assert.NotEqual(0, len(oneNodeVbsCkptMap))

	var brokenMapShaFound bool
	for _, ckptDoc := range oneNodeVbsCkptMap {
		if brokenMapShaFound {
			break
		}
		if ckptDoc == nil {
			continue
		}
		for _, record := range ckptDoc.Checkpoint_records {
			if record == nil {
				continue
			}
			for _, gts := range record.GlobalTimestamp {
				if gts.BrokenMappingSha256 != "" {
					brokenMapShaFound = true
					break
				}
			}
		}
	}
	assert.True(brokenMapShaFound)
	brokenMapChk := vbMasterpayload.GetBrokenMappingDoc()
	assert.NotNil(brokenMapChk)
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
	mainReplPayload := NewReplicationPayload(specId, specInternalId, "")

	bucketName := "bucketName"
	bucketVBMPayload := make(BucketVBMPayloadType)
	vbList := []uint16{0, 1}
	vbMasterPayload := &VBMasterPayload{
		PushVBs: NewVBsPayload(vbList),
	}
	bucketVBMPayload[bucketName] = vbMasterPayload

	mainReplPayload.payload = &bucketVBMPayload

	vbPeriodicReq := &VBPeriodicReplicateReq{
		ReplicationPayload: &mainReplPayload,
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

// TODO - MB-49485 caused the data to change... not immediately important but the unit test file is out of date
//func TestPeriodicPushSendPkt(t *testing.T) {
//	fmt.Println("============== Test case start: TestPeriodicPushSendPkt =================")
//	defer fmt.Println("============== Test case end: TestPeriodicPushSendPkt =================")
//	assert := assert.New(t)
//
//	utilsReal := utils.NewUtilities()
//
//	prePushData := getPrePushFile()
//	prePush := VBPeriodicReplicateReq{}
//	assert.Nil(json.Unmarshal(prePushData, &prePush))
//	assert.Nil(prePush.PostSerialize())
//	//assert.NotNil(prePush.ReplicationPayload)
//	//var atLeastOneBackfill bool
//	//for i := uint16(0); i < 512; i++ {
//	//	if (*(*prePush.ReplicationPayload.payload)[prePush.ReplicationPayload.SourceBucketName].PushVBs)[i] != nil &&
//	//		(*(*prePush.ReplicationPayload.payload)[prePush.ReplicationPayload.SourceBucketName].PushVBs)[i].BackfillTsks != nil {
//	//		atLeastOneBackfill = true
//	//	}
//	//}
//	//assert.True(atLeastOneBackfill)
//
//	securitySvcMock := &service_def.SecuritySvc{}
//
//	data1 := getPushFile1()
//	var reqCommon RequestCommon
//	err := json.Unmarshal(data1, &reqCommon)
//	assert.Nil(err)
//	reqRaw, err := generateRequest(utilsReal, reqCommon, data1, securitySvcMock)
//	assert.Nil(err)
//	req, ok := reqRaw.(*PeerVBPeriodicPushReq)
//	assert.True(ok)
//	assert.NotNil(req)
//
//	for _, request := range *(req.PushRequests) {
//		for i := uint16(0); i < 512; i++ {
//			assert.NotNil((*(*request.ReplicationPayload.payload)[request.ReplicationPayload.SourceBucketName].PushVBs)[i].CheckpointsDoc)
//		}
//		var atLeastOneBackfill bool
//		for i := uint16(0); i < 512; i++ {
//			assert.NotNil((*(*request.ReplicationPayload.payload)[request.ReplicationPayload.SourceBucketName].PushVBs)[i].CheckpointsDoc)
//			if (*(*request.ReplicationPayload.payload)[request.ReplicationPayload.SourceBucketName].PushVBs)[i].BackfillTsks != nil {
//				atLeastOneBackfill = true
//			}
//		}
//		assert.True(atLeastOneBackfill)
//	}
//}

func TestPeriodicPushSendPktCorners(t *testing.T) {
	fmt.Println("============== Test case start: TestPeriodicPushSendPktCorners =================")
	defer fmt.Println("============== Test case end: TestPeriodicPushSendPktCorners =================")
	assert := assert.New(t)

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

func TestInvalidSnappyPacket(t *testing.T) {
	fmt.Println("============== Test case start: TestInvalidSnappyPacket =================")
	defer fmt.Println("============== Test case end: TestInvalidSnappyPacket =================")
	assert := assert.New(t)

	failedPacketFile := "./unitTestData/failedPacket.json"
	failedPacketBytes, err := ioutil.ReadFile(failedPacketFile)
	if err != nil {
		panic(err)
	}
	failedPacket := &VBMasterCheckResp{}
	err = failedPacket.DeSerialize(failedPacketBytes)
	assert.Nil(err)

	assert.NotEqual("", failedPacket.GetErrorString())
	assert.True(strings.Contains(failedPacket.GetErrorString(), base.ErrorDoesNotExistString))
}

func TestRespMarshalBigManifest(t *testing.T) {
	fmt.Println("============== Test case start: TestRespMarshalBigManifest =================")
	defer fmt.Println("============== Test case end: TestRespMarshalBigManifest =================")
	assert := assert.New(t)

	dataFile := "./unitTestData/1kCollectionManifestMap.json"
	testRespMarshalBigManifestGivenFile(dataFile, assert)

	dataFile = "./unitTestData/1kCollectionWithNoDefaultColletion.json"
	testRespMarshalBigManifestGivenFile(dataFile, assert)
}

func testRespMarshalBigManifestGivenFile(dataFile string, assert *assert.Assertions) {
	data, err := ioutil.ReadFile(dataFile)
	assert.Nil(err)
	checkMap := make(map[string]interface{})
	assert.Nil(json.Unmarshal(data, &checkMap))

	checkManifest, err := metadata.NewCollectionsManifestFromMap(checkMap)
	assert.Nil(err)
	assert.NotNil(checkManifest)

	cachedSrcManifests := make(metadata.ManifestsCache)
	cachedTgtManifests := make(metadata.ManifestsCache)
	cachedSrcManifests[checkManifest.Uid()] = &checkManifest
	cachedTgtManifests[checkManifest.Uid()] = &checkManifest

	var srcBucketName = "srcBucket"
	respCommon := NewResponseCommon(ReqVBMasterChk, "", "", 0, "")
	resp := &VBMasterCheckResp{
		ResponseCommon:     respCommon,
		ReplicationPayload: NewReplicationPayload("dummySpec", srcBucketName, "dummyInternal"),
	}
	resp.Init()
	resp.InitBucket(srcBucketName)
	assert.NotNil(resp)

	assert.Nil(resp.LoadManifests(cachedSrcManifests, cachedTgtManifests, srcBucketName))
	respBytes, err := resp.Serialize()
	assert.Nil(err)
	assert.NotNil(respBytes)

	checkPacket := &VBMasterCheckResp{}
	err = checkPacket.DeSerialize(respBytes)
	assert.Nil(err)
}

func TestPerfVBChkResp(t *testing.T) {
	fmt.Println("============== Test case start: TestPerfVBChkResp =================")
	defer fmt.Println("============== Test case end: TestPerfVBChkResp =================")
	assert := assert.New(t)

	perfVBChkResp := "./unitTestData/perfFailedVBChkResp.json"
	perfVBChkRespBytes, err := ioutil.ReadFile(perfVBChkResp)
	if err != nil {
		panic(err)
	}

	failedPacket := &VBMasterCheckResp{}
	err = failedPacket.DeSerialize(perfVBChkRespBytes)
	assert.Nil(err)
}

func TestDeserializeGetManifestId(t *testing.T) {
	fmt.Println("============== Test case start: TestDeserializeGetManifestId =================")
	defer fmt.Println("============== Test case end: TestDeserializeGetManifestId =================")
	assert := assert.New(t)

	reqBytes := getPushWithManifestIdFile()
	pushReq := &PeerVBPeriodicPushReq{}
	assert.Nil(pushReq.DeSerialize(reqBytes))

	for _, onePushReq := range *pushReq.PushRequests {
		payload, unlock := onePushReq.GetPayloadWithReadLock()
		assert.NotNil(payload)
		assert.NotEqual(0, len(*payload))
		for _, v := range *payload {
			pushVB := v.PushVBs
			assert.NotNil(pushVB)
			assert.Equal(uint64(7), v.GetBackfillVBTasksManifestsId())
		}
		unlock()
	}
}

func TestSerializingHeartbeat(t *testing.T) {
	fmt.Println("============== Test case start: TestSerializingHeartbeat =================")
	defer fmt.Println("============== Test case end: TestSerializingHeartbeat =================")
	assert := assert.New(t)

	spec1, err := metadata.NewReplicationSpecification("B1", "uuid1", "uuid2", "B2", "uuid3")
	assert.Nil(err)

	spec2, err := metadata.NewReplicationSpecification("B1a", "uuid1", "uuid2", "B2a", "uuid3")
	assert.Nil(err)

	reqCommon := NewRequestCommon("sender", "target", "", "", uint32(9))
	msg := NewSourceHeartbeatReq(reqCommon)
	msg.AppendSpec(spec1)
	msg.AppendSpec(spec2)

	seralizedByte, err := msg.Serialize()
	assert.Nil(err)

	checkMsg := NewSourceHeartbeatReq(reqCommon)
	assert.Nil(checkMsg.DeSerialize(seralizedByte))
}
