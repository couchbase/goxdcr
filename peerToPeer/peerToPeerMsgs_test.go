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
	"github.com/couchbase/goxdcr/metadata"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

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

	//type BucketVBMapType map[string][]uint16
	bucketMap := make(BucketVBMapType)
	bucketMap[bucketName] = vbList
	req.SetBucketVBMap(bucketMap)

	resp := req.GenerateResponse().(*VBMasterCheckResp)
	resp.Init()
	resp.InitBucket(bucketName)
	(*resp.responsePayload)[bucketName] = &VBMasterPayload{
		OverallPayloadErr: "",
		NotMyVBs:          NewVBsPayload(nil),
		ConflictingVBs:    nil,
	}
	(*resp.responsePayload)[bucketName].RegisterNotMyVBs(vbList)

	assert.NotNil(resp)
	lenCheck := len(vbList)
	notMyVBs := (*resp.responsePayload)[bucketName].NotMyVBs
	assert.Len(*notMyVBs, lenCheck)

	for _, vb := range vbList {
		notMyVbs := *(*resp.responsePayload)[bucketName].NotMyVBs
		assert.NotEqual(testDoc, notMyVbs[vb].CheckpointsDoc)
	}

	assert.Nil(resp.LoadPipelineCkpts(ckptDocs, bucketName))

	for _, vb := range vbList {
		notMyVbs := *(*resp.responsePayload)[bucketName].NotMyVBs
		assert.Equal(testDoc, notMyVbs[vb].CheckpointsDoc)
	}

	marshalBytes, err := resp.Serialize()
	assert.Nil(err)

	newResp := &VBMasterCheckResp{}
	assert.Nil(newResp.DeSerialize(marshalBytes))

	payload := newResp.GetReponse()
	assert.NotNil((*payload)[bucketName])
	ckptDocsValidate := (*payload)[bucketName].GetAllCheckpoints()
	for _, vb := range vbList {
		assert.Equal(specInternalId, ckptDocsValidate[vb].SpecInternalId)
	}

	var respCast ReqRespCommon
	respCast = newResp
	var regIfaceCast interface{}
	regIfaceCast = respCast

	var castBack *VBMasterCheckResp
	castBack = regIfaceCast.(*VBMasterCheckResp)
	assert.Equal(castBack.responsePayload, newResp.responsePayload)
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
