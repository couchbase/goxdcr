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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/base/filter"
	utils2 "github.com/couchbase/goxdcr/v8/utils"
	"github.com/stretchr/testify/assert"
)

func TestDiscoveryReqMarsh(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestDiscoveryReqMarsh =================")
	defer fmt.Println("============== Test case end: TestDiscoveryReqMarsh =================")

	common := NewRequestCommon("127.0.0.1:9000", "127.0.0.1:9001", "randId", "", getOpaqueWrapper())
	discoveryReq := NewP2PDiscoveryReq(common)

	bytes, err := discoveryReq.Serialize()
	assert.Nil(err)

	testValidate := &DiscoveryRequest{}
	err = testValidate.DeSerialize(bytes)
	assert.Nil(err)

	assert.True(discoveryReq.SameAs(testValidate))
}

func TestMagic(t *testing.T) {
	assert := assert.New(t)

	fmt.Println("============== Test case start: TestMagic =================")
	defer fmt.Println("============== Test case end: TestMagic =================")
	reqCommon := RequestCommon{
		Magic:             ReqMagic,
		ReqType:           ReqDiscovery,
		Sender:            "test",
		TargetAddr:        "test",
		Opaque:            0,
		LocalLifeCycleId:  "test",
		RemoteLifeCycleId: "test",
	}

	req := NewP2PDiscoveryReq(reqCommon)
	bytes, err := req.Serialize()
	assert.Nil(err)

	utils := utils2.NewUtilities()
	reqFilter, err := filter.NewFilter("magicCheckReq", fmt.Sprintf("Magic=%d", ReqMagic), utils, 0, base.MobileCompatibilityOff)
	matched, _, err := reqFilter.FilterByteSlice(bytes)
	assert.Nil(err)
	assert.True(matched)

	resp := req.GenerateResponse().(*DiscoveryResponse)
	bytes, err = resp.Serialize()
	assert.Nil(err)

	respFilter, err := filter.NewFilter("magicCheckResp", fmt.Sprintf("Magic=%d", RespMagic), utils, 0, base.MobileCompatibilityOff)
	matched, _, err = respFilter.FilterByteSlice(bytes)
	assert.Nil(err)
	assert.True(matched)

	matched, _, err = reqFilter.FilterByteSlice(bytes)
	assert.Nil(err)
	assert.False(matched)

	respCheck := &DiscoveryResponse{}
	err = json.Unmarshal(bytes, &respCheck)
	assert.Nil(err)
}

func TestVBMasterCheckReq(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestVBMasterCheckReq =================")
	defer fmt.Println("============== Test case end: TestVBMasterCheckReq =================")

	common := NewRequestCommon("127.0.0.1:9000", "127.0.0.1:9001", "randId", "", getOpaqueWrapper())
	checkReq := NewVBMasterCheckReq(common)
	checkReq.bucketVBMap = make(BucketVBMapType)
	checkReq.bucketVBMap["testBucket"] = []uint16{0, 1, 2, 3}

	bytes, err := checkReq.Serialize()
	assert.Nil(err)

	testValidate := &VBMasterCheckReq{}
	err = testValidate.DeSerialize(bytes)
	assert.Nil(err)

	assert.True(testValidate.SameAs(checkReq))
}
