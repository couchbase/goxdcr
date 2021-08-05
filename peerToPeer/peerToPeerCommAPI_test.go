package peerToPeer

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/goxdcr/base/filter"
	utils2 "github.com/couchbase/goxdcr/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDiscoveryReqMarsh(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestDiscoveryReqMarsh =================")
	defer fmt.Println("============== Test case end: TestDiscoveryReqMarsh =================")

	common := NewRequestCommon("127.0.0.1:9000", "127.0.0.1:9001", "randId", "", getOpaqueWrapper())
	discoveryReq := NewP2PDiscoveryReq(common)

	bytes, err := discoveryReq.Serialize()
	assert.Nil(err)

	testValidate := DiscoveryRequest{}
	err = testValidate.DeSerialize(bytes)
	assert.Nil(err)

	fmt.Printf("testValidate: %v\n", testValidate)
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
	reqFilter, err := filter.NewFilter("magicCheckReq", fmt.Sprintf("Magic=%d", ReqMagic), utils)
	matched, _, err := reqFilter.FilterByteSlice(bytes)
	assert.Nil(err)
	assert.True(matched)

	resp := req.GenerateResponse().(*DiscoveryResponse)
	bytes, err = resp.Serialize()
	assert.Nil(err)

	respFilter, err := filter.NewFilter("magicCheckResp", fmt.Sprintf("Magic=%d", RespMagic), utils)
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
