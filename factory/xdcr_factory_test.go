// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package factory

import (
	"fmt"
	"github.com/couchbase/goxdcr/peerToPeer"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFactoryParseVBMasterResp(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestFactoryParseVBMasterResp =================")
	defer fmt.Println("============== Test case end: TestFactoryParseVBMasterResp =================")

	sourceVBs := []uint16{0, 1, 2, 3}
	srcBucketName := "srcBucket"
	respMap := make(map[string]*peerToPeer.VBMasterCheckResp)

	payloadType := make(peerToPeer.BucketVBMPayloadType)
	notMyVbs := make(peerToPeer.VBsPayload)
	payloadType[srcBucketName] = &peerToPeer.VBMasterPayload{
		OverallPayloadErr: "",
		NotMyVBs:          &notMyVbs,
		ConflictingVBs:    nil,
	}
	for _, vb := range sourceVBs {
		srcBucketNotMyVbs := *payloadType[srcBucketName].NotMyVBs
		srcBucketNotMyVbs[vb] = &peerToPeer.Payload{}
	}

	respMap["testHost"] = peerToPeer.NewVBMasterCheckRespGivenPayload(payloadType)

	err := checkNoOtherVBMasters(respMap, srcBucketName, sourceVBs)
	assert.Nil(err)

	// Take out one VB and test
	delete(notMyVbs, sourceVBs[0])
	respMap["testHost"] = peerToPeer.NewVBMasterCheckRespGivenPayload(payloadType)

	err = checkNoOtherVBMasters(respMap, srcBucketName, sourceVBs)
	assert.NotNil(err)
}
