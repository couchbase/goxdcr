/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software
will be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadata

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

var testExtDataDir = "../utils/testExternalData/"

// Copied over from utils test
func readJsonHelper(fileName string) (retMap map[string]interface{}, byteSlice []byte, err error) {
	byteSlice, err = ioutil.ReadFile(fileName)
	if err != nil {
		return
	}
	var unmarshalledIface interface{}
	err = json.Unmarshal(byteSlice, &unmarshalledIface)
	if err != nil {
		return
	}

	retMap = unmarshalledIface.(map[string]interface{})
	return
}

func get651CloudDefaultPool() (map[string]interface{}, error) {
	fileName := fmt.Sprintf("%v%v", testExtDataDir, "cloudDefaultPoolInfo.json")
	retMap, _, err := readJsonHelper(fileName)
	return retMap, err
}

func Test651Capability(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: Test651Capability =================")
	defer fmt.Println("============== Test case done: Test651Capability =================")

	cloudDefaultPool, err := get651CloudDefaultPool()
	if err != nil {
		panic(err)
	}
	assert.NotNil(cloudDefaultPool)

	var capability Capability
	capability.LoadFromDefaultPoolInfo(cloudDefaultPool, nil)

	assert.False(capability.HasCollectionSupport())
}
