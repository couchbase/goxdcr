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
