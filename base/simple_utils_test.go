package base

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMapUnion(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestMapUnion =================")
	map1 := make(ErrorMap)
	map2 := make(ErrorMap)
	var dummyErr error

	map1["a"] = dummyErr
	map1["b"] = dummyErr

	map2["a"] = dummyErr
	map2["c"] = dummyErr

	assert.Equal(GetUnionOfErrorMapsSize(map1, map2), 3)
	fmt.Println("============== Test case start: TestMapUnion =================")
}

func TestMapUnionNil(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestMapUnionNil =================")
	map1 := make(ErrorMap)
	var dummyErr error

	map1["a"] = dummyErr
	map1["b"] = dummyErr

	assert.Equal(GetUnionOfErrorMapsSize(map1, nil), 2)
	fmt.Println("============== Test case start: TestMapUnionNil =================")
}

func TestMapUnionNils(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestMapUnionNils =================")
	assert.Equal(GetUnionOfErrorMapsSize(nil, nil), 0)
	fmt.Println("============== Test case start: TestMapUnionNils =================")
}
