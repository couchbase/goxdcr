package memcached

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCollectionsFilter(t *testing.T) {
	fmt.Println("============== Test case start: TestCollectionsFilter =================")
	assert := assert.New(t)

	var testFilter CollectionsFilter
	testFilter.ScopeId = 129
	assert.Nil(testFilter.IsValid())
	data, err := testFilter.ToStreamReqBody()
	assert.NotNil(data)
	assert.Nil(err)
	assert.Equal(`{"scope":"81"}`, string(data))

	testFilter = CollectionsFilter{}
	testFilter.CollectionsList = append(testFilter.CollectionsList, 2)
	testFilter.CollectionsList = append(testFilter.CollectionsList, 20)
	testFilter.CollectionsList = append(testFilter.CollectionsList, 200)
	data, err = testFilter.ToStreamReqBody()
	assert.NotNil(data)
	assert.Nil(err)
	assert.Equal(`{"collections":["2","14","c8"]}`, string(data))

	fmt.Println("============== Test case end: TestCollectionsFilter =================")
}
