// +build !pcre

package parts

import (
	"encoding/json"
	"fmt"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	utilities "github.com/couchbase/goxdcr/utils"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

var filterId string = "testFilter"
var realUtil *utilities.Utilities = utilities.NewUtilities()
var dp utilities.DataPoolIface = utilities.NewDataPool()

func TestFilter(t *testing.T) {
	fmt.Println("============== Test case start: TestFilterCreation =================")
	assert := assert.New(t)

	filter, err := NewFilter(filterId, "REGEXP_CONTAINS(META().id, \"^abc\")", realUtil)

	assert.Nil(err)
	assert.NotNil(filter)
	assert.Equal(0, len(base.ReservedWordsReplaceMap))

	fmt.Println("============== Test case end: TestFilterCreation =================")
}

func TestFilterBool(t *testing.T) {
	fmt.Println("============== Test case start: TestFilterBool =================")
	assert := assert.New(t)

	udMarsh, err := ioutil.ReadFile("testData/MB-33010.json")
	assert.Nil(err)

	filter, err := NewFilter(filterId, "bool=false", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)

	matchResult, err := filter.matcher.Match(udMarsh)
	assert.Nil(err)
	assert.True(matchResult)

	fmt.Println("============== Test case end: TestFilterBool =================")
}

func RetrieveUprFile(fileName string) (*mcc.UprEvent, error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	var uprEvent mcc.UprEvent
	err = json.Unmarshal(data, &uprEvent)
	if err != nil {
		return nil, err
	}

	return &uprEvent, nil
}

func TestFilterBool2(t *testing.T) {
	fmt.Println("============== Test case start: TestFilterBool2 =================")
	assert := assert.New(t)

	uprEvent, err := RetrieveUprFile("testData/MB-33010.json")
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "bool=false AND NOT int > dfdfk", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)

	dataSlice, err, _ := realUtil.ProcessUprEventForFiltering(uprEvent, dp, 0 /*flags*/)
	assert.Nil(err)

	matchResult, err := filter.matcher.Match(dataSlice)
	assert.Nil(err)
	assert.False(matchResult)

	fmt.Println("============== Test case end: TestFilterBool2 =================")
}
