// +build pcre

package parts

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	utilities "github.com/couchbase/goxdcr/utils"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

var filterId string = "testFilter"
var realUtil *utilities.Utilities = utilities.NewUtilities()

func TestKeyAdd(t *testing.T) {
	fmt.Println("============== Test case start: TestKeyAdd =================")
	assert := assert.New(t)

	filter, err := NewFilter(filterId, "Testdoc = true AND REGEXP_CONTAINS(META().id, \"^abc\")", realUtil)

	assert.NotEqual(0, len(base.ReservedWordsReplaceMap))
	assert.Nil(err)
	assert.NotNil(filter)

	testRaw := json.RawMessage(`{"Testdoc": true}`)
	testData, err := testRaw.MarshalJSON()
	assert.Nil(err)
	assert.False(filter.FilterByteSlice(testData))

	tempMap := make(map[string]interface{})
	err = json.Unmarshal(testData, &tempMap)
	tempMap[base.ReservedWordsMap[base.ExternalKeyKey]] = "abcdef"
	testData2, err := json.Marshal(tempMap)
	assert.Nil(err)

	assert.True(filter.FilterByteSlice(testData2))

	fmt.Println("============== Test case end: TestKeyAdd =================")
}

func TestXattrAdd(t *testing.T) {
	fmt.Println("============== Test case start: TestXattrAdd =================")
	assert := assert.New(t)

	filter, err := NewFilter(filterId, "META().xattrs.VersionSupported > 1.0", realUtil)

	assert.Nil(err)
	assert.NotNil(filter)
	assert.True((filter.flags & base.FilterFlagSkipXattr) == 0)
	assert.True((filter.flags & base.FilterFlagSkipKey) > 0)

	testRaw := json.RawMessage(`{"Testdoc": true}`)
	testData, err := testRaw.MarshalJSON()
	assert.Nil(err)
	assert.False(filter.FilterByteSlice(testData))

	tempMap := make(map[string]interface{})
	err = json.Unmarshal(testData, &tempMap)

	testXattr := make(map[string]interface{})
	testXattr["VersionSupported"] = 2.0

	tempMap[base.ReservedWordsMap[base.ExternalKeyXattr]] = testXattr
	testData2, err := json.Marshal(tempMap)
	assert.Nil(err)

	assert.True(filter.FilterByteSlice(testData2))

	fmt.Println("============== Test case end: TestXattrAdd =================")
}

func TestKeyAddXattrPretest(t *testing.T) {
	fmt.Println("============== Test case start: TestKeyAddXattrPretest =================")
	assert := assert.New(t)

	filter, err := NewFilter(filterId, "Testdoc = true AND REGEXP_CONTAINS(META().id, \"^abc\") AND META().xattrs.VersionSupported > 1.0", realUtil)

	assert.Nil(err)
	assert.NotNil(filter)

	testDoc := map[string]interface{}{
		base.ReservedWordsMap[base.ExternalKeyXattr]: map[string]interface{}{
			"VersionSupported": 2.0,
		},
		base.ReservedWordsMap[base.ExternalKeyKey]: "abcdef",
		"Testdoc":  true,
		"Testdoc2": true,
	}

	testData, err := json.Marshal(testDoc)
	assert.Nil(err)

	assert.True(filter.FilterByteSlice(testData))

	fmt.Println("============== Test case end: TestKeyAddXattrPretest =================")
}

func TestKeyAddXattr(t *testing.T) {
	fmt.Println("============== Test case start: TestKeyAddXattr =================")
	assert := assert.New(t)

	filter, err := NewFilter(filterId, "Testdoc = true AND REGEXP_CONTAINS(META().id, \"^abc\") AND META().xattrs.VersionSupported > 1.0", realUtil)

	assert.Nil(err)
	assert.NotNil(filter)

	testDoc := map[string]interface{}{
		"Testdoc":  true,
		"Testdoc2": true,
	}

	testData, err := json.Marshal(testDoc)
	assert.Nil(err)
	assert.False(filter.FilterByteSlice(testData))

	var dataSlice []byte = testData
	dataSlice, err, _ = base.AddKeyToBeFiltered(dataSlice, []byte("abcdef"), nil, nil)
	assert.Nil(err)

	testXattr := make(map[string]interface{})
	testXattr["VersionSupported"] = 2.0
	xattrMapData, err := json.Marshal(testXattr)
	assert.Nil(err)

	dataSlice, err, _ = base.AddXattrToBeFiltered(dataSlice, xattrMapData, nil, nil)
	assert.Nil(err)

	assert.True(filter.FilterByteSlice(dataSlice))
	fmt.Println("============== Test case end: TestKeyAddXattr =================")
}

func TestCompressionXattrKeyFiltering(t *testing.T) {
	fmt.Println("============== Test case start: TestCompressionXattrKeyFiltering =================")
	assert := assert.New(t)

	xAttrCompressedFile := "../utils/testInternalData/uprXattrCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(xAttrCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	var testExpression string = fmt.Sprintf("META().xattrs.AnotherXattr = \"TestValueString\" AND META().xattrs.TestXattr = 30 AND META().id = \"%v\" AND REGEXP_CONTAINS(Key, \"^AA\")", "TestDocKey")
	filter, err := NewFilter(filterId, testExpression, realUtil)
	assert.Nil(err)

	result, err, _, _ := filter.FilterUprEvent(uprEvent)
	assert.True(result)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestCompressionXattrKeyFiltering =================")
}

func TestCompressionKeyFiltering(t *testing.T) {
	fmt.Println("============== Test case start: TestCompressionXattrKeyFiltering =================")
	assert := assert.New(t)

	compressedFile := "../utils/testInternalData/uprCompression.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(compressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "META().id = \"TestDocKey\" AND REGEXP_CONTAINS(`Key`, \"^A+$\")", realUtil)
	assert.Nil(err)

	result, err, _, _ := filter.FilterUprEvent(uprEvent)
	assert.True(result)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestCompressionXattrKeyFiltering =================")
}

func TestReservedWords(t *testing.T) {
	fmt.Println("============== Test case start: TestCompressionXattrKeyFiltering =================")
	assert := assert.New(t)

	filter, err := NewFilter(filterId, "META().id = \"something\"", realUtil)
	assert.Nil(err)
	assert.False(strings.Contains(filter.filterExpressionInternal, "META"))

	filter, err = NewFilter(filterId, "`KEY` = \"something\"", realUtil)
	assert.Nil(err)
	assert.True(strings.Contains(filter.filterExpressionInternal, "KEY"))
	fmt.Println("============== Test case start: TestCompressionXattrKeyFiltering =================")
}
