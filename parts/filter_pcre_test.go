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
	dataSlice, err = base.AddKeyToBeFiltered(dataSlice, []byte("abcdef"))
	assert.Nil(err)

	testXattr := make(map[string]interface{})
	testXattr["VersionSupported"] = 2.0
	xattrMapData, err := json.Marshal(testXattr)
	assert.Nil(err)

	dataSlice, err = base.AddXattrToBeFiltered(dataSlice, xattrMapData)
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

	// NOTE - I screwed this up by using "Key" for internal doc. It's supposed to be KEY so I could test between unescaped KEY vs escaped KEY
	filter, err := NewFilter(filterId, "META().id = \"TestDocKey\" AND REGEXP_CONTAINS(`Key`, \"^A+$\") AND META().xattrs.TestXattr = 30 AND META().xattrs.AnotherXattr = \"TestValueString\"", realUtil)
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

	// NOTE - I screwed this up by using "Key" for internal doc. It's supposed to be KEY so I could test between unescaped KEY vs escaped KEY
	filter, err := NewFilter(filterId, "META().id = \"TestDocKey\" AND REGEXP_CONTAINS(`Key`, \"^A+$\")", realUtil)
	assert.Nil(err)

	result, err, _, _ := filter.FilterUprEvent(uprEvent)
	assert.True(result)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestCompressionXattrKeyFiltering =================")
}

// This is a make-up test to test the KEY vs Key one
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
