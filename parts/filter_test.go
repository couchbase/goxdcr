package parts

import (
	"encoding/json"
	"fmt"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	utilities "github.com/couchbase/goxdcr/utils"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"strings"
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
	assert.Equal(2, len(base.ReservedWordsReplaceMap))
	assert.True(filter.flags&base.FilterFlagSkipKey == 0)
	assert.True(filter.flags&base.FilterFlagSkipXattr > 0)

	fmt.Println("============== Test case end: TestFilterCreation =================")
}

func TestFilterBool(t *testing.T) {
	fmt.Println("============== Test case start: TestFilterBool =================")
	assert := assert.New(t)

	udMarsh, err := ioutil.ReadFile("./testdata/MB-33010.json")
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

	uprEvent, err := RetrieveUprFile("./testdata/MB-33010.json")
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "bool=false AND NOT int > dfdfk", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)

	slices := make([][]byte, 0, 2)
	dataSlice, err, _, _ := realUtil.ProcessUprEventForFiltering(uprEvent, nil, 0, dp, 0 /*flags*/, &slices)
	assert.Nil(err)

	matchResult, err := filter.matcher.Match(dataSlice)
	assert.Nil(err)
	assert.False(matchResult)

	fmt.Println("============== Test case end: TestFilterBool2 =================")
}

func TestFilterPerf(t *testing.T) {
	fmt.Println("============== Test case start: TestFilterPerf =================")
	assert := assert.New(t)

	uprEvent, err := RetrieveUprFile("./testdata/perfData.bin")
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "EXISTS(city)", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)
	assert.Equal(base.FilterFlagType(3), filter.flags)

	slices := make([][]byte, 0, 2)
	dataSlice, err, _, _ := realUtil.ProcessUprEventForFiltering(uprEvent, nil, 0, dp, filter.flags, &slices)
	assert.Nil(err)

	matchResult, err := filter.matcher.Match(dataSlice)
	assert.Nil(err)
	assert.True(matchResult)

	fmt.Println("============== Test case end: TestFilterPerf =================")
}

func TestFilterPerfKeyOnly(t *testing.T) {
	fmt.Println("============== Test case start: TestFilterPerfKeyOnly =================")
	assert := assert.New(t)

	uprEvent, err := RetrieveUprFile("./testdata/perfData.bin")
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "META().id = \"26dcc0-000000001586\"", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)
	assert.True(filter.flags&base.FilterFlagKeyOnly > 0)

	slices := make([][]byte, 0, 2)
	dataSlice, err, _, _ := realUtil.ProcessUprEventForFiltering(uprEvent, nil, 0, dp, filter.flags, &slices)
	assert.Nil(err)

	matchResult, err := filter.matcher.Match(dataSlice)
	assert.Nil(err)
	assert.True(matchResult)

	fmt.Println("============== Test case end: TestFilterPerfKeyOnly =================")
}

func TestKeyPanic(t *testing.T) {
	fmt.Println("============== Test case start: TestKeyPanic =================")
	assert := assert.New(t)

	uprEvent, err := RetrieveUprFile("./testdata/edgyMB-33583.json")
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "REGEXP_CONTAINS(META().id, 'C1-key-1')", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)
	assert.True(filter.flags&base.FilterFlagKeyOnly > 0)

	slices := make([][]byte, 0, 2)
	dataSlice, err, _, _ := realUtil.ProcessUprEventForFiltering(uprEvent, nil, 0, dp, filter.flags, &slices)
	assert.Nil(err)

	matchResult, err := filter.matcher.Match(dataSlice)
	assert.Nil(err)
	assert.False(matchResult)

	fmt.Println("============== Test case end: TestKeyPanic =================")
}

func TestFilterUtilsMethods(t *testing.T) {
	fmt.Println("============== Test case end: TestFilterUtilsMethod =================")
	assert := assert.New(t)
	unCompressedFile := "../utils/testInternalData/uprNotCompress.json"
	compressedFile := "../utils/testInternalData/uprCompression.json"
	xAttrUncompressedFile := "../utils/testInternalData/uprXattrNotCompress.json"
	xAttrCompressedFile := "../utils/testInternalData/uprXattrCompress.json"

	uprEvent, err := base.RetrieveUprJsonAndConvert(unCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)
	filter, err := NewFilter(filterId, "REGEXP_CONTAINS(Key, \"^A+\")", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)
	match, err, _, _ := filter.FilterUprEvent(uprEvent)
	assert.Nil(err)
	assert.True(match)

	uprEvent, err = base.RetrieveUprJsonAndConvert(compressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)
	filter, err = NewFilter(filterId, "REGEXP_CONTAINS(Key, \"^A+\")", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)
	match, err, _, _ = filter.FilterUprEvent(uprEvent)
	assert.Nil(err)
	assert.True(match)

	uprEvent, err = base.RetrieveUprJsonAndConvert(xAttrUncompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)
	filter, err = NewFilter(filterId, "META().xattrs.AnotherXattr = \"TestValueString\" AND META().xattrs.TestXattr = 30 AND META().id = \"TestDocKey\"", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)
	match, err, _, _ = filter.FilterUprEvent(uprEvent)
	assert.Nil(err)
	assert.True(match)

	uprEvent, err = base.RetrieveUprJsonAndConvert(xAttrCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)
	filter, err = NewFilter(filterId, "META().xattrs.AnotherXattr = \"TestValueString\" AND META().xattrs.TestXattr = 30", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)
	match, err, _, _ = filter.FilterUprEvent(uprEvent)
	assert.Nil(err)
	assert.True(match)
	fmt.Println("============== Test case end: TestFilterUtilsMethod =================")
}

// test that active transaction record is filtered out
func TestActiveTxnRecordFiltering(t *testing.T) {
	fmt.Println("============== Test case start: TestActiveTxnRecordFiltering =================")
	assert := assert.New(t)

	activeTxnRecordFile := "../utils/testInternalData/uprActiveTxnRecordNotCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(activeTxnRecordFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)

	result, err, _, _ := filter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestActiveTxnRecordFiltering =================")
}

// test that transaction client record is filtered out
func TestTxnClientRecordFiltering(t *testing.T) {
	fmt.Println("============== Test case start: TestTxnClientRecordFiltering =================")
	assert := assert.New(t)

	txnClientRecordFile := "../utils/testInternalData/uprTxnClientRecordNotCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(txnClientRecordFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)

	result, err, _, _ := filter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestTxnClientRecordFiltering =================")
}

// test that txn attrs, when not mixed with non txn attrs, are filtered out
func TestTransXattrOnlyFilteringWithoutCompression(t *testing.T) {
	fmt.Println("============== Test case start: TestTransXattrOnlyFilteringWithoutCompression =================")
	assert := assert.New(t)

	transXattrFile := "../utils/testInternalData/uprTransXattrOnlyNotCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(transXattrFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)
	result, err, _, _ := filter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestTransXattrOnlyFilteringWithoutCompression =================")
}

// test that txn attrs, when not mixed with non txn attrs, are filtered out
func TestTransXattrOnlyFilteringWithCompression(t *testing.T) {
	fmt.Println("============== Test case start: TestTransXattrOnlyFilteringWithCompression =================")
	assert := assert.New(t)

	transXattrFile := "../utils/testInternalData/uprTransXattrOnlyCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(transXattrFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)
	result, err, _, _ := filter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestTransXattrOnlyFilteringWithCompression =================")
}

// test that txn attrs, when mixed with non txn attrs, are filtered out
func TestMixedXattrFilteringWithCompression(t *testing.T) {
	fmt.Println("============== Test case start: TestMixedXattrFilteringWithCompression =================")
	assert := assert.New(t)

	transXattrCompressedFile := "../utils/testInternalData/uprTransXattrCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(transXattrCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)
	result, err, _, _ := filter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestMixedXattrFilteringWithCompression =================")
}

func TestMixedTransXattrFilteringWithoutCompression(t *testing.T) {
	fmt.Println("============== Test case start: TestMixedTransXattrFilteringWithoutCompression =================")
	assert := assert.New(t)

	transXattrNotCompressedFile := "../utils/testInternalData/uprTransXattrNotCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(transXattrNotCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)
	result, err, _, _ := filter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestMixedTransXattrFilteringWithoutCompression =================")
}

// Regression test - make sure that when txn xattrs are not present, none txn related attrs
// can be processed correctly
func TestNonTransXattrFilteringWithoutCompression(t *testing.T) {
	fmt.Println("============== Test case start: TestNonTransXattrFilteringWithoutCompression =================")
	assert := assert.New(t)

	xattrNotCompressedFile := "../utils/testInternalData/uprXattrNotCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(xattrNotCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "META().xattrs.TestXattr = 30", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)
	result, err, _, _ := filter.FilterUprEvent(uprEvent)
	assert.True(result)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestNonTransXattrFilteringWithoutCompression =================")
}

// Regression test - make sure that when txn xattrs are not present, none txn related attrs
// can be processed correctly
func TestNonTransXattrFilteringWithoutCompressionNegative(t *testing.T) {
	fmt.Println("============== Test case start: TestNonTransXattrFilteringWithoutCompressionNegative =================")
	assert := assert.New(t)

	xattrNotCompressedFile := "../utils/testInternalData/uprXattrNotCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(xattrNotCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "META().xattrs.TestXattr = 31", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)
	result, err, _, _ := filter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestNonTransXattrFilteringWithoutCompressionNegative =================")
}

// Regression test - make sure that when txn xattrs are not present, none txn related attrs
// can be processed correctly
func TestNonTransXattrFilteringWithCompression(t *testing.T) {
	fmt.Println("============== Test case start: TestNonTransXattrFilteringWithCompression =================")
	assert := assert.New(t)

	xattrCompressedFile := "../utils/testInternalData/uprXattrCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(xattrCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "META().xattrs.TestXattr = 30", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)
	result, err, _, _ := filter.FilterUprEvent(uprEvent)
	assert.True(result)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestNonTransXattrFilteringWithCompression =================")
}

// Regression test - make sure that when txn xattrs are not present, none txn related attrs
// can be processed correctly
func TestNonTransXattrFilteringWithCompressionNegative(t *testing.T) {
	fmt.Println("============== Test case start: TestNonTransXattrFilteringWithCompressionNegative =================")
	assert := assert.New(t)

	xattrCompressedFile := "../utils/testInternalData/uprXattrCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(xattrCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "META().xattrs.TestXattr = 31", realUtil)
	assert.Nil(err)
	assert.NotNil(filter)
	result, err, _, _ := filter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestNonTransXattrFilteringWithCompressionNegative =================")
}

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
	testKey := []byte("abcdef")
	dataSlice, err, _, lastBracketPos := realUtil.AddKeyToBeFiltered(dataSlice, testKey, nil, nil, len(testKey)-1)
	assert.Nil(err)
	assert.Equal("}", string(dataSlice[lastBracketPos]))

	testXattr := make(map[string]interface{})
	testXattr["VersionSupported"] = 2.0
	xattrMapData, err := json.Marshal(testXattr)
	assert.Nil(err)

	dataSlice, err = base.AddXattrToBeFilteredWithoutDP(dataSlice, xattrMapData)
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
