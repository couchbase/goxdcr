/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package filter

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"
	"testing"

	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
)

var filterId string = "testFilter"
var realUtil = &FilterUtilsImpl{}
var dp base.DataPool = base.NewDataPool()

func TestFilter(t *testing.T) {
	fmt.Println("============== Test case start: TestFilterCreation =================")
	assert := assert.New(t)

	filter, err := NewFilter(filterId, "REGEXP_CONTAINS(META().id, \"^abc\")", realUtil, 0, base.MobileCompatibilityOff)

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

	filter, err := NewFilter(filterId, "bool=false", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)

	matchResult, err := filter.matcher.Match(udMarsh)
	assert.Nil(err)
	assert.True(matchResult)

	fmt.Println("============== Test case end: TestFilterBool =================")
}

func RetrieveUprFile(fileName string) (*base.WrappedUprEvent, error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	var uprEvent mcc.UprEvent
	err = json.Unmarshal(data, &uprEvent)
	if err != nil {
		return nil, err
	}

	wrappedUpr := &base.WrappedUprEvent{
		UprEvent:     &uprEvent,
		ColNamespace: nil,
		Flags:        0,
		ByteSliceGetter: func(size uint64) ([]byte, error) {
			return make([]byte, int(size)), nil
		},
	}
	return wrappedUpr, nil
}

func TestFilterBool2(t *testing.T) {
	fmt.Println("============== Test case start: TestFilterBool2 =================")
	assert := assert.New(t)

	uprEvent, err := RetrieveUprFile("./testdata/MB-33010.json")
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "bool=false AND NOT int > dfdfk", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)

	slices := make([][]byte, 0, 2)
	dataSlice, err, _, _ := realUtil.ProcessUprEventForFiltering(uprEvent.UprEvent, nil, 0, dp, 0 /*flags*/, &slices)
	assert.Nil(err)

	matchResult, err := filter.matcher.Match(dataSlice)
	assert.Nil(err)
	assert.False(matchResult)

	fmt.Println("============== Test case end: TestFilterBool2 =================")
}

func TestFilterOnDocWithWhiteSpaces(t *testing.T) {
	fmt.Println("============== Test case start: TestFilterOnDocWithWhiteSpaces =================")
	assert := assert.New(t)

	uprEvent, err := RetrieveUprFile("./testdata/MB-60674UprWithWhitespaceAtEnd.json")
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "__t=\"cos\"", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)
	assert.Equal(base.FilterFlagType(3), filter.flags)

	slices := make([][]byte, 0, 2)
	dataSlice, err, _, _ := realUtil.ProcessUprEventForFiltering(uprEvent.UprEvent, nil, 0, dp, filter.flags, &slices)
	assert.Nil(err)

	matchResult, err := filter.matcher.Match(dataSlice)
	assert.Nil(err)
	assert.True(matchResult)

	fmt.Println("============== Test case end: TestFilterOnDocWithWhiteSpaces =================")
}

func TestFilterPerf(t *testing.T) {
	fmt.Println("============== Test case start: TestFilterPerf =================")
	assert := assert.New(t)

	uprEvent, err := RetrieveUprFile("./testdata/perfData.bin")
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "EXISTS(city)", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)
	assert.Equal(base.FilterFlagType(3), filter.flags)

	slices := make([][]byte, 0, 2)
	dataSlice, err, _, _ := realUtil.ProcessUprEventForFiltering(uprEvent.UprEvent, nil, 0, dp, filter.flags, &slices)
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

	filter, err := NewFilter(filterId, "META().id = \"26dcc0-000000001586\"", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)
	assert.True(filter.flags&base.FilterFlagKeyOnly > 0)

	slices := make([][]byte, 0, 2)
	dataSlice, err, _, _ := realUtil.ProcessUprEventForFiltering(uprEvent.UprEvent, nil, 0, dp, filter.flags, &slices)
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

	filter, err := NewFilter(filterId, "REGEXP_CONTAINS(META().id, 'C1-key-1')", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)
	assert.True(filter.flags&base.FilterFlagKeyOnly > 0)

	slices := make([][]byte, 0, 2)
	dataSlice, err, _, _ := realUtil.ProcessUprEventForFiltering(uprEvent.UprEvent, nil, 0, dp, filter.flags, &slices)
	assert.Nil(err)

	matchResult, err := filter.matcher.Match(dataSlice)
	assert.Nil(err)
	assert.False(matchResult)

	fmt.Println("============== Test case end: TestKeyPanic =================")
}

func TestFilterUtilsMethods(t *testing.T) {
	fmt.Println("============== Test case end: TestFilterUtilsMethod =================")
	assert := assert.New(t)
	unCompressedFile := "../../utils/testInternalData/uprNotCompress.json"
	compressedFile := "../../utils/testInternalData/uprCompression.json"
	xAttrUncompressedFile := "../../utils/testInternalData/uprXattrNotCompress.json"
	xAttrCompressedFile := "../../utils/testInternalData/uprXattrCompress.json"

	uprEvent, err := base.RetrieveUprJsonAndConvert(unCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)
	filter, err := NewFilter(filterId, "REGEXP_CONTAINS(Key, \"^A+\")", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)
	match, err, _, _, status := filter.FilterUprEvent(uprEvent)
	assert.Nil(err)
	assert.True(match)
	assert.Equal(status, NotFiltered)

	uprEvent, err = base.RetrieveUprJsonAndConvert(compressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)
	filter, err = NewFilter(filterId, "REGEXP_CONTAINS(Key, \"^A+\")", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)
	match, err, _, _, status = filter.FilterUprEvent(uprEvent)
	assert.Nil(err)
	assert.True(match)
	assert.Equal(status, NotFiltered)

	uprEvent, err = base.RetrieveUprJsonAndConvert(xAttrUncompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)
	filter, err = NewFilter(filterId, "META().xattrs.AnotherXattr = \"TestValueString\" AND META().xattrs.TestXattr = 30 AND META().id = \"TestDocKey\"", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)
	match, err, _, _, status = filter.FilterUprEvent(uprEvent)
	assert.Nil(err)
	assert.True(match)
	assert.Equal(status, NotFiltered)

	uprEvent, err = base.RetrieveUprJsonAndConvert(xAttrCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)
	filter, err = NewFilter(filterId, "META().xattrs.AnotherXattr = \"TestValueString\" AND META().xattrs.TestXattr = 30", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)
	match, err, _, _, status = filter.FilterUprEvent(uprEvent)
	assert.Nil(err)
	assert.True(match)
	assert.Equal(status, NotFiltered)
	fmt.Println("============== Test case end: TestFilterUtilsMethod =================")
}

// test that active transaction record is filtered out
func TestActiveTxnRecordFiltering(t *testing.T) {
	fmt.Println("============== Test case start: TestActiveTxnRecordFiltering =================")
	assert := assert.New(t)

	activeTxnRecordFile := "../../utils/testInternalData/uprActiveTxnRecordNotCompressV2.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(activeTxnRecordFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	var filterType base.FilterExpDelType
	filterType.SetSkipReplicateUncommittedTxn(true)
	filter, err := NewFilter(filterId, "", realUtil, filterType, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)

	result, err, _, _, status := filter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)
	assert.Equal(status, FilteredOnATRDocument)

	fmt.Println("============== Test case end: TestActiveTxnRecordFiltering =================")
}

// test that transaction client record is filtered out
func TestTxnClientRecordFiltering(t *testing.T) {
	fmt.Println("============== Test case start: TestTxnClientRecordFiltering =================")
	assert := assert.New(t)

	txnClientRecordFile := "../../utils/testInternalData/uprTxnClientRecordNotCompressV2.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(txnClientRecordFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	var filterType base.FilterExpDelType
	filterType.SetSkipReplicateUncommittedTxn(true)
	filter, err := NewFilter(filterId, "", realUtil, filterType, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)

	result, err, _, _, status := filter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)
	assert.Equal(status, FilteredOnTxnClientRecord)

	fmt.Println("============== Test case end: TestTxnClientRecordFiltering =================")
}

// The document here seems to have one Xattr KV pair for transaction, and a weird but valid JSON body
func TestTransXattrOnlyFilteringWithoutCompression(t *testing.T) {
	fmt.Println("============== Test case start: TestTransXattrOnlyFilteringWithoutCompression =================")
	defer fmt.Println("============== Test case end: TestTransXattrOnlyFilteringWithoutCompression =================")
	assert := assert.New(t)

	transXattrFile := "../../utils/testInternalData/uprTransXattrOnlyNotCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(transXattrFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	assert.False(uprEvent.UprEvent.IsSnappyDataType())
	strippedData, err := base.StripXattrAndGetBody(uprEvent.UprEvent.Value)
	assert.Nil(err)

	// Validate that the body is a valid json
	type dummyStruct struct{}
	testStruct := dummyStruct{}
	assert.Nil(json.Unmarshal(strippedData, &testStruct))

	// Make sure filter can detect the transaction xattribute
	var filterType base.FilterExpDelType
	filterType.SetSkipReplicateUncommittedTxn(true)
	legacyFilter, err := NewFilter(filterId, "", realUtil, filterType, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(legacyFilter)
	result, err, _, _, status := legacyFilter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)
	assert.Equal(status, FilteredOnTxnsXattr)

	// Without skip, make sure it replicates and that it can strip the xattribute
	stripFilter, err := NewFilter(filterId, "", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(stripFilter)
	result, err, _, _, status = stripFilter.FilterUprEvent(uprEvent)
	assert.True(result)
	assert.Nil(err)
	assert.Equal(status, NotFiltered)

	body := uprEvent.DecompressedValue
	assert.True(uprEvent.Flags.ShouldUseDecompressedValue())
	assert.True(reflect.DeepEqual(body, strippedData))

}

// test that txn attrs, when mixed with non txn attrs, are filtered out
func TestMixedXattrFilteringWithCompression(t *testing.T) {
	fmt.Println("============== Test case start: TestMixedXattrFilteringWithCompression =================")
	assert := assert.New(t)

	transXattrCompressedFile := "../../utils/testInternalData/uprTransXattrCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(transXattrCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	assert.True(uprEvent.UprEvent.IsSnappyDataType())

	// Before decompress, xattr should have some error
	xattrIter, err := base.NewXattrIterator(uprEvent.UprEvent.Value)
	assert.NotNil(err)
	assert.Nil(xattrIter)

	// Uncompress should have no problem
	uncompressedUprEventVal, err := snappy.Decode(nil, uprEvent.UprEvent.Value)
	assert.Nil(err)

	// After decompress, xattr iterator should have no error
	xattrIter, err = base.NewXattrIterator(uncompressedUprEventVal)
	assert.Nil(err)
	assert.NotNil(xattrIter)
	origXattrSize, err := base.GetXattrSize(uncompressedUprEventVal)
	assert.Nil(err)

	// Calculate total number of xattributes
	var originalTotalXattrCnt int
	var transactionXattrFound bool
	for xattrIter.HasNext() {
		originalTotalXattrCnt++
		key, value, err := xattrIter.Next()
		assert.Nil(err)
		if string(key) == base.TransactionXattrKey {
			transactionXattrFound = true
			assert.NotNil(value)
		}
	}
	assert.True(transactionXattrFound)

	strippedData, err := base.StripXattrAndGetBody(uncompressedUprEventVal)
	assert.Nil(err)

	// Validate that the body is a valid json
	type dummyStruct struct{}
	testStruct := dummyStruct{}
	assert.Nil(json.Unmarshal(strippedData, &testStruct))

	var filterType base.FilterExpDelType
	filterType.SetSkipReplicateUncommittedTxn(true)
	legacyFilter, err := NewFilter(filterId, "", realUtil, filterType, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(legacyFilter)
	result, err, _, _, status := legacyFilter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)
	assert.Equal(status, FilteredOnTxnsXattr)

	sdkFilter, err := NewFilter(filterId, "", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(sdkFilter)
	result, err, _, _, status = sdkFilter.FilterUprEvent(uprEvent)
	assert.True(result)
	assert.Nil(err)
	assert.Equal(status, NotFiltered)

	var dummySlice [][]byte
	result, body, endPos, err, _, _, _, status := sdkFilter.filterTransactionRelatedUprEvent(uprEvent.UprEvent, &dummySlice)
	assert.True(result)
	assert.NotNil(body)
	assert.True(endPos <= len(body))
	assert.Nil(err)
	assert.Equal(status, NotFiltered)

	// The body returned is xattributes + body
	bodyWoXattr, err := base.StripXattrAndGetBody(body)
	assert.Nil(err)
	assert.True(reflect.DeepEqual(bodyWoXattr, strippedData))

	// Xattrs should not have SDK transaction Xattribute
	xattrIter, err = base.NewXattrIterator(body)
	assert.Nil(err)
	newXattrSize, err := base.GetXattrSize(body)
	assert.Nil(err)
	assert.True(newXattrSize < origXattrSize)
	var postStripXattrCnt int
	for xattrIter.HasNext() {
		postStripXattrCnt++
		key, _, err := xattrIter.Next()
		assert.NotEqual(key, base.TransactionXattrKey)
		assert.Nil(err)
	}
	assert.Equal(postStripXattrCnt, originalTotalXattrCnt-1)
	fmt.Println("============== Test case end: TestMixedXattrFilteringWithCompression =================")
}

// test that txn attrs and only txn exist by themselves, are filtered out and body + flags updated correctly
func TestTxnOnlyXattrFilteringWithCompression(t *testing.T) {
	fmt.Println("============== Test case start: TestTxnOnlyXattrFilteringWithCompression =================")
	defer fmt.Println("============== Test case end: TestTxnOnlyXattrFilteringWithCompression =================")
	assert := assert.New(t)

	transXattrCompressedFile := "../../utils/testFilteringData/uprTransXattrOnlyCompressed.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(transXattrCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	assert.True(uprEvent.UprEvent.IsSnappyDataType())

	// Before decompress, xattr should have some error
	xattrIter, err := base.NewXattrIterator(uprEvent.UprEvent.Value)
	assert.NotNil(err)
	assert.Nil(xattrIter)

	// Uncompress should have no problem
	uncompressedUprEventVal, err := snappy.Decode(nil, uprEvent.UprEvent.Value)
	assert.Nil(err)

	// After decompress, xattr iterator should have no error
	xattrIter, err = base.NewXattrIterator(uncompressedUprEventVal)
	assert.Nil(err)
	assert.NotNil(xattrIter)
	assert.Nil(err)

	// Calculate total number of xattributes
	var originalTotalXattrCnt int
	var transactionXattrFound bool
	for xattrIter.HasNext() {
		originalTotalXattrCnt++
		key, value, err := xattrIter.Next()
		assert.Nil(err)
		if string(key) == base.TransactionXattrKey {
			transactionXattrFound = true
			assert.NotNil(value)
		}
	}
	assert.True(transactionXattrFound)
	// Before filtering out txn xattr, xattr flag must be set
	assert.True(uprEvent.UprEvent.DataType&mcc.XattrDataType > 0)

	strippedData, err := base.StripXattrAndGetBody(uncompressedUprEventVal)
	assert.Nil(err)

	// Validate that the body is a valid json
	type dummyStruct struct{}
	testStruct := dummyStruct{}
	assert.Nil(json.Unmarshal(strippedData, &testStruct))

	var filterType base.FilterExpDelType
	filterType.SetSkipReplicateUncommittedTxn(true)
	legacyFilter, err := NewFilter(filterId, "", realUtil, filterType, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(legacyFilter)
	result, err, _, _, status := legacyFilter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)
	assert.Equal(status, FilteredOnTxnsXattr)

	sdkFilter, err := NewFilter(filterId, "", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(sdkFilter)
	result, err, _, _, status = sdkFilter.FilterUprEvent(uprEvent)
	assert.True(result)
	assert.Nil(err)
	assert.Equal(status, NotFiltered)

	// Afterwards, no xattr flag
	assert.True(uprEvent.UprEvent.DataType&mcc.XattrDataType == 0)
	assert.True(uprEvent.Flags.ShouldUseDecompressedValue())
	body := uprEvent.DecompressedValue

	_, err = base.StripXattrAndGetBody(body)
	// This now fails because the body has no more xattribute
	assert.NotNil(err)
	assert.True(reflect.DeepEqual(body, strippedData))

	// Xattrs should not have any XAttribute, including SDK transaction Xattribute
	_, err = base.GetXattrSize(body)
	assert.NotNil(err)
	_, err = base.NewXattrIterator(body)
	assert.NotNil(err)
}

func TestMixedTransXattrFilteringWithoutCompression(t *testing.T) {
	fmt.Println("============== Test case start: TestMixedTransXattrFilteringWithoutCompression =================")
	assert := assert.New(t)

	transXattrNotCompressedFile := "../../utils/testInternalData/uprTransXattrNotCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(transXattrNotCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	var filterType base.FilterExpDelType
	filterType.SetSkipReplicateUncommittedTxn(true)
	filter, err := NewFilter(filterId, "", realUtil, filterType, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)
	result, err, _, _, status := filter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)
	assert.Equal(status, FilteredOnTxnsXattr)

	fmt.Println("============== Test case end: TestMixedTransXattrFilteringWithoutCompression =================")
}

// Regression test - make sure that when txn xattrs are not present, none txn related attrs
// can be processed correctly
func TestNonTransXattrFilteringWithoutCompression(t *testing.T) {
	fmt.Println("============== Test case start: TestNonTransXattrFilteringWithoutCompression =================")
	assert := assert.New(t)

	xattrNotCompressedFile := "../../utils/testInternalData/uprXattrNotCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(xattrNotCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	var filterType base.FilterExpDelType
	filterType.SetSkipReplicateUncommittedTxn(true)
	filter, err := NewFilter(filterId, "META().xattrs.TestXattr = 30", realUtil, filterType, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)
	result, err, _, _, status := filter.FilterUprEvent(uprEvent)
	assert.True(result)
	assert.Nil(err)
	assert.Equal(status, NotFiltered)

	fmt.Println("============== Test case end: TestNonTransXattrFilteringWithoutCompression =================")
}

// Regression test - make sure that when txn xattrs are not present, none txn related attrs
// can be processed correctly
func TestNonTransXattrFilteringWithoutCompressionNegative(t *testing.T) {
	fmt.Println("============== Test case start: TestNonTransXattrFilteringWithoutCompressionNegative =================")
	assert := assert.New(t)

	xattrNotCompressedFile := "../../utils/testInternalData/uprXattrNotCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(xattrNotCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	var filterType base.FilterExpDelType
	filterType.SetSkipReplicateUncommittedTxn(true)
	filter, err := NewFilter(filterId, "META().xattrs.TestXattr = 31", realUtil, filterType, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)
	result, err, _, _, status := filter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)
	assert.Equal(status, FilteredOnUserDefinedFilter)

	fmt.Println("============== Test case end: TestNonTransXattrFilteringWithoutCompressionNegative =================")
}

// Regression test - make sure that when txn xattrs are not present, none txn related attrs
// can be processed correctly
func TestNonTransXattrFilteringWithCompression(t *testing.T) {
	fmt.Println("============== Test case start: TestNonTransXattrFilteringWithCompression =================")
	assert := assert.New(t)

	xattrCompressedFile := "../../utils/testInternalData/uprXattrCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(xattrCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	var filterType base.FilterExpDelType
	filterType.SetSkipReplicateUncommittedTxn(true)
	filter, err := NewFilter(filterId, "META().xattrs.TestXattr = 30", realUtil, filterType, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)
	result, err, _, _, status := filter.FilterUprEvent(uprEvent)
	assert.True(result)
	assert.Nil(err)
	assert.Equal(status, NotFiltered)

	fmt.Println("============== Test case end: TestNonTransXattrFilteringWithCompression =================")
}

// Regression test - make sure that when txn xattrs are not present, none txn related attrs
// can be processed correctly
func TestNonTransXattrFilteringWithCompressionNegative(t *testing.T) {
	fmt.Println("============== Test case start: TestNonTransXattrFilteringWithCompressionNegative =================")
	assert := assert.New(t)

	xattrCompressedFile := "../../utils/testInternalData/uprXattrCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(xattrCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	var filterType base.FilterExpDelType
	filterType.SetSkipReplicateUncommittedTxn(true)
	filter, err := NewFilter(filterId, "META().xattrs.TestXattr = 31", realUtil, filterType, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)
	result, err, _, _, status := filter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)
	assert.Equal(status, FilteredOnUserDefinedFilter)

	fmt.Println("============== Test case end: TestNonTransXattrFilteringWithCompressionNegative =================")
}

func TestKeyAdd(t *testing.T) {
	fmt.Println("============== Test case start: TestKeyAdd =================")
	assert := assert.New(t)

	filter, err := NewFilter(filterId, "Testdoc = true AND REGEXP_CONTAINS(META().id, \"^abc\")", realUtil, 0, base.MobileCompatibilityOff)

	assert.NotEqual(0, len(base.ReservedWordsReplaceMap))
	assert.Nil(err)
	assert.NotNil(filter)

	testRaw := json.RawMessage(`{"Testdoc": true}`)
	testData, err := testRaw.MarshalJSON()
	assert.Nil(err)
	matched, status, err := filter.FilterByteSlice(testData)
	assert.False(matched)
	assert.Equal(0, status)
	assert.Nil(err)

	tempMap := make(map[string]interface{})
	err = json.Unmarshal(testData, &tempMap)
	tempMap[base.ReservedWordsMap[base.ExternalKeyKey]] = "abcdef"
	testData2, err := json.Marshal(tempMap)
	assert.Nil(err)

	matched, status, err = filter.FilterByteSlice(testData2)
	assert.True(matched)
	assert.Equal(0, status)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestKeyAdd =================")
}

func TestXattrAdd(t *testing.T) {
	fmt.Println("============== Test case start: TestXattrAdd =================")
	assert := assert.New(t)

	filter, err := NewFilter(filterId, "META().xattrs.VersionSupported > 1.0", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(filter)
	assert.True((filter.flags & base.FilterFlagSkipXattr) == 0)
	assert.True((filter.flags & base.FilterFlagSkipKey) > 0)

	testRaw := json.RawMessage(`{"Testdoc": true}`)
	testData, err := testRaw.MarshalJSON()
	assert.Nil(err)
	matched, status, err := filter.FilterByteSlice(testData)
	assert.False(matched)
	assert.Equal(0, status)
	assert.Nil(err)

	tempMap := make(map[string]interface{})
	err = json.Unmarshal(testData, &tempMap)

	testXattr := make(map[string]interface{})
	testXattr["VersionSupported"] = 2.0

	tempMap[base.ReservedWordsMap[base.ExternalKeyXattr]] = testXattr
	testData2, err := json.Marshal(tempMap)
	assert.Nil(err)

	matched, status, err = filter.FilterByteSlice(testData2)
	assert.True(matched)
	assert.Equal(0, status)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestXattrAdd =================")
}

func TestKeyAddXattrPretest(t *testing.T) {
	fmt.Println("============== Test case start: TestKeyAddXattrPretest =================")
	assert := assert.New(t)

	filter, err := NewFilter(filterId, "Testdoc = true AND REGEXP_CONTAINS(META().id, \"^abc\") AND META().xattrs.VersionSupported > 1.0", realUtil, 0, base.MobileCompatibilityOff)

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

	matched, status, err := filter.FilterByteSlice(testData)
	assert.True(matched)
	assert.Equal(0, status)
	assert.Nil(err)

	fmt.Println("============== Test case end: TestKeyAddXattrPretest =================")
}

func TestKeyAddXattr(t *testing.T) {
	fmt.Println("============== Test case start: TestKeyAddXattr =================")
	assert := assert.New(t)

	filter, err := NewFilter(filterId, "Testdoc = true AND REGEXP_CONTAINS(META().id, \"^abc\") AND META().xattrs.VersionSupported > 1.0", realUtil, 0, base.MobileCompatibilityOff)

	assert.Nil(err)
	assert.NotNil(filter)

	testDoc := map[string]interface{}{
		"Testdoc":  true,
		"Testdoc2": true,
	}

	testData, err := json.Marshal(testDoc)
	assert.Nil(err)
	matched, status, err := filter.FilterByteSlice(testData)
	assert.False(matched)
	assert.Equal(0, status)
	assert.Nil(err)

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

	matched, status, err = filter.FilterByteSlice(dataSlice)
	assert.True(matched)
	assert.Equal(0, status)
	assert.Nil(err)
	fmt.Println("============== Test case end: TestKeyAddXattr =================")
}

func TestCompressionXattrKeyFiltering(t *testing.T) {
	fmt.Println("============== Test case start: TestCompressionXattrKeyFiltering =================")
	assert := assert.New(t)

	xAttrCompressedFile := "../../utils/testInternalData/uprXattrCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(xAttrCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	var testExpression string = fmt.Sprintf("META().xattrs.AnotherXattr = \"TestValueString\" AND META().xattrs.TestXattr = 30 AND META().id = \"%v\" AND REGEXP_CONTAINS(Key, \"^AA\")", "TestDocKey")
	filter, err := NewFilter(filterId, testExpression, realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)

	result, err, _, _, status := filter.FilterUprEvent(uprEvent)
	assert.True(result)
	assert.Nil(err)
	assert.Equal(status, NotFiltered)

	fmt.Println("============== Test case end: TestCompressionXattrKeyFiltering =================")
}

func TestCompressionKeyFiltering(t *testing.T) {
	fmt.Println("============== Test case start: TestCompressionXattrKeyFiltering =================")
	assert := assert.New(t)

	compressedFile := "../../utils/testInternalData/uprCompression.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(compressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	filter, err := NewFilter(filterId, "META().id = \"TestDocKey\" AND REGEXP_CONTAINS(`Key`, \"^A+$\")", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)

	result, err, _, _, status := filter.FilterUprEvent(uprEvent)
	assert.True(result)
	assert.Nil(err)
	assert.Equal(status, NotFiltered)

	fmt.Println("============== Test case end: TestCompressionXattrKeyFiltering =================")
}

func TestReservedWords(t *testing.T) {
	fmt.Println("============== Test case start: TestCompressionXattrKeyFiltering =================")
	assert := assert.New(t)

	filter, err := NewFilter(filterId, "META().id = \"something\"", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.False(strings.Contains(filter.filterExpressionInternal, "META"))

	filter, err = NewFilter(filterId, "`KEY` = \"something\"", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.True(strings.Contains(filter.filterExpressionInternal, "KEY"))
	fmt.Println("============== Test case start: TestCompressionXattrKeyFiltering =================")
}

// Tests that the ATR record naming scheme is filtered out
func TestTransactionMB36043(t *testing.T) {
	assert := assert.New(t)

	var filterType base.FilterExpDelType
	filterType.SetSkipReplicateUncommittedTxn(true)
	legacyFilter, err := NewFilter(filterId, "REGEXP_CONTAINS(META().id, \".*\")", realUtil, filterType, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(legacyFilter)

	txnFile := "./testdata/transactionDoc.json"
	txnUprEvent, err := base.RetrieveUprJsonAndConvert(txnFile)
	assert.Nil(err)
	assert.NotNil(txnUprEvent)

	needToReplicate, _, _, _, _, _, _, status := legacyFilter.filterTransactionRelatedUprEvent(txnUprEvent.UprEvent, nil)
	assert.False(needToReplicate)
	assert.Equal(status, FilteredOnATRDocument)

	// Even with a new, non-legacy filter, it should still prevent ATR related doc keys from being replicated
	txFilter, err := NewFilter(filterId, "REGEXP_CONTAINS(META().id, \".*\")", realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	assert.NotNil(txFilter)

	needToReplicate, _, _, _, _, _, _, status = legacyFilter.filterTransactionRelatedUprEvent(txnUprEvent.UprEvent, nil)
	assert.Equal(status, FilteredOnATRDocument)
	assert.False(needToReplicate)
}

func TestTransactionXattrAndKeyOnlyFiltering(t *testing.T) {
	fmt.Println("============== Test case start: TestTransactionXattrAndKeyOnlyFiltering =================")
	assert := assert.New(t)

	uprFile := "./testInternalData/uprTransXattrNotCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(uprFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	var testExpression string = fmt.Sprintf("META().id = \"%v\"", "test1")
	filter, err := NewFilter(filterId, testExpression, realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)

	result, err, _, _, status := filter.FilterUprEvent(uprEvent)
	assert.True(result)
	assert.Nil(err)
	assert.Equal(status, NotFiltered)

	fmt.Println("============== Test case end: TestTransactionXattrAndKeyOnlyFiltering =================")
}

func TestBinaryFilter(t *testing.T) {
	fmt.Println("============== Test case start: TestBinaryFilter =================")
	defer fmt.Println("============== Test case end: TestBinaryFilter =================")
	assert := assert.New(t)
	binaryFile := "./testInternalData/uprBinary.json"

	uprEvent, err := base.RetrieveUprJsonAndConvert(binaryFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)
	assert.False(uprEvent.UprEvent.DataType&mcc.JSONDataType > 0)

	// Default mode - binary goes through if not key-specific
	var testExpression string = fmt.Sprintf("key=val")
	filter, err := NewFilter(filterId, testExpression, realUtil, 0, base.MobileCompatibilityOff)
	assert.Nil(err)
	result, err, _, _, status := filter.FilterUprEvent(uprEvent)
	assert.True(result)
	assert.Nil(err)
	assert.Equal(status, NotFiltered)

	// Should not pass through in either key only filter nor any other filter
	var filterType base.FilterExpDelType
	filterType.SetSkipBinary(true)
	filter, err = NewFilter(filterId, testExpression, realUtil, filterType, base.MobileCompatibilityOff)
	assert.Nil(err)
	result, err, _, _, status = filter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)
	assert.Equal(status, FilteredOnOthers)

	testExpression = fmt.Sprintf("META().id = \"%v\"", "test1")
	filter, err = NewFilter(filterId, testExpression, realUtil, filterType, base.MobileCompatibilityOff)
	fmt.Printf("expression: %v\n", testExpression)
	fmt.Printf("filter: %v\n", filter)
	assert.Nil(err)

	result, err, _, _, _ = filter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)
	assert.Equal(status, FilteredOnOthers)
}

func TestMobileFiltering(t *testing.T) {
	fmt.Println("============== Test case start: TestMobileFiltering =================")
	assert := assert.New(t)

	filter, err := NewFilter(filterId, "", realUtil, 0, base.MobileCompatibilityActive)
	assert.Nil(err)

	// Document not prefixed with _sync: should be replicated
	uprFile := "./testInternalData/uprTransXattrNotCompress.json"
	uprEvent, err := base.RetrieveUprJsonAndConvert(uprFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)
	result, err, _, _, status := filter.FilterUprEvent(uprEvent)
	assert.True(result)
	assert.Nil(err)
	assert.Equal(status, NotFiltered)

	// Document key with prefix _sync:att should be replicated
	uprFile = "./testInternalData/syncAttTest.json"
	uprEvent, err = base.RetrieveUprJsonAndConvert(uprFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)
	result, err, _, _, status = filter.FilterUprEvent(uprEvent)
	assert.True(result)
	assert.Nil(err)
	assert.Equal(status, NotFiltered)

	// Document key with prefix _sync: but not _sync:att should be skipped
	uprFile = "./testInternalData/syncTest.json"
	uprEvent, err = base.RetrieveUprJsonAndConvert(uprFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)
	result, err, _, _, status = filter.FilterUprEvent(uprEvent)
	assert.False(result)
	assert.Nil(err)
	assert.Equal(status, FilteredOnMobileRecord)
}
