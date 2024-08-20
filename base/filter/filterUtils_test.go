//go:build pcre
// +build pcre

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
	"bytes"
	"encoding/json"
	"fmt"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbaselabs/gojsonsm"
	"github.com/stretchr/testify/assert"
	// "gopkg.in/couchbase/gocb.v1"
	"io/ioutil"
	"testing"
)

// Don't change these - these are already dumped in the testData files
var docKey string = "TestDocKey"
var docValue string = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
var docMap map[string]interface{} = map[string]interface{}{
	"Key": docValue,
}

var docArrayKey string = "TestDocArrayKey"
var docArray []interface{} = []interface{}{
	"a",
	"b",
	"c",
}

var testExpression string = fmt.Sprintf("META().xattrs.AnotherXattr = \"TestValueString\" AND META().xattrs.TestXattr = 30 AND META().id = \"%v\" AND REGEXP_CONTAINS(Key, \"^AA\")", docKey)

func MakeSlicesBuf() [][]byte {
	return make([][]byte, 0, 2)
}

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

const testFilteringDataDir = "testFilteringData/"

func getDocValueMock() (map[string]interface{}, []byte, error) {
	fileName := fmt.Sprintf("%v%v", testFilteringDataDir, "docValueSlice.bin")
	return readJsonHelper(fileName)
}

func getXattrValueMock() (map[string]interface{}, []byte, error) {
	fileName := fmt.Sprintf("%v%v", testFilteringDataDir, "xattrSlice.bin")
	return readJsonHelper(fileName)
}

var testUtils = &FilterUtilsImpl{}

func TestStripXattrAndCompression(t *testing.T) {
	unCompressedFile := "./testInternalData/uprNotCompress.json"
	compressedFile := "./testInternalData/uprCompression.json"
	xAttrUncompressedFile := "./testInternalData/uprXattrNotCompress.json"
	xAttrCompressedFile := "./testInternalData/uprXattrCompress.json"

	fmt.Println("============== Test case start: TestStripXattrAndCompression =================")
	assert := assert.New(t)
	dp := base.NewDataPool()
	fp := base.NewFakeDataPool()

	checkMap, err := json.Marshal(docMap)
	assert.Nil(err)

	uprEvent, err := base.RetrieveUprJsonAndConvert(unCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)
	assert.Equal(docKey, string(uprEvent.UprEvent.Key))
	assert.Equal(checkMap, uprEvent.UprEvent.Value)
	slices := MakeSlicesBuf()
	_, err, _, _ = testUtils.ProcessUprEventForFiltering(uprEvent.UprEvent, nil, 0, dp, base.FilterFlagType(0) /*skipXattr*/, &slices)
	assert.Nil(err)

	// Use a fake datapool and test if the utilities can trim it correctly
	trimSliceCheck, _, _, _ := testUtils.ProcessUprEventForFiltering(uprEvent.UprEvent, nil, 0, fp, base.FilterFlagType(0) /*skipXattr*/, &slices)
	assert.Equal("}", string(trimSliceCheck[len(trimSliceCheck)-1]))

	uprEventCompressed, err := base.RetrieveUprJsonAndConvert(compressedFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)
	assert.Equal(docKey, string(uprEventCompressed.UprEvent.Key))
	assert.NotEqual(checkMap, uprEventCompressed.UprEvent.Value)
	_, err, _, _ = testUtils.ProcessUprEventForFiltering(uprEventCompressed.UprEvent, nil, 0, dp, base.FilterFlagType(0) /*skipXattr*/, &slices)
	trimSliceCheck, _, _, _ = testUtils.ProcessUprEventForFiltering(uprEventCompressed.UprEvent, nil, 0, fp, base.FilterFlagType(0) /*skipXattr*/, &slices)
	assert.Equal("}", string(trimSliceCheck[len(trimSliceCheck)-1]))

	assert.Nil(err)

	uprEventXattr, err := base.RetrieveUprJsonAndConvert(xAttrUncompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEventXattr)
	assert.Equal(docKey, string(uprEventXattr.UprEvent.Key))
	_, err, _, _ = testUtils.ProcessUprEventForFiltering(uprEventXattr.UprEvent, nil, 0, dp, base.FilterFlagType(0) /*skipXattr*/, &slices)
	assert.Nil(err)

	trimSliceCheck, _, _, _ = testUtils.ProcessUprEventForFiltering(uprEventXattr.UprEvent, nil, 0, fp, base.FilterFlagType(0) /*skipXattr*/, &slices)
	assert.Equal("}", string(trimSliceCheck[len(trimSliceCheck)-1]))

	uprEventXattrCompressed, err := base.RetrieveUprJsonAndConvert(xAttrCompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEventXattrCompressed)
	assert.Equal(docKey, string(uprEventXattrCompressed.UprEvent.Key))
	assert.NotEqual(checkMap, uprEventXattrCompressed.UprEvent.Value)
	_, err, _, _ = testUtils.ProcessUprEventForFiltering(uprEventXattrCompressed.UprEvent, nil, 0, dp, base.FilterFlagType(0) /*skipXattr*/, &slices)
	assert.Nil(err)

	trimSliceCheck, _, _, _ = testUtils.ProcessUprEventForFiltering(uprEventXattrCompressed.UprEvent, nil, 0, dp, base.FilterFlagType(0) /*skipXattr*/, &slices)
	assert.Equal("}", string(trimSliceCheck[len(trimSliceCheck)-1]))

	uprEventStripXattr, err := base.RetrieveUprJsonAndConvert(xAttrUncompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEventStripXattr)
	// Pretend it's not a JSON doc
	uprEventStripXattr.UprEvent.DataType = mcc.XattrDataType
	serializedXattr, err, _, _ := testUtils.ProcessUprEventForFiltering(uprEventStripXattr.UprEvent, nil, 0, dp, base.FilterFlagSkipKey, &slices)
	assert.Nil(err)
	serializedXattrStr := `{"[$%XDCRInternalMeta*%$]":{"TestXattr":30,"AnotherXattr":"TestValueString"}}`
	assert.Equal(serializedXattrStr, string(bytes.Trim(serializedXattr, "\x00")))
	trimSliceCheck, _, _, _ = testUtils.ProcessUprEventForFiltering(uprEventStripXattr.UprEvent, nil, 0, fp, base.FilterFlagSkipKey, &slices)
	assert.Equal(serializedXattrStr, string(trimSliceCheck))

	fmt.Println("============== Test case end: TestStripXattrAndCompression =================")
}

func TestProcessUprEventWithGarbage(t *testing.T) {
	fmt.Println("============== Test case start: TestProcessUprEventWithGarbage =================")
	assert := assert.New(t)
	xAttrUncompressedFile := "./testInternalData/uprXattrNotCompress.json"
	slices := MakeSlicesBuf()
	fp := base.NewFakeDataPool()
	uprEventStripXattr, err := base.RetrieveUprJsonAndConvert(xAttrUncompressedFile)
	assert.Nil(err)
	assert.NotNil(uprEventStripXattr)
	// Pretend it's not a JSON doc
	uprEventStripXattr.UprEvent.DataType = mcc.XattrDataType
	serializedXattr, err, _, _ := testUtils.ProcessUprEventForFiltering(uprEventStripXattr.UprEvent, nil, 0, fp, base.FilterFlagSkipKey, &slices)
	assert.Nil(err)
	serializedXattrStr := `{"[$%XDCRInternalMeta*%$]":{"TestXattr":30,"AnotherXattr":"TestValueString"}}`
	assert.Equal(serializedXattrStr, string(bytes.Trim(serializedXattr, "\x00")))

	fmt.Println("============== Test case end: TestProcessUprEventWithGarbage =================")
}

func TestProcessBinaryFile(t *testing.T) {
	fmt.Println("============== Test case start: TestProcessBinaryFile =================")
	assert := assert.New(t)
	binaryFile := "./testInternalData/uprBinary.json"
	dp := base.NewDataPool()

	uprEvent, err := base.RetrieveUprJsonAndConvert(binaryFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)
	assert.False(uprEvent.UprEvent.DataType&mcc.JSONDataType > 0)

	checkMap := map[string]interface{}{
		base.ReservedWordsMap[base.ExternalKeyKey]: string(uprEvent.UprEvent.Key),
	}
	checkSlice, err := json.Marshal(checkMap)
	assert.Nil(err)

	slices := MakeSlicesBuf()
	retSlice, err, _, _ := testUtils.ProcessUprEventForFiltering(uprEvent.UprEvent, nil, 0, dp, base.FilterFlagType(0) /*skipXattr*/, &slices)
	assert.Nil(err)

	assert.Equal(checkSlice, bytes.Trim(retSlice, "\x00"))

	fmt.Println("============== Test case end: TestProcessBinaryFile =================")
}

func TestDataPool(t *testing.T) {
	fmt.Println("============== Test case start: TestDataPool =================")
	dp := base.NewDataPool()

	assert := assert.New(t)
	_, err := dp.GetByteSlice(uint64(22 << 20))
	assert.NotNil(err)

	slice, err := dp.GetByteSlice(uint64(51))
	assert.Equal(100, cap(slice))

	fmt.Println("============== Test case start: TestDataPool =================")
}

// If ns_server cluster run is running, generate a Xattr doc
// And then also retrieve it back via gocb
// At DCP nozzle, serialize the uprEvent as a json and write it to a file
// func TestGenerateXattrUsingGoCB(t *testing.T) {
// 	cluster, err := gocb.Connect("http://localhost:9000")
// 	if err != nil {
// 		return
// 	}
// 	fmt.Println("============== Test case start: TestGenerateXattrUsingGoCB =================")

// 	cluster.Authenticate(gocb.PasswordAuthenticator{
// 		Username: "Administrator",
// 		Password: "wewewe",
// 	})

// 	bucket, err := cluster.OpenBucket("B1", "")
// 	if err != nil {
// 		return
// 	}

// 	bucket.Remove(docKey, 0)

// 	_, err = bucket.Insert(docKey, docMap, 0)
// 	if err != nil {
// 		fmt.Printf("err %v\n", err)
// 		return
// 	}

// 	_, err = bucket.MutateIn(docKey, 0, 0).InsertEx("TestXattr", 30, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
// 	if err != nil {
// 		fmt.Printf("Error with Insert: %v\n", err)
// 		return
// 	}
// 	_, err = bucket.MutateIn(docKey, 0, 0).InsertEx("AnotherXattr", "TestValueString", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
// 	if err != nil {
// 		fmt.Printf("Error with Insert2: %v\n", err)
// 		return
// 	}

// 	_, err = bucket.MutateIn(docKey, 0, 0).InsertEx("NilXattr", nil, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
// 	if err != nil {
// 		fmt.Printf("Error with Insert3: %v\n", err)
// 		return
// 	}

// 	// The following is extracted from datafile from TestTransXattrOnlyFilteringWithoutCompression
// 	dummyStagedData := make(map[string]interface{})
// 	dummyStagedData["name"] = "neil"
// 	txDummyVal := make(map[string]interface{})
// 	txDummyVal["ver"] = "d0cb40cf-41e0-4f9d-94ef-0ef29fa76d37"
// 	txDummyVal["atr_id"] = "atr-690-#18"
// 	txDummyVal["staged"] = dummyStagedData
// 	_, err = bucket.MutateIn(docKey, 0, 0).InsertEx(base.TransactionXattrKey, txDummyVal, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
// 	if err != nil {
// 		fmt.Printf("Error with Insert4: %v\n", err)
// 		return
// 	}

// 	mapTypeXattr := make(map[string]interface{})
// 	mapTypeXattrArr := []string{"a", "b", "c"}
// 	mapTypeXattr["stringType"] = "string"
// 	mapTypeXattr["floatType"] = 1.0
// 	mapTypeXattr["array"] = mapTypeXattrArr

// 	_, err = bucket.MutateIn(docKey, 0, 0).InsertEx("MapXattr", mapTypeXattr, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
// 	if err != nil {
// 		fmt.Printf("Error with Insert3: %v\n", err)
// 		return
// 	}

// 	var docRetrieveVal interface{}
// 	_, err = bucket.Get(docKey, &docRetrieveVal)

// 	retrievedMap, ok := docRetrieveVal.(map[string]interface{})
// 	if !ok {
// 		fmt.Printf("Error with retrieving\n")
// 	}

// 	if val, ok := retrievedMap["Key"]; ok && val == docMap["Key"] {
// 		fmt.Printf("Set and get working\n")
// 	}

// 	bodySlice, err := json.Marshal(retrievedMap)
// 	if err != nil {
// 		fmt.Printf("err marshal %v\n", err)
// 	}

// 	fileName := "/tmp/docValueSlice.bin"
// 	//	dumpBytes := new(bytes.Buffer)
// 	//	json.NewEncoder(dumpBytes).Encode(bodySlice)
// 	//	writeErr := ioutil.WriteFile(fileName, dumpBytes.Bytes(), 0644)
// 	writeErr := ioutil.WriteFile(fileName, bodySlice, 0644)
// 	if writeErr == nil {
// 		fmt.Printf("Wrote dump file successfully to %v\n", fileName)
// 	} else {
// 		fmt.Printf("Unable to write file due to %v\n", writeErr)
// 	}

// 	frag, err := bucket.LookupIn(docKey).GetEx(base.XattributeToc, gocb.SubdocFlagXattr).Execute()
// 	if err != nil {
// 		fmt.Printf("err1: %v\n", err)
// 	}

// 	var docMap interface{}
// 	err = frag.Content(base.XattributeToc, &docMap)
// 	if err != nil {
// 		fmt.Printf("err frag.Content: %v\n", err)
// 	}

// 	xattrMap := make(map[string]interface{})
// 	tocList := docMap.([]interface{})
// 	for _, aToc := range tocList {
// 		if entry, ok := aToc.(string); ok {
// 			frag, err := bucket.LookupIn(docKey).GetEx(entry, gocb.SubdocFlagXattr).Execute()
// 			if err != nil {
// 				fmt.Printf("err lookupIn: %v\n", err)
// 			}

// 			// TODO - check CAS

// 			var value interface{}
// 			frag.Content(entry, &value)
// 			xattrMap[entry] = value
// 		}
// 	}

// 	xattrSlice, err := json.Marshal(xattrMap)
// 	if err != nil {
// 		fmt.Printf("err marshal2 %v\n", err)
// 	}

// 	fileName = "/tmp/xattrSlice.bin"
// 	//	dumpBytes = new(bytes.Buffer)
// 	//	json.NewEncoder(dumpBytes).Encode(xattrSlice)
// 	//	writeErr = ioutil.WriteFile(fileName, dumpBytes.Bytes(), 0644)
// 	writeErr = ioutil.WriteFile(fileName, xattrSlice, 0644)
// 	if writeErr == nil {
// 		fmt.Printf("Wrote dump file successfully to %v\n", fileName)
// 	} else {
// 		fmt.Printf("Unable to write file due to %v\n", writeErr)
// 	}

// 	//	frag, err = bucket.LookupIn(docKey).GetEx(base.XattributeDoc, gocb.SubdocFlagXattr).Execute()
// 	//	if err != nil {
// 	//		fmt.Printf("err getting $XTOC: %v\n", err)
// 	//	}

// 	//	_, err = base.AddXattrToBeFilteredWithoutDP(bodySlice, xattrSlice)
// 	//	if err != nil {
// 	//		fmt.Printf("err adding Xattr %v\n", err)
// 	//	}
// 	//
// 	//	_, err, _ = base.AddKeyToBeFiltered(bodySlice, []byte(docKey), nil, nil)
// 	//	if err != nil {
// 	//		fmt.Printf("err adding key %v\n", err)
// 	//	}

// 	//Put the following in DCP nozzle to dump uprEvent to a file
// 	// Import: bytes, encoding/json, io/ioutil

// 	//	fileName := "/tmp/uprEventDump.bin"
// 	//	dumpBytes := new(bytes.Buffer)
// 	//	json.NewEncoder(dumpBytes).Encode(*m)
// 	//	writeErr := ioutil.WriteFile(fileName, dumpBytes.Bytes(), 0644)
// 	//	if writeErr == nil {
// 	//		dcp.LoggerImpl().Infof("Wrote dump file successfully to %v\n", fileName)
// 	//	} else {
// 	//		dcp.LoggerImpl().Warnf("Unable to write file due to %v\n", writeErr)
// 	//	}

// 	fmt.Println("============== Test case end: TestGenerateXattrUsingGoCB =================")
// }

// func TestGenerateXattrPureArrayUsingGoCB(t *testing.T) {
// 	cluster, err := gocb.Connect("http://localhost:9000")
// 	if err != nil {
// 		return
// 	}
// 	fmt.Println("============== Test case start: TestGenerateXattrPureArrayUsingGoCB =================")

// 	cluster.Authenticate(gocb.PasswordAuthenticator{
// 		Username: "Administrator",
// 		Password: "wewewe",
// 	})

// 	bucket, err := cluster.OpenBucket("B1", "")
// 	if err != nil {
// 		return
// 	}

// 	bucket.Remove(docArrayKey, 0)

// 	_, err = bucket.Insert(docArrayKey, docArray, 0)
// 	if err != nil {
// 		fmt.Printf("err %v\n", err)
// 		return
// 	}

// 	_, err = bucket.MutateIn(docArrayKey, 0, 0).InsertEx("TestXattr", 30, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
// 	if err != nil {
// 		fmt.Printf("Error with Insert: %v\n", err)
// 		return
// 	}
// 	_, err = bucket.MutateIn(docArrayKey, 0, 0).InsertEx("AnotherXattr", "TestValueString", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
// 	if err != nil {
// 		fmt.Printf("Error with Insert2: %v\n", err)
// 		return
// 	}

// 	_, err = bucket.MutateIn(docArrayKey, 0, 0).InsertEx("NilXattr", nil, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
// 	if err != nil {
// 		fmt.Printf("Error with Insert3: %v\n", err)
// 		return
// 	}

// 	// The following is extracted from datafile from TestTransXattrOnlyFilteringWithoutCompression
// 	dummyStagedData := make(map[string]interface{})
// 	dummyStagedData["name"] = "neil"
// 	txDummyVal := make(map[string]interface{})
// 	txDummyVal["ver"] = "d0cb40cf-41e0-4f9d-94ef-0ef29fa76d37"
// 	txDummyVal["atr_id"] = "atr-690-#18"
// 	txDummyVal["staged"] = dummyStagedData
// 	_, err = bucket.MutateIn(docArrayKey, 0, 0).InsertEx(base.TransactionXattrKey, txDummyVal, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
// 	if err != nil {
// 		fmt.Printf("Error with Insert4: %v\n", err)
// 		return
// 	}

// 	mapTypeXattr := make(map[string]interface{})
// 	mapTypeXattrArr := []string{"a", "b", "c"}
// 	mapTypeXattr["stringType"] = "string"
// 	mapTypeXattr["floatType"] = 1.0
// 	mapTypeXattr["array"] = mapTypeXattrArr

// 	_, err = bucket.MutateIn(docArrayKey, 0, 0).InsertEx("MapXattr", mapTypeXattr, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
// 	if err != nil {
// 		fmt.Printf("Error with Insert3: %v\n", err)
// 		return
// 	}

// 	var docRetrieveVal interface{}
// 	_, err = bucket.Get(docArrayKey, &docRetrieveVal)

// 	retrievedSlice, ok := docRetrieveVal.([]interface{})
// 	if !ok {
// 		fmt.Printf("Error with retrieving\n")
// 	}

// 	bodySlice, err := json.Marshal(retrievedSlice)
// 	if err != nil {
// 		fmt.Printf("err marshal %v\n", err)
// 	}

// 	fileName := "/tmp/docValueArraySlice.bin"
// 	//	dumpBytes := new(bytes.Buffer)
// 	//	json.NewEncoder(dumpBytes).Encode(bodySlice)
// 	//	writeErr := ioutil.WriteFile(fileName, dumpBytes.Bytes(), 0644)
// 	writeErr := ioutil.WriteFile(fileName, bodySlice, 0644)
// 	if writeErr == nil {
// 		fmt.Printf("Wrote dump file successfully to %v\n", fileName)
// 	} else {
// 		fmt.Printf("Unable to write file due to %v\n", writeErr)
// 	}
// 	fmt.Println("============== Test case end: TestGenerateXattrPureArrayUsingGoCB =================")
// }

// If ns_server cluster run is running, generate a Xattr doc
// And then also retrieve it back via gocb
// At DCP nozzle, serialize the uprEvent as a json and write it to a file
// func TestGenerateOnlyTxnXattrUsingGoCB(t *testing.T) {
// 	cluster, err := gocb.Connect("http://localhost:9000")
// 	if err != nil {
// 		return
// 	}
// 	fmt.Println("============== Test case start: TestGenerateOnlyTxnXattrUsingGoCB =================")
// 	defer fmt.Println("============== Test case end: TestGenerateOnlyTxnXattrUsingGoCB =================")

// 	cluster.Authenticate(gocb.PasswordAuthenticator{
// 		Username: "Administrator",
// 		Password: "wewewe",
// 	})

// 	bucket, err := cluster.OpenBucket("B1", "")
// 	if err != nil {
// 		return
// 	}

// 	bucket.Remove(docKey, 0)

// 	_, err = bucket.Insert(docKey, docMap, 0)
// 	if err != nil {
// 		fmt.Printf("err %v\n", err)
// 		return
// 	}

// 	// The following is extracted from datafile from TestTransXattrOnlyFilteringWithoutCompression
// 	dummyStagedData := make(map[string]interface{})
// 	dummyStagedData["name"] = "neil"
// 	txDummyVal := make(map[string]interface{})
// 	txDummyVal["ver"] = "d0cb40cf-41e0-4f9d-94ef-0ef29fa76d37"
// 	txDummyVal["atr_id"] = "atr-690-#18"
// 	txDummyVal["staged"] = dummyStagedData
// 	_, err = bucket.MutateIn(docKey, 0, 0).InsertEx(base.TransactionXattrKey, txDummyVal, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
// 	if err != nil {
// 		fmt.Printf("Error with Insert4: %v\n", err)
// 		return
// 	}

// 	var docRetrieveVal interface{}
// 	_, err = bucket.Get(docKey, &docRetrieveVal)

// 	retrievedMap, ok := docRetrieveVal.(map[string]interface{})
// 	if !ok {
// 		fmt.Printf("Error with retrieving\n")
// 	}

// 	if val, ok := retrievedMap["Key"]; ok && val == docMap["Key"] {
// 		fmt.Printf("Set and get working\n")
// 	}

// 	bodySlice, err := json.Marshal(retrievedMap)
// 	if err != nil {
// 		fmt.Printf("err marshal %v\n", err)
// 	}

// 	fileName := "/tmp/docValueSlice.bin"
// 	//	dumpBytes := new(bytes.Buffer)
// 	//	json.NewEncoder(dumpBytes).Encode(bodySlice)
// 	//	writeErr := ioutil.WriteFile(fileName, dumpBytes.Bytes(), 0644)
// 	writeErr := ioutil.WriteFile(fileName, bodySlice, 0644)
// 	if writeErr == nil {
// 		fmt.Printf("Wrote dump file successfully to %v\n", fileName)
// 	} else {
// 		fmt.Printf("Unable to write file due to %v\n", writeErr)
// 	}

// 	frag, err := bucket.LookupIn(docKey).GetEx(base.XattributeToc, gocb.SubdocFlagXattr).Execute()
// 	if err != nil {
// 		fmt.Printf("err1: %v\n", err)
// 	}

// 	var docMap interface{}
// 	err = frag.Content(base.XattributeToc, &docMap)
// 	if err != nil {
// 		fmt.Printf("err frag.Content: %v\n", err)
// 	}

// 	xattrMap := make(map[string]interface{})
// 	tocList := docMap.([]interface{})
// 	for _, aToc := range tocList {
// 		if entry, ok := aToc.(string); ok {
// 			frag, err := bucket.LookupIn(docKey).GetEx(entry, gocb.SubdocFlagXattr).Execute()
// 			if err != nil {
// 				fmt.Printf("err lookupIn: %v\n", err)
// 			}

// 			// TODO - check CAS

// 			var value interface{}
// 			frag.Content(entry, &value)
// 			xattrMap[entry] = value
// 		}
// 	}

// 	xattrSlice, err := json.Marshal(xattrMap)
// 	if err != nil {
// 		fmt.Printf("err marshal2 %v\n", err)
// 	}

// 	fileName = "/tmp/xattrSlice.bin"
// 	//	dumpBytes = new(bytes.Buffer)
// 	//	json.NewEncoder(dumpBytes).Encode(xattrSlice)
// 	//	writeErr = ioutil.WriteFile(fileName, dumpBytes.Bytes(), 0644)
// 	writeErr = ioutil.WriteFile(fileName, xattrSlice, 0644)
// 	if writeErr == nil {
// 		fmt.Printf("Wrote dump file successfully to %v\n", fileName)
// 	} else {
// 		fmt.Printf("Unable to write file due to %v\n", writeErr)
// 	}

// 	//Put the following in DCP nozzle to dump uprEvent to a file
// 	// Import: bytes, encoding/json, io/ioutil

// 	//	fileName := "/tmp/uprEventDump.bin"
// 	//	dumpBytes := new(bytes.Buffer)
// 	//	json.NewEncoder(dumpBytes).Encode(*m)
// 	//	writeErr := ioutil.WriteFile(fileName, dumpBytes.Bytes(), 0644)
// 	//	if writeErr == nil {
// 	//		dcp.LoggerImpl().Infof("Wrote dump file successfully to %v\n", fileName)
// 	//	} else {
// 	//		dcp.LoggerImpl().Warnf("Unable to write file due to %v\n", writeErr)
// 	//	}
// }

func TestMatchDocKeyValueAndXattr(t *testing.T) {
	fmt.Println("============== Test case start: TestMatchDocKeyValueAndXattr =================")
	assert := assert.New(t)

	retMap, bodySlice, err := getDocValueMock()
	assert.Nil(err)

	assert.NotEqual(0, len(retMap))
	assert.NotEqual(0, len(bodySlice))

	retMap, xattrSlice, err := getXattrValueMock()
	assert.Nil(err)
	assert.NotEqual(0, len(retMap))
	assert.NotEqual(0, len(xattrSlice))

	assert.True(base.FilterContainsXattrExpression(testExpression))

	bodySlice, err = base.AddXattrToBeFilteredWithoutDP(bodySlice, xattrSlice)
	assert.Nil(err)

	docKeyBytes := []byte(docKey)

	bodySlice, err, _, lastBracketPos := testUtils.AddKeyToBeFiltered(bodySlice, docKeyBytes, nil, nil, len(docKeyBytes)-1)
	assert.Nil(err)
	assert.Equal("}", string(bodySlice[lastBracketPos]))

	testMap := make(map[string]interface{})
	err = json.Unmarshal(bodySlice, &testMap)
	assert.Nil(err)

	matcher, err := gojsonsm.GetFilterExpressionMatcher(base.ReplaceKeyWordsForExpression(testExpression))
	assert.Nil(err)

	match, err := matcher.Match(bodySlice)
	assert.Nil(err)
	assert.True(match)

	fmt.Println("============== Test case end: TestMatchDocKeyValueAndXattr =================")
}

func TestMatchPcreNegLookahead(t *testing.T) {
	fmt.Println("============== Test case start: TestMatchPcreNegLookahead =================")
	assert := assert.New(t)

	testMap := make(map[string]interface{})
	testMap["state"] = "California"

	bodySlice, err := json.Marshal(testMap)
	assert.Nil(err)

	matcher, err := gojsonsm.GetFilterExpressionMatcher(base.ReplaceKeyWordsForExpression("REGEXP_CONTAINS(state, \"C(?!Z)alifornia\")"))
	assert.Nil(err)

	match, err := matcher.Match(bodySlice)
	assert.Nil(err)
	assert.True(match)

	fmt.Println("============== Test case end: TestMatchPcreNegLookahead =================")
}

// func TestGenerateBinaryDoc(t *testing.T) {
// 	cluster, err := gocb.Connect("http://localhost:9000")
// 	if err != nil {
// 		return
// 	}
// 	fmt.Println("============== Test case start: TestGenerateBinaryDoc =================")
// 	defer fmt.Println("============== Test case end: TestGenerateBinaryDoc =================")

// 	cluster.Authenticate(gocb.PasswordAuthenticator{
// 		Username: "Administrator",
// 		Password: "wewewe",
// 	})

// 	bucket, err := cluster.OpenBucket("B1", "")
// 	if err != nil {
// 		return
// 	}

// 	bucket.Remove(docKey, 0)

// 	slice := []byte("abcde")

// 	_, err = bucket.Insert(docKey, slice, 0)
// 	if err != nil {
// 		fmt.Printf("err %v\n", err)
// 		return
// 	}

// 	//	fileName := "/tmp/uprEventDump.bin"
// 	//	dumpBytes := new(bytes.Buffer)
// 	//	json.NewEncoder(dumpBytes).Encode(*m)
// 	//	writeErr := ioutil.WriteFile(fileName, dumpBytes.Bytes(), 0644)
// 	//	if writeErr == nil {
// 	//		dcp.LoggerImpl().Infof("Wrote dump file successfully to %v\n", fileName)
// 	//	} else {
// 	//		dcp.LoggerImpl().Warnf("Unable to write file due to %v\n", writeErr)
// 	//	}
// }
