// +build pcre

package utils

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/gocb"
	"github.com/couchbase/gojsonsm"
	base "github.com/couchbase/goxdcr/base"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

// Don't change these - these are already dumped in the testData files
var docKey string = "TestDocKey"
var docValue string = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
var docMap map[string]interface{} = map[string]interface{}{
	"Key": docValue,
}

const testExternalDataDir = "testExternalData/"
const testInternalDataDir = "testInternalData/"
const testK8DataDir = "testK8ExternalData/"
const testFilteringDataDir = "testFilteringData/"

var testUtils = NewUtilities()
var testExpression string = fmt.Sprintf("META().xattrs.AnotherXattr = \"TestValueString\" AND META().xattrs.TestXattr = 30 AND META().id = \"%v\" AND REGEXP_CONTAINS(Key, \"^AA\")", docKey)

func getDocValueMock() (map[string]interface{}, []byte, error) {
	fileName := fmt.Sprintf("%v%v", testFilteringDataDir, "docValueSlice.bin")
	return readJsonHelper(fileName)
}

func getXattrValueMock() (map[string]interface{}, []byte, error) {
	fileName := fmt.Sprintf("%v%v", testFilteringDataDir, "xattrSlice.bin")
	return readJsonHelper(fileName)
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

// If ns_server cluster run is running, generate a Xattr doc
// And then also retrieve it back via gocb
// At DCP nozzle, serialize the uprEvent as a json and write it to a file
func TestGenerateXattrUsingGoCB(t *testing.T) {
	cluster, err := gocb.Connect("http://localhost:9000")
	if err != nil {
		return
	}
	fmt.Println("============== Test case start: TestGenerateXattrUsingGoCB =================")

	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: "Administrator",
		Password: "wewewe",
	})

	bucket, err := cluster.OpenBucket("b1", "")
	if err != nil {
		return
	}

	bucket.Remove(docKey, 0)

	_, err = bucket.Insert(docKey, docMap, 0)
	if err != nil {
		fmt.Printf("err %v\n", err)
		return
	}

	_, err = bucket.MutateIn(docKey, 0, 0).InsertEx("TestXattr", 30, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
	if err != nil {
		fmt.Printf("Error with Insert: %v\n", err)
		return
	}
	_, err = bucket.MutateIn(docKey, 0, 0).InsertEx("AnotherXattr", "TestValueString", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
	if err != nil {
		fmt.Printf("Error with Insert2: %v\n", err)
		return
	}

	var docRetrieveVal interface{}
	_, err = bucket.Get(docKey, &docRetrieveVal)

	retrievedMap, ok := docRetrieveVal.(map[string]interface{})
	if !ok {
		fmt.Printf("Error with retrieving\n")
	}

	if val, ok := retrievedMap["Key"]; ok && val == docMap["Key"] {
		fmt.Printf("Set and get working\n")
	}

	bodySlice, err := json.Marshal(retrievedMap)
	if err != nil {
		fmt.Printf("err marshal %v\n", err)
	}

	fileName := "/tmp/docValueSlice.bin"
	//	dumpBytes := new(bytes.Buffer)
	//	json.NewEncoder(dumpBytes).Encode(bodySlice)
	//	writeErr := ioutil.WriteFile(fileName, dumpBytes.Bytes(), 0644)
	writeErr := ioutil.WriteFile(fileName, bodySlice, 0644)
	if writeErr == nil {
		fmt.Printf("Wrote dump file successfully to %v\n", fileName)
	} else {
		fmt.Printf("Unable to write file due to %v\n", writeErr)
	}

	frag, err := bucket.LookupIn(docKey).GetEx(base.XattributeToc, gocb.SubdocFlagXattr).Execute()
	if err != nil {
		fmt.Printf("err1: %v\n", err)
	}

	var docMap interface{}
	err = frag.Content(base.XattributeToc, &docMap)
	if err != nil {
		fmt.Printf("err frag.Content: %v\n", err)
	}

	xattrMap := make(map[string]interface{})
	tocList := docMap.([]interface{})
	for _, aToc := range tocList {
		if entry, ok := aToc.(string); ok {
			frag, err := bucket.LookupIn(docKey).GetEx(entry, gocb.SubdocFlagXattr).Execute()
			if err != nil {
				fmt.Printf("err lookupIn: %v\n", err)
			}

			// TODO - check CAS

			var value interface{}
			frag.Content(entry, &value)
			xattrMap[entry] = value
		}
	}

	xattrSlice, err := json.Marshal(xattrMap)
	if err != nil {
		fmt.Printf("err marshal2 %v\n", err)
	}

	fileName = "/tmp/xattrSlice.bin"
	//	dumpBytes = new(bytes.Buffer)
	//	json.NewEncoder(dumpBytes).Encode(xattrSlice)
	//	writeErr = ioutil.WriteFile(fileName, dumpBytes.Bytes(), 0644)
	writeErr = ioutil.WriteFile(fileName, xattrSlice, 0644)
	if writeErr == nil {
		fmt.Printf("Wrote dump file successfully to %v\n", fileName)
	} else {
		fmt.Printf("Unable to write file due to %v\n", writeErr)
	}

	//	frag, err = bucket.LookupIn(docKey).GetEx(base.XattributeDoc, gocb.SubdocFlagXattr).Execute()
	//	if err != nil {
	//		fmt.Printf("err getting $XTOC: %v\n", err)
	//	}

	//	_, err = base.AddXattrToBeFiltered(bodySlice, xattrSlice)
	//	if err != nil {
	//		fmt.Printf("err adding Xattr %v\n", err)
	//	}
	//
	//	_, err = base.AddKeyToBeFiltered(bodySlice, []byte(docKey))
	//	if err != nil {
	//		fmt.Printf("err adding key %v\n", err)
	//	}

	//Put the following in DCP nozzle to dump uprEvent to a file
	// Import: bytes, encoding/json, io/ioutil

	//	fileName := "/tmp/uprEventDump.bin"
	//	dumpBytes := new(bytes.Buffer)
	//	json.NewEncoder(dumpBytes).Encode(*m)
	//	writeErr := ioutil.WriteFile(fileName, dumpBytes.Bytes(), 0644)
	//	if writeErr == nil {
	//		dcp.Logger().Infof("Wrote dump file successfully to %v\n", fileName)
	//	} else {
	//		dcp.Logger().Warnf("Unable to write file due to %v\n", writeErr)
	//	}

	assert := assert.New(t)
	result, checkErr := testUtils.FilterExpressionMatchesDoc(testExpression, docKey, "Administrator", "wewewe", "b1", "127.0.0.1", 9000)
	assert.Nil(checkErr)
	assert.True(result)

	fmt.Println("============== Test case end: TestGenerateXattrUsingGoCB =================")
}

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

	bodySlice, err = base.AddXattrToBeFiltered(bodySlice, xattrSlice)
	assert.Nil(err)

	bodySlice, err = base.AddKeyToBeFiltered(bodySlice, []byte(docKey))
	assert.Nil(err)

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
