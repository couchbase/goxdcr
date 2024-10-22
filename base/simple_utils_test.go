/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/couchbaselabs/gojsonsm"
	"github.com/stretchr/testify/assert"
)

const testFilteringDataDir = "filter/testFilteringData/"

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

func TestInSert(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestInsert =================")
	totalBytes := json.RawMessage(`abcdef`)
	insertBytes := json.RawMessage(`12345`)

	totalBytes, err := CleanInsert(totalBytes, insertBytes, 1)
	assert.Nil(err)

	resultBytes := json.RawMessage(`a12345bcdef`)

	assert.Equal(resultBytes, totalBytes)

	fmt.Println("============== Test case end: TestInsert =================")
}

func TestKeyOnlyExpr(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestKeyOnlyExpr =================")
	var expressions []string
	expressions = append(expressions, fmt.Sprintf("%v=\"abc\"", ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("%v='abc'", ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("%v =   \"abc\"", ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("NOT %v<\"abc\"", ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("NOT %v>=\"abc\"", ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("NOT %v>=\"abc\" OR REGEXP_CONTAINS(%v, \"^d\")", ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("NOT %v>=\"abc\" OR REGEXP_CONTAINS(%v, '^d')", ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("REGEXP_CONTAINS(%v,\"^abc$\")", ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("REGEXP_CONTAINS(%v,'^abc$')", ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("NOT REGEXP_CONTAINS(%v ,  \"^abc$\"  )", ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("NOT REGEXP_CONTAINS(%v ,  \"^abc$\"  ) AND %v = \"abc\"", ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("NOT REGEXP_CONTAINS(%v ,  \"^abc$\"  ) AND %v > \"a\"", ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("REGEXP_CONTAINS(%v ,  \"^abc$\"  ) AND NOT %v > \"a\" OR REGEXP_CONTAINS(%v,\"^123\")", ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("NOT REGEXP_CONTAINS(%v ,  \"^abc$\"  ) AND %v = \"abc\" AND NOT %v = \"def\"", ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey]))

	var falseExpr []string
	// fields are not wrapped
	falseExpr = append(falseExpr, fmt.Sprintf("%v = testField", ReverseReservedWordsMap[InternalKeyKey]))
	falseExpr = append(falseExpr, fmt.Sprintf("%v > testField", ReverseReservedWordsMap[InternalKeyKey]))
	falseExpr = append(falseExpr, fmt.Sprintf("REGEXP_CONTAINS(%v, testField)", ReverseReservedWordsMap[InternalKeyKey]))
	falseExpr = append(falseExpr, fmt.Sprintf("%v=\"abc\" AND %v.xatrKey = \"value\"", ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyXattr]))
	falseExpr = append(falseExpr, fmt.Sprintf("%v=\"abc\" AND %v.xatrKey = \"value\"", ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyXattr]))
	falseExpr = append(falseExpr, fmt.Sprintf("NOT REGEXP_CONTAINS(%v ,  \"^abc$\"  ) AND ", ReverseReservedWordsMap[InternalKeyKey]))
	falseExpr = append(falseExpr, fmt.Sprintf("NOT %v>=\"abc\" AND", ReverseReservedWordsMap[InternalKeyKey]))
	falseExpr = append(falseExpr, fmt.Sprintf("REGEXP_CONTAINS(%v, \"value\") AND field = \"value\")", ReverseReservedWordsMap[InternalKeyKey]))
	// Fails because = op should only take identifier string
	falseExpr = append(falseExpr, fmt.Sprintf("%v=\"^abc$\"", ReverseReservedWordsMap[InternalKeyXattr]))
	// Parenthesis not supported
	falseExpr = append(falseExpr, fmt.Sprintf("REGEXP_CONTAINS(%v ,  \"^abc$\"  ) AND (NOT %v > \"a\" OR REGEXP_CONTAINS(%v,\"^123\"))", ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey]))
	// Try to sneak one in
	falseExpr = append(falseExpr, fmt.Sprintf("NOT REGEXP_CONTAINS(%v ,  \"^abc$\"  ) AND %v = \"abc\" AND NOT %v = field", ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey]))
	// Disallow double quotes within REGEXP_CONTAINS to prevent greedy injection (i.e. mixing of valid syntax)
	falseExpr = append(falseExpr, fmt.Sprintf("REGEXP_CONTAINS(%v, \"value\") AND field2 = field1 AND field = ROUND(\"23\")", ReverseReservedWordsMap[InternalKeyKey]))
	falseExpr = append(falseExpr, fmt.Sprintf("REGEXP_CONTAINS(%v,\"^a\"+b\"+c$\")", ReverseReservedWordsMap[InternalKeyKey]))
	falseExpr = append(falseExpr, fmt.Sprintf("REGEXP_CONTAINS(%v,\"^a'+b'+c$\")", ReverseReservedWordsMap[InternalKeyKey]))
	falseExpr = append(falseExpr, fmt.Sprintf("REGEXP_CONTAINS(%v,'^a\"b\"c$')", ReverseReservedWordsMap[InternalKeyKey]))
	falseExpr = append(falseExpr, fmt.Sprintf("%v = \"a \" AND %v = \"b\"\"", ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey]))
	// Disallow single quotes
	falseExpr = append(falseExpr, fmt.Sprintf("REGEXP_CONTAINS(%v,''^abc$'')", ReverseReservedWordsMap[InternalKeyKey]))
	falseExpr = append(falseExpr, fmt.Sprintf("REGEXP_CONTAINS(%v,\"'^abc$'\")", ReverseReservedWordsMap[InternalKeyKey]))

	for _, expr := range expressions {
		result := FilterOnlyContainsKeyExpression(expr)
		assert.True(result)
		if !result {
			fmt.Printf("Failed expr: %v\n", expr)
		}
	}

	for _, expr := range falseExpr {
		result := FilterOnlyContainsKeyExpression(expr)
		assert.False(result)
		if result {
			fmt.Printf("Succeeded expr: %v\n", expr)
		}
	}
	fmt.Println("============== Test case end: TestKeyOnlyExpr =================")
}

func TestXattrOnlyExpr(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestXattrOnlyExpr =================")
	var expressions []string
	expressions = append(expressions, fmt.Sprintf("%v.key=\"abc\"", ReverseReservedWordsMap[InternalKeyXattr]))
	expressions = append(expressions, fmt.Sprintf("%v.key='abc'", ReverseReservedWordsMap[InternalKeyXattr]))
	expressions = append(expressions, fmt.Sprintf("%v.key =   \"abc\"", ReverseReservedWordsMap[InternalKeyXattr]))
	expressions = append(expressions, fmt.Sprintf("%v.key.key2 =   \"abc\"", ReverseReservedWordsMap[InternalKeyXattr]))
	expressions = append(expressions, fmt.Sprintf("NOT %v.key<\"abc\"", ReverseReservedWordsMap[InternalKeyXattr]))
	expressions = append(expressions, fmt.Sprintf("NOT %v.key>=\"abc\"", ReverseReservedWordsMap[InternalKeyXattr]))
	expressions = append(expressions, fmt.Sprintf("NOT %v.key.key2 >=\"abc\"", ReverseReservedWordsMap[InternalKeyXattr]))
	expressions = append(expressions, fmt.Sprintf("NOT %v.key>=\"abc\" OR REGEXP_CONTAINS(%v.key, \"^d\")", ReverseReservedWordsMap[InternalKeyXattr], ReverseReservedWordsMap[InternalKeyXattr]))
	expressions = append(expressions, fmt.Sprintf("NOT %v.key.key2>=\"abc\" OR REGEXP_CONTAINS(%v.key.key2, '^d')", ReverseReservedWordsMap[InternalKeyXattr], ReverseReservedWordsMap[InternalKeyXattr]))
	expressions = append(expressions, fmt.Sprintf("REGEXP_CONTAINS(%v.key,\"^abc$\")", ReverseReservedWordsMap[InternalKeyXattr]))
	expressions = append(expressions, fmt.Sprintf("REGEXP_CONTAINS(%v.key,'^abc$')", ReverseReservedWordsMap[InternalKeyXattr]))
	expressions = append(expressions, fmt.Sprintf("NOT REGEXP_CONTAINS(%v.key ,  \"^abc$\"  )", ReverseReservedWordsMap[InternalKeyXattr]))
	expressions = append(expressions, fmt.Sprintf("NOT REGEXP_CONTAINS(%v.key ,  \"^abc$\"  ) AND %v.key = \"abc\"", ReverseReservedWordsMap[InternalKeyXattr], ReverseReservedWordsMap[InternalKeyXattr]))
	expressions = append(expressions, fmt.Sprintf("NOT REGEXP_CONTAINS(%v.key ,  \"^abc$\"  ) AND %v.key.key2 > \"a\"", ReverseReservedWordsMap[InternalKeyXattr], ReverseReservedWordsMap[InternalKeyXattr]))
	expressions = append(expressions, fmt.Sprintf("NOT REGEXP_CONTAINS(%v.key ,  \"^abc$\"  ) AND %v.key = \"abc\" AND NOT %v.key = \"def\"", ReverseReservedWordsMap[InternalKeyXattr], ReverseReservedWordsMap[InternalKeyXattr], ReverseReservedWordsMap[InternalKeyXattr]))
	expressions = append(expressions, fmt.Sprintf("REGEXP_CONTAINS(%v.key ,  \"^abc$\"  ) OR REGEXP_CONTAINS(%v.key,\"^123\")", ReverseReservedWordsMap[InternalKeyXattr], ReverseReservedWordsMap[InternalKeyXattr]))

	var falseExpr []string
	// fields are not wrapped
	falseExpr = append(falseExpr, fmt.Sprintf("%v.key = testField", ReverseReservedWordsMap[InternalKeyXattr]))
	falseExpr = append(falseExpr, fmt.Sprintf("%v.key > testField", ReverseReservedWordsMap[InternalKeyXattr]))
	falseExpr = append(falseExpr, fmt.Sprintf("REGEXP_CONTAINS(%v.key, testField)", ReverseReservedWordsMap[InternalKeyXattr]))
	// Adding a field - actually adding anything is considered a complex enough to not need to optimize for
	falseExpr = append(falseExpr, fmt.Sprintf("%v.key + field > \"abc\"", ReverseReservedWordsMap[InternalKeyXattr]))
	falseExpr = append(falseExpr, fmt.Sprintf("ABS(%v.key) > \"abc\"", ReverseReservedWordsMap[InternalKeyXattr]))
	// Mixed key with xattr mode
	falseExpr = append(falseExpr, fmt.Sprintf("%v=\"abc\" AND %v.xatrKey = \"value\"", ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyXattr]))
	falseExpr = append(falseExpr, fmt.Sprintf("NOT REGEXP_CONTAINS(%v.key ,  \"^abc$\"  ) AND ", ReverseReservedWordsMap[InternalKeyXattr]))
	falseExpr = append(falseExpr, fmt.Sprintf("NOT %v.key>=\"abc\" AND", ReverseReservedWordsMap[InternalKeyXattr]))
	// Fails because = op should only take identifier string
	falseExpr = append(falseExpr, fmt.Sprintf("%v.key=\"^abc$\"", ReverseReservedWordsMap[InternalKeyXattr]))
	// Parenthesis not supported
	falseExpr = append(falseExpr, fmt.Sprintf("REGEXP_CONTAINS(%v.key ,  \"^abc$\"  ) AND (NOT %v.key > \"a\" OR REGEXP_CONTAINS(%v.key,\"^123\"))", ReverseReservedWordsMap[InternalKeyXattr], ReverseReservedWordsMap[InternalKeyXattr], ReverseReservedWordsMap[InternalKeyXattr]))
	// Try to sneak one in
	falseExpr = append(falseExpr, fmt.Sprintf("NOT REGEXP_CONTAINS(%v.key ,  \"^abc$\"  ) AND %v.key = \"abc\" AND NOT %v.key = field", ReverseReservedWordsMap[InternalKeyXattr], ReverseReservedWordsMap[InternalKeyXattr], ReverseReservedWordsMap[InternalKeyXattr]))
	// No " or ' in character classes
	falseExpr = append(falseExpr, fmt.Sprintf("REGEXP_CONTAINS(%v.key,\"^a\"+b\"+c$\")", ReverseReservedWordsMap[InternalKeyXattr]))
	falseExpr = append(falseExpr, fmt.Sprintf("REGEXP_CONTAINS(%v.key,\"^a'+b'+c$\")", ReverseReservedWordsMap[InternalKeyXattr]))
	falseExpr = append(falseExpr, fmt.Sprintf("REGEXP_CONTAINS(%v.key,'^a\"b\"c$')", ReverseReservedWordsMap[InternalKeyXattr]))
	// Arrays are not supported. Too bad
	falseExpr = append(falseExpr, fmt.Sprintf("NOT %v.key[3] > \"a\"", ReverseReservedWordsMap[InternalKeyXattr]))

	for _, expr := range expressions {
		result := FilterOnlyContainsXattrExpression(expr)
		assert.True(result)
		if !result {
			fmt.Printf("Failed expr: %v\n", expr)
		}
	}

	for _, expr := range falseExpr {
		result := FilterOnlyContainsXattrExpression(expr)
		assert.False(result)
		if result {
			fmt.Printf("Succeeded expr: %v\n", expr)
		}
	}
	fmt.Println("============== Test case end: TestXattrOnlyExpr =================")
}

// Before MB-33032 panics... now should not panic
func TestInvalidRegex(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestInsert =================")
	err := ValidateAdvFilter("REGEXP_CONTAINS(META().id, \"Invalid((((((\")")
	assert.NotNil(err)
	fmt.Println("============== Test case end: TestInsert =================")
}

func TestCustomRawJson(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCustomRawJson =================")
	retMap, xattrSlice, err := getXattrValueMock()
	assert.Nil(err)
	assert.NotEqual(0, len(retMap))
	assert.NotEqual(0, len(xattrSlice))

	allocatedBytes := make([]byte, len(xattrSlice))
	var pos int

	for k, v := range retMap {
		keySlice := []byte(k)
		allocatedBytes, pos = WriteJsonRawMsg(allocatedBytes, keySlice, pos, WriteJsonKey, len(keySlice), pos == 0 /*firstKey*/)

		marshaledValueBytes, err := json.Marshal(v)
		assert.Nil(err)
		allocatedBytes, pos = WriteJsonRawMsg(allocatedBytes, marshaledValueBytes, pos, WriteJsonValueNoQuotes, len(marshaledValueBytes), pos == 0 /*firstKey*/)
	}

	checkMap := make(map[string]interface{})
	err = json.Unmarshal(allocatedBytes, &checkMap)
	assert.Nil(err)

	for k, v := range checkMap {
		checkValue, ok := retMap[k]
		assert.True(ok)
		assert.True(reflect.DeepEqual(checkValue, v))
	}
	fmt.Println("============== Test case end: TestCustomRawJson =================")
}

func TestSkipXattrAndStringsConversionNonPcre(t *testing.T) {
	fmt.Println("============== Test case start: TestSkipXattrAndStringsConversionNonPcre =================")
	assert := assert.New(t)
	InitPcreVars()

	userFilter := "META().id = \"something\" AND META().xattrs.testXattrKey EXISTS"
	assert.True(FilterContainsXattrExpression(userFilter))
	assert.True(FilterContainsKeyExpression(userFilter))

	filterExpressionInternal := ReplaceKeyWordsForExpression(userFilter)

	assert.False(strings.Contains(filterExpressionInternal, ExternalKeyXattrContains))
	assert.False(strings.Contains(filterExpressionInternal, ExternalKeyKeyContains))
	assert.False(strings.Contains(filterExpressionInternal, "META()"))

	filterExpressionExternal := ReplaceKeyWordsForOutput(filterExpressionInternal)
	assert.True(strings.Contains(filterExpressionExternal, "META()"))
	// No escaped (i.e. no "META\\(\\)" )
	assert.False(strings.Contains(filterExpressionExternal, ExternalKeyXattr))
	assert.False(strings.Contains(filterExpressionExternal, ExternalKeyKey))
	assert.True(strings.Contains(filterExpressionExternal, ExternalKeyXattrContains))
	assert.True(strings.Contains(filterExpressionExternal, ExternalKeyKeyContains))

	userFilter = fmt.Sprintf("REGEXP_CONTAINS(`%v`, \"^d\")", InternalKeyKey)
	assert.False(strings.Contains(userFilter, "META()"))
	userFilter = ReplaceKeyWordsForOutput(userFilter)
	assert.True(strings.Contains(filterExpressionExternal, "META()"))

	fmt.Println("============== Test case end: TestSkipXattrAndStringsConversionNonPcre =================")
}

func TestFlagType(t *testing.T) {
	fmt.Println("============== Test case start: TestFlagType =================")
	assert := assert.New(t)
	baseType := FilterExpDelStripExpiration
	assert.True(baseType&FilterExpDelStripExpiration > 0)
	baseType.SetStripExpiration(false)
	assert.False(baseType&FilterExpDelStripExpiration > 0)
	fmt.Println("============== Test case end: TestFlagType =================")
}

// MB-33121
func TestLegacyKeyMsg(t *testing.T) {
	fmt.Println("============== Test case start: TestLegacyKeyMsg =================")
	assert := assert.New(t)

	err := ValidateAdvFilter("aSingleRegex$")
	assert.Equal(ErrorFilterInvalidFormat, err)

	err = ValidateAdvFilter("int LIKE 0")
	assert.Equal(ErrorFilterInvalidExpression, err)
	fmt.Println("============== Test case end: TestLegacyKeyMsg =================")
}

// Show and tell demo
func TestDemo(t *testing.T) {
	assert := assert.New(t)

	filter := `(county = "United States" OR country = "Canada" AND type="brewery") OR (type="beer" AND DATE(updated) >= DATE("2019-01-01"))`
	matcher, err := ValidateAndGetAdvFilter(filter)
	assert.Nil(err)

	realMatcher, realErr := gojsonsm.GetFilterExpressionMatcher(filter)
	assert.Nil(realErr)

	marshalledData := []byte(`{"[$%XDCRInternalKey*%$]":"big_buck_brewery-big_buck_beer","[$%XDCRInternalMeta*%$]":{},"abv":5.2,"brewery_id":"big_buck_brewery","category":"North American Ale","description":"A standard American-style beer and our flagship brand.  A small amount of corn is added to the grist to give the brew a smooth character.  Features a rich, golden color and a light malt character balanced with a mild dose of hops.","ibu":0,"name":"Big Buck Beer","srm":0,"style":"American-Style Pale Ale","type":"beer","upc":0,"updated":"2019-03-22 20:00:20"}`)

	result, status, err := MatchWrapper(matcher, marshalledData)
	assert.True(result)
	assert.Equal(gojsonsm.MatcherNoStatus, status)

	result, err = realMatcher.Match(marshalledData)
	assert.True(result)
	assert.Nil(err)
}

func TestSkipXattrAndStringsConversion(t *testing.T) {
	fmt.Println("============== Test case start: TestSkipXattrAndStringsConversion =================")
	assert := assert.New(t)
	InitPcreVars()

	userFilter := "META().id = \"something\" AND META().xattrs.testXattrKey EXISTS"
	assert.True(FilterContainsXattrExpression(userFilter))
	assert.True(FilterContainsKeyExpression(userFilter))

	filterExpressionInternal := ReplaceKeyWordsForExpression(userFilter)

	assert.False(strings.Contains(filterExpressionInternal, ExternalKeyXattrContains))
	assert.False(strings.Contains(filterExpressionInternal, ExternalKeyKeyContains))
	assert.False(strings.Contains(filterExpressionInternal, "META()"))

	filterExpressionExternal := ReplaceKeyWordsForOutput(filterExpressionInternal)
	assert.True(strings.Contains(filterExpressionExternal, "META()"))
	// No escaped (i.e. no "META\\(\\)" )
	assert.False(strings.Contains(filterExpressionExternal, ExternalKeyXattr))
	assert.False(strings.Contains(filterExpressionExternal, ExternalKeyKey))
	assert.True(strings.Contains(filterExpressionExternal, ExternalKeyXattrContains))
	assert.True(strings.Contains(filterExpressionExternal, ExternalKeyKeyContains))

	userFilter = fmt.Sprintf("REGEXP_CONTAINS(`%v`, \"^d\")", InternalKeyKey)
	assert.False(strings.Contains(userFilter, "META()"))
	userFilter = ReplaceKeyWordsForOutput(userFilter)
	assert.True(strings.Contains(filterExpressionExternal, "META()"))

	fmt.Println("============== Test case end: TestSkipXattrAndStringsConversion =================")
}

func TestLowerCaseOperator(t *testing.T) {
	assert := assert.New(t)

	filter := "regexp_contains(key, \"something\")"
	err := ValidateAdvFilter(filter)
	assert.NotNil(err)
	assert.True(strings.Contains(err.Error(), "case sensitive"))
}

func TestReverseVBNodesMap(t *testing.T) {
	fmt.Println("============== Test case start: TestReverseVBNodesMap =================")
	defer fmt.Println("============== Test case end: TestReverseVBNodesMap =================")

	assert := assert.New(t)
	inputMap := make(VbHostsMapType)
	str0 := []string{"node0", "node1"}
	str1 := []string{"node1", "node2"}
	str2 := []string{"node2", "node3"}
	inputMap[0] = &str0
	inputMap[1] = &str1
	inputMap[2] = &str2

	output := ReverseVBNodesMap(inputMap, func([]string) *KvVBMapType {
		retMap := make(KvVBMapType)
		return &retMap
	})
	assert.Len(*output, 4)
	assert.Len((*output)["node0"], 1)
	assert.Len((*output)["node1"], 2)
	assert.Len((*output)["node2"], 2)
	assert.Len((*output)["node3"], 1)
}

func TestSanitizeSeqnoMapKey(t *testing.T) {
	fmt.Println("============== Test case start: SanitizeSeqnoMapKey =================")
	defer fmt.Println("============== Test case end: SanitizeSeqnoMapKey =================")

	assert := assert.New(t)

	key := ComposeVBHighSeqnoStatsKey(123)
	vbno, err := DecomposeVBHighSeqnoStatsKey(key)
	assert.Nil(err)
	assert.Equal(uint16(123), vbno)
}

func TestValidateRemoteClusterName(t *testing.T) {
	fmt.Println("============== Test case start: TestValidateRemoteClusterName =================")
	defer fmt.Println("============== Test case end: TestValidateRemoteClusterName =================")

	nonWhiteSpaceErrMsg := "Remote cluster name should only be IPv4, IPv6, or alpha-numeric characters"
	whiteSpaceErrMsg := "Remote cluster name should only be IPv4, IPv6, or alpha-numeric characters. It contains white-space characters like space, newline, tabspace etc"
	type args struct {
		name      string
		errorsMap map[string]error
	}
	tests := []struct {
		tname string
		args  args
		res   string
	}{
		{tname: "no_errors1", args: args{name: "dst", errorsMap: map[string]error{}}, res: ""},
		{tname: "no_errors2", args: args{name: "192.168.2.3", errorsMap: map[string]error{}}, res: ""},
		{tname: "no_errors3", args: args{name: "svc-qi-node-009.fd0tr99wpja7egdl.cloud.couchbase.com", errorsMap: map[string]error{}}, res: ""},
		{tname: "with_space", args: args{name: "new dst", errorsMap: map[string]error{}}, res: whiteSpaceErrMsg},
		{tname: "with_multiple_spaces", args: args{name: "new   dst", errorsMap: map[string]error{}}, res: whiteSpaceErrMsg},
		{tname: "with_newline", args: args{name: "new\ndst", errorsMap: map[string]error{}}, res: whiteSpaceErrMsg},
		{tname: "with_tabspace", args: args{name: "new\tdst", errorsMap: map[string]error{}}, res: whiteSpaceErrMsg},
		{tname: "non_whitespace_error1", args: args{name: "dst^2", errorsMap: map[string]error{}}, res: nonWhiteSpaceErrMsg},
		{tname: "non_whitespace_error2", args: args{name: "dstnew#", errorsMap: map[string]error{}}, res: nonWhiteSpaceErrMsg},
	}
	for _, tt := range tests {
		t.Run(tt.tname, func(t *testing.T) {
			ValidateRemoteClusterName(tt.args.name, tt.args.errorsMap)
			op := ""
			err, ok := tt.args.errorsMap[RemoteClusterName]
			if ok {
				op = fmt.Sprintf("%v", err)
			}
			assert.Equal(t, tt.res, op)
		})
	}
}

func TestIsJsonEndValid(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		want bool
	}{
		{
			name: "Success, without whitespaces",
			body: []byte("{\"foo\":\"bar\",\"foo1\":\"bar1\"}"),
			want: true,
		},
		{
			name: "Success, with whitespaces",
			body: []byte("[\"foo\",\"bar\"] \r\n"),
			want: true,
		},
		{
			name: "Failure, no } or ]",
			body: []byte("{\"foo\":\"bar\",\"foo1\":\"bar1\""),
		},
		{
			name: "Failure, no } or ], with whitespaces",
			body: []byte("{\"foo\":\"bar\",\"foo1\":\"bar1\"\r\n "),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsJsonEndValid(tt.body, len(tt.body)-1); got != tt.want {
				t.Errorf("IsJsonEndValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUint64ToHexLittleEndianAndStrip0s(t *testing.T) {
	a := assert.New(t)

	// 1. test all positions
	hexLE := []byte("0x0000000000000000")
	for i := 2; i < 18; i++ {
		hexLE[i] = '1'
		u64, err := HexLittleEndianToUint64(hexLE)
		a.Nil(err)
		hexLEStripped := Uint64ToHexLittleEndianAndStrip0s(u64)
		// inverse should result in the same result
		u64Stripped, err := HexLittleEndianToUint64(hexLEStripped)
		a.Nil(err)
		a.Equal(u64, u64Stripped)

		hexLE[i] = '9'
		u64, err = HexLittleEndianToUint64(hexLE)
		a.Nil(err)
		hexLEStripped = Uint64ToHexLittleEndianAndStrip0s(u64)
		u64Stripped, err = HexLittleEndianToUint64(hexLEStripped)
		a.Nil(err)
		a.Equal(u64, u64Stripped)

		hexLE[i] = 'a'
		u64, err = HexLittleEndianToUint64(hexLE)
		a.Nil(err)
		hexLEStripped = Uint64ToHexLittleEndianAndStrip0s(u64)
		u64Stripped, err = HexLittleEndianToUint64(hexLEStripped)
		a.Nil(err)
		a.Equal(u64, u64Stripped)

		hexLE[i] = 'f'
		u64, err = HexLittleEndianToUint64(hexLE)
		a.Nil(err)
		hexLEStripped = Uint64ToHexLittleEndianAndStrip0s(u64)
		u64Stripped, err = HexLittleEndianToUint64(hexLEStripped)
		a.Nil(err)
		a.Equal(u64, u64Stripped)

		hexLE[i] = '0'
	}

	// 2. test some cases
	hexLE = []byte("0x0000000000000000")
	u64, err := HexLittleEndianToUint64(hexLE)
	a.Nil(err)
	hexLEStripped := Uint64ToHexLittleEndianAndStrip0s(u64)
	u64Stripped, err := HexLittleEndianToUint64(hexLEStripped)
	a.Nil(err)
	a.Equal(u64, u64Stripped)

	hexLE = []byte("0xffffffffffffffff")
	u64, err = HexLittleEndianToUint64(hexLE)
	a.Nil(err)
	hexLEStripped = Uint64ToHexLittleEndianAndStrip0s(u64)
	u64Stripped, err = HexLittleEndianToUint64(hexLEStripped)
	a.Nil(err)
	a.Equal(u64, u64Stripped)

	hexLE = []byte("0xd123456e789a0bcf")
	u64, err = HexLittleEndianToUint64(hexLE)
	a.Nil(err)
	hexLEStripped = Uint64ToHexLittleEndianAndStrip0s(u64)
	u64Stripped, err = HexLittleEndianToUint64(hexLEStripped)
	a.Nil(err)
	a.Equal(u64, u64Stripped)

	if testing.Short() {
		return
	}

	// 3. random subset: run manually with -timeout 1000s
	threads := 20
	nums := 60000000
	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < nums; j++ {
				u64 := uint64(rand.Int63())
				hexLEStripped := Uint64ToHexLittleEndianAndStrip0s(u64)
				u64Stripped, err := HexLittleEndianToUint64(hexLEStripped)
				assert.Nil(t, err)
				assert.Equal(t, u64, u64Stripped)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
