// +build !pcre

package base

import (
	"encoding/json"
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
	expressions = append(expressions, fmt.Sprintf("%v =   \"abc\"", ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("NOT %v<\"abc\"", ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("NOT %v>=\"abc\"", ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("NOT %v>=\"abc\" OR REGEXP_CONTAINS(%v, \"^d\")", ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey]))
	expressions = append(expressions, fmt.Sprintf("REGEXP_CONTAINS(%v,\"^abc$\")", ReverseReservedWordsMap[InternalKeyKey]))
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
	falseExpr = append(falseExpr, fmt.Sprintf("NOT REGEXP_CONTAINS(%v ,  \"^abc$\"  ) AND ", ReverseReservedWordsMap[InternalKeyKey]))
	falseExpr = append(falseExpr, fmt.Sprintf("NOT %v>=\"abc\" AND", ReverseReservedWordsMap[InternalKeyKey]))
	// Fails because = op should only take identifier string
	falseExpr = append(falseExpr, fmt.Sprintf("%v=\"^abc$\"", ReverseReservedWordsMap[InternalKeyXattr]))
	// Parenthesis not supported
	falseExpr = append(falseExpr, fmt.Sprintf("REGEXP_CONTAINS(%v ,  \"^abc$\"  ) AND (NOT %v > \"a\" OR REGEXP_CONTAINS(%v,\"^123\"))", ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey]))
	// Try to sneak one in
	falseExpr = append(falseExpr, fmt.Sprintf("NOT REGEXP_CONTAINS(%v ,  \"^abc$\"  ) AND %v = \"abc\" AND NOT %v = field", ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey], ReverseReservedWordsMap[InternalKeyKey]))

	for _, expr := range expressions {
		assert.True(FilterOnlyContainsKeyExpression(expr))
	}

	for _, expr := range falseExpr {
		assert.False(FilterOnlyContainsKeyExpression(expr))
	}
	fmt.Println("============== Test case end: TestKeyOnlyExpr =================")
}
