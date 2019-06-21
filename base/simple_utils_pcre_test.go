// +build pcre

package base

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

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
