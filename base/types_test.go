/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUleb128EncoderDecoder(t *testing.T) {
	fmt.Println("============== Test case start: TestUleb128EncoderDecoder =================")
	assert := assert.New(t)

	seed := rand.NewSource(time.Now().UnixNano())
	generator := rand.New(seed)

	for i := 0; i < 50; i++ {
		input := generator.Uint32()
		testLeb, _, err := NewUleb128(input, nil, true)
		assert.Nil(err)

		verifyOutput := testLeb.ToUint32()
		assert.Equal(verifyOutput, input)
	}

	// Direct mem mapping test - for reading key with embedded CID
	var testByteSlice []byte = make([]byte, 1, 1)
	testByteSlice[0] = 0x09
	var testOut uint32 = Uleb128(testByteSlice).ToUint32()
	assert.Equal(uint32(9), testOut)

	fmt.Println("============== Test case end: TestUleb128EncoderDecoder =================")
}

func TestCollectionNamespaceFromString(t *testing.T) {
	fmt.Println("============== Test case start: TestCollectionNamespaceFromString =================")
	defer fmt.Println("============== Test case end: TestCollectionNamespaceFromString =================")

	assert := assert.New(t)
	namespace, err := NewCollectionNamespaceFromString("a123.123b")
	assert.Nil(err)
	assert.Equal("a123", namespace.ScopeName)
	assert.Equal("123b", namespace.CollectionName)

	_, err = NewCollectionNamespaceFromString("abcdef")
	assert.NotNil(err)
}

func TestExplicitMappingValidatorParseRule(t *testing.T) {
	fmt.Println("============== Test case start: TestExplicitMappingValidatorParseRule =================")
	defer fmt.Println("============== Test case end: TestExplicitMappingValidatorParseRule =================")
	assert := assert.New(t)

	validator := NewExplicitMappingValidator()

	key := "Scope"
	value := "Scope"
	assert.Equal(explicitRuleScopeToScope, validator.parseRule(key, value))
	assert.Equal(explicitRuleScopeToScope, validator.parseRule(key, nil))

	key = "Scope.collection"
	value = "scope2.collection2"
	assert.Equal(explicitRuleOneToOne, validator.parseRule(key, value))
	assert.Equal(explicitRuleOneToOne, validator.parseRule(key, nil))

	// Invalid names
	key = "#%(@&#FJ"
	value = "scope"
	assert.Equal(explicitRuleInvalidScopeName, validator.parseRule(key, value))

	// Too Long
	var longStrArr []string
	for i := 0; i < 256; i++ {
		longStrArr = append(longStrArr, "a")
	}
	longStr := strings.Join(longStrArr, "")
	key = longStr
	assert.Equal(explicitRuleStringTooLong, validator.parseRule(longStr, nil))

	value = longStr
	assert.Equal(explicitRuleStringTooLong, validator.parseRule(longStr, nil))

	key = "abc"
	assert.Equal(explicitRuleStringTooLong, validator.parseRule(longStr, nil))
}

func TestExplicitMappingValidatorRules(t *testing.T) {
	fmt.Println("============== Test case start: TestExplicitMappingValidatorRules =================")
	defer fmt.Println("============== Test case end: TestExplicitMappingValidatorRules =================")
	assert := assert.New(t)

	validator := NewExplicitMappingValidator()
	// We only disallow _system scope
	key := "_invalidScopeName"
	value := "validTargetScopeName"
	assert.Nil(validator.ValidateKV(key, value))

	key = "validScopeName"
	value = "%invalidScopeName"
	assert.NotNil(validator.ValidateKV(key, value))

	// Positive test cases
	key = "Scope"
	value = "TargetScope"
	assert.Nil(validator.ValidateKV(key, value))

	key = "Scope2"
	value = "TargetScope2"
	assert.Nil(validator.ValidateKV(key, value))

	// 1-N is disallowed
	key = "Scope"
	value = "TargetScope2"
	assert.NotNil(validator.ValidateKV(key, value))

	// N-1 is not allowed
	key = "Scope2Test"
	value = "TargetScope2"
	assert.NotNil(validator.ValidateKV(key, value))

	key = "AnotherScope.AnotherCollection"
	value = "AnotherTargetScope.AnotherTargetCollection"
	assert.Nil(validator.ValidateKV(key, value))

	key = "AnotherScope2.AnotherCollection2"
	value = "AnotherTargetScope2.AnotherTargetCollection2"
	assert.Nil(validator.ValidateKV(key, value))

	// 1-N is not allowed
	key = "AnotherScope.AnotherCollection"
	value = "AnotherTargetScope.AnotherTargetCollection2"
	assert.NotNil(validator.ValidateKV(key, value))

	// N-1 is not allowed
	key = "AnotherScope2.AnotherCollection3"
	value = "AnotherTargetScope2.AnotherTargetCollection2"
	assert.NotNil(validator.ValidateKV(key, value))

	// Adding non-duplicating blacklist rules
	key = "Scope3"
	assert.Nil(validator.ValidateKV(key, nil))

	key = "Scope.Collection"
	assert.Nil(validator.ValidateKV(key, nil))

	// Adding duplicating blacklist rules
	key = "Scope3.Collection3"
	assert.NotNil(validator.ValidateKV(key, nil))

	key = "Scope"
	assert.NotNil(validator.ValidateKV(key, nil))

	// Test complex mapping - one specific collection will have special mapping, everything else implicit under scope
	// 1. ScopeRedundant.ColRedundant -> ScopeTRedundant.ColTRedundant
	// 2. ScopeRedundant -> ScopeTRedundant
	key = "ScopeRedundant"
	value = "ScopeTRedundant"
	assert.Nil(validator.ValidateKV(key, value))

	// This is not redundant
	key = "ScopeRedundant.ColRedundant"
	value = "ScopeTRedundant.ColTRedundant"
	assert.Nil(validator.ValidateKV(key, value))

	// This is redundant
	key = "ScopeRedundant.ColRedundant"
	value = "ScopeTRedundant.ColRedundant"
	assert.NotNil(validator.ValidateKV(key, value))

	// ** Converse of above
	// Not Redundant
	key = "ScopeRedundant2.ColRedundant2"
	value = "ScopeTRedundant2.ColTRedundant2"
	assert.Nil(validator.ValidateKV(key, value))

	// Combining both should be fine
	key = "ScopeRedundant2"
	value = "ScopeTRedundant2"
	assert.Nil(validator.ValidateKV(key, value))

	// System scope cannot be mapped
	key = "_system"
	value = "scope"
	assert.Equal(ErrorSystemScopeMapped, validator.ValidateKV(key, value))

	// Cannot map to system scope
	key = "scope"
	value = "_system"
	assert.Equal(ErrorSystemScopeMapped, validator.ValidateKV(key, value))

	// System collection cannot be mapped
	key = "_system._mobile"
	value = "scope.collection"
	assert.Equal(ErrorSystemScopeMapped, validator.ValidateKV(key, value))

	// Cannot map to a system collection
	key = "scope.collection"
	value = "_system._mobile"
	assert.Equal(ErrorSystemScopeMapped, validator.ValidateKV(key, value))
}

func TestWrappedFlags(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestWrappedFlags =================")
	defer fmt.Println("============== Test case end: TestWrappedFlags =================")

	wrappedUpr := WrappedUprEvent{}
	assert.False(wrappedUpr.Flags.CollectionDNE())

	wrappedUpr.Flags.SetCollectionDNE()
	assert.True(wrappedUpr.Flags.CollectionDNE())
}

func TestDefaultNs(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestDefaultNS =================")
	defer fmt.Println("============== Test case end: TestDefaultNS =================")

	validator := NewExplicitMappingValidator()
	assert.Equal(explicitRuleScopeToScope, validator.parseRule("_default", "_default"))
	assert.Equal(explicitRuleScopeToScope, validator.parseRule("_default", nil))
	assert.Equal(explicitRuleOneToOne, validator.parseRule("_default._default", nil))
	assert.Equal(explicitRuleOneToOne, validator.parseRule("_default._default", "_default._default"))
	validator = NewExplicitMappingValidator()
	assert.Equal(explicitRuleOneToOne, validator.parseRule("_default.testCol", nil))
	assert.Equal(explicitRuleOneToOne, validator.parseRule("_default.testCol", "_default._default"))
	assert.Equal(explicitRuleOneToOne, validator.parseRule("testScope.testCol", nil))
	assert.Equal(explicitRuleOneToOne, validator.parseRule("testScope.testCol", "_default.nonDefCol"))

}

func TestValidRemoteClusterName(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestValidRemoteClusterName =================")
	defer fmt.Println("============== Test case end: TestValidRemoteClusterName =================")

	errMap := make(ErrorMap)

	ValidateRemoteClusterName("abc.be_fd.com", errMap)
	assert.Equal(0, len(errMap))

	ValidateRemoteClusterName("abc", errMap)
	assert.Equal(0, len(errMap))

	ValidateRemoteClusterName("12.23.34.45", errMap)
	assert.Equal(0, len(errMap))

	ValidateRemoteClusterName("endwithaPeriod.com.", errMap)
	assert.NotEqual(0, len(errMap))
}

func TestValidRules(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestValidRules =================")
	defer fmt.Println("============== Test case end: TestValidRules =================")

	validator := NewExplicitMappingValidator()
	assert.Nil(validator.ValidateKV("scope1", DefaultScopeCollectionName))
	assert.Nil(validator.ValidateKV("scope1.collection1", "scope1.collection1"))
	assert.Nil(validator.ValidateKV("scope1.collection2", "scope1.collection2"))

	validator = NewExplicitMappingValidator()
	assert.Nil(validator.ValidateKV("scope1.collection1", "scope1.collection1"))
	assert.Nil(validator.ValidateKV("scope1.collection2", "scope1.collection2"))
	assert.Nil(validator.ValidateKV("scope1", nil))

	validator = NewExplicitMappingValidator()
	assert.Nil(validator.ValidateKV("scope1", nil))
	assert.Nil(validator.ValidateKV("scope1.collection1", "scope1.collection1"))
	assert.Nil(validator.ValidateKV("scope1.collection2", "scope1.collection2"))
}

func TestRulesRedundancyCheck(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestRulesRedundancyCheck =================")
	defer fmt.Println("============== Test case end: TestRulesRedundancyCheck =================")

	validator := NewExplicitMappingValidator()
	assert.Nil(validator.ValidateKV("testScope.testCol", "testScope2.testCol"))
	assert.NotNil(validator.ValidateKV("testScope", "testScope2"))

	validator = NewExplicitMappingValidator()
	assert.Nil(validator.ValidateKV("testScope", "testScope2"))
	assert.NotNil(validator.ValidateKV("testScope.testCol", "testScope2.testCol"))

	validator = NewExplicitMappingValidator()
	assert.NotNil(validator.ValidateKV("scope1", ""))
	assert.NotNil(validator.ValidateKV("", "scope1"))
}

func TestRuleRedundancyNilCheck(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestRulesRedundancyCheck =================")
	defer fmt.Println("============== Test case end: TestRulesRedundancyCheck =================")

	validator := NewExplicitMappingValidator()
	assert.Nil(validator.ValidateKV("testScope", nil))
	assert.NotNil(validator.ValidateKV("testScope.testCol", nil))

	validator = NewExplicitMappingValidator()
	assert.Nil(validator.ValidateKV("testScope.testCol", nil))
	assert.NotNil(validator.ValidateKV("testScope", nil))
}

func TestRuleNameTooLong(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestRuleNameTooLong =================")
	defer fmt.Println("============== Test case end: TestRuleNameTooLong =================")

	longStr := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	validator := NewExplicitMappingValidator()
	assert.NotNil(validator.ValidateKV(longStr, longStr))
	ns2 := fmt.Sprintf("%v%v%v", longStr, ScopeCollectionDelimiter, longStr)
	assert.NotNil(validator.ValidateKV(ns2, ns2))
}
