package base

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
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
	namespace, err := NewCollectionNamespaceFromString("a123:_123b")
	assert.Nil(err)
	assert.Equal("a123", namespace.ScopeName)
	assert.Equal("_123b", namespace.CollectionName)

	_, err = NewCollectionNamespaceFromString("abcdef")
	assert.NotNil(err)
}

func TestCollectionMigrateRuleValidation(t *testing.T) {
	fmt.Println("============== Test case start: TestCollectionMigrateRuleValidation =================")
	defer fmt.Println("============== Test case end: TestCollectionMigrateRuleValidation =================")
	assert := assert.New(t)

	validRules := make(map[string]interface{})
	validRules[fmt.Sprintf("REGEXP_CONTAINS(META().id, %v%v%v)", "\"", "^_abc", "\"")] = "targetScope1:targetCol1"
	validRules[fmt.Sprintf("doc.Value == %v%v%v AND doc.Value2 != %v%v%v", "\"", "abc", "\"", "\"", "def", "\"")] = "targetScope2:targetCol2"

	rules, err := ValidateAndConvertJsonMapToRuleType(validRules)
	assert.Nil(err)
	err = rules.ValidateMigrateRules()
	assert.Nil(err)

	invalidRules := make(map[string]interface{})
	// Incorrect target namespace
	invalidRules[fmt.Sprintf("REGEXP_CONTAINS(META().id, %v%v%v)", "\"", "^_abc", "\"")] = "targetScope1*"
	// Incorrect filter
	invalidRules[fmt.Sprintf("WRONGREGEXP_CONTAINS(META().id, %v%v%v)", "\"", "^_abc", "\"")] = "targetScope1*"
	rules, err = ValidateAndConvertJsonMapToRuleType(invalidRules)
	assert.Nil(err)
	err = rules.ValidateMigrateRules()
	assert.NotNil(err)

	doubleKey := "{\"key\":\"val\",\"key\":\"val2\"}"
	rules, err = ValidateAndConvertStringToMappingRuleType(doubleKey)
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

	key = "Scope:collection"
	value = "scope2:collection2"
	assert.Equal(explicitRuleOneToOne, validator.parseRule(key, value))
	assert.Equal(explicitRuleOneToOne, validator.parseRule(key, nil))

	// Invalid names
	key = "#%(@&#FJ"
	value = "scope"
	assert.Equal(explicitRuleInvalid, validator.parseRule(key, value))
}

func TestExplicitMappingValidatorRules(t *testing.T) {
	fmt.Println("============== Test case start: TestExplicitMappingValidatorRules =================")
	defer fmt.Println("============== Test case end: TestExplicitMappingValidatorRules =================")
	assert := assert.New(t)

	validator := NewExplicitMappingValidator()
	key := "Scope"
	value := "TargetScope"
	assert.Nil(validator.ValidateKV(key, value))

	key = "Scope2"
	value = "TargetScope2"
	assert.Nil(validator.ValidateKV(key, value))

	key = "AnotherScope:AnotherCollection"
	value = "AnotherTargetScope:AnotherTargetCollection"
	assert.Nil(validator.ValidateKV(key, value))

	key = "AnotherScope2:AnotherCollection2"
	value = "AnotherTargetScope2:AnotherTargetCollection2"
	assert.Nil(validator.ValidateKV(key, value))

	// Adding non-duplicating blacklist rules
	key = "Scope3"
	assert.Nil(validator.ValidateKV(key, nil))

	key = "Scope:Collection"
	assert.Nil(validator.ValidateKV(key, nil))

	// Adding duplicating blacklist rules
	key = "Scope3:Collection3"
	assert.NotNil(validator.ValidateKV(key, nil))

	key = "Scope"
	assert.NotNil(validator.ValidateKV(key, nil))

	// Test complex mapping - one specific collection will have special mapping, everything else implicit under scope
	// 1. ScopeRedundant:ColRedundant -> ScopeTRedundant:ColTRedundant
	// 2. ScopeRedundant -> ScopeTRedundant
	key = "ScopeRedundant"
	value = "ScopeTRedundant"
	assert.Nil(validator.ValidateKV(key, value))

	// This is not redundant
	key = "ScopeRedundant:ColRedundant"
	value = "ScopeTRedundant:ColTRedundant"
	assert.Nil(validator.ValidateKV(key, value))

	// This is redundant
	key = "ScopeRedundant:ColRedundant"
	value = "ScopeTRedundant:ColRedundant"
	assert.NotNil(validator.ValidateKV(key, value))

	// ** Converse of above
	// Not Redundant
	key = "ScopeRedundant2:ColRedundant2"
	value = "ScopeTRedundant2:ColTRedundant2"
	assert.Nil(validator.ValidateKV(key, value))

	// Combining both should be fine
	key = "ScopeRedundant2"
	value = "ScopeTRedundant2"
	assert.Nil(validator.ValidateKV(key, value))
}

func TestExplicitMatchFunc(t *testing.T) {
	fmt.Println("============== Test case start: TestExplicitMatchFunc =================")
	defer fmt.Println("============== Test case end: TestExplicitMatchFunc =================")
	assert := assert.New(t)

	rules := make(CollectionsMappingRulesType)
	rules["S1:C1"] = "S1T:C1T"
	rules["S1"] = "S1TT"
	rules["S2"] = "S2T"
	rules["S2:C1"] = nil
	rules["S3"] = nil

	srcNamespace := &CollectionNamespace{ScopeName: "S1", CollectionName: "C1"}
	tgtNamespace := &CollectionNamespace{ScopeName: "S1T", CollectionName: "C1T"}
	assert.True(rules.ExplicitMatch(srcNamespace, tgtNamespace))
	tgtNamespaceCheck, err := rules.GetPotentialTargetNamespace(srcNamespace)
	assert.Nil(err)
	assert.True(tgtNamespace.IsSameAs(*tgtNamespaceCheck))
	assert.False(rules.ExplicitlyDenied(srcNamespace))

	srcNamespace = &CollectionNamespace{ScopeName: "S1", CollectionName: "C1T"}
	tgtNamespace = &CollectionNamespace{"S1TT", "C1T"}
	assert.True(rules.ExplicitMatch(srcNamespace, tgtNamespace))
	tgtNamespaceCheck, err = rules.GetPotentialTargetNamespace(srcNamespace)
	assert.Nil(err)
	assert.True(tgtNamespace.IsSameAs(*tgtNamespaceCheck))
	assert.False(rules.ExplicitlyDenied(srcNamespace))

	srcNamespace = &CollectionNamespace{ScopeName: "S2", CollectionName: "C3"}
	tgtNamespace = &CollectionNamespace{"S2T", "C3"}
	assert.True(rules.ExplicitMatch(srcNamespace, tgtNamespace))
	tgtNamespaceCheck, err = rules.GetPotentialTargetNamespace(srcNamespace)
	assert.Nil(err)
	assert.True(tgtNamespace.IsSameAs(*tgtNamespaceCheck))
	assert.False(rules.ExplicitlyDenied(srcNamespace))

	srcNamespace = &CollectionNamespace{ScopeName: "S2", CollectionName: "C3"}
	tgtNamespace = &CollectionNamespace{"S2T", "C3T"}
	assert.False(rules.ExplicitMatch(srcNamespace, tgtNamespace))
	tgtNamespaceCheck, err = rules.GetPotentialTargetNamespace(srcNamespace)
	assert.Nil(err)
	assert.False(tgtNamespace.IsSameAs(*tgtNamespaceCheck))
	tgtNamespace.ScopeName = "S2T"
	tgtNamespace.CollectionName = "C3"
	tgtNamespaceCheck, _ = rules.GetPotentialTargetNamespace(srcNamespace)
	assert.True(tgtNamespace.IsSameAs(*tgtNamespaceCheck))
	assert.False(rules.ExplicitlyDenied(srcNamespace))

	srcNamespace = &CollectionNamespace{ScopeName: "S2", CollectionName: "C1"}
	tgtNamespace = &CollectionNamespace{"S2T", "C1"}
	assert.False(rules.ExplicitMatch(srcNamespace, tgtNamespace))
	tgtNamespaceCheck, err = rules.GetPotentialTargetNamespace(srcNamespace)
	assert.Nil(err)
	assert.Nil(tgtNamespaceCheck)
	assert.True(rules.ExplicitlyDenied(srcNamespace))

	// Invalid one just for kicks
	srcNamespace = &CollectionNamespace{ScopeName: "FOO", CollectionName: "BAR"}
	tgtNamespaceCheck, err = rules.GetPotentialTargetNamespace(srcNamespace)
	assert.NotNil(err)
	assert.Nil(tgtNamespaceCheck)
	assert.False(rules.ExplicitlyDenied(srcNamespace))

	srcNamespace = &CollectionNamespace{ScopeName: "S3", CollectionName: "C1"}
	assert.True(rules.ExplicitlyDenied(srcNamespace))
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
