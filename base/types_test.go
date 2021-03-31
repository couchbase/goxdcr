/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software
will be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"encoding/binary"
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
	// First do negative test case
	key := "_invalidScopeName"
	value := "validTargetScopeName"
	assert.NotNil(validator.ValidateKV(key, value))

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

func TestConstructCustomCRXattrForSetMeta(t *testing.T) {
	fmt.Println("============== Test case start: TestConstructCustomCRXattr =================")
	defer fmt.Println("============== Test case end: TestConstructCustomCRXattr =================")

	assert := assert.New(t)
	// _xdcr:{"cv":"0x0b0085b25e8d1416","id":"Cluster4","pc":{"Cluster1":"FhSITdr4AAA","Cluster2":"FhSITdr4ABU","Cluster3":"FhSITdr4ACA"}}
	cv, err := HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))

	sourceClusterId := []byte("SourceCluster")
	//targetClusterId := []byte("TargetCluster")

	body := make([]byte, 1000)

	// Test 1. First change, no existing _xdcr
	CCRMeta, err := NewCustomCRMeta(sourceClusterId, cv, nil, nil, nil, nil)
	assert.Nil(err)
	pos, err := CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 1)
	assert.Nil(err)
	assert.Equal("_xdcr\x00{\"id\":\"SourceCluster\",\"cv\":\"0x0b0085b25e8d1416\"}\x00", string(body[4:pos]))
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2: New change cas > cv, expected to have updated id, cv, and pcas
	// oldXattr = "_xdcr\x00{\"id\":\"Cluster4\",\"cv\":\"0x0b0085b25e8d1416\"}\x00"
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, cv+1000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, nil)
	assert.Nil(err)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 1)
	assert.Nil(err)
	newXattr := "_xdcr\x00{\"id\":\"SourceCluster\",\"cv\":\"0xf30385b25e8d1416\",\"pc\":{\"Cluster4\":\"FhSNXrKFAAs\"}}\x00"
	assert.Equal(newXattr, string(body[4:pos]))
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3: New change (cas=cv+1000) with existing XATTR (pc):
	// _xdcr:{"cv":"0x0b0085b25e8d1416","id":"Cluster4","pc":{"Cluster1":"FhSITdr4AAA","Cluster2":"FhSITdr4ABU","Cluster3":"FhSITdr4ACA"}}
	// oldXattr = "_xdcr\x00{\"id\":\"Cluster4\",\"cv\":\"0x0b0085b25e8d1416\",\"pc\":{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}}\x00"
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, cv+1000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		[]byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 0)
	assert.Nil(err)
	newXattr = "_xdcr\x00{\"id\":\"SourceCluster\",\"cv\":\"0xf30385b25e8d1416\",\"pc\":{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\",\"Cluster4\":\"FhSNXrKFAAs\"}}\x00"
	assert.Contains(string(body[4:pos]), "_xdcr\x00{\"id\":\"SourceCluster\",\"cv\":\"0xf30385b25e8d1416\",\"pc\":")
	assert.Contains(string(body[4:pos]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(body[4:pos]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(body[4:pos]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(body[4:pos]), "\"Cluster4\":\"FhSNXrKFAAs\"")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 4: New change (cas=cv+1000) with existing XATTR (mv):
	// _xdcr:{"cv":"0x0b0085b25e8d1416","id":"Cluster4","pc":{"Cluster1":"FhSITdr4AAA","Cluster2":"FhSITdr4ABU","Cluster3":"FhSITdr4ACA"}}
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, cv+1000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		nil, []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 0)
	assert.Nil(err)
	assert.Contains(string(body[0:pos]), "_xdcr\x00{\"id\":\"SourceCluster\",\"cv\":\"0xf30385b25e8d1416\",\"pc\":")
	assert.Contains(string(body[0:pos]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(body[0:pos]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(body[0:pos]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 5: New change (cas=cv+1000) with existing XATTR(pcas and mv):
	// _xdcr:{"cv":"0x0b0085b25e8d1416","id":"Cluster4","pc":{"Cluster1":"FhSITdr4AAA","Cluster2":"FhSITdr4ABU","Cluster3":"FhSITdr4ACA"}}
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, cv+1000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		[]byte("{\"Cluster1\":\"FhSITdr4AAA\"}"), []byte("{\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 0)
	assert.Nil(err)
	assert.Contains(string(body[0:pos]), "_xdcr\x00{\"id\":\"SourceCluster\",\"cv\":\"0xf30385b25e8d1416\",\"pc\":")
	assert.Contains(string(body[0:pos]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(body[0:pos]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(body[0:pos]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	setMetaPruningWithNoNewChange(t)
	setMetaPruningWithNewChange(t)
	setMetaPruningWithMvNoNewChange(t)
	setMetaPruningWithMvNewChange(t)
}

func setMetaPruningWithNoNewChange(t *testing.T) {
	assert := assert.New(t)

	sourceClusterId := []byte("SourceCluster")

	body := make([]byte, 1000)
	var t1 uint64 = 1591052006230130699
	t2 := t1 - 1000000000 // 1 second before
	t3 := t2 - 1000000000
	t4 := t3 - 1000000000
	t5 := t4 - 1000000000

	// First we have no new change (cas==cv) with four in pv
	cas := t1
	cv := cas
	cvHex := Uint64ToHexLittleEndian(cv)

	// Test 1. With 5 second pruning window, the whole pv survives
	pv := fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}",
		Uint64ToBase64(t2), Uint64ToBase64(t3), Uint64ToBase64(t4), Uint64ToBase64(t5))
	CCRMeta, err := NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), nil)
	assert.Nil(err)
	pos, err := CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 5*time.Second)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"id\":\"Cluster1\",\"cv\":\"%s\",", cvHex))
	assert.Contains(string(body[4:pos]), pv)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 3 second pruning window, the last 2 items are pruned
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 3*time.Second)
	assert.Nil(err)
	pvPruned := fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\"}", Uint64ToBase64(t2), Uint64ToBase64(t3))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"id\":\"Cluster1\",\"cv\":\"%s\",", cvHex))
	assert.Contains(string(body[4:pos]), pvPruned)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3. With 3 second pruning window, the first 2 items are pruned
	pv = fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}",
		Uint64ToBase64(t5), Uint64ToBase64(t4), Uint64ToBase64(t2), Uint64ToBase64(t3))
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), nil)
	assert.Nil(err)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 3*time.Second)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"id\":\"Cluster1\",\"cv\":\"%s\",", cvHex))
	pvPruned = fmt.Sprintf("{\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}", Uint64ToBase64(t2), Uint64ToBase64(t3))
	assert.Contains(string(body[4:pos]), pvPruned)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 4. With 3 second pruning window, 1st and 3rd items are pruned
	pv = fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}",
		Uint64ToBase64(t5), Uint64ToBase64(t3), Uint64ToBase64(t4), Uint64ToBase64(t2))
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), nil)
	assert.Nil(err)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 3*time.Second)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"id\":\"Cluster1\",\"cv\":\"%s\",", cvHex))
	pvPruned = fmt.Sprintf("{\"Cluster3\":\"%s\",\"Cluster5\":\"%s\"}", Uint64ToBase64(t3), Uint64ToBase64(t2))
	assert.Contains(string(body[4:pos]), pvPruned)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))
}

func setMetaPruningWithNewChange(t *testing.T) {
	assert := assert.New(t)

	sourceClusterId := []byte("SourceCluster")

	body := make([]byte, 1000)
	var t1 uint64 = 1591052006230130699
	t2 := t1 - 1000000000 // 1 second before
	t3 := t2 - 1000000000
	t4 := t3 - 1000000000
	t5 := t4 - 1000000000

	// First we have new change (cas>cv) with four in pv
	cas := t1 + 1000000000
	cv := t1
	cvHex := Uint64ToHexLittleEndian(cv)

	// Test 1. With 6 second pruning window, the whole pv survives, plus the id/cv also goes into PV
	pv := fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}",
		Uint64ToBase64(t2), Uint64ToBase64(t3), Uint64ToBase64(t4), Uint64ToBase64(t5))
	pvPruned := fmt.Sprintf("{\"Cluster1\":\"%s\",\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}",
		Uint64ToBase64(cv), Uint64ToBase64(t2), Uint64ToBase64(t3), Uint64ToBase64(t4), Uint64ToBase64(t5))
	CCRMeta, err := NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), nil)
	assert.Nil(err)
	pos, err := CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 6*time.Second)
	assert.Nil(err)
	newCvHex := Uint64ToHexLittleEndian(cas)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"id\":\"SourceCluster\",\"cv\":\"%s\",", newCvHex))
	assert.Contains(string(body[4:pos]), pvPruned)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 3 second pruning window, the last 3 items are pruned, plus id/cv are added to PV
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 3*time.Second)
	assert.Nil(err)
	pvPruned = fmt.Sprintf("{\"Cluster1\":\"%s\",\"Cluster2\":\"%s\"}", Uint64ToBase64(cv), Uint64ToBase64(t2))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"id\":\"SourceCluster\",\"cv\":\"%s\",", newCvHex))
	assert.Contains(string(body[4:pos]), pvPruned)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3. With 3 second pruning window, id/cv is added and only t2 in PV stays
	pv = fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}",
		Uint64ToBase64(t5), Uint64ToBase64(t4), Uint64ToBase64(t2), Uint64ToBase64(t3))
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), nil)
	assert.Nil(err)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 3*time.Second)
	assert.Nil(err)
	pvPruned = fmt.Sprintf("{\"Cluster1\":\"%s\",\"Cluster4\":\"%s\"}", Uint64ToBase64(t1), Uint64ToBase64(t2))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"id\":\"SourceCluster\",\"cv\":\"%s\",", newCvHex))
	assert.Contains(string(body[4:pos]), pvPruned)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 4. With 4 second pruning window, 1st and 3rd items are pruned, id/cv added
	pv = fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}",
		Uint64ToBase64(t5), Uint64ToBase64(t3), Uint64ToBase64(t4), Uint64ToBase64(t2))
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), nil)
	assert.Nil(err)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 4*time.Second)
	assert.Nil(err)
	pvPruned = fmt.Sprintf("{\"Cluster1\":\"%s\",\"Cluster3\":\"%s\",\"Cluster5\":\"%s\"}", Uint64ToBase64(t1), Uint64ToBase64(t3), Uint64ToBase64(t2))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"id\":\"SourceCluster\",\"cv\":\"%s\",", newCvHex))
	assert.Contains(string(body[4:pos]), pvPruned)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))
}

func setMetaPruningWithMvNoNewChange(t *testing.T) {
	assert := assert.New(t)

	sourceClusterId := []byte("SourceCluster")

	body := make([]byte, 1000)
	var t1 uint64 = 1591052006230130699
	t2 := t1 - 1000000000 // 1 second before cas
	t3 := t2 - 1000000000 // 2 seconds before cas
	t4 := t3 - 1000000000 // 3 seconds before cas
	t5 := t4 - 1000000000 // 4 seconds before cas
	t6 := t5 - 1000000000 // 5 seconds before cas
	// First we have no new change (cas==cv) with four in pv
	cas := t1
	cv := cas
	cvHex := Uint64ToHexLittleEndian(cv)

	// Test 1. With 5 second pruning window, the whole pv survives
	mv := fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\"}", Uint64ToBase64(t2), Uint64ToBase64(t3))
	pv := fmt.Sprintf("{\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}", Uint64ToBase64(t4), Uint64ToBase64(t5))
	CCRMeta, err := NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), []byte(mv))
	assert.Nil(err)
	pos, err := CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 5*time.Second)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"id\":\"Cluster1\",\"cv\":\"%s\",", cvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"pc\":%v", pv))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"mv\":%v", mv))
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 3 second pruning window and a valid MV, the PV is pruned
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 3*time.Second)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"id\":\"Cluster1\",\"cv\":\"%s\",", cvHex))
	assert.NotContains(string(body[4:pos]), "pc")
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"mv\":%v", mv))
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3. With 4 second pruning window, only Cluster5/t4 survives
	pv = fmt.Sprintf("{\"Cluster4\":\"%s\",\"Cluster5\":\"%s\",\"Cluster6\":\"%s\"}",
		Uint64ToBase64(t5), Uint64ToBase64(t4), Uint64ToBase64(t6))
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), []byte(mv))
	assert.Nil(err)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 4*time.Second)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"id\":\"Cluster1\",\"cv\":\"%s\",", cvHex))
	pvPruned := fmt.Sprintf("{\"Cluster5\":\"%s\"}", Uint64ToBase64(t4))
	assert.Contains(string(body[4:pos]), pvPruned)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"mv\":%v", mv))
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))
}

func setMetaPruningWithMvNewChange(t *testing.T) {
	assert := assert.New(t)

	sourceClusterId := []byte("SourceCluster")

	body := make([]byte, 1000)
	var t1 uint64 = 1591052006230130699
	t2 := t1 - 1000000000 // 2 second before cas
	t3 := t2 - 1000000000 // 3 seconds before cas
	t4 := t3 - 1000000000 // 4 seconds before cas
	t5 := t4 - 1000000000 // 5 seconds before cas
	t6 := t5 - 1000000000 // 6 seconds before cas
	// First we have no new change (cas==cv) with four in pv
	cas := t1 + 1000000000
	cv := t1
	cvHex := Uint64ToHexLittleEndian(cv)

	// Test 1. With 7 second pruning window, the whole pv survives
	mv := fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\"}", Uint64ToBase64(t2), Uint64ToBase64(t3))
	pv := fmt.Sprintf("{\"Cluster4\":\"%s\",\"Cluster5\":\"%s\",\"Cluster6\":\"%s\"}", Uint64ToBase64(t4), Uint64ToBase64(t5), Uint64ToBase64(t6))
	CCRMeta, err := NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), []byte(mv))
	assert.Nil(err)
	pos, err := CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 7*time.Second)
	assert.Nil(err)
	newCvHex := Uint64ToHexLittleEndian(cas)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"id\":\"SourceCluster\",\"cv\":\"%s\",", newCvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"pc\":{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\",\"Cluster6\":\"%s\"}",
		Uint64ToBase64(t2), Uint64ToBase64(t3), Uint64ToBase64(t4), Uint64ToBase64(t5), Uint64ToBase64(t6)))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 5 second pruning window, part of pv is pruned
	mv = fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\"}", Uint64ToBase64(t2), Uint64ToBase64(t3))
	pv = fmt.Sprintf("{\"Cluster4\":\"%s\",\"Cluster5\":\"%s\",\"Cluster6\":\"%s\"}", Uint64ToBase64(t4), Uint64ToBase64(t5), Uint64ToBase64(t6))
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), []byte(mv))
	assert.Nil(err)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 5*time.Second)
	assert.Nil(err)
	newCvHex = Uint64ToHexLittleEndian(cas)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"id\":\"SourceCluster\",\"cv\":\"%s\",", newCvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"pc\":{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\"}",
		Uint64ToBase64(t2), Uint64ToBase64(t3), Uint64ToBase64(t4)))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 3 second pruning window, mv is moved to pv, The old pv is all pruned
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(body, 0, 3*time.Second)
	assert.Nil(err)
	newCvHex = Uint64ToHexLittleEndian(cas)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"id\":\"SourceCluster\",\"cv\":\"%s\",", newCvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"pc\":{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\"}",
		Uint64ToBase64(t2), Uint64ToBase64(t3)))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))
}

func TestMergeMeta(t *testing.T) {
	fmt.Println("============== Test case start: TestMergeMeta =================")
	defer fmt.Println("============== Test case end: TestMergeMeta =================")

	sourceClusterId := []byte("SourceCluster")
	targetClusterId := []byte("TargetCluster")

	assert := assert.New(t)
	cv, err := HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))
	assert.Nil(err)

	/*
	 * 1. New at both source and target. Make sure we have MV but not PCAS.
	 */
	sourceMeta, err := NewCustomCRMeta(sourceClusterId, cv+20000, nil, nil, nil, nil)
	assert.Nil(err)
	targetMeta, err := NewCustomCRMeta(targetClusterId, cv+10000, nil, nil, nil, nil)
	assert.Nil(err)
	mvlen := MergedMvLength(sourceMeta, targetMeta)
	pcaslen := MergedPcasLength(sourceMeta, targetMeta)
	mergedMvSlice := make([]byte, mvlen)
	mergedPcasSlice := make([]byte, pcaslen)
	mvlen, pcaslen, err = sourceMeta.MergeMeta(targetMeta, mergedMvSlice, mergedPcasSlice, 0)
	assert.Nil(err)
	//assert.Equal(xmem.sourceClusterId, mergedMeta.Cvid)
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"SourceCluster\":\"FhSNXrKFTis\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"TargetCluster\":\"FhSNXrKFJxs\"")
	assert.Equal(0, pcaslen)

	/*
	 * 2. Source and target both updated the same old document (from Cluster4)
	 *    The two pcas should be combined with id/cv
	 * oldXattr = "_xdcr\x00{\"id\":\"Cluster4\",\"cv\":\"0x0b0085b25e8d1416\",\"pc\":{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}}\x00"
	 */
	sourceMeta, err = NewCustomCRMeta(sourceClusterId, cv+20000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		[]byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv+10000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		[]byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	mvlen = MergedMvLength(sourceMeta, targetMeta)
	pcaslen = MergedPcasLength(sourceMeta, targetMeta)
	mergedMvSlice = make([]byte, mvlen)
	mergedPcasSlice = make([]byte, pcaslen)
	mvlen, pcaslen, err = sourceMeta.MergeMeta(targetMeta, mergedMvSlice, mergedPcasSlice, 0)
	assert.Nil(err)
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"SourceCluster\":\"FhSNXrKFTis\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"TargetCluster\":\"FhSNXrKFJxs\"")
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster4\":\"FhSNXrKFAAs\"")

	/*
	 * 3. Source and target contain conflict with updates from other clusters. Both have different pcas
	 * Source cluster contains changes coming from cluster4: "_xdcr\x00{\"id\":\"Cluster4\",\"Cv\":\"0x0b0085b25e8d1416\",\"pc\":{\"Cluster1\":\"FhSITdr4AAA\"}}\x00"
	 * Target cluster contains changes coming from cluster5: "_xdcr\x00{\"id\":\"Cluster5\"Cv\":\"0x0b0085b25e8d1416\",\"pc\":{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}}\x00"
	 */
	cv, _ = HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))
	sourceMeta, err = NewCustomCRMeta(sourceClusterId, cv, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\"FhSITdr4AAA\"}"), nil)
	assert.Nil(err)
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv, []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	mvlen = MergedMvLength(sourceMeta, targetMeta)
	pcaslen = MergedPcasLength(sourceMeta, targetMeta)
	mergedMvSlice = make([]byte, mvlen)
	mergedPcasSlice = make([]byte, pcaslen)
	mvlen, pcaslen, err = sourceMeta.MergeMeta(targetMeta, mergedMvSlice, mergedPcasSlice, 0)
	assert.Nil(err)
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster4\":\"FhSNXrKFAAs\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster5\":\"FhSNXrKFAAs\"")
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster3\":\"FhSITdr4ACA\"")

	/*
	 * 4. Source and target both updated. Both have pcas, one has mv
	 */
	cv, _ = HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))
	sourceMeta, err = NewCustomCRMeta(sourceClusterId, cv+20000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\"FhSITdr4AAA\"}"), []byte("{\"Cluster3\":\"FhSITdr4ACA\",\"Cluster2\":\"FhSITdr4ABU\"}"))
	assert.Nil(err)
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv+10000, []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\"}"), nil)
	assert.Nil(err)
	mvlen = MergedMvLength(sourceMeta, targetMeta)
	pcaslen = MergedPcasLength(sourceMeta, targetMeta)
	mergedMvSlice = make([]byte, mvlen)
	mergedPcasSlice = make([]byte, pcaslen)
	mvlen, pcaslen, err = sourceMeta.MergeMeta(targetMeta, mergedMvSlice, mergedPcasSlice, 0)
	assert.Nil(err)
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"SourceCluster\":\"FhSNXrKFTis\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"TargetCluster\":\"FhSNXrKFJxs\"")
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster3\":\"FhSITdr4ACA\"")

	/*
	 * 5. Source is a merged doc. Target is an update with pcas
	 */
	cv, _ = HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))
	c1 := Uint64ToBase64(1591046436336173056)
	c2 := Uint64ToBase64(1591046436336173056 - 10000)
	sourceMeta, err = NewCustomCRMeta(sourceClusterId, cv, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\""+string(c1)+"\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv+10000, []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\""+string(c2)+"\",\"Cluster2\":\"FhSITdr4ABU\"}"), nil)
	assert.Nil(err)
	mvlen = MergedMvLength(sourceMeta, targetMeta)
	pcaslen = MergedPcasLength(sourceMeta, targetMeta)
	mergedMvSlice = make([]byte, mvlen)
	mergedPcasSlice = make([]byte, pcaslen)
	mvlen, pcaslen, err = sourceMeta.MergeMeta(targetMeta, mergedMvSlice, mergedPcasSlice, 0)
	assert.Nil(err)
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"TargetCluster\":\"FhSNXrKFJxs\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster5\":\"FhSNXrKFAAs\"")

	/*
	 * 6. Target is a merged doc. Source is an update with Pcas and Mv
	 */
	sourceMeta, err = NewCustomCRMeta(sourceClusterId, cv+10000, []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\""+string(c2)+"\",\"Cluster2\":\"FhSITdr4ABU\"}"), nil)
	assert.Nil(err)
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\""+string(c1)+"\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)
	mvlen = MergedMvLength(sourceMeta, targetMeta)
	pcaslen = MergedPcasLength(sourceMeta, targetMeta)
	mergedMvSlice = make([]byte, mvlen)
	mergedPcasSlice = make([]byte, pcaslen)
	mvlen, pcaslen, err = sourceMeta.MergeMeta(targetMeta, mergedMvSlice, mergedPcasSlice, 0)
	assert.Nil(err)
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"SourceCluster\":\"FhSNXrKFJxs\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster5\":\"FhSNXrKFAAs\"")

	/*
	 * 7. Both are merged docs.
	 */
	sourceMeta, err = NewCustomCRMeta(sourceClusterId, cv, []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\""+string(c2)+"\",\"Cluster2\":\"FhSITdr4ABU\"}"))
	assert.Nil(err)
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\""+string(c1)+"\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)
	mvlen = MergedMvLength(sourceMeta, targetMeta)
	pcaslen = MergedPcasLength(sourceMeta, targetMeta)
	mergedMvSlice = make([]byte, mvlen)
	mergedPcasSlice = make([]byte, pcaslen)
	mvlen, pcaslen, err = sourceMeta.MergeMeta(targetMeta, mergedMvSlice, mergedPcasSlice, 0)
	assert.Nil(err)
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Equal(0, pcaslen)

	/*
	 * 8. Source is a new change. Target has a history
	 */
	sourceMeta, err = NewCustomCRMeta(sourceClusterId, cv+2000, nil, nil, nil, nil)
	assert.Nil(err)
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv+1000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\""+string(c1)+"\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	mvlen = MergedMvLength(sourceMeta, targetMeta)
	pcaslen = MergedPcasLength(sourceMeta, targetMeta)
	mergedMvSlice = make([]byte, mvlen)
	mergedPcasSlice = make([]byte, pcaslen)
	mvlen, pcaslen, err = sourceMeta.MergeMeta(targetMeta, mergedMvSlice, mergedPcasSlice, 0)
	assert.Nil(err)
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(mergedPcasSlice[:pcaslen]), "\"Cluster4\":\"FhSNXrKFAAs\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"SourceCluster\":\"FhSNXrKFB9s\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"TargetCluster\":\"FhSNXrKFA/M\"")
}

func TestUpdateMetaForSetBack(t *testing.T) {
	fmt.Println("============== Test case start: TestUpdateMetaForSetBack =================")
	defer fmt.Println("============== Test case end: TestUpdateMetaForSetBack =================")

	targetClusterId := []byte("TargetCluster")
	assert := assert.New(t)
	/*
	 * 1. Target has a PCAS, no MV, and no new update. cv/cvid and cas/senderId are added to pcas
	 */
	cvHex := []byte("0x0b0085b25e8d1416")
	cv, err := HexLittleEndianToUint64(cvHex)
	assert.Nil(err)
	targetMeta, err := NewCustomCRMeta(targetClusterId, cv, []byte("Cluster4"), []byte(cvHex), []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	pcas, mv, err := targetMeta.UpdateMetaForSetBack()
	assert.Nil(err)
	assert.Nil(mv)
	assert.Contains(string(pcas), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(pcas), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(pcas), "\"Cluster4\":\"FhSNXrKFAAs\"")
	assert.Contains(string(pcas), "\"TargetCluster\":\"FhSNXrKFAAs\"")

	/*
	 * 2. Target has a PCAS, no MV, and new update. cv/cvid and cas/senderId are added to pcas
	 */
	cvHex = []byte("0x0b0085b25e8d1416")
	cv, err = HexLittleEndianToUint64(cvHex)
	assert.Nil(err)
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv+1000, []byte("Cluster4"), []byte(cvHex), []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	pcas, mv, err = targetMeta.UpdateMetaForSetBack()
	assert.Nil(err)
	assert.Nil(mv)
	assert.Contains(string(pcas), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(pcas), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(pcas), "\"Cluster4\":\"FhSNXrKFAAs\"")
	assert.Contains(string(pcas), "\"TargetCluster\":\"FhSNXrKFA/M\"")

	/*
	 * 3. Target is a merged docs with no new change.
	 */
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)

	pcas, mv, err = targetMeta.UpdateMetaForSetBack()
	assert.Nil(err)
	assert.Nil(pcas)
	assert.Contains(string(mv), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mv), "\"Cluster3\":\"FhSITdr4ACA\"")

	/*
	 * 4. Target is a merged docs with new change.
	 */
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv+1000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster2\":\"FhSITdr4ABU\"}"), []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)

	pcas, mv, err = targetMeta.UpdateMetaForSetBack()
	assert.Nil(err)
	assert.Nil(mv)
	assert.Contains(string(pcas), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(pcas), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(pcas), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(pcas), "\"Cluster4\":\"FhSNXrKFAAs\"")
	assert.Contains(string(pcas), "\"TargetCluster\":\"FhSNXrKFA/M\"")
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
	assert.Equal(explicitRuleInvalid, validator.parseRule("testScope.testCol", "_nonDefault.nonDefCol"))

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
