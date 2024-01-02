/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package ccrMetadata

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/base"
	"github.com/stretchr/testify/assert"
)

func TestConstructCustomCRXattrForSetMeta(t *testing.T) {
	fmt.Println("============== Test case start: TestConstructCustomCRXattr =================")
	defer fmt.Println("============== Test case end: TestConstructCustomCRXattr =================")

	assert := assert.New(t)
	// _vv:{"ver":"0x0b0085b25e8d1416","src":"Cluster4","pv":{"Cluster1":"FhSITdr4AAA","Cluster2":"FhSITdr4ABU","Cluster3":"FhSITdr4ACA"}}
	ver, err := base.HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))

	sourceClusterId := []byte("SourceCluster")
	//targetClusterId := []byte("TargetCluster")

	body := make([]byte, 1000)

	// Test 1. First change, no existing _vv
	CCRMeta, err := NewCustomCRMeta(sourceClusterId, ver, nil, nil, nil, nil)
	assert.Nil(err)
	composer := base.NewXattrRawComposer(body)
	pos, err := CCRMeta.ConstructCustomCRXattrForSetMeta(1, composer)
	assert.Nil(err)
	assert.Equal("_vv\x00{\"src\":\"SourceCluster\",\"ver\":\"0x0b0085b25e8d1416\"}\x00", string(body[4:pos]))
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2: New change cas > ver, expected to have updated srv, ver, and pv
	// oldXattr = "_vv\x00{\"src\":\"Cluster4\",\"ver\":\"0x0b0085b25e8d1416\"}\x00"
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, ver+1000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(1, composer)
	assert.Nil(err)
	newXattr := "_vv\x00{\"src\":\"SourceCluster\",\"ver\":\"0xf30385b25e8d1416\",\"pv\":{\"Cluster4\":\"FhSNXrKFAAs\"}}\x00"
	assert.Equal(newXattr, string(body[4:pos]))
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3: New change (cas=ver+1000) with existing XATTR (pv):
	// _vv:{"ver":"0x0b0085b25e8d1416","src":"Cluster4","pv":{"Cluster1":"FhSITdr4AAA","Cluster2":"FhSITdr4ABU","Cluster3":"FhSITdr4ACA"}}
	// oldXattr = "_vv\x00{\"src\":\"Cluster4\",\"ver\":\"0x0b0085b25e8d1416\",\"pv\":{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}}\x00"
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, ver+1000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		[]byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(0, composer)
	assert.Nil(err)
	newXattr = "_vv\x00{\"src\":\"SourceCluster\",\"ver\":\"0xf30385b25e8d1416\",\"pv\":{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\",\"Cluster4\":\"FhSNXrKFAAs\"}}\x00"
	assert.Contains(string(body[4:pos]), "_vv\x00{\"src\":\"SourceCluster\",\"ver\":\"0xf30385b25e8d1416\",\"pv\":")
	assert.Contains(string(body[4:pos]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(body[4:pos]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(body[4:pos]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(body[4:pos]), "\"Cluster4\":\"FhSNXrKFAAs\"")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 4: New change (cas=ver+1000) with existing XATTR (mv):
	// _vv:{"ver":"0x0b0085b25e8d1416","src":"Cluster4","pv":{"Cluster1":"FhSITdr4AAA","Cluster2":"FhSITdr4ABU","Cluster3":"FhSITdr4ACA"}}
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, ver+1000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		nil, []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(0, composer)
	assert.Nil(err)
	assert.Contains(string(body[0:pos]), "_vv\x00{\"src\":\"SourceCluster\",\"ver\":\"0xf30385b25e8d1416\",\"pv\":")
	assert.Contains(string(body[0:pos]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(body[0:pos]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(body[0:pos]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 5: New change (cas=ver+1000) with existing XATTR(pv and mv):
	// _vv:{"ver":"0x0b0085b25e8d1416","src":"Cluster4","pv":{"Cluster1":"FhSITdr4AAA","Cluster2":"FhSITdr4ABU","Cluster3":"FhSITdr4ACA"}}
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, ver+1000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		[]byte("{\"Cluster1\":\"FhSITdr4AAA\"}"), []byte("{\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(0, composer)
	assert.Nil(err)
	assert.Contains(string(body[0:pos]), "_vv\x00{\"src\":\"SourceCluster\",\"ver\":\"0xf30385b25e8d1416\",\"pv\":")
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

	// First we have no new change (cas==ver) with four in pv
	cas := t1
	cv := cas
	cvHex := base.Uint64ToHexLittleEndian(cv)

	// Test 1. With 5 second pruning window, the whole pv survives
	pv := fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}",
		base.Uint64ToBase64(t2), base.Uint64ToBase64(t3), base.Uint64ToBase64(t4), base.Uint64ToBase64(t5))
	CCRMeta, err := NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), nil)
	assert.Nil(err)
	composer := base.NewXattrRawComposer(body)
	pos, err := CCRMeta.ConstructCustomCRXattrForSetMeta(5*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex))
	assert.Contains(string(body[4:pos]), pv)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 3 second pruning window, the last 2 items are pruned
	composer = base.NewXattrRawComposer(body)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(3*time.Second, composer)
	assert.Nil(err)
	pvPruned := fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\"}", base.Uint64ToBase64(t2), base.Uint64ToBase64(t3))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex))
	assert.Contains(string(body[4:pos]), pvPruned)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3. With 3 second pruning window, the first 2 items are pruned
	pv = fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}",
		base.Uint64ToBase64(t5), base.Uint64ToBase64(t4), base.Uint64ToBase64(t2), base.Uint64ToBase64(t3))
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(3*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex))
	pvPruned = fmt.Sprintf("{\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}", base.Uint64ToBase64(t2), base.Uint64ToBase64(t3))
	assert.Contains(string(body[4:pos]), pvPruned)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 4. With 3 second pruning window, 1st and 3rd items are pruned
	pv = fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}",
		base.Uint64ToBase64(t5), base.Uint64ToBase64(t3), base.Uint64ToBase64(t4), base.Uint64ToBase64(t2))
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(3*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex))
	pvPruned = fmt.Sprintf("{\"Cluster3\":\"%s\",\"Cluster5\":\"%s\"}", base.Uint64ToBase64(t3), base.Uint64ToBase64(t2))
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

	// First we have new change (cas>ver) with four in pv
	cas := t1 + 1000000000
	cv := t1
	cvHex := base.Uint64ToHexLittleEndian(cv)

	// Test 1. With 6 second pruning window, the whole pv survives, plus the id/ver also goes into PV
	pv := fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}",
		base.Uint64ToBase64(t2), base.Uint64ToBase64(t3), base.Uint64ToBase64(t4), base.Uint64ToBase64(t5))
	pvPruned := fmt.Sprintf("{\"Cluster1\":\"%s\",\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}",
		base.Uint64ToBase64(cv), base.Uint64ToBase64(t2), base.Uint64ToBase64(t3), base.Uint64ToBase64(t4), base.Uint64ToBase64(t5))
	CCRMeta, err := NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), nil)
	assert.Nil(err)
	composer := base.NewXattrRawComposer(body)
	pos, err := CCRMeta.ConstructCustomCRXattrForSetMeta(6*time.Second, composer)
	assert.Nil(err)
	newCvHex := base.Uint64ToHexLittleEndian(cas)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex))
	assert.Contains(string(body[4:pos]), pvPruned)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 3 second pruning window, the last 3 items are pruned, plus id/ver are added to PV
	composer = base.NewXattrRawComposer(body)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(3*time.Second, composer)
	assert.Nil(err)
	pvPruned = fmt.Sprintf("{\"Cluster1\":\"%s\",\"Cluster2\":\"%s\"}", base.Uint64ToBase64(cv), base.Uint64ToBase64(t2))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex))
	assert.Contains(string(body[4:pos]), pvPruned)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3. With 3 second pruning window, id/ver is added and only t2 in PV stays
	pv = fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}",
		base.Uint64ToBase64(t5), base.Uint64ToBase64(t4), base.Uint64ToBase64(t2), base.Uint64ToBase64(t3))
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(3*time.Second, composer)
	assert.Nil(err)
	pvPruned = fmt.Sprintf("{\"Cluster1\":\"%s\",\"Cluster4\":\"%s\"}", base.Uint64ToBase64(t1), base.Uint64ToBase64(t2))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex))
	assert.Contains(string(body[4:pos]), pvPruned)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 4. With 4 second pruning window, 1st and 3rd items are pruned, id/ver added
	pv = fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}",
		base.Uint64ToBase64(t5), base.Uint64ToBase64(t3), base.Uint64ToBase64(t4), base.Uint64ToBase64(t2))
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(4*time.Second, composer)
	assert.Nil(err)
	pvPruned = fmt.Sprintf("{\"Cluster1\":\"%s\",\"Cluster3\":\"%s\",\"Cluster5\":\"%s\"}", base.Uint64ToBase64(t1), base.Uint64ToBase64(t3), base.Uint64ToBase64(t2))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex))
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
	// First we have no new change (cas==ver) with four in pv
	cas := t1
	cv := cas
	cvHex := base.Uint64ToHexLittleEndian(cv)

	// Test 1. With 5 second pruning window, the whole pv survives
	mv := fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\"}", base.Uint64ToBase64(t2), base.Uint64ToBase64(t3))
	pv := fmt.Sprintf("{\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}", base.Uint64ToBase64(t4), base.Uint64ToBase64(t5))
	CCRMeta, err := NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), []byte(mv))
	assert.Nil(err)
	composer := base.NewXattrRawComposer(body)
	pos, err := CCRMeta.ConstructCustomCRXattrForSetMeta(5*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"pv\":%v", pv))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"mv\":%v", mv))
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 3 second pruning window and a valid MV, the PV is pruned
	composer = base.NewXattrRawComposer(body)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(3*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex))
	assert.NotContains(string(body[4:pos]), "pv")
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"mv\":%v", mv))
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3. With 4 second pruning window, only Cluster5/t4 survives
	pv = fmt.Sprintf("{\"Cluster4\":\"%s\",\"Cluster5\":\"%s\",\"Cluster6\":\"%s\"}",
		base.Uint64ToBase64(t5), base.Uint64ToBase64(t4), base.Uint64ToBase64(t6))
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), []byte(mv))
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(4*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex))
	pvPruned := fmt.Sprintf("{\"Cluster5\":\"%s\"}", base.Uint64ToBase64(t4))
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
	// First we have no new change (cas==ver) with four in pv
	cas := t1 + 1000000000
	cv := t1
	cvHex := base.Uint64ToHexLittleEndian(cv)

	// Test 1. With 7 second pruning window, the whole pv survives
	mv := fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\"}", base.Uint64ToBase64(t2), base.Uint64ToBase64(t3))
	pv := fmt.Sprintf("{\"Cluster4\":\"%s\",\"Cluster5\":\"%s\",\"Cluster6\":\"%s\"}", base.Uint64ToBase64(t4), base.Uint64ToBase64(t5), base.Uint64ToBase64(t6))
	CCRMeta, err := NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), []byte(mv))
	assert.Nil(err)
	composer := base.NewXattrRawComposer(body)
	pos, err := CCRMeta.ConstructCustomCRXattrForSetMeta(7*time.Second, composer)
	assert.Nil(err)
	newCvHex := base.Uint64ToHexLittleEndian(cas)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"pv\":{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\",\"Cluster6\":\"%s\"}",
		base.Uint64ToBase64(t2), base.Uint64ToBase64(t3), base.Uint64ToBase64(t4), base.Uint64ToBase64(t5), base.Uint64ToBase64(t6)))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 5 second pruning window, part of pv is pruned
	mv = fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\"}", base.Uint64ToBase64(t2), base.Uint64ToBase64(t3))
	pv = fmt.Sprintf("{\"Cluster4\":\"%s\",\"Cluster5\":\"%s\",\"Cluster6\":\"%s\"}", base.Uint64ToBase64(t4), base.Uint64ToBase64(t5), base.Uint64ToBase64(t6))
	CCRMeta, err = NewCustomCRMeta(sourceClusterId, cas, []byte("Cluster1"), []byte(cvHex), []byte(pv), []byte(mv))
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(5*time.Second, composer)
	assert.Nil(err)
	newCvHex = base.Uint64ToHexLittleEndian(cas)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"pv\":{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\"}",
		base.Uint64ToBase64(t2), base.Uint64ToBase64(t3), base.Uint64ToBase64(t4)))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 3 second pruning window, mv is moved to pv, The old pv is all pruned
	composer = base.NewXattrRawComposer(body)
	pos, err = CCRMeta.ConstructCustomCRXattrForSetMeta(3*time.Second, composer)
	assert.Nil(err)
	newCvHex = base.Uint64ToHexLittleEndian(cas)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"pv\":{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\"}",
		base.Uint64ToBase64(t2), base.Uint64ToBase64(t3)))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))
}

func TestMergeMeta(t *testing.T) {
	fmt.Println("============== Test case start: TestMergeMeta =================")
	defer fmt.Println("============== Test case end: TestMergeMeta =================")

	sourceClusterId := []byte("SourceCluster")
	targetClusterId := []byte("TargetCluster")

	assert := assert.New(t)
	cv, err := base.HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))
	assert.Nil(err)

	/*
	 * 1. New at both source and target. Make sure we have MV but not PV.
	 */
	sourceMeta, err := NewCustomCRMeta(sourceClusterId, cv+20000, nil, nil, nil, nil)
	assert.Nil(err)
	targetMeta, err := NewCustomCRMeta(targetClusterId, cv+10000, nil, nil, nil, nil)
	assert.Nil(err)
	mvlen := MergedMvLength(sourceMeta, targetMeta)
	pvlen := MergedPvLength(sourceMeta, targetMeta)
	mergedMvSlice := make([]byte, mvlen)
	mergedPvSlice := make([]byte, pvlen)
	mvlen, pvlen, err = sourceMeta.MergeCustomCRMetadata(targetMeta, mergedMvSlice, mergedPvSlice, 0)
	assert.Nil(err)
	//assert.Equal(xmem.sourceClusterId, mergedMeta.Cvid)
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"SourceCluster\":\"FhSNXrKFTis\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"TargetCluster\":\"FhSNXrKFJxs\"")
	assert.Equal(0, pvlen)

	/*
	 * 2. Source and target both updated the same old document (from Cluster4)
	 *    The two pv should be combined with id/ver
	 * oldXattr = "_vv\x00{\"src\":\"Cluster4\",\"ver\":\"0x0b0085b25e8d1416\",\"pv\":{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}}\x00"
	 */
	sourceMeta, err = NewCustomCRMeta(sourceClusterId, cv+20000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		[]byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv+10000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		[]byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	mvlen = MergedMvLength(sourceMeta, targetMeta)
	pvlen = MergedPvLength(sourceMeta, targetMeta)
	mergedMvSlice = make([]byte, mvlen)
	mergedPvSlice = make([]byte, pvlen)
	mvlen, pvlen, err = sourceMeta.MergeCustomCRMetadata(targetMeta, mergedMvSlice, mergedPvSlice, 0)
	assert.Nil(err)
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"SourceCluster\":\"FhSNXrKFTis\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"TargetCluster\":\"FhSNXrKFJxs\"")
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster4\":\"FhSNXrKFAAs\"")

	/*
	 * 3. Source and target contain conflict with updates from other clusters. Both have different pv
	 * Source cluster contains changes coming from cluster4: "_vv\x00{\"src\":\"Cluster4\",\"ver\":\"0x0b0085b25e8d1416\",\"pv\":{\"Cluster1\":\"FhSITdr4AAA\"}}\x00"
	 * Target cluster contains changes coming from cluster5: "_vv\x00{\"src\":\"Cluster5\"ver\":\"0x0b0085b25e8d1416\",\"pv\":{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}}\x00"
	 */
	cv, _ = base.HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))
	sourceMeta, err = NewCustomCRMeta(sourceClusterId, cv, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\"FhSITdr4AAA\"}"), nil)
	assert.Nil(err)
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv, []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	mvlen = MergedMvLength(sourceMeta, targetMeta)
	pvlen = MergedPvLength(sourceMeta, targetMeta)
	mergedMvSlice = make([]byte, mvlen)
	mergedPvSlice = make([]byte, pvlen)
	mvlen, pvlen, err = sourceMeta.MergeCustomCRMetadata(targetMeta, mergedMvSlice, mergedPvSlice, 0)
	assert.Nil(err)
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster4\":\"FhSNXrKFAAs\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster5\":\"FhSNXrKFAAs\"")
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")

	/*
	 * 4. Source and target both updated. Both have pv, one has mv
	 */
	cv, _ = base.HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))
	sourceMeta, err = NewCustomCRMeta(sourceClusterId, cv+20000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\"FhSITdr4AAA\"}"), []byte("{\"Cluster3\":\"FhSITdr4ACA\",\"Cluster2\":\"FhSITdr4ABU\"}"))
	assert.Nil(err)
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv+10000, []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\"}"), nil)
	assert.Nil(err)
	mvlen = MergedMvLength(sourceMeta, targetMeta)
	pvlen = MergedPvLength(sourceMeta, targetMeta)
	mergedMvSlice = make([]byte, mvlen)
	mergedPvSlice = make([]byte, pvlen)
	mvlen, pvlen, err = sourceMeta.MergeCustomCRMetadata(targetMeta, mergedMvSlice, mergedPvSlice, 0)
	assert.Nil(err)
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"SourceCluster\":\"FhSNXrKFTis\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"TargetCluster\":\"FhSNXrKFJxs\"")
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")

	/*
	 * 5. Source is a merged doc. Target is an update with pv
	 */
	cv, _ = base.HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))
	c1 := base.Uint64ToBase64(1591046436336173056)
	c2 := base.Uint64ToBase64(1591046436336173056 - 10000)
	sourceMeta, err = NewCustomCRMeta(sourceClusterId, cv, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\""+string(c1)+"\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv+10000, []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\""+string(c2)+"\",\"Cluster2\":\"FhSITdr4ABU\"}"), nil)
	assert.Nil(err)
	mvlen = MergedMvLength(sourceMeta, targetMeta)
	pvlen = MergedPvLength(sourceMeta, targetMeta)
	mergedMvSlice = make([]byte, mvlen)
	mergedPvSlice = make([]byte, pvlen)
	mvlen, pvlen, err = sourceMeta.MergeCustomCRMetadata(targetMeta, mergedMvSlice, mergedPvSlice, 0)
	assert.Nil(err)
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"TargetCluster\":\"FhSNXrKFJxs\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster5\":\"FhSNXrKFAAs\"")

	/*
	 * 6. Target is a merged doc. Source is an update with PV and MV
	 */
	sourceMeta, err = NewCustomCRMeta(sourceClusterId, cv+10000, []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\""+string(c2)+"\",\"Cluster2\":\"FhSITdr4ABU\"}"), nil)
	assert.Nil(err)
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\""+string(c1)+"\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)
	mvlen = MergedMvLength(sourceMeta, targetMeta)
	pvlen = MergedPvLength(sourceMeta, targetMeta)
	mergedMvSlice = make([]byte, mvlen)
	mergedPvSlice = make([]byte, pvlen)
	mvlen, pvlen, err = sourceMeta.MergeCustomCRMetadata(targetMeta, mergedMvSlice, mergedPvSlice, 0)
	assert.Nil(err)
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"SourceCluster\":\"FhSNXrKFJxs\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster5\":\"FhSNXrKFAAs\"")

	/*
	 * 7. Both are merged docs.
	 */
	sourceMeta, err = NewCustomCRMeta(sourceClusterId, cv, []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\""+string(c2)+"\",\"Cluster2\":\"FhSITdr4ABU\"}"))
	assert.Nil(err)
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\""+string(c1)+"\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)
	mvlen = MergedMvLength(sourceMeta, targetMeta)
	pvlen = MergedPvLength(sourceMeta, targetMeta)
	mergedMvSlice = make([]byte, mvlen)
	mergedPvSlice = make([]byte, pvlen)
	mvlen, pvlen, err = sourceMeta.MergeCustomCRMetadata(targetMeta, mergedMvSlice, mergedPvSlice, 0)
	assert.Nil(err)
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Equal(0, pvlen)

	/*
	 * 8. Source is a new change. Target has a history
	 */
	sourceMeta, err = NewCustomCRMeta(sourceClusterId, cv+2000, nil, nil, nil, nil)
	assert.Nil(err)
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv+1000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\""+string(c1)+"\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	mvlen = MergedMvLength(sourceMeta, targetMeta)
	pvlen = MergedPvLength(sourceMeta, targetMeta)
	mergedMvSlice = make([]byte, mvlen)
	mergedPvSlice = make([]byte, pvlen)
	mvlen, pvlen, err = sourceMeta.MergeCustomCRMetadata(targetMeta, mergedMvSlice, mergedPvSlice, 0)
	assert.Nil(err)
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(mergedPvSlice[:pvlen]), "\"Cluster4\":\"FhSNXrKFAAs\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"SourceCluster\":\"FhSNXrKFB9s\"")
	assert.Contains(string(mergedMvSlice[:mvlen]), "\"TargetCluster\":\"FhSNXrKFA/M\"")
}

func TestUpdateMetaForSetBack(t *testing.T) {
	fmt.Println("============== Test case start: TestUpdateMetaForSetBack =================")
	defer fmt.Println("============== Test case end: TestUpdateMetaForSetBack =================")

	targetClusterId := []byte("TargetCluster")
	assert := assert.New(t)
	/*
	 * 1. Target has a PV, no MV, and no new update. ver/cvid and cas/source are added to pv
	 */
	cvHex := []byte("0x0b0085b25e8d1416")
	cv, err := base.HexLittleEndianToUint64(cvHex)
	assert.Nil(err)
	targetMeta, err := NewCustomCRMeta(targetClusterId, cv, []byte("Cluster4"), []byte(cvHex), []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	pv, mv, err := targetMeta.UpdateMetaForSetBack()
	assert.Nil(err)
	assert.Nil(mv)
	assert.Contains(string(pv), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(pv), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(pv), "\"Cluster4\":\"FhSNXrKFAAs\"")
	assert.Contains(string(pv), "\"TargetCluster\":\"FhSNXrKFAAs\"")

	/*
	 * 2. Target has a PV, no MV, and new update. ver/src and cas/source are added to pv
	 */
	cvHex = []byte("0x0b0085b25e8d1416")
	cv, err = base.HexLittleEndianToUint64(cvHex)
	assert.Nil(err)
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv+1000, []byte("Cluster4"), []byte(cvHex), []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	pv, mv, err = targetMeta.UpdateMetaForSetBack()
	assert.Nil(err)
	assert.Nil(mv)
	assert.Contains(string(pv), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(pv), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(pv), "\"Cluster4\":\"FhSNXrKFAAs\"")
	assert.Contains(string(pv), "\"TargetCluster\":\"FhSNXrKFA/M\"")

	/*
	 * 3. Target is a merged docs with no new change.
	 */
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)

	pv, mv, err = targetMeta.UpdateMetaForSetBack()
	assert.Nil(err)
	assert.Nil(pv)
	assert.Contains(string(mv), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mv), "\"Cluster3\":\"FhSITdr4ACA\"")

	/*
	 * 4. Target is a merged docs with new change.
	 */
	targetMeta, err = NewCustomCRMeta(targetClusterId, cv+1000, []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster2\":\"FhSITdr4ABU\"}"), []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)

	pv, mv, err = targetMeta.UpdateMetaForSetBack()
	assert.Nil(mv)
	assert.Contains(string(pv), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(pv), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(pv), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(pv), "\"TargetCluster\":\"FhSNXrKFA/M\"")
}
