/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package crMeta

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/hlv"
	"github.com/stretchr/testify/assert"
)

func NewMetadataForTest(key, source []byte, cas, revId uint64, cvCasHex, cvSrc, verHex, pv, mv []byte) (*CRMetadata, error) {
	var cvCas, ver uint64
	var err error
	if len(verHex) == 0 {
		ver = 0
	} else {
		ver, err = base.HexLittleEndianToUint64(verHex)
		if err != nil {
			return nil, err
		}
	}
	if len(cvCasHex) == 0 {
		cvCas = 0
	} else {
		cvCas, err = base.HexLittleEndianToUint64(verHex)
		if err != nil {
			return nil, err
		}
	}
	pvMap, err := xattrVVtoMap(pv)
	if err != nil {
		return nil, err
	}
	mvMap, err := xattrVVtoMap(mv)
	if err != nil {
		return nil, err
	}
	hlv, err := hlv.NewHLV(hlv.DocumentSourceId(source), cas, cvCas, hlv.DocumentSourceId(cvSrc), ver, pvMap, mvMap)
	if err != nil {
		return nil, err
	}
	meta := CRMetadata{
		docMeta: &base.DocumentMetadata{
			Key:      key,
			RevSeq:   revId,
			Cas:      cas,
			Flags:    0,
			Expiry:   0,
			Deletion: false,
			DataType: 0,
		},
		hlv: hlv,
	}
	return &meta, nil
}

func TestConstructCustomCRXattrForSetMeta(t *testing.T) {
	fmt.Println("============== Test case start: TestConstructCustomCRXattr =================")
	defer fmt.Println("============== Test case end: TestConstructCustomCRXattr =================")

	assert := assert.New(t)
	// _vv:{"ver":"0x0b0085b25e8d1416","src":"Cluster4","pv":{"Cluster1":"FhSITdr4AAA","Cluster2":"FhSITdr4ABU","Cluster3":"FhSITdr4ACA"}}
	ver, err := base.HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))

	docKey := []byte("docKey")
	sourceClusterId := []byte("SourceCluster")
	//targetClusterId := []byte("TargetCluster")

	body := make([]byte, 1000)

	// Test 1. First change, no existing _vv
	meta, err := NewMetadataForTest(docKey, sourceClusterId, ver, 1, nil, nil, nil, nil, nil)
	assert.Nil(err)
	composer := base.NewXattrRawComposer(body)
	pos, err := ConstructCustomCRXattrForSetMeta(meta, 0, composer)
	assert.Nil(err)
	assert.Equal("_vv\x00{\"cvCas\":\"0x0b0085b25e8d1416\",\"src\":\"SourceCluster\",\"ver\":\"0x0b0085b25e8d1416\"}\x00", string(body[4:pos]))
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2: New change cas > ver, expected to have updated srv, ver, and pv
	// oldXattr = "_vv\x00{\"src\":\"Cluster4\",\"ver\":\"0x0b0085b25e8d1416\"}\x00"
	meta, err = NewMetadataForTest(docKey, sourceClusterId, ver+1000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = ConstructCustomCRXattrForSetMeta(meta, 0, composer)
	assert.Nil(err)
	newXattr := "_vv\x00{\"cvCas\":\"0xf30385b25e8d1416\",\"src\":\"SourceCluster\",\"ver\":\"0xf30385b25e8d1416\",\"pv\":{\"Cluster4\":\"FhSNXrKFAAs\"}}\x00"
	assert.Equal(newXattr, string(body[4:pos]))
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3: New change (cas=ver+1000) with existing XATTR (pv):
	// _vv:{\"cvCas\":\"0xf30385b25e8d1416\","ver":"0x0b0085b25e8d1416","src":"Cluster4","pv":{"Cluster1":"FhSITdr4AAA","Cluster2":"FhSITdr4ABU","Cluster3":"FhSITdr4ACA"}}
	// oldXattr = "_vv\x00{\"cvCas\":\"0x0b0085b25e8d1416\",\"src\":\"Cluster4\",\"ver\":\"0x0b0085b25e8d1416\",\"pv\":{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}}\x00"
	meta, err = NewMetadataForTest(docKey, sourceClusterId, ver+1000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		[]byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = ConstructCustomCRXattrForSetMeta(meta, 0, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), "_vv\x00{\"cvCas\":\"0xf30385b25e8d1416\",\"src\":\"SourceCluster\",\"ver\":\"0xf30385b25e8d1416\",\"pv\":")
	assert.Contains(string(body[4:pos]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(body[4:pos]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(body[4:pos]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(body[4:pos]), "\"Cluster4\":\"FhSNXrKFAAs\"")
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 4: New change (cas=ver+1000) with existing XATTR (mv):
	meta, err = NewMetadataForTest(docKey, sourceClusterId, ver+1000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		nil, []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = ConstructCustomCRXattrForSetMeta(meta, 0, composer)
	assert.Nil(err)
	assert.Contains(string(body[0:pos]), "_vv\x00{\"cvCas\":\"0xf30385b25e8d1416\",\"src\":\"SourceCluster\",\"ver\":\"0xf30385b25e8d1416\",\"pv\":")
	assert.Contains(string(body[0:pos]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(body[0:pos]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(body[0:pos]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.NotContains(string(body[0:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 5: New change (cas=ver+1000) with existing XATTR(pv and mv):
	meta, err = NewMetadataForTest(docKey, sourceClusterId, ver+1000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		[]byte("{\"Cluster1\":\"FhSITdr4AAA\"}"), []byte("{\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = ConstructCustomCRXattrForSetMeta(meta, 0, composer)
	assert.Nil(err)
	assert.Contains(string(body[0:pos]), "_vv\x00{\"cvCas\":\"0xf30385b25e8d1416\",\"src\":\"SourceCluster\",\"ver\":\"0xf30385b25e8d1416\",\"pv\":")
	assert.Contains(string(body[0:pos]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(body[0:pos]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(body[0:pos]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(body[0:pos]), "pv")
	assert.NotContains(string(body[0:pos]), "mv")
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
	key := []byte("docKey")

	// Test 1. With 5 second pruning window, the whole pv survives
	p2 := fmt.Sprintf("\"Cluster2\":\"%s\"", base.Uint64ToBase64(t2))
	p3 := fmt.Sprintf("\"Cluster3\":\"%s\"", base.Uint64ToBase64(t3))
	p4 := fmt.Sprintf("\"Cluster4\":\"%s\"", base.Uint64ToBase64(t4))
	p5 := fmt.Sprintf("\"Cluster5\":\"%s\"", base.Uint64ToBase64(t5))
	pv := fmt.Sprintf("{%s,%s,%s,%s}", p2, p3, p4, p5)
	meta, err := NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), nil)
	assert.Nil(err)
	composer := base.NewXattrRawComposer(body)
	pos, err := ConstructCustomCRXattrForSetMeta(meta, 5*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex, cvHex))
	assert.Contains(string(body[4:pos]), p2)
	assert.Contains(string(body[4:pos]), p3)
	assert.Contains(string(body[4:pos]), p4)
	assert.Contains(string(body[4:pos]), p5)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 3 second pruning window, the last 2 items are pruned
	composer = base.NewXattrRawComposer(body)
	pos, err = ConstructCustomCRXattrForSetMeta(meta, 3*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex, cvHex))
	assert.Contains(string(body[4:pos]), p2)
	assert.Contains(string(body[4:pos]), p3)
	assert.NotContains(string(body[4:pos]), p4)
	assert.NotContains(string(body[4:pos]), p5)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3. With 3 second pruning window, the first 2 items are pruned
	p2 = fmt.Sprintf("\"Cluster2\":\"%s\"", base.Uint64ToBase64(t5))
	p3 = fmt.Sprintf("\"Cluster3\":\"%s\"", base.Uint64ToBase64(t4))
	p4 = fmt.Sprintf("\"Cluster4\":\"%s\"", base.Uint64ToBase64(t2))
	p5 = fmt.Sprintf("\"Cluster5\":\"%s\"", base.Uint64ToBase64(t3))
	pv = fmt.Sprintf("{%s,%s,%s,%s}", p2, p3, p4, p5)
	meta, err = NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = ConstructCustomCRXattrForSetMeta(meta, 3*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex, cvHex))
	assert.Contains(string(body[4:pos]), p4)
	assert.Contains(string(body[4:pos]), p5)
	assert.NotContains(string(body[4:pos]), p2)
	assert.NotContains(string(body[4:pos]), p3)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 4. With 3 second pruning window, 1st and 3rd items are pruned
	p2 = fmt.Sprintf("\"Cluster2\":\"%s\"", base.Uint64ToBase64(t5))
	p3 = fmt.Sprintf("\"Cluster3\":\"%s\"", base.Uint64ToBase64(t3))
	p4 = fmt.Sprintf("\"Cluster4\":\"%s\"", base.Uint64ToBase64(t4))
	p5 = fmt.Sprintf("\"Cluster5\":\"%s\"", base.Uint64ToBase64(t2))
	pv = fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}",
		base.Uint64ToBase64(t5), base.Uint64ToBase64(t3), base.Uint64ToBase64(t4), base.Uint64ToBase64(t2))
	meta, err = NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = ConstructCustomCRXattrForSetMeta(meta, 3*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex, cvHex))
	assert.Contains(string(body[4:pos]), p3)
	assert.Contains(string(body[4:pos]), p5)
	assert.NotContains(string(body[4:pos]), p2)
	assert.NotContains(string(body[4:pos]), p4)
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
	key := []byte("docKey")

	// Test 1. With 6 second pruning window, the whole pv survives, plus the id/ver also goes into PV
	p1 := fmt.Sprintf("\"Cluster1\":\"%s\"", base.Uint64ToBase64(cv))
	p2 := fmt.Sprintf("\"Cluster2\":\"%s\"", base.Uint64ToBase64(t2))
	p3 := fmt.Sprintf("\"Cluster3\":\"%s\"", base.Uint64ToBase64(t3))
	p4 := fmt.Sprintf("\"Cluster4\":\"%s\"", base.Uint64ToBase64(t4))
	p5 := fmt.Sprintf("\"Cluster5\":\"%s\"", base.Uint64ToBase64(t5))
	pv := fmt.Sprintf("{%s,%s,%s,%s}", p2, p3, p4, p5)

	meta, err := NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), nil)
	assert.Nil(err)
	composer := base.NewXattrRawComposer(body)
	pos, err := ConstructCustomCRXattrForSetMeta(meta, 6*time.Second, composer)
	assert.Nil(err)
	newCvHex := base.Uint64ToHexLittleEndian(cas)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex, newCvHex))
	assert.Contains(string(body[4:pos]), p1)
	assert.Contains(string(body[4:pos]), p2)
	assert.Contains(string(body[4:pos]), p3)
	assert.Contains(string(body[4:pos]), p4)
	assert.Contains(string(body[4:pos]), p5)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 3 second pruning window, the last 3 items are pruned, plus id/ver are added to PV
	composer = base.NewXattrRawComposer(body)
	pos, err = ConstructCustomCRXattrForSetMeta(meta, 3*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex, newCvHex))
	assert.Contains(string(body[4:pos]), p1)
	assert.Contains(string(body[4:pos]), p2)
	assert.NotContains(string(body[4:pos]), p3)
	assert.NotContains(string(body[4:pos]), p4)
	assert.NotContains(string(body[4:pos]), p5)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3. With 3 second pruning window, id/ver is added and only t2 in PV stays
	p1 = fmt.Sprintf("\"Cluster1\":\"%s\"", base.Uint64ToBase64(t1))
	p2 = fmt.Sprintf("\"Cluster2\":\"%s\"", base.Uint64ToBase64(t5))
	p3 = fmt.Sprintf("\"Cluster3\":\"%s\"", base.Uint64ToBase64(t4))
	p4 = fmt.Sprintf("\"Cluster4\":\"%s\"", base.Uint64ToBase64(t2))
	pv = fmt.Sprintf("{%s,%s,%s,%s}", p2, p3, p4, p5)
	meta, err = NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = ConstructCustomCRXattrForSetMeta(meta, 3*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex, newCvHex))
	assert.Contains(string(body[4:pos]), p1)
	assert.Contains(string(body[4:pos]), p4)
	assert.NotContains(string(body[4:pos]), p2)
	assert.NotContains(string(body[4:pos]), p3)
	assert.NotContains(string(body[4:pos]), p5)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 4. With 4 second pruning window, 1st and 3rd items are pruned, id/ver added
	p1 = fmt.Sprintf("\"Cluster1\":\"%s\"", base.Uint64ToBase64(t1))
	p2 = fmt.Sprintf("\"Cluster2\":\"%s\"", base.Uint64ToBase64(t5))
	p3 = fmt.Sprintf("\"Cluster3\":\"%s\"", base.Uint64ToBase64(t3))
	p4 = fmt.Sprintf("\"Cluster4\":\"%s\"", base.Uint64ToBase64(t4))
	p5 = fmt.Sprintf("\"Cluster5\":\"%s\"", base.Uint64ToBase64(t2))
	pv = fmt.Sprintf("{\"Cluster2\":\"%s\",\"Cluster3\":\"%s\",\"Cluster4\":\"%s\",\"Cluster5\":\"%s\"}",
		base.Uint64ToBase64(t5), base.Uint64ToBase64(t3), base.Uint64ToBase64(t4), base.Uint64ToBase64(t2))
	meta, err = NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = ConstructCustomCRXattrForSetMeta(meta, 4*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex, newCvHex))
	assert.Contains(string(body[4:pos]), p1)
	assert.Contains(string(body[4:pos]), p3)
	assert.Contains(string(body[4:pos]), p5)
	assert.NotContains(string(body[4:pos]), p2)
	assert.NotContains(string(body[4:pos]), p4)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))
}

func setMetaPruningWithMvNoNewChange(t *testing.T) {
	assert := assert.New(t)

	sourceClusterId := []byte("SourceCluster")

	key := []byte("dockey")
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
	v2 := fmt.Sprintf("\"Cluster2\":\"%s\"", base.Uint64ToBase64(t2))
	v3 := fmt.Sprintf("\"Cluster3\":\"%s\"", base.Uint64ToBase64(t3))
	v4 := fmt.Sprintf("\"Cluster4\":\"%s\"", base.Uint64ToBase64(t4))
	v5 := fmt.Sprintf("\"Cluster5\":\"%s\"", base.Uint64ToBase64(t5))
	mv := fmt.Sprintf("{%s,%s}", v2, v3)
	pv := fmt.Sprintf("{%s,%s}", v4, v5)
	meta, err := NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), []byte(mv))
	assert.Nil(err)
	composer := base.NewXattrRawComposer(body)
	pos, err := ConstructCustomCRXattrForSetMeta(meta, 5*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex, cvHex))
	assert.Contains(string(body[4:pos]), v4)
	assert.Contains(string(body[4:pos]), v5)
	assert.Contains(string(body[4:pos]), v2)
	assert.Contains(string(body[4:pos]), v3)
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 3 second pruning window and a valid MV, the PV is pruned
	composer = base.NewXattrRawComposer(body)
	pos, err = ConstructCustomCRXattrForSetMeta(meta, 3*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex, cvHex))
	assert.NotContains(string(body[4:pos]), "pv")
	assert.Contains(string(body[4:pos]), v2)
	assert.Contains(string(body[4:pos]), v3)
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3. With 4 second pruning window, only Cluster5/t4 survives
	pv = fmt.Sprintf("{\"Cluster4\":\"%s\",\"Cluster5\":\"%s\",\"Cluster6\":\"%s\"}",
		base.Uint64ToBase64(t5), base.Uint64ToBase64(t4), base.Uint64ToBase64(t6))
	meta, err = NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), []byte(mv))
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = ConstructCustomCRXattrForSetMeta(meta, 4*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex, cvHex))
	pvPruned := fmt.Sprintf("\"pv\":{\"Cluster5\":\"%s\"}", base.Uint64ToBase64(t4))
	assert.Contains(string(body[4:pos]), pvPruned)
	// These are in MV
	assert.Contains(string(body[4:pos]), v2)
	assert.Contains(string(body[4:pos]), v3)
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))
}

func setMetaPruningWithMvNewChange(t *testing.T) {
	assert := assert.New(t)

	sourceClusterId := []byte("SourceCluster")

	key := []byte("docKey")
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
	v2 := fmt.Sprintf("\"Cluster2\":\"%s\"", base.Uint64ToBase64(t2))
	v3 := fmt.Sprintf("\"Cluster3\":\"%s\"", base.Uint64ToBase64(t3))
	v4 := fmt.Sprintf("\"Cluster4\":\"%s\"", base.Uint64ToBase64(t4))
	v5 := fmt.Sprintf("\"Cluster5\":\"%s\"", base.Uint64ToBase64(t5))
	v6 := fmt.Sprintf("\"Cluster6\":\"%s\"", base.Uint64ToBase64(t6))
	mv := fmt.Sprintf("{%s,%s}", v2, v3)
	pv := fmt.Sprintf("{%s,%s,%s}", v4, v5, v6)
	meta, err := NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), []byte(mv))
	assert.Nil(err)
	composer := base.NewXattrRawComposer(body)
	pos, err := ConstructCustomCRXattrForSetMeta(meta, 7*time.Second, composer)
	assert.Nil(err)
	newCvHex := base.Uint64ToHexLittleEndian(cas)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex, newCvHex))
	assert.Contains(string(body[4:pos]), "pv")
	assert.Contains(string(body[4:pos]), v2)
	assert.Contains(string(body[4:pos]), v3)
	assert.Contains(string(body[4:pos]), v4)
	assert.Contains(string(body[4:pos]), v5)
	assert.Contains(string(body[4:pos]), v6)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 5 second pruning window, part of pv is pruned
	mv = fmt.Sprintf("{%s,%s}", v2, v3)
	pv = fmt.Sprintf("{%s,%s,%s}", v4, v5, v6)
	meta, err = NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), []byte(cvHex), []byte(pv), []byte(mv))
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, err = ConstructCustomCRXattrForSetMeta(meta, 5*time.Second, composer)
	assert.Nil(err)
	newCvHex = base.Uint64ToHexLittleEndian(cas)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex, newCvHex))
	assert.Contains(string(body[4:pos]), "pv")
	assert.Contains(string(body[4:pos]), v2)
	assert.Contains(string(body[4:pos]), v3)
	assert.Contains(string(body[4:pos]), v4)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3. With 3 second pruning window, mv is moved to pv, then everything pass 3 second is pruned. So only Cluster2 version survives
	composer = base.NewXattrRawComposer(body)
	pos, err = ConstructCustomCRXattrForSetMeta(meta, 3*time.Second, composer)
	assert.Nil(err)
	newCvHex = base.Uint64ToHexLittleEndian(cas)

	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex, newCvHex))
	assert.Contains(string(body[4:pos]), v2)
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))
}

func TestMergeMeta(t *testing.T) {
	fmt.Println("============== Test case start: TestMergeMeta =================")
	defer fmt.Println("============== Test case end: TestMergeMeta =================")

	sourceClusterId := []byte("SourceCluster")
	targetClusterId := []byte("TargetCluster")
	key := []byte("dockey")
	mv := make([]byte, 1000)
	pv := make([]byte, 1000)
	assert := assert.New(t)
	cv, err := base.HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))
	assert.Nil(err)

	/*
	* 1. New at both source and target. Make sure we have MV but not PV.
	 */
	sourceMeta, err := NewMetadataForTest(key, sourceClusterId, cv+20000, 1, nil, nil, nil, nil, nil)
	assert.Nil(err)
	targetMeta, err := NewMetadataForTest(key, targetClusterId, cv+10000, 1, nil, nil, nil, nil, nil)
	assert.Nil(err)
	mergedMeta, err := sourceMeta.Merge(targetMeta)
	assert.Nil(err)
	mvlen := VersionMapToBytes(mergedMeta.GetHLV().GetMV(), mv, 0, nil)
	pvlen := VersionMapToBytes(mergedMeta.GetHLV().GetPV(), pv, 0, nil)

	assert.Contains(string(mv[:mvlen]), "\"SourceCluster\":\"FhSNXrKFTis\"")
	assert.Contains(string(mv[:mvlen]), "\"TargetCluster\":\"FhSNXrKFJxs\"")
	assert.Equal(0, pvlen)

	/*
	* 2. Source and target both updated the same old document (from Cluster4)
	*    The two pv should be combined with srv/ver
	* oldXattr = "_vv\x00{\"src\":\"Cluster4\",\"ver\":\"0x0b0085b25e8d1416\",\"pv\":{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}}\x00"
	 */
	sourceMeta, err = NewMetadataForTest(key, sourceClusterId, cv+20000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		[]byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv+10000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		[]byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	mergedMeta, err = sourceMeta.Merge(targetMeta)
	assert.Nil(err)
	mvlen = VersionMapToBytes(mergedMeta.GetHLV().GetMV(), mv, 0, nil)
	pvlen = VersionMapToBytes(mergedMeta.GetHLV().GetPV(), pv, 0, nil)
	assert.Contains(string(mv[:mvlen]), "\"SourceCluster\":\"FhSNXrKFTis\"")
	assert.Contains(string(mv[:mvlen]), "\"TargetCluster\":\"FhSNXrKFJxs\"")
	assert.Contains(string(pv[:pvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(pv[:pvlen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(pv[:pvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(pv[:pvlen]), "\"Cluster4\":\"FhSNXrKFAAs\"")

	/*
	* 3. Source and target contain conflict with updates from other clusters. Both have different pv
	* Source cluster contains changes coming from cluster4: "_vv\x00{\"src\":\"Cluster4\",\"ver\":\"0x0b0085b25e8d1416\",\"pv\":{\"Cluster1\":\"FhSITdr4AAA\"}}\x00"
	* Target cluster contains changes coming from cluster5: "_vv\x00{\"src\":\"Cluster5\"ver\":\"0x0b0085b25e8d1416\",\"pv\":{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}}\x00"
	 */
	cv, _ = base.HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))
	sourceMeta, err = NewMetadataForTest(key, sourceClusterId, cv, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\"FhSITdr4AAA\"}"), nil)
	assert.Nil(err)
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	mergedMeta, err = sourceMeta.Merge(targetMeta)
	assert.Nil(err)
	mvlen = VersionMapToBytes(mergedMeta.GetHLV().GetMV(), mv, 0, nil)
	pvlen = VersionMapToBytes(mergedMeta.GetHLV().GetPV(), pv, 0, nil)
	assert.Contains(string(mv[:mvlen]), "\"Cluster4\":\"FhSNXrKFAAs\"")
	assert.Contains(string(mv[:mvlen]), "\"Cluster5\":\"FhSNXrKFAAs\"")
	assert.Contains(string(pv[:pvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(pv[:pvlen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(pv[:pvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")

	/*
	* 4. Source and target both updated. Both have pv, one has mv
	 */
	cv, _ = base.HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))
	sourceMeta, err = NewMetadataForTest(key, sourceClusterId, cv+20000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\"FhSITdr4AAA\"}"), []byte("{\"Cluster3\":\"FhSITdr4ACA\",\"Cluster2\":\"FhSITdr4ABU\"}"))
	assert.Nil(err)
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv+10000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\"}"), nil)
	assert.Nil(err)
	mergedMeta, err = sourceMeta.Merge(targetMeta)
	assert.Nil(err)
	mvlen = VersionMapToBytes(mergedMeta.GetHLV().GetMV(), mv, 0, nil)
	pvlen = VersionMapToBytes(mergedMeta.GetHLV().GetPV(), pv, 0, nil)
	assert.Contains(string(mv[:mvlen]), "\"SourceCluster\":\"FhSNXrKFTis\"")
	assert.Contains(string(mv[:mvlen]), "\"TargetCluster\":\"FhSNXrKFJxs\"")
	assert.Contains(string(pv[:pvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(pv[:pvlen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(pv[:pvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")

	/*
	* 5. Source is a merged doc. Target is an update with pv
	 */
	cv, _ = base.HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))
	c1 := base.Uint64ToBase64(1591046436336173056)
	c2 := base.Uint64ToBase64(1591046436336173056 - 10000)
	sourceMeta, err = NewMetadataForTest(key, sourceClusterId, cv, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\""+string(c1)+"\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv+10000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\""+string(c2)+"\",\"Cluster2\":\"FhSITdr4ABU\"}"), nil)
	assert.Nil(err)
	mergedMeta, err = sourceMeta.Merge(targetMeta)
	assert.Nil(err)
	mvlen = VersionMapToBytes(mergedMeta.GetHLV().GetMV(), mv, 0, nil)
	pvlen = VersionMapToBytes(mergedMeta.GetHLV().GetPV(), pv, 0, nil)
	assert.Contains(string(mv[:mvlen]), "\"TargetCluster\":\"FhSNXrKFJxs\"")
	assert.Contains(string(mv[:mvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mv[:mvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(pv[:pvlen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(pv[:pvlen]), "\"Cluster5\":\"FhSNXrKFAAs\"")

	/*
	* 6. Target is a merged doc. Source is an update with PV and MV
	 */
	sourceMeta, err = NewMetadataForTest(key, sourceClusterId, cv+10000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\""+string(c2)+"\",\"Cluster2\":\"FhSITdr4ABU\"}"), nil)
	assert.Nil(err)
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\""+string(c1)+"\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)
	mergedMeta, err = sourceMeta.Merge(targetMeta)
	assert.Nil(err)
	mvlen = VersionMapToBytes(mergedMeta.GetHLV().GetMV(), mv, 0, nil)
	pvlen = VersionMapToBytes(mergedMeta.GetHLV().GetPV(), pv, 0, nil)
	assert.Contains(string(mv[:mvlen]), "\"SourceCluster\":\"FhSNXrKFJxs\"")
	assert.Contains(string(mv[:mvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mv[:mvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(pv[:pvlen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(pv[:pvlen]), "\"Cluster5\":\"FhSNXrKFAAs\"")

	/*
	* 7. Both are merged docs.
	 */
	sourceMeta, err = NewMetadataForTest(key, sourceClusterId, cv, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\""+string(c2)+"\",\"Cluster2\":\"FhSITdr4ABU\"}"))
	assert.Nil(err)
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\""+string(c1)+"\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)
	mergedMeta, err = sourceMeta.Merge(targetMeta)
	assert.Nil(err)
	mvlen = VersionMapToBytes(mergedMeta.GetHLV().GetMV(), mv, 0, nil)
	pvlen = VersionMapToBytes(mergedMeta.GetHLV().GetPV(), pv, 0, nil)
	assert.Contains(string(mv[:mvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mv[:mvlen]), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(mv[:mvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Equal(0, pvlen)

	/*
	* 8. Source is a new change. Target has a history
	 */
	sourceMeta, err = NewMetadataForTest(key, sourceClusterId, cv+2000, 1, nil, nil, nil, nil, nil)
	assert.Nil(err)
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv+1000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\""+string(c1)+"\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	mergedMeta, err = sourceMeta.Merge(targetMeta)
	assert.Nil(err)
	mvlen = VersionMapToBytes(mergedMeta.GetHLV().GetMV(), mv, 0, nil)
	pvlen = VersionMapToBytes(mergedMeta.GetHLV().GetPV(), pv, 0, nil)
	assert.Contains(string(pv[:pvlen]), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(pv[:pvlen]), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(pv[:pvlen]), "\"Cluster4\":\"FhSNXrKFAAs\"")
	assert.Contains(string(mv[:mvlen]), "\"SourceCluster\":\"FhSNXrKFB9s\"")
	assert.Contains(string(mv[:mvlen]), "\"TargetCluster\":\"FhSNXrKFA/M\"")
}

func TestUpdateMetaForSetBack(t *testing.T) {
	fmt.Println("============== Test case start: TestUpdateMetaForSetBack =================")
	defer fmt.Println("============== Test case end: TestUpdateMetaForSetBack =================")

	targetClusterId := []byte("TargetCluster")
	assert := assert.New(t)
	key := []byte("dockey")
	/*
	* 1. Target has a PV, no MV, and no new update at TargetCluster. srv/ver (Cluster4) is added to pv
	 */
	cvHex := []byte("0x0b0085b25e8d1416")
	cv, err := base.HexLittleEndianToUint64(cvHex)
	assert.Nil(err)
	targetMeta, err := NewMetadataForTest(key, targetClusterId, cv, 1, cvHex, []byte("Cluster4"), cvHex, []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
	assert.Nil(err)
	pv, mv, err := targetMeta.UpdateMetaForSetBack()
	assert.Nil(err)
	assert.Nil(mv)
	assert.Contains(string(pv), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(pv), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(pv), "\"Cluster4\":\"FhSNXrKFAAs\"")

	/*
	* 2. Target has a PV, no MV, and new update. ver/src and cas/source are added to pv
	 */
	cvHex = []byte("0x0b0085b25e8d1416")
	cv, err = base.HexLittleEndianToUint64(cvHex)
	assert.Nil(err)
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv+1000, 1, cvHex, []byte("Cluster4"), cvHex, []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster3\":\"FhSITdr4ACA\"}"), nil)
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
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)

	pv, mv, err = targetMeta.UpdateMetaForSetBack()
	assert.Nil(err)
	assert.Nil(pv)
	assert.Contains(string(mv), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(mv), "\"Cluster3\":\"FhSITdr4ACA\"")

	/*
	* 4. Target is a merged docs with new change.
	 */
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv+1000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster2\":\"FhSITdr4ABU\"}"), []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster3\":\"FhSITdr4ACA\"}"))
	assert.Nil(err)

	pv, mv, err = targetMeta.UpdateMetaForSetBack()
	assert.Nil(mv)
	assert.Contains(string(pv), "\"Cluster1\":\"FhSITdr4AAA\"")
	assert.Contains(string(pv), "\"Cluster2\":\"FhSITdr4ABU\"")
	assert.Contains(string(pv), "\"Cluster3\":\"FhSITdr4ACA\"")
	assert.Contains(string(pv), "\"TargetCluster\":\"FhSNXrKFA/M\"")
}
