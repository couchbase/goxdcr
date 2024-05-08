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
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gomemcached"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/hlv"
	"github.com/couchbaselabs/gojsonsm"
	"github.com/stretchr/testify/assert"
)

func TestConstructCustomCRXattrForSetMeta(t *testing.T) {
	fmt.Println("============== Test case start: TestConstructCustomCRXattr =================")
	defer fmt.Println("============== Test case end: TestConstructCustomCRXattr =================")

	assert := assert.New(t)

	// sorted order for deltas
	vh1, s1 := "0x0000f8da4d881416", "Cluster1"
	vh2, s2 := "0x1500f8da4d881416", "Cluster2"
	vh3, s3 := "0x2000f8da4d881416", "Cluster3"
	vh4, s4 := "0x0b0085b25e8d1416", "Cluster4"
	v1, err := base.HexLittleEndianToUint64([]byte(vh1))
	assert.Nil(err)
	v2, err := base.HexLittleEndianToUint64([]byte(vh2))
	assert.Nil(err)
	v3, err := base.HexLittleEndianToUint64([]byte(vh3))
	assert.Nil(err)
	v4, err := base.HexLittleEndianToUint64([]byte(vh4))
	assert.Nil(err)

	// different deltas
	d1 := v1
	d2 := v2 - v1
	d3 := v3 - v2
	d4 := v4 - v3
	dh1 := base.Uint64ToHexLittleEndianAndStrip0s(d1)
	dh2 := base.Uint64ToHexLittleEndianAndStrip0s(d2)
	dh3 := base.Uint64ToHexLittleEndianAndStrip0s(d3)
	dh4 := base.Uint64ToHexLittleEndianAndStrip0s(d4)

	// _vv:{"ver":"0x0b0085b25e8d1416","src":"Cluster4","pv":{"Cluster1":"0x0000f8da4d881416","Cluster2":"0x1500f8da4d881416","Cluster3":"0x2000f8da4d881416"}}
	ver, err := base.HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))
	assert.Nil(err)

	docKey := []byte("docKey")
	sourceClusterId := []byte("SourceCluster")

	body := make([]byte, 1000)

	// Test 1. First change, no existing _vv
	meta, err := NewMetadataForTest(docKey, sourceClusterId, ver, 1, nil, nil, nil, nil, nil)
	assert.Nil(err)
	composer := base.NewXattrRawComposer(body)
	pos, _, err := ConstructXattrFromHlvForSetMeta(meta, 0, composer)
	assert.Nil(err)
	assert.Equal("_vv\x00{\"cvCas\":\"0x0b0085b25e8d1416\",\"src\":\"SourceCluster\",\"ver\":\"0x0b0085b25e8d1416\"}\x00", string(body[4:pos]))
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2: New change cas > ver, expected to have updated srv, ver, and pv
	// oldXattr = "_vv\x00{\"src\":\"Cluster4\",\"ver\":\"0x0b0085b25e8d1416\"}\x00"
	meta, err = NewMetadataForTest(docKey, sourceClusterId, ver+1000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, _, err = ConstructXattrFromHlvForSetMeta(meta, 0, composer)
	assert.Nil(err)
	newXattr := "_vv\x00{\"cvCas\":\"0xf30385b25e8d1416\",\"src\":\"SourceCluster\",\"ver\":\"0xf30385b25e8d1416\",\"pv\":{\"Cluster4\":\"0x0b0085b25e8d1416\"}}\x00"
	assert.Equal(newXattr, string(body[4:pos]))
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3: New change (cas=ver+1000) with existing XATTR (pv with deltas):
	// _vv:{\"cvCas\":\"0xf30385b25e8d1416\","ver":"0x0b0085b25e8d1416","src":"Cluster4","pv":{"Cluster1":"0x0000f8da4d881416","Cluster2":"0x1500f8da4d881416","Cluster3":"0x2000f8da4d881416"}}
	// oldXattr = "_vv\x00{\"cvCas\":\"0x0b0085b25e8d1416\",\"src\":\"Cluster4\",\"ver\":\"0x0b0085b25e8d1416\",\"pv\":{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster2\":\"0x1500f8da4d881416\",\"Cluster3\":\"0x2000f8da4d881416\"}}\x00"
	meta, err = NewMetadataForTest(docKey, sourceClusterId, ver+1000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		[]byte(fmt.Sprintf(`{"%s":"%s","%s":"%s","%s":"%s"}`, s1, dh1, s2, dh2, s3, dh3)), nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, _, err = ConstructXattrFromHlvForSetMeta(meta, 0, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), "_vv\x00{\"cvCas\":\"0xf30385b25e8d1416\",\"src\":\"SourceCluster\",\"ver\":\"0xf30385b25e8d1416\",\"pv\":")
	assert.Contains(string(body[4:pos]), fmt.Sprintf(`"pv":{"%s":"%s","%s":"%s","%s":"%s","%s":"%s"}}`, s1, dh1, s2, dh2, s3, dh3, s4, dh4))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 4: New change (cas=ver+1000) with existing XATTR (mv):
	meta, err = NewMetadataForTest(docKey, sourceClusterId, ver+1000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		nil, []byte("{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster2\":\"0x15\",\"Cluster3\":\"0x0b\"}"))
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, _, err = ConstructXattrFromHlvForSetMeta(meta, 0, composer)
	assert.Nil(err)
	assert.Contains(string(body[0:pos]), "_vv\x00{\"cvCas\":\"0xf30385b25e8d1416\",\"src\":\"SourceCluster\",\"ver\":\"0xf30385b25e8d1416\",\"pv\":")
	assert.Contains(string(body[4:pos]), fmt.Sprintf(`"pv":{"%s":"%s","%s":"%s","%s":"%s"}}`, s1, dh1, s2, dh2, s3, dh3))
	assert.NotContains(string(body[0:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 5: New change (cas=ver+1000) with existing XATTR(pv and mv):
	meta, err = NewMetadataForTest(docKey, sourceClusterId, ver+1000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		[]byte("{\"Cluster1\":\"0x0000f8da4d881416\"}"), []byte("{\"Cluster2\":\"0x1500f8da4d881416\",\"Cluster3\":\"0x0b\"}"))
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, _, err = ConstructXattrFromHlvForSetMeta(meta, 0, composer)
	assert.Nil(err)
	assert.Contains(string(body[0:pos]), "_vv\x00{\"cvCas\":\"0xf30385b25e8d1416\",\"src\":\"SourceCluster\",\"ver\":\"0xf30385b25e8d1416\",\"pv\":")
	assert.Contains(string(body[4:pos]), fmt.Sprintf(`"pv":{"%s":"%s","%s":"%s","%s":"%s"}}`, s1, dh1, s2, dh2, s3, dh3))
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
	t2, s2 := t1-1000000000, "Cluster2" // 1 second before
	t3, s3 := t2-1000000000, "Cluster3"
	t4, s4 := t3-1000000000, "Cluster4"
	t5, s5 := t4-1000000000, "Cluster5"

	// First we have no new change (cas==ver) with four in pv
	cas := t1
	cv := cas
	cvHex := base.Uint64ToHexLittleEndian(cv)
	key := []byte("docKey")

	// Test 1. With 5 second pruning window, the whole pv survives
	p2 := fmt.Sprintf("\"%s\":\"%s\"", s5, base.Uint64ToHexLittleEndianAndStrip0s(t5))
	p3 := fmt.Sprintf("\"%s\":\"%s\"", s4, base.Uint64ToHexLittleEndianAndStrip0s(t4-t5))
	p4 := fmt.Sprintf("\"%s\":\"%s\"", s3, base.Uint64ToHexLittleEndianAndStrip0s(t3-t4))
	p5 := fmt.Sprintf("\"%s\":\"%s\"", s2, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3))
	pv := fmt.Sprintf("{%s,%s,%s,%s}", p2, p3, p4, p5)
	meta, err := NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), nil)
	assert.Nil(err)
	composer := base.NewXattrRawComposer(body)
	pos, _, err := ConstructXattrFromHlvForSetMeta(meta, 5*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex, cvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf(`"pv":{%s,%s,%s,%s}`, p2, p3, p4, p5))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 3 second pruning window, the last 2 items are pruned i.e. s4 and s5's entries pruned
	composer = base.NewXattrRawComposer(body)
	pos, _, err = ConstructXattrFromHlvForSetMeta(meta, 3*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex, cvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"pv\":{\"%s\":\"%s\",\"%s\":\"%s\"}", s3, base.Uint64ToHexLittleEndianAndStrip0s(t3), s2, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3)))
	assert.NotContains(string(body[4:pos]), fmt.Sprintf("\"%s\":", s4))
	assert.NotContains(string(body[4:pos]), fmt.Sprintf("\"%s\":", s5))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3. With 3 second pruning window, the first 2 items are pruned i.e. s2 and s3's entries pruned
	p2 = fmt.Sprintf("\"%s\":\"%s\"", s2, base.Uint64ToHexLittleEndianAndStrip0s(t5))
	p3 = fmt.Sprintf("\"%s\":\"%s\"", s3, base.Uint64ToHexLittleEndianAndStrip0s(t4-t5))
	p4 = fmt.Sprintf("\"%s\":\"%s\"", s4, base.Uint64ToHexLittleEndianAndStrip0s(t3-t4))
	p5 = fmt.Sprintf("\"%s\":\"%s\"", s5, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3))
	pv = fmt.Sprintf("{%s,%s,%s,%s}", p2, p3, p4, p5)
	meta, err = NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, _, err = ConstructXattrFromHlvForSetMeta(meta, 3*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex, cvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"pv\":{\"%s\":\"%s\",\"%s\":\"%s\"}", s4, base.Uint64ToHexLittleEndianAndStrip0s(t3), s5, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3)))
	assert.NotContains(string(body[4:pos]), fmt.Sprintf("\"%s\":", s2))
	assert.NotContains(string(body[4:pos]), fmt.Sprintf("\"%s\":", s3))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 4. With 3 second pruning window, 1st and 3rd items are pruned i.e. s2 and s4's entries pruned
	p2 = fmt.Sprintf("\"%s\":\"%s\"", s2, base.Uint64ToHexLittleEndianAndStrip0s(t5))
	p3 = fmt.Sprintf("\"%s\":\"%s\"", s4, base.Uint64ToHexLittleEndianAndStrip0s(t4-t5))
	p4 = fmt.Sprintf("\"%s\":\"%s\"", s5, base.Uint64ToHexLittleEndianAndStrip0s(t3-t4))
	p5 = fmt.Sprintf("\"%s\":\"%s\"", s3, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3))
	pv = fmt.Sprintf("{%s,%s,%s,%s}", p2, p3, p4, p5)
	meta, err = NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, _, err = ConstructXattrFromHlvForSetMeta(meta, 3*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex, cvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"pv\":{\"%s\":\"%s\",\"%s\":\"%s\"}", s5, base.Uint64ToHexLittleEndianAndStrip0s(t3), s3, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3)))
	assert.NotContains(string(body[4:pos]), fmt.Sprintf("\"%s\":", s2))
	assert.NotContains(string(body[4:pos]), fmt.Sprintf("\"%s\":", s4))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))
}

func setMetaPruningWithNewChange(t *testing.T) {
	assert := assert.New(t)

	sourceClusterId := []byte("SourceCluster")

	body := make([]byte, 1000)
	var t1 uint64 = 1591052006230130699
	t2, s2 := t1-1000000000, "Cluster2" // 1 second before
	t3, s3 := t2-1000000000, "Cluster3"
	t4, s4 := t3-1000000000, "Cluster4"
	t5, s5 := t4-1000000000, "Cluster5"

	// First we have new change (cas>ver) with four in pv
	cas, s1 := t1+1000000000, "Cluster1"
	cv := t1
	cvHex := base.Uint64ToHexLittleEndian(cv)
	key := []byte("docKey")

	// Test 1. With 6 second pruning window, the whole pv survives, plus the id/ver also goes into PV
	p2 := fmt.Sprintf("\"%s\":\"%s\"", s5, base.Uint64ToHexLittleEndianAndStrip0s(t5))
	p3 := fmt.Sprintf("\"%s\":\"%s\"", s4, base.Uint64ToHexLittleEndianAndStrip0s(t4-t5))
	p4 := fmt.Sprintf("\"%s\":\"%s\"", s3, base.Uint64ToHexLittleEndianAndStrip0s(t3-t4))
	p5 := fmt.Sprintf("\"%s\":\"%s\"", s2, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3))
	pv := fmt.Sprintf("{%s,%s,%s,%s}", p2, p3, p4, p5)

	meta, err := NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), nil)
	assert.Nil(err)
	composer := base.NewXattrRawComposer(body)
	pos, _, err := ConstructXattrFromHlvForSetMeta(meta, 6*time.Second, composer)
	assert.Nil(err)
	newCvHex := base.Uint64ToHexLittleEndian(cas)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex, newCvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"pv\":{%s,%s,%s,%s,\"%s\":\"%s\"}", p2, p3, p4, p5, s1, base.Uint64ToHexLittleEndianAndStrip0s(cas-t1)))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 3 second pruning window, the last 3 items are pruned i.e. s3, s4 and s5's entries pruned; plus id/ver are added to PV
	composer = base.NewXattrRawComposer(body)
	pos, _, err = ConstructXattrFromHlvForSetMeta(meta, 3*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex, newCvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"pv\":{\"%s\":\"%s\",\"%s\":\"%s\"}", s2, base.Uint64ToHexLittleEndianAndStrip0s(t2), s1, base.Uint64ToHexLittleEndianAndStrip0s(t1-t2)))
	assert.NotContains(string(body[4:pos]), fmt.Sprintf("\"%s\":", s3))
	assert.NotContains(string(body[4:pos]), fmt.Sprintf("\"%s\":", s4))
	assert.NotContains(string(body[4:pos]), fmt.Sprintf("\"%s\":", s5))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3. With 3 second pruning window, id/ver is added; only s2 in PV stays, along with s1 in cv
	p2 = fmt.Sprintf("\"%s\":\"%s\"", s2, base.Uint64ToHexLittleEndianAndStrip0s(t5))
	p3 = fmt.Sprintf("\"%s\":\"%s\"", s3, base.Uint64ToHexLittleEndianAndStrip0s(t4-t5))
	p4 = fmt.Sprintf("\"%s\":\"%s\"", s4, base.Uint64ToHexLittleEndianAndStrip0s(t2-t4))
	p5 = fmt.Sprintf("\"%s\":\"%s\"", s1, base.Uint64ToHexLittleEndianAndStrip0s(t1-t2))
	pv = fmt.Sprintf("{%s,%s,%s,%s}", p2, p3, p4, p5)
	meta, err = NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, _, err = ConstructXattrFromHlvForSetMeta(meta, 3*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex, newCvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"pv\":{\"%s\":\"%s\",\"%s\":\"%s\"}", s4, base.Uint64ToHexLittleEndianAndStrip0s(t2), s1, base.Uint64ToHexLittleEndianAndStrip0s(t1-t2)))
	assert.NotContains(string(body[4:pos]), fmt.Sprintf("\"%s\":", s2))
	assert.NotContains(string(body[4:pos]), fmt.Sprintf("\"%s\":", s3))
	assert.NotContains(string(body[4:pos]), fmt.Sprintf("\"%s\":", s5))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 4. With 4 second pruning window, 1st and 3rd items are pruned, id/ver added i.e. s2 and s4 entries pruned
	p2 = fmt.Sprintf("\"%s\":\"%s\"", s2, base.Uint64ToHexLittleEndianAndStrip0s(t5))
	p3 = fmt.Sprintf("\"%s\":\"%s\"", s4, base.Uint64ToHexLittleEndianAndStrip0s(t4-t5))
	p4 = fmt.Sprintf("\"%s\":\"%s\"", s3, base.Uint64ToHexLittleEndianAndStrip0s(t3-t4))
	p5 = fmt.Sprintf("\"%s\":\"%s\"", s5, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3))
	pv = fmt.Sprintf("{%s,%s,%s,%s}", p2, p3, p4, p5)
	meta, err = NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), nil)
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, _, err = ConstructXattrFromHlvForSetMeta(meta, 4*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex, newCvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"pv\":{\"%s\":\"%s\",\"%s\":\"%s\",\"%s\":\"%s\"}", s3, base.Uint64ToHexLittleEndianAndStrip0s(t3), s5, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3), s1, base.Uint64ToHexLittleEndianAndStrip0s(t1-t2)))
	assert.NotContains(string(body[4:pos]), fmt.Sprintf("\"%s\":", s2))
	assert.NotContains(string(body[4:pos]), fmt.Sprintf("\"%s\":", s4))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))
}

func setMetaPruningWithMvNoNewChange(t *testing.T) {
	assert := assert.New(t)

	sourceClusterId := []byte("SourceCluster")

	key := []byte("dockey")
	body := make([]byte, 1000)
	var t1 uint64 = 1591052006230130699
	t2, s2 := t1-1000000000, "Cluster2" // 1 second before cas
	t3, s3 := t2-1000000000, "Cluster3" // 2 seconds before cas
	t4, s4 := t3-1000000000, "Cluster4" // 3 seconds before cas
	t5, s5 := t4-1000000000, "Cluster5" // 4 seconds before cas
	t6, s6 := t5-1000000000, "Cluster6" // 5 seconds before cas
	// First we have no new change (cas==ver) with four in pv
	cas := t1
	cv := cas
	cvHex := base.Uint64ToHexLittleEndian(cv)

	// Test 1. With 5 second pruning window, the whole pv survives
	v2 := fmt.Sprintf("\"%s\":\"%s\"", s5, base.Uint64ToHexLittleEndianAndStrip0s(t5))
	v3 := fmt.Sprintf("\"%s\":\"%s\"", s4, base.Uint64ToHexLittleEndianAndStrip0s(t4-t5))
	v4 := fmt.Sprintf("\"%s\":\"%s\"", s3, base.Uint64ToHexLittleEndianAndStrip0s(t3))
	v5 := fmt.Sprintf("\"%s\":\"%s\"", s2, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3))
	pv := fmt.Sprintf("{%s,%s}", v2, v3)
	mv := fmt.Sprintf("{%s,%s}", v4, v5)
	meta, err := NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), []byte(mv))
	assert.Nil(err)
	composer := base.NewXattrRawComposer(body)
	pos, _, err := ConstructXattrFromHlvForSetMeta(meta, 5*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex, cvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"pv\":{\"%s\":\"%s\",\"%s\":\"%s\"}", s5, base.Uint64ToHexLittleEndianAndStrip0s(t5), s4, base.Uint64ToHexLittleEndianAndStrip0s(t4-t5)))
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"mv\":{\"%s\":\"%s\",\"%s\":\"%s\"}", s3, base.Uint64ToHexLittleEndianAndStrip0s(t3), s2, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3)))
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 3 second pruning window and a valid MV, the PV is pruned
	composer = base.NewXattrRawComposer(body)
	pos, _, err = ConstructXattrFromHlvForSetMeta(meta, 3*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex, cvHex))
	assert.NotContains(string(body[4:pos]), "pv")
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"mv\":{\"%s\":\"%s\",\"%s\":\"%s\"}", s3, base.Uint64ToHexLittleEndianAndStrip0s(t3), s2, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3)))
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3. With 4 second pruning window, only Cluster5/t4 survives
	pv = fmt.Sprintf("{\"%s\":\"%s\",\"%s\":\"%s\",\"%s\":\"%s\"}",
		s6, base.Uint64ToHexLittleEndianAndStrip0s(t6),
		s4, base.Uint64ToHexLittleEndianAndStrip0s(t5-t6),
		s5, base.Uint64ToHexLittleEndianAndStrip0s(t4-t5))
	meta, err = NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), []byte(mv))
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, _, err = ConstructXattrFromHlvForSetMeta(meta, 4*time.Second, composer)
	assert.Nil(err)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"Cluster1\",\"ver\":\"%s\",", cvHex, cvHex))
	pvPruned := fmt.Sprintf("\"pv\":{\"%s\":\"%s\"}", s5, base.Uint64ToHexLittleEndianAndStrip0s(t4))
	assert.Contains(string(body[4:pos]), pvPruned)
	// These are in MV
	assert.Contains(string(body[4:pos]), fmt.Sprintf("\"mv\":{\"%s\":\"%s\",\"%s\":\"%s\"}", s3, base.Uint64ToHexLittleEndianAndStrip0s(t3), s2, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3)))
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))
}

func setMetaPruningWithMvNewChange(t *testing.T) {
	assert := assert.New(t)

	sourceClusterId := []byte("SourceCluster")

	key := []byte("docKey")
	body := make([]byte, 1000)
	var t1 uint64 = 1591052006230130699
	t2, s2 := t1-1000000000, "Cluster2" // 1 second before cas
	t3, s3 := t2-1000000000, "Cluster3" // 2 seconds before cas
	t4, s4 := t3-1000000000, "Cluster4" // 3 seconds before cas
	t5, s5 := t4-1000000000, "Cluster5" // 4 seconds before cas
	t6, s6 := t5-1000000000, "Cluster6" // 5 seconds before cas
	// First we have no new change (cas==ver) with four in pv
	cas := t1 + 1000000000
	cv := t1
	cvHex := base.Uint64ToHexLittleEndian(cv)

	// Test 1. With 7 second pruning window, the whole pv survives
	v2 := fmt.Sprintf("\"%s\":\"%s\"", s6, base.Uint64ToHexLittleEndianAndStrip0s(t6))
	v3 := fmt.Sprintf("\"%s\":\"%s\"", s5, base.Uint64ToHexLittleEndianAndStrip0s(t5-t6))
	v4 := fmt.Sprintf("\"%s\":\"%s\"", s4, base.Uint64ToHexLittleEndianAndStrip0s(t4-t5))
	v5 := fmt.Sprintf("\"%s\":\"%s\"", s3, base.Uint64ToHexLittleEndianAndStrip0s(t3))
	v6 := fmt.Sprintf("\"%s\":\"%s\"", s2, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3))
	pv := fmt.Sprintf("{%s,%s,%s}", v2, v3, v4)
	mv := fmt.Sprintf("{%s,%s}", v5, v6)
	meta, err := NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), cvHex, []byte(pv), []byte(mv))
	assert.Nil(err)
	composer := base.NewXattrRawComposer(body)
	pos, _, err := ConstructXattrFromHlvForSetMeta(meta, 7*time.Second, composer)
	assert.Nil(err)
	newCvHex := base.Uint64ToHexLittleEndian(cas)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex, newCvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf(`"pv":{"%s":"%s","%s":"%s","%s":"%s","%s":"%s","%s":"%s"}`, s6, base.Uint64ToHexLittleEndianAndStrip0s(t6), s5,
		base.Uint64ToHexLittleEndianAndStrip0s(t5-t6), s4, base.Uint64ToHexLittleEndianAndStrip0s(t4-t5), s3, base.Uint64ToHexLittleEndianAndStrip0s(t3-t4), s2, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3)))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 2. With 5 second pruning window, part of pv is pruned
	v2 = fmt.Sprintf("\"%s\":\"%s\"", s6, base.Uint64ToHexLittleEndianAndStrip0s(t6))
	v3 = fmt.Sprintf("\"%s\":\"%s\"", s5, base.Uint64ToHexLittleEndianAndStrip0s(t5-t6))
	v4 = fmt.Sprintf("\"%s\":\"%s\"", s4, base.Uint64ToHexLittleEndianAndStrip0s(t4))
	v5 = fmt.Sprintf("\"%s\":\"%s\"", s3, base.Uint64ToHexLittleEndianAndStrip0s(t3-t4))
	v6 = fmt.Sprintf("\"%s\":\"%s\"", s2, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3))
	mv = fmt.Sprintf("{%s,%s}", v2, v3)
	pv = fmt.Sprintf("{%s,%s,%s}", v4, v5, v6)
	meta, err = NewMetadataForTest(key, sourceClusterId, cas, 1, cvHex, []byte("Cluster1"), []byte(cvHex), []byte(pv), []byte(mv))
	assert.Nil(err)
	composer = base.NewXattrRawComposer(body)
	pos, _, err = ConstructXattrFromHlvForSetMeta(meta, 5*time.Second, composer)
	assert.Nil(err)
	newCvHex = base.Uint64ToHexLittleEndian(cas)
	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex, newCvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf(`"pv":{"%s":"%s","%s":"%s","%s":"%s"}`, s4, base.Uint64ToHexLittleEndianAndStrip0s(t4), s3, base.Uint64ToHexLittleEndianAndStrip0s(t3-t4), s2, base.Uint64ToHexLittleEndianAndStrip0s(t2-t3)))
	assert.NotContains(string(body[4:pos]), "mv")
	assert.Equal(uint32(pos-4), binary.BigEndian.Uint32(body[0:4]))

	// Test 3. With 3 second pruning window, mv is moved to pv, then everything pass 3 second is pruned. So only Cluster2 version survives
	composer = base.NewXattrRawComposer(body)
	pos, _, err = ConstructXattrFromHlvForSetMeta(meta, 3*time.Second, composer)
	assert.Nil(err)
	newCvHex = base.Uint64ToHexLittleEndian(cas)

	assert.Contains(string(body[4:pos]), fmt.Sprintf("{\"cvCas\":\"%s\",\"src\":\"SourceCluster\",\"ver\":\"%s\",", newCvHex, newCvHex))
	assert.Contains(string(body[4:pos]), fmt.Sprintf(`"pv":{"%s":"%s"}`, s2, base.Uint64ToHexLittleEndianAndStrip0s(t2)))
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
	mvlen, _ := VersionMapToDeltasBytes(mergedMeta.GetHLV().GetMV(), mv, 0, nil)
	pvlen, _ := VersionMapToDeltasBytes(mergedMeta.GetHLV().GetPV(), pv, 0, nil)

	assert.Equal(string(mv[:mvlen]), "{\"TargetCluster\":\"0x1b2785b25e8d1416\",\"SourceCluster\":\"0x1027\"}")
	assert.Equal(0, pvlen)

	/*
	* 2. Source and target both updated the same old document (from Cluster4)
	*    The two pv should be combined with srv/ver
	* oldXattr = "_vv\x00{\"src\":\"Cluster4\",\"ver\":\"0x0b0085b25e8d1416\",\"pv\":{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster2\":\"0x15\",\"Cluster3\":\"0x0b\"}}\x00"
	 */
	sourceMeta, err = NewMetadataForTest(key, sourceClusterId, cv+20000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		[]byte("{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster2\":\"0x15\",\"Cluster3\":\"0x0b\"}"), nil)
	assert.Nil(err)
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv+10000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"),
		[]byte("{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster2\":\"0x15\",\"Cluster3\":\"0x0b\"}"), nil)
	assert.Nil(err)
	mergedMeta, err = sourceMeta.Merge(targetMeta)
	assert.Nil(err)
	mvlen, _ = VersionMapToDeltasBytes(mergedMeta.GetHLV().GetMV(), mv, 0, nil)
	pvlen, _ = VersionMapToDeltasBytes(mergedMeta.GetHLV().GetPV(), pv, 0, nil)
	assert.Equal(string(mv[:mvlen]), "{\"TargetCluster\":\"0x1b2785b25e8d1416\",\"SourceCluster\":\"0x1027\"}")
	assert.Equal(string(pv[:pvlen]), "{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster2\":\"0x15\",\"Cluster3\":\"0x0b\",\"Cluster4\":\"0xebff8cd71005\"}")

	/*
	* 3. Source and target contain conflict with updates from other clusters. Both have different pv
	* Source cluster contains changes coming from cluster4: "_vv\x00{\"src\":\"Cluster4\",\"ver\":\"0x0b0085b25e8d1416\",\"pv\":{\"Cluster1\":\"0x0000f8da4d881416\"}}\x00"
	* Target cluster contains changes coming from cluster5: "_vv\x00{\"src\":\"Cluster5\"ver\":\"0x0b0085b25e8d1416\",\"pv\":{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster2\":\"0x15\",\"Cluster3\":\"0x0b\"}}\x00"
	 */
	cv, _ = base.HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))
	sourceMeta, err = NewMetadataForTest(key, sourceClusterId, cv, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\"0x0000f8da4d881416\"}"), nil)
	assert.Nil(err)
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster2\":\"0x15\",\"Cluster3\":\"0x0b\"}"), nil)
	assert.Nil(err)
	mergedMeta, err = sourceMeta.Merge(targetMeta)
	assert.Nil(err)
	mvlen, _ = VersionMapToDeltasBytes(mergedMeta.GetHLV().GetMV(), mv, 0, nil)
	pvlen, _ = VersionMapToDeltasBytes(mergedMeta.GetHLV().GetPV(), pv, 0, nil)
	assert.Equal(string(mv[:mvlen]), "{\"Cluster4\":\"0x0b0085b25e8d1416\",\"Cluster5\":\"0x0\"}")
	assert.Equal(string(pv[:pvlen]), "{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster2\":\"0x15\",\"Cluster3\":\"0x0b\"}")

	/*
	* 4. Source and target both updated. Both have pv, one has mv
	 */
	cv, _ = base.HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))
	sourceMeta, err = NewMetadataForTest(key, sourceClusterId, cv+20000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\"0x0000f8da4d881416\"}"), []byte("{\"Cluster2\":\"0x1500f8da4d881416\",\"Cluster3\":\"0x0b\"}"))
	assert.Nil(err)
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv+10000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster2\":\"0x15\"}"), nil)
	assert.Nil(err)
	mergedMeta, err = sourceMeta.Merge(targetMeta)
	assert.Nil(err)
	mvlen, _ = VersionMapToDeltasBytes(mergedMeta.GetHLV().GetMV(), mv, 0, nil)
	pvlen, _ = VersionMapToDeltasBytes(mergedMeta.GetHLV().GetPV(), pv, 0, nil)
	assert.Equal(string(mv[:mvlen]), "{\"TargetCluster\":\"0x1b2785b25e8d1416\",\"SourceCluster\":\"0x1027\"}")
	assert.Equal(string(pv[:pvlen]), "{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster2\":\"0x15\",\"Cluster3\":\"0x0b\",\"Cluster5\":\"0xebff8cd71005\"}")

	/*
	* 5. Source is a merged doc. Target is an update with pv
	 */
	cv, _ = base.HexLittleEndianToUint64([]byte("0x0b0085b25e8d1416"))
	c1 := base.Uint64ToHexLittleEndian(1591046436336173056)
	c2 := base.Uint64ToHexLittleEndian(1591046436336173056 - 10000)
	sourceMeta, err = NewMetadataForTest(key, sourceClusterId, cv, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\""+string(c1)+"\",\"Cluster3\":\"0x2\"}"))
	assert.Nil(err)
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv+10000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\""+string(c2)+"\",\"Cluster2\":\"0x2527\"}"), nil)
	assert.Nil(err)
	mergedMeta, err = sourceMeta.Merge(targetMeta)
	assert.Nil(err)
	mvlen, _ = VersionMapToDeltasBytes(mergedMeta.GetHLV().GetMV(), mv, 0, nil)
	pvlen, _ = VersionMapToDeltasBytes(mergedMeta.GetHLV().GetPV(), pv, 0, nil)
	assert.Equal(string(mv[:mvlen]), "{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster3\":\"0x2\",\"TargetCluster\":\"0xfb268dd71005\"}")
	assert.Equal(string(pv[:pvlen]), "{\"Cluster2\":\"0x1500f8da4d881416\",\"Cluster5\":\"0xf6ff8cd71005\"}")

	/*
	* 6. Target is a merged doc. Source is an update with PV and MV
	 */
	sourceMeta, err = NewMetadataForTest(key, sourceClusterId, cv+10000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\""+string(c2)+"\",\"Cluster2\":\"0x2527\"}"), nil)
	assert.Nil(err)
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\""+string(c1)+"\",\"Cluster3\":\"0x2\"}"))
	assert.Nil(err)
	mergedMeta, err = sourceMeta.Merge(targetMeta)
	assert.Nil(err)
	mvlen, _ = VersionMapToDeltasBytes(mergedMeta.GetHLV().GetMV(), mv, 0, nil)
	pvlen, _ = VersionMapToDeltasBytes(mergedMeta.GetHLV().GetPV(), pv, 0, nil)
	assert.Equal(string(mv[:mvlen]), "{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster3\":\"0x2\",\"SourceCluster\":\"0xfb268dd71005\"}")
	assert.Equal(string(pv[:pvlen]), "{\"Cluster2\":\"0x1500f8da4d881416\",\"Cluster5\":\"0xf6ff8cd71005\"}")

	/*
	* 7. Both are merged docs.
	 */
	sourceMeta, err = NewMetadataForTest(key, sourceClusterId, cv, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster5"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\""+string(c2)+"\",\"Cluster2\":\"0x2527\"}"))
	assert.Nil(err)
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\""+string(c1)+"\",\"Cluster3\":\"0x2\"}"))
	assert.Nil(err)
	mergedMeta, err = sourceMeta.Merge(targetMeta)
	assert.Nil(err)
	mvlen, _ = VersionMapToDeltasBytes(mergedMeta.GetHLV().GetMV(), mv, 0, nil)
	pvlen, _ = VersionMapToDeltasBytes(mergedMeta.GetHLV().GetPV(), pv, 0, nil)
	assert.Equal(string(mv[:mvlen]), "{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster2\":\"0x15\",\"Cluster3\":\"0x0b\"}")
	assert.Equal(0, pvlen)

	/*
	* 8. Source is a new change. Target has a history
	 */
	sourceMeta, err = NewMetadataForTest(key, sourceClusterId, cv+2000, 1, nil, nil, nil, nil, nil)
	assert.Nil(err)
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv+1000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster1\":\""+string(c1)+"\",\"Cluster3\":\"0x2\"}"), nil)
	assert.Nil(err)
	mergedMeta, err = sourceMeta.Merge(targetMeta)
	assert.Nil(err)
	mvlen, _ = VersionMapToDeltasBytes(mergedMeta.GetHLV().GetMV(), mv, 0, nil)
	pvlen, _ = VersionMapToDeltasBytes(mergedMeta.GetHLV().GetPV(), pv, 0, nil)
	assert.Equal(string(pv[:pvlen]), "{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster3\":\"0x2\",\"Cluster4\":\"0xebff8cd71005\"}")
	assert.Contains(string(mv[:mvlen]), "{\"TargetCluster\":\"0xf30385b25e8d1416\",\"SourceCluster\":\"0xe803\"}")
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
	targetMeta, err := NewMetadataForTest(key, targetClusterId, cv, 1, cvHex, []byte("Cluster4"), cvHex, []byte("{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster3\":\"0x2\"}"), nil)
	assert.Nil(err)
	pv, mv, err := targetMeta.UpdateMetaForSetBack()
	assert.Nil(err)
	assert.Nil(mv)
	assert.Equal(string(pv), "{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster3\":\"0x2\",\"Cluster4\":\"0xebff8cd71005\"}")

	/*
	* 2. Target has a PV, no MV, and new update. ver/src and cas/source are added to pv
	 */
	cvHex = []byte("0x0b0085b25e8d1416")
	cv, err = base.HexLittleEndianToUint64(cvHex)
	assert.Nil(err)
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv+1000, 1, cvHex, []byte("Cluster4"), cvHex, []byte("{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster3\":\"0x2\"}"), nil)
	assert.Nil(err)
	pv, mv, err = targetMeta.UpdateMetaForSetBack()
	assert.Nil(err)
	assert.Nil(mv)
	assert.Equal(string(pv), "{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster3\":\"0x2\",\"Cluster4\":\"0xebff8cd71005\",\"TargetCluster\":\"0xe803\"}")

	/*
	* 3. Target is a merged docs with no new change.
	 */
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), nil, []byte("{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster3\":\"0x2\"}"))
	assert.Nil(err)

	pv, mv, err = targetMeta.UpdateMetaForSetBack()
	assert.Nil(err)
	assert.Nil(pv)
	assert.Equal(string(mv), "{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster3\":\"0x2\"}")

	/*
	* 4. Target is a merged docs with new change.
	 */
	targetMeta, err = NewMetadataForTest(key, targetClusterId, cv+1000, 1, []byte("0x0b0085b25e8d1416"), []byte("Cluster4"), []byte("0x0b0085b25e8d1416"), []byte("{\"Cluster2\":\"0x1500f8da4d881416\"}"), []byte("{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster3\":\"0x2\"}"))
	assert.Nil(err)

	pv, mv, err = targetMeta.UpdateMetaForSetBack()
	assert.Nil(err)
	assert.Nil(mv)
	assert.Equal(string(pv), "{\"Cluster1\":\"0x0000f8da4d881416\",\"Cluster2\":\"0x15\",\"Cluster3\":\"0x0b\",\"TargetCluster\":\"0xd3038dd71005\"}")
}

// Heap's Algorithm: generates all possible permutations for a given array, with minimal movements.
func Permutations(arr []int) [][]int {
	var helper func([]int, int)
	res := [][]int{}

	helper = func(arr []int, n int) {
		if n == 1 {
			tmp := make([]int, len(arr))
			copy(tmp, arr)
			res = append(res, tmp)
		} else {
			for i := 0; i < n; i++ {
				helper(arr, n-1)
				if n%2 == 1 {
					arr[i], arr[n-1] = arr[n-1], arr[i]
				} else {
					arr[0], arr[n-1] = arr[n-1], arr[0]
				}
			}
		}
	}
	helper(arr, len(arr))
	return res
}
func TestParseHlvFromMCRequest(t *testing.T) {
	a := assert.New(t)

	uncompress := func(req *base.WrappedMCRequest) error { return nil }
	req := &base.WrappedMCRequest{Req: &gomemcached.MCRequest{Key: []byte("test")}}
	req.Req.DataType = base.XattrDataType | base.JSONDataType
	req.Req.Extras = make([]byte, 24)
	var CAS uint64 = 27382937393749
	binary.BigEndian.PutUint64(req.Req.Extras[16:24], uint64(CAS))
	srcVal := "asqsqqwasass"
	src := fmt.Sprintf("\"%s\":\"%s\"", HLV_SRC_FIELD, srcVal)
	var verVal uint64 = 6172839283
	ver := fmt.Sprintf("\"%s\":\"%s\"", HLV_VER_FIELD, base.Uint64ToHexLittleEndian(verVal))
	cvCasVal := verVal
	cvCas := fmt.Sprintf("\"%s\":\"%s\"", HLV_CVCAS_FIELD, base.Uint64ToHexLittleEndian(cvCasVal))
	pv1Val := verVal - 100
	var pv2Val uint64 = 10
	pv1Key := "ksajxsjx"
	pv2Key := "ksajxsjy"
	PV := fmt.Sprintf("\"%s\":{\"%s\":\"%s\",\"%s\":\"%s\"}", HLV_PV_FIELD, pv1Key, base.Uint64ToHexLittleEndianAndStrip0s(pv1Val), pv2Key, base.Uint64ToHexLittleEndianAndStrip0s(pv2Val))
	vv1s := []string{
		"{" + src + "," + ver + "," + cvCas + "}",
		"{" + src + "," + cvCas + "," + ver + "}",
		"{" + ver + "," + cvCas + "," + src + "}",
		"{" + ver + "," + src + "," + cvCas + "}",
		"{" + cvCas + "," + src + "," + ver + "}",
		"{" + cvCas + "," + ver + "," + src + "}",
	}
	str := "\"foo\":\"bar\""
	num := "\"prevRev\":12345678901234567890"
	objStr := "\"lorem\":\"ipsum\""
	objNum := "\"foo\":12345"
	objObj1 := "\"obj1\":{\"def\":\"geh\",\"num\":9876}"
	objObj2 := "\"obj2\":{\"num\":9876,\"def\":\"geh\"}"
	importCasVal := verVal
	importCAS := fmt.Sprintf("\"%s\":\"%s\"", base.IMPORTCAS, base.Uint64ToHexLittleEndian(importCasVal))
	var pRevIdVal uint64 = 314231423142
	pRevId := fmt.Sprintf("\"%s\":\"%v\"", base.PREVIOUSREV, pRevIdVal)
	doc := "{\"foo\":\"bar\"}"

	for _, vv1 := range vv1s {
		// 1. _vv.src and _vv.ver, no _vv.pv, no _mou
		body := make([]byte, 4+4+2+len(base.XATTR_HLV)+len(vv1)+len(doc))
		pos := 0
		binary.BigEndian.PutUint32(body[pos:pos+4], uint32(len(base.XATTR_HLV)+len(vv1)+2+4))
		pos += 4
		binary.BigEndian.PutUint32(body[pos:pos+4], uint32(len(base.XATTR_HLV)+len(vv1)+2))
		pos += 4
		copy(body[pos:pos+len(base.XATTR_HLV)], []byte(base.XATTR_HLV))
		pos += len(base.XATTR_HLV)
		body[pos] = '\x00'
		pos++
		copy(body[pos:pos+len(vv1)], []byte(vv1))
		pos += len(vv1)
		body[pos] = '\x00'
		pos++
		copy(body[pos:pos+len(doc)], []byte(doc))
		pos += len(doc)
		req.Req.Body = body[:pos]
		cas, cvCAS, cvSrc, cvVer, pv, mv, importCas, pRev, err := getHlvFromMCRequest(req, uncompress)
		a.Nil(err)
		a.Equal(cas, CAS)
		a.Equal(cvCAS, cvCasVal)
		a.Equal(string(cvSrc), srcVal)
		a.Equal(cvVer, verVal)
		a.Equal(len(mv), 0)
		a.Equal(len(pv), 0)
		a.Equal(importCas, uint64(0))
		a.Equal(pRev, uint64(0))

		// 2. _vv.src, _vv.ver, _vv.pv, no _mou
		vv2s := []string{
			"{" + vv1[1:len(vv1)-1] + "," + PV + "}",
			"{" + PV + "," + vv1[1:len(vv1)-1] + "}",
		}

		for _, vv2 := range vv2s {
			body = make([]byte, 4+4+2+len(base.XATTR_HLV)+len(vv2)+len(doc))
			pos = 0
			binary.BigEndian.PutUint32(body[pos:pos+4], uint32(len(base.XATTR_HLV)+len(vv2)+2+4))
			pos += 4
			binary.BigEndian.PutUint32(body[pos:pos+4], uint32(len(base.XATTR_HLV)+len(vv2)+2))
			pos += 4
			copy(body[pos:pos+len(base.XATTR_HLV)], []byte(base.XATTR_HLV))
			pos += len(base.XATTR_HLV)
			body[pos] = '\x00'
			pos++
			copy(body[pos:pos+len(vv2)], []byte(vv2))
			pos += len(vv2)
			body[pos] = '\x00'
			pos++
			copy(body[pos:pos+len(doc)], []byte(doc))
			pos += len(doc)
			req.Req.Body = body[:pos]
			cas, cvCAS, cvSrc, cvVer, pv, mv, importCas, pRev, err = getHlvFromMCRequest(req, uncompress)
			a.Nil(err)
			a.Equal(cas, CAS)
			a.Equal(cvCAS, cvCasVal)
			a.Equal(string(cvSrc), srcVal)
			a.Equal(cvVer, verVal)
			a.Equal(len(mv), 0)
			a.Equal(pv[hlv.DocumentSourceId(pv1Key)], pv1Val)
			a.Equal(pv[hlv.DocumentSourceId(pv2Key)], pv2Val+pv1Val)
			a.Equal(importCas, uint64(0))
			a.Equal(pRev, uint64(0))

			// 3. _vv.src, _vv.ver, _vv.pv, and _mou.importCAS
			objs := []string{}
			possibleObjFields := []string{objStr, objNum, objObj1, objObj2}
			objIdxs := []int{0, 1, 2, 3}
			objPerms := Permutations(objIdxs)
			for _, perm := range objPerms {
				input := ""
				for _, idx := range perm {
					input = input + possibleObjFields[idx] + ","
				}
				// remove , at the end and add { and }
				input = "{" + input[0:len(input)-1] + "}"
				objs = append(objs, "\"obj\":"+input)
			}

			// test all possible permutation arrangements of mou possible items
			for _, obj := range objs {
				possibleFields := []string{str, num, obj, importCAS, pRevId}
				idxs := []int{0, 1, 2, 3, 4}
				perms := Permutations(idxs)
				for _, perm := range perms {
					mou := ""
					for _, idx := range perm {
						mou = mou + possibleFields[idx] + ","
					}
					// remove , at the end and add { and }
					mou = "{" + mou[0:len(mou)-1] + "}"

					vvs := []string{
						vv1, vv2,
					}
					for _, vv := range vvs {
						// 3a. _mou first
						body = make([]byte, 4+4+2+len(base.XATTR_HLV)+len(vv)+4+2+len(mou)+len(base.XATTR_MOU)+len(doc))
						pos = 0
						binary.BigEndian.PutUint32(body[pos:pos+4], uint32(4+2+len(base.XATTR_HLV)+len(vv)+4+2+len(base.XATTR_MOU)+len(mou)))
						pos += 4
						binary.BigEndian.PutUint32(body[pos:pos+4], uint32(len(base.XATTR_MOU)+len(mou)+2))
						pos += 4
						copy(body[pos:pos+len(base.XATTR_MOU)], []byte(base.XATTR_MOU))
						pos += len(base.XATTR_MOU)
						body[pos] = '\x00'
						pos++
						copy(body[pos:pos+len(mou)], []byte(mou))
						pos += len(mou)
						body[pos] = '\x00'
						pos++
						binary.BigEndian.PutUint32(body[pos:pos+4], uint32(len(base.XATTR_HLV)+len(vv)+2))
						pos += 4
						copy(body[pos:pos+len(base.XATTR_HLV)], []byte(base.XATTR_HLV))
						pos += len(base.XATTR_HLV)
						body[pos] = '\x00'
						pos++
						copy(body[pos:pos+len(vv)], []byte(vv))
						pos += len(vv)
						body[pos] = '\x00'
						pos++
						copy(body[pos:pos+len(doc)], []byte(doc))
						pos += len(doc)
						req.Req.Body = body[:pos]
						cas, cvCAS, cvSrc, cvVer, pv, mv, importCas, pRev, err = getHlvFromMCRequest(req, uncompress)
						a.Nil(err)
						a.Equal(cas, CAS)
						a.Equal(cvCAS, cvCasVal)
						a.Equal(string(cvSrc), srcVal)
						a.Equal(cvVer, verVal)
						a.Equal(len(mv), 0)
						if strings.Contains(vv, HLV_PV_FIELD) {
							a.Equal(pv[hlv.DocumentSourceId(pv1Key)], pv1Val)
							a.Equal(pv[hlv.DocumentSourceId(pv2Key)], pv2Val+pv1Val)
						} else {
							a.Equal(len(pv), 0)
						}
						a.Equal(importCas, importCasVal)
						a.Equal(pRev, pRevIdVal)

						// 3b. _vv first
						body = make([]byte, 4+4+2+len(base.XATTR_HLV)+len(vv)+4+2+len(mou)+len(base.XATTR_MOU)+len(doc))
						pos = 0
						binary.BigEndian.PutUint32(body[pos:pos+4], uint32(4+2+len(base.XATTR_HLV)+len(vv)+4+2+len(base.XATTR_MOU)+len(mou)))
						pos += 4
						binary.BigEndian.PutUint32(body[pos:pos+4], uint32(len(base.XATTR_HLV)+len(vv)+2))
						pos += 4
						copy(body[pos:pos+len(base.XATTR_HLV)], []byte(base.XATTR_HLV))
						pos += len(base.XATTR_HLV)
						body[pos] = '\x00'
						pos++
						copy(body[pos:pos+len(vv)], []byte(vv))
						pos += len(vv)
						body[pos] = '\x00'
						pos++
						binary.BigEndian.PutUint32(body[pos:pos+4], uint32(len(base.XATTR_MOU)+len(mou)+2))
						pos += 4
						copy(body[pos:pos+len(base.XATTR_MOU)], []byte(base.XATTR_MOU))
						pos += len(base.XATTR_MOU)
						body[pos] = '\x00'
						pos++
						copy(body[pos:pos+len(mou)], []byte(mou))
						pos += len(mou)
						body[pos] = '\x00'
						pos++
						copy(body[pos:pos+len(doc)], []byte(doc))
						pos += len(doc)
						req.Req.Body = body[:pos]
						cas, cvCAS, cvSrc, cvVer, pv, mv, importCas, pRev, err = getHlvFromMCRequest(req, uncompress)
						a.Nil(err)
						a.Equal(cas, CAS)
						a.Equal(cvCAS, cvCasVal)
						a.Equal(string(cvSrc), srcVal)
						a.Equal(cvVer, verVal)
						a.Equal(len(mv), 0)
						if strings.Contains(vv, HLV_PV_FIELD) {
							a.Equal(pv[hlv.DocumentSourceId(pv1Key)], pv1Val)
							a.Equal(pv[hlv.DocumentSourceId(pv2Key)], pv2Val+pv1Val)
						} else {
							a.Equal(len(pv), 0)
						}
						a.Equal(importCas, importCasVal)
						a.Equal(pRev, pRevIdVal)
					}
				}
			}
		}
	}
}

func TestXattrToVersionMap(t *testing.T) {
	a := assert.New(t)

	// deltas
	var pv1Val uint64 = 10000
	var pv2Val uint64 = 1000
	var pv3Val uint64 = 100
	// source
	pv1Key := "ksajxsjx"
	pv2Key := "ksajxsjy"
	pv3Key := "ksajxsjz"

	PV := fmt.Sprintf("{\"%s\":\"%s\",\"%s\":\"%s\",\"%s\":\"%s\"}", pv1Key, base.Uint64ToHexLittleEndianAndStrip0s(pv1Val), pv2Key, base.Uint64ToHexLittleEndianAndStrip0s(pv2Val), pv3Key, base.Uint64ToHexLittleEndianAndStrip0s(pv3Val))
	pv, err := xattrVVtoDeltas([]byte(PV))
	a.Nil(err)
	a.Equal(pv[hlv.DocumentSourceId(pv1Key)], pv1Val)
	a.Equal(pv[hlv.DocumentSourceId(pv2Key)], pv2Val+pv1Val)
	a.Equal(pv[hlv.DocumentSourceId(pv3Key)], pv3Val+pv2Val+pv1Val)

	PV = fmt.Sprintf("{\"%s\":\"%s\",\"%s\":\"%s\"}", pv1Key, base.Uint64ToHexLittleEndianAndStrip0s(pv1Val), pv2Key, base.Uint64ToHexLittleEndianAndStrip0s(pv2Val))
	pv, err = xattrVVtoDeltas([]byte(PV))
	a.Nil(err)
	a.Equal(pv[hlv.DocumentSourceId(pv1Key)], pv1Val)
	a.Equal(pv[hlv.DocumentSourceId(pv2Key)], pv2Val+pv1Val)

	PV = fmt.Sprintf("{\"%s\":\"%s\"}", pv1Key, base.Uint64ToHexLittleEndianAndStrip0s(pv1Val))
	pv, err = xattrVVtoDeltas([]byte(PV))
	a.Nil(err)
	a.Equal(pv[hlv.DocumentSourceId(pv1Key)], pv1Val)

	PV = "{}"
	pv, err = xattrVVtoDeltas([]byte(PV))
	a.Nil(err)
	a.Equal(len(pv), 0)

	PV = ""
	pv, err = xattrVVtoDeltas([]byte(PV))
	a.Nil(err)
	a.Equal(len(pv), 0)
}

func Test_MouNestedXattrParsing(t *testing.T) {
	a := assert.New(t)

	// As of the date of writing this test, _mou will contain the following 3 fields.
	// Add more and test them here when mobile adds more.
	pCAS := "\"pCAS\":\"0x1234567890123456\""
	importCas := fmt.Sprintf("\"%s\":\"0x1234567890123456\"", base.IMPORTCAS)
	pRev := fmt.Sprintf("\"%s\":\"1234567890123456\"", base.PREVIOUSREV)

	// 1. test all possible permutation arrangements of mou xattr's fields
	possibleFields := []string{pCAS, importCas, pRev}
	idxs := []int{0, 1, 2}
	perms := Permutations(idxs)
	for _, perm := range perms {
		input := ""
		output := ""
		for _, idx := range perm {
			input = input + possibleFields[idx] + ","
			if idx != len(possibleFields)-1 && idx != len(possibleFields)-2 {
				// if importCas or pRev - don't add in output
				output = output + possibleFields[idx] + ","
			}
		}
		// remove , at the end and add { and }
		input = "{" + input[0:len(input)-1] + "}"
		output = "{" + output[0:len(output)-1] + "}"

		dst := make([]byte, len(input))
		removed := make(map[string][]byte)
		dstLen, removedLen, atleastOneField, err := gojsonsm.MatchAndRemoveItemsFromJsonObject([]byte(input), base.MouXattrValuesForCR, dst, removed)
		dst = dst[:dstLen]
		a.Nil(err)
		a.Equal(removedLen, len(base.MouXattrValuesForCR))
		a.Equal(atleastOneField, true)
		a.Equal(bytes.Equal(dst, []byte(output)), true)
	}

	// 2. test corner cases
	tests := []struct {
		name                       string
		before, expectedAfter      []byte
		expectedAtleastOnFieldLeft bool
		expectedRemoved            int
	}{
		{
			name:          "empty mou",
			before:        []byte("{}"),
			expectedAfter: []byte("{}"),
		},
		{
			name:            "only importCAS and pRev",
			before:          []byte("{\"pRev\":\"12345\",\"importCAS\":\"0x1234567890123456\"}"),
			expectedAfter:   []byte("{}"),
			expectedRemoved: 2,
		},
		{
			name:            "only importCAS",
			before:          []byte("{\"importCAS\":\"0x1234567890123456\"}"),
			expectedAfter:   []byte("{}"),
			expectedRemoved: 1,
		},
		{
			name:                       "only importCas and pCas",
			before:                     []byte("{\"pCAS\":\"0x1234567890123456\",\"importCAS\":\"0x1234567890123456\"}"),
			expectedAfter:              []byte("{\"pCAS\":\"0x1234567890123456\"}"),
			expectedRemoved:            1,
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "only pCas",
			before:                     []byte("{\"pCAS\":\"0x1234567890123456\"}"),
			expectedAfter:              []byte("{\"pCAS\":\"0x1234567890123456\"}"),
			expectedRemoved:            0,
			expectedAtleastOnFieldLeft: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := make([]byte, len(tt.before))
			removed := make(map[string][]byte)
			dstLen, removedLen, atleastOneField, err := gojsonsm.MatchAndRemoveItemsFromJsonObject([]byte(tt.before), base.MouXattrValuesForCR, dst, removed)
			dst = dst[:dstLen]
			a.Nil(err)
			a.Equal(removedLen, tt.expectedRemoved)
			a.Equal(atleastOneField, tt.expectedAtleastOnFieldLeft)
			a.Equal(bytes.Equal(dst, []byte(tt.expectedAfter)), true)
		})
	}
}
