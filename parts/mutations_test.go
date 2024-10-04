// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package parts

import (
	"crypto/x509"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v9"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/crMeta"
	utilsReal "github.com/couchbase/goxdcr/v8/utils"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
)

// The tests in this file validates the different types of mutations processed by xmem.
// To execute the test, one has to follow these steps:
//
// STEP 1) Setup a cluster_run node on 9001 port with a bucket B2.
// One can use: tools/provision_oneCluster.sh --bucket=B2 --cluster=9001
//
// STEP 2) Manually inject the "testMcRequest" function in the right place of xmem.
// Eg: testMcRequest() placed after xmem.buf.enSlot(item)
//
// STEP 3) Manually inject the "testMcResponse" function in the right place of xmem.
// Eg: testMcResponse() placed after xmem.readFromClient(xmem.client_for_setMeta, true)
//
// STEP 4) Run the Mutations tests using the following command: (use -v for verbose output on stdout)
/*
	(if necessary) go clean -cache
	go test -timeout 12000s -run ^Test_LegacyMutations$ github.com/couchbase/goxdcr/v8/parts -failfast
	go test -timeout 12000s -run ^Test_ECCVOnlyMutations$ github.com/couchbase/goxdcr/v8/parts -failfast
	go test -timeout 12000s -run ^Test_MobileOnlyMutations$ github.com/couchbase/goxdcr/v8/parts -failfast
	go test -timeout 12000s -run ^Test_ECCVAndMobileMutations$ github.com/couchbase/goxdcr/v8/parts -failfast
*/
//
// STEP 5) Run the Mutations CR tests using the following command: (use -v for verbose output on stdout)
/*
	(if necessary) go clean -cache
	go test -timeout 12000s -run ^Test_LegacyMutationsCRTest$ github.com/couchbase/goxdcr/v8/parts -failfast
	go test -timeout 12000s -run ^Test_ECCVOnlyMutationsCRTestWithCasGreaterThanMaxCas$ github.com/couchbase/goxdcr/v8/parts -failfast
	go test -timeout 12000s -run ^Test_ECCVOnlyMutationsCRTestWithCasLessThanMaxCas$ github.com/couchbase/goxdcr/v8/parts -failfast
	go test -timeout 12000s -run ^Test_MobileOnlyMutationsCRTest$ github.com/couchbase/goxdcr/v8/parts -failfast
	go test -timeout 12000s -run ^Test_ECCVAndMobileOnlyMutationsCRTestWithCasGreaterThanMaxCas$ github.com/couchbase/goxdcr/v8/parts -failfast
	go test -timeout 12000s -run ^Test_ECCVAndMobileOnlyMutationsCRTestWithCasLessThanMaxCas$ github.com/couchbase/goxdcr/v8/parts -failfast
*/
//
// STEP 6) Manually delete the injected TestMcRequest and TestMcResponse function calls from xmem.
// NOTE: all these tests can be run concurrently.

var targetSync string = `{"cas":"0x1234"}` // _sync for target doc
var sourceSync string = `{"cas":"0x5678"}` // _sync for source doc
var userXattrKey string = "foo"            // used as source doc xattr
var userXattr string = `{"foo":"bar"}`     // value for userXattrKey
var targetDoc string = `{"target":"doc"}`  // target doc value
var xattrK1 string = userXattrKey          // used as target doc xattr, same as source doc xattr
var xattrV1 string = `{}`                  // value for xattrK1
var xattrK2 string = `bar`                 // used as target doc xattr. Assumed to always exist only on target doc.
var xattrV2 string = `"blah"`              // value for xattrK2
var NumVbs int = 128

var crc32tab = []uint32{
	0x00000000, 0x77073096, 0xee0e612c, 0x990951ba,
	0x076dc419, 0x706af48f, 0xe963a535, 0x9e6495a3,
	0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
	0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91,
	0x1db71064, 0x6ab020f2, 0xf3b97148, 0x84be41de,
	0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
	0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec,
	0x14015c4f, 0x63066cd9, 0xfa0f3d63, 0x8d080df5,
	0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
	0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b,
	0x35b5a8fa, 0x42b2986c, 0xdbbbc9d6, 0xacbcf940,
	0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
	0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116,
	0x21b4f4b5, 0x56b3c423, 0xcfba9599, 0xb8bda50f,
	0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
	0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d,
	0x76dc4190, 0x01db7106, 0x98d220bc, 0xefd5102a,
	0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
	0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818,
	0x7f6a0dbb, 0x086d3d2d, 0x91646c97, 0xe6635c01,
	0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
	0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457,
	0x65b0d9c6, 0x12b7e950, 0x8bbeb8ea, 0xfcb9887c,
	0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
	0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2,
	0x4adfa541, 0x3dd895d7, 0xa4d1c46d, 0xd3d6f4fb,
	0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
	0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9,
	0x5005713c, 0x270241aa, 0xbe0b1010, 0xc90c2086,
	0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
	0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4,
	0x59b33d17, 0x2eb40d81, 0xb7bd5c3b, 0xc0ba6cad,
	0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
	0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683,
	0xe3630b12, 0x94643b84, 0x0d6d6a3e, 0x7a6a5aa8,
	0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
	0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe,
	0xf762575d, 0x806567cb, 0x196c3671, 0x6e6b06e7,
	0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
	0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5,
	0xd6d6a3e8, 0xa1d1937e, 0x38d8c2c4, 0x4fdff252,
	0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
	0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60,
	0xdf60efc3, 0xa867df55, 0x316e8eef, 0x4669be79,
	0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
	0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f,
	0xc5ba3bbe, 0xb2bd0b28, 0x2bb45a92, 0x5cb36a04,
	0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
	0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a,
	0x9c0906a9, 0xeb0e363f, 0x72076785, 0x05005713,
	0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
	0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21,
	0x86d3d2d4, 0xf1d4e242, 0x68ddb3f8, 0x1fda836e,
	0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
	0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c,
	0x8f659eff, 0xf862ae69, 0x616bffd3, 0x166ccf45,
	0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
	0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db,
	0xaed16a4a, 0xd9d65adc, 0x40df0b66, 0x37d83bf0,
	0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
	0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6,
	0xbad03605, 0xcdd70693, 0x54de5729, 0x23d967bf,
	0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
	0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d}

func cbCrc(key []byte) uint32 {
	crc := uint32(0xffffffff)
	for x := 0; x < len(key); x++ {
		crc = (crc >> 8) ^ crc32tab[(uint64(crc)^uint64(key[x]))&0xff]
	}
	return (^crc) >> 16
}

func getVBucketNo(key string, vbCount int) uint16 {
	return uint16(cbCrc([]byte(key)) % uint32(vbCount))
}

func setMeta(kvAddr, bucketName string, key, value []byte, datatype uint8, cas, revID uint64, lww bool) error {
	tgtAgentConfig := &gocbcore.AgentConfig{
		BucketName:        bucketName,
		UserAgent:         "XmemTestSetMeta",
		UseTLS:            false,
		TLSRootCAProvider: func() *x509.CertPool { return nil },
		UseCollections:    true,
		AuthMechanisms:    []gocbcore.AuthMechanism{gocbcore.ScramSha256AuthMechanism},
		Auth:              gocbcore.PasswordAuthProvider{Username: username, Password: password},
		MemdAddrs:         []string{kvAddr},
	}

	ch := make(chan error)
	tgtAgent, closeFunc := createSDKAgent(tgtAgentConfig)
	defer closeFunc()
	if tgtAgent == nil {
		return fmt.Errorf("tgtAgent is nil")
	}

	var options uint32
	if lww {
		options |= base.FORCE_ACCEPT_WITH_META_OPS
	}

	_, err := tgtAgent.SetMeta(gocbcore.SetMetaOptions{
		Key:            key,
		Value:          value,
		CollectionName: "_default",
		ScopeName:      "_default",
		Cas:            gocbcore.Cas(cas),
		RevNo:          revID,
		Options:        options,
		Datatype:       datatype,
	}, func(smr *gocbcore.SetMetaResult, err error) {
		ch <- err
	})
	if err != nil {
		return fmt.Errorf("tgtAgent.SetMeta, err=%v", err)
	}
	err = <-ch
	if err != nil {
		return fmt.Errorf("tgtAgent.SetMeta return, err=%v", err)
	}

	return nil
}

func createTombstone(value []byte, datatype uint8) ([]byte, uint8, error) {
	if datatype&base.SnappyDataType > 0 {
		len, err := snappy.DecodedLen(value)
		if err != nil {
			return nil, 0, err
		}
		val := make([]byte, int(len))
		value, err = snappy.Decode(val, value)
		if err != nil {
			return nil, 0, err
		}

		datatype &= ^base.SnappyDataType
	}

	if datatype&base.JSONDataType > 0 {
		datatype &= ^base.JSONDataType
	}

	// tombstones have xattrs,
	// but not doc body.
	var tombstone []byte
	if datatype&base.XattrDataType > 0 {
		xattrSize, err := base.GetXattrSize(value)
		if err != nil {
			return nil, 0, err
		}

		tombstone = value[:4+xattrSize]
	}

	return tombstone, datatype, nil
}

// given any value, this routine will strip off the body and preserve the xattrs passed in
func delMeta(kvAddr, bucketName string, key, value []byte, datatype uint8, cas, revID uint64, lww bool) error {
	tgtAgentConfig := &gocbcore.AgentConfig{
		BucketName:        bucketName,
		UserAgent:         "XmemTestDelMeta",
		UseTLS:            false,
		TLSRootCAProvider: func() *x509.CertPool { return nil },
		UseCollections:    true,
		AuthMechanisms:    []gocbcore.AuthMechanism{gocbcore.ScramSha256AuthMechanism},
		Auth:              gocbcore.PasswordAuthProvider{Username: username, Password: password},
		MemdAddrs:         []string{kvAddr},
	}

	ch := make(chan error)
	tgtAgent, closeFunc := createSDKAgent(tgtAgentConfig)
	defer closeFunc()
	if tgtAgent == nil {
		return fmt.Errorf("DelTgtAgent is nil")
	}

	var options uint32
	if lww {
		options |= base.FORCE_ACCEPT_WITH_META_OPS
	}

	value, datatype, err = createTombstone(value, datatype)
	if err != nil {
		return err
	}

	_, err := tgtAgent.DeleteMeta(gocbcore.DeleteMetaOptions{
		Key:            key,
		Value:          value,
		CollectionName: "_default",
		ScopeName:      "_default",
		Cas:            gocbcore.Cas(cas),
		RevNo:          revID,
		Options:        options,
		Datatype:       datatype,
	}, func(smr *gocbcore.DeleteMetaResult, err error) {
		ch <- err
	})
	if err != nil {
		return fmt.Errorf("tgtAgent.DelMeta, err=%v", err)
	}
	err = <-ch
	if err != nil {
		return fmt.Errorf("tgtAgent.DelMeta return, err=%v", err)
	}

	return nil
}

func generateHlv(cas uint64, src string) []byte {
	return []byte(fmt.Sprintf(`{"%s":"%s","%s":"%s","%s":"%s"}`,
		crMeta.HLV_CVCAS_FIELD, base.Uint64ToHexLittleEndian(cas),
		crMeta.HLV_SRC_FIELD, src,
		crMeta.HLV_VER_FIELD, base.Uint64ToHexLittleEndian(cas)))
}

func generateHlvWithPv(cas uint64, src string) []byte {
	return []byte(fmt.Sprintf(`{"%s":"%s","%s":"%s","%s":"%s","%s":["1234567890123456@oldSrc"]}`,
		crMeta.HLV_CVCAS_FIELD, base.Uint64ToHexLittleEndian(cas),
		crMeta.HLV_SRC_FIELD, src,
		crMeta.HLV_VER_FIELD, base.Uint64ToHexLittleEndian(cas),
		crMeta.HLV_PV_FIELD))
}

func generateSync() []byte {
	return []byte(targetSync)
}

func generateMou(cas, rev uint64) []byte {
	return []byte(fmt.Sprintf(`{"%s":"%s","%s":"%v"}`,
		base.IMPORTCAS, base.Uint64ToHexLittleEndian(cas),
		base.PREVIOUSREV, rev))
}

func generateBody(hlv, mou, sync, xattrKey1, xattrVal1, xattrKey2, xattrVal2, doc []byte) ([]byte, uint8, error) {
	bodyLen := 4 + 4 + len(base.XATTR_HLV) + 1 + len(hlv) + 1 +
		4 + len(base.XATTR_MOBILE) + 1 + len(sync) + 1 +
		4 + len(base.XATTR_MOU) + 1 + len(mou) + 1 +
		4 + len(xattrKey1) + 1 + len(xattrVal1) + 1 +
		4 + len(xattrKey2) + 1 + len(xattrVal2) + 1 +
		len(doc)
	body := make([]byte, bodyLen)
	comp := base.NewXattrComposer(body)

	if len(hlv) > 0 {
		err := comp.WriteKV([]byte(base.XATTR_HLV), hlv)
		if err != nil {
			return nil, 0, err
		}
	}

	if len(sync) > 0 {
		err := comp.WriteKV([]byte(base.XATTR_MOBILE), sync)
		if err != nil {
			return nil, 0, err
		}
	}

	if len(mou) > 0 {
		err := comp.WriteKV([]byte(base.XATTR_MOU), mou)
		if err != nil {
			return nil, 0, err
		}
	}

	if len(xattrKey1) > 0 && len(xattrVal1) > 0 {
		err := comp.WriteKV(xattrKey1, xattrVal1)
		if err != nil {
			return nil, 0, err
		}
	}

	if len(xattrKey2) > 0 && len(xattrVal2) > 0 {
		err := comp.WriteKV(xattrKey2, xattrVal2)
		if err != nil {
			return nil, 0, err
		}
	}

	body, xattr := comp.FinishAndAppendDocValue(doc, nil, nil)
	var datatype uint8
	if xattr {
		datatype = base.XattrDataType
	}

	return body, datatype, nil
}

func getAllTargetBodys(cas, rev uint64, src string) ([][]byte, []uint8, []string, [][]byte, [][]byte, [][]byte, error) {
	var targets [][]byte
	var datatypes []uint8
	var hlvs, syncs, mous [][]byte
	var desc []string

	sync := generateSync()
	doc := []byte(targetDoc)

	// import mutation + sync
	desc = append(desc, "import mutation + sync")

	hlv := generateHlv(cas-1, src)
	mou := generateMou(cas, rev-1)

	body, datatype, err := generateBody(hlv, mou, sync, []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	datatypes = append(datatypes, datatype)
	targets = append(targets, body)

	hlvs = append(hlvs, hlv)
	syncs = append(syncs, sync)
	mous = append(mous, mou)

	// non-import mutation + sync
	desc = append(desc, "non-import mutation + sync")

	hlv = generateHlv(cas-2, src)
	mou = generateMou(cas-1, rev-2)

	body, datatype, err = generateBody(hlv, mou, sync, []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	datatypes = append(datatypes, datatype)
	targets = append(targets, body)

	hlvs = append(hlvs, hlv)
	syncs = append(syncs, sync)
	mous = append(mous, mou)

	// updated hlv
	desc = append(desc, "updated hlv + no sync")

	hlv = generateHlv(cas, src)

	body, datatype, err = generateBody(hlv, nil, nil, []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	datatypes = append(datatypes, datatype)
	targets = append(targets, body)

	hlvs = append(hlvs, hlv)
	syncs = append(syncs, nil)
	mous = append(mous, nil)

	// outdated hlv
	desc = append(desc, "outdated hlv + no sync")

	hlv = generateHlv(cas-1, src)

	body, datatype, err = generateBody(hlv, nil, nil, []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	datatypes = append(datatypes, datatype)
	targets = append(targets, body)

	hlvs = append(hlvs, hlv)
	syncs = append(syncs, nil)
	mous = append(mous, nil)

	// updated hlv + sync
	desc = append(desc, "updated hlv + sync")

	hlv = generateHlv(cas, src)

	body, datatype, err = generateBody(hlv, nil, sync, []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	datatypes = append(datatypes, datatype)
	targets = append(targets, body)

	hlvs = append(hlvs, hlv)
	syncs = append(syncs, sync)
	mous = append(mous, nil)

	// outdated hlv + sync
	desc = append(desc, "outdated hlv + sync")

	hlv = generateHlv(cas-1, src)

	body, datatype, err = generateBody(hlv, nil, sync, []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	datatypes = append(datatypes, datatype)
	targets = append(targets, body)

	hlvs = append(hlvs, hlv)
	syncs = append(syncs, sync)
	mous = append(mous, nil)

	// only sync
	desc = append(desc, "only sync")

	body, datatype, err = generateBody(nil, nil, sync, []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	datatypes = append(datatypes, datatype)
	targets = append(targets, body)

	hlvs = append(hlvs, nil)
	syncs = append(syncs, sync)
	mous = append(mous, nil)

	// only user xattrs
	desc = append(desc, "only user xattrs")

	body, datatype, err = generateBody(nil, nil, nil, []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	datatypes = append(datatypes, datatype)
	targets = append(targets, body)

	hlvs = append(hlvs, nil)
	syncs = append(syncs, nil)
	mous = append(mous, nil)

	// no xattrs
	desc = append(desc, "no xattrs")

	body, datatype, err = generateBody(nil, nil, nil, nil, nil, nil, nil, doc)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	datatypes = append(datatypes, datatype)
	targets = append(targets, body)

	hlvs = append(hlvs, nil)
	syncs = append(syncs, nil)
	mous = append(mous, nil)

	return targets, datatypes, desc, hlvs, syncs, mous, nil
}

// given document body, extracts and returns hlv, mou and sync from xattr section
func getHlvMouSyncFromDocBody(body []byte, datatype uint8) ([]byte, []byte, []byte, error) {
	var hlv, sync, mou []byte
	if datatype&base.XattrDataType == 0 {
		return hlv, mou, sync, nil
	}

	if datatype&base.SnappyDataType > 0 {
		len, err := snappy.DecodedLen(body)
		if err != nil {
			return hlv, mou, sync, err
		}
		val := make([]byte, int(len))
		body, err = snappy.Decode(val, body)
		if err != nil {
			return hlv, mou, sync, err
		}

		datatype &= ^base.SnappyDataType
	}

	it, err := base.NewXattrIterator(body)
	if err != nil {
		return hlv, mou, sync, err
	}

	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return hlv, mou, sync, err
		}

		if base.Equals(k, base.XATTR_HLV) {
			hlv = v
		} else if base.Equals(k, base.XATTR_MOBILE) {
			sync = v
		} else if base.Equals(k, base.XATTR_MOU) {
			mou = v
		}
	}

	return hlv, mou, sync, nil
}

type mutationTC struct {
	name string
	// composed MCRequest in xmem to test by TestMcRequest
	reqT reqTestParams
	// received MCResponse in xmem to test by TestMcResponse
	respT       respTestParams
	getUprEvent func() (*mcc.UprEvent, error)
	// If mobile replication setting is turned on
	mobile bool
	// If ECCV bucket setting is turned on
	eccv bool
	// uprEvent.Cas will be set as uprEvent.Cas + casDelta
	casDelta int
	// max_cas for the vbucket will be set as uprEvent.Cas + max_cas_delta
	max_cas_delta int
	// xattr content of the target document after replication was complete
	hlv          string
	mou          string
	sync         string
	userXattrKey string
	userXattrVal string
	// if hlv in source document was outdated.
	// if it is true, hlv above is expected to have updated hlv and
	// mou to be the updated mou.
	hlvOutdated bool
	// a target document with the same key as source document is created before executing the test
	targetDocExists bool
	// if targetDocExists above, the body to be written before executing the test,
	// using SetWithMeta with the following metadata
	targetBody     []byte
	targetDatatype uint8
	targetCas      uint64
	targetRevId    uint64
	// write a mutation on target after conflict resolution, but before source mutation is replicated
	targetCasChanged bool
	// target cas changed after conflict resolution, but before source mutation is replicated
	casLockingFailure bool
	// if target doc wins CR
	targetWins bool
}

func (test mutationTC) executeTest(assert assert.Assertions, bucketName string, xmemBucket base.ConflictResolutionMode, cluster *gocb.Cluster) {
	_, err1 := strconv.Atoi(test.name)
	_, err2 := strconv.Atoi(test.name[:len(test.name)-1])
	if err1 == nil || err2 == nil {
		fmt.Printf("=============== Test: %s ===============\n", test.name)
		defer fmt.Printf("=============== Done Test: %s ===============\n", test.name)
	}

	uprEvent, err := test.getUprEvent()
	assert.Nil(err)
	uprEvent.Cas = uprEvent.Cas + uint64(test.casDelta)

	valueCopy := uprEvent.Value
	datatypeCopy := uprEvent.DataType
	testOpcodeCopy := test.reqT.setOpcode
	statusCopy := test.respT.Status
	retryCntCopy := test.reqT.retryCnt
	setCasCopy := test.reqT.setCas
	setHasXattrsCopy := test.reqT.setHasXattrs
	targetSyncValCopy := test.sync
	uprEventOpcodeCopy := uprEvent.Opcode
	casLockingCopy := test.reqT.casLocking
	casLockingFailureCopy := test.casLockingFailure

	bucket := cluster.Bucket(bucketName)

	defer func() {
		// reset it back before returning
		uprEvent.Value = valueCopy
		uprEvent.DataType = datatypeCopy
		test.reqT.setOpcode = testOpcodeCopy
		test.respT.Status = statusCopy
		test.reqT.retryCnt = retryCntCopy
		test.reqT.setCas = setCasCopy
		test.reqT.setHasXattrs = setHasXattrsCopy
		test.sync = targetSyncValCopy
		uprEvent.Opcode = uprEventOpcodeCopy
		test.reqT.targetCasChanger = targetWriter{}
		test.casLockingFailure = casLockingFailureCopy
		test.reqT.casLocking = casLockingCopy
	}()

	// execute the test for all combinations.
	sourceOpcodes := []mc.CommandCode{mc.UPR_MUTATION, mc.UPR_DELETION, mc.UPR_EXPIRATION}
	targetsAreTombstone := []bool{false} // KV crashes with true - MB-63769

	for _, sourceOpcode := range sourceOpcodes {
		for _, targetIsTombstone := range targetsAreTombstone {
			key := fmt.Sprintf("key-%v", time.Now().UnixNano())
			uprEvent.Key = []byte(key)
			uprEvent.VBucket = getVBucketNo(key, NumVbs)

			fmt.Printf("$$$$ Key generated=%s, source mutation=%s, target is tombstone=%v\n", key, sourceOpcode, targetIsTombstone)

			uprEvent.Opcode = sourceOpcode
			sourceDocIsTombstone := sourceOpcode == mc.UPR_DELETION || sourceOpcode == mc.UPR_EXPIRATION

			sourceHlv, sourceMou, sourceSyncVal, err := getHlvMouSyncFromDocBody(uprEvent.Value, uprEvent.DataType)
			assert.Nil(err)
			targetHlv, targetMou, targetSyncVal, err := getHlvMouSyncFromDocBody(test.targetBody, test.targetDatatype)
			assert.Nil(err)

			if sourceDocIsTombstone {
				if !test.targetDocExists {
					// this setup will compose a deleteWithMeta with command.cas = 0
					// because target doc did not exist.
					// so cas locking will never happen
					test.casLockingFailure = false
					test.reqT.casLocking = false
					test.respT.Status = mc.SUCCESS
				} else {
					test.reqT.casLocking = casLockingCopy
					test.respT.Status = statusCopy
					test.casLockingFailure = casLockingFailureCopy
				}
				if !test.reqT.setHasXattrs && (len(targetHlv) > 0 || len(targetMou) > 0 || len(targetSyncVal) > 0) {
					// KV bug - MB-63765
					continue
				}
				uprEvent.Value, uprEvent.DataType, err = createTombstone(uprEvent.Value, uprEvent.DataType)
				assert.Nil(err)
				if test.reqT.setOpcode != base.SUBDOC_MULTI_MUTATION {
					test.reqT.setOpcode = mc.DELETE_WITH_META
				} else {
					test.reqT.setOpcode = testOpcodeCopy
				}
			} else {
				uprEvent.Value = valueCopy
				uprEvent.DataType = datatypeCopy
				test.reqT.setOpcode = testOpcodeCopy
				test.reqT.casLocking = casLockingCopy
				test.respT.Status = statusCopy
				test.casLockingFailure = casLockingFailureCopy
			}

			test.reqT.retryCnt = retryCntCopy
			test.reqT.setCas = setCasCopy
			test.reqT.setHasXattrs = setHasXattrsCopy
			test.sync = targetSyncValCopy
			test.reqT.targetCasChanger = targetWriter{}

			if test.targetDocExists {
				if targetIsTombstone {
					err = delMeta(kvStringTgt, bucketName, []byte(key), test.targetBody, test.targetDatatype,
						test.targetCas, test.targetRevId, xmemBucket == base.CRMode_LWW)
				} else {
					err = setMeta(kvStringTgt, bucketName, []byte(key), test.targetBody, test.targetDatatype,
						test.targetCas, test.targetRevId, xmemBucket == base.CRMode_LWW)
				}
				assert.Nil(err)
			}

			expectedRev := []byte(fmt.Sprintf(`"%v"`, test.targetRevId+1))
			if test.targetCasChanged {
				test.reqT.targetCasChanger = targetWriter{
					enabled:       true,
					waiter:        checkTarget,
					kvAddr:        kvStringTgt,
					bucketName:    bucketName,
					key:           []byte(key),
					val:           test.targetBody,
					datatype:      test.targetDatatype,
					cas:           test.targetCas + 1,
					revId:         test.targetRevId + 1,
					lww:           xmemBucket == base.CRMode_LWW,
					bucket:        bucket,
					expectedCas:   []byte(`"` + string(base.Uint64ToHexBigEndian(test.targetCas+1)) + `"`),
					expectedRev:   expectedRev,
					accessDeleted: true,
				}

				if targetIsTombstone {
					test.reqT.targetCasChanger.writer = delMeta
				} else {
					test.reqT.targetCasChanger.writer = setMeta
				}

				if test.mobile && !test.targetDocExists {
					// the first request composed will not have sync because target doesn't exist first.
					// Then when target cas changed, retry request will have sync
					test.reqT.setHasXattrs = len(test.hlv) > 0 || len(test.mou) > 0 || len(test.userXattrKey) > 0
					test.sync = ``
				}
			}

			if !test.targetWins {
				reqTestCh <- test.reqT
				respTestCh <- test.respT
			}

			if test.casLockingFailure {
				// the retry request
				test.respT.Status = mc.SUCCESS
				test.reqT.retryCnt++
				test.reqT.targetCasChanger = targetWriter{}
				test.reqT.setCas = test.targetCas + 1
				test.reqT.setHasXattrs = setHasXattrsCopy
				test.sync = targetSyncValCopy

				if sourceDocIsTombstone {
					test.reqT.setOpcode = mc.DELETE_WITH_META
				} else {
					test.reqT.setOpcode = base.SET_WITH_META
				}

				go func() {
					reqTestCh <- test.reqT
					respTestCh <- test.respT
				}()
			}

			// Create xmem and router for testing
			utilsNotUsed, settings, xmem, router, throttler, remoteClusterSvc, colManSvc, eventProducer := setupBoilerPlateXmem(bucketName, xmemBucket)
			realUtils := utilsReal.NewUtilities()
			xmem.utils = realUtils
			xmem.sourceBucketUuid = "93fcf4f0fcc94fdb3d6196235029d6bf"
			xmem.source_cr_mode = xmemBucket
			router.sourceCRMode = xmemBucket

			settings[base.EnableCrossClusterVersioningKey] = test.eccv
			settings[base.VersionPruningWindowHrsKey] = 720
			mobileSetting := base.MobileCompatibilityOff
			if test.mobile {
				mobileSetting = base.MobileCompatibilityActive
			}

			setupMocksXmem(xmem, utilsNotUsed, throttler, remoteClusterSvc, colManSvc, eventProducer)
			settings[MOBILE_COMPATBILE] = mobileSetting
			startTargetXmem(xmem, settings, bucketName, &assert)

			req, err := router.ComposeMCRequest(&base.WrappedUprEvent{UprEvent: uprEvent})
			assert.Nil(err)

			if test.eccv {
				max_cas := int(uprEvent.Cas) + test.max_cas_delta
				if max_cas < 0 {
					panic("max_cas is < 0, but should be >= 0")
				}
				xmem.config.vbHlvMaxCas[req.Req.VBucket] = uint64(max_cas)
			}

			xmem.Receive(req)

			if !test.targetWins {
				if test.reqT.setOpcode != base.SUBDOC_MULTI_MUTATION {
					err = waitForReplication(string(uprEvent.Key), gocb.Cas(uprEvent.Cas), bucket)
					assert.Nil(err)

					if sourceDocIsTombstone && (test.targetCasChanged || test.targetDocExists) {
						// for delete_with_meta, system xattrs will remain even after delete
						if test.eccv && uprEvent.Cas >= xmem.config.vbHlvMaxCas[uprEvent.VBucket] {
							// these are with eccv on + cas >= max_cas,
							// hlv will always be stamped/updated.
							assert.NotEqual(len(test.hlv), 0)
							err = checkTarget(bucket, string(uprEvent.Key), base.XATTR_HLV, []byte(test.hlv), true, true)
							assert.Nil(err)
						} else if len(sourceHlv) > 0 {
							// hlv will only be updated if it already exists.
							if test.hlvOutdated {
								// if hlv in source document was outdated.
								// if it is true, updated hlv would be test.hlv
								err = checkTarget(bucket, string(uprEvent.Key), base.XATTR_HLV, []byte(test.hlv), true, true)
								assert.Nil(err)
							} else {
								err = checkTarget(bucket, string(uprEvent.Key), base.XATTR_HLV, sourceHlv, true, true)
								assert.Nil(err)
							}
						} else if len(targetHlv) > 0 && !test.reqT.setHasXattrs {
							// delete_with_meta with no xattrs in the command value,
							// system xattrs will be preserved in the tombstone.
							err = checkTarget(bucket, string(uprEvent.Key), base.XATTR_HLV, targetHlv, true, true)
							assert.Nil(err)
						} else {
							err = checkTargetDNE(bucket, string(uprEvent.Key), base.XATTR_HLV, true, true)
							assert.Nil(err)
						}
					} else {
						if len(test.hlv) > 0 {
							err = checkTarget(bucket, string(uprEvent.Key), base.XATTR_HLV, []byte(test.hlv), true, true)
							assert.Nil(err)
						} else {
							err = checkTargetDNE(bucket, string(uprEvent.Key), base.XATTR_HLV, true, true)
							assert.Nil(err)
						}
					}
				} else {
					// cas will regenerate
					newCas := waitForCasChange(nil, string(uprEvent.Key), gocb.Cas(test.targetCas), bucket, true)

					// cvCas should be macro expanded
					cvCas := []byte(`"` + string(base.Uint64ToHexLittleEndian(uint64(newCas))) + `"`)
					err = checkTarget(bucket, string(uprEvent.Key), crMeta.XATTR_CVCAS_PATH, cvCas, true, true)
					assert.Nil(err)
				}

				if sourceDocIsTombstone && (test.targetCasChanged || test.targetDocExists) {
					// for delete_with_meta, system xattrs will remain even after delete

					if len(sourceMou) > 0 {
						if test.hlvOutdated {
							// if hlv in source document was outdated.
							// if it is true, updated mou would be test.mou
							err = checkTarget(bucket, string(uprEvent.Key), base.XATTR_MOU, []byte(test.mou), true, true)
							assert.Nil(err)
						} else {
							err = checkTarget(bucket, string(uprEvent.Key), base.XATTR_MOU, sourceMou, true, true)
							assert.Nil(err)
						}
					} else if len(targetMou) > 0 && !test.reqT.setHasXattrs {
						// delete_with_meta with no xattrs in command value,
						// system xattrs will be preserved in the tombstone.
						err = checkTarget(bucket, string(uprEvent.Key), base.XATTR_MOU, targetMou, true, true)
						assert.Nil(err)
					} else {
						err = checkTargetDNE(bucket, string(uprEvent.Key), base.XATTR_MOU, true, true)
						assert.Nil(err)
					}

					if test.mobile {
						// target sync will be preserved
						if len(targetSyncVal) > 0 && !(sourceDocIsTombstone && !test.targetDocExists && test.reqT.setHasXattrs) {
							// say the target doc did not exist when deleteWithMeta command was composed by XDCR
							// that means it would have command.Cas = 0
							// but by the time the mutation reaches the target KV, if any update happened which has _sync now,
							// it will not be preserved since command.Cas = 0 and deleteWithMeta already has xattrs in it's command.Value.
							// if len(command.Value) is 0, then KV performs delWithMeta and preserves system xattrs.
							// if len(command.Value) is not 0, then KV performs a setWithMeta with the command.Value (i.e tombstone with command.Value's xattrs) and
							// system xattrs are not preserved (unless they are part of command.Value)
							err = checkTarget(bucket, string(uprEvent.Key), base.XATTR_MOBILE, targetSyncVal, true, true)
							assert.Nil(err)
						} else {
							err = checkTargetDNE(bucket, string(uprEvent.Key), base.XATTR_MOBILE, true, true)
							assert.Nil(err)
						}
					} else if len(sourceSyncVal) > 0 {
						err = checkTarget(bucket, string(uprEvent.Key), base.XATTR_MOBILE, sourceSyncVal, true, true)
						assert.Nil(err)
					} else if len(targetSyncVal) > 0 && !test.reqT.setHasXattrs {
						// delete_with_meta with no xattrs in command value,
						// system xattrs will be preserved in the tombstone.
						err = checkTarget(bucket, string(uprEvent.Key), base.XATTR_MOBILE, targetSyncVal, true, true)
						assert.Nil(err)
					} else {
						err = checkTargetDNE(bucket, string(uprEvent.Key), base.XATTR_MOBILE, true, true)
						assert.Nil(err)
					}
				} else {
					if len(test.mou) > 0 {
						err = checkTarget(bucket, string(uprEvent.Key), base.XATTR_MOU, []byte(test.mou), true, true)
						assert.Nil(err)
					} else {
						err = checkTargetDNE(bucket, string(uprEvent.Key), base.XATTR_MOU, true, true)
						assert.Nil(err)
					}

					if len(test.sync) > 0 {
						// expected sync needs to be specified by the test
						err = checkTarget(bucket, string(uprEvent.Key), base.XATTR_MOBILE, []byte(test.sync), true, true)
						assert.Nil(err)
					} else {
						err = checkTargetDNE(bucket, string(uprEvent.Key), base.XATTR_MOBILE, true, true)
						assert.Nil(err)
					}
				}

				if !(sourceDocIsTombstone && test.reqT.setOpcode == base.SUBDOC_MULTI_MUTATION) && len(test.userXattrKey) > 0 {
					// if we issue a subdoc mc.DELETE, all user xattrs will be pruned and
					// only system xattrs are preserved
					// source doc had this xattr
					err = checkTarget(bucket, string(uprEvent.Key), test.userXattrKey, []byte(test.userXattrVal), true, true)
					assert.Nil(err)
				} else {
					// source doc didn't have the xattr and won CR,
					// so final target doc should not have it.
					err = checkTargetDNE(bucket, string(uprEvent.Key), xattrK1, true, true)
					assert.Nil(err)
				}

				err = checkTargetDNE(bucket, string(uprEvent.Key), xattrK2, true, true)
				assert.Nil(err)
			} else {
				// this xattr is assumed to always only exist on target.
				if test.targetDatatype&base.XattrDataType > 0 {
					err = checkTarget(bucket, string(uprEvent.Key), xattrK2, []byte(xattrV2), true, true)
					assert.Nil(err)
				}

				if !targetIsTombstone {
					err = checkTarget(bucket, string(uprEvent.Key), "", []byte(targetDoc), false, true)
					assert.Nil(err)
				} else {
					err = checkTargetDNE(bucket, string(uprEvent.Key), "", false, true)
					assert.Nil(err)
				}
			}

			if len(reqTestCh) > 0 {
				panic(fmt.Sprintf("%v failed because of xmem inresponsiveness for req, len=%v", test.name, len(reqTestCh)))
			}
			if len(respTestCh) > 0 {
				panic(fmt.Sprintf("%v failed because of xmem inresponsiveness for resp %v", test.name, len(respTestCh)))
			}
		}
	}
}

func noXattrs() (*mcc.UprEvent, error) {
	uprfile := "./testdata/mutationTypes/noXattrs.json"
	docEvent, err := RetrieveUprFile(uprfile)
	if err != nil {
		panic(err)
	}
	return docEvent, err
}

func userXattrs() (*mcc.UprEvent, error) {
	uprfile := "./testdata/mutationTypes/userXattrs.json"
	docEvent, err := RetrieveUprFile(uprfile)
	if err != nil {
		panic(err)
	}
	return docEvent, err
}

func syncAndMouAndHlvAndUserXattrs() (*mcc.UprEvent, error) {
	uprfile := "./testdata/mutationTypes/syncAndImportMouAndUptodateHlvAndUserXattrs.json"
	docEvent, err := RetrieveUprFile(uprfile)
	if err != nil {
		panic(err)
	}
	return docEvent, err
}

func syncAndMouAndHlvXattrs() (*mcc.UprEvent, error) {
	uprfile := "./testdata/mutationTypes/syncAndImportMouAndUptodateHlvXattrs.json"
	docEvent, err := RetrieveUprFile(uprfile)
	if err != nil {
		panic(err)
	}
	return docEvent, err
}

func hlvXattrs() (*mcc.UprEvent, error) {
	uprfile := "./testdata/mutationTypes/uptodateHlvXattrs.json"
	docEvent, err := RetrieveUprFile(uprfile)
	if err != nil {
		panic(err)
	}
	return docEvent, err
}

func hlvAndUserXattrs() (*mcc.UprEvent, error) {
	uprfile := "./testdata/mutationTypes/uptodateHlvAndUserXattrs.json"
	docEvent, err := RetrieveUprFile(uprfile)
	if err != nil {
		panic(err)
	}
	return docEvent, err
}

var legacyMutationTCs = []mutationTC{
	{
		name: "legacy mutation, no xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.SET_WITH_META,
			getOpcode:     mc.GET_META,
			subdocLookup:  false,
			specs:         nil,
			casLocking:    false,
			setNoTargetCR: false,
			setHasXattrs:  false,
		},
		mobile:        false,
		eccv:          false,
		max_cas_delta: 0,
		casDelta:      0,
		hlv:           "",
		sync:          "",
		mou:           "",
		getUprEvent:   noXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
	},
	{
		name: "legacy mutation, user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.SET_WITH_META,
			getOpcode:     mc.GET_META,
			subdocLookup:  false,
			specs:         nil,
			casLocking:    false,
			setNoTargetCR: false,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          false,
		max_cas_delta: 0,
		casDelta:      0,
		hlv:           "",
		sync:          "",
		mou:           "",
		userXattrKey:  "foo",
		userXattrVal:  `{"foo":"bar"}`,
		getUprEvent:   userXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
	},
	{
		name: "legacy mutation, already has updated hlv, sync and mou indicating import mutation along with user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.SET_WITH_META,
			getOpcode:     mc.GET_META,
			subdocLookup:  false,
			specs:         nil,
			casLocking:    false,
			setNoTargetCR: false,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          false,
		max_cas_delta: 0,
		casDelta:      0,
		hlv:           `{"cvCas":"0x0000f6942f7ce817","src":"27hMZgvcvI5YMQJBIGkxxw","ver":"0x0000f6942f7ce817"}`,
		mou:           `{"cas":"0x00008d393a7ce817","pRev":"9"}`,
		sync:          `{"cas":"0x1234567890123456","revid":"1-abcde"}`,
		getUprEvent:   syncAndMouAndHlvAndUserXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
	},
	{
		name: "legacy mutation, already has updated hlv, sync and mou indicating import mutation, no user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.SET_WITH_META,
			getOpcode:     mc.GET_META,
			subdocLookup:  false,
			specs:         nil,
			casLocking:    false,
			setNoTargetCR: false,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          false,
		max_cas_delta: 0,
		casDelta:      0,
		hlv:           `{"cvCas":"0x18fcf8cbc47de817","src":"qa5lP/5Ae1V6ZQt4VojG6g","ver":"0x18fcf8cbc47de817"}`,
		mou:           `{"cas":"0x00006f49c87de817","pRev":"9"}`,
		sync:          `{"cas":"0x1234567890123456","revid":"1-abcde"}`,
		getUprEvent:   syncAndMouAndHlvXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
	},
	{
		name: "legacy mutation, already has outdated hlv",
		reqT: reqTestParams{
			setOpcode:     mc.SET_WITH_META,
			getOpcode:     mc.GET_META,
			subdocLookup:  false,
			specs:         nil,
			casLocking:    false,
			setNoTargetCR: false,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          false,
		max_cas_delta: 0,
		casDelta:      1000000,
		hlv:           `{"cvCas":"0x4042f769ae7de817","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x4042f769ae7de817","pv":["0000669ea77de817@qa5lP/5Ae1V6ZQt4VojG6g"]}`,
		mou:           ``,
		sync:          ``,
		getUprEvent:   hlvXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		hlvOutdated: true,
	},
	{
		name: "legacy mutation, already has outdated hlv and user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.SET_WITH_META,
			getOpcode:     mc.GET_META,
			subdocLookup:  false,
			specs:         nil,
			casLocking:    false,
			setNoTargetCR: false,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          false,
		max_cas_delta: 0,
		casDelta:      1000000,
		hlv:           `{"cvCas":"0x4042bb32e17de817","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x4042bb32e17de817","pv":["0000ac32e17de817@qa5lP/5Ae1V6ZQt4VojG6g"]}`,
		mou:           ``,
		sync:          ``,
		getUprEvent:   hlvAndUserXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
		hlvOutdated:  true,
	},
}

func Test_LegacyMutations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Test_LegacyMutations")
	}
	if !targetXmemIsUpAndCorrectSetupExists(targetConnStr, targetPort, "") {
		fmt.Println("Skipping Test_LegacyMutations since live cluster_run setup has not been detected")
		return
	}

	respTestCh = make(chan respTestParams, 1)
	reqTestCh = make(chan reqTestParams, 1)
	assert := assert.New(t)

	gocbBuckets := []gocb.ConflictResolutionType{"seqno", "lww"}
	xmemBuckets := []base.ConflictResolutionMode{base.CRMode_RevId, base.CRMode_LWW}

	for i := range gocbBuckets {
		fmt.Printf("*************** (%v/%v) Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])

		bucketName := string(gocbBuckets[i])
		cluster, _, err := createBucket(targetConnStr, bucketName, gocbBuckets[i])
		assert.Nil(err)

		for n, test := range legacyMutationTCs {
			fmt.Printf("=============== (%v/%v) Test: %s ===============\n", n+1, len(legacyMutationTCs), test.name)

			// source doc exists, target doc doesn't
			test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

			test.targetRevId = 3
			test.targetCas = 100000000000

			test.targetCasChanged = true
			test.casLockingFailure = false
			targets, datatypes, desc, _, _, _, err := getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				test.targetBody = target
				test.targetDatatype = datatypes[j]

				fmt.Println(">>>> source doc exists, target doc doesn't; but by the time source doc is replicated there was a change on target")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			test.targetDocExists = true

			test.targetWins = true
			test.targetRevId = 100
			test.targetCas = 9999999999999999999
			test.respT.Status = mc.KEY_EEXISTS
			targets, datatypes, desc, _, _, _, err = getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				test.targetBody = target
				test.targetDatatype = datatypes[j]

				fmt.Println(">>>> source doc exists, target doc exists and target wins CR.")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			test.targetWins = false
			test.targetRevId = 3
			test.targetCas = 100000000000
			test.respT.Status = mc.SUCCESS
			targets, datatypes, desc, _, _, _, err = getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				test.targetBody = target
				test.targetDatatype = datatypes[j]
				test.targetCasChanged = false
				test.casLockingFailure = false

				fmt.Println(">>>> source doc exists, target doc exists, source wins CR.")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				test.targetCasChanged = true
				test.casLockingFailure = false

				fmt.Println(">>>> source doc exists, target doc exists, source wins CR; but by the time source doc is replicated target changed.")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			fmt.Printf("=============== (%v/%v) Done Test: %s ===============\n", n+1, len(legacyMutationTCs), test.name)
		}

		cluster.Close(nil)

		fmt.Printf("*************** (%v/%v) Done Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])
	}
}

// cas >= max_cas
var eccvOnlyMutationTCsWithCasGreaterThanMaxCas = []mutationTC{
	{
		name: "eccv=on (cas >= max_cas), mobile=off, no hlv, no user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.ADD_WITH_META,
			getOpcode:     mc.SUBDOC_MULTI_LOOKUP,
			subdocLookup:  true,
			specs:         []string{base.XATTR_HLV, base.XATTR_IMPORTCAS, base.XATTR_PREVIOUSREV, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID},
			casLocking:    true,
			setNoTargetCR: true,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          true,
		max_cas_delta: -100,
		casDelta:      0,
		hlv:           `{"cvCas":"0x0000611cb792e817","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x0000611cb792e817"}`,
		mou:           ``,
		sync:          ``,
		getUprEvent:   noXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
	},
	{
		name: "eccv=on (cas >= max_cas), mobile=off, no hlv, user xattrs",
		reqT: reqTestParams{
			setOpcode:    mc.ADD_WITH_META,
			getOpcode:    mc.SUBDOC_MULTI_LOOKUP,
			subdocLookup: true,
			specs:        []string{base.XATTR_HLV, base.XATTR_IMPORTCAS, base.XATTR_PREVIOUSREV, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID},

			casLocking:    true,
			setNoTargetCR: true,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          true,
		max_cas_delta: -100,
		casDelta:      0,
		hlv:           `{"cvCas":"0x0000f602a577e817","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x0000f602a577e817"}`,
		mou:           ``,
		sync:          ``,
		getUprEvent:   userXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
	},
	{
		name: "eccv=on (cas >= max_cas), mobile=off, updated hlv, import mutation, user xattrs",
		reqT: reqTestParams{
			setOpcode:    mc.ADD_WITH_META,
			getOpcode:    mc.SUBDOC_MULTI_LOOKUP,
			subdocLookup: true,
			specs:        []string{base.XATTR_HLV, base.XATTR_IMPORTCAS, base.XATTR_PREVIOUSREV, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID},

			casLocking:    true,
			setNoTargetCR: true,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          true,
		max_cas_delta: -100,
		casDelta:      0,
		hlv:           `{"cvCas":"0x0000f6942f7ce817","src":"27hMZgvcvI5YMQJBIGkxxw","ver":"0x0000f6942f7ce817"}`,
		mou:           `{"cas":"0x00008d393a7ce817","pRev":"9"}`,
		sync:          `{"cas":"0x1234567890123456","revid":"1-abcde"}`,
		getUprEvent:   syncAndMouAndHlvAndUserXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
	},
	{
		name: "eccv=on (cas >= max_cas), mobile=off, updated hlv, import mutation, no user xattrs",
		reqT: reqTestParams{
			setOpcode:    mc.ADD_WITH_META,
			getOpcode:    mc.SUBDOC_MULTI_LOOKUP,
			subdocLookup: true,
			specs:        []string{base.XATTR_HLV, base.XATTR_IMPORTCAS, base.XATTR_PREVIOUSREV, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID},

			casLocking:    true,
			setNoTargetCR: true,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          true,
		max_cas_delta: -100,
		casDelta:      0,
		hlv:           `{"cvCas":"0x18fcf8cbc47de817","src":"qa5lP/5Ae1V6ZQt4VojG6g","ver":"0x18fcf8cbc47de817"}`,
		mou:           `{"cas":"0x00006f49c87de817","pRev":"9"}`,
		sync:          `{"cas":"0x1234567890123456","revid":"1-abcde"}`,
		getUprEvent:   syncAndMouAndHlvXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
	},
	{
		name: "eccv=on (cas >= max_cas), mobile=off, only updated hlv and user xattrs",
		reqT: reqTestParams{
			setOpcode:    mc.ADD_WITH_META,
			getOpcode:    mc.SUBDOC_MULTI_LOOKUP,
			subdocLookup: true,
			specs:        []string{base.XATTR_HLV, base.XATTR_IMPORTCAS, base.XATTR_PREVIOUSREV, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID},

			casLocking:    true,
			setNoTargetCR: true,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          true,
		max_cas_delta: -100,
		casDelta:      0,
		hlv:           `{"cvCas":"0x0000ac32e17de817","src":"qa5lP/5Ae1V6ZQt4VojG6g","ver":"0x0000ac32e17de817"}`,
		mou:           ``,
		sync:          ``,
		getUprEvent:   hlvAndUserXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
	},
	{
		name: "eccv=on (cas >= max_cas), mobile=off, outdated hlv, import mutation, user xattrs",
		reqT: reqTestParams{
			setOpcode:    mc.ADD_WITH_META,
			getOpcode:    mc.SUBDOC_MULTI_LOOKUP,
			subdocLookup: true,
			specs:        []string{base.XATTR_HLV, base.XATTR_IMPORTCAS, base.XATTR_PREVIOUSREV, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID},

			casLocking:    true,
			setNoTargetCR: true,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          true,
		max_cas_delta: -100,
		casDelta:      100000,
		hlv:           `{"cvCas":"0xa0868e393a7ce817","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0xa0868e393a7ce817","pv":["0000f6942f7ce817@27hMZgvcvI5YMQJBIGkxxw"]}`,
		mou:           ``,
		sync:          `{"cas":"0x1234567890123456","revid":"1-abcde"}`,
		getUprEvent:   syncAndMouAndHlvAndUserXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
		hlvOutdated:  true,
	},
	{
		name: "eccv=on (cas >= max_cas), mobile=off, outdated hlv, import mutation, no user xattrs",
		reqT: reqTestParams{
			setOpcode:    mc.ADD_WITH_META,
			getOpcode:    mc.SUBDOC_MULTI_LOOKUP,
			subdocLookup: true,
			specs:        []string{base.XATTR_HLV, base.XATTR_IMPORTCAS, base.XATTR_PREVIOUSREV, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID},

			casLocking:    true,
			setNoTargetCR: true,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          true,
		max_cas_delta: -100,
		casDelta:      100000,
		hlv:           `{"cvCas":"0xa0867049c87de817","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0xa0867049c87de817","pv":["18fcf8cbc47de817@qa5lP/5Ae1V6ZQt4VojG6g"]}`,
		mou:           ``,
		sync:          `{"cas":"0x1234567890123456","revid":"1-abcde"}`,
		getUprEvent:   syncAndMouAndHlvXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		hlvOutdated: true,
	},
	{
		name: "eccv=on (cas >= max_cas), mobile=off, only outdated hlv and user xattrs",
		reqT: reqTestParams{
			setOpcode:    mc.ADD_WITH_META,
			getOpcode:    mc.SUBDOC_MULTI_LOOKUP,
			subdocLookup: true,
			specs:        []string{base.XATTR_HLV, base.XATTR_IMPORTCAS, base.XATTR_PREVIOUSREV, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID},

			casLocking:    true,
			setNoTargetCR: true,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          true,
		max_cas_delta: -100,
		casDelta:      100000,
		hlv:           `{"cvCas":"0xa086ad32e17de817","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0xa086ad32e17de817","pv":["0000ac32e17de817@qa5lP/5Ae1V6ZQt4VojG6g"]}`,
		mou:           ``,
		sync:          ``,
		getUprEvent:   hlvAndUserXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
		hlvOutdated:  true,
	},
}

// cas < max_cas
var eccvOnlyMutationTCsWithCasLessThanMaxCas = []mutationTC{
	{
		name: "eccv=on (cas < max_cas), mobile=off, no hlv, no user xattrs",
		reqT: reqTestParams{
			setOpcode:    mc.SET_WITH_META,
			getOpcode:    base.GET_WITH_META,
			subdocLookup: false,
			specs:        nil,

			casLocking:    false,
			setNoTargetCR: false,
			setHasXattrs:  false,
		},
		mobile:        false,
		eccv:          true,
		max_cas_delta: 100,
		casDelta:      0,
		hlv:           ``,
		mou:           ``,
		sync:          ``,
		getUprEvent:   noXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
	},
	{
		name: "eccv=on (cas < max_cas), mobile=off, no hlv, user xattrs",
		reqT: reqTestParams{
			setOpcode:    mc.SET_WITH_META,
			getOpcode:    base.GET_WITH_META,
			subdocLookup: false,
			specs:        nil,

			casLocking:    false,
			setNoTargetCR: false,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          true,
		max_cas_delta: 100,
		casDelta:      0,
		hlv:           ``,
		mou:           ``,
		sync:          ``,
		getUprEvent:   userXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
	},
	{
		name: "eccv=on (cas < max_cas), mobile=off, updated hlv, import mutation, user xattrs",
		reqT: reqTestParams{
			setOpcode:    mc.SET_WITH_META,
			getOpcode:    base.GET_WITH_META,
			subdocLookup: false,
			specs:        nil,

			casLocking:    false,
			setNoTargetCR: false,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          true,
		max_cas_delta: 100,
		casDelta:      0,
		hlv:           `{"cvCas":"0x0000f6942f7ce817","src":"27hMZgvcvI5YMQJBIGkxxw","ver":"0x0000f6942f7ce817"}`,
		mou:           `{"cas":"0x00008d393a7ce817","pRev":"9"}`,
		sync:          `{"cas":"0x1234567890123456","revid":"1-abcde"}`,
		getUprEvent:   syncAndMouAndHlvAndUserXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
	},
	{
		name: "eccv=on (cas < max_cas), mobile=off, updated hlv, import mutation, no user xattrs",
		reqT: reqTestParams{
			setOpcode:    mc.SET_WITH_META,
			getOpcode:    base.GET_WITH_META,
			subdocLookup: false,
			specs:        nil,

			casLocking:    false,
			setNoTargetCR: false,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          true,
		max_cas_delta: 100,
		casDelta:      0,
		hlv:           `{"cvCas":"0x18fcf8cbc47de817","src":"qa5lP/5Ae1V6ZQt4VojG6g","ver":"0x18fcf8cbc47de817"}`,
		mou:           `{"cas":"0x00006f49c87de817","pRev":"9"}`,
		sync:          `{"cas":"0x1234567890123456","revid":"1-abcde"}`,
		getUprEvent:   syncAndMouAndHlvXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
	},
	{
		name: "eccv=on (cas < max_cas), mobile=off, only updated hlv and user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.SET_WITH_META,
			getOpcode:     base.GET_WITH_META,
			subdocLookup:  false,
			specs:         nil,
			casLocking:    false,
			setNoTargetCR: false,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          true,
		max_cas_delta: 100,
		casDelta:      0,
		hlv:           `{"cvCas":"0x0000ac32e17de817","src":"qa5lP/5Ae1V6ZQt4VojG6g","ver":"0x0000ac32e17de817"}`,
		mou:           ``,
		sync:          ``,
		getUprEvent:   hlvAndUserXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
	},
	{
		name: "eccv=on (cas < max_cas), mobile=off, outdated hlv, import mutation, user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.SET_WITH_META,
			getOpcode:     base.GET_WITH_META,
			subdocLookup:  false,
			specs:         nil,
			casLocking:    false,
			setNoTargetCR: false,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          true,
		max_cas_delta: 100,
		casDelta:      10,
		hlv:           `{"cvCas":"0x0a008d393a7ce817","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x0a008d393a7ce817","pv":["0000f6942f7ce817@27hMZgvcvI5YMQJBIGkxxw"]}`,
		mou:           ``,
		sync:          `{"cas":"0x1234567890123456","revid":"1-abcde"}`,
		getUprEvent:   syncAndMouAndHlvAndUserXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
		hlvOutdated:  true,
	},
	{
		name: "eccv=on (cas < max_cas), mobile=off, outdated hlv, import mutation, no user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.SET_WITH_META,
			getOpcode:     base.GET_WITH_META,
			subdocLookup:  false,
			specs:         nil,
			casLocking:    false,
			setNoTargetCR: false,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          true,
		max_cas_delta: 100,
		casDelta:      10,
		hlv:           `{"cvCas":"0x0a006f49c87de817","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x0a006f49c87de817","pv":["18fcf8cbc47de817@qa5lP/5Ae1V6ZQt4VojG6g"]}`,
		mou:           ``,
		sync:          `{"cas":"0x1234567890123456","revid":"1-abcde"}`,
		getUprEvent:   syncAndMouAndHlvXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		hlvOutdated: true,
	},
	{
		name: "eccv=on (cas < max_cas), mobile=off, only outdated hlv and user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.SET_WITH_META,
			getOpcode:     base.GET_WITH_META,
			subdocLookup:  false,
			specs:         nil,
			casLocking:    false,
			setNoTargetCR: false,
			setHasXattrs:  true,
		},
		mobile:        false,
		eccv:          true,
		max_cas_delta: 100,
		casDelta:      10,
		hlv:           `{"cvCas":"0x0a00ac32e17de817","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x0a00ac32e17de817","pv":["0000ac32e17de817@qa5lP/5Ae1V6ZQt4VojG6g"]}`,
		mou:           ``,
		sync:          ``,
		getUprEvent:   hlvAndUserXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
		hlvOutdated:  true,
	},
}

func Test_ECCVOnlyMutations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Test_LegacyMutations")
	}
	if !targetXmemIsUpAndCorrectSetupExists(targetConnStr, targetPort, "") {
		fmt.Println("Skipping Test_LegacyMutations since live cluster_run setup has not been detected")
		return
	}

	respTestCh = make(chan respTestParams, 1)
	reqTestCh = make(chan reqTestParams, 1)
	assert := assert.New(t)

	gocbBuckets := []gocb.ConflictResolutionType{"seqno", "lww"}
	xmemBuckets := []base.ConflictResolutionMode{base.CRMode_RevId, base.CRMode_LWW}

	for i := range gocbBuckets {
		fmt.Printf("*************** (%v/%v) Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])

		bucketName := string(gocbBuckets[i])
		cluster, _, err := createBucket(targetConnStr, bucketName, gocbBuckets[i])
		assert.Nil(err)

		for n, test := range eccvOnlyMutationTCsWithCasGreaterThanMaxCas {
			fmt.Printf("=============== (%v/%v) Test: %s ===============\n", n+1, len(eccvOnlyMutationTCsWithCasGreaterThanMaxCas), test.name)

			fmt.Println(">>>> source doc exists with cas >= max_cas, target doc doesn't")
			test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

			test.targetRevId = 3
			test.targetCas = 100000000000

			test.targetCasChanged = true
			test.casLockingFailure = true
			test.respT.Status = mc.KEY_EEXISTS
			targets, datatypes, desc, _, _, _, err := getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				test.targetBody = target
				test.targetDatatype = datatypes[j]

				fmt.Println(">>>> source doc exists with cas >= max_cas, target doc doesn't; source wins CR and cas locking fails")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			test.targetDocExists = true

			test.targetWins = true
			test.targetRevId = 100
			test.targetCas = 9999999999999999999
			test.targetCasChanged = false
			test.casLockingFailure = false
			targets, datatypes, desc, _, _, _, err = getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				test.targetBody = target
				test.targetDatatype = datatypes[j]

				fmt.Println(">>>> source doc exists with cas >= max_cas, target doc exists and wins CR")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			test.targetWins = false
			test.targetRevId = 3
			test.targetCas = 100000000000
			test.respT.Status = mc.SUCCESS
			test.reqT.setOpcode = base.SET_WITH_META
			test.reqT.setCas = test.targetCas
			targets, datatypes, desc, _, _, _, err = getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				test.targetBody = target
				test.targetDatatype = datatypes[j]
				test.targetCasChanged = false
				test.casLockingFailure = false
				test.respT.Status = mc.SUCCESS

				fmt.Println(">>>> source doc exists with cas >= max_cas, target doc exists, source wins CR")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				test.targetCasChanged = true
				test.casLockingFailure = true
				test.respT.Status = mc.KEY_EEXISTS

				fmt.Println(">>>> source doc exists with cas >= max_cas, target doc exists, source wins CR and cas locking fails")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			fmt.Printf("=============== (%v/%v) Done Test: %s ===============\n", n+1, len(eccvOnlyMutationTCsWithCasGreaterThanMaxCas), test.name)
		}

		for n, test := range eccvOnlyMutationTCsWithCasLessThanMaxCas {
			fmt.Printf("=============== (%v/%v) Test: %s ===============\n", n+1, len(eccvOnlyMutationTCsWithCasLessThanMaxCas), test.name)

			fmt.Println(">>>> source doc exists with cas < max_cas, target doc doesn't")
			test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

			test.targetRevId = 3
			test.targetCas = 100000000000

			test.targetCasChanged = true
			test.casLockingFailure = false
			targets, datatypes, desc, _, _, _, err := getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				test.targetBody = target
				test.targetDatatype = datatypes[j]

				fmt.Println(">>>> source doc exists with cas < max_cas, target doc doesn't; target doc is created by the time source doc is replicated")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			test.targetDocExists = true

			test.targetWins = true
			test.targetRevId = 100
			test.targetCas = 9999999999999999999
			test.respT.Status = mc.KEY_EEXISTS
			targets, datatypes, desc, _, _, _, err = getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				test.targetBody = target
				test.targetDatatype = datatypes[j]

				fmt.Println(">>>> source doc exists with cas < max_cas, target doc exists and target wins CR")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			test.targetWins = false
			test.targetRevId = 3
			test.targetCas = 100000000000
			test.respT.Status = mc.SUCCESS
			targets, datatypes, desc, _, _, _, err = getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				test.targetBody = target
				test.targetDatatype = datatypes[j]
				test.targetCasChanged = false
				test.casLockingFailure = false

				fmt.Println(">>>> source doc exists with cas < max_cas, target doc exists and source wins CR")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				test.targetCasChanged = true
				test.casLockingFailure = false

				fmt.Println(">>>> source doc exists with cas < max_cas, target doc exists and source wins CR; target doc gets modified by the time source doc is replicated")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			fmt.Printf("=============== (%v/%v) Done Test: %s ===============\n", n+1, len(eccvOnlyMutationTCsWithCasLessThanMaxCas), test.name)
		}

		cluster.Close(nil)

		fmt.Printf("*************** (%v/%v) Done Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])
	}
}

var mobileOnlyMutationTCs = []mutationTC{
	{
		name: "eccv=off, mobile=on, no hlv, no user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.ADD_WITH_META,
			getOpcode:     mc.SUBDOC_MULTI_LOOKUP,
			subdocLookup:  true,
			specs:         []string{base.XattributeToc, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID, base.XATTR_MOBILE},
			casLocking:    true,
			setNoTargetCR: true,
			setHasXattrs:  false,
		},
		mobile:        true,
		eccv:          false,
		max_cas_delta: 0,
		casDelta:      0,
		hlv:           ``,
		mou:           ``,
		sync:          ``,
		getUprEvent:   noXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
	},
	{
		name: "eccv=off, mobile=on, no hlv, user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.ADD_WITH_META,
			getOpcode:     mc.SUBDOC_MULTI_LOOKUP,
			subdocLookup:  true,
			specs:         []string{base.XattributeToc, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID, base.XATTR_MOBILE},
			casLocking:    true,
			setNoTargetCR: true,
			setHasXattrs:  true,
		},
		mobile:        true,
		eccv:          false,
		max_cas_delta: 0,
		casDelta:      0,
		hlv:           ``,
		mou:           ``,
		sync:          ``,
		getUprEvent:   userXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
	},
	{
		name: "eccv=off, mobile=on, updated hlv, import mutation, user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.ADD_WITH_META,
			getOpcode:     mc.SUBDOC_MULTI_LOOKUP,
			subdocLookup:  true,
			specs:         []string{base.XattributeToc, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID, base.XATTR_MOBILE},
			casLocking:    true,
			setNoTargetCR: true,
			setHasXattrs:  true,
		},
		mobile:        true,
		eccv:          false,
		max_cas_delta: 0,
		casDelta:      0,
		hlv:           `{"cvCas":"0x0000f6942f7ce817","src":"27hMZgvcvI5YMQJBIGkxxw","ver":"0x0000f6942f7ce817"}`,
		mou:           `{"cas":"0x00008d393a7ce817","pRev":"9"}`,
		sync:          ``,
		getUprEvent:   syncAndMouAndHlvAndUserXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
	},
	{
		name: "eccv=off, mobile=on, updated hlv, import mutation, no user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.ADD_WITH_META,
			getOpcode:     mc.SUBDOC_MULTI_LOOKUP,
			subdocLookup:  true,
			specs:         []string{base.XattributeToc, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID, base.XATTR_MOBILE},
			casLocking:    true,
			setNoTargetCR: true,
			setHasXattrs:  true,
		},
		mobile:        true,
		eccv:          false,
		max_cas_delta: 0,
		casDelta:      0,
		hlv:           `{"cvCas":"0x18fcf8cbc47de817","src":"qa5lP/5Ae1V6ZQt4VojG6g","ver":"0x18fcf8cbc47de817"}`,
		mou:           `{"cas":"0x00006f49c87de817","pRev":"9"}`,
		sync:          ``,
		getUprEvent:   syncAndMouAndHlvXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
	},
	{
		name: "eccv=off, mobile=on, only updated hlv and user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.ADD_WITH_META,
			getOpcode:     mc.SUBDOC_MULTI_LOOKUP,
			subdocLookup:  true,
			specs:         []string{base.XattributeToc, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID, base.XATTR_MOBILE},
			casLocking:    true,
			setNoTargetCR: true,
			setHasXattrs:  true,
		},
		mobile:        true,
		eccv:          false,
		max_cas_delta: -100,
		casDelta:      0,
		hlv:           `{"cvCas":"0x0000ac32e17de817","src":"qa5lP/5Ae1V6ZQt4VojG6g","ver":"0x0000ac32e17de817"}`,
		mou:           ``,
		sync:          ``,
		getUprEvent:   hlvAndUserXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
	},
	{
		name: "eccv=off, mobile=on, outdated hlv, import mutation, user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.ADD_WITH_META,
			getOpcode:     mc.SUBDOC_MULTI_LOOKUP,
			subdocLookup:  true,
			specs:         []string{base.XattributeToc, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID, base.XATTR_MOBILE},
			casLocking:    true,
			setNoTargetCR: true,
			setHasXattrs:  true,
		},
		mobile:        true,
		eccv:          false,
		max_cas_delta: 0,
		casDelta:      100000,
		hlv:           `{"cvCas":"0xa0868e393a7ce817","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0xa0868e393a7ce817","pv":["0000f6942f7ce817@27hMZgvcvI5YMQJBIGkxxw"]}`,
		mou:           ``,
		sync:          ``,
		getUprEvent:   syncAndMouAndHlvAndUserXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
		hlvOutdated:  true,
	},
	{
		name: "eccv=off, mobile=on, outdated hlv, import mutation, no user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.ADD_WITH_META,
			getOpcode:     mc.SUBDOC_MULTI_LOOKUP,
			subdocLookup:  true,
			specs:         []string{base.XattributeToc, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID, base.XATTR_MOBILE},
			casLocking:    true,
			setNoTargetCR: true,
			setHasXattrs:  true,
		},
		mobile:        true,
		eccv:          false,
		max_cas_delta: 0,
		casDelta:      100000,
		hlv:           `{"cvCas":"0xa0867049c87de817","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0xa0867049c87de817","pv":["18fcf8cbc47de817@qa5lP/5Ae1V6ZQt4VojG6g"]}`,
		mou:           ``,
		sync:          ``,
		getUprEvent:   syncAndMouAndHlvXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		hlvOutdated: true,
	},
	{
		name: "eccv=off, mobile=on, only outdated hlv and user xattrs",
		reqT: reqTestParams{
			setOpcode:     mc.ADD_WITH_META,
			getOpcode:     mc.SUBDOC_MULTI_LOOKUP,
			subdocLookup:  true,
			specs:         []string{base.XattributeToc, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID, base.XATTR_MOBILE},
			casLocking:    true,
			setNoTargetCR: true,
			setHasXattrs:  true,
		},
		mobile:        true,
		eccv:          false,
		max_cas_delta: 0,
		casDelta:      100000,
		hlv:           `{"cvCas":"0xa086ad32e17de817","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0xa086ad32e17de817","pv":["0000ac32e17de817@qa5lP/5Ae1V6ZQt4VojG6g"]}`,
		mou:           ``,
		sync:          ``,
		getUprEvent:   hlvAndUserXattrs,
		respT: respTestParams{
			Status: mc.SUCCESS,
		},
		userXattrKey: userXattrKey,
		userXattrVal: userXattr,
		hlvOutdated:  true,
	},
}

func Test_MobileOnlyMutations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Test_MobileOnlyMutations")
	}
	if !targetXmemIsUpAndCorrectSetupExists(targetConnStr, targetPort, "") {
		fmt.Println("Skipping Test_MobileOnlyMutations since live cluster_run setup has not been detected")
		return
	}

	respTestCh = make(chan respTestParams, 1)
	reqTestCh = make(chan reqTestParams, 1)
	assert := assert.New(t)

	gocbBuckets := []gocb.ConflictResolutionType{"seqno", "lww"}
	xmemBuckets := []base.ConflictResolutionMode{base.CRMode_RevId, base.CRMode_LWW}

	for i := range gocbBuckets {
		fmt.Printf("*************** (%v/%v) Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])

		bucketName := string(gocbBuckets[i])
		cluster, _, err := createBucket(targetConnStr, bucketName, gocbBuckets[i])
		assert.Nil(err)

		for n, test := range mobileOnlyMutationTCs {
			fmt.Printf("=============== (%v/%v) Test: %s ===============\n", n+1, len(mobileOnlyMutationTCs), test.name)

			setHasXattrs := test.reqT.setHasXattrs

			fmt.Println(">>>> source doc exists, target doc doesn't")
			test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

			test.targetRevId = 3
			test.targetCas = 100000000000

			test.targetCasChanged = true
			test.casLockingFailure = true
			test.respT.Status = mc.KEY_EEXISTS
			targets, datatypes, desc, _, syncs, _, err := getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				if len(syncs[j]) > 0 {
					test.reqT.setHasXattrs = true // target sync is preserved
					test.sync = string(syncs[j])
				} else {
					test.reqT.setHasXattrs = setHasXattrs
					test.sync = ``
				}
				test.targetBody = target
				test.targetDatatype = datatypes[j]

				fmt.Println(">>>> source doc exists, target doc doesn't; but target doc gets created by the time source doc was replicated")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			test.targetDocExists = true

			test.targetWins = true
			test.targetRevId = 100
			test.targetCas = 9999999999999999999
			test.targetCasChanged = false
			test.casLockingFailure = false
			targets, datatypes, desc, _, syncs, _, err = getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				if len(syncs[j]) > 0 {
					test.reqT.setHasXattrs = true // target sync is preserved
					test.sync = string(syncs[j])
				} else {
					test.reqT.setHasXattrs = setHasXattrs
					test.sync = ``
				}
				test.targetBody = target
				test.targetDatatype = datatypes[j]

				fmt.Println(">>>> source doc exists, target doc exists, target wins CR")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			test.targetWins = false
			test.targetRevId = 3
			test.targetCas = 100000000000
			test.reqT.setOpcode = base.SET_WITH_META
			test.reqT.setCas = test.targetCas
			test.sync = targetSync
			test.respT.Status = mc.SUCCESS
			targets, datatypes, desc, _, syncs, _, err = getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				test.targetBody = target
				test.targetDatatype = datatypes[j]
				test.targetCasChanged = false
				test.casLockingFailure = false
				test.respT.Status = mc.SUCCESS
				if len(syncs[j]) > 0 {
					test.reqT.setHasXattrs = true // target sync is preserved
					test.sync = string(syncs[j])
				} else {
					test.reqT.setHasXattrs = setHasXattrs
					test.sync = ``
				}

				fmt.Println(">>>> source doc exists, target doc exists, source wins CR")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				test.targetCasChanged = true
				test.casLockingFailure = true
				test.respT.Status = mc.KEY_EEXISTS

				fmt.Println(">>>> source doc exists, target doc exists, source wins CR; but target doc gets modified by the time source doc was replicated")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			fmt.Printf("=============== (%v/%v) Done Test: %s ===============\n", n+1, len(mobileOnlyMutationTCs), test.name)
		}
		cluster.Close(nil)

		fmt.Printf("*************** (%v/%v) Done Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])
	}
}

func Test_ECCVAndMobileMutations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Test_ECCVAndMobileMutations")
	}
	if !targetXmemIsUpAndCorrectSetupExists(targetConnStr, targetPort, "") {
		fmt.Println("Skipping Test_ECCVAndMobileMutations since live cluster_run setup has not been detected")
		return
	}

	respTestCh = make(chan respTestParams, 1)
	reqTestCh = make(chan reqTestParams, 1)
	assert := assert.New(t)

	gocbBuckets := []gocb.ConflictResolutionType{"seqno", "lww"}
	xmemBuckets := []base.ConflictResolutionMode{base.CRMode_RevId, base.CRMode_LWW}

	for i := range gocbBuckets {
		fmt.Printf("*************** (%v/%v) Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])

		bucketName := string(gocbBuckets[i])
		cluster, _, err := createBucket(targetConnStr, bucketName, gocbBuckets[i])
		assert.Nil(err)

		for n, test := range eccvOnlyMutationTCsWithCasGreaterThanMaxCas {
			fmt.Printf("=============== (%v/%v) Test: %s ===============\n", n+1, len(eccvOnlyMutationTCsWithCasGreaterThanMaxCas), test.name)

			// reuse eccvOnlyMutationTCs,
			// but since mobile is true, mobile specs extra, sync is always "" to preserve target sync,
			// always has cas locking, NoTargetCR, ADD_WITH_META and subdoc lookup
			test.mobile = true
			test.name = fmt.Sprintf("%s + mobile=on override", test.name)

			setHasXattrs := test.reqT.setHasXattrs

			uniqspecs := make(map[string]bool)
			if test.reqT.specs == nil {
				test.reqT.specs = make([]string, 0)
			}
			for _, spec := range test.reqT.specs {
				uniqspecs[spec] = true
			}
			mobilespecs := []string{base.XattributeToc, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID, base.XATTR_MOBILE}
			for _, spec := range mobilespecs {
				uniqspecs[spec] = true
			}
			specs := make([]string, 0)
			for spec := range uniqspecs {
				specs = append(specs, spec)
			}

			test.reqT.specs = specs
			test.sync = ""
			test.reqT.setOpcode = base.ADD_WITH_META
			test.reqT.casLocking = true
			test.reqT.setNoTargetCR = true
			test.reqT.getOpcode = mc.SUBDOC_MULTI_LOOKUP

			fmt.Println(">>>> source doc exists, target doc doesn't")
			test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

			test.targetRevId = 3
			test.targetCas = 100000000000

			test.targetCasChanged = true
			test.casLockingFailure = true
			test.respT.Status = mc.KEY_EEXISTS
			targets, datatypes, desc, _, syncs, _, err := getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				if len(syncs[j]) > 0 {
					test.reqT.setHasXattrs = true // target sync is preserved
					test.sync = string(syncs[j])
				} else {
					test.reqT.setHasXattrs = setHasXattrs
					test.sync = ``
				}
				test.targetBody = target
				test.targetDatatype = datatypes[j]

				fmt.Println(">>>> source doc exists, target doc doesn't; target docs gets created by the time source doc was replicated")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			test.targetDocExists = true

			test.targetWins = true
			test.targetRevId = 100
			test.targetCas = 9999999999999999999
			test.targetCasChanged = false
			test.casLockingFailure = false
			targets, datatypes, desc, _, syncs, _, err = getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				if len(syncs[j]) > 0 {
					test.reqT.setHasXattrs = true // target sync is preserved
					test.sync = string(syncs[j])
				} else {
					test.reqT.setHasXattrs = setHasXattrs
					test.sync = ``
				}
				test.targetBody = target
				test.targetDatatype = datatypes[j]

				fmt.Println(">>>> source doc exists, target doc exists; target doc wins CR")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			test.targetWins = false
			test.targetRevId = 3
			test.targetCas = 100000000000
			test.respT.Status = mc.SUCCESS
			test.reqT.setOpcode = base.SET_WITH_META
			test.reqT.setCas = test.targetCas
			test.sync = targetSync
			test.reqT.setHasXattrs = true // target sync is preserved
			targets, datatypes, desc, _, syncs, _, err = getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				test.targetCasChanged = false
				test.casLockingFailure = false
				test.respT.Status = mc.SUCCESS

				if len(syncs[j]) > 0 {
					test.reqT.setHasXattrs = true // target sync is preserved
					test.sync = string(syncs[j])
				} else {
					test.reqT.setHasXattrs = setHasXattrs
					test.sync = ``
				}
				test.targetBody = target
				test.targetDatatype = datatypes[j]

				fmt.Println(">>>> source doc exists, target doc exists and source doc wins CR.")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				test.targetCasChanged = true
				test.casLockingFailure = true
				test.respT.Status = mc.KEY_EEXISTS

				fmt.Println(">>>> source doc exists, target doc exists and source doc wins CR; but target doc is changed by the time source doc was replicated")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			fmt.Printf("=============== (%v/%v) Done Test: %s ===============\n", n+1, len(eccvOnlyMutationTCsWithCasGreaterThanMaxCas), test.name)
		}

		for n, test := range eccvOnlyMutationTCsWithCasLessThanMaxCas {
			fmt.Printf("=============== (%v/%v) Test: %s ===============\n", n+1, len(eccvOnlyMutationTCsWithCasLessThanMaxCas), test.name)

			// reuse eccvOnlyMutationTCs,
			// but since mobile is true, mobile specs extra, sync is always "" to preserve target sync,
			// always has cas locking, NoTargetCR, ADD_WITH_META and subdoc lookup
			test.mobile = true
			test.name = fmt.Sprintf("%s + mobile=on override", test.name)

			setHasXattrs := test.reqT.setHasXattrs

			uniqspecs := make(map[string]bool)
			if test.reqT.specs == nil {
				test.reqT.specs = make([]string, 0)
			}
			for _, spec := range test.reqT.specs {
				uniqspecs[spec] = true
			}
			mobilespecs := []string{base.XattributeToc, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID, base.XATTR_MOBILE}
			for _, spec := range mobilespecs {
				uniqspecs[spec] = true
			}
			specs := make([]string, 0)
			for spec := range uniqspecs {
				specs = append(specs, spec)
			}

			test.reqT.specs = specs
			test.sync = ""
			test.reqT.setOpcode = base.ADD_WITH_META
			test.reqT.casLocking = true
			test.reqT.setNoTargetCR = true
			test.reqT.getOpcode = mc.SUBDOC_MULTI_LOOKUP

			fmt.Println(">>>> source doc exists, target doc doesn't")
			test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

			test.targetRevId = 3
			test.targetCas = 100000000000

			test.targetCasChanged = true
			test.casLockingFailure = true
			test.respT.Status = mc.KEY_EEXISTS
			targets, datatypes, desc, _, syncs, _, err := getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				if len(syncs[j]) > 0 {
					test.reqT.setHasXattrs = true // target sync is preserved
					test.sync = string(syncs[j])
				} else {
					test.reqT.setHasXattrs = setHasXattrs
					test.sync = ``
				}
				test.targetBody = target
				test.targetDatatype = datatypes[j]

				fmt.Println(">>>> source doc exists, target doc doesn't; but target doc gets created by the time source doc is replicated")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			test.targetDocExists = true

			test.targetWins = true
			test.targetRevId = 100
			test.targetCas = 9999999999999999999
			test.targetCasChanged = false
			test.casLockingFailure = false
			targets, datatypes, desc, _, syncs, _, err = getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				if len(syncs[j]) > 0 {
					test.reqT.setHasXattrs = true // target sync is preserved
					test.sync = string(syncs[j])
				} else {
					test.reqT.setHasXattrs = setHasXattrs
					test.sync = ``
				}
				test.targetBody = target
				test.targetDatatype = datatypes[j]

				fmt.Println(">>>> source doc exists, target doc exists and target wins CR")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			test.targetWins = false
			test.targetRevId = 3
			test.targetCas = 100000000000
			test.respT.Status = mc.SUCCESS
			test.reqT.setOpcode = base.SET_WITH_META
			test.reqT.setCas = test.targetCas
			test.sync = targetSync
			test.reqT.setHasXattrs = true // target sync is preserved
			targets, datatypes, desc, _, syncs, _, err = getAllTargetBodys(test.targetCas, test.targetRevId, "Target")
			assert.Nil(err)

			for j, target := range targets {
				fmt.Printf("--------------- (%v/%v) Target: %s ---------------\n", j+1, len(targets), desc[j])

				if len(syncs[j]) > 0 {
					test.reqT.setHasXattrs = true // target sync is preserved
					test.sync = string(syncs[j])
				} else {
					test.reqT.setHasXattrs = setHasXattrs
					test.sync = ``
				}
				test.targetBody = target
				test.targetDatatype = datatypes[j]
				test.targetCasChanged = false
				test.casLockingFailure = false
				test.respT.Status = mc.SUCCESS

				fmt.Println(">>>> source doc exists, target doc exists and source wins CR")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				test.targetCasChanged = true
				test.casLockingFailure = true
				test.respT.Status = mc.KEY_EEXISTS

				fmt.Println(">>>> source doc exists, target doc exists and source wins CR; but target doc got changed before source doc was replicated")
				test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

				fmt.Printf("--------------- (%v/%v) Done Target: %s ---------------\n", j+1, len(targets), desc[j])
			}

			fmt.Printf("=============== (%v/%v) Done Test: %s ===============\n", n+1, len(eccvOnlyMutationTCsWithCasLessThanMaxCas), test.name)
		}

		cluster.Close(nil)

		fmt.Printf("*************** (%v/%v) Done Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])
	}
}

func Test_LegacyMutationsCRTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Test_LegacyMutationsCRTest")
	}
	if !targetXmemIsUpAndCorrectSetupExists(targetConnStr, targetPort, "") {
		fmt.Println("Skipping Test_LegacyMutationsCRTest since live cluster_run setup has not been detected")
		return
	}

	// Always use cas and revId for CR for both source and target.
	// Never use cvCas/pRevs for CR.

	respTestCh = make(chan respTestParams, 1)
	reqTestCh = make(chan reqTestParams, 1)
	assert := assert.New(t)

	gocbBuckets := []gocb.ConflictResolutionType{"seqno", "lww"}
	xmemBuckets := []base.ConflictResolutionMode{base.CRMode_RevId, base.CRMode_LWW}

	for i := range gocbBuckets {
		fmt.Printf("*************** (%v/%v) Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])

		bucketName := string(gocbBuckets[i])
		cluster, _, err := createBucket(targetConnStr, bucketName, gocbBuckets[i])
		assert.Nil(err)

		var cas uint64 = 100
		var rev uint64 = 30

		test := mutationTC{
			eccv:   false,
			mobile: false,
			reqT: reqTestParams{
				setOpcode:     mc.SET_WITH_META,
				getOpcode:     mc.GET_META,
				subdocLookup:  false,
				specs:         nil,
				casLocking:    false,
				setNoTargetCR: false,
				setHasXattrs:  true,
			},
			max_cas_delta: 0,
			sync:          sourceSync,
			respT: respTestParams{
				Status: mc.SUCCESS,
			},
			userXattrKey: userXattrKey,
			userXattrVal: userXattr,
		}

		doc := []byte(targetDoc)

		// cas2 > cas1 > cvCas2 > cvCas1 OR rev2 > rev1 > pRev2 > pRev1
		cas2, rev2 := cas-2, rev-2
		cas1, rev1 := cas-4, rev-4
		cvCas2, pRev2 := cas-6, rev-6
		cvCas1, pRev1 := cas-8, rev-8
		assert.True(cas2 > cas1 && cas1 > cvCas2 && cvCas2 > cvCas1)
		assert.True(rev2 > rev1 && rev1 > pRev2 && pRev2 > pRev1)

		// import, non-import - cas1 < cas2 - target wins
		test.name = "1"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 := generateHlv(cvCas2, "Target")
		mou2 := generateMou(cas2-1, pRev2)
		body2, datatype2, err := generateBody(hlv2, mou2, nil, []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cas1 < cas2 - target wins
		test.name = "2"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 < cas2 - target wins
		test.name = "3"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "4"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// cas2 > cas1 > cvCas1 > cvCas2 OR rev2 > rev1 > pRev1 > pRev2
		cas2, rev2 = cas-2, rev-2
		cas1, rev1 = cas-4, rev-4
		cvCas1, pRev1 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas2 > cas1 && cas1 > cvCas1 && cvCas1 > cvCas2)
		assert.True(rev2 > rev1 && rev1 > pRev1 && pRev1 > pRev2)

		// import, non-import - cas1 < cas2 - target wins
		test.name = "5"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cas1 < cas2 - target wins
		test.name = "6"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 < cas2 - target wins
		test.name = "7"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "8"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// cas1 > cas2 > cvCas1 > cvCas2 OR rev1 > rev2 > pRev1 > pRev2
		cas1, rev1 = cas-2, rev-2
		cas2, rev2 = cas-4, rev-4
		cvCas1, pRev1 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas1 > cas2 && cas2 > cvCas1 && cvCas1 > cvCas2)
		assert.True(rev1 > rev2 && rev2 > pRev1 && pRev1 > pRev2)

		// import, non-import - cas1 > cas2 - source wins
		test.name = "9"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5e00000000000000","src":"Source","ver":"0x5e00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"24"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cas1 > cas2 - source wins
		test.name = "10"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5e00000000000000","src":"Source","ver":"0x5e00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"24"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cas2 - source wins
		test.name = "11"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5e00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "12"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5e00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas1 > cvCas1 > cas2 > cvCas2 OR rev1 > pRev1 > rev2 > pRev2
		cas1, rev1 = cas-2, rev-2
		cvCas1, pRev1 = cas-4, rev-4
		cas2, rev2 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas1 > cvCas1 && cvCas1 > cas2 && cas2 > cvCas2)
		assert.True(rev1 > pRev1 && pRev1 > rev2 && rev2 > pRev2)

		// import, non-import - cas1 > cas2 - source wins
		test.name = "13"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x6000000000000000","src":"Source","ver":"0x6000000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"26"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cas1 > cas2 - source wins
		test.name = "14"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x6000000000000000","src":"Source","ver":"0x6000000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"26"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cas2 - source wins
		test.name = "15"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["6000000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "16"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["6000000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas1 > cas2 > cvCas2 > cvCas1 OR rev1 > rev2 > pRev2 > pRev1
		cas1, rev1 = cas-2, rev-2
		cas2, rev2 = cas-4, rev-4
		cvCas2, pRev2 = cas-6, rev-6
		cvCas1, pRev1 = cas-8, rev-8
		assert.True(cas1 > cas2 && cas2 > cvCas2 && cvCas2 > cvCas1)
		assert.True(rev1 > rev2 && rev2 > pRev2 && pRev2 > pRev1)

		// import, non-import - cas1 > cas2 - source wins
		test.name = "17"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5c00000000000000","src":"Source","ver":"0x5c00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"22"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cas1 > cas2 - source wins
		test.name = "18"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5c00000000000000","src":"Source","ver":"0x5c00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"22"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cas2 - source wins
		test.name = "19"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5c00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "20"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5c00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas2 > cvCas2 > cas1 > cvCas1 OR rev2 > pRev2 > rev1 > pRev1
		cas2, rev2 = cas-2, rev-2
		cvCas2, pRev2 = cas-4, rev-4
		cas1, rev1 = cas-6, rev-6
		cvCas1, pRev1 = cas-8, rev-8
		assert.True(cas2 > cvCas2 && cvCas2 > cas1 && cas1 > cvCas1)
		assert.True(rev2 > pRev2 && pRev2 > rev1 && rev1 > pRev1)

		// import, non-import - cas1 < cas2 - target wins
		test.name = "21"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cas1 < cas2 - target wins
		test.name = "22"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 < cas2 - target wins
		test.name = "23"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "24"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		cluster.Close(nil)
		fmt.Printf("*************** (%v/%v) Done Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])
	}
}

func Test_ECCVOnlyMutationsCRTestWithCasGreaterThanMaxCas(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Test_ECCVOnlyMutationsCRTestWithCasGreaterThanMaxCas")
	}
	if !targetXmemIsUpAndCorrectSetupExists(targetConnStr, targetPort, "") {
		fmt.Println("Skipping Test_ECCVOnlyMutationsCRTestWithCasGreaterThanMaxCas since live cluster_run setup has not been detected")
		return
	}

	// Use cvCas and pRev when cas == importCAS for CR.
	// And cas and revId otherwise.

	respTestCh = make(chan respTestParams, 1)
	reqTestCh = make(chan reqTestParams, 1)
	assert := assert.New(t)

	gocbBuckets := []gocb.ConflictResolutionType{"seqno", "lww"}
	xmemBuckets := []base.ConflictResolutionMode{base.CRMode_RevId, base.CRMode_LWW}

	for i := range gocbBuckets {
		fmt.Printf("*************** (%v/%v) Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])

		bucketName := string(gocbBuckets[i])
		cluster, _, err := createBucket(targetConnStr, bucketName, gocbBuckets[i])
		assert.Nil(err)

		var cas uint64 = 100
		var rev uint64 = 30

		test := mutationTC{
			eccv:   true,
			mobile: false,
			reqT: reqTestParams{
				setOpcode:     mc.SET_WITH_META,
				getOpcode:     mc.SUBDOC_MULTI_LOOKUP,
				subdocLookup:  true,
				specs:         []string{base.XATTR_HLV, base.XATTR_IMPORTCAS, base.XATTR_PREVIOUSREV, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID},
				casLocking:    true,
				setNoTargetCR: true,
				setHasXattrs:  true,
			},
			max_cas_delta: -50,
			sync:          sourceSync,
			respT: respTestParams{
				Status: mc.SUCCESS,
			},
			userXattrKey: userXattrKey,
			userXattrVal: userXattr,
		}

		doc := []byte(targetDoc)

		// cas2 > cas1 > cvCas2 > cvCas1 OR rev2 > rev1 > pRev2 > pRev1
		cas2, rev2 := cas-2, rev-2
		cas1, rev1 := cas-4, rev-4
		cvCas2, pRev2 := cas-6, rev-6
		cvCas1, pRev1 := cas-8, rev-8
		assert.True(cas2 > cas1 && cas1 > cvCas2 && cvCas2 > cvCas1)
		assert.True(rev2 > rev1 && rev1 > pRev2 && pRev2 > pRev1)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "1"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 := generateHlv(cvCas2, "Target")
		mou2 := generateMou(cas2-1, pRev2)
		body2, datatype2, err := generateBody(hlv2, mou2, nil, []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 < cvCas2 - target wins
		test.name = "2"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cvCas2 - source wins
		test.name = "3"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlvWithPv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6000000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6000000000000000","pv":["5c00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// same as above, but source PV completely pruned
		test.name = "3a"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1 * 10000000000000000
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1*10000000000000000, "Source")
			mou1 := generateMou(cas1*10000000000000000-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2 * 10000000000000000
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlvWithPv(cvCas2*10000000000000000, "Target")
		mou2 = generateMou(cas2*10000000000000000, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x000060e8e99a520d","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x000060e8e99a520d"}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// same as above, but source PV completely pruned, but pv doesn't exist on target.
		test.name = "3b"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1 * 10000000000000000
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1*10000000000000000, "Source")
			mou1 := generateMou(cas1*10000000000000000-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2 * 10000000000000000
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2*10000000000000000, "Target")
		mou2 = generateMou(cas2*10000000000000000, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x000060e8e99a520d","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x000060e8e99a520d"}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "4"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// cas2 > cas1 > cvCas1 > cvCas2 OR rev2 > rev1 > pRev1 > pRev2
		cas2, rev2 = cas-2, rev-2
		cas1, rev1 = cas-4, rev-4
		cvCas1, pRev1 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas2 > cas1 && cas1 > cvCas1 && cvCas1 > cvCas2)
		assert.True(rev2 > rev1 && rev1 > pRev1 && pRev1 > pRev2)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "5"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 > cvCas2 - source wins
		test.name = "6"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlvWithPv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5e00000000000000","src":"Source","ver":"0x5e00000000000000"}`
		test.mou = string(generateMou(cas1, pRev1))
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// same as above, but source PV is completely pruned
		test.name = "6a"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1 * 10000000000000000
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1*10000000000000000, "Source")
			mou1 := generateMou(cas1*10000000000000000, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2 * 10000000000000000
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlvWithPv(cvCas2*10000000000000000, "Target")
		mou2 = generateMou(cas2*10000000000000000, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x0000de08058d0b0d","src":"Source","ver":"0x0000de08058d0b0d"}`
		test.mou = string(generateMou(cas1*10000000000000000, pRev1))
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// same as above, but source PV is completely pruned and pv doesn't exist on target
		test.name = "6b"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1 * 10000000000000000
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1*10000000000000000, "Source")
			mou1 := generateMou(cas1*10000000000000000, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2 * 10000000000000000
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2*10000000000000000, "Target")
		mou2 = generateMou(cas2*10000000000000000, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x0000de08058d0b0d","src":"Source","ver":"0x0000de08058d0b0d"}`
		test.mou = string(generateMou(cas1*10000000000000000, pRev1))
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cvCas2 - source wins
		test.name = "7"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlvWithPv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6000000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6000000000000000","pv":["5e00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// same as above, but source PV is completely pruned
		test.name = "7a"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1 * 10000000000000000
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1*10000000000000000, "Source")
			mou1 := generateMou(cas1*10000000000000000-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2 * 10000000000000000
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlvWithPv(cvCas2*10000000000000000, "Target")
		mou2 = generateMou(cas2*10000000000000000, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x000060e8e99a520d","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x000060e8e99a520d"}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// same as above, but source PV is completely pruned and pv doesnt exists on target
		test.name = "7b"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1 * 10000000000000000
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1*10000000000000000, "Source")
			mou1 := generateMou(cas1*10000000000000000-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2 * 10000000000000000
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2*10000000000000000, "Target")
		mou2 = generateMou(cas2*10000000000000000, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x000060e8e99a520d","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x000060e8e99a520d"}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "8"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// cas1 > cas2 > cvCas1 > cvCas2 OR rev1 > rev2 > pRev1 > pRev2
		cas1, rev1 = cas-2, rev-2
		cas2, rev2 = cas-4, rev-4
		cvCas1, pRev1 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas1 > cas2 && cas2 > cvCas1 && cvCas1 > cvCas2)
		assert.True(rev1 > rev2 && rev2 > pRev1 && pRev1 > pRev2)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "9"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5e00000000000000","src":"Source","ver":"0x5e00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"24"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 > cvCas2 - source wins
		test.name = "10"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5e00000000000000","src":"Source","ver":"0x5e00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"24"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cvCas2 - source wins
		test.name = "11"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5e00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "12"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5e00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas1 > cvCas1 > cas2 > cvCas2 OR rev1 > pRev1 > rev2 > pRev2
		cas1, rev1 = cas-2, rev-2
		cvCas1, pRev1 = cas-4, rev-4
		cas2, rev2 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas1 > cvCas1 && cvCas1 > cas2 && cas2 > cvCas2)
		assert.True(rev1 > pRev1 && pRev1 > rev2 && rev2 > pRev2)

		// import, non-import - cvCas1 > cas2 - source wins
		test.name = "13"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x6000000000000000","src":"Source","ver":"0x6000000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"26"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 > cvCas2 - source wins
		test.name = "14"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x6000000000000000","src":"Source","ver":"0x6000000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"26"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cvCas2 - source wins
		test.name = "15"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["6000000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "16"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["6000000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas1 > cas2 > cvCas2 > cvCas1 OR rev1 > rev2 > pRev2 > pRev1
		cas1, rev1 = cas-2, rev-2
		cas2, rev2 = cas-4, rev-4
		cvCas2, pRev2 = cas-6, rev-6
		cvCas1, pRev1 = cas-8, rev-8
		assert.True(cas1 > cas2 && cas2 > cvCas2 && cvCas2 > cvCas1)
		assert.True(rev1 > rev2 && rev2 > pRev2 && pRev2 > pRev1)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "17"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5c00000000000000","src":"Source","ver":"0x5c00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"22"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 < cvCas2 - target wins
		test.name = "18"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.reqT.setCas = test.targetCas
		test.hlv = `{"cvCas":"0x5c00000000000000","src":"Source","ver":"0x5c00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"22"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cvCas2 - source wins
		test.name = "19"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5c00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "20"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5c00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas2 > cvCas2 > cas1 > cvCas1 OR rev2 > pRev2 > rev1 > pRev1
		cas2, rev2 = cas-2, rev-2
		cvCas2, pRev2 = cas-4, rev-4
		cas1, rev1 = cas-6, rev-6
		cvCas1, pRev1 = cas-8, rev-8
		assert.True(cas2 > cvCas2 && cvCas2 > cas1 && cas1 > cvCas1)
		assert.True(rev2 > pRev2 && pRev2 > rev1 && rev1 > pRev1)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "21"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 < cvCas2 - target wins
		test.name = "22"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 < cvCas2 - target wins
		test.name = "23"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "24"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		cluster.Close(nil)
		fmt.Printf("*************** (%v/%v) Done Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])
	}
}

func Test_ECCVOnlyMutationsCRTestWithCasLessThanMaxCas(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Test_ECCVOnlyMutationsCRTestWithCasLessThanMaxCas")
	}
	if !targetXmemIsUpAndCorrectSetupExists(targetConnStr, targetPort, "") {
		fmt.Println("Skipping Test_ECCVOnlyMutationsCRTestWithCasLessThanMaxCas since live cluster_run setup has not been detected")
		return
	}

	// Always use cas and revId for CR for both source and target.
	// Never use cvCas/pRevs.

	respTestCh = make(chan respTestParams, 1)
	reqTestCh = make(chan reqTestParams, 1)
	assert := assert.New(t)

	gocbBuckets := []gocb.ConflictResolutionType{"seqno", "lww"}
	xmemBuckets := []base.ConflictResolutionMode{base.CRMode_RevId, base.CRMode_LWW}

	for i := range gocbBuckets {
		fmt.Printf("*************** (%v/%v) Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])

		bucketName := string(gocbBuckets[i])
		cluster, _, err := createBucket(targetConnStr, bucketName, gocbBuckets[i])
		assert.Nil(err)

		var cas uint64 = 100
		var rev uint64 = 30

		test := mutationTC{
			eccv:   true,
			mobile: false,
			reqT: reqTestParams{
				setOpcode:     mc.SET_WITH_META,
				getOpcode:     base.GET_WITH_META,
				subdocLookup:  false,
				specs:         nil,
				casLocking:    false,
				setNoTargetCR: false,
				setHasXattrs:  true,
			},
			max_cas_delta: 50, // cas < max_cas
			sync:          sourceSync,
			respT: respTestParams{
				Status: mc.SUCCESS,
			},
			userXattrKey: userXattrKey,
			userXattrVal: userXattr,
		}

		doc := []byte(targetDoc)

		// cas2 > cas1 > cvCas2 > cvCas1 OR rev2 > rev1 > pRev2 > pRev1
		cas2, rev2 := cas-2, rev-2
		cas1, rev1 := cas-4, rev-4
		cvCas2, pRev2 := cas-6, rev-6
		cvCas1, pRev1 := cas-8, rev-8
		assert.True(cas2 > cas1 && cas1 > cvCas2 && cvCas2 > cvCas1)
		assert.True(rev2 > rev1 && rev1 > pRev2 && pRev2 > pRev1)

		// import, non-import - cas1 < cas2 - target wins
		test.name = "1"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 := generateHlv(cvCas2, "Target")
		mou2 := generateMou(cas2-1, pRev2)
		body2, datatype2, err := generateBody(hlv2, mou2, nil, []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cas1 < cas2 - target wins
		test.name = "2"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 < cas2 - target wins
		test.name = "3"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "4"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// cas2 > cas1 > cvCas1 > cvCas2 OR rev2 > rev1 > pRev1 > pRev2
		cas2, rev2 = cas-2, rev-2
		cas1, rev1 = cas-4, rev-4
		cvCas1, pRev1 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas2 > cas1 && cas1 > cvCas1 && cvCas1 > cvCas2)
		assert.True(rev2 > rev1 && rev1 > pRev1 && pRev1 > pRev2)

		// import, non-import - cas1 < cas2 - target wins
		test.name = "5"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cas1 < cas2 - target wins
		test.name = "6"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 < cas2 - target wins
		test.name = "7"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "8"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// cas1 > cas2 > cvCas1 > cvCas2 OR rev1 > rev2 > pRev1 > pRev2
		cas1, rev1 = cas-2, rev-2
		cas2, rev2 = cas-4, rev-4
		cvCas1, pRev1 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas1 > cas2 && cas2 > cvCas1 && cvCas1 > cvCas2)
		assert.True(rev1 > rev2 && rev2 > pRev1 && pRev1 > pRev2)

		// import, non-import - cas1 > cas2 - source wins
		test.name = "9"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5e00000000000000","src":"Source","ver":"0x5e00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"24"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cas1 > cas2 - source wins
		test.name = "10"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5e00000000000000","src":"Source","ver":"0x5e00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"24"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cas2 - source wins
		test.name = "11"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5e00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "12"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5e00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas1 > cvCas1 > cas2 > cvCas2 OR rev1 > pRev1 > rev2 > pRev2
		cas1, rev1 = cas-2, rev-2
		cvCas1, pRev1 = cas-4, rev-4
		cas2, rev2 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas1 > cvCas1 && cvCas1 > cas2 && cas2 > cvCas2)
		assert.True(rev1 > pRev1 && pRev1 > rev2 && rev2 > pRev2)

		// import, non-import - cas1 > cas2 - source wins
		test.name = "13"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x6000000000000000","src":"Source","ver":"0x6000000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"26"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cas1 > cas2 - source wins
		test.name = "14"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x6000000000000000","src":"Source","ver":"0x6000000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"26"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cas2 - source wins
		test.name = "15"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["6000000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "16"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["6000000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas1 > cas2 > cvCas2 > cvCas1 OR rev1 > rev2 > pRev2 > pRev1
		cas1, rev1 = cas-2, rev-2
		cas2, rev2 = cas-4, rev-4
		cvCas2, pRev2 = cas-6, rev-6
		cvCas1, pRev1 = cas-8, rev-8
		assert.True(cas1 > cas2 && cas2 > cvCas2 && cvCas2 > cvCas1)
		assert.True(rev1 > rev2 && rev2 > pRev2 && pRev2 > pRev1)

		// import, non-import - cas1 > cas2 - source wins
		test.name = "17"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5c00000000000000","src":"Source","ver":"0x5c00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"22"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cas1 > cas2 - source wins
		test.name = "18"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5c00000000000000","src":"Source","ver":"0x5c00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"22"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cas2 - source wins
		test.name = "19"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5c00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "20"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5c00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas2 > cvCas2 > cas1 > cvCas1 OR rev2 > pRev2 > rev1 > pRev1
		cas2, rev2 = cas-2, rev-2
		cvCas2, pRev2 = cas-4, rev-4
		cas1, rev1 = cas-6, rev-6
		cvCas1, pRev1 = cas-8, rev-8
		assert.True(cas2 > cvCas2 && cvCas2 > cas1 && cas1 > cvCas1)
		assert.True(rev2 > pRev2 && pRev2 > rev1 && rev1 > pRev1)

		// import, non-import - cas1 < cas2 - target wins
		test.name = "21"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cas1 < cas2 - target wins
		test.name = "22"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 < cas2 - target wins
		test.name = "23"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "24"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		cluster.Close(nil)
		fmt.Printf("*************** (%v/%v) Done Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])
	}
}

func Test_MobileOnlyMutationsCRTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Test_MobileOnlyMutationsCRTest")
	}
	if !targetXmemIsUpAndCorrectSetupExists(targetConnStr, targetPort, "") {
		fmt.Println("Skipping Test_MobileOnlyMutationsCRTest since live cluster_run setup has not been detected")
		return
	}

	// need to always use cas and revId of target for CR.
	// use cvCas and pRev for source import mutations for CR.

	respTestCh = make(chan respTestParams, 1)
	reqTestCh = make(chan reqTestParams, 1)
	assert := assert.New(t)

	gocbBuckets := []gocb.ConflictResolutionType{"seqno", "lww"}
	xmemBuckets := []base.ConflictResolutionMode{base.CRMode_RevId, base.CRMode_LWW}

	for i := range gocbBuckets {
		fmt.Printf("*************** (%v/%v) Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])

		bucketName := string(gocbBuckets[i])
		cluster, _, err := createBucket(targetConnStr, bucketName, gocbBuckets[i])
		assert.Nil(err)

		var cas uint64 = 100
		var rev uint64 = 30

		test := mutationTC{
			eccv:   false,
			mobile: true,
			reqT: reqTestParams{
				setOpcode:     mc.SET_WITH_META,
				getOpcode:     mc.SUBDOC_MULTI_LOOKUP,
				subdocLookup:  true,
				specs:         []string{base.XattributeToc, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID, base.XATTR_MOBILE},
				casLocking:    true,
				setNoTargetCR: true,
				setHasXattrs:  true,
			},
			max_cas_delta: 0,
			sync:          targetSync,
			respT: respTestParams{
				Status: mc.SUCCESS,
			},
			userXattrKey: userXattrKey,
			userXattrVal: userXattr,
		}

		doc := []byte(targetDoc)

		// cas2 > cas1 > cvCas2 > cvCas1 OR rev2 > rev1 > pRev2 > pRev1
		cas2, rev2 := cas-2, rev-2
		cas1, rev1 := cas-4, rev-4
		cvCas2, pRev2 := cas-6, rev-6
		cvCas1, pRev1 := cas-8, rev-8
		assert.True(cas2 > cas1 && cas1 > cvCas2 && cvCas2 > cvCas1)
		assert.True(rev2 > rev1 && rev1 > pRev2 && pRev2 > pRev1)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "1"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 := generateHlv(cvCas2, "Target")
		mou2 := generateMou(cas2-1, pRev2)
		body2, datatype2, err := generateBody(hlv2, mou2, nil, []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 < cas2 - target wins
		test.name = "2"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 < cas2 - target wins
		test.name = "3"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "4"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// cas2 > cas1 > cvCas1 > cvCas2 OR rev2 > rev1 > pRev1 > pRev2
		cas2, rev2 = cas-2, rev-2
		cas1, rev1 = cas-4, rev-4
		cvCas1, pRev1 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas2 > cas1 && cas1 > cvCas1 && cvCas1 > cvCas2)
		assert.True(rev2 > rev1 && rev1 > pRev1 && pRev1 > pRev2)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "5"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 < cas2 - target wins
		test.name = "6"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 < cas2 - target wins
		test.name = "7"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "8"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// cas1 > cas2 > cvCas1 > cvCas2 OR rev1 > rev2 > pRev1 > pRev2
		cas1, rev1 = cas-2, rev-2
		cas2, rev2 = cas-4, rev-4
		cvCas1, pRev1 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas1 > cas2 && cas2 > cvCas1 && cvCas1 > cvCas2)
		assert.True(rev1 > rev2 && rev2 > pRev1 && pRev1 > pRev2)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "9"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5e00000000000000","src":"Source","ver":"0x5e00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"24"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 < cas2 - target wins
		test.name = "10"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5e00000000000000","src":"Source","ver":"0x5e00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"24"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cas2 - source wins
		test.name = "11"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5e00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "12"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5e00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas1 > cvCas1 > cas2 > cvCas2 OR rev1 > pRev1 > rev2 > pRev2
		cas1, rev1 = cas-2, rev-2
		cvCas1, pRev1 = cas-4, rev-4
		cas2, rev2 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas1 > cvCas1 && cvCas1 > cas2 && cas2 > cvCas2)
		assert.True(rev1 > pRev1 && pRev1 > rev2 && rev2 > pRev2)

		// import, non-import - cvCas1 > cas2 - source wins
		test.name = "13"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x6000000000000000","src":"Source","ver":"0x6000000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"26"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 > cas2 - source wins
		test.name = "14"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x6000000000000000","src":"Source","ver":"0x6000000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"26"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cas2 - source wins
		test.name = "15"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["6000000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "16"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["6000000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas1 > cas2 > cvCas2 > cvCas1 OR rev1 > rev2 > pRev2 > pRev1
		cas1, rev1 = cas-2, rev-2
		cas2, rev2 = cas-4, rev-4
		cvCas2, pRev2 = cas-6, rev-6
		cvCas1, pRev1 = cas-8, rev-8
		assert.True(cas1 > cas2 && cas2 > cvCas2 && cvCas2 > cvCas1)
		assert.True(rev1 > rev2 && rev2 > pRev2 && pRev2 > pRev1)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "17"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5c00000000000000","src":"Source","ver":"0x5c00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"22"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 < cas2 - target wins
		test.name = "18"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5c00000000000000","src":"Source","ver":"0x5c00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"22"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cas2 - source wins
		test.name = "19"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5c00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "20"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5c00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas2 > cvCas2 > cas1 > cvCas1 OR rev2 > pRev2 > rev1 > pRev1
		cas2, rev2 = cas-2, rev-2
		cvCas2, pRev2 = cas-4, rev-4
		cas1, rev1 = cas-6, rev-6
		cvCas1, pRev1 = cas-8, rev-8
		assert.True(cas2 > cvCas2 && cvCas2 > cas1 && cas1 > cvCas1)
		assert.True(rev2 > pRev2 && pRev2 > rev1 && rev1 > pRev1)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "21"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 < cas2 - target wins
		test.name = "22"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 < cas2 - target wins
		test.name = "23"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "24"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		cluster.Close(nil)
		fmt.Printf("*************** (%v/%v) Done Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])
	}
}
func Test_ECCVAndMobileOnlyMutationsCRTestWithCasGreaterThanMaxCas(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Test_ECCVAndMobileOnlyMutationsCRTestWithCasGreaterThanMaxCas")
	}
	if !targetXmemIsUpAndCorrectSetupExists(targetConnStr, targetPort, "") {
		fmt.Println("Skipping Test_ECCVAndMobileOnlyMutationsCRTestWithCasGreaterThanMaxCas since live cluster_run setup has not been detected")
		return
	}

	// Use cvCas and pRev for CR when cas == importCAS
	// Use cas and revId otherwise.

	respTestCh = make(chan respTestParams, 1)
	reqTestCh = make(chan reqTestParams, 1)
	assert := assert.New(t)

	gocbBuckets := []gocb.ConflictResolutionType{"seqno", "lww"}
	xmemBuckets := []base.ConflictResolutionMode{base.CRMode_RevId, base.CRMode_LWW}

	for i := range gocbBuckets {
		fmt.Printf("*************** (%v/%v) Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])

		bucketName := string(gocbBuckets[i])
		cluster, _, err := createBucket(targetConnStr, bucketName, gocbBuckets[i])
		assert.Nil(err)

		var cas uint64 = 100
		var rev uint64 = 30

		test := mutationTC{
			eccv:   true,
			mobile: true,
			reqT: reqTestParams{
				setOpcode:     mc.SET_WITH_META,
				getOpcode:     mc.SUBDOC_MULTI_LOOKUP,
				subdocLookup:  true,
				specs:         []string{base.XattributeToc, base.XATTR_HLV, base.XATTR_IMPORTCAS, base.XATTR_PREVIOUSREV, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID, base.XATTR_MOBILE},
				casLocking:    true,
				setNoTargetCR: true,
				setHasXattrs:  true,
			},
			max_cas_delta: -50,
			sync:          targetSync,
			respT: respTestParams{
				Status: mc.SUCCESS,
			},
			userXattrKey: userXattrKey,
			userXattrVal: userXattr,
		}

		doc := []byte(targetDoc)

		// cas2 > cas1 > cvCas2 > cvCas1 OR rev2 > rev1 > pRev2 > pRev1
		cas2, rev2 := cas-2, rev-2
		cas1, rev1 := cas-4, rev-4
		cvCas2, pRev2 := cas-6, rev-6
		cvCas1, pRev1 := cas-8, rev-8
		assert.True(cas2 > cas1 && cas1 > cvCas2 && cvCas2 > cvCas1)
		assert.True(rev2 > rev1 && rev1 > pRev2 && pRev2 > pRev1)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "1"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 := generateHlv(cvCas2, "Target")
		mou2 := generateMou(cas2-1, pRev2)
		body2, datatype2, err := generateBody(hlv2, mou2, nil, []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 < cvCas2 - target wins
		test.name = "2"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cvCas2 - source wins
		test.name = "3"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		test.reqT.setOpcode = base.SUBDOC_MULTI_MUTATION
		hlv2 = generateHlvWithPv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.hlvOutdated = true
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.reqT.setOpcode = base.SET_WITH_META
		test.hlvOutdated = false

		// same as above, but pv is completely pruned on source
		test.name = "3a"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1 * 10000000000000000
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1*10000000000000000, "Source")
			mou1 := generateMou(cas1*10000000000000000-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2 * 10000000000000000
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		test.reqT.setOpcode = base.SUBDOC_MULTI_MUTATION
		hlv2 = generateHlvWithPv(cvCas2*10000000000000000, "Target")
		mou2 = generateMou(cas2*10000000000000000, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.hlvOutdated = true
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.reqT.setOpcode = base.SET_WITH_META
		test.hlvOutdated = false

		// same as above, but pv is completely pruned on source and target PV doesn't exist.
		test.name = "3b"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1 * 10000000000000000
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1*10000000000000000, "Source")
			mou1 := generateMou(cas1*10000000000000000-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2 * 10000000000000000
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		test.reqT.setOpcode = base.SUBDOC_MULTI_MUTATION
		hlv2 = generateHlv(cvCas2*10000000000000000, "Target")
		mou2 = generateMou(cas2*10000000000000000, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.hlvOutdated = true
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.reqT.setOpcode = base.SET_WITH_META
		test.hlvOutdated = false

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "4"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// cas2 > cas1 > cvCas1 > cvCas2 OR rev2 > rev1 > pRev1 > pRev2
		cas2, rev2 = cas-2, rev-2
		cas1, rev1 = cas-4, rev-4
		cvCas1, pRev1 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas2 > cas1 && cas1 > cvCas1 && cvCas1 > cvCas2)
		assert.True(rev2 > rev1 && rev1 > pRev1 && pRev1 > pRev2)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "5"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 > cvCas2 - source wins
		test.name = "6"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setOpcode = base.SUBDOC_MULTI_MUTATION
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlvWithPv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = string(generateMou(cas1, pRev1))
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.reqT.setOpcode = base.SET_WITH_META

		// same as above but source pv is completely pruned
		test.name = "6a"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1 * 10000000000000000
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1*10000000000000000, "Source")
			mou1 := generateMou(cas1*10000000000000000, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2 * 10000000000000000
		test.targetRevId = rev2
		test.reqT.setOpcode = base.SUBDOC_MULTI_MUTATION
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlvWithPv(cvCas2*10000000000000000, "Target")
		mou2 = generateMou(cas2*10000000000000000, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = string(generateMou(cas1*10000000000000000, pRev1))
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.reqT.setOpcode = base.SET_WITH_META

		// same as above but source pv is completely pruned and PV doesn't exists on target
		test.name = "6b"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1 * 10000000000000000
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1*10000000000000000, "Source")
			mou1 := generateMou(cas1*10000000000000000, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2 * 10000000000000000
		test.targetRevId = rev2
		test.reqT.setOpcode = base.SUBDOC_MULTI_MUTATION
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2*10000000000000000, "Target")
		mou2 = generateMou(cas2*10000000000000000, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = string(generateMou(cas1*10000000000000000, pRev1))
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.reqT.setOpcode = base.SET_WITH_META

		// non-import, import - cas1 > cvCas2 - source wins
		test.name = "7"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		test.reqT.setOpcode = base.SUBDOC_MULTI_MUTATION
		hlv2 = generateHlvWithPv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.hlvOutdated = true
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.reqT.setOpcode = base.SET_WITH_META
		test.hlvOutdated = false

		// same as above, but source pv completely pruned
		test.name = "7a"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1 * 10000000000000000
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1*10000000000000000, "Source")
			mou1 := generateMou(cas1*10000000000000000-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2 * 10000000000000000
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		test.reqT.setOpcode = base.SUBDOC_MULTI_MUTATION
		hlv2 = generateHlvWithPv(cvCas2*10000000000000000, "Target")
		mou2 = generateMou(cas2*10000000000000000, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.hlvOutdated = true
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.reqT.setOpcode = base.SET_WITH_META
		test.hlvOutdated = false

		// same as above, but source pv completely pruned and pv doesn't exist on target
		test.name = "7b"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1 * 10000000000000000
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1*10000000000000000, "Source")
			mou1 := generateMou(cas1*10000000000000000-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2 * 10000000000000000
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		test.reqT.setOpcode = base.SUBDOC_MULTI_MUTATION
		hlv2 = generateHlv(cvCas2*10000000000000000, "Target")
		mou2 = generateMou(cas2*10000000000000000, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.hlvOutdated = true
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.reqT.setOpcode = base.SET_WITH_META
		test.hlvOutdated = false

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "8"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// cas1 > cas2 > cvCas1 > cvCas2 OR rev1 > rev2 > pRev1 > pRev2
		cas1, rev1 = cas-2, rev-2
		cas2, rev2 = cas-4, rev-4
		cvCas1, pRev1 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas1 > cas2 && cas2 > cvCas1 && cvCas1 > cvCas2)
		assert.True(rev1 > rev2 && rev2 > pRev1 && pRev1 > pRev2)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "9"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5e00000000000000","src":"Source","ver":"0x5e00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"24"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 > cvCas2 - source wins
		test.name = "10"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5e00000000000000","src":"Source","ver":"0x5e00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"24"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cvCas2 - source wins
		test.name = "11"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5e00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "12"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5e00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas1 > cvCas1 > cas2 > cvCas2 OR rev1 > pRev1 > rev2 > pRev2
		cas1, rev1 = cas-2, rev-2
		cvCas1, pRev1 = cas-4, rev-4
		cas2, rev2 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas1 > cvCas1 && cvCas1 > cas2 && cas2 > cvCas2)
		assert.True(rev1 > pRev1 && pRev1 > rev2 && rev2 > pRev2)

		// import, non-import - cvCas1 > cas2 - source wins
		test.name = "13"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x6000000000000000","src":"Source","ver":"0x6000000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"26"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 > cvCas2 - source wins
		test.name = "14"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x6000000000000000","src":"Source","ver":"0x6000000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"26"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cvCas2 - source wins
		test.name = "15"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["6000000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "16"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["6000000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas1 > cas2 > cvCas2 > cvCas1 OR rev1 > rev2 > pRev2 > pRev1
		cas1, rev1 = cas-2, rev-2
		cas2, rev2 = cas-4, rev-4
		cvCas2, pRev2 = cas-6, rev-6
		cvCas1, pRev1 = cas-8, rev-8
		assert.True(cas1 > cas2 && cas2 > cvCas2 && cvCas2 > cvCas1)
		assert.True(rev1 > rev2 && rev2 > pRev2 && pRev2 > pRev1)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "17"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5c00000000000000","src":"Source","ver":"0x5c00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"22"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 < cvCas2 - target wins
		test.name = "18"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.reqT.setCas = test.targetCas
		test.hlv = `{"cvCas":"0x5c00000000000000","src":"Source","ver":"0x5c00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"22"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cvCas2 - source wins
		test.name = "19"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5c00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "20"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5c00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas2 > cvCas2 > cas1 > cvCas1 OR rev2 > pRev2 > rev1 > pRev1
		cas2, rev2 = cas-2, rev-2
		cvCas2, pRev2 = cas-4, rev-4
		cas1, rev1 = cas-6, rev-6
		cvCas1, pRev1 = cas-8, rev-8
		assert.True(cas2 > cvCas2 && cvCas2 > cas1 && cas1 > cvCas1)
		assert.True(rev2 > pRev2 && pRev2 > rev1 && rev1 > pRev1)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "21"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 < cvCas2 - target wins
		test.name = "22"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 < cvCas2 - target wins
		test.name = "23"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "24"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		cluster.Close(nil)
		fmt.Printf("*************** (%v/%v) Done Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])
	}
}

func Test_ECCVAndMobileOnlyMutationsCRTestWithCasLessThanMaxCas(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Test_ECCVAndMobileOnlyMutationsCRTestWithCasLessThanMaxCas")
	}
	if !targetXmemIsUpAndCorrectSetupExists(targetConnStr, targetPort, "") {
		fmt.Println("Skipping Test_ECCVAndMobileOnlyMutationsCRTestWithCasLessThanMaxCas since live cluster_run setup has not been detected")
		return
	}

	// need to always use cas and revId of target for CR.
	// use cvCas and pRev for source import mutations for CR.

	respTestCh = make(chan respTestParams, 1)
	reqTestCh = make(chan reqTestParams, 1)
	assert := assert.New(t)

	gocbBuckets := []gocb.ConflictResolutionType{"seqno", "lww"}
	xmemBuckets := []base.ConflictResolutionMode{base.CRMode_RevId, base.CRMode_LWW}

	for i := range gocbBuckets {
		fmt.Printf("*************** (%v/%v) Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])

		bucketName := string(gocbBuckets[i])
		cluster, _, err := createBucket(targetConnStr, bucketName, gocbBuckets[i])
		assert.Nil(err)

		var cas uint64 = 100
		var rev uint64 = 30

		test := mutationTC{
			eccv:   true,
			mobile: true,
			reqT: reqTestParams{
				setOpcode:     mc.SET_WITH_META,
				getOpcode:     mc.SUBDOC_MULTI_LOOKUP,
				subdocLookup:  true,
				specs:         []string{base.XattributeToc, base.VXATTR_DATATYPE, base.VXATTR_FLAGS, base.VXATTR_EXPIRY, base.VXATTR_REVID, base.XATTR_MOBILE},
				casLocking:    true,
				setNoTargetCR: true,
				setHasXattrs:  true,
			},
			max_cas_delta: 50, // cas < max_cas
			sync:          targetSync,
			respT: respTestParams{
				Status: mc.SUCCESS,
			},
			userXattrKey: userXattrKey,
			userXattrVal: userXattr,
		}

		doc := []byte(targetDoc)

		// cas2 > cas1 > cvCas2 > cvCas1 OR rev2 > rev1 > pRev2 > pRev1
		cas2, rev2 := cas-2, rev-2
		cas1, rev1 := cas-4, rev-4
		cvCas2, pRev2 := cas-6, rev-6
		cvCas1, pRev1 := cas-8, rev-8
		assert.True(cas2 > cas1 && cas1 > cvCas2 && cvCas2 > cvCas1)
		assert.True(rev2 > rev1 && rev1 > pRev2 && pRev2 > pRev1)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "1"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 := generateHlv(cvCas2, "Target")
		mou2 := generateMou(cas2-1, pRev2)
		body2, datatype2, err := generateBody(hlv2, mou2, nil, []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 < cas2 - target wins
		test.name = "2"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 < cas2 - target wins
		test.name = "3"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "4"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// cas2 > cas1 > cvCas1 > cvCas2 OR rev2 > rev1 > pRev1 > pRev2
		cas2, rev2 = cas-2, rev-2
		cas1, rev1 = cas-4, rev-4
		cvCas1, pRev1 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas2 > cas1 && cas1 > cvCas1 && cvCas1 > cvCas2)
		assert.True(rev2 > rev1 && rev1 > pRev1 && pRev1 > pRev2)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "5"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 < cas2 - target wins
		test.name = "6"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 < cas2 - target wins
		test.name = "7"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "8"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// cas1 > cas2 > cvCas1 > cvCas2 OR rev1 > rev2 > pRev1 > pRev2
		cas1, rev1 = cas-2, rev-2
		cas2, rev2 = cas-4, rev-4
		cvCas1, pRev1 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas1 > cas2 && cas2 > cvCas1 && cvCas1 > cvCas2)
		assert.True(rev1 > rev2 && rev2 > pRev1 && pRev1 > pRev2)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "9"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5e00000000000000","src":"Source","ver":"0x5e00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"24"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 < cas2 - target wins
		test.name = "10"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5e00000000000000","src":"Source","ver":"0x5e00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"24"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cas2 - source wins
		test.name = "11"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5e00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "12"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5e00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas1 > cvCas1 > cas2 > cvCas2 OR rev1 > pRev1 > rev2 > pRev2
		cas1, rev1 = cas-2, rev-2
		cvCas1, pRev1 = cas-4, rev-4
		cas2, rev2 = cas-6, rev-6
		cvCas2, pRev2 = cas-8, rev-8
		assert.True(cas1 > cvCas1 && cvCas1 > cas2 && cas2 > cvCas2)
		assert.True(rev1 > pRev1 && pRev1 > rev2 && rev2 > pRev2)

		// import, non-import - cvCas1 > cas2 - source wins
		test.name = "13"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x6000000000000000","src":"Source","ver":"0x6000000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"26"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 > cas2 - source wins
		test.name = "14"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x6000000000000000","src":"Source","ver":"0x6000000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"26"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cas2 - source wins
		test.name = "15"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["6000000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "16"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["6000000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas1 > cas2 > cvCas2 > cvCas1 OR rev1 > rev2 > pRev2 > pRev1
		cas1, rev1 = cas-2, rev-2
		cas2, rev2 = cas-4, rev-4
		cvCas2, pRev2 = cas-6, rev-6
		cvCas1, pRev1 = cas-8, rev-8
		assert.True(cas1 > cas2 && cas2 > cvCas2 && cvCas2 > cvCas1)
		assert.True(rev1 > rev2 && rev2 > pRev2 && pRev2 > pRev1)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "17"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5c00000000000000","src":"Source","ver":"0x5c00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"22"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 < cas2 - target wins
		test.name = "18"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = `{"cvCas":"0x5c00000000000000","src":"Source","ver":"0x5c00000000000000"}`
		test.mou = `{"cas":"0x6200000000000000","pRev":"22"}`
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 > cas2 - source wins
		test.name = "19"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5c00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// non-import, non-import - cas1 > cas2 - source wins
		test.name = "20"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = false
		test.targetCas = cas2
		test.targetRevId = rev2
		test.reqT.setCas = test.targetCas
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlvOutdated = true
		test.hlv = `{"cvCas":"0x6200000000000000","src":"D5j49m2akBIzCHraw6p0Gw","ver":"0x6200000000000000","pv":["5c00000000000000@Source"]}`
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)
		test.hlvOutdated = false

		// cas2 > cvCas2 > cas1 > cvCas1 OR rev2 > pRev2 > rev1 > pRev1
		cas2, rev2 = cas-2, rev-2
		cvCas2, pRev2 = cas-4, rev-4
		cas1, rev1 = cas-6, rev-6
		cvCas1, pRev1 = cas-8, rev-8
		assert.True(cas2 > cvCas2 && cvCas2 > cas1 && cas1 > cvCas1)
		assert.True(rev2 > pRev2 && pRev2 > rev1 && rev1 > pRev1)

		// import, non-import - cvCas1 < cas2 - target wins
		test.name = "21"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// import, import - cvCas1 < cas2 - target wins
		test.name = "22"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, import - cas1 < cas2 - target wins
		test.name = "23"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		// non-import, non-import - cas1 < cas2 - target wins
		test.name = "24"
		test.getUprEvent = func() (*mcc.UprEvent, error) {
			uprEvent := mcc.UprEvent{}
			uprEvent.Cas = cas1
			uprEvent.RevSeqno = rev1
			hlv1 := generateHlv(cvCas1, "Source")
			mou1 := generateMou(cas1-1, pRev1)
			body1, datatype1, err := generateBody(hlv1, mou1, []byte(sourceSync), []byte(userXattrKey), []byte(userXattr), nil, nil, doc)
			if err != nil {
				return nil, err
			}

			uprEvent.DataType = datatype1
			uprEvent.Value = body1
			uprEvent.ValueLen = len(body1)
			uprEvent.Opcode = mc.UPR_MUTATION

			return &uprEvent, nil
		}
		test.targetDocExists = true
		test.targetWins = true
		test.targetCas = cas2
		test.targetRevId = rev2
		hlv2 = generateHlv(cvCas2, "Target")
		mou2 = generateMou(cas2-1, pRev2)
		body2, datatype2, err = generateBody(hlv2, mou2, []byte(targetSync), []byte(xattrK1), []byte(xattrV1), []byte(xattrK2), []byte(xattrV2), doc)
		assert.Nil(err)
		test.targetBody = body2
		test.targetDatatype = datatype2
		test.hlv = ``
		test.mou = ``
		test.executeTest(*assert, bucketName, xmemBuckets[i], cluster)

		cluster.Close(nil)
		fmt.Printf("*************** (%v/%v) Done Bucket: %s ***************\n", i+1, len(gocbBuckets), gocbBuckets[i])
	}
}
