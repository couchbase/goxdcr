// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package parts

import (
	"encoding/binary"
	"fmt"

	"github.com/couchbase/gocb/v2"
	mc "github.com/couchbase/gomemcached"
	"github.com/couchbase/goxdcr/v8/base"
)

// Note that this file is only used for testing purposes
// and should not be called in production.

// The function testMcRequest and testMcResponse should be manually injected in the right places of the xmem execution,
// [Eg: testMcRequest() placed after xmem.buf.enSlot(item) and testMcResponse() placed after xmem.readFromClient(xmem.client_for_setMeta, true)]
// and a struct of type reqTestParams needs to be passed to the reqTestCh channel and a struct of type respTestParams
// needs to be passed to the respTestCh channel for verification of the MCRequest and MCResponse processed by xmem.

var reqTestCh chan reqTestParams
var respTestCh chan respTestParams

type targetWriter struct {
	enabled bool
	writer  func(kvAddr, bucketName string, key, val []byte,
		datatype uint8, cas uint64, revId uint64, lww bool) error
	waiter        func(bucket *gocb.Bucket, key, path string, expectedValue []byte, isXattr bool, accessDeleted bool) error
	kvAddr        string
	bucketName    string
	key           []byte
	val           []byte
	datatype      uint8
	cas           uint64
	revId         uint64
	lww           bool
	bucket        *gocb.Bucket
	expectedCas   []byte
	expectedRev   []byte
	accessDeleted bool
}

func (w targetWriter) changeTargetCas() error {
	err := w.writer(w.kvAddr, w.bucketName, w.key, w.val, w.datatype, w.cas, w.revId, w.lww)
	if err != nil {
		return err
	}
	// make sure cas/revId is changed as expected
	if w.lww {
		err = w.waiter(w.bucket, string(w.key), "$document.CAS", w.expectedCas, true, w.accessDeleted)
		if err != nil {
			return err
		}
	} else {
		err = w.waiter(w.bucket, string(w.key), "$document.revid", w.expectedRev, true, w.accessDeleted)
		if err != nil {
			return err
		}
	}
	return err
}

type reqTestParams struct {
	setOpcode        mc.CommandCode
	getOpcode        mc.CommandCode
	setCas           uint64
	subdocLookup     bool
	specs            []string
	casLocking       bool
	setNoTargetCR    bool
	setHasXattrs     bool
	targetCasChanger targetWriter
	retryCnt         int
}

type respTestParams struct {
	Status mc.Status
}

func testMcRequest(req *base.WrappedMCRequest, lookup *base.SubdocLookupResponse) {
	if reqTestCh == nil {
		panic("reqTestCh nil")
	}

	var t reqTestParams
	select {
	case t = <-reqTestCh:
	default:
		panic("No request to test")
	}

	if t.targetCasChanger.enabled {
		err := t.targetCasChanger.changeTargetCas()
		if err != nil {
			panic(fmt.Sprintf("error writing conflict, err=%v", err))
		}
	}

	if t.setOpcode != req.Req.Opcode {
		panic(fmt.Sprintf("Set opcode mismatch - %v != %v", t.setOpcode, req.Req.Opcode))
	}

	if !t.targetCasChanger.enabled && t.setCas != req.Req.Cas {
		// req Cas will be changed if target cas was changed.
		panic(fmt.Sprintf("Set cas mismatch - %v != %v", t.setCas, req.Req.Cas))
	}

	if t.subdocLookup && lookup == nil {
		panic("no subdoc lookup done, but expected")
	}

	if t.getOpcode != lookup.Resp.Opcode {
		panic(fmt.Sprintf("Get opcode mismatch - %v != %v", t.getOpcode, lookup.Resp.Opcode))
	}

	if t.casLocking != req.IsCasLockingRequest() {
		panic(fmt.Sprintf("Set CASLocking mismatch - %v != (%v, %v)", t.casLocking, req.Req.Cas, req.Req.Opcode))
	}

	if t.setOpcode != base.SUBDOC_MULTI_MUTATION {
		var options uint32
		if len(req.Req.Extras) >= 28 {
			options = binary.BigEndian.Uint32(req.Req.Extras[24:28])
		}
		noTargetCR := (options&base.SKIP_CONFLICT_RESOLUTION_FLAG > 0)
		if t.setNoTargetCR != noTargetCR {
			panic(fmt.Sprintf("Set no target CR - %v != (%v = %v)", t.setNoTargetCR, noTargetCR, options))
		}

		hasXattrs := req.Req.DataType&base.XattrDataType > 0
		if t.setHasXattrs != hasXattrs {
			panic(fmt.Sprintf("Set XATTRs mismatch - %v != (%v = %v)", t.setHasXattrs, hasXattrs, req.Req.DataType))
		}
	}

	if t.retryCnt != req.RetryCRCount && !(t.targetCasChanger.enabled && t.retryCnt+1 == req.RetryCRCount) {
		panic(fmt.Sprintf("RetryCnt mismatch - %v != %v, caschanged=%v", t.retryCnt, req.RetryCRCount, t.targetCasChanger.enabled))
	}

	if lookup != nil {
		specs := lookup.Specs
		if len(specs) != len(t.specs) {
			panic(fmt.Sprintf("spec length not equal - %v != %v", t.specs, specs))
		}

		for _, spec1 := range t.specs {
			found := false
			for _, spec2 := range specs {
				if spec1 == string(spec2.Path) {
					found = true
				}
			}
			if !found {
				panic(fmt.Sprintf("spec %s expected, but not found - %v != %v", spec1, t.specs, specs))
			}
		}
	} else if len(t.specs) > 0 {
		panic(fmt.Sprintf("lookup is nil, but specs=%v", t.specs))
	}
}

func testMcResponse(resp *mc.MCResponse) {
	if respTestCh == nil {
		panic("respTestCh nil")
	}

	var t respTestParams
	select {
	case t = <-respTestCh:
	default:
		panic("No response to test")
	}

	if t.Status != resp.Status {
		panic(fmt.Sprintf("Response status mismatch - %v != %v", t.Status, resp.Status))
	}
}
