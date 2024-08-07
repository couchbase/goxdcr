/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"encoding/binary"
	"sync"

	"github.com/couchbase/gomemcached"
	"github.com/couchbase/goxdcr/log"
)

type MCRequestPool struct {
	name     string
	obj_pool *sync.Pool
	logger   *log.CommonLogger
}

func NewMCRequestPool(name string, logger *log.CommonLogger) *MCRequestPool {
	pool := &MCRequestPool{name: name,
		obj_pool: &sync.Pool{},
		logger:   logger,
	}
	pool.obj_pool.New = pool.addOne
	return pool
}

func (pool *MCRequestPool) Get() *WrappedMCRequest {
	var obj_ret *WrappedMCRequest = nil
	obj := pool.obj_pool.Get()
	obj_ret, ok := obj.(*WrappedMCRequest)
	if !ok {
		panic("object in MCRequestPool should be of type *WrappedMCRequest")
	}

	return obj_ret
}

func (pool *MCRequestPool) addOne() interface{} {
	obj := &WrappedMCRequest{Seqno: 0,
		Req: &gomemcached.MCRequest{},
	}

	return obj
}

func (pool *MCRequestPool) Put(req *WrappedMCRequest) {
	//make the request vanilar
	req_clean := pool.cleanReq(req)
	pool.obj_pool.Put(req_clean)
}

func (pool *MCRequestPool) cleanReq(req *WrappedMCRequest) *WrappedMCRequest {
	req.Req = pool.cleanMCReq(req.Req)
	req.Seqno = 0
	req.SiblingReqsMtx.Lock()
	req.SiblingReqs = req.SiblingReqs[:0]
	req.SiblingReqsMtx.Unlock()
	req.RetryCRCount = 0
	req.NeedToRecompress = false
	req.SkippedRecompression = false
	req.ImportMutation = false
	req.HLVModeOptions.NoTargetCR = false
	req.HLVModeOptions.PreserveSync = false
	req.HLVModeOptions.SendHlv = false
	req.HLVModeOptions.ActualCas = 0
	req.MouAfterProcessing = nil
	req.ResetSubdocOptionsForRetry()
	return req
}

func (pool *MCRequestPool) cleanMCReq(req *gomemcached.MCRequest) *gomemcached.MCRequest {
	req.Cas = 0
	req.Opaque = 0
	req.VBucket = 0
	pool.cleanExtras(req)
	//opCode
	req.Opcode = 0

	req.Key = req.Key[:0]
	req.Body = req.Body[:0]
	req.ExtMeta = req.ExtMeta[:0]

	req.DataType = 0
	req.Keylen = 0
	for i := 0; i < binary.MaxVarintLen32; i++ {
		req.CollId[i] = 0
	}
	req.CollIdLen = 0
	for i := 0; i < gomemcached.MAX_USER_LEN; i++ {
		req.Username[i] = 0
	}
	req.UserLen = 0
	req.FramingExtras = req.FramingExtras[:0]
	req.FramingElen = 0
	return req
}

func (pool *MCRequestPool) cleanExtras(req *gomemcached.MCRequest) {
	for i := 0; i < len(req.Extras); i++ {
		req.Extras[i] = 0
	}
}
