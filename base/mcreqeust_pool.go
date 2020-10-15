package base

import (
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
	req.SiblingReqs = req.SiblingReqs[:0]
	req.RetryCRCount = 0
	return req
}

func (pool *MCRequestPool) cleanMCReq(req *gomemcached.MCRequest) *gomemcached.MCRequest {
	req.Cas = 0
	req.Opaque = 0
	req.VBucket = 0
	req.Key = nil
	req.Body = nil
	pool.cleanExtras(req)
	//opCode
	req.Opcode = 0

	return req
}

func (pool *MCRequestPool) cleanExtras(req *gomemcached.MCRequest) {
	for i := 0; i < len(req.Extras); i++ {
		req.Extras[i] = 0
	}
}
