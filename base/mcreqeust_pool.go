package base

import (
	"fmt"
	"github.com/couchbase/gomemcached"
	"github.com/couchbase/goxdcr/log"
	"sync"
)

type MCRequestPool struct {
	name        string
	obj_pool    *sync.Pool
	lock        *sync.RWMutex
	max_size    int
	lease_count int
	cur_size    int
	logger      *log.CommonLogger
}

func NewMCRequestPool(name string, max_size int, logger *log.CommonLogger) *MCRequestPool {
	return &MCRequestPool{name: name,
		obj_pool:    &sync.Pool{},
		lock:        &sync.RWMutex{},
		max_size:    max_size,
		cur_size:    0,
		lease_count: 0,
		logger:      logger,
	}
}

func (pool *MCRequestPool) Get() *WrappedMCRequest {
	var obj_ret *WrappedMCRequest = nil
	obj := pool.obj_pool.Get()
	if obj == nil {
		obj = pool.addOne()
		pool.lock.Lock()
		pool.lease_count++
		pool.lock.Unlock()
	} else {
		pool.lock.Lock()
		pool.cur_size--
		pool.lease_count++
		pool.lock.Unlock()
	}
	obj_ret, ok := obj.(*WrappedMCRequest)
	if !ok {
		panic("object in MCRequestPool should be of type *WrappedMCRequest")
	}

	return obj_ret
}

func (pool *MCRequestPool) addOne() *WrappedMCRequest {
	obj := &WrappedMCRequest{Seqno: 0,
		Req: &gomemcached.MCRequest{Extras: make([]byte, 24)},
	}

	return obj
}

func (pool *MCRequestPool) Put(req *WrappedMCRequest) {
	//make the request vanilar
	if pool.cur_size < pool.max_size {
		req_clean := pool.cleanReq(req)
		pool.obj_pool.Put(req_clean)
		pool.lock.Lock()
		pool.cur_size++
		pool.lease_count--
		pool.lock.Unlock()
	} else {
		//drop on the floor
		pool.lock.Lock()
		pool.lease_count--
		pool.lock.Unlock()

	}
}

func (pool *MCRequestPool) cleanReq(req *WrappedMCRequest) *WrappedMCRequest {
	req.Req = pool.cleanMCReq(req.Req)
	req.Seqno = 0
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
	for i := 0; i < 24; i++ {
		req.Extras[0] = 0
	}
}

func (pool *MCRequestPool) String() string {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	return fmt.Sprintf("MCRequestPool %v cur_size=%v, lease_count=%v, max_size=%v\n", pool.name, pool.cur_size, pool.lease_count, pool.max_size)
}

func (pool *MCRequestPool) SetMaxSize(max_size int) {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	pool.max_size = max_size
	pool.logger.Infof("MCRequestPool's max_size is set to %v\n", pool.max_size)
}
