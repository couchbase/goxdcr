/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"github.com/couchbase/goxdcr/log"
	"math/rand"
	"sort"
	"sync"
)

const NumOfSizes = 20

// Fake data pool used to simulate returning a byte slice filled with garbage
type FakeDataPool struct {
}

func NewFakeDataPool() *FakeDataPool {
	return &FakeDataPool{}
}

func (fd *FakeDataPool) GetByteSlice(sizeRequested uint64) ([]byte, error) {
	garbageSlice := make([]byte, sizeRequested+50, sizeRequested+50)
	rand.Read(garbageSlice)
	return garbageSlice, nil
}

// Nothing
func (fd *FakeDataPool) PutByteSlice(doneSlice []byte) {}

type DataPoolImpl struct {
	byteSlicePoolClasses [NumOfSizes]uint64
	byteSlicePools       [NumOfSizes]sync.Pool

	logger_utils *log.CommonLogger
}

func NewDataPool() *DataPoolImpl {
	newPool := &DataPoolImpl{
		logger_utils: log.NewLogger("DataPoolImpl", log.DefaultLoggerContext),
	}

	newPool.byteSlicePoolClasses = [NumOfSizes]uint64{
		50, // 50 bytes
		100,
		250,
		500,
		1 << 10,  // 1k
		5 << 10,  // 5k
		10 << 10, // 10k
		20 << 10, // 20k... etc
		40 << 10,
		80 << 10,
		100 << 10,
		200 << 10,
		400 << 10,
		800 << 10,
		1 << 20, // 1MB
		2 << 20, // 2MB
		4 << 20,
		8 << 20,
		10 << 20,
		21 << 20, // max value size is 20MB
	}

	newPool.byteSlicePools = [NumOfSizes]sync.Pool{
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[0]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[1]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[2]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[3]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[4]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[5]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[6]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[7]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[8]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[9]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[10]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[11]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[12]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[13]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[14]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[15]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[16]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[17]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[18]) }},
		{New: func() interface{} { return make([]byte, newPool.byteSlicePoolClasses[19]) }},
	}

	return newPool
}

func (p *DataPoolImpl) GetByteSlice(sizeRequested uint64) ([]byte, error) {
	i := sort.Search(NumOfSizes, func(i int) bool {
		return p.byteSlicePoolClasses[i] >= sizeRequested
	})

	if i >= 0 && i < NumOfSizes {
		return p.byteSlicePools[i].Get().([]byte), nil
	}
	return nil, ErrorSizeExceeded
}

func (p *DataPoolImpl) PutByteSlice(doneSlice []byte) {
	sliceCap := uint64(cap(doneSlice))
	i := sort.Search(NumOfSizes, func(i int) bool {
		return sliceCap <= p.byteSlicePoolClasses[i]
	})

	if i >= 0 && i < NumOfSizes {
		p.byteSlicePools[i].Put(doneSlice)
	}
}

type DataPool interface {
	GetByteSlice(sizeRequested uint64) ([]byte, error)
	PutByteSlice(doneSlice []byte)
}
