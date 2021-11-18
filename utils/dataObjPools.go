/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software
will be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package utils

import (
	base "github.com/couchbase/goxdcr/base"
	"sync"
)

type WrappedUprPoolIface interface {
	Get() *base.WrappedUprEvent
	Put(uprEvent *base.WrappedUprEvent)
}

type WrappedUprPool struct {
	pool      sync.Pool
	slicePool DataPoolIface
}

func NewWrappedUprPool(slicePool DataPoolIface) WrappedUprPoolIface {
	newPool := &WrappedUprPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &base.WrappedUprEvent{}
			},
		},
		slicePool: slicePool,
	}
	return newPool
}

func (w *WrappedUprPool) Get() *base.WrappedUprEvent {
	wrappedUprEvent := w.pool.Get().(*base.WrappedUprEvent)
	// Ensure no residue
	wrappedUprEvent.UprEvent = nil
	//wrappedUprEvent.ColNamespace = nil
	wrappedUprEvent.Flags = 0
	wrappedUprEvent.ByteSliceGetter = w.slicePool.GetByteSlice
	return wrappedUprEvent
}

func (w *WrappedUprPool) Put(uprEvent *base.WrappedUprEvent) {
	if uprEvent != nil && uprEvent.Flags.ShouldUseDecompressedValue() {
		w.slicePool.PutByteSlice(uprEvent.DecompressedValue)
	}
	w.pool.Put(uprEvent)
}
