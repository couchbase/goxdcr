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
	slicePool base.DataPool
}

func NewWrappedUprPool(slicePool base.DataPool) WrappedUprPoolIface {
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
	wrappedUprEvent.ColNamespace = nil
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

type CollectionNamespacePoolIface interface {
	Get() *base.CollectionNamespace
	Put(ns *base.CollectionNamespace)
}

type CollectionNamespacePool struct {
	pool sync.Pool
}

func NewCollectionNamespacePool() CollectionNamespacePoolIface {
	newPool := &CollectionNamespacePool{
		pool: sync.Pool{
			New: func() interface{} {
				return &base.CollectionNamespace{}
			},
		},
	}
	return newPool
}

func (c *CollectionNamespacePool) Get() *base.CollectionNamespace {
	namespace := c.pool.Get().(*base.CollectionNamespace)
	namespace.ScopeName = ""
	namespace.CollectionName = ""
	return namespace
}

func (c *CollectionNamespacePool) Put(ns *base.CollectionNamespace) {
	c.pool.Put(ns)
}

type TargetCollectionInfoPoolIface interface {
	Get() *base.TargetCollectionInfo
	Put(*base.TargetCollectionInfo)
}

type TargetCollectionInfoPool struct {
	pool sync.Pool
}

func NewTargetCollectionInfoPool() TargetCollectionInfoPoolIface {
	newPool := &TargetCollectionInfoPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &base.TargetCollectionInfo{}
			},
		},
	}
	return newPool
}

func (t *TargetCollectionInfoPool) Get() *base.TargetCollectionInfo {
	colInfo := t.pool.Get().(*base.TargetCollectionInfo)
	colInfo.ManifestId = 0
	colInfo.ColIDPrefixedKey = nil
	colInfo.TargetNamespace = nil
	return colInfo
}

func (t *TargetCollectionInfoPool) Put(tc *base.TargetCollectionInfo) {
	tc.ColIDPrefixedKey = nil
	tc.TargetNamespace = nil
	t.pool.Put(tc)
}
