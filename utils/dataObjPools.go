/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package utils

import (
	"sync"

	base "github.com/couchbase/goxdcr/base"
)

// TODO - we should revisit these to use *WrappedUprPool instead to make sure pools do not get copied
// Right now, there is only one user. If it ever becomes > 1 user, then it'll be a problem (i.e. bucketTopologySvc)
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

type KvVbMapPool struct {
	pool sync.Pool
}

func (k *KvVbMapPool) Get(nodes []string) *base.KvVBMapType {
	kvVbMap := k.pool.Get().(*base.KvVBMapType)
	for oldNodeName, value := range *kvVbMap {
		var found bool
		for _, nodeName := range nodes {
			if oldNodeName == nodeName {
				found = true
				break
			}
		}
		if !found {
			delete(*kvVbMap, oldNodeName)
		} else {
			(*kvVbMap)[oldNodeName] = value[:0]
		}
	}
	return kvVbMap
}

func (k *KvVbMapPool) Put(kvVbMap *base.KvVBMapType) {
	k.pool.Put(kvVbMap)
}

func NewKvVbMapPool() *KvVbMapPool {
	newPool := &KvVbMapPool{
		pool: sync.Pool{
			New: func() interface{} {
				retMap := make(base.KvVBMapType)
				return &retMap
			},
		},
	}
	return newPool
}

type HighSeqnosMapPool struct {
	pool sync.Pool
}

// The internal seqnoMap cleaniness is responsible by the caller
func (s *HighSeqnosMapPool) Get(keys []string) *base.HighSeqnosMapType {
	retMap := s.pool.Get().(*base.HighSeqnosMapType)
	for oldKey, _ := range *retMap {
		var found bool
		for _, checkKey := range keys {
			if checkKey == oldKey {
				found = true
				break
			}
		}
		if !found {
			delete(*retMap, oldKey)
		}
	}
	return retMap
}

func (s *HighSeqnosMapPool) Put(incoming *base.HighSeqnosMapType) {
	s.pool.Put(incoming)
}

func NewHighSeqnosMapPool() *HighSeqnosMapPool {
	return &HighSeqnosMapPool{
		pool: sync.Pool{
			New: func() interface{} {
				retMap := make(base.HighSeqnosMapType)
				return &retMap
			},
		},
	}
}

type StringStringMapPool struct {
	pool sync.Pool
}

func (s *StringStringMapPool) Get(keys []string) *base.StringStringMap {
	retMap := s.pool.Get().(*base.StringStringMap)
	for oldKey, _ := range *retMap {
		var found bool
		for _, checkKey := range keys {
			if checkKey == oldKey {
				found = true
				break
			}
		}
		if !found {
			delete(*retMap, oldKey)
		} else {
			(*retMap)[oldKey] = ""
		}
	}
	return retMap
}

func (s *StringStringMapPool) Put(incoming *base.StringStringMap) {
	s.pool.Put(incoming)
}

func NewStringStringMapPool() *StringStringMapPool {
	return &StringStringMapPool{
		pool: sync.Pool{
			New: func() interface{} {
				retMap := make(base.StringStringMap)
				return &retMap
			},
		},
	}
}

type VbSeqnoMapPool struct {
	pool sync.Pool
}

func (v *VbSeqnoMapPool) Get(vbnos []uint16) *map[uint16]uint64 {
	retMap := v.pool.Get().(*map[uint16]uint64)
	for oldVb, _ := range *retMap {
		var found bool
		for _, checkKey := range vbnos {
			if checkKey == oldVb {
				found = true
				break
			}
		}
		if !found {
			delete(*retMap, oldVb)
		} else {
			(*retMap)[oldVb] = 0
		}
	}
	return retMap
}

func (v *VbSeqnoMapPool) Put(incoming *map[uint16]uint64) {
	v.pool.Put(incoming)
}

func NewVbSeqnoMapPool() *VbSeqnoMapPool {
	return &VbSeqnoMapPool{
		pool: sync.Pool{
			New: func() interface{} {
				retMap := make(map[uint16]uint64)
				return &retMap
			},
		},
	}
}

type BucketInfoMapPool struct {
	pool sync.Pool
}

func (b *BucketInfoMapPool) Put(incoming *base.BucketInfoMapType) {
	b.pool.Put(incoming)
}

func (b *BucketInfoMapPool) Get(keys []string) *base.BucketInfoMapType {
	retMap := b.pool.Get().(*base.BucketInfoMapType)
	for oldKey, _ := range *retMap {
		var found bool
		for _, checkKey := range keys {
			if checkKey == oldKey {
				found = true
				break
			}
		}
		if !found {
			delete(*retMap, oldKey)
		}
	}
	for key, _ := range *retMap {
		(*retMap)[key] = nil
	}
	return retMap
}

func NewBucketInfoMapPool() *BucketInfoMapPool {
	return &BucketInfoMapPool{
		pool: sync.Pool{
			New: func() interface{} {
				retMap := make(base.BucketInfoMapType)
				return &retMap
			},
		},
	}
}

type VbHostsMapPool struct {
	pool         sync.Pool
	strSlicePool *StringSlicePool
}

func (v *VbHostsMapPool) Get(vbnos []uint16) *base.VbHostsMapType {
	retMap := v.pool.Get().(*base.VbHostsMapType)
	for oldVb, _ := range *retMap {
		var found bool
		for _, checkKey := range vbnos {
			if checkKey == oldVb {
				found = true
				break
			}
		}
		if !found {
			delete(*retMap, oldVb)
		}
	}
	for vbno, _ := range *retMap {
		(*retMap)[vbno] = v.strSlicePool.Get()
	}
	return retMap
}

func (v *VbHostsMapPool) Put(incoming *base.VbHostsMapType) {
	for _, strSlice := range *incoming {
		v.strSlicePool.Put(strSlice)
	}
	v.pool.Put(incoming)
}

func NewVbHostsMapPool(strSlicePool *StringSlicePool) *VbHostsMapPool {
	return &VbHostsMapPool{
		pool: sync.Pool{
			New: func() interface{} {
				retMap := make(base.VbHostsMapType)
				return &retMap
			},
		},
		strSlicePool: strSlicePool,
	}
}

type StringSlicePool struct {
	pool sync.Pool
}

func (s *StringSlicePool) Put(incoming *[]string) {
	s.pool.Put(incoming)
}

func (s *StringSlicePool) Get() *[]string {
	strSlice := s.pool.Get().(*[]string)
	*strSlice = (*strSlice)[:0]
	return strSlice
}

func NewStringSlicePool() *StringSlicePool {
	return &StringSlicePool{
		pool: sync.Pool{
			New: func() interface{} {
				var retSlice []string
				return &retSlice
			},
		},
	}
}
