// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata_svc

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"time"
)

type CheckpointsServiceCache interface {
	GetLatestDocs() (metadata.VBsCkptsDocMap, error)
	GetOneVBDoc(vbno uint16) (*metadata.CheckpointsDoc, error)

	InvalidateCache()
	ValidateCache() // GetLatestDocs() also validate cache automatically

	StoreLatestDocs(incoming metadata.VBsCkptsDocMap) error
	StoreOneVbDoc(vbno uint16, ckpt *metadata.CheckpointsDoc) error

	SpecChangeCb(oldSpec, newSpec *metadata.ReplicationSpecification)
}

type CheckpointsServiceCacheImpl struct {
	logger        *log.CommonLogger
	ckptInterval  time.Duration
	isActive      bool
	isInitialized bool

	cacheEnabled         bool
	finCh                chan bool
	activeUpdateCh       chan *metadata.ReplicationSpecification
	invalidateCh         chan bool
	requestCh            chan *cacheReq
	setCh                chan *cacheReq
	setOneCh             chan *individualSetReq
	externalInvalidateCh chan *invalidateReq
	externalValidateCh   chan *validateReq

	// These caches (specifically shaMap) will grow until cache is invalidated
	// Currently, periodic push path will trigger a call to InvalidateCache()
	// Whenever this is no longer true, will need to revisit to prevent cache bloating up
	latestCompressedCache   metadata.VBsCkptsDocSnappyMap
	latestCompressedShaMaps metadata.ShaMappingCompressedMap
}

type cacheReq struct {
	clonedRetVal metadata.VBsCkptsDocMap
	errorCode    error
	resultReady  chan bool

	individualVbReq       uint16
	individualVbRequested bool
}

type individualSetReq struct {
	vbno    uint16
	ckptDoc *metadata.CheckpointsDoc
	setDone chan bool
}

type invalidateReq struct {
	invalidatedAck chan bool
}

type validateReq struct {
	validatedAck chan bool
}

func NewInvalidateReq() *invalidateReq {
	return &invalidateReq{invalidatedAck: make(chan bool)}
}

func NewValidateReq() *validateReq {
	return &validateReq{validatedAck: make(chan bool)}
}

func NewCacheRequest() *cacheReq {
	return &cacheReq{resultReady: make(chan bool)}
}

func NewCheckpointsServiceCache(logger *log.CommonLogger) *CheckpointsServiceCacheImpl {
	cache := &CheckpointsServiceCacheImpl{
		logger:                  logger,
		finCh:                   make(chan bool),
		activeUpdateCh:          make(chan *metadata.ReplicationSpecification, 5), // Shouldn't have so many updates anyway
		invalidateCh:            make(chan bool),
		requestCh:               make(chan *cacheReq, base.CkptCacheReqChLen),
		setCh:                   make(chan *cacheReq), // Try to be synchronous
		setOneCh:                make(chan *individualSetReq, base.NumberOfVbs),
		latestCompressedCache:   map[uint16][]byte{},
		latestCompressedShaMaps: map[string][]byte{},
		externalInvalidateCh:    make(chan *invalidateReq, base.CkptCacheCtrlChLen), // blocking
		externalValidateCh:      make(chan *validateReq, base.CkptCacheCtrlChLen),   // blocking
	}
	go cache.Run()
	return cache
}

func (c *CheckpointsServiceCacheImpl) StoreLatestDocs(incoming metadata.VBsCkptsDocMap) error {
	req := &cacheReq{
		clonedRetVal: incoming.Clone(),
		resultReady:  make(chan bool),
	}
	select {
	case c.setCh <- req:
		select {
		case <-req.resultReady:
			return nil
		}
	}
}

func (c *CheckpointsServiceCacheImpl) StoreOneVbDoc(vbno uint16, ckpt *metadata.CheckpointsDoc) error {
	var storedCkptDoc *metadata.CheckpointsDoc
	if ckpt != nil {
		storedCkptDoc = ckpt.Clone()
	}
	req := &individualSetReq{
		vbno:    vbno,
		ckptDoc: storedCkptDoc,
		setDone: make(chan bool),
	}
	select {
	case c.setOneCh <- req:
		select {
		case <-req.setDone:
			return nil
		}
	}
}

func (c *CheckpointsServiceCacheImpl) GetLatestDocs() (metadata.VBsCkptsDocMap, error) {
	request := NewCacheRequest()

	select {
	case c.requestCh <- request:
		<-request.resultReady
	}
	return request.clonedRetVal, request.errorCode
}

func (c *CheckpointsServiceCacheImpl) GetOneVBDoc(vbno uint16) (*metadata.CheckpointsDoc, error) {
	request := NewCacheRequest()
	request.individualVbRequested = true
	request.individualVbReq = vbno

	select {
	case c.requestCh <- request:
		select {
		case <-request.resultReady:
		}
	}
	return request.clonedRetVal[vbno], request.errorCode
}

func (c *CheckpointsServiceCacheImpl) SpecChangeCb(oldSpec, newSpec *metadata.ReplicationSpecification) {
	if newSpec == nil {
		// deleted
		close(c.finCh)
	} else {
		c.activeUpdateCh <- newSpec
	}
}

// External, blocking API
// We need an external API to ensure that the cache is indeed invalidated before proceeding
// Internal invalidate calls are non-blocking
func (c *CheckpointsServiceCacheImpl) InvalidateCache() {
	req := NewInvalidateReq()
	select {
	case c.externalInvalidateCh <- req:
		<-req.invalidatedAck
		return
	}
}

func (c *CheckpointsServiceCacheImpl) ValidateCache() {
	req := NewValidateReq()
	select {
	case c.externalValidateCh <- req:
		<-req.validatedAck
		return
	}
}

// To be called by run only
func (c *CheckpointsServiceCacheImpl) requestInvalidateCache() {
	select {
	case c.invalidateCh <- true:
	// that's it
	default:
		// breakout
	}
}

// To be called by run only
func (c *CheckpointsServiceCacheImpl) validateCache() {
	select {
	case <-c.invalidateCh:
		// that's it
		break
	default:
		// breakout
	}
}

func (c *CheckpointsServiceCacheImpl) Run() {
	for {
		select {
		case <-c.finCh:
			return
		case spec := <-c.activeUpdateCh:
			c.cacheEnabled = spec.Settings.GetCkptSvcCacheEnabled()
			if c.cacheEnabled {
				if !spec.Settings.Active && c.isActive {
					// Replication paused:
					c.isActive = spec.Settings.Active
					c.requestInvalidateCache()
				} else if spec.Settings.Active && !c.isActive {
					// Replication resumed:
					// To be safe, invalidate cache and let pipeline resume restore a valid copy
					c.requestInvalidateCache()
					c.isActive = spec.Settings.Active
				}
			} else {
				c.requestInvalidateCache()
			}
		case <-c.invalidateCh:
			c.latestCompressedShaMaps = nil
			c.latestCompressedCache = nil
		case getReq := <-c.requestCh:
			if len(c.invalidateCh) > 0 || c.latestCompressedCache == nil || len(c.latestCompressedCache) == 0 ||
				len(c.setCh) > 0 || len(c.setOneCh) > 0 || !c.cacheEnabled {
				// Cache is going to be invalidated or updated, do not return them
				if getReq.individualVbRequested {
					getReq.clonedRetVal = make(metadata.VBsCkptsDocMap)
					getReq.clonedRetVal[getReq.individualVbReq] = nil
				} else {
					getReq.clonedRetVal = nil
				}
				getReq.errorCode = service_def.MetadataNotFoundErr
			} else {
				if getReq.individualVbRequested {
					getReq.clonedRetVal = make(metadata.VBsCkptsDocMap)
					if c.latestCompressedCache[getReq.individualVbReq] == nil {
						getReq.clonedRetVal[getReq.individualVbReq] = nil
						getReq.errorCode = service_def.MetadataNotFoundErr
					} else {
						compressedBytes := c.latestCompressedCache[getReq.individualVbReq]
						ckptDoc, err := metadata.NewCheckpointsDocFromSnappy(compressedBytes, c.latestCompressedShaMaps)
						if err != nil {
							getReq.errorCode = err
							c.requestInvalidateCache()
						} else {
							getReq.clonedRetVal[getReq.individualVbReq] = ckptDoc
						}
					}
				} else {
					regCkptDocs, err := c.latestCompressedCache.SnappyDecompress(c.latestCompressedShaMaps)
					if err != nil {
						getReq.errorCode = err
						c.requestInvalidateCache()
					} else {
						getReq.clonedRetVal = regCkptDocs
					}
				}
			}
			close(getReq.resultReady)
		case setReq := <-c.setCh:
			if c.cacheEnabled {
				snappyCkpt, snappyShaMaps, err := setReq.clonedRetVal.SnappyCompress()
				if err != nil {
					c.logger.Errorf("Unable to snappyCompress %v", setReq.clonedRetVal)
					c.requestInvalidateCache()
				} else {
					// cache is valid
					c.latestCompressedCache = snappyCkpt
					c.latestCompressedShaMaps = snappyShaMaps
					c.validateCache()
				}
			}
			close(setReq.resultReady)
		case setOneReq := <-c.setOneCh:
			if c.cacheEnabled {
				if c.latestCompressedShaMaps == nil {
					c.latestCompressedShaMaps = make(metadata.ShaMappingCompressedMap)
				}
				if c.latestCompressedCache == nil {
					c.latestCompressedCache = make(metadata.VBsCkptsDocSnappyMap)
				}

				if setOneReq.ckptDoc == nil {
					// Passing in a nil doc means "unset"
					c.latestCompressedCache[setOneReq.vbno] = nil
				} else {
					snappyBytes, snappyShaMap, err := setOneReq.ckptDoc.SnappyCompress()
					if err != nil {
						c.logger.Errorf("Unable to snappy compress vbno %v doc %v: %v", setOneReq.vbno, setOneReq.ckptDoc, err)
						c.requestInvalidateCache()
					} else {
						c.latestCompressedCache[setOneReq.vbno] = snappyBytes
						c.latestCompressedShaMaps.Merge(snappyShaMap)
					}
				}
				c.validateCache()
			}
			close(setOneReq.setDone)
		case oneInvalidateReq := <-c.externalInvalidateCh:
			c.requestInvalidateCache()
			close(oneInvalidateReq.invalidatedAck)
		case oneValidateReq := <-c.externalValidateCh:
			c.validateCache()
			close(oneValidateReq.validatedAck)
		}
	}
}
