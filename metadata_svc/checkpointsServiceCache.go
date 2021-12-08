// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata_svc

import (
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
	ckptInterval  time.Duration
	isActive      bool
	isInitialized bool

	finCh          chan bool
	activeUpdateCh chan *metadata.ReplicationSpecification
	invalidateCh   chan bool
	//invalidateTicker     time.Ticker
	requestCh            chan *cacheReq
	setCh                chan *cacheReq
	setOneCh             chan *individualSetReq
	externalInvalidateCh chan *invalidateReq
	externalValidateCh   chan *validateReq

	latestCache           metadata.VBsCkptsDocMap
	latestCompressedCache VBsCkptsDocSnappyMap
}

// TODO - compression next
type VBsCkptsDocSnappyMap map[uint16][]byte

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

func NewCheckpointsServiceCache() *CheckpointsServiceCacheImpl {
	cache := &CheckpointsServiceCacheImpl{
		finCh:                 make(chan bool),
		activeUpdateCh:        make(chan *metadata.ReplicationSpecification, 5), // Shouldn't have so many updates anyway
		invalidateCh:          make(chan bool),
		requestCh:             make(chan *cacheReq, 1000), // TODO - all these consts...
		setCh:                 make(chan *cacheReq),       // Try to be synchronous
		setOneCh:              make(chan *individualSetReq, 1024),
		latestCompressedCache: map[uint16][]byte{},
		externalInvalidateCh:  make(chan *invalidateReq, 10), // blocking
		externalValidateCh:    make(chan *validateReq, 10),   // blocking
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
			if !spec.Settings.Active && c.isActive {
				// Replication paused:
				//c.invalidateTicker.Stop()
				c.isActive = spec.Settings.Active
			} else if spec.Settings.Active && !c.isActive {
				// Replication resumed:
				// To be safe, invalidate cache
				c.requestInvalidateCache()

				// Start a timer that invalidates the cache every ckpt interval
				//specCkptInterval := time.Duration(spec.Settings.CheckpointInterval) * time.Second
				//c.invalidateTicker.Reset(specCkptInterval)
				//c.ckptInterval = specCkptInterval
				c.isActive = spec.Settings.Active
			} else {
				// Not pipeline pause nor resume
				// If ckpt interval changed, update the timer
				//specCkptInterval := time.Duration(spec.Settings.CheckpointInterval) * time.Second
				//if specCkptInterval != c.ckptInterval {
				//	c.requestInvalidateCache()
				//	//c.invalidateTicker.Reset(specCkptInterval)
				//}
			}
		case <-c.invalidateCh:
			c.latestCache = nil
		case getReq := <-c.requestCh:
			if len(c.invalidateCh) > 0 || c.latestCache == nil || len(c.latestCache) == 0 ||
				len(c.setCh) > 0 || len(c.setOneCh) > 0 {
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
					if c.latestCache[getReq.individualVbReq] == nil {
						getReq.clonedRetVal[getReq.individualVbReq] = nil
						getReq.errorCode = service_def.MetadataNotFoundErr
					} else {
						getReq.clonedRetVal[getReq.individualVbReq] = c.latestCache[getReq.individualVbReq].Clone()
					}
				} else {
					getReq.clonedRetVal = c.latestCache.Clone()
				}
			}
			close(getReq.resultReady)
		case setReq := <-c.setCh:
			c.latestCache = setReq.clonedRetVal
			// cache is valid
			c.validateCache()
			close(setReq.resultReady)
		case setOneReq := <-c.setOneCh:
			if c.latestCache == nil {
				c.latestCache = make(metadata.VBsCkptsDocMap)
			}
			c.latestCache[setOneReq.vbno] = setOneReq.ckptDoc
			// cache is valid
			c.validateCache()
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
