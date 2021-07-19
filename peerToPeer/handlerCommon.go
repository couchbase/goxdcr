// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package peerToPeer

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
	"sync"
	"time"
)

var SyncChRespondTimeout = 10 * time.Second

type OpaqueMap map[uint32]*time.Timer
type OpaqueReqMap map[uint32]*Request

type OpaqueReqRespCbMap map[uint32]chan ReqRespPair

type HandlerCommon struct {
	logger          *log.CommonLogger
	lifeCycleId     string
	finCh           chan bool
	cleanupInterval time.Duration

	opaqueMap          OpaqueMap
	opaqueReqMap       OpaqueReqMap
	opaqueReqRespCbMap OpaqueReqRespCbMap
	opaqueMapMtx       sync.RWMutex
	opaquesClearCh     chan uint32
}

func NewHandlerCommon(logger *log.CommonLogger, lifeCycleId string, finCh chan bool, cleanupInterval time.Duration) *HandlerCommon {
	handler := &HandlerCommon{
		logger:             logger,
		lifeCycleId:        lifeCycleId,
		finCh:              finCh,
		opaqueMap:          make(OpaqueMap),
		opaqueReqMap:       map[uint32]*Request{},
		opaqueMapMtx:       sync.RWMutex{},
		opaquesClearCh:     make(chan uint32, 1000),
		cleanupInterval:    cleanupInterval,
		opaqueReqRespCbMap: map[uint32]chan ReqRespPair{},
	}
	return handler
}

func (h *HandlerCommon) RegisterOpaque(request Request, opts *SendOpts) error {
	h.opaqueMapMtx.RLock()
	_, found := h.opaqueMap[request.GetOpaque()]
	if found {
		h.opaqueMapMtx.RUnlock()
		return service_def.ErrorKeyAlreadyExist
	}
	h.opaqueMapMtx.RUnlock()

	h.opaqueMapMtx.Lock()
	defer h.opaqueMapMtx.Unlock()
	h.opaqueMap[request.GetOpaque()] = time.AfterFunc(base.P2POpaqueTimeout, func() {
		h.logger.Errorf("%v Request to %v with opaque %v timed out", request.GetType(), request.GetTarget(), request.GetOpaque())
		h.opaquesClearCh <- request.GetOpaque()
	})
	h.opaqueReqMap[request.GetOpaque()] = &request

	if opts.synchronous {
		ch := make(chan ReqRespPair)
		opts.respMapMtx.Lock()
		opts.respMap[request.GetTarget()] = ch
		opts.respMapMtx.Unlock()
		h.opaqueReqRespCbMap[request.GetOpaque()] = ch
	}
	return nil
}

func (h *HandlerCommon) Start() {
	go h.ClearOpaqueBg()
}

func (h *HandlerCommon) ClearOpaqueBg() {
	ticker := time.NewTicker(h.cleanupInterval)
	for {
		select {
		case <-h.finCh:
			ticker.Stop()
			return
		case <-ticker.C:
			for {
				select {
				case opaque := <-h.opaquesClearCh:
					h.opaqueMapMtx.Lock()
					delete(h.opaqueMap, opaque)
					delete(h.opaqueReqMap, opaque)
					delete(h.opaqueReqRespCbMap, opaque)
					h.opaqueMapMtx.Unlock()
				default:
					break
				}
			}
		}
	}
}

func (h *HandlerCommon) GetReqAndClearOpaque(opaque uint32) (*Request, chan ReqRespPair, bool) {
	h.opaqueMapMtx.RLock()
	timer, found := h.opaqueMap[opaque]
	req := h.opaqueReqMap[opaque]
	syncCh, chFound := h.opaqueReqRespCbMap[opaque]
	h.opaqueMapMtx.RUnlock()
	if !chFound {
		syncCh = nil
	}
	if found {
		timer.Stop()
		h.opaquesClearCh <- opaque
	}
	return req, syncCh, found
}

func (h *HandlerCommon) sendBackSynchronously(retCh chan ReqRespPair, retPair ReqRespPair) {
	timer := time.NewTimer(SyncChRespondTimeout)
	select {
	case retCh <- retPair:
	// Done
	case <-timer.C:
		h.logger.Errorf("Unable to respond to caller given %v timed out", retPair)
		break
	}
}
