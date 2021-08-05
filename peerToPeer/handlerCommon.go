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

type OpaqueMap map[uint32]*time.Timer

type HandlerCommon struct {
	logger          *log.CommonLogger
	lifeCycleId     string
	finCh           chan bool
	cleanupInterval time.Duration

	opaqueMap      OpaqueMap
	opaqueMapMtx   sync.RWMutex
	opaquesClearCh chan uint32
}

func NewHandlerCommon(logger *log.CommonLogger, lifeCycleId string, finCh chan bool, cleanupInterval time.Duration) *HandlerCommon {
	handler := &HandlerCommon{
		logger:          logger,
		lifeCycleId:     lifeCycleId,
		finCh:           finCh,
		opaqueMap:       make(OpaqueMap),
		opaqueMapMtx:    sync.RWMutex{},
		opaquesClearCh:  make(chan uint32, 1000),
		cleanupInterval: cleanupInterval,
	}
	return handler
}

func (h *HandlerCommon) RegisterOpaque(request Request) error {
	h.opaqueMapMtx.Lock()
	defer h.opaqueMapMtx.Unlock()
	_, found := h.opaqueMap[request.GetOpaque()]
	if found {
		return service_def.ErrorKeyAlreadyExist
	}
	h.opaqueMap[request.GetOpaque()] = time.AfterFunc(base.P2POpaqueTimeout, func() {
		h.logger.Errorf("%v Request to %v with opaque %v timed out", request.GetType(), request.GetTarget(), request.GetOpaque())
		h.opaquesClearCh <- request.GetOpaque()
	})
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
					h.opaqueMapMtx.Unlock()
				default:
					break
				}
			}
		}
	}
}

func (h *DiscoveryHandler) GetAndClearOpaque(opaque uint32) bool {
	h.opaqueMapMtx.RLock()
	timer, found := h.opaqueMap[opaque]
	h.opaqueMapMtx.RUnlock()
	if found {
		timer.Stop()
		h.opaquesClearCh <- opaque
	}
	return found
}
