// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package peerToPeer

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"sync"
	"sync/atomic"
	"time"
)

var SyncChRespondTimeout = 10 * time.Second

type OpaqueMap map[uint32]*time.Timer
type OpaqueReqMap map[uint32]*Request

type OpaqueReqRespCbMap map[uint32]chan ReqRespPair

type HandlerCommon struct {
	logger          *log.CommonLogger
	lifeCycleId     string
	cleanupInterval time.Duration

	opaqueMap          OpaqueMap
	opaqueReqMap       OpaqueReqMap
	opaqueReqRespCbMap OpaqueReqRespCbMap
	opaqueMapMtx       sync.RWMutex
	opaquesClearCh     chan uint32

	finCh     chan bool
	receiveCh chan interface{}

	replSpecSvc    service_def.ReplicationSpecSvc
	replSpecFinMtx sync.RWMutex
	replSpecFinCh  map[string]chan bool
}

func NewHandlerCommon(logger *log.CommonLogger, lifeCycleId string, finCh chan bool, cleanupInterval time.Duration, reqCh chan interface{}, replicationSpecSvc service_def.ReplicationSpecSvc) *HandlerCommon {
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
		receiveCh:          reqCh,
		replSpecSvc:        replicationSpecSvc,
		replSpecFinCh:      make(map[string]chan bool),
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

	if opts.synchronous {
		ch := make(chan ReqRespPair)
		opts.respMapMtx.Lock()
		opts.respMap[request.GetTarget()] = ch
		opts.respMapMtx.Unlock()
		h.opaqueMapMtx.Lock()
		h.opaqueReqRespCbMap[request.GetOpaque()] = ch
		h.opaqueMapMtx.Unlock()
		if request.GetOpcode().IsInterruptable() {
			finChKey, err := getFinChKeyHelper(request)
			if err != nil {
				h.logger.Errorf("Unable to get finChKey for %v - coding error?", request.GetOpcode())
			} else {
				finCh, getErr := h.getSpecDelNotificationInternal(finChKey)
				if getErr != nil {
					// Likely spec got deleted underneath
					getErr = fmt.Errorf("Unable to getSpecDelNotificationInternal with key %v - %v", finChKey, getErr)
					h.logger.Errorf(getErr.Error())
					return getErr
				}
				opts.finCh = finCh
			}
		}
	}

	h.opaqueMapMtx.Lock()
	defer h.opaqueMapMtx.Unlock()
	opaqueTimeoutDuration := time.Duration(atomic.LoadUint32(&P2POpaqueTimeoutAtomicMin)) * time.Minute
	h.opaqueMap[request.GetOpaque()] = time.AfterFunc(opaqueTimeoutDuration, func() {
		h.logger.Errorf("Request type %v to %v with opaque %v timed out", request.GetType(), request.GetTarget(), request.GetOpaque())
		h.opaquesClearCh <- request.GetOpaque()
	})
	h.opaqueReqMap[request.GetOpaque()] = &request
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
		FORLOOP:
			for {
				select {
				case opaque := <-h.opaquesClearCh:
					h.opaqueMapMtx.Lock()
					delete(h.opaqueMap, opaque)
					delete(h.opaqueReqMap, opaque)
					delete(h.opaqueReqRespCbMap, opaque)
					h.opaqueMapMtx.Unlock()
				default:
					break FORLOOP
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

func getReplSpecFinChMapKey(spec *metadata.ReplicationSpecification) string {
	return getReplSpecFinChMapKeyInternal(spec.Id, spec.InternalId)
}

func getReplSpecFinChMapKeyInternal(specId, internalId string) string {
	return fmt.Sprintf("%v_%v", specId, internalId)
}

func (h *HandlerCommon) HandleSpecCreation(newSpec *metadata.ReplicationSpecification) {
	h.replSpecFinMtx.Lock()
	defer h.replSpecFinMtx.Unlock()
	h.replSpecFinCh[getReplSpecFinChMapKey(newSpec)] = make(chan bool)
}

func (h *HandlerCommon) HandleSpecDeletion(oldSpec *metadata.ReplicationSpecification) {
	h.replSpecFinMtx.Lock()
	defer h.replSpecFinMtx.Unlock()
	close(h.replSpecFinCh[getReplSpecFinChMapKey(oldSpec)])
	delete(h.replSpecFinCh, getReplSpecFinChMapKey(oldSpec))
}

func (h *HandlerCommon) HandleSpecChange(oldSpec, newSpec *metadata.ReplicationSpecification) {
	if oldSpec.Settings.Active && !newSpec.Settings.Active {
		h.replSpecFinMtx.Lock()
		defer h.replSpecFinMtx.Unlock()
		close(h.replSpecFinCh[getReplSpecFinChMapKey(oldSpec)])
		h.replSpecFinCh[getReplSpecFinChMapKey(newSpec)] = make(chan bool)
	}
}

func (h *HandlerCommon) getSpecDelNotificationInternal(key string) (chan bool, error) {
	h.replSpecFinMtx.RLock()
	defer h.replSpecFinMtx.RUnlock()

	finCh, exists := h.replSpecFinCh[key]
	if !exists {
		return nil, base.ErrorResourceDoesNotExist
	}
	return finCh, nil
}

func (h *HandlerCommon) GetSpecDelNotification(specId, internalId string) (chan bool, error) {
	key := getReplSpecFinChMapKeyInternal(specId, internalId)
	return h.getSpecDelNotificationInternal(key)
}
