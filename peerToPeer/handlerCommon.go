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
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
)

type OpaqueMap map[uint32]*time.Timer
type OpaqueReqMap map[uint32]*Request

type OpaqueReqRespCbMap map[uint32]chan ReqRespPair

type HandlerCommon struct {
	id              string
	logger          *log.CommonLogger
	lifeCycleId     string
	cleanupInterval time.Duration

	opaqueMap          OpaqueMap
	opaqueReqMap       OpaqueReqMap
	opaqueReqRespCbMap OpaqueReqRespCbMap
	opaqueMapMtx       sync.RWMutex
	opaquesClearCh     chan uint32

	finCh         chan bool
	receiveReqCh  chan interface{}
	receiveRespCh chan interface{}

	replSpecSvc    service_def.ReplicationSpecSvc
	replSpecFinMtx sync.RWMutex
	replSpecFinCh  map[string]chan bool
}

func NewHandlerCommon(id string, logger *log.CommonLogger, lifeCycleId string, finCh chan bool, cleanupInterval time.Duration, reqCh []chan interface{}, replicationSpecSvc service_def.ReplicationSpecSvc) *HandlerCommon {
	handler := &HandlerCommon{
		id:                 id,
		logger:             logger,
		lifeCycleId:        lifeCycleId,
		finCh:              finCh,
		opaqueMap:          make(OpaqueMap),
		opaqueReqMap:       map[uint32]*Request{},
		opaqueMapMtx:       sync.RWMutex{},
		opaquesClearCh:     make(chan uint32, 1000),
		cleanupInterval:    cleanupInterval,
		opaqueReqRespCbMap: map[uint32]chan ReqRespPair{},
		receiveReqCh:       reqCh[RequestType],
		receiveRespCh:      reqCh[ResponseType],
		replSpecSvc:        replicationSpecSvc,
		replSpecFinCh:      make(map[string]chan bool),
	}
	return handler
}

func (h *HandlerCommon) RegisterOpaque(request Request, opts *SendOpts) error {
	// The undoReservation is needed if RegisterOpaque call fails
	err, undoReservation := h.registerOpaqueKey(request)
	if err != nil {
		return err
	}

	var ch chan ReqRespPair
	if opts.synchronous {
		ch = make(chan ReqRespPair)
		opts.respMapMtx.Lock()
		opts.respMap[request.GetTarget()] = ch
		opts.respMapMtx.Unlock()
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
					undoReservation()
					return getErr
				}
				opts.finCh = finCh
			}
		}
	}

	h.opaqueMapMtx.Lock()
	defer h.opaqueMapMtx.Unlock()
	if opts.synchronous {
		h.opaqueReqRespCbMap[request.GetOpaque()] = ch
	}
	opaqueTimeoutDuration := time.Duration(atomic.LoadUint32(&P2POpaqueTimeoutAtomicMin)) * time.Minute
	h.opaqueMap[request.GetOpaque()] = time.AfterFunc(opaqueTimeoutDuration, func() {
		h.logger.Errorf("Request type %v to %v with opaque %v timed out (chan %v)", request.GetOpcode(), request.GetTarget(), request.GetOpaque(), &ch)
		h.opaquesClearCh <- request.GetOpaque()
	})
	h.opaqueReqMap[request.GetOpaque()] = &request
	return nil
}

func (h *HandlerCommon) registerOpaqueKey(request Request) (error, func()) {
	h.opaqueMapMtx.RLock()
	_, found := h.opaqueMap[request.GetOpaque()]
	if found {
		h.opaqueMapMtx.RUnlock()
		return service_def.ErrorKeyAlreadyExist, nil
	}
	h.opaqueMapMtx.RUnlock()

	// Register the opaque now with double check
	h.opaqueMapMtx.Lock()
	_, found = h.opaqueMap[request.GetOpaque()]
	if found {
		h.opaqueMapMtx.Unlock()
		return service_def.ErrorKeyAlreadyExist, nil
	}
	h.opaqueMap[request.GetOpaque()] = nil // set a nil timer to reserve the opaque key
	h.opaqueMapMtx.Unlock()

	undoRegistration := func() {
		h.opaqueMapMtx.Lock()
		defer h.opaqueMapMtx.Unlock()
		delete(h.opaqueMap, request.GetOpaque())
	}

	return nil, undoRegistration
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
			h.clearOpaqueOnce()
		}
	}
}

func (h *HandlerCommon) clearOpaqueOnce() {
	// Should not take more than a second
	var counter int
	startTime := time.Now()
	var locked bool

FORLOOP:
	for {
		select {
		case opaque := <-h.opaquesClearCh:
			if !locked {
				locked = true
				h.opaqueMapMtx.Lock()
			}
			delete(h.opaqueMap, opaque)
			delete(h.opaqueReqMap, opaque)
			delete(h.opaqueReqRespCbMap, opaque)
			counter++
		default:
			break FORLOOP
		}
	}

	if locked {
		h.opaqueMapMtx.Unlock()
	}
	elapsedTime := time.Since(startTime)
	if elapsedTime > 1*time.Second {
		h.logger.Warnf("%v - Clearing %v opaque took %v", h.id, counter, elapsedTime)
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
	RPCTotalWaitTimeInt := (base.PeerToPeerMaxRetry + 1) * (int(base.P2PCommTimeout.Seconds()) + 2)
	RPCTotalWaitTimeSec := time.Duration(RPCTotalWaitTimeInt) * time.Second
	totalTimeOutPeriod := base.PeerToPeerNonExponentialWaitTime + RPCTotalWaitTimeSec + 1*time.Second
	timer := time.NewTimer(totalTimeOutPeriod)
	select {
	case retCh <- retPair:
	// Done
	case <-timer.C:
		if retPair.RespPtr != nil {
			h.logger.Errorf("Unable to respond to caller given type %v opaque %v timed out chan %v",
				retPair.RespPtr.(Response).GetOpcode(),
				retPair.RespPtr.(Response).GetOpaque(), &retCh)
		} else {
			h.logger.Errorf("Unable to respond to caller given %v timed out chan %v", retPair, &retCh)
		}
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
