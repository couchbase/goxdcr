// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package peerToPeer

import (
	"github.com/couchbase/goxdcr/log"
	"sync"
	"time"
)

type DiscoveryHandler struct {
	*HandlerCommon
	finCh     chan bool
	receiveCh chan interface{}

	knownPeers *KnownPeers
}

type PeersMapType map[string]string

type KnownPeers struct {
	PeersMap PeersMapType
	mapMtx   sync.RWMutex
}

func NewDiscoveryHandler(reqCh chan interface{}, logger *log.CommonLogger, lifecycleId string, knownPeers *KnownPeers, cleanupInterval time.Duration) *DiscoveryHandler {
	finCh := make(chan bool)
	handler := &DiscoveryHandler{
		HandlerCommon: NewHandlerCommon(logger, lifecycleId, finCh, cleanupInterval),
		finCh:         finCh,
		receiveCh:     reqCh,
		knownPeers:    knownPeers,
	}
	return handler
}

func (h *DiscoveryHandler) Start() error {
	h.HandlerCommon.Start()
	go h.handler()
	return nil
}

func (h *DiscoveryHandler) Stop() error {
	close(h.finCh)
	return nil
}

func (h *DiscoveryHandler) handler() {
	for {
		select {
		case <-h.finCh:
			return
		case req := <-h.receiveCh:
			// Can be either req or response
			discoveryReq, isReq := req.(*DiscoveryRequest)
			discoveryResp, isResp := req.(*DiscoveryResponse)
			if isReq {
				h.handleRequest(discoveryReq)
			} else if isResp {
				h.handleResponse(discoveryResp)
			}

		}
	}
}

func (h *DiscoveryHandler) handleRequest(req *DiscoveryRequest) {
	resp := req.GenerateResponse().(*DiscoveryResponse)
	if resp.LocalLifeCycleId == "" {
		// Remote did not know of us before... need to respond back with current life cycle ID
		resp.LocalLifeCycleId = h.lifeCycleId
	}
	if req.RemoteLifeCycleId != "" && req.RemoteLifeCycleId != h.lifeCycleId {
		resp.DiscoveryErrString = ErrorLifecycleMismatch.Error()
	} else {
		var needToUpdate bool
		h.knownPeers.mapMtx.RLock()
		lastKnownSenderId, exists := h.knownPeers.PeersMap[req.Sender]
		if !exists || lastKnownSenderId != req.LocalLifeCycleId {
			needToUpdate = true
		}
		h.knownPeers.mapMtx.RUnlock()

		// Remember this peer
		if needToUpdate {
			h.knownPeers.mapMtx.Lock()
			h.knownPeers.PeersMap[req.Sender] = req.LocalLifeCycleId
			h.logger.Infof("Discovered peers: %v", h.knownPeers.PeersMap)
			h.knownPeers.mapMtx.Unlock()
		}
	}

	handlerResult, err := req.CallBack(resp)
	if err != nil || handlerResult.GetError() != nil {
		h.logger.Errorf("Unable to send resp %v to original req %v - %v %v", resp, req, err, handlerResult.GetError())
	}
}

func (h *DiscoveryHandler) handleResponse(resp *DiscoveryResponse) {
	_, _, found := h.GetReqAndClearOpaque(resp.GetOpaque())
	if !found {
		h.logger.Errorf("Unable to find opaque %v", resp.GetOpaque())
		return
	}

	if resp.DiscoveryErrString == "" {
		// Nothing to do
		return
	}

	if resp.DiscoveryErrString == ErrorLifecycleMismatch.Error() {
		// Erase the out of match entry and wait for re-discovery
		h.knownPeers.mapMtx.Lock()
		h.logger.Infof("Removing peer %v since lifecycle ID mismatched", resp.Sender)
		delete(h.knownPeers.PeersMap, resp.Sender)
		h.knownPeers.mapMtx.Unlock()
	}
}
