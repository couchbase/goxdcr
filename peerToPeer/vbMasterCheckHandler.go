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
	"time"
)

type VBMasterCheckHandler struct {
	*HandlerCommon
	finCh     chan bool
	receiveCh chan interface{}
}

func NewVBMasterCheckHandler(reqCh chan interface{}, logger *log.CommonLogger, lifeCycleId string, cleanupInterval time.Duration) *VBMasterCheckHandler {
	finCh := make(chan bool)
	handler := &VBMasterCheckHandler{
		HandlerCommon: NewHandlerCommon(logger, lifeCycleId, finCh, cleanupInterval),
		finCh:         finCh,
		receiveCh:     reqCh,
	}
	return handler
}

func (v *VBMasterCheckHandler) Start() error {
	v.HandlerCommon.Start()
	return nil
}

func (v *VBMasterCheckHandler) Stop() error {
	close(v.finCh)
	return nil
}

func (h *VBMasterCheckHandler) handleRequest(req *VBMasterCheckReq) {
	// VBMasterCheck doesn't care about life cycle ID
	if req == nil {
		return
	}
	h.logger.Infof("Received VB master check request with the following Bucket -> VBs %v", req.GetBucketVBMap())

	return
}
