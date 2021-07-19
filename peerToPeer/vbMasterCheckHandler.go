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
	"time"
)

type VBMasterCheckHandler struct {
	*HandlerCommon
	finCh     chan bool
	receiveCh chan interface{}

	bucketTopologySvc service_def.BucketTopologySvc
}

const VBMasterCheckSubscriberId = "VBMasterCheckHandler"

func NewVBMasterCheckHandler(reqCh chan interface{}, logger *log.CommonLogger, lifeCycleId string,
	cleanupInterval time.Duration, bucketTopologySvc service_def.BucketTopologySvc) *VBMasterCheckHandler {
	finCh := make(chan bool)
	handler := &VBMasterCheckHandler{
		HandlerCommon:     NewHandlerCommon(logger, lifeCycleId, finCh, cleanupInterval),
		finCh:             finCh,
		receiveCh:         reqCh,
		bucketTopologySvc: bucketTopologySvc,
	}
	return handler
}

func (v *VBMasterCheckHandler) Start() error {
	v.HandlerCommon.Start()
	go v.handler()
	return nil
}

func (v *VBMasterCheckHandler) Stop() error {
	close(v.finCh)
	return nil
}

func (h *VBMasterCheckHandler) handleRequest(req *VBMasterCheckReq) {
	// VBMasterCheck doesn't care about life cycle ID
	if req == nil {
		h.logger.Warnf("Received nil req")
		return
	}
	bucketVBsMap := req.GetBucketVBMap()
	h.logger.Infof("Received VB master check request from %v with the following Bucket -> VBs %v", req.GetSender(), bucketVBsMap)

	resp := req.GenerateResponse().(*VBMasterCheckResp)
	resp.Init()

	for bucketName, vbsList := range bucketVBsMap {
		resp.InitBucket(bucketName)

		// localFeed only cares about source bucket name
		tempRef, err := metadata.NewReplicationSpecification(bucketName, "", "", "", "")
		if err != nil {
			errMsg := fmt.Sprintf("Unable to get vbsList for bucket %v - %v", bucketName, err)
			h.logger.Warnf(errMsg)
			resp.responsePayload[bucketName].OverallPayloadErr = errMsg
			continue
		}
		srcNotificationCh, err := h.bucketTopologySvc.SubscribeToLocalBucketFeed(tempRef, VBMasterCheckSubscriberId)
		if err != nil {
			errMsg := fmt.Sprintf("Unable to get srcNotificationCh for bucket %v - %v", bucketName, err)
			h.logger.Warnf(errMsg)
			resp.responsePayload[bucketName].OverallPayloadErr = errMsg
			continue
		}

		unsubsFunc := func() {
			err = h.bucketTopologySvc.UnSubscribeLocalBucketFeed(tempRef, VBMasterCheckSubscriberId)
			if err != nil {
				h.logger.Warnf("Unable to unsubscribe srcNotificationCh for bucket %v - %v", bucketName, err)
				// Not an error remote side cares about
			}
		}

		latestInfo := <-srcNotificationCh

		// SourceVBMapRO should only contain one node
		myVBMap := latestInfo.GetSourceVBMapRO()
		var oneKey string
		for key, _ := range myVBMap {
			oneKey = key
		}
		myVbsList := myVBMap[oneKey]
		_, _, vbsIntersect := base.ComputeDeltaOfUint16Lists(myVbsList, vbsList, true)
		// Given my list and another list of VBs that I should not own,
		// if there is any intersection, then that's an issue
		if len(vbsIntersect) > 0 {
			errMsg := fmt.Sprintf("Bucket %v has VBs intersect of %v", bucketName, vbsIntersect)
			h.logger.Errorf(errMsg)
			resp.responsePayload[bucketName].RegisterVbsIntersect(vbsIntersect)
			removed, _, _ := base.ComputeDeltaOfUint16Lists(myVbsList, vbsIntersect, true)
			// Whatever are not intersected are OK
			resp.responsePayload[bucketName].RegisterNotMyVBs(removed)
		} else {
			// Everything is not my VBs
			resp.responsePayload[bucketName].RegisterNotMyVBs(vbsList)
		}
		unsubsFunc()
	}

	handlerResult, err := req.CallBack(resp)
	if err != nil || handlerResult.GetError() != nil {
		h.logger.Errorf("Unable to send resp %v to original req %v - %v %v", resp, req, err, handlerResult.GetError())
	}
	return
}

func (h *VBMasterCheckHandler) handler() {
	for {
		select {
		case <-h.finCh:
			return
		case req := <-h.receiveCh:
			// Can be either req or response
			vbMasterReq, isReq := req.(*VBMasterCheckReq)
			vbMasterResp, isResp := req.(*VBMasterCheckResp)
			if isReq {
				h.handleRequest(vbMasterReq)
			} else if isResp {
				h.handleResponse(vbMasterResp)
			}

		}
	}
}

func (v *VBMasterCheckHandler) handleResponse(resp *VBMasterCheckResp) {
	req, retCh, found := v.GetReqAndClearOpaque(resp.GetOpaque())
	if !found {
		v.logger.Warnf("Unable to find opaque %v", resp.GetOpaque())
	}
	if retCh != nil {
		retPair := ReqRespPair{
			ReqPtr:  req,
			RespPtr: resp,
		}
		go v.sendBackSynchronously(retCh, retPair)
	}
}
