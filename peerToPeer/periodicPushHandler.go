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
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"time"
)

type PeriodicPushHandler struct {
	*HandlerCommon

	ckptSvc         service_def.CheckpointsService
	colManifestSvc  service_def.CollectionsManifestSvc
	backfillReplSvc service_def.BackfillReplSvc
	utils           utilities.UtilsIface

	requestMerger func(fullTopic string, request interface{}) error
}

func NewPeriodicPushHandler(reqCh chan interface{}, logger *log.CommonLogger, lifeCycleId string, cleanupInterval time.Duration, ckptSvc service_def.CheckpointsService, colManifestSvc service_def.CollectionsManifestSvc, backfillReplSvc service_def.BackfillReplSvc, utils utilities.UtilsIface, merger func(string, interface{}) error) *PeriodicPushHandler {
	finCh := make(chan bool)
	return &PeriodicPushHandler{
		HandlerCommon:   NewHandlerCommon(logger, lifeCycleId, finCh, cleanupInterval, reqCh),
		ckptSvc:         ckptSvc,
		colManifestSvc:  colManifestSvc,
		backfillReplSvc: backfillReplSvc,
		utils:           utils,
		requestMerger:   merger,
	}
}

func (p *PeriodicPushHandler) Start() error {
	p.HandlerCommon.Start()
	go p.handler()
	return nil
}

func (p *PeriodicPushHandler) Stop() error {
	close(p.finCh)
	return nil
}

func (p *PeriodicPushHandler) handler() {
	for {
		select {
		case <-p.finCh:
			return
		case reqOrResp := <-p.receiveCh:
			peerVBPeriodicPushReq, isReq := reqOrResp.(*PeerVBPeriodicPushReq)
			peerVBPeriodicPusHResp, isResp := reqOrResp.(*PeerVBPeriodicPushResp)
			if isReq {
				p.handleRequest(peerVBPeriodicPushReq)
			} else if isResp {
				p.handleResponse(peerVBPeriodicPusHResp)
			}
		}
	}
}

func (p *PeriodicPushHandler) handleRequest(req *PeerVBPeriodicPushReq) {
	if req == nil {
		p.logger.Warnf("Received nil req")
		return
	}
	resp := req.GenerateResponse().(*PeerVBPeriodicPushResp)

	if req.PushRequests == nil {
		resp.ErrorString = fmt.Sprintf("Received nil push request list")
	} else {
		errMap := make(base.ErrorMap)
		var unknownCounter = 1
		for _, onePushRequest := range *req.PushRequests {
			// TODO - handle this parallelly
			err := p.storePushRequestInfo(onePushRequest)
			if err != nil {
				if onePushRequest.MainReplication == nil && onePushRequest.BackfillReplication == nil {
					errMap[fmt.Sprintf("unknown_%v", unknownCounter)] = err
					unknownCounter++
				} else {
					var specId string
					if onePushRequest.MainReplication != nil {
						specId = onePushRequest.MainReplication.ReplicationSpecId
					} else {
						specId = onePushRequest.BackfillReplication.ReplicationSpecId
					}
					errMap[specId] = err
				}
			}
		}
		if len(errMap) > 0 {
			resp.ErrorString = base.FlattenErrorMap(errMap)
		}
	}

	handlerResult, err := req.CallBack(resp)
	if err != nil || handlerResult != nil && handlerResult.GetError() != nil {
		var handlerResultErr error
		if handlerResult != nil {
			handlerResultErr = handlerResult.GetError()
		}
		p.logger.Errorf("Unable to send resp %v to original req %v - %v %v", resp, req, err, handlerResultErr)
	}
	return
}

func (p *PeriodicPushHandler) handleResponse(resp *PeerVBPeriodicPushResp) {
	_, _, found := p.GetReqAndClearOpaque(resp.GetOpaque())
	if !found {
		p.logger.Errorf("Unable to find opaque %v", resp.GetOpaque())
		return
	}

	if resp.ErrorString != "" {
		p.logger.Errorf("Received err %v from peer node %v", resp.ErrorString, resp.Sender)
	}
}

func (p *PeriodicPushHandler) storePushRequestInfo(pushReq *VBPeriodicReplicateReq) error {
	if pushReq.MainReplication != nil {
		err := p.storePushReqInfoByType(pushReq.MainReplication, common.MainPipeline)
		// TODO - remove after successfully handling store
		p.logger.Infof("NEIL DEBUG got main replication request with %v, err - %v\n", (*pushReq.MainReplication.payload)[pushReq.MainReplication.SourceBucketName], err)
	}

	if pushReq.BackfillReplication != nil && pushReq.BackfillReplication.payload != nil && (*pushReq.BackfillReplication.payload)[pushReq.BackfillReplication.SourceBucketName] != nil {
		err := p.storePushReqInfoByType(pushReq.BackfillReplication, common.BackfillPipeline)
		var atLeastOneFound bool
		for i := uint16(0); i < 1024; i++ {
			pushVB := (*(*pushReq.BackfillReplication.payload)[pushReq.BackfillReplication.SourceBucketName].PushVBs)[i]
			if pushVB != nil && pushVB.BackfillTsks != nil && pushVB.BackfillTsks.Len() > 0 {
				atLeastOneFound = true
			}
		}
		// TODO - remove after successfully handling store
		p.logger.Infof("NEIL DEBUG got backfill replication request with %v - %v - atleastOneFOUND? %v\n", (*pushReq.BackfillReplication.payload)[pushReq.BackfillReplication.SourceBucketName], err, atLeastOneFound)
	}
	return nil
}

func (p *PeriodicPushHandler) storePushReqInfoByType(payload *ReplicationPayload, pipelineType common.PipelineType) error {
	// The push request can only happen when a pipeline is running
	// This means that when a request is received here, pipeline should be active, including this node's
	// If a peer node sends a request but this node does not have an active pipeline, then it's a race condition
	// and should be treated as such

	fullTopic := common.ComposeFullTopic(payload.ReplicationSpecId, pipelineType)
	return p.requestMerger(fullTopic, payload)
}
