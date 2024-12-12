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
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"
)

type PeriodicPushHandler struct {
	*HandlerCommon

	ckptSvc         service_def.CheckpointsService
	colManifestSvc  service_def.CollectionsManifestSvc
	backfillReplSvc service_def.BackfillReplSvc
	utils           utilities.UtilsIface

	requestMerger func(fullTopic, sender string, request interface{}) error
}

func NewPeriodicPushHandler(reqChs []chan interface{}, logger *log.CommonLogger, lifeCycleId string, cleanupInterval time.Duration, ckptSvc service_def.CheckpointsService, colManifestSvc service_def.CollectionsManifestSvc, backfillReplSvc service_def.BackfillReplSvc, utils utilities.UtilsIface, merger func(string, string, interface{}) error, replSpecSvc service_def.ReplicationSpecSvc) *PeriodicPushHandler {
	finCh := make(chan bool)
	return &PeriodicPushHandler{
		HandlerCommon:   NewHandlerCommon("PeriodicPushHandler", logger, lifeCycleId, finCh, cleanupInterval, reqChs, replSpecSvc),
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
	go func() {
		for {
			select {
			case <-p.finCh:
				return
			case req := <-p.receiveReqCh:
				peerVBPeriodicPushReq, isReq := req.(*PeerVBPeriodicPushReq)
				if isReq {
					// The idea is that the backend merger for multiple periodic push requests should consolidate them
					// Do not let handleRequest become a bottleneck
					go p.handleRequest(peerVBPeriodicPushReq)
				} else {
					p.logger.Errorf("PeriodicPushHandler (Req) received invalid format: %v", reflect.TypeOf(req))
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-p.finCh:
				return
			case resp := <-p.receiveRespCh:
				peerVBPeriodicPushResp, isResp := resp.(*PeerVBPeriodicPushResp)
				if isResp {
					p.handleResponse(peerVBPeriodicPushResp)
				} else {
					p.logger.Errorf("PeriodicPushHandler (Resp) received invalid format: %v", reflect.TypeOf(resp))
				}
			}
		}
	}()
}

func (p *PeriodicPushHandler) handleRequest(req *PeerVBPeriodicPushReq) {
	if req == nil {
		p.logger.Warnf("Received nil req")
		return
	}
	resp := req.GenerateResponse().(*PeerVBPeriodicPushResp)

	if req.PushRequests == nil {
		resp.ErrorString = fmt.Sprintf("Received nil push request list from %v (opaque %v)", req.Sender, req.Opaque)
	} else {
		var waitGrp sync.WaitGroup
		errMap := make(base.ErrorMap)
		var errMapMtx sync.Mutex

		var unknownCounter = 1
		var mainExists bool
		var backfillExists bool
		var globalInfoShasCnt int // represents both GlobalTimestamp and GlobalCounters - need different counters?
		for _, onePushRequest := range *req.PushRequests {
			if onePushRequest == nil {
				continue
			}
			if onePushRequest.MainReplicationExists() {
				mainExists = true
			}
			if onePushRequest.BackfillReplicationExists() {
				backfillExists = true
			}
			for _, oneVBMasterPayload := range *onePushRequest.payload {
				if oneVBMasterPayload == nil {
					continue
				}
				if oneVBMasterPayload.GlobalInfoDoc != nil {
					globalInfoShasCnt += len(oneVBMasterPayload.GlobalInfoDoc.NsMappingRecords)
				}
			}
		}

		p.logger.Infof("Received peer-to-peer push requests from %v (opaque %v): main %v backfill %v globalInfoShaCnt %v",
			req.Sender, req.GetOpaque(), mainExists, backfillExists, globalInfoShasCnt)
		startTime := time.Now()
		for _, pushReqPtr := range *req.PushRequests {
			if pushReqPtr == nil {
				continue
			}
			pushReq := *pushReqPtr
			waitGrp.Add(1)
			go func() {
				defer waitGrp.Done()
				err := p.storePushRequestInfo(pushReq, req.Sender)
				if err != nil {
					specId := pushReq.ReplicationSpecId
					if specId == "" {
						errMapMtx.Lock()
						unknownCounter++
						errMap[fmt.Sprintf("unknown_%v", unknownCounter)] = err
						errMapMtx.Unlock()
					} else {
						errMapMtx.Lock()
						errMap[specId] = err
						errMapMtx.Unlock()
					}
				}
			}()
		}
		waitGrp.Wait()
		if len(errMap) > 0 {
			resp.ErrorString = base.FlattenErrorMap(errMap)
			p.logger.Warnf("Handling peer-to-peer push requests from %v finished with errs %v (opaque %v timeTaken %v)",
				req.Sender, resp.ErrorString, req.GetOpaque(), time.Since(startTime))
		} else {
			p.logger.Infof("Done handling peer-to-peer push requests from %v (opaque %v timeTaken %v)",
				req.Sender, req.GetOpaque(), time.Since(startTime))
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
		p.logger.Errorf("PeriodicPushHandler Unable to find opaque %v", resp.GetOpaque())
		return
	}

	if resp.ErrorString != "" {
		p.logger.Errorf("Received err %v from peer node %v", resp.ErrorString, resp.Sender)
	}
}

func (p *PeriodicPushHandler) storePushRequestInfo(pushReq VBPeriodicReplicateReq, sender string) error {
	errMap := make(base.ErrorMap)
	var checkSpec *metadata.ReplicationSpecification
	var err error

	if pushReq.PushReqIsEmpty() {
		p.logger.Warnf("Received empty pushReq %v from %v", pushReq.ReplicationSpecId, sender)
		return nil
	}

	if pushReq.MainReplicationExists() {
		checkSpec, err = p.replSpecSvc.ReplicationSpecReadOnly(pushReq.ReplicationSpecId)
		if err != nil {
			errMap[fmt.Sprintf("node %v mainReplication", sender)] = err
			return fmt.Errorf(base.FlattenErrorMap(errMap))
		}
		if pushReq.InternalSpecId != "" && pushReq.InternalSpecId != checkSpec.InternalId {
			errMap[fmt.Sprintf("node %v mainReplication", sender)] = fmt.Errorf("Mismatch internalSpecID for %v: Expected %v got %v",
				checkSpec.Id, checkSpec.InternalId, pushReq.InternalSpecId)
			return fmt.Errorf(base.FlattenErrorMap(errMap))
		}
		err = p.storePushReqInfoByType(pushReq.ReplicationPayload, common.MainPipeline, sender)
		if err != nil && !strings.Contains(err.Error(), base.ErrorNoBackfillNeeded.Error()) {
			errMap[fmt.Sprintf("node %v mainReplication", sender)] = err
		}
	}

	if pushReq.BackfillReplicationExists() {
		if checkSpec == nil {
			checkSpec, err = p.replSpecSvc.ReplicationSpecReadOnly(pushReq.ReplicationSpecId)
			if err != nil {
				errMap[fmt.Sprintf("node %v backfillReplication", sender)] = err
				return fmt.Errorf(base.FlattenErrorMap(errMap))
			}
		}
		if pushReq.InternalSpecId != "" && pushReq.InternalSpecId != checkSpec.InternalId {
			errMap[fmt.Sprintf("node %v backfillReplication", sender)] = fmt.Errorf("Mismatch internalSpecID for %v: Expected %v got %v",
				checkSpec.Id, checkSpec.InternalId, pushReq.InternalSpecId)
			return fmt.Errorf(base.FlattenErrorMap(errMap))
		}
		err = p.storePushReqInfoByType(pushReq.ReplicationPayload, common.BackfillPipeline, sender)
		if err != nil && !strings.Contains(err.Error(), base.ErrorNoBackfillNeeded.Error()) {
			errMap[fmt.Sprintf("node %v backfillReplication", sender)] = err
		}
	}

	if len(errMap) == 0 {
		return nil
	} else {
		return fmt.Errorf(base.FlattenErrorMap(errMap))
	}
}

func (p *PeriodicPushHandler) storePushReqInfoByType(payload *ReplicationPayload, pipelineType common.PipelineType, sender string) error {
	// The push request can only happen when a pipeline is running
	// This means that when a request is received here, pipeline should be active, including this node's
	// If a peer node sends a request but this node does not have an active pipeline, then it's a race condition
	// and should be treated as such

	fullTopic := common.ComposeFullTopic(payload.ReplicationSpecId, pipelineType)
	return p.requestMerger(fullTopic, sender, payload)
}
