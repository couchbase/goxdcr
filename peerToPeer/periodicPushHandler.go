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
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"reflect"
	"sync"
	"time"
)

type PeriodicPushHandler struct {
	*HandlerCommon

	ckptSvc         service_def.CheckpointsService
	colManifestSvc  service_def.CollectionsManifestSvc
	backfillReplSvc service_def.BackfillReplSvc
	utils           utilities.UtilsIface

	requestMerger func(fullTopic, sender string, request interface{}) error
}

func NewPeriodicPushHandler(reqCh chan interface{}, logger *log.CommonLogger, lifeCycleId string, cleanupInterval time.Duration, ckptSvc service_def.CheckpointsService, colManifestSvc service_def.CollectionsManifestSvc, backfillReplSvc service_def.BackfillReplSvc, utils utilities.UtilsIface, merger func(string, string, interface{}) error, replSpecSvc service_def.ReplicationSpecSvc) *PeriodicPushHandler {
	finCh := make(chan bool)
	return &PeriodicPushHandler{
		HandlerCommon:   NewHandlerCommon(logger, lifeCycleId, finCh, cleanupInterval, reqCh, replSpecSvc),
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
			} else {
				p.logger.Errorf("PeriodicPushHandler received invalid format: %v", reflect.TypeOf(reqOrResp))
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
		var waitGrp sync.WaitGroup
		errMap := make(base.ErrorMap)
		var errMapMtx sync.Mutex

		var unknownCounter = 1
		p.logger.Infof("Received peer-to-peer push requests from %v", req.Sender)
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
					if pushReq.MainReplication == nil && pushReq.BackfillReplication == nil {
						errMapMtx.Lock()
						unknownCounter++
						errMap[fmt.Sprintf("unknown_%v", unknownCounter)] = err
						errMapMtx.Unlock()
					} else {
						var specId string
						if pushReq.MainReplication != nil {
							specId = pushReq.MainReplication.ReplicationSpecId
						} else {
							specId = pushReq.BackfillReplication.ReplicationSpecId
						}
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
			p.logger.Warnf("Handling peer-to-peer push requests from %v finished with errs %v", req.Sender, resp.ErrorString)
		} else {
			p.logger.Infof("Done handling peer-to-peer push requests from %v", req.Sender)
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

// Since these will be using the same ckpt mgr underneath, they must be run sequentially
func (p *PeriodicPushHandler) storePushRequestInfo(pushReq VBPeriodicReplicateReq, sender string) error {
	errMap := make(base.ErrorMap)
	var checkSpec *metadata.ReplicationSpecification
	var err error
	if pushReq.MainReplication != nil {
		checkSpec, err = p.replSpecSvc.ReplicationSpecReadOnly(pushReq.MainReplication.ReplicationSpecId)
		if err != nil {
			errMap[fmt.Sprintf("node %v mainReplication", sender)] = err
			return fmt.Errorf(base.FlattenErrorMap(errMap))
		}
		if pushReq.MainReplication.InternalSpecId != "" && pushReq.MainReplication.InternalSpecId != checkSpec.InternalId {
			errMap[fmt.Sprintf("node %v mainReplication", sender)] = fmt.Errorf("Mismatch internalSpecID for %v: Expected %v got %v",
				checkSpec.Id, checkSpec.InternalId, pushReq.MainReplication.InternalSpecId)
			return fmt.Errorf(base.FlattenErrorMap(errMap))
		}
		err = p.storePushReqInfoByType(pushReq.MainReplication, common.MainPipeline, sender)
		if err != nil {
			errMap[fmt.Sprintf("node %v mainReplication", sender)] = err
		}
	}

	if pushReq.BackfillReplication != nil && pushReq.BackfillReplication.payload != nil && (*pushReq.BackfillReplication.payload)[pushReq.BackfillReplication.SourceBucketName] != nil {
		if checkSpec == nil {
			checkSpec, err = p.replSpecSvc.ReplicationSpecReadOnly(pushReq.MainReplication.ReplicationSpecId)
			if err != nil {
				errMap[fmt.Sprintf("node %v backfillReplication", sender)] = err
				return fmt.Errorf(base.FlattenErrorMap(errMap))
			}
		}
		if pushReq.BackfillReplication.InternalSpecId != "" && pushReq.BackfillReplication.InternalSpecId != checkSpec.InternalId {
			errMap[fmt.Sprintf("node %v backfillReplication", sender)] = fmt.Errorf("Mismatch internalSpecID for %v: Expected %v got %v",
				checkSpec.Id, checkSpec.InternalId, pushReq.BackfillReplication.InternalSpecId)
			return fmt.Errorf(base.FlattenErrorMap(errMap))
		}
		err = p.storePushReqInfoByType(pushReq.BackfillReplication, common.BackfillPipeline, sender)
		if err != nil {
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
