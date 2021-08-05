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
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
	"net/http"
	"sync/atomic"
	"time"
)

const ModuleName = "P2PManagerImpl"
const randIdLen = 32

type P2PManager interface {
	Start() (PeerToPeerCommAPI, error)
	Stop() error
	GetLifecycleId() string
}

type Handler interface {
	Start() error
	Stop() error
	RegisterOpaque(req Request) error
}

type P2PManagerImpl struct {
	xdcrCompSvc service_def.XDCRCompTopologySvc
	utils       utils.UtilsIface

	lifeCycleId string
	logger      *log.CommonLogger

	started uint32
	finCh   chan bool
	commAPI PeerToPeerCommAPI

	receiveChsMap map[OpCode]chan interface{}

	receiveHandlerFinCh chan bool
	receiveHandlers     map[OpCode]Handler

	latestKnownPeers *KnownPeers
}

func NewPeerToPeerMgr(loggerCtx *log.LoggerContext, xdcrCompTopologySvc service_def.XDCRCompTopologySvc, utilsIn utils.UtilsIface) (*P2PManagerImpl, error) {
	randId, err := base.GenerateRandomId(randIdLen, 100)
	if err != nil {
		return nil, err
	}

	return &P2PManagerImpl{
		xdcrCompSvc:         xdcrCompTopologySvc,
		utils:               utilsIn,
		lifeCycleId:         randId,
		logger:              log.NewLogger(ModuleName, loggerCtx),
		finCh:               make(chan bool),
		receiveChsMap:       initReceiveChsMap(),
		receiveHandlerFinCh: make(chan bool),
		receiveHandlers:     map[OpCode]Handler{},
		latestKnownPeers:    &KnownPeers{PeersMap: PeersMapType{}},
	}, nil
}

func initReceiveChsMap() map[OpCode]chan interface{} {
	receiveMap := make(map[OpCode]chan interface{})
	for i := OpcodeMin; i < OpcodeMax; i++ {
		receiveMap[i] = make(chan interface{}, base.MaxP2PReceiveChLen)
	}
	return receiveMap
}

func (p *P2PManagerImpl) Start() (PeerToPeerCommAPI, error) {
	if atomic.CompareAndSwapUint32(&p.started, 0, 1) {
		err := p.runHandlers()
		if err != nil {
			return nil, err
		}
		p.initCommAPI()
		p.logger.Infof("P2PManagerImpl started with lifeCycleId %v", p.lifeCycleId)
		go p.sendDiscoveryRequest()
		return p.commAPI, nil
	}
	return nil, fmt.Errorf("P2PManagerImpl already started")
}

func (p *P2PManagerImpl) Stop() error {
	return nil
}

func (p *P2PManagerImpl) GetLifecycleId() string {
	return p.lifeCycleId
}

func (p *P2PManagerImpl) runHandlers() error {
	for i := OpcodeMin; i < OpcodeMax; i++ {
		switch i {
		case ReqDiscovery:
			p.receiveChsMap[i] = make(chan interface{}, base.MaxP2PReceiveChLen)
			p.receiveHandlers[i] = NewDiscoveryHandler(p.receiveChsMap[i], p.logger, p.lifeCycleId, p.latestKnownPeers, base.P2POpaqueCleanupInterval)
		default:
			return fmt.Errorf(fmt.Sprintf("Unknown opcode %v", i))
		}
	}

	for i := OpcodeMin; i < OpcodeMax; i++ {
		err := p.receiveHandlers[i].Start()
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *P2PManagerImpl) initCommAPI() {
	p.commAPI = NewP2pCommAPIHelper(p.receiveChsMap, p.utils, p.xdcrCompSvc)
}

func (p *P2PManagerImpl) sendDiscoveryRequest() error {
	var peers []string
	var err error
	retryOp := func() error {
		peers, err = p.xdcrCompSvc.PeerNodesAdminAddrs()
		if err != nil {
			p.logger.Errorf("PeerNodesAdminAddrs() err: %v\n", err)
		}
		errMap := make(base.ErrorMap)
		for _, peerAddr := range peers {
			myHost, err := p.xdcrCompSvc.MyHostAddr()
			if err != nil {
				p.logger.Errorf("Error getting myHostAddr %v", err)
				continue
			}

			common := NewRequestCommon(myHost, peerAddr, p.GetLifecycleId(), "", getOpaqueWrapper())
			discoveryReq := NewP2PDiscoveryReq(common)
			err = p.receiveHandlers[ReqDiscovery].RegisterOpaque(discoveryReq)
			if err != nil {
				errMap[peerAddr] = err
				continue
			}

			handlerResult, err := p.commAPI.P2PSend(discoveryReq)
			if err != nil {
				p.logger.Errorf("P2PSend %v resulted in %v", discoveryReq, err)
				errMap[peerAddr] = err
				continue
			}
			if handlerResult.GetError() != nil || handlerResult.GetHttpStatusCode() != http.StatusOK {
				errMap[peerAddr] = fmt.Errorf("%v-%v", handlerResult.GetError(), handlerResult.GetHttpStatusCode())
			}
		}
		if len(errMap) > 0 {
			return fmt.Errorf(base.FlattenErrorMap(errMap))
		} else {
			return nil
		}
	}
	err = p.utils.ExponentialBackoffExecutor("sendDiscoveryRequest", base.BucketInfoOpWaitTime, base.BucketInfoOpMaxRetry,
		base.BucketInfoOpRetryFactor, retryOp)
	if err != nil {
		p.logger.Errorf("Unable to send discovery request to some or all the nodes... %v", err)
		return err
	}
	return nil
}

func getOpaqueWrapper() uint32 {
	return base.GetOpaque(0, uint16(time.Now().UnixNano()))
}

type P2pCommAPIimpl struct {
	receiveChs     map[OpCode]chan interface{}
	utils          utils.UtilsIface
	xdcrCompTopSvc service_def.XDCRCompTopologySvc
}

func NewP2pCommAPIHelper(receiveChs map[OpCode]chan interface{}, utils utils.UtilsIface, xdcrCompTopSvc service_def.XDCRCompTopologySvc) *P2pCommAPIimpl {
	return &P2pCommAPIimpl{
		receiveChs:     receiveChs,
		utils:          utils,
		xdcrCompTopSvc: xdcrCompTopSvc,
	}
}

func (p2p *P2pCommAPIimpl) P2PReceive(req ReqRespCommon) (HandlerResult, error) {
	result := &HandlerResultImpl{}

	reqType := req.GetOpcode()

	receiveCh, found := p2p.receiveChs[reqType]
	if !found {
		result.Err = ErrorInvalidOpcode
		return result, ErrorInvalidOpcode
	}

	select {
	case receiveCh <- req:
		return result, nil
	default:
		result.Err = ErrorReceiveChanFull
	}

	return result, ErrorReceiveChanFull
}

func getDevOnlyPath() string {
	return fmt.Sprintf("_goxdcr/%v", base.XDCRPeerToPeerPath)
}

func (p2p *P2pCommAPIimpl) P2PSend(req Request) (HandlerResult, error) {
	payload, err := req.Serialize()
	if err != nil {
		return nil, err
	}

	var out interface{}
	err, statusCode := p2p.utils.QueryRestApiWithAuth(req.GetTarget(), getDevOnlyPath(), false, "", "", base.HttpAuthMechPlain, nil, false, nil, nil, base.MethodPost, base.JsonContentType,
		payload, 0, &out, nil, false, nil)
	result := &HandlerResultImpl{HttpStatusCode: statusCode, Err: err}
	return result, err
}
