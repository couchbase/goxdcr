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
	"github.com/couchbase/goxdcr/utils"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const ModuleName = "P2PManager"
const randIdLen = 32

type P2PManager interface {
	Start() (PeerToPeerCommAPI, error)
	Stop() error
	GetLifecycleId() string

	ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}, wg *sync.WaitGroup) error

	VBMasterCheck
}

type VBMasterCheck interface {
	CheckVBMaster(BucketVBMapType) (map[string]*VBMasterCheckResp, error)
}

type Handler interface {
	Start() error
	Stop() error
	RegisterOpaque(req Request, opts *SendOpts) error
}

type P2PManagerImpl struct {
	utils             utils.UtilsIface
	xdcrCompSvc       service_def.XDCRCompTopologySvc
	bucketTopologySvc service_def.BucketTopologySvc
	replSpecSvc       service_def.ReplicationSpecSvc

	lifeCycleId string
	logger      *log.CommonLogger

	started         uint32
	finCh           chan bool
	commAPI         PeerToPeerCommAPI
	cleanupInterval time.Duration

	receiveChsMap map[OpCode]chan interface{}

	receiveHandlerFinCh chan bool
	receiveHandlers     map[OpCode]Handler

	latestKnownPeers *KnownPeers

	vbMasterCheckHelper VbMasterCheckHelper
}

func NewPeerToPeerMgr(loggerCtx *log.LoggerContext, xdcrCompTopologySvc service_def.XDCRCompTopologySvc, utilsIn utils.UtilsIface, bucketTopologySvc service_def.BucketTopologySvc, replicationSpecSvc service_def.ReplicationSpecSvc, cleanupInt time.Duration) (*P2PManagerImpl, error) {
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
		vbMasterCheckHelper: NewVBMasterCheckHelper(),
		bucketTopologySvc:   bucketTopologySvc,
		replSpecSvc:         replicationSpecSvc,
		cleanupInterval:     cleanupInt,
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
		return p.commAPI, p.loadSpecsFromMetakv()
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
			p.receiveHandlers[i] = NewDiscoveryHandler(p.receiveChsMap[i], p.logger, p.lifeCycleId, p.latestKnownPeers, p.cleanupInterval)
		case ReqVBMasterChk:
			p.receiveChsMap[i] = make(chan interface{}, base.MaxP2PReceiveChLen)
			p.receiveHandlers[i] = NewVBMasterCheckHandler(p.receiveChsMap[i], p.logger, p.lifeCycleId, p.cleanupInterval, p.bucketTopologySvc)
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
	getReqFunc := func(src, tgt string) Request {
		common := NewRequestCommon(src, tgt, p.GetLifecycleId(), "", getOpaqueWrapper())
		discoveryReq := NewP2PDiscoveryReq(common)
		return discoveryReq
	}

	return p.sendToEachPeerOnce(ReqDiscovery, getReqFunc, NewSendOpts(false))
}

func (p *P2PManagerImpl) getSendPreReq() ([]string, string, error) {
	var peers []string
	var myHostAddr string
	var retry1Err error
	var myHostErr error
	retry1 := func() error {
		peers, retry1Err = p.xdcrCompSvc.PeerNodesAdminAddrs()
		if retry1Err != nil {
			p.logger.Errorf("PeerNodesAdminAddrs() err: %v\n", retry1Err)
			return retry1Err
		}

		myHostAddr, myHostErr = p.xdcrCompSvc.MyHostAddr()
		if myHostErr != nil {
			p.logger.Errorf("Error getting myHostAddr %v", myHostErr)
			return myHostErr
		}
		return nil
	}

	err := p.utils.ExponentialBackoffExecutor("getSendPreReq", base.BucketInfoOpWaitTime, base.BucketInfoOpMaxRetry,
		base.BucketInfoOpRetryFactor, retry1)
	if err != nil {
		return nil, "", err
	}
	return peers, myHostAddr, nil
}

func (p *P2PManagerImpl) sendToEachPeerOnce(opCode OpCode, getReqFunc GetReqFunc, cbOpts *SendOpts) error {
	peers, myHost, err := p.getSendPreReq()
	if err != nil {
		return err
	}

	peersToRetry := make(map[string]bool)
	for _, peer := range peers {
		peersToRetry[peer] = true
	}

	retryOp := func() error {
		peersToRetryReplacement := make(map[string]bool)
		var peersToRetryMtx sync.Mutex
		var waitGrp sync.WaitGroup

		var errMapMtx sync.RWMutex
		errMap := make(base.ErrorMap)
		var err error
		for peerAddrTransient, _ := range peersToRetry {
			peerAddr := peerAddrTransient
			waitGrp.Add(1)
			go func() {
				defer waitGrp.Done()
				compiledReq := getReqFunc(myHost, peerAddr)
				err = p.receiveHandlers[opCode].RegisterOpaque(compiledReq, cbOpts)
				if err != nil {
					errMapMtx.Lock()
					errMap[peerAddr] = err
					errMapMtx.Unlock()
					peersToRetryMtx.Lock()
					peersToRetryReplacement[peerAddr] = true
					peersToRetryMtx.Unlock()
					return
				}

				handlerResult, err := p.commAPI.P2PSend(compiledReq)
				if err != nil {
					p.logger.Errorf("P2PSend %v resulted in %v", compiledReq, err)
					errMapMtx.Lock()
					errMap[peerAddr] = err
					errMapMtx.Unlock()
					peersToRetryMtx.Lock()
					peersToRetryReplacement[peerAddr] = true
					peersToRetryMtx.Unlock()
					return
				}

				if handlerResult.GetError() != nil || handlerResult.GetHttpStatusCode() != http.StatusOK {
					errMapMtx.Lock()
					errMap[peerAddr] = fmt.Errorf("%v-%v", handlerResult.GetError(), handlerResult.GetHttpStatusCode())
					errMapMtx.Unlock()
					peersToRetryMtx.Lock()
					peersToRetryReplacement[peerAddr] = true
					peersToRetryMtx.Unlock()
				}
			}()
		}
		waitGrp.Wait()

		peersToRetryMtx.Lock()
		defer peersToRetryMtx.Unlock()
		if len(peersToRetryReplacement) > 0 {
			peersToRetry = peersToRetryReplacement
			return fmt.Errorf(base.FlattenErrorMap(errMap))
		} else {
			return nil
		}
	}

	err = p.utils.ExponentialBackoffExecutor(fmt.Sprintf("sendPeerToPeerReq(%v)", opCode.String()),
		base.BucketInfoOpWaitTime, base.BucketInfoOpMaxRetry, base.BucketInfoOpRetryFactor, retryOp)
	if err != nil {
		p.logger.Errorf("Unable to send %v to some or all the nodes... %v", opCode.String(), err)
		return err
	}
	return nil
}

type GetReqFunc func(source, target string) Request

type SendOpts struct {
	synchronous bool // Do we need to wait until response is heard

	respMapMtx sync.RWMutex
	respMap    SendOptsMap // If synchronous, then the sent requests and responses are stored
}

type SendOptsMap map[string]chan ReqRespPair

func NewSendOpts(sync bool) *SendOpts {
	if sync {
		return &SendOpts{
			synchronous: true,
			respMap:     make(SendOptsMap),
		}
	} else {
		return &SendOpts{}
	}
}

func (s *SendOpts) GetResults() map[string]*ReqRespPair {
	retMap := make(map[string]*ReqRespPair)

	var waitGrp sync.WaitGroup

	s.respMapMtx.RLock()
	for serverName, _ := range s.respMap {
		retMap[serverName] = &ReqRespPair{}
	}

	for serverName, ch := range s.respMap {
		serverNameCpy := serverName
		chCpy := ch
		waitGrp.Add(1)
		go func() {
			defer waitGrp.Done()
			select {
			case pair := <-chCpy:
				retMap[serverNameCpy].ReqPtr = pair.ReqPtr
				retMap[serverNameCpy].RespPtr = pair.RespPtr
			}
		}()
	}
	s.respMapMtx.RUnlock()
	waitGrp.Wait()

	return retMap
}

type ReqRespPair struct {
	ReqPtr  interface{}
	RespPtr interface{}
}

// VB Master check involves:
// 1. Look at all the VBs request incoming
// 2. For VBs that this node hasn't ensured that nobody else has the same VB, send check request to those nodes
// 3. Peer nodes respond with:
//    a. Happy path - NOT_MY_VBUCKET status code with optional payload (if exists checkpoint and backfill information)
//    b. Error path - "I'm VB Owner too" - which means something is wrong and recovery action may be needed (TODO)
func (p *P2PManagerImpl) CheckVBMaster(bucketAndVBs BucketVBMapType) (map[string]*VBMasterCheckResp, error) {
	// Only need to check the non-verified VBs
	filteredSubsets, err := p.vbMasterCheckHelper.GetUnverifiedSubset(bucketAndVBs)
	if err != nil {
		return nil, err
	}

	getReqFunc := func(src, tgt string) Request {
		common := NewRequestCommon(src, tgt, p.GetLifecycleId(), "", getOpaqueWrapper())
		vbCheckReq := NewVBMasterCheckReq(common)
		vbCheckReq.SetBucketVBMap(filteredSubsets)
		return vbCheckReq
	}

	opts := NewSendOpts(true)
	err = p.sendToEachPeerOnce(ReqVBMasterChk, getReqFunc, opts)
	if err != nil {
		return nil, err
	}

	result := opts.GetResults()

	respMap := make(map[string]*VBMasterCheckResp)
	for k, v := range result {
		respMap[k] = v.RespPtr.(*VBMasterCheckResp)
	}

	return respMap, nil
}

func (p *P2PManagerImpl) ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}, wg *sync.WaitGroup) error {
	if wg != nil {
		defer wg.Done()
	}

	oldSpec, ok := oldVal.(*metadata.ReplicationSpecification)
	if !ok {
		return base.ErrorInvalidInput
	}
	newSpec, ok := newVal.(*metadata.ReplicationSpecification)
	if !ok {
		return base.ErrorInvalidInput
	}

	if oldSpec == nil && newSpec != nil {
		p.vbMasterCheckHelper.HandleSpecCreation(newSpec)
	} else if oldSpec != nil && newSpec == nil {
		p.vbMasterCheckHelper.HandleSpecDeletion(oldSpec)
	}
	return nil
}

func (p *P2PManagerImpl) loadSpecsFromMetakv() error {
	specs, err := p.replSpecSvc.AllReplicationSpecs()
	if err != nil {
		return err
	}

	var nilSpec *metadata.ReplicationSpecification
	for _, spec := range specs {
		err = p.ReplicationSpecChangeCallback("", nilSpec, spec, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func getOpaqueWrapper() uint32 {
	return base.GetOpaque(0, uint16(time.Now().UnixNano()))
}
