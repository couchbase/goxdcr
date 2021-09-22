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
	common "github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const ModuleName = "P2PManager"
const randIdLen = 32

var ErrorNoPeerDiscovered error = fmt.Errorf("no peer has been discovered")

type P2PManager interface {
	Start() (PeerToPeerCommAPI, error)
	Stop() error
	GetLifecycleId() string

	ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}, wg *sync.WaitGroup) error

	VBMasterCheck
}

type VBMasterCheck interface {
	CheckVBMaster(BucketVBMapType, common.Pipeline) (map[string]*VBMasterCheckResp, error)
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
	ckptSvc           service_def.CheckpointsService
	colManifestSvc    service_def.CollectionsManifestSvc
	backfillReplSvc   service_def.BackfillReplSvc

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
	replicator          ReplicaReplicator
}

func NewPeerToPeerMgr(loggerCtx *log.LoggerContext, xdcrCompTopologySvc service_def.XDCRCompTopologySvc, utilsIn utils.UtilsIface,
	bucketTopologySvc service_def.BucketTopologySvc, replicationSpecSvc service_def.ReplicationSpecSvc,
	cleanupInt time.Duration, ckptSvc service_def.CheckpointsService, colManifestSvc service_def.CollectionsManifestSvc,
	backfillReplSvc service_def.BackfillReplSvc) (*P2PManagerImpl, error) {
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
		ckptSvc:             ckptSvc,
		colManifestSvc:      colManifestSvc,
		backfillReplSvc:     backfillReplSvc,
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
		p.initReplicator()
		p.logger.Infof("P2PManagerImpl started with lifeCycleId %v", p.lifeCycleId)
		go func() {
			// Give ns_server some time to boot up before sending discovery requests
			time.Sleep(base.TopologySvcStatusNotFoundCoolDownPeriod)
			p.sendDiscoveryRequest()
		}()
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
			p.receiveHandlers[i] = NewVBMasterCheckHandler(p.receiveChsMap[i], p.logger, p.lifeCycleId, p.cleanupInterval,
				p.bucketTopologySvc, p.ckptSvc, p.colManifestSvc, p.backfillReplSvc, p.utils)
		case ReqPeriodicPush:
			p.receiveChsMap[i] = make(chan interface{}, base.MaxP2PReceiveChLen)
			p.receiveHandlers[i] = NewPeriodicPushHandler()
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

	_, err := p.xdcrCompSvc.PeerNodesAdminAddrs()
	if err != nil && strings.Contains(err.Error(), service_def.UnknownPoolStr) {
		p.logger.Warnf("%v not sending peer discovery requests since no other nodes have been detected",
			p.GetLifecycleId())
		return ErrorNoPeerDiscovered
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
			return retry1Err
		}

		myHostAddr, myHostErr = p.xdcrCompSvc.MyHostAddr()
		if myHostErr != nil {
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

	err = p.sendToSpecifiedPeersOnce(opCode, getReqFunc, cbOpts, peersToRetry, myHost)
	if err != nil {
		return err
	}
	return nil
}

// getReqFunc must log/handle errors
func (p *P2PManagerImpl) sendToSpecifiedPeersOnce(opCode OpCode, getReqFunc GetReqFunc, cbOpts *SendOpts, peersToRetry map[string]bool, myHost string) error {
	retryOp := func() error {
		peersToRetryReplacement := make(map[string]bool)
		var peersToRetryMtx sync.Mutex
		var waitGrp sync.WaitGroup

		var errMapMtx sync.RWMutex
		errMap := make(base.ErrorMap)
		for peerAddrTransient, _ := range peersToRetry {
			peerAddr := peerAddrTransient
			waitGrp.Add(1)
			go func() {
				defer waitGrp.Done()
				compiledReq := getReqFunc(myHost, peerAddr)
				if compiledReq == nil {
					// Something is wrong and the getReqFunc should have logged an error
					// Since this is not network related, no need to retry
					return
				}
				registerOpaqueErr := p.receiveHandlers[opCode].RegisterOpaque(compiledReq, cbOpts)
				if registerOpaqueErr != nil {
					errMapMtx.Lock()
					errMap[peerAddr] = registerOpaqueErr
					errMapMtx.Unlock()
					peersToRetryMtx.Lock()
					peersToRetryReplacement[peerAddr] = true
					peersToRetryMtx.Unlock()
					return
				}

				handlerResult, p2pSendErr := p.commAPI.P2PSend(compiledReq)
				if p2pSendErr != nil {
					p.logger.Errorf("P2PSend resulted in %v", p2pSendErr)
					errMapMtx.Lock()
					errMap[peerAddr] = p2pSendErr
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

	err := p.utils.ExponentialBackoffExecutor(fmt.Sprintf("sendPeerToPeerReq(%v)", opCode.String()),
		base.PeerToPeerRetryWaitTime, base.PeerToPeerMaxRetry, base.PeerToPeerRetryFactor, retryOp)
	if err != nil {
		p.logger.Errorf("Unable to send %v to some or all the nodes... %v", opCode.String(), err)
		return err
	}
	return nil
}

type GetReqFunc func(source, target string) Request

type SendOpts struct {
	synchronous bool // Do we need to wait until response is heard
	timeout     time.Duration

	respMapMtx sync.RWMutex
	respMap    SendOptsMap // If synchronous, then the sent requests and responses are stored
}

type SendOptsMap map[string]chan ReqRespPair

func NewSendOpts(sync bool) *SendOpts {
	if sync {
		return &SendOpts{
			synchronous: true,
			respMap:     make(SendOptsMap),
			timeout:     base.PeerToPeerNonExponentialWaitTime,
		}
	} else {
		return &SendOpts{}
	}
}

func (s *SendOpts) GetResults() (map[string]*ReqRespPair, base.ErrorMap) {
	retMap := make(map[string]*ReqRespPair)
	type errWrapper struct {
		err error
	}
	retErrMap := make(map[string]*errWrapper)

	var waitGrp sync.WaitGroup

	s.respMapMtx.RLock()
	for serverName, _ := range s.respMap {
		retMap[serverName] = &ReqRespPair{}
		retErrMap[serverName] = &errWrapper{err: nil}
	}

	for serverName, ch := range s.respMap {
		serverNameCpy := serverName
		chCpy := ch
		waitGrp.Add(1)
		go func() {
			defer waitGrp.Done()
			timeoutTimer := time.NewTimer(s.timeout)
			select {
			case pair := <-chCpy:
				timeoutTimer.Stop()
				retMap[serverNameCpy].ReqPtr = pair.ReqPtr
				retMap[serverNameCpy].RespPtr = pair.RespPtr
			case <-timeoutTimer.C:
				retErrMap[serverNameCpy].err = fmt.Errorf("%v - did not hear back from node after %v. Could be due to peer node being busy to respond in time or this XDCR being too busy to handle incoming requests", base.ErrorExecutionTimedOut, s.timeout)
			}
		}()
	}
	s.respMapMtx.RUnlock()
	waitGrp.Wait()

	for serverName, serverErrWrapper := range retErrMap {
		if serverErrWrapper.err == nil {
			delete(retErrMap, serverName)
		} else {
			delete(retMap, serverName)
		}
	}

	// Need to convert back to errorMap
	errMap := make(base.ErrorMap)
	for k, v := range retErrMap {
		if v.err != nil {
			errMap[k] = v.err
		}
	}
	return retMap, errMap
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
func (p *P2PManagerImpl) CheckVBMaster(bucketAndVBs BucketVBMapType, pipeline common.Pipeline) (map[string]*VBMasterCheckResp, error) {
	// Only need to check the non-verified VBs
	filteredSubsets, err := p.vbMasterCheckHelper.GetUnverifiedSubset(bucketAndVBs)
	if err != nil {
		p.logger.Errorf("error GetUnverifiedSubset %v", err)
		return nil, err
	}

	if pipeline == nil {
		return nil, fmt.Errorf("Pipeline is nil")
	}
	genSpec := pipeline.Specification()
	if genSpec == nil || genSpec.GetReplicationSpec() == nil {
		return nil, fmt.Errorf("spec is nil for %v", pipeline.FullTopic())
	}
	spec := genSpec.GetReplicationSpec()

	getReqFunc := func(src, tgt string) Request {
		requestCommon := NewRequestCommon(src, tgt, p.GetLifecycleId(), "", getOpaqueWrapper())
		vbCheckReq := NewVBMasterCheckReq(requestCommon)
		vbCheckReq.SetBucketVBMap(filteredSubsets)
		vbCheckReq.PipelineType = pipeline.Type()
		vbCheckReq.ReplicationId = spec.Id
		vbCheckReq.SourceBucketName = spec.SourceBucketName
		return vbCheckReq
	}

	opts := NewSendOpts(true)
	err = p.sendToEachPeerOnce(ReqVBMasterChk, getReqFunc, opts)
	if err != nil {
		p.logger.Errorf("sendToEachPeerOnce err %v", err)
		return nil, err
	}

	result, errMap := opts.GetResults()

	respMap := make(map[string]*VBMasterCheckResp)
	for k, v := range result {
		if errMap[k] == nil {
			respMap[k] = v.RespPtr.(*VBMasterCheckResp)
		}
	}
	var flattenedErr error
	if len(errMap) > 0 {
		flattenedErr = fmt.Errorf(base.FlattenErrorMap(errMap))
	}
	return respMap, flattenedErr
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
		p.replicator.HandleSpecCreation(newSpec)
	} else if oldSpec != nil && newSpec == nil {
		p.vbMasterCheckHelper.HandleSpecDeletion(oldSpec)
		p.replicator.HandleSpecDeletion(oldSpec)
	} else {
		p.vbMasterCheckHelper.HandleSpecChange(oldSpec, newSpec)
		p.replicator.HandleSpecChange(oldSpec, newSpec)
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

func (p *P2PManagerImpl) initReplicator() {
	p.replicator = NewReplicaReplicator(p.bucketTopologySvc, p.logger.LoggerContext(), p.ckptSvc, p.backfillReplSvc, p.utils, p.colManifestSvc, p.replSpecSvc, p.sendPeriodicPushRequest)
	p.replicator.Start()
}

func (p *P2PManagerImpl) sendPeriodicPushRequest(compiledRequests PeersVBPeriodicReplicateReqs) error {
	var waitGrp sync.WaitGroup
	var errMapMtx sync.RWMutex
	errMap := make(base.ErrorMap)

	for hostPreCpy, reqsListPreCpy := range compiledRequests {
		waitGrp.Add(1)
		host := hostPreCpy
		reqsList := reqsListPreCpy
		go func() {
			defer waitGrp.Done()
			// First prepare common
			getReqFunc := func(src, tgt string) Request {
				requestCommon := NewRequestCommon(src, tgt, p.GetLifecycleId(), "", getOpaqueWrapper())
				peerSendReq := NewPeerVBPeriodicPushReq(requestCommon)
				okList := &VBPeriodicReplicateReqList{}
				getReqErrMap := make(base.ErrorMap)
				for _, req := range *reqsList {
					preSerializeErr := req.PreSerlialize()
					if preSerializeErr != nil {
						preSerializeErr = fmt.Errorf("PeriodicPushReq for %v has PreSerialize error: %v and is not sent to targets", req.GetId(), preSerializeErr)
						getReqErrMap[req.GetId()] = preSerializeErr
					} else {
						*okList = append(*okList, req)
					}
				}
				if len(getReqErrMap) > 0 {
					errMapMtx.Lock()
					errMap[fmt.Sprintf("%v_getReqFunc", host)] = fmt.Errorf(base.FlattenErrorMap(getReqErrMap))
					errMapMtx.Unlock()
				}
				if len(*okList) == 0 {
					return nil
				} else {
					peerSendReq.PushRequests = okList
					return peerSendReq
				}
			}

			_, myHostAddr, err := p.getSendPreReq()
			if err != nil {
				errMapMtx.Lock()
				errMap[host] = err
				errMapMtx.Unlock()
				return
			}

			peersToRetry := make(map[string]bool)
			peersToRetry[host] = true

			opts := NewSendOpts(true)
			err = p.sendToSpecifiedPeersOnce(ReqPeriodicPush, getReqFunc, opts, peersToRetry, myHostAddr)
			if err != nil {
				errMapMtx.Lock()
				errMap[host] = err
				errMapMtx.Unlock()
			}
			// TODO NEIL - next step, handle the incoming request
			// fmt.Printf("sending req to host %v - err %v\n", host, err)
		}()
		waitGrp.Wait()
	}

	if len(errMap) > 0 {
		return fmt.Errorf(base.FlattenErrorMap(errMap))
	}
	return nil
}

func getOpaqueWrapper() uint32 {
	return base.GetOpaque(0, uint16(time.Now().UnixNano()))
}

// Variable and dynamically updated per number of peer KV nodes
var P2POpaqueTimeoutAtomicMin uint32 = 5
