// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package peerToPeer

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	common "github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/peerToPeer/peerToPeerResults"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/couchbase/goxdcr/v8/utils"
)

const ModuleName = "P2PManager"
const randIdLen = 32

var ErrorNoPeerDiscovered error = fmt.Errorf("no peer has been discovered")
var ErrorNoActiveMergerSet error = fmt.Errorf("No active pipeline is executing to get merger")
var ErrorPreCheckP2PSendFailure error = fmt.Errorf("Could not send request to peer: P2PSend was unsuccessful")

type P2PManager interface {
	VBMasterCheck
	ConnectionPreCheck
	service_def.ClusterHeartbeatAPI

	Start() (PeerToPeerCommAPI, error)
	Stop() error
	GetLifecycleId() string

	ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}, wg *sync.WaitGroup) error

	// Various P2P operations
	SetPushReqMergerOnce(pm func(fullTopic, sender string, req interface{}) error)
	RequestImmediateCkptBkfillPush(replicationId string) error
	SendManifests(spec *metadata.ReplicationSpecification, manifests *metadata.CollectionsManifestPair) error
	SendManualBackfill(specId string, request string) error
	SendDelBackfill(specId string) error
}

type VBMasterCheck interface {
	CheckVBMaster(BucketVBMapType, common.Pipeline) (map[string]*VBMasterCheckResp, error)
}

type InitRemoteClusterRefFunc func(*log.CommonLogger, *metadata.RemoteClusterReference) error

type ConnectionPreCheck interface {
	SendConnectionPreCheckRequest(*metadata.RemoteClusterReference, InitRemoteClusterRefFunc, string)
	RetrieveConnectionPreCheckResult(string) (base.ConnectionErrMapType, bool, error)
}

type Handler interface {
	Start() error
	Stop() error
	RegisterOpaque(req Request, opts *SendOpts) error
	HandleSpecCreation(newSpec *metadata.ReplicationSpecification)
	HandleSpecDeletion(oldSpec *metadata.ReplicationSpecification)
	HandleSpecChange(oldSpec, newSpec *metadata.ReplicationSpecification)
	GetSpecDelNotification(specId, internalId string) (chan bool, error)
	GetReqAndClearOpaque(opaque uint32) (*Request, chan ReqRespPair, bool)
}

type P2PManagerImpl struct {
	utils             utils.UtilsIface
	xdcrCompSvc       service_def.XDCRCompTopologySvc
	bucketTopologySvc service_def.BucketTopologySvc
	replSpecSvc       service_def.ReplicationSpecSvc
	ckptSvc           service_def.CheckpointsService
	colManifestSvc    service_def.CollectionsManifestSvc
	backfillReplSvc   service_def.BackfillReplSvc
	securitySvc       service_def.SecuritySvc
	backfillMgrSvc    func() service_def.BackfillMgrIface
	remoteClusterSvc  service_def.RemoteClusterSvc

	lifeCycleId string
	logger      *log.CommonLogger

	started         uint32
	finCh           chan bool
	commAPI         PeerToPeerCommAPI
	cleanupInterval time.Duration

	receiveChsMap map[OpCode][]chan interface{}

	receiveHandlerFinCh chan bool
	receiveHandlers     map[OpCode]Handler
	receiveHandlersMtx  sync.RWMutex

	latestKnownPeers *KnownPeers

	vbMasterCheckHelper VbMasterCheckHelper
	replicator          ReplicaReplicator
	replicatorInitCh    chan bool

	mergerSetOnce sync.Once
	mergerSetCh   chan bool
	pushReqMerger func(string, string, interface{}) error
}

func NewPeerToPeerMgr(loggerCtx *log.LoggerContext, xdcrCompTopologySvc service_def.XDCRCompTopologySvc, utilsIn utils.UtilsIface, bucketTopologySvc service_def.BucketTopologySvc, replicationSpecSvc service_def.ReplicationSpecSvc, cleanupInt time.Duration, ckptSvc service_def.CheckpointsService, colManifestSvc service_def.CollectionsManifestSvc, backfillReplSvc service_def.BackfillReplSvc, securitySvc service_def.SecuritySvc, backfillMgr func() service_def.BackfillMgrIface, remoteClusterSvc service_def.RemoteClusterSvc) (*P2PManagerImpl, error) {
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
		mergerSetCh:         make(chan bool),
		securitySvc:         securitySvc,
		replicatorInitCh:    make(chan bool, 1),
		backfillMgrSvc:      backfillMgr,
		remoteClusterSvc:    remoteClusterSvc,
	}, nil
}

func initReceiveChsMap() map[OpCode][]chan interface{} {
	receiveMap := make(map[OpCode][]chan interface{})
	return receiveMap
}

func (p *P2PManagerImpl) Start() (PeerToPeerCommAPI, error) {
	if atomic.CompareAndSwapUint32(&p.started, 0, 1) {
		p.remoteClusterSvc.SetHeartbeatSenderAPI(p)

		p.waitForMergerToBeSet()
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
	p.receiveHandlersMtx.Lock()
	defer p.receiveHandlersMtx.Unlock()

	for i := OpcodeMin; i < OpcodeMax; i++ {
		switch i {
		case ReqDiscovery:
			for j := RequestType; j < InvalidType; j++ {
				p.receiveChsMap[i] = append(p.receiveChsMap[i], make(chan interface{}, base.MaxP2PReceiveChLen/int(InvalidType)))
			}
			p.receiveHandlers[i] = NewDiscoveryHandler(p.receiveChsMap[i], p.logger, p.lifeCycleId, p.latestKnownPeers, p.cleanupInterval, p.replSpecSvc)
		case ReqVBMasterChk:
			for j := RequestType; j < InvalidType; j++ {
				p.receiveChsMap[i] = append(p.receiveChsMap[i], make(chan interface{}, base.MaxP2PReceiveChLen/int(InvalidType)))
			}
			p.receiveHandlers[i] = NewVBMasterCheckHandler(p.receiveChsMap[i], p.logger, p.lifeCycleId, p.cleanupInterval,
				p.bucketTopologySvc, p.ckptSvc, p.colManifestSvc, p.backfillReplSvc, p.utils, p.replSpecSvc)
		case ReqPeriodicPush:
			for j := RequestType; j < InvalidType; j++ {
				p.receiveChsMap[i] = append(p.receiveChsMap[i], make(chan interface{}, base.MaxP2PReceiveChLen/int(InvalidType)))
			}
			p.receiveHandlers[i] = NewPeriodicPushHandler(p.receiveChsMap[i], p.logger, p.lifeCycleId, p.cleanupInterval,
				p.ckptSvc, p.colManifestSvc, p.backfillReplSvc, p.utils, p.getPushReqMerger(), p.replSpecSvc)
		case ReqConnectionPreCheck:
			for j := RequestType; j < InvalidType; j++ {
				p.receiveChsMap[i] = append(p.receiveChsMap[i], make(chan interface{}, base.MaxP2PReceiveChLen/int(InvalidType)))
			}
			p.receiveHandlers[i] = NewConnectionPreCheckHandler(p.receiveChsMap[i], p.logger, p.lifeCycleId, p.cleanupInterval, p.utils, p.replSpecSvc)
		case ReqReplSpecManifests:
			for j := RequestType; j < InvalidType; j++ {
				p.receiveChsMap[i] = append(p.receiveChsMap[i], make(chan interface{}, base.MaxP2PReceiveChLen/int(InvalidType)))
			}
			p.receiveHandlers[i] = NewManifestsHandler(p.receiveChsMap[i], p.logger, p.lifeCycleId, p.cleanupInterval, p.replSpecSvc, p.colManifestSvc)
		case ReqManualBackfill:
			for j := RequestType; j < InvalidType; j++ {
				p.receiveChsMap[i] = append(p.receiveChsMap[i], make(chan interface{}, base.MaxP2PReceiveChLen/int(InvalidType)))
			}
			p.receiveHandlers[i] = NewManualBackfillHandler(p.receiveChsMap[i], p.logger, p.lifeCycleId, p.cleanupInterval, p.replSpecSvc, p.backfillMgrSvc)
		case ReqDeleteBackfill:
			for j := RequestType; j < InvalidType; j++ {
				p.receiveChsMap[i] = append(p.receiveChsMap[i], make(chan interface{}, base.MaxP2PReceiveChLen/int(InvalidType)))
			}
			p.receiveHandlers[i] = NewBackfillDelHandler(p.receiveChsMap[i], p.logger, p.lifeCycleId, p.cleanupInterval, p.replSpecSvc, p.backfillMgrSvc)
		case ReqSrcHeartbeat:
			for j := RequestType; j < InvalidType; j++ {
				p.receiveChsMap[i] = append(p.receiveChsMap[i], make(chan interface{}, base.MaxP2PReceiveChLen/int(InvalidType)))
			}
			p.receiveHandlers[i] = NewSrcHeartbeatHandler(p.receiveChsMap[i], p.logger, p.lifeCycleId, p.cleanupInterval, p.replSpecSvc, p.xdcrCompSvc, p.sendToEachPeerOnce, p.GetLifecycleId)
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
	p.commAPI = NewP2pCommAPIHelper(p.receiveChsMap, p.utils, p.xdcrCompSvc, p.securitySvc)
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
	err, _ = p.sendToEachPeerOnce(ReqDiscovery, getReqFunc, NewSendOpts(false, base.PeerToPeerNonExponentialWaitTime, base.PeerToPeerMaxRetry))
	return err
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

type sendPeerOnceFunc func(opCode OpCode, getReqFunc GetReqFunc, cbOpts *SendOpts) (error, map[string]bool)

func (p *P2PManagerImpl) sendToEachPeerOnce(opCode OpCode, getReqFunc GetReqFunc, cbOpts *SendOpts) (error, map[string]bool) {
	peers, myHost, err := p.getSendPreReq()
	if err != nil {
		return err, nil
	}

	if cbOpts != nil && cbOpts.remoteClusterRef != nil {
		// override "peers" to mean "target reference"
		peerAddr, err := cbOpts.remoteClusterRef.MyConnectionStr()
		if err != nil {
			return err, nil
		}
		peers = []string{peerAddr}
	}

	peersToRetry := make(map[string]bool)
	for _, peer := range peers {
		peersToRetry[peer] = true
	}

	var peersFailedToSend map[string]bool
	err, peersFailedToSend = p.sendToSpecifiedPeersOnce(opCode, getReqFunc, cbOpts, peersToRetry, myHost)
	if err != nil {
		// Major failure - unable to send to all nodes
		return err, peersFailedToSend
	}
	return nil, peersFailedToSend
}

func (p *P2PManagerImpl) getMaxRetry(getReqFunc GetReqFunc, myHost string, peersToRetry map[string]bool) int {
	for peer, _ := range peersToRetry {
		opcode := getReqFunc(myHost, peer).GetOpcode()
		// The following operations should not be retried if there is a network partition or connectivity issues
		if opcode == ReqConnectionPreCheck || opcode == ReqReplSpecManifests {
			return 0
		}
	}
	return base.PeerToPeerMaxRetry
}

func (p *P2PManagerImpl) setConnectionPreCheckP2PMsgToStore(taskId string, src string, tgt string, msg string) {
	store := peerToPeerResults.GetConnectionPreCheckStore(p.logger)

	err := store.SetToConnectionPreCheckStoreSpecificTarget(taskId, "P2P/"+src, tgt, []string{msg})
	if err != nil {
		p.logger.Errorf("SetToConnectionPreCheckStore resulted in %v", err)
	}
}

func (p *P2PManagerImpl) handlePreCheckP2PSendErr(taskId string, src string, tgt string, err error) {
	p.setConnectionPreCheckP2PMsgToStore(taskId, src, tgt, fmt.Sprintf("%v", err))
}

func (p *P2PManagerImpl) handlePreCheckP2PSendSuccess(taskId string, src string, tgt string) {
	p.setConnectionPreCheckP2PMsgToStore(taskId, src, tgt, base.ConnectionPreCheckMsgs[base.ConnPreChkResponseWait])
}

func (p *P2PManagerImpl) sendToSpecifiedPeersOnce(opCode OpCode, getReqFunc GetReqFunc, cbOpts *SendOpts, peersToRetryOrig map[string]bool, myHost string) (error, map[string]bool) {
	peersToRetry := make(map[string]bool)
	if len(peersToRetryOrig) == 0 {
		p.logger.Debugf("No peer nodes provided, skipping send of %v request", opCode.String())
		return nil, peersToRetry
	}

	for k, v := range peersToRetryOrig {
		peersToRetry[k] = v
	}
	successOpaqueMap := make(map[string]*uint32)

	retryOp := func() error {
		peersToRetryReplacement := make(map[string]bool)
		var peersToRetryMtx sync.Mutex
		var waitGrp sync.WaitGroup

		var errMapMtx sync.RWMutex
		errMap := make(base.ErrorMap)

		for peerAddr, _ := range peersToRetry {
			var uintVal uint32
			successOpaqueMap[peerAddr] = &uintVal
		}

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
				p.receiveHandlersMtx.RLock()
				registerOpaqueErr := p.receiveHandlers[opCode].RegisterOpaque(compiledReq, cbOpts)
				p.receiveHandlersMtx.RUnlock()
				if registerOpaqueErr != nil {
					errMapMtx.Lock()
					errMap[peerAddr] = registerOpaqueErr
					errMapMtx.Unlock()
					peersToRetryMtx.Lock()
					peersToRetryReplacement[peerAddr] = true
					peersToRetryMtx.Unlock()
					return
				}

				var handlerResult HandlerResult
				var p2pSendErr error
				if cbOpts.remoteClusterRef == nil {
					handlerResult, p2pSendErr = p.commAPI.P2PSend(compiledReq, p.logger)
				} else {
					handlerResult, p2pSendErr = p.commAPI.P2PRemoteSend(compiledReq, cbOpts.remoteClusterRef, p.logger)
				}

				if p2pSendErr != nil {
					p.logger.Errorf("P2PSend resulted in %v", p2pSendErr)

					errMapMtx.Lock()
					errMap[peerAddr] = p2pSendErr
					errMapMtx.Unlock()
					peersToRetryMtx.Lock()
					peersToRetryReplacement[peerAddr] = true
					peersToRetryMtx.Unlock()
					// If unable to send properly, there is no reason to hear back from an opaque
					p.receiveHandlersMtx.RLock()
					p.receiveHandlers[opCode].GetReqAndClearOpaque(compiledReq.GetOpaque())
					p.receiveHandlersMtx.RUnlock()
					return
				}

				if handlerResult.GetError() != nil || handlerResult.GetHttpStatusCode() != http.StatusOK {
					errMapMtx.Lock()
					errMap[peerAddr] = fmt.Errorf("%v-%v", handlerResult.GetError(), handlerResult.GetHttpStatusCode())
					errMapMtx.Unlock()
					peersToRetryMtx.Lock()
					peersToRetryReplacement[peerAddr] = true
					peersToRetryMtx.Unlock()
					// If unable to send properly, there is no reason to hear back from an opaque
					p.receiveHandlersMtx.RLock()
					p.receiveHandlers[opCode].GetReqAndClearOpaque(compiledReq.GetOpaque())
					p.receiveHandlersMtx.RUnlock()
				} else if cbOpts.remoteClusterRef != nil {
					// If we are sending to a remote cluster, the remote cluster won't be able
					// to directly reply back to us because they do not have response back capability since they are not
					// in the same cluster
					p.receiveHandlersMtx.RLock()
					p.receiveHandlers[opCode].GetReqAndClearOpaque(compiledReq.GetOpaque())
					p.receiveHandlersMtx.RUnlock()
				} else {
					cbOpts.sentTimesMtx.Lock()
					cbOpts.sentTimesMap[peerAddr] = time.Now()
					cbOpts.sentTimesMtx.Unlock()
					atomic.StoreUint32(successOpaqueMap[peerAddr], compiledReq.GetOpaque())
				}
			}()
		}
		waitGrp.Wait()

		peersToRetryMtx.Lock()
		defer peersToRetryMtx.Unlock()
		peersToRetry = peersToRetryReplacement
		if len(peersToRetry) > 0 {
			return fmt.Errorf(base.FlattenErrorMap(errMap))
		} else {
			return nil
		}
	}

	err := p.utils.ExponentialBackoffExecutor(fmt.Sprintf("sendPeerToPeerReq(%v)", opCode.String()),
		base.PeerToPeerRetryWaitTime, cbOpts.maxRetry, base.PeerToPeerRetryFactor, retryOp)

	if err != nil && len(peersToRetry) == len(peersToRetryOrig) {
		p.logger.Errorf("Unable to send %v to all the nodes... %v", opCode.String(), err)
		return err, peersToRetry
	} else {
		printOpaqueMap := make(map[string]uint32)
		for k, vPtr := range successOpaqueMap {
			printOpaqueMap[k] = *vPtr
		}
		if len(successOpaqueMap) > 0 {
			if err != nil {
				p.logger.Infof("Sent request type %v to a subset of nodes with opaque: %v... failed to send to nodes: %v",
					opCode.String(), printOpaqueMap, base.GetKeysListFromStrBoolMap(peersToRetry))
			} else {
				p.logger.Infof("Successfully sent request type %v to nodes with opaque: %v", opCode.String(), printOpaqueMap)
			}
		} else {
			p.logger.Infof("Successfully sent request type %v to nodes %v", opCode.String(), peersToRetryOrig)
		}
	}
	return nil, peersToRetry
}

type GetReqFunc func(source, target string) Request

type PeerNodesTimeMap map[string]time.Time
type PeerNodesDurationMap map[string]time.Duration

type SendOpts struct {
	sentTimesMap PeerNodesTimeMap
	sentTimesMtx sync.RWMutex

	synchronous bool // Do we need to wait until response is heard
	timeout     time.Duration
	finCh       chan bool

	respMapMtx sync.RWMutex
	respMap    SendOptsMap // If synchronous, then the sent requests and responses are stored

	maxRetry int

	remoteClusterRef *metadata.RemoteClusterReference // non-nil if we plan to send to a remote cluster's XDCR
}

type SendOptsMap map[string]chan ReqRespPair

func NewSendOpts(sync bool, timeout time.Duration, maxRetry int) *SendOpts {
	newOpt := &SendOpts{
		sentTimesMap: make(PeerNodesTimeMap),
		maxRetry:     maxRetry,
	}
	if sync {
		newOpt.synchronous = true
		newOpt.respMap = make(SendOptsMap)
		newOpt.timeout = timeout
		newOpt.finCh = make(chan bool)
	}
	return newOpt
}

func (s *SendOpts) SetRemoteClusterRef(ref *metadata.RemoteClusterReference) *SendOpts {
	s.remoteClusterRef = ref
	return s
}

func (s *SendOpts) GetSentTime(key string) (time.Time, error) {
	s.sentTimesMtx.RLock()
	defer s.sentTimesMtx.RUnlock()

	if _, exists := s.sentTimesMap[key]; !exists {
		return time.Now(), base.ErrorNotFound
	}
	return s.sentTimesMap[key], nil
}

func (s *SendOpts) GetResults() (map[string]*ReqRespPair, base.ErrorMap, PeerNodesDurationMap) {
	retMap := make(map[string]*ReqRespPair)
	type errWrapper struct {
		err error
	}
	retErrMap := make(map[string]*errWrapper)
	ackTimeMap := make(PeerNodesDurationMap)
	var ackDurationMapMtx sync.RWMutex

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
				response := pair.RespPtr.(Response)
				if response.GetErrorString() != "" {
					retErrMap[serverNameCpy].err = fmt.Errorf(response.GetErrorString())
				}
				sentTime, sentTimeErr := s.GetSentTime(serverNameCpy)
				if sentTimeErr == nil {
					ackDurationMapMtx.Lock()
					ackTimeMap[serverNameCpy] = time.Since(sentTime)
					ackDurationMapMtx.Unlock()
				}
			case <-timeoutTimer.C:
				retErrMap[serverNameCpy].err = fmt.Errorf("%v - did not hear back from node after %v. Could be due to peer node being busy to respond in time or this XDCR being too busy to handle incoming requests", base.ErrorExecutionTimedOut, s.timeout)
			case <-s.finCh:
				retErrMap[serverNameCpy].err = base.ErrorOpInterrupted
				return
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
		// It is possible that a peer node doesn't have
		if v.err != nil {
			errMap[k] = v.err
		}
	}

	return retMap, errMap, ackTimeMap
}

func (s *SendOpts) RemovePeers(toRemove map[string]bool) {
	if len(toRemove) == 0 {
		return
	}

	s.respMapMtx.Lock()
	defer s.respMapMtx.Unlock()

	for k, _ := range toRemove {
		delete(s.respMap, k)
	}
}

type ReqRespPair struct {
	ReqPtr  interface{}
	RespPtr interface{}
}

// VB Master check involves:
//  1. Look at all the VBs request incoming
//  2. For VBs that this node hasn't ensured that nobody else has the same VB, send check request to those nodes
//  3. Peer nodes respond with:
//     a. Happy path - NOT_MY_VBUCKET status code with optional payload (if exists checkpoint and backfill information)
//     b. Error path - "I'm VB Owner too" - which means something is wrong and recovery action may be needed (TODO)
func (p *P2PManagerImpl) CheckVBMaster(bucketAndVBs BucketVBMapType, pipeline common.Pipeline) (map[string]*VBMasterCheckResp, error) {
	// Only need to check the non-verified VBs
	filteredSubsets, err := p.vbMasterCheckHelper.GetUnverifiedSubset(bucketAndVBs)
	if err != nil {
		p.logger.Errorf("error GetUnverifiedSubset %v", err)
		return nil, err
	}

	if pipeline == nil {
		return nil, base.ErrorNilPipeline
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
		vbCheckReq.ReplicationId = spec.Id
		vbCheckReq.SourceBucketName = spec.SourceBucketName
		vbCheckReq.InternalSpecId = spec.InternalId
		return vbCheckReq
	}

	opts := NewSendOpts(true, metadata.GetP2PTimeoutFromSettings(pipeline.Settings()), base.PeerToPeerMaxRetry)
	err, peersFailedToSend := p.sendToEachPeerOnce(ReqVBMasterChk, getReqFunc, opts)
	if err != nil {
		// Major error unable to send to all peers, otherwise continue to get the rest that was able to sent
		p.logger.Errorf("sendToEachPeerOnce err %v", err)
		return nil, err
	}

	opts.RemovePeers(peersFailedToSend)
	result, errMap, durationMap := opts.GetResults()

	if len(durationMap) > 0 {
		p.logger.Infof("CheckVBMaster peer nodes' successful response times: %v", durationMap)
	}

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

	<-p.replicatorInitCh
	if oldSpec == nil && newSpec != nil {
		p.vbMasterCheckHelper.HandleSpecCreation(newSpec)
		p.replicator.HandleSpecCreation(newSpec)
		p.receiveHandlersMtx.RLock()
		for _, handler := range p.receiveHandlers {
			handler.HandleSpecCreation(newSpec)
		}
		p.receiveHandlersMtx.RUnlock()
	} else if oldSpec != nil && newSpec == nil {
		p.vbMasterCheckHelper.HandleSpecDeletion(oldSpec)
		p.replicator.HandleSpecDeletion(oldSpec)
		p.receiveHandlersMtx.RLock()
		for _, handler := range p.receiveHandlers {
			handler.HandleSpecDeletion(oldSpec)
		}
		p.receiveHandlersMtx.RUnlock()
	} else {
		p.vbMasterCheckHelper.HandleSpecChange(oldSpec, newSpec)
		p.replicator.HandleSpecChange(oldSpec, newSpec)
		p.receiveHandlersMtx.RLock()
		for _, handler := range p.receiveHandlers {
			handler.HandleSpecChange(oldSpec, newSpec)
		}
		p.receiveHandlersMtx.RUnlock()
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
	defer close(p.replicatorInitCh)
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

			opts := NewSendOpts(false, base.PeerToPeerNonExponentialWaitTime, base.PeerToPeerMaxRetry)
			err, _ = p.sendToSpecifiedPeersOnce(ReqPeriodicPush, getReqFunc, opts, peersToRetry, myHostAddr)
			if err != nil {
				errMapMtx.Lock()
				errMap[host] = err
				errMapMtx.Unlock()
			}
		}()
	}
	waitGrp.Wait()

	if len(errMap) > 0 {
		return fmt.Errorf(base.FlattenErrorMap(errMap))
	}
	return nil
}

func getOpaqueWrapper() uint32 {
	return base.GetOpaque(0, uint16(time.Now().UnixNano()))
}

// Variable and dynamically updated per number of peer KV nodes
var P2POpaqueTimeoutAtomicMin uint32 = 2

func (p *P2PManagerImpl) SetPushReqMergerOnce(merger func(fullTopic, sender string, req interface{}) error) {
	p.mergerSetOnce.Do(func() {
		p.pushReqMerger = merger
		close(p.mergerSetCh)
	})
}

func (p *P2PManagerImpl) getPushReqMerger() func(string, string, interface{}) error {
	return p.pushReqMerger
}

func (p *P2PManagerImpl) waitForMergerToBeSet() {
	<-p.mergerSetCh
}

func (p *P2PManagerImpl) RequestImmediateCkptBkfillPush(replicationId string) error {
	return p.replicator.RequestImmediatePush(replicationId)
}

/* Connection Pre-Check */
func sendConnectionPreCheckRequestErrHelper(errMsg string, taskId string, myHostAddr string, info bool, remoteRefForLogs *metadata.RemoteClusterReference, logger *log.CommonLogger) {
	logMsg := errMsg
	errMsg = fmt.Sprintf("%v, taskId=%v", errMsg, taskId)
	if remoteRefForLogs != nil {
		logMsg = fmt.Sprintf("%v, remoteRef=%v", errMsg, remoteRefForLogs.CloneAndRedact())
	}
	if info {
		logger.Infof(logMsg)
	} else {
		logger.Errorf(logMsg)
	}
	store := peerToPeerResults.GetConnectionPreCheckStore(logger)
	connErr := make(base.HostToErrorsMapType)
	connErr[base.PlaceHolderFieldKey] = []string{errMsg}
	err := store.InitConnectionPreCheckResults(taskId, myHostAddr, []string{}, connErr)
	if err != nil {
		logger.Errorf("Error in InitConnectionPreCheckResults(myHosrAddr=%v), err=%v", myHostAddr, err)
	}
}

func (p *P2PManagerImpl) SendConnectionPreCheckRequest(remoteRef *metadata.RemoteClusterReference, initRemoteClusterRef InitRemoteClusterRefFunc, taskId string) {
	loggerCtx := log.CopyCtx(p.logger.LoggerContext())
	additionalInfo := map[string]string{base.ConnectionPreCheckTaskId: taskId}
	loggerCtx.AddMoreContext(additionalInfo)
	logger := log.NewLogger("ConnectionPreCheck", loggerCtx)

	myHostAddr, err := p.xdcrCompSvc.MyHostAddr()
	if err != nil {
		errMsg := fmt.Sprintf("Connection Pre-Check exited because of the error in getting myHostAddr, err=%v", err)
		sendConnectionPreCheckRequestErrHelper(errMsg, taskId, myHostAddr, false, nil, logger)
		return
	}

	// initialise authMech, active hostnames etc.
	err = initRemoteClusterRef(logger, remoteRef)
	if err != nil {
		errMsg := fmt.Sprintf("Connection Pre-Check exited because of the error initialising remote cluster reference, err=%v", err)
		sendConnectionPreCheckRequestErrHelper(errMsg, taskId, myHostAddr, false, remoteRef, logger)
		return
	}
	logger.Infof("Remote cluster reference for Connection Pre-Check initialised as ref=%v", remoteRef.SmallString())

	srvHostnames := remoteRef.GetSRVHostNames()

	connStr, err := remoteRef.MyConnectionStr()
	if err != nil {
		errMsg := fmt.Sprintf("Connection Pre-Check exited because of the error in getting connStr, err=%v", err)
		sendConnectionPreCheckRequestErrHelper(errMsg, taskId, myHostAddr, false, remoteRef, logger)
		return
	}
	logger.Infof("Connection pre-check for myHostAddr=%v, will be with connStr=%v", myHostAddr, connStr)

	username, password, authMech, cert, SANInCert, clientCert, clientKey, err := remoteRef.MyCredentials()
	if err != nil {
		errMsg := fmt.Sprintf("Connection Pre-Check exited because of error in getting credentials err=%v", err)
		sendConnectionPreCheckRequestErrHelper(errMsg, taskId, myHostAddr, false, remoteRef, logger)
		return
	}

	remoteClusterRefMap := remoteRef.ToMap()

	hostname, ok := remoteClusterRefMap[base.RemoteClusterHostName].(string)
	if !ok {
		errMsg := "Connection Pre-Check exited because of error in getting hostname"
		sendConnectionPreCheckRequestErrHelper(errMsg, taskId, myHostAddr, false, remoteRef, logger)
		return
	}

	targetUUID, err := p.utils.GetClusterUUID(
		connStr,
		username,
		password,
		authMech,
		cert,
		SANInCert,
		clientCert,
		clientKey,
		logger,
	)

	if err != nil {
		errMsg := fmt.Sprintf("Connection Pre-Check exited because the cluster credentials are wrong or cluster doesn't exist with the specified IP, err=%v", err)
		sendConnectionPreCheckRequestErrHelper(errMsg, taskId, myHostAddr, false, remoteRef, logger)
		return
	}

	// check version compatility for pre-check support
	srcClusterCompatibility, err := p.xdcrCompSvc.MyClusterCompatibility()
	if err != nil {
		errMsg := fmt.Sprintf("Connection Pre-Check exited because of the error in getting source cluster compatibilty, err=%v", err)
		sendConnectionPreCheckRequestErrHelper(errMsg, taskId, myHostAddr, false, nil, logger)
		return
	}

	if !base.IsClusterCompatible(srcClusterCompatibility, base.VersionForConnectionPreCheckSupport) {
		logger.Infof("Connection Pre-Check exited because: srcClusterCompatibility=%v and VersionForConnectionPreCheckRequest=%v", srcClusterCompatibility, base.VersionForConnectionPreCheckSupport)
		sendConnectionPreCheckRequestErrHelper(base.ConnectionPreCheckMsgs[base.ConnPreChkIsCompatibleVersion], taskId, myHostAddr, true, nil, logger)
		return
	}

	// check for intra-cluster replication
	var sourceClusterUUID string
	if sourceClusterUUID, err = p.xdcrCompSvc.MyClusterUUID(); err != nil {
		logger.Infof("Connection Pre-Check exited because of an error in fetching source cluster UUID: %v", err)
		sendConnectionPreCheckRequestErrHelper(base.ConnectionPreCheckMsgs[base.ConnPreChkUnableToFetchUUID], taskId, myHostAddr, false, nil, logger)
		return
	}
	if sourceClusterUUID == targetUUID {
		logger.Infof("Connection Pre-Check exited because: srcUUID=%v and tgtUUID=%v", sourceClusterUUID, targetUUID)
		sendConnectionPreCheckRequestErrHelper(base.ConnectionPreCheckMsgs[base.ConnPreChkIsIntraClusterReplication], taskId, myHostAddr, true, nil, logger)
		return
	}

	nodeServicesInfo, err := p.utils.GetNodeServicesInfo(
		connStr,
		username,
		password,
		authMech,
		cert,
		SANInCert,
		clientCert,
		clientKey,
		logger,
	)
	if err != nil {
		errMsg := fmt.Sprintf("Connection Pre-Check exited while getting nodeServicesInfo, err=%v", err)
		sendConnectionPreCheckRequestErrHelper(errMsg, taskId, myHostAddr, false, nil, logger)
		return
	}

	nodesList, err := p.utils.GetNodesListFromNodeServicesInfo(logger, nodeServicesInfo)
	if err != nil {
		errMsg := fmt.Sprintf("Connection Pre-Check exited while getting nodesList, nodeServicesInfo=%v, err=%v", nodeServicesInfo, err)
		sendConnectionPreCheckRequestErrHelper(errMsg, taskId, myHostAddr, false, nil, logger)
		return
	}

	var isExternal bool
	if remoteRef.HostnameMode() == metadata.HostnameMode_External {
		// user has network_type set to "external"
		// so use external mode forcefully.
		isExternal = true
	} else {
		// determine user intent for addressing from entered hostname (internal or external addressing)
		isExternal, err = base.CheckIfHostnameIsAlternate(p.utils.GetExternalMgtHostAndPort, nodesList, connStr, remoteRef.IsHttps(), srvHostnames)
		if err != nil {
			errMsg := fmt.Sprintf("Connection Pre-Check exited while checking for user intent, nodeServicesInfo=%v, connStr=%v, isHttps=%v, srvHostnames=%v, err=%v", nodesList, connStr, remoteRef.IsHttps(), srvHostnames, err)
			sendConnectionPreCheckRequestErrHelper(errMsg, taskId, myHostAddr, false, nil, logger)
			return
		}
	}
	logger.Infof("User intent for Connection Pre-Check initialised as isExternal=%v, network_type=%v", isExternal, remoteRef.HostnameMode())

	// get appropriate hostnames and ports to connect to based on user intent
	portsMap, targetNodes, err := p.utils.GetPortsAndHostAddrsFromNodeServices(nodesList, hostname, remoteRef.IsHttps(), isExternal, logger)
	if err != nil {
		errMsg := fmt.Sprintf("Connection Pre-Check exited while getting ports and hostAddrs from nodeServicesInfo, useSecurePort=%v, err=%v", remoteRef.IsHttps(), err)
		sendConnectionPreCheckRequestErrHelper(errMsg, taskId, myHostAddr, false, nil, logger)
		return
	}

	if len(targetNodes) == 0 {
		errMsg := fmt.Sprintf("Connection Pre-Check exited because, no valid nodes found in the target cluser with targetNodes=%v", targetNodes)
		sendConnectionPreCheckRequestErrHelper(errMsg, taskId, myHostAddr, false, remoteRef, logger)
		return
	}

	getReqFunc := func(src, tgt string) Request {
		common := NewRequestCommon(src, tgt, p.GetLifecycleId(), "", getOpaqueWrapper())
		connectionPreCheckReq := NewP2PConnectionPreCheckReq(common, remoteRef, targetNodes, srvHostnames, portsMap, taskId)
		return connectionPreCheckReq
	}

	// Perform connection pre-check.
	// No need to perform DNS SRV resolution check, since this node is the initiating node.
	connectionErrs := executeConnectionPreCheck(remoteRef, targetNodes, nil, portsMap, p.utils, logger)
	for _, node := range targetNodes {
		if connectionErrs == nil {
			connectionErrs = make(base.HostToErrorsMapType)
		}
		_, ok := connectionErrs[node]
		if !ok || len(connectionErrs[node]) == 0 {
			connectionErrs[node] = []string{base.ConnectionPreCheckMsgs[base.ConnPreChkSuccessful]}
		}
	}

	peers, err := p.xdcrCompSvc.PeerNodesAdminAddrs()
	if err != nil && strings.Contains(err.Error(), service_def.UnknownPoolStr) {
		errMsg := fmt.Sprintf("Connection Pre-Check exited because, %v is not sending connection pre-check requests since no other nodes have been detected", p.GetLifecycleId())
		sendConnectionPreCheckRequestErrHelper(errMsg, taskId, myHostAddr, true, nil, logger)
		return
	}

	store := peerToPeerResults.GetConnectionPreCheckStore(logger)

	err = store.InitConnectionPreCheckResults(taskId, myHostAddr, peers, connectionErrs)
	if err != nil {
		logger.Errorf("Connection Pre-Check exited because of the error in InitConnectionPreCheckResults(myHostAddr=%v)=%v", myHostAddr, err)
		return
	}

	// perform pre-check on all the other source nodes except self
	err, peersFailedToSend := p.sendToEachPeerOnce(ReqConnectionPreCheck, getReqFunc, NewSendOpts(false, base.PeerToPeerNonExponentialWaitTime, 1))
	if err != nil {
		logger.Errorf("Connection Pre-Check exited because P2PSend failed for all peers, err=%v", err)
	}
	for _, peer := range peers {
		if failed, ok := peersFailedToSend[peer]; ok && failed {
			p.handlePreCheckP2PSendErr(taskId, myHostAddr, peer, ErrorPreCheckP2PSendFailure)
		} else {
			p.handlePreCheckP2PSendSuccess(taskId, myHostAddr, peer)
		}
	}
}

func (p *P2PManagerImpl) RetrieveConnectionPreCheckResult(taskId string) (base.ConnectionErrMapType, bool, error) {
	store := peerToPeerResults.GetConnectionPreCheckStore(p.logger)
	result, done, err := store.GetFromConnectionPreCheckStore(taskId)
	if err != nil {
		return nil, false, err
	}
	p.logger.Infof("Connection Pre-Check results obtained for taskId=%v are: %v\n", taskId, result)
	return result, done, nil
}

func (p *P2PManagerImpl) SendManifests(spec *metadata.ReplicationSpecification, manifests *metadata.CollectionsManifestPair) error {
	if spec == nil || manifests == nil {
		return base.ErrorInvalidInput
	}

	getReqFunc := func(src, tgt string) Request {
		common := NewRequestCommon(src, tgt, p.GetLifecycleId(), "", getOpaqueWrapper())
		manifestShareReq, reqErr := NewManifestsReq(common, spec, manifests)
		if reqErr != nil {
			// If it returns an error, unlikely to handle it here. But it is also unlikely to return an error
			errStr := fmt.Sprintf("NewManifestsReq returned error %v given spec %v ", reqErr, spec.Id)
			if manifests == nil {
				errStr += "and manifestsPair is nil"
			} else {
				if manifests.Source == nil {
					errStr += "and source manifest is nil"
				} else {
					errStr += fmt.Sprintf("and source manifest is ID %v", manifests.Source.Uid())
				}
				if manifests.Target == nil {
					errStr += "and target manifest is nil"
				} else {
					errStr += fmt.Sprintf("and target manifest is ID %v", manifests.Target.Uid())
				}
			}
			p.logger.Errorf(errStr)
		}
		return manifestShareReq
	}

	// This send has to be async because this node's adminport is busy handling a replication creation request
	// and unable to listen to the response
	err, peersFailedToSend := p.sendToEachPeerOnce(ReqReplSpecManifests, getReqFunc, NewSendOpts(false, base.ShortHttpTimeout, 0))
	if err != nil {
		return err
	}

	if len(peersFailedToSend) > 0 {
		errStr := "Unable to send to the following nodes: "
		for k, _ := range peersFailedToSend {
			errStr += fmt.Sprintf("%v ", k)
		}
		return errors.New(errStr)
	}

	// Peers have all been sent - save this ourselves for our own lookup
	p.receiveHandlersMtx.RLock()
	defer p.receiveHandlersMtx.RUnlock()
	manifestHandler := p.receiveHandlers[ReqReplSpecManifests].(*ManifestsHandler)
	manifestHandler.storeManifestsPair(spec.Id, spec.InternalId, manifests)
	return nil
}

func (p *P2PManagerImpl) SendManualBackfill(specId string, request string) error {
	namespace, err := base.NewCollectionNamespaceFromString(request)
	if err != nil {
		return err
	}

	getReqFunc := func(src, tgt string) Request {
		common := NewRequestCommon(src, tgt, p.GetLifecycleId(), "", getOpaqueWrapper())
		manualBackfillReq, reqErr := NewBackfillReq(common, specId, namespace)
		if reqErr != nil {
			// If it returns an error, unlikely to handle it here. But it is also unlikely to return an error
			errStr := fmt.Sprintf("NewBackfillReq returned error %v given spec %v ", reqErr, specId)
			p.logger.Errorf(errStr)
		}
		return manualBackfillReq
	}

	spec, err := p.replSpecSvc.ReplicationSpec(specId)
	if err != nil {
		return err
	}
	opts := NewSendOpts(false, metadata.GetP2PTimeoutFromSettings(spec.Settings.ToMap(false)),
		base.PeerToPeerMaxRetry)
	err, peersFailedToSend := p.sendToEachPeerOnce(ReqManualBackfill, getReqFunc, opts)
	if err != nil {
		return err
	}

	if len(peersFailedToSend) > 0 {
		errStr := "Unable to send to the following nodes: "
		for k, _ := range peersFailedToSend {
			errStr += fmt.Sprintf("%v ", k)
		}
		return errors.New(errStr)
	}

	return nil
}

func (p *P2PManagerImpl) SendDelBackfill(specId string) error {
	getReqFunc := func(src, tgt string) Request {
		common := NewRequestCommon(src, tgt, p.GetLifecycleId(), "", getOpaqueWrapper())
		delReq, reqErr := NewBackfillDelReq(common, specId)
		if reqErr != nil {
			// If it returns an error, unlikely to handle it here. But it is also unlikely to return an error
			errStr := fmt.Sprintf("NewBackfillDelReq returned error %v given spec %v ", reqErr, specId)
			p.logger.Errorf(errStr)
		}
		return delReq
	}
	spec, err := p.replSpecSvc.ReplicationSpec(specId)
	if err != nil {
		return err
	}

	opts := NewSendOpts(false, metadata.GetP2PTimeoutFromSettings(spec.Settings.ToMap(false)),
		base.PeerToPeerMaxRetry)
	err, peersFailedToSend := p.sendToEachPeerOnce(ReqDeleteBackfill, getReqFunc, opts)
	if err != nil {
		return err
	}

	if len(peersFailedToSend) > 0 {
		errStr := "Unable to send to the following nodes: "
		for k, _ := range peersFailedToSend {
			errStr += fmt.Sprintf("%v ", k)
		}
		return errors.New(errStr)
	}
	return nil
}

func (p *P2PManagerImpl) SendHeartbeatToRemoteV1(reference *metadata.RemoteClusterReference, specs []*metadata.ReplicationSpecification) error {
	srcStr, err := p.xdcrCompSvc.MyHostAddr()
	if err != nil {
		return err
	}
	target, err := reference.MyConnectionStr()
	if err != nil {
		return err
	}

	nodesList, err := p.xdcrCompSvc.PeerNodesAdminAddrs()
	if err != nil {
		return err
	}
	// Add myself back in amongst the peers
	nodesList = append(nodesList, srcStr)

	var sourceClusterUUID, sourceClusterName string
	if sourceClusterUUID, err = p.xdcrCompSvc.MyClusterUUID(); err != nil {
		return err
	}
	if sourceClusterName, err = p.xdcrCompSvc.MyClusterName(); err != nil {
		return err
	}

	getReqFunc := func(src, tgt string) Request {
		common := NewRequestCommon(srcStr, target, p.GetLifecycleId(), "", getOpaqueWrapper())
		req := NewSourceHeartbeatReq(common).SetUUID(sourceClusterUUID).SetClusterName(sourceClusterName).SetNodesList(nodesList)
		for _, spec := range specs {
			req.AppendSpec(spec)
		}
		return req
	}

	// Cannot be synchronous because remote cluster may not be able to respond back
	// This will be a send and forget operation, no ack from the remote
	opts := NewSendOpts(false, 0, base.PeerToPeerMaxRetry).SetRemoteClusterRef(reference)
	err, failedToSend := p.sendToEachPeerOnce(ReqSrcHeartbeat, getReqFunc, opts)
	if err != nil {
		return err
	}

	if len(failedToSend) > 0 {
		errStr := "Unable to send to the following nodes: "
		for k, _ := range failedToSend {
			errStr += fmt.Sprintf("%v ", k)
		}
		return errors.New(errStr)
	}

	return nil
}

func (p *P2PManagerImpl) GetHeartbeatsReceivedV1() (map[string]string, map[string][]*metadata.ReplicationSpecification, map[string][]string, error) {
	p.receiveHandlersMtx.RLock()
	handlerGeneric, ok := p.receiveHandlers[ReqSrcHeartbeat]
	p.receiveHandlersMtx.RUnlock()
	if !ok {
		return nil, nil, nil, fmt.Errorf("unable to find heartbeat handler")
	}
	srcClustersProvider, ok := handlerGeneric.(SourceClustersProvider)
	if !ok {
		return nil, nil, nil, fmt.Errorf("unable to cast heartbeat handler as source cluster provider")
	}
	return srcClustersProvider.GetSourceClustersInfoV1()
}
