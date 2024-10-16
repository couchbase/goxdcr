package peerToPeer

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
)

type SourceClustersProvider interface {
	GetSourceClustersInfoV1() (map[string]string, map[string][]*metadata.ReplicationSpecification, map[string][]string, map[string]time.Time, map[string]time.Time, error)
}

type HeartbeatCache struct {
	metadata.HeartbeatMetadata

	cacheMtx    sync.RWMutex
	expiryTimer *time.Timer
	refreshTime time.Time
}

// note: should NOT be concurrently called on the same HeartbeatCache value
func (h *HeartbeatCache) LoadInfoFrom(req *SourceHeartbeatReq) {
	h.SourceClusterUUID = req.SourceClusterUUID
	h.SourceClusterName = req.SourceClusterName
	h.NodesList = req.NodesList
	h.SourceSpecsList = req.specs
	h.TTL = req.TTL
}

func (h *HeartbeatCache) HasHeartbeatMetadataChanged(incomingReq *SourceHeartbeatReq) bool {
	// check if source cluster's TTL for heartbeats has changed
	if h.TTL != incomingReq.TTL {
		return true
	}

	// check if the list of source cluster nodes has changed
	if len(h.NodesList) != len(incomingReq.NodesList) ||
		len(base.StringListsFindMissingFromFirst(h.NodesList, incomingReq.NodesList)) > 0 {
		return true
	}

	// check if replications specs have changed
	if len(h.SourceSpecsList) != len(incomingReq.specs) ||
		!h.SourceSpecsList.SameAs(incomingReq.specs) {
		return true
	}

	return false
}

// note: should NOT be concurrently called on the same HeartbeatCache value
func (h *HeartbeatCache) UpdateRefreshTime() {
	h.refreshTime = time.Now()
}

func NewHeartbeatCache(ttl time.Duration, destroyOp func()) *HeartbeatCache {
	return &HeartbeatCache{
		expiryTimer: time.AfterFunc(ttl, destroyOp),
		refreshTime: time.Now(),
	}
}

type SrcHeartbeatHandler struct {
	*HandlerCommon
	xdcrCompTopologySvc service_def.XDCRCompTopologySvc
	sendFunc            sendPeerOnceFunc
	lifecycleIdGetter   func() string

	finCh chan bool

	heartbeatMtx sync.RWMutex
	heartbeatMap map[string]*HeartbeatCache // key is cluster UUID
}

var _ SourceClustersProvider = (*SrcHeartbeatHandler)(nil)

func NewSrcHeartbeatHandler(reqCh []chan interface{}, logger *log.CommonLogger, lifecycleId string, cleanupInterval time.Duration, replSpecSvc service_def.ReplicationSpecSvc, xdcrCompTopologySvc service_def.XDCRCompTopologySvc, sendPeerOnce sendPeerOnceFunc, getLifeCycleId func() string) *SrcHeartbeatHandler {
	finCh := make(chan bool)
	handler := &SrcHeartbeatHandler{
		HandlerCommon:       NewHandlerCommon("SrcHeartbeatHandler", logger, lifecycleId, finCh, cleanupInterval, reqCh, replSpecSvc),
		finCh:               finCh,
		xdcrCompTopologySvc: xdcrCompTopologySvc,
		heartbeatMap:        map[string]*HeartbeatCache{},
		sendFunc:            sendPeerOnce,
		lifecycleIdGetter:   getLifeCycleId,
	}
	return handler
}

func (s *SrcHeartbeatHandler) Start() error {
	s.HandlerCommon.Start()
	go s.handler()
	go s.periodicPrintSummary()
	return nil
}

func (s *SrcHeartbeatHandler) Stop() error {
	close(s.finCh)
	return nil
}

func (s *SrcHeartbeatHandler) handler() {
	for {
		select {
		case <-s.finCh:
			return
		case req := <-s.receiveReqCh:
			if base.SrcHeartbeatIgnoreIncoming {
				s.logger.Debugf("ignoring heartbeat from remote: %v", req)
				continue
			}
			sourceHeartbeatReq, isReq := req.(*SourceHeartbeatReq)
			if isReq {
				go s.handleRequest(sourceHeartbeatReq)
			}
		case resp := <-s.receiveRespCh:
			manifestsResp, isResp := resp.(*SourceHeartbeatResp)
			if isResp {
				s.handleResponse(manifestsResp)
			}
		}
	}
}

func (s *SrcHeartbeatHandler) handleRequest(req *SourceHeartbeatReq) {
	// For now just simple messaging
	s.heartbeatMtx.RLock()
	hbCache, found := s.heartbeatMap[req.SourceClusterUUID]
	if found {
		defer s.heartbeatMtx.RUnlock()

		hbCache.cacheMtx.Lock()
		defer hbCache.cacheMtx.Unlock()

		if !hbCache.expiryTimer.Stop() {
			// heartbeat arrived too late; cache entry has already expired
			return
		}
		hbCache.expiryTimer.Reset(req.TTL)

		if req.ProxyMode {
			go s.forwardToPeers(req)
		}
		hbCache.UpdateRefreshTime()

		if hbCache.HasHeartbeatMetadataChanged(req) {
			hbCache.LoadInfoFrom(req)
		}
		return
	}
	s.heartbeatMtx.RUnlock()

	if req.ProxyMode {
		go s.forwardToPeers(req)
	}

	s.heartbeatMtx.Lock()
	_, found = s.heartbeatMap[req.SourceClusterUUID]
	if !found {
		hbCache = NewHeartbeatCache(req.TTL,
			func() {
				s.heartbeatMtx.Lock()
				delete(s.heartbeatMap, req.SourceClusterUUID)
				s.heartbeatMtx.Unlock()
			},
		)
		s.heartbeatMap[req.SourceClusterUUID] = hbCache
		hbCache.LoadInfoFrom(req)
	}
	s.heartbeatMtx.Unlock()
}

func (s *SrcHeartbeatHandler) handleResponse(resp *SourceHeartbeatResp) {
	// TODO - we'll see if cache stuff later
	s.logger.Infof("Got heartbeat response from target cluster %v\n")

}

func (s *SrcHeartbeatHandler) PrintStatusSummary() {
	s.heartbeatMtx.RLock()
	defer s.heartbeatMtx.RUnlock()

	if len(s.heartbeatMap) == 0 {
		return
	}

	var statusLogStmt strings.Builder
	statusLogStmt.WriteString("Heartbeats heard from ")

	for srcUUID, cache := range s.heartbeatMap {
		statusLogStmt.WriteString(fmt.Sprintf("- SrcUUID: %v SrcName: %v Nodes: %v Replications: { ", srcUUID, cache.SourceClusterName, cache.NodesList))
		for _, spec := range cache.SourceSpecsList {
			statusLogStmt.WriteString(fmt.Sprintf("SrcBucket %v => TgtBucket %v ", spec.SourceBucketName, spec.TargetBucketName))
		}
		statusLogStmt.WriteString(fmt.Sprintf("} RefreshTime: %v TTL: %v", cache.refreshTime, cache.TTL))
	}

	s.logger.Infof(statusLogStmt.String())
}

func (s *SrcHeartbeatHandler) forwardToPeers(origReq *SourceHeartbeatReq) {
	origReq.DisableProxyMode()

	opts := NewSendOpts(false, base.P2PCommTimeout, base.PeerToPeerMaxRetry)
	getReqFunc := func(src, tgt string) Request {
		requestCommon := NewRequestCommon(src, tgt, s.lifecycleIdGetter(), "", getOpaqueWrapper())
		return NewSourceHeartbeatReq(requestCommon).LoadInfoFrom(origReq)
	}

	err, peersFailedToSend := s.sendFunc(ReqSrcHeartbeat, getReqFunc, opts)
	if err != nil {
		s.logger.Warnf("Unable to send proxy heartbeats to other nodes %v", err)
	}

	if len(peersFailedToSend) > 0 {
		errStr := "Unable to proxy heartbeats to the following nodes: "
		for k, _ := range peersFailedToSend {
			errStr += fmt.Sprintf("%v ", k)
		}
		s.logger.Warnf(errStr)
	}
}

func (s *SrcHeartbeatHandler) GetSourceClustersInfoV1() (map[string]string, map[string][]*metadata.ReplicationSpecification, map[string][]string, map[string]time.Time, map[string]time.Time, error) {
	sourceClusterNamesMap := make(map[string]string)
	sourceUuidSpecsMap := make(map[string][]*metadata.ReplicationSpecification)
	sourceUuidNodesMap := make(map[string][]string)
	heartbeatReceiveTime := make(map[string]time.Time)
	heartbeatExpiryTime := make(map[string]time.Time)

	s.heartbeatMtx.RLock()
	defer s.heartbeatMtx.RUnlock()
	for srcUUID, hb := range s.heartbeatMap {
		hb.cacheMtx.RLock()

		sourceClusterNamesMap[srcUUID] = hb.SourceClusterName
		sourceUuidSpecsMap[srcUUID] = hb.SourceSpecsList.Clone()
		sourceUuidNodesMap[srcUUID] = base.SortStringList(base.CloneStringList(hb.NodesList))
		heartbeatReceiveTime[srcUUID] = hb.refreshTime
		heartbeatExpiryTime[srcUUID] = hb.refreshTime.Add(hb.TTL)

		hb.cacheMtx.RUnlock()
	}

	return sourceClusterNamesMap, sourceUuidSpecsMap, sourceUuidNodesMap, heartbeatReceiveTime, heartbeatExpiryTime, nil
}

func (s *SrcHeartbeatHandler) periodicPrintSummary() {
	if base.SrcHeartbeatIgnoreIncoming {
		return
	}

	ticker := time.NewTicker(base.SrcHeartbeatSummaryInterval)
	for {
		select {
		case <-s.finCh:
			ticker.Stop()
			return
		case <-ticker.C:
			s.PrintStatusSummary()
		}
	}
}
