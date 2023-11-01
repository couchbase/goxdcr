package peerToPeer

import (
	"strings"
	"time"

	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def"
)

type SrcHeartbeatHandler struct {
	*HandlerCommon
	xdcrCompTopologySvc service_def.XDCRCompTopologySvc

	finCh chan bool
}

func (s *SrcHeartbeatHandler) Start() error {
	s.HandlerCommon.Start()
	go s.handler()
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
			manifestsReq, isReq := req.(*SourceHeartbeatReq)
			if isReq {
				s.handleRequest(manifestsReq)
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
	var specIDs []string
	for _, spec := range req.specs {
		specIDs = append(specIDs, spec.Id)
	}
	s.logger.Infof("Received heartbeat from source cluster %v with specs %v", req.SourceClusterUUID,
		strings.Join(specIDs, ", "))

	// TODO get my uuid
	myUUID, err := s.xdcrCompTopologySvc.MyClusterUuid()
	if err != nil {
		errStr := "Unable to handle source heart beat req because this node could not get source UUID"
		s.logger.Errorf(errStr)
		resp := req.GenerateResponse().(*SourceHeartbeatResp)
		resp.ErrorString = errStr
		req.CallBack(resp)
		return
	}

	peers, err := s.xdcrCompTopologySvc.PeerNodesAdminAddrs()
	if err != nil {
		errStr := "Unable to get peer addresses to verify if I'm a proxy node or not"
		s.logger.Errorf(errStr)
		resp := req.GenerateResponse().(*SourceHeartbeatResp)
		resp.ErrorString = errStr
		req.CallBack(resp)
		return
	}

	s.logger.Infof("peer addresses %v sender %v", peers, req.GetSender())

	// Only respond if I am the proxy node
	resp := req.GenerateResponse().(*SourceHeartbeatResp)

	// Pack response with some things that are useful for the target cluster to know
	resp.TargetClusterUUID = myUUID

	handlerResult, err := req.CallBack(resp)
	if err != nil {
		s.logger.Errorf("Unable to respond to source cluster %v with error %v", req.SourceClusterUUID, err)
	} else if handlerResult.GetError() != nil {
		s.logger.Errorf("Unable to respond to source cluster %v with handlerErr %v", req.SourceClusterUUID, handlerResult.GetError())
	}
}

func (s *SrcHeartbeatHandler) handleResponse(resp *SourceHeartbeatResp) {
	// TODO - we'll see if cache stuff later
	s.logger.Infof("Got heartbeat response from target cluster %v\n")

}

func NewSrcHeartbeatHandler(reqCh []chan interface{}, logger *log.CommonLogger, lifecycleId string, cleanupInterval time.Duration, replSpecSvc service_def.ReplicationSpecSvc, xdcrCompTopologySvc service_def.XDCRCompTopologySvc) *SrcHeartbeatHandler {
	finCh := make(chan bool)
	handler := &SrcHeartbeatHandler{
		HandlerCommon:       NewHandlerCommon("SrcHeartbeatHandler", logger, lifecycleId, finCh, cleanupInterval, reqCh, replSpecSvc),
		finCh:               finCh,
		xdcrCompTopologySvc: xdcrCompTopologySvc,
	}
	return handler
}
