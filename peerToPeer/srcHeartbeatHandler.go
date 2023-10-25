package peerToPeer

import (
	"time"

	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def"
)

type SrcHeartbeatHandler struct {
	*HandlerCommon

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

}

func (s *SrcHeartbeatHandler) handleResponse(resp *SourceHeartbeatResp) {

}

func NewSrcHeartbeatHandler(reqCh []chan interface{}, logger *log.CommonLogger, lifecycleId string,
	cleanupInterval time.Duration, replSpecSvc service_def.ReplicationSpecSvc) *SrcHeartbeatHandler {
	finCh := make(chan bool)
	handler := &SrcHeartbeatHandler{
		HandlerCommon: NewHandlerCommon("SrcHeartbeatHandler", logger, lifecycleId, finCh, cleanupInterval, reqCh, replSpecSvc),
		finCh:         finCh,
	}
	return handler
}
