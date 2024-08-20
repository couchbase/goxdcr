package peerToPeer

import (
	"fmt"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	"time"
)

type ManualBackfillHandler struct {
	*BackfillHandlerCommon
}

type BackfillDelHandler struct {
	*BackfillHandlerCommon
}

type BackfillHandlerCommon struct {
	*HandlerCommon
	backfillMgrGetter func() service_def.BackfillMgrIface // Only used during init

	backfillMgr service_def.BackfillMgrIface
	finCh       chan bool
}

func NewManualBackfillHandler(reqCh []chan interface{}, logger *log.CommonLogger, lifecycleId string,
	cleanupInterval time.Duration, replSpecSvc service_def.ReplicationSpecSvc,
	backfillMgr func() service_def.BackfillMgrIface) *ManualBackfillHandler {
	return &ManualBackfillHandler{
		BackfillHandlerCommon: NewManualBackfillHandlerCommon(reqCh, logger, lifecycleId, cleanupInterval, replSpecSvc, backfillMgr),
	}
}

func NewManualBackfillHandlerCommon(reqCh []chan interface{}, logger *log.CommonLogger, lifecycleId string,
	cleanupInterval time.Duration, replSpecSvc service_def.ReplicationSpecSvc,
	backfillMgr func() service_def.BackfillMgrIface) *BackfillHandlerCommon {
	finCh := make(chan bool)
	handler := &BackfillHandlerCommon{
		HandlerCommon:     NewHandlerCommon("ManualBackfillHandler", logger, lifecycleId, finCh, cleanupInterval, reqCh, replSpecSvc),
		finCh:             nil,
		backfillMgrGetter: backfillMgr,
	}
	return handler
}

func NewBackfillDelHandler(reqCh []chan interface{}, logger *log.CommonLogger, lifecycleId string,
	cleanupInterval time.Duration, replSpecSvc service_def.ReplicationSpecSvc,
	backfillMgr func() service_def.BackfillMgrIface) *BackfillDelHandler {
	return &BackfillDelHandler{
		BackfillHandlerCommon: NewManualBackfillHandlerCommon(reqCh, logger, lifecycleId, cleanupInterval, replSpecSvc, backfillMgr),
	}
}

func (m *BackfillHandlerCommon) Start() error {
	m.HandlerCommon.Start()
	go m.handler()
	return nil
}

func (m *BackfillHandlerCommon) Stop() error {
	close(m.finCh)
	return nil
}

func (m *BackfillHandlerCommon) handler() {
	// We should only start once backfillmgr is ready, so this needs to be done asynchronously
	m.backfillMgr = m.backfillMgrGetter()

	for {
		select {
		case <-m.finCh:
			return
		case req := <-m.receiveReqCh:
			backfillReq, isBackfillReq := req.(*BackfillRequest)
			delReq, isDelReq := req.(*BackfillDelRequest)
			if isBackfillReq {
				m.handleBackfillReq(backfillReq)
			} else if isDelReq {
				m.handleBackfillDeqReq(delReq)
			}
		case resp := <-m.receiveRespCh:
			backfillResp, isBackfillResp := resp.(*BackfillResponse)
			delResp, isDelResp := resp.(*BackfillDelResponse)
			if isBackfillResp {
				m.handleBackfillResp(backfillResp)
			} else if isDelResp {
				m.handleBackfillDelResp(delResp)
			}
		}

	}
}

func (m *BackfillHandlerCommon) handleBackfillReq(req *BackfillRequest) {
	if req == nil || req.SpecId == "" || req.Namespace.IsEmpty() {
		return
	}

	replSpec, err := m.replSpecSvc.ReplicationSpec(req.SpecId)
	if err != nil {
		// Quite odd
		errStr := fmt.Sprintf("Unable to find spec ID %v", req.SpecId)
		m.sendBackBackfillErrAsync(req, errStr)
		return
	}

	settingMap, err := metadata.ParseBackfillIntoSettingMap(req.Namespace.ToIndexString(), replSpec)
	if err != nil {
		m.sendBackBackfillErrAsync(req, err.Error())
		return
	}

	err = m.backfillMgr.GetPipelineSvc().UpdateSettings(settingMap)
	if err != nil {
		m.sendBackBackfillErrAsync(req, err.Error())
	}
}

func (m *BackfillHandlerCommon) sendBackBackfillErrAsync(req *BackfillRequest, errStr string) {
	m.logger.Errorf(errStr)
	// Since sender is holding on to adminport, send this asynchronously
	go func() {
		time.Sleep(base.P2PCommTimeout)
		resp := req.GenerateResponse().(*BackfillResponse)
		resp.ErrorString = errStr
		handlerResult, err := req.CallBack(resp)
		if err != nil || handlerResult.GetError() != nil {
			m.logger.Errorf("Unable to send back error %v - to sender %v (opaque %v): %v - %v",
				errStr, req.Sender, req.GetOpaque(), err, handlerResult.GetError())
		}
	}()
}

func (m *BackfillHandlerCommon) sendBackDelBackfillErrAsync(req *BackfillDelRequest, errStr string) {
	m.logger.Errorf(errStr)
	// Since sender is holding on to adminport, send this asynchronously
	go func() {
		time.Sleep(base.P2PCommTimeout)
		resp := req.GenerateResponse().(*BackfillDelResponse)
		resp.ErrorString = errStr
		handlerResult, err := req.CallBack(resp)
		if err != nil || handlerResult.GetError() != nil {
			m.logger.Errorf("Unable to send back error %v - to sender %v (opaque %v): %v - %v",
				errStr, req.Sender, req.GetOpaque(), err, handlerResult.GetError())
		}
	}()
}

func (m *BackfillHandlerCommon) handleBackfillResp(resp *BackfillResponse) {
	_, _, found := m.GetReqAndClearOpaque(resp.GetOpaque())
	if !found {
		m.logger.Errorf("ManualBackfillHandler Unable to find opaque %v", resp.GetOpaque())
		return
	}

	if resp.ErrorString == "" {
		return
	}

	m.logger.Infof("Manual Backfill opaque %v from node %v returned error %v", resp.GetOpaque(),
		resp.GetSender(), resp.ErrorString)
}

func (m *BackfillHandlerCommon) handleBackfillDeqReq(req *BackfillDelRequest) {
	if req == nil || req.SpecId == "" {
		return
	}

	settingsMap := metadata.ParseDelBackfillIntoSettingMap(req.SpecId)
	err := m.backfillMgr.GetPipelineSvc().UpdateSettings(settingsMap)
	if err != nil {
		m.sendBackDelBackfillErrAsync(req, err.Error())
	}
	return
}

func (m *BackfillHandlerCommon) handleBackfillDelResp(resp *BackfillDelResponse) {
	_, _, found := m.GetReqAndClearOpaque(resp.GetOpaque())
	if !found {
		m.logger.Errorf("ManualBackfillHandler Unable to find opaque %v", resp.GetOpaque())
		return
	}

	if resp.ErrorString == "" {
		return
	}

	m.logger.Infof("Delete Backfill opaque %v from node %v returned error %v", resp.GetOpaque(),
		resp.GetSender(), resp.ErrorString)
}
