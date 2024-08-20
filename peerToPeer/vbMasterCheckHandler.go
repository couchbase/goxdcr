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
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"
	"reflect"
	"sync"
	"time"
)

type VBMasterCheckHandler struct {
	*HandlerCommon

	bucketTopologySvc service_def.BucketTopologySvc
	ckptSvc           service_def.CheckpointsService
	colManifestSvc    service_def.CollectionsManifestSvc
	backfillReplSvc   service_def.BackfillReplSvc
	utils             utilities.UtilsIface
}

const VBMasterCheckSubscriberId = "VBMasterCheckHandler"

func NewVBMasterCheckHandler(reqChs []chan interface{}, logger *log.CommonLogger, lifeCycleId string, cleanupInterval time.Duration, bucketTopologySvc service_def.BucketTopologySvc, ckptSvc service_def.CheckpointsService, collectionsManifestSvc service_def.CollectionsManifestSvc, backfillReplSvc service_def.BackfillReplSvc, utils utilities.UtilsIface, replicationSpecSvc service_def.ReplicationSpecSvc) *VBMasterCheckHandler {
	finCh := make(chan bool)
	handler := &VBMasterCheckHandler{
		HandlerCommon:     NewHandlerCommon(VBMasterCheckSubscriberId, logger, lifeCycleId, finCh, cleanupInterval, reqChs, replicationSpecSvc),
		bucketTopologySvc: bucketTopologySvc,
		ckptSvc:           ckptSvc,
		colManifestSvc:    collectionsManifestSvc,
		backfillReplSvc:   backfillReplSvc,
		utils:             utils,
	}
	return handler
}

func (v *VBMasterCheckHandler) Start() error {
	v.HandlerCommon.Start()
	go v.handler()
	return nil
}

func (v *VBMasterCheckHandler) Stop() error {
	close(v.finCh)
	return nil
}

func (h *VBMasterCheckHandler) handleRequest(req *VBMasterCheckReq) {
	// VBMasterCheck doesn't care about life cycle ID
	if req == nil {
		h.logger.Warnf("Received nil req")
		return
	}

	// It is possible that a peer node has received a spec and is running VBMasterCheck
	// when this node hasn't even gotten the spec set up yet from metakv
	// Retry for a max of 3 seconds
	var finCh chan bool
	var getFinChError error
	retryFunc := func() error {
		var existsErr error
		finCh, existsErr = h.GetSpecDelNotification(req.ReplicationId, req.InternalSpecId)
		if existsErr != nil {
			existsErr = fmt.Errorf("VB master check request from %v with specID %v internalID %v does not exist", req.GetSender(), req.ReplicationId, req.InternalSpecId)
		}
		return existsErr
	}
	getFinChError = h.utils.ExponentialBackoffExecutor("VBMasterCheckHandler.GetSpecDelNotification", base.BucketInfoOpWaitTime,
		base.BucketInfoOpMaxRetry, base.BucketInfoOpRetryFactor, retryFunc)
	if getFinChError != nil {
		resp := req.GenerateResponse().(*VBMasterCheckResp)
		resp.Init()
		resp.ErrorString = getFinChError.Error()
		req.CallBack(resp)
		return
	}

	bucketVBsMap := req.GetBucketVBMap()
	startTime := time.Now()
	h.logger.Infof("Received VB master check request from %v with specID %v opaque %v for the following Bucket -> VBs %v", req.GetSender(), req.ReplicationId, req.GetOpaque(), bucketVBsMap)
	stopMetakvMeasureFunc := h.utils.StartDiagStopwatch(fmt.Sprintf("VBMasterCheckHandler(%v,%v,%v)", req.GetSender(), req.ReplicationId, req.GetOpcode()), base.DiagVBMasterHandleThreshold)

	resp := req.GenerateResponse().(*VBMasterCheckResp)
	resp.Init()
	resp.InternalSpecId = req.InternalSpecId

	waitGrp := &sync.WaitGroup{}
	waitGrp.Add(1)
	go h.populateBucketVBMapsIntoResp(bucketVBsMap, resp, waitGrp, finCh)

	waitGrp.Add(1)
	var mainCkptFetchErr error
	var mainPipelineCkpts map[uint16]*metadata.CheckpointsDoc
	go h.populatePipelineCkpts(common.ComposeFullTopic(req.ReplicationId, common.MainPipeline), waitGrp, &mainCkptFetchErr, &mainPipelineCkpts, finCh)

	waitGrp.Add(1)
	var backfillCkptFetchErr error
	var backfillPipelineCkpts map[uint16]*metadata.CheckpointsDoc
	go h.populatePipelineCkpts(common.ComposeFullTopic(req.ReplicationId, common.BackfillPipeline), waitGrp, &backfillCkptFetchErr, &backfillPipelineCkpts, finCh)

	cachedSrcManifests := make(metadata.ManifestsCache)
	cachedTgtManifests := make(metadata.ManifestsCache)
	var manifestErr error
	waitGrp.Add(1)
	go h.fetchAllManifests(req.ReplicationId, &cachedSrcManifests, &cachedTgtManifests, &manifestErr, waitGrp, finCh)

	var brokenMappingErr error
	var brokenMappingDoc metadata.CollectionNsMappingsDoc
	waitGrp.Add(1)
	go h.fetchBrokenMappingDoc(req.ReplicationId, &brokenMappingDoc, &brokenMappingErr, waitGrp, finCh)

	var backfillTasksErr error
	var backfillTaskSrcManifestId uint64
	backfillTasks := metadata.NewVBTasksMap()
	waitGrp.Add(1)
	go h.fetchBackfillTasks(req.ReplicationId, backfillTasks, &backfillTasksErr, &backfillTaskSrcManifestId, waitGrp, finCh)

	// Get all errors in order
	waitGrp.Wait()
	stopMetakvMeasureFunc()

	if mainCkptFetchErr != nil {
		h.logger.Errorf("%v (%v) - fetchMainCkpt error %v", req.ReplicationId, req.GetOpaque(), mainCkptFetchErr)
		resp.ErrorString = mainCkptFetchErr.Error()
		_, cbErr := req.CallBack(resp)
		if cbErr != nil {
			h.logger.Errorf("Responding back to %v (%v) has err %v", req.Sender, req.GetOpaque(), cbErr)
		}
		return
	}

	if backfillCkptFetchErr != nil {
		h.logger.Errorf("%v (%v) - fetchBackfillCkpt error %v", req.ReplicationId, req.GetOpaque(), backfillCkptFetchErr)
		resp.ErrorString = backfillCkptFetchErr.Error()
		_, cbErr := req.CallBack(resp)
		if cbErr != nil {
			h.logger.Errorf("Responding back to %v (%v) has err %v", req.Sender, req.GetOpaque(), cbErr)
		}
		return
	}

	if manifestErr != nil {
		h.logger.Errorf("%v (%v) - manifest error %v", req.ReplicationId, req.GetOpaque(), manifestErr)
		resp.ErrorString = manifestErr.Error()
		_, cbErr := req.CallBack(resp)
		if cbErr != nil {
			h.logger.Errorf("Responding back to %v (%v) has err %v", req.Sender, req.GetOpaque(), cbErr)
		}
		return
	}

	if brokenMappingErr != nil {
		h.logger.Errorf("%v (%v) - brokenMapping error %v", req.ReplicationId, req.GetOpaque(), brokenMappingErr)
		resp.ErrorString = brokenMappingErr.Error()
		_, cbErr := req.CallBack(resp)
		if cbErr != nil {
			h.logger.Errorf("Responding back to %v (%v) has err %v", req.Sender, req.GetOpaque(), cbErr)
		}
		return
	}

	if backfillTasksErr != nil {
		h.logger.Errorf("%v (%v) - backfillTasks error %v", req.ReplicationId, req.GetOpaque(), backfillTasksErr)
		resp.ErrorString = backfillTasksErr.Error()
		_, cbErr := req.CallBack(resp)
		if cbErr != nil {
			h.logger.Errorf("Responding back to %v (%v) has err %v", req.Sender, req.GetOpaque(), cbErr)
		}
		return
	}

	err := resp.LoadMainPipelineCkpt(mainPipelineCkpts, req.SourceBucketName)
	if err != nil {
		h.logger.Errorf("%v (%v) - when loading pipeline ckpt into response, got %v", req.ReplicationId, req.GetOpaque(), err)
		resp.ErrorString = err.Error()
		_, cbErr := req.CallBack(resp)
		if cbErr != nil {
			h.logger.Errorf("Responding back to %v (%v) has err %v", req.Sender, req.GetOpaque(), cbErr)
		}
		return
	}

	err = resp.LoadBackfillPipelineCkpt(backfillPipelineCkpts, req.SourceBucketName)
	if err != nil {
		h.logger.Errorf("%v (%v) - when loading pipeline ckpt into response, got %v", req.ReplicationId, req.GetOpaque(), err)
		resp.ErrorString = err.Error()
		_, cbErr := req.CallBack(resp)
		if cbErr != nil {
			h.logger.Errorf("Responding back to %v (%v) has err %v", req.Sender, req.GetOpaque(), cbErr)
		}
		return
	}

	err = resp.LoadManifests(cachedSrcManifests, cachedTgtManifests, req.SourceBucketName)
	if err != nil {
		h.logger.Errorf("%v (%v) - when loading manifests into response, got %v", req.ReplicationId, req.GetOpaque(), err)
		resp.ErrorString = err.Error()
		_, cbErr := req.CallBack(resp)
		if cbErr != nil {
			h.logger.Errorf("Responding back to %v (%v) has err %v", req.Sender, req.GetOpaque(), cbErr)
		}
		return
	}

	err = resp.LoadBrokenMappingDoc(brokenMappingDoc, req.SourceBucketName)
	if err != nil {
		h.logger.Errorf("%v (%v) - when loading brokenMappingDoc into response, got %v", req.ReplicationId, req.GetOpaque(), err)
		resp.ErrorString = err.Error()
		_, cbErr := req.CallBack(resp)
		if cbErr != nil {
			h.logger.Errorf("Responding back to %v (%v) has err %v", req.Sender, req.GetOpaque(), cbErr)
		}
		return
	}

	err = resp.LoadBackfillTasks(backfillTasks, req.SourceBucketName, backfillTaskSrcManifestId)
	if err != nil {
		h.logger.Errorf("%v (%v) - when loading brokenMappingDoc into response, got %v", req.ReplicationId, req.GetOpaque(), err)
		resp.ErrorString = err.Error()
		_, cbErr := req.CallBack(resp)
		if cbErr != nil {
			h.logger.Errorf("Responding back to %v (%v) has err %v", req.Sender, req.GetOpaque(), cbErr)
		}
		return
	}

	// Final Callback
	doneProcessedTime := time.Now()
	handlerResult, err := req.CallBack(resp)
	if err != nil || handlerResult != nil && handlerResult.GetError() != nil {
		var handlerResultErr error
		if handlerResult != nil {
			handlerResultErr = handlerResult.GetError()
		}
		h.logger.Errorf("Unable to send resp %v to original req %v (%v) - %v %v", resp.ReplicationSpecId, req.Sender, req.GetOpaque(), err, handlerResultErr)
	} else {
		h.logger.Infof("Replied to VB master check request from %v with specID %v (%v) - total elapsed time: %v processing time: %v timeInQueue: %v",
			req.GetSender(), req.ReplicationId, req.GetOpaque(), time.Since(startTime), doneProcessedTime.Sub(startTime), startTime.Sub(req.GetEnqueuedTime()))
	}
	return
}

var vbmasterChkHandlerIteration uint32

func (h *VBMasterCheckHandler) populateBucketVBMapsIntoResp(bucketVBsMap BucketVBMapType, resp *VBMasterCheckResp, waitGrp *sync.WaitGroup, finCh chan bool) {
	defer waitGrp.Done()
	for bucketName, vbsList := range bucketVBsMap {
		resp.InitBucket(bucketName)

		subsId := VBMasterCheckSubscriberId + base.GetIterationId(&vbmasterChkHandlerIteration)
		// localFeed only cares about source bucket name
		tempRef, err := metadata.NewReplicationSpecification(bucketName, "", "", "", "")
		if err != nil {
			errMsg := fmt.Sprintf("Unable to get vbsList for bucket %v - %v", bucketName, err)
			h.logger.Warnf(errMsg)
			(*resp.payload)[bucketName].OverallPayloadErr = errMsg
			continue
		}
		srcNotificationCh, err := h.bucketTopologySvc.SubscribeToLocalBucketFeed(tempRef, subsId)
		if err != nil {
			errMsg := fmt.Sprintf("Unable to get srcNotificationCh for bucket %v - %v", bucketName, err)
			h.logger.Warnf(errMsg)
			(*resp.payload)[bucketName].OverallPayloadErr = errMsg
			continue
		}

		unsubsFunc := func() {
			err = h.bucketTopologySvc.UnSubscribeLocalBucketFeed(tempRef, subsId)
			if err != nil {
				h.logger.Warnf("Unable to unsubscribe srcNotificationCh for bucket %v - %v", bucketName, err)
				// Not an error remote side cares about
			}
		}

		select {
		case <-finCh:
			// Don't proceed
			unsubsFunc()
			return
		default:
			// Nothing
			break
		}

		var latestInfo service_def.SourceNotification
		latestInfo = <-srcNotificationCh

		// SourceVBMapRO should only contain one node
		myVBMap := latestInfo.GetSourceVBMapRO()
		var oneKey string
		for key, _ := range myVBMap {
			oneKey = key
		}
		myVbsList := base.CloneUint16List(myVBMap[oneKey])
		_, _, vbsIntersect := base.ComputeDeltaOfUint16Lists(myVbsList, vbsList, true)
		// Given my list and another list of VBs that I should not own,
		// if there is any intersection, then that's an issue
		if len(vbsIntersect) > 0 {
			errMsg := fmt.Sprintf("Bucket %v has VBs intersect of %v", bucketName, vbsIntersect)
			h.logger.Errorf(errMsg)
			(*resp.payload)[bucketName].RegisterVbsIntersect(vbsIntersect)
			removed, _, _ := base.ComputeDeltaOfUint16Lists(myVbsList, vbsIntersect, true)
			// Whatever are not intersected are OK
			(*resp.payload)[bucketName].RegisterNotMyVBs(removed)
		} else {
			// Everything is not my VBs
			(*resp.payload)[bucketName].RegisterNotMyVBs(vbsList)
		}
		unsubsFunc()
		latestInfo.Recycle()
	}
}

func (h *VBMasterCheckHandler) handler() {
	go func() {
		for {
			select {
			case <-h.finCh:
				return
			case req := <-h.receiveReqCh:
				vbMasterReq, isReq := req.(*VBMasterCheckReq)
				if isReq {
					h.handleRequest(vbMasterReq)
				} else {
					h.logger.Errorf("VBMasterCheckHandler (Req) received invalid format: %v", reflect.TypeOf(req))
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-h.finCh:
				return
			case resp := <-h.receiveRespCh:
				vbMasterResp, isResp := resp.(*VBMasterCheckResp)
				if isResp {
					h.handleResponse(vbMasterResp)
				} else {
					h.logger.Errorf("VBMasterCheckHandler (resp) received invalid format: %v", reflect.TypeOf(resp))
				}
			}
		}
	}()

}

func (v *VBMasterCheckHandler) handleResponse(resp *VBMasterCheckResp) {
	req, retCh, found := v.GetReqAndClearOpaque(resp.GetOpaque())
	if !found {
		v.logger.Errorf("VBMasterCheckHandler Unable to find opaque %v", resp.GetOpaque())
		// Unable to find opaque means the original request has timed out
		return
	}
	if resp.GetErrorString() != "" {
		v.logger.Warnf("%v", resp.GetErrorString())
	}
	if retCh != nil {
		retPair := ReqRespPair{
			ReqPtr:  req,
			RespPtr: resp,
		}
		go v.sendBackSynchronously(retCh, retPair)
	}
}

func (v *VBMasterCheckHandler) populatePipelineCkpts(replSpecId string, waitGrp *sync.WaitGroup, err *error, result *map[uint16]*metadata.CheckpointsDoc, finCh chan bool) {
	defer waitGrp.Done()

	var ckptDocs map[uint16]*metadata.CheckpointsDoc
	var opErr error
	doneCh := make(chan bool)

	go func() {
		ckptDocs, opErr = v.ckptSvc.CheckpointsDocs(replSpecId, true)
		close(doneCh)
	}()

	select {
	case <-doneCh:
		// Regular path
		break
	case <-finCh:
		*err = base.ErrorOpInterrupted
		return
	}

	v.logger.Infof("Handler for %v retrieving CheckpointsDocs request found %v docs", replSpecId, len(ckptDocs))
	if opErr != nil {
		if opErr != service_def.MetadataNotFoundErr {
			v.logger.Errorf("Error getting ckpt docs for %v - %v", replSpecId, opErr)
			*err = opErr
		}
		return
	}

	*result = ckptDocs
	return
}

// We depend on CollectionsManifestSvc to always keep only the minimal manifests needed
// So whatever it returns, most likely it is needed, and no need to go filter through them
func (v *VBMasterCheckHandler) fetchAllManifests(replId string, srcManifests *metadata.ManifestsCache, tgtManifests *metadata.ManifestsCache, errPtr *error, waitGrp *sync.WaitGroup, finCh chan bool) {
	defer waitGrp.Done()
	var src map[uint64]*metadata.CollectionsManifest
	var tgt map[uint64]*metadata.CollectionsManifest
	var err error

	doneCh := make(chan bool)
	go func() {
		nameOnlySpec := &metadata.ReplicationSpecification{}
		nameOnlySpec.Id = replId
		src, tgt, err = v.colManifestSvc.GetAllCachedManifests(nameOnlySpec)
		close(doneCh)
	}()

	select {
	case <-doneCh:
		break
	case <-finCh:
		*errPtr = base.ErrorOpInterrupted
		return
	}

	if err != nil {
		*errPtr = err
		return
	}
	*srcManifests = src
	*tgtManifests = tgt
}

func (v *VBMasterCheckHandler) fetchBrokenMappingDoc(replId string, mappingDoc *metadata.CollectionNsMappingsDoc, errPtr *error, waitGrp *sync.WaitGroup, finCh chan bool) {
	defer waitGrp.Done()

	var err error
	var loadedDoc *metadata.CollectionNsMappingsDoc
	doneCh := make(chan bool)

	go func() {
		_, loadedDoc, _, _, err = v.ckptSvc.LoadBrokenMappings(replId)
		close(doneCh)
	}()

	select {
	case <-doneCh:
		break
	case <-finCh:
		*errPtr = base.ErrorOpInterrupted
		return
	}

	if err != nil {
		if err != service_def.MetadataNotFoundErr {
			*errPtr = err
		}
		return
	}
	if loadedDoc == nil {
		*errPtr = fmt.Errorf("Nil doc when loading brokenMapping")
		return
	}
	*mappingDoc = *loadedDoc
}

func (v *VBMasterCheckHandler) fetchBackfillTasks(replId string, backfillTasks *metadata.VBTasksMapType, backfillErr *error, backfillTaskSrcManifestId *uint64, waitGrp *sync.WaitGroup, finCh chan bool) {
	defer waitGrp.Done()

	select {
	case <-finCh:
		*backfillErr = base.ErrorOpInterrupted
		return
	default:
		break
	}

	backfillSpec, err := v.backfillReplSvc.BackfillReplSpec(replId)
	if err != nil {
		if err != base.ReplNotFoundErr {
			*backfillErr = err
		}
		return
	}

	if backfillSpec.VBTasksMap != nil {
		clonedTask := backfillSpec.VBTasksMap.Clone()
		*backfillTasks = *clonedTask
		*backfillTaskSrcManifestId = backfillSpec.SourceManifestUid
	}
}
