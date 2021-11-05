// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package peerToPeer

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/base/filter"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"github.com/golang/snappy"
	"io/ioutil"
	"net/http"
	"reflect"
	"sync"
)

// Each type below will need a handler
const (
	ReqDiscovery    OpCode = iota
	ReqVBMasterChk  OpCode = iota
	ReqPeriodicPush OpCode = iota
	ReqMaxInvalid   OpCode = iota
)

func (o OpCode) String() string {
	switch o {
	case ReqDiscovery:
		return "Discovery"
	case ReqVBMasterChk:
		return "VBMasterCheck"
	case ReqPeriodicPush:
		return "PeriodicPush"
	default:
		return "?? (InvalidRequest)"
	}
}

func (o OpCode) IsInterruptable() bool {
	switch o {
	case ReqDiscovery:
		return false
	case ReqVBMasterChk:
		return true
	case ReqPeriodicPush:
		return false
	default:
		return false
	}
}

const OpcodeMin = ReqDiscovery
const OpcodeMax = ReqMaxInvalid

const ReqMagic = 0x001
const RespMagic = 0x100

const RequestType ReqRespType = iota
const ResponseType ReqRespType = iota

const VBUnableToLoad = "VB not able to stored into response"

var ErrorCompleteVBOverlap = fmt.Errorf("VBs ownerships completely overlap")

const MergeBackfillKey = "mergeBackfillInfoFromPeers"
const PeriodicPushSenderKey = "periodicPushSender"

type RequestCommon struct {
	Magic             int
	ReqType           OpCode
	Sender            string
	TargetAddr        string
	Opaque            uint32
	LocalLifeCycleId  string
	RemoteLifeCycleId string

	responseCb func(resp Response) (HandlerResult, error)
}

func NewRequestCommon(sender, target, localLifecycle, remoteLifecycle string, opaque uint32) RequestCommon {
	return RequestCommon{
		Magic:             ReqMagic,
		Sender:            sender,
		TargetAddr:        target,
		Opaque:            opaque,
		LocalLifeCycleId:  localLifecycle,
		RemoteLifeCycleId: remoteLifecycle,
	}
}

func (p *RequestCommon) GetType() ReqRespType {
	return RequestType
}

func (p *RequestCommon) CallBack(resp Response) (HandlerResult, error) {
	return p.responseCb(resp)
}

func (p *RequestCommon) GetSender() string {
	return p.Sender
}

func (p *RequestCommon) GetTarget() string {
	return p.TargetAddr
}

func (p *RequestCommon) GetOpaque() uint32 {
	return p.Opaque
}

func getWrongTypeErr(expectedStr string, raw interface{}) error {
	return fmt.Errorf("Wrong type: expecting %v, but got %v", expectedStr, reflect.TypeOf(raw))
}

func (p *RequestCommon) SameAs(otherRaw interface{}) (bool, error) {
	other, ok := otherRaw.(*RequestCommon)
	if !ok {
		return false, getWrongTypeErr("*RequestCommon", otherRaw)
	}
	return p.ReqType == other.ReqType &&
		p.LocalLifeCycleId == other.LocalLifeCycleId &&
		p.RemoteLifeCycleId == other.RemoteLifeCycleId &&
		p.Sender == other.Sender &&
		p.TargetAddr == other.TargetAddr &&
		p.Opaque == other.Opaque, nil
}

func (p *RequestCommon) GetOpcode() OpCode {
	return p.ReqType
}

func (p *RequestCommon) ComposeResponseCommon() *ResponseCommon {
	respCommon := &ResponseCommon{
		RespType: p.ReqType,
		Opaque:   p.Opaque,
		// Response should only be composed if LifeCycleId match
		// And the view is now reversed
		LocalLifeCycleId:  p.RemoteLifeCycleId,
		RemoteLifeCycleId: p.LocalLifeCycleId,
	}
	return respCommon
}

type DiscoveryRequest struct {
	RequestCommon
}

func (d *DiscoveryRequest) Serialize() ([]byte, error) {
	return json.Marshal(d)
}

func (d *DiscoveryRequest) DeSerialize(stream []byte) error {
	return json.Unmarshal(stream, d)
}

func (d *DiscoveryRequest) SameAs(otherRaw interface{}) (bool, error) {
	other, ok := otherRaw.(*DiscoveryRequest)
	if !ok {
		return false, getWrongTypeErr("*DiscoveryRequest", otherRaw)
	}
	return d.RequestCommon.SameAs(&other.RequestCommon)
}

func (d *DiscoveryRequest) GenerateResponse() interface{} {
	common := NewResponseCommon(d.ReqType, d.RemoteLifeCycleId, d.LocalLifeCycleId, d.Opaque, d.TargetAddr)
	common.RespType = d.ReqType
	resp := &DiscoveryResponse{
		ResponseCommon: common,
	}
	return resp
}

type ResponseCommon struct {
	Magic             int
	Sender            string
	RespType          OpCode
	Opaque            uint32
	LocalLifeCycleId  string
	RemoteLifeCycleId string
	ErrorString       string
}

func NewResponseCommon(opcode OpCode, senderLifeCycleId string, receiverLifeCycleId string, opaque uint32, sender string) ResponseCommon {
	return ResponseCommon{
		Magic:             RespMagic,
		Sender:            sender,
		RespType:          opcode,
		Opaque:            opaque,
		LocalLifeCycleId:  senderLifeCycleId,
		RemoteLifeCycleId: receiverLifeCycleId}
}

func (r *ResponseCommon) GetType() ReqRespType {
	return ResponseType
}

func (r *ResponseCommon) GetSender() string {
	return r.Sender
}

func (r *ResponseCommon) GetOpaque() uint32 {
	return r.Opaque
}

func (r *ResponseCommon) GetOpcode() OpCode {
	return r.RespType
}

type DiscoveryResponse struct {
	ResponseCommon
}

func (d *DiscoveryResponse) Serialize() ([]byte, error) {
	return json.Marshal(d)
}

func (d *DiscoveryResponse) DeSerialize(stream []byte) error {
	return json.Unmarshal(stream, d)
}

type HandlerResultImpl struct {
	Err            error
	HttpStatusCode int
}

func (r *HandlerResultImpl) GetError() error {
	return r.Err
}

func (r *HandlerResultImpl) GetHttpStatusCode() int {
	return r.HttpStatusCode
}

var filterUtils = utilities.NewUtilities()
var reqMagicCheckFilter, _ = filter.NewFilter("magicCheckReq", fmt.Sprintf("Magic=%d", ReqMagic), filterUtils, false)
var respMagicCheckFilter, _ = filter.NewFilter("magicCheckResp", fmt.Sprintf("Magic=%d", RespMagic), filterUtils, false)

func GenerateP2PReqOrResp(httpReq *http.Request, utils utilities.UtilsIface, securitySvc service_def.SecuritySvc) (ReqRespCommon, error) {
	body, err := ioutil.ReadAll(httpReq.Body)
	if err != nil {
		return nil, fmt.Errorf("reading httpReq.Body resulted in err: %v", err)
	}

	isRequestType, _, reqFilterErr := reqMagicCheckFilter.FilterByteSlice(body)
	_, _, respFilterErr := respMagicCheckFilter.FilterByteSlice(body)
	if reqFilterErr != nil && respFilterErr != nil {
		return nil, fmt.Errorf("Unable to determine magic... body %v\n", hex.Dump(body))
	}

	var reqCommon RequestCommon
	var respCommon ResponseCommon
	if isRequestType {
		err = json.Unmarshal(body, &reqCommon)
		if err != nil {
			return nil, fmt.Errorf("unmarshalling request with reqCommon %v had err %v", reqCommon, err)
		}
		return generateRequest(utils, reqCommon, body, securitySvc)
	} else {
		err = json.Unmarshal(body, &respCommon)
		if err != nil {
			return nil, fmt.Errorf("unmarshalling response with respCommon %v had err %v", respCommon, err)
		}
		return generateResp(respCommon, err, body)
	}
}

func generateResp(respCommon ResponseCommon, err error, body []byte) (ReqRespCommon, error) {
	switch respCommon.RespType {
	case ReqDiscovery:
		respDisc := &DiscoveryResponse{}
		err = respDisc.DeSerialize(body)
		if err != nil {
			return nil, fmt.Errorf("respDiscovery deSerialize err: %v", err)
		}
		return respDisc, nil
	case ReqVBMasterChk:
		resp := &VBMasterCheckResp{}
		err = resp.DeSerialize(body)
		if err != nil {
			return nil, fmt.Errorf("respVBMastChk deSerialize err: %v", err)
		}
		if len(resp.PayloadCompressed) > 0 && len(*resp.payload) == 0 {
			return nil, fmt.Errorf("respVBMasterChk from %v for %v - %v has payloadCompressed but no payload after deserialization",
				resp.Sender, resp.ReplicationSpecId, resp.InternalSpecId)
		}
		return resp, nil
	case ReqPeriodicPush:
		resp := &PeerVBPeriodicPushResp{}
		err = resp.DeSerialize(body)
		if err != nil {
			return nil, fmt.Errorf("respPeriodicPush deSerialize err: %v", err)
		}
		return resp, nil
	default:
		return nil, fmt.Errorf("Unknown response %v", respCommon.RespType)
	}
}

func generateRequest(utils utilities.UtilsIface, reqCommon RequestCommon, body []byte, securitySvc service_def.SecuritySvc) (ReqRespCommon, error) {
	cbFunc := func(resp Response) (HandlerResult, error) {
		payload, err := resp.Serialize()
		if err != nil {
			return &HandlerResultImpl{}, fmt.Errorf("generating response %v err: %v", resp.GetType(), err)
		}
		var out interface{}
		var certificates []byte
		authMech := base.HttpAuthMechPlain
		if securitySvc.IsClusterEncryptionLevelStrict() {
			authMech = base.HttpAuthMechHttps
			certificates = securitySvc.GetCACertificates()
			if len(certificates) == 0 {
				return &HandlerResultImpl{
					Err:            base.ErrorNilCertificate,
					HttpStatusCode: http.StatusInternalServerError,
				}, base.ErrorNilCertificateStrictMode
			}
		}
		err, statusCode := utils.QueryRestApiWithAuth(reqCommon.GetSender(), base.XDCRPeerToPeerPath, false,
			"", "", authMech, certificates, true, nil, nil,
			base.MethodPost, base.JsonContentType, payload, base.P2PCommTimeout, &out, nil, false, nil)
		result := &HandlerResultImpl{
			Err:            err,
			HttpStatusCode: statusCode,
		}
		if err != nil {
			err = fmt.Errorf("response %v callback err: %v", resp.GetType(), err)
		}
		return result, err
	}
	reqCommon.responseCb = cbFunc

	var err error
	switch reqCommon.ReqType {
	case ReqDiscovery:
		reqDisc := &DiscoveryRequest{}
		err = reqDisc.DeSerialize(body)
		if err != nil {
			err = fmt.Errorf("reqDiscovery deSerialize err: %v", err)
		}
		reqDisc.RequestCommon = reqCommon
		return reqDisc, err
	case ReqVBMasterChk:
		reqVBChk := &VBMasterCheckReq{}
		err = reqVBChk.DeSerialize(body)
		if err != nil {
			err = fmt.Errorf("reqVBMasterChk deSerialize err: %v", err)
		}
		reqVBChk.RequestCommon = reqCommon
		return reqVBChk, err
	case ReqPeriodicPush:
		pushReq := &PeerVBPeriodicPushReq{}
		err = pushReq.DeSerialize(body)
		if err != nil {
			err = fmt.Errorf("reqPeriodicPush deSerialize err: %v", err)
		}
		pushReq.RequestCommon = reqCommon
		return pushReq, err
	default:
		return nil, fmt.Errorf("Unknown request %v", reqCommon.ReqType)
	}
}

func getFinChKeyHelper(reqRaw interface{}) (string, error) {
	req := reqRaw.(ReqRespCommon)
	switch req.GetOpcode() {
	case ReqVBMasterChk:
		checkReq := reqRaw.(*VBMasterCheckReq)
		return getReplSpecFinChMapKeyInternal(checkReq.ReplicationId, checkReq.InternalSpecId), nil
	default:
		return "", fmt.Errorf("Type %v is not interruptable", req.GetOpcode())
	}
}

func NewP2PDiscoveryReq(common RequestCommon) *DiscoveryRequest {
	p2pReq := &DiscoveryRequest{RequestCommon: common}
	p2pReq.ReqType = ReqDiscovery
	return p2pReq
}

type VBMasterCheckReq struct {
	RequestCommon

	// Request peer node's response given a map of bucket names and VBs for each bucket
	bucketVBMap           BucketVBMapType // small case to not be marshalled
	BucketVBMapCompressed []byte          // Not to be used except for marshalling

	// For now, only one ckpt request for one replication
	ReplicationId    string
	InternalSpecId   string
	SourceBucketName string // already in replicationId but for ease of use
	PipelineType     common.PipelineType
}

func (v *VBMasterCheckReq) GetBucketVBMap() BucketVBMapType {
	return v.bucketVBMap
}

func (v *VBMasterCheckReq) SetBucketVBMap(vNew BucketVBMapType) {
	v.bucketVBMap = vNew
}

type BucketVBMapType map[string][]uint16

func (b *BucketVBMapType) SameAs(other BucketVBMapType) bool {
	if len(*b) != len(other) {
		return false
	}

	return BucketVBMapTypeAreSame(other, *b)
}

func BucketVBMapTypeAreSame(other BucketVBMapType, b BucketVBMapType) bool {
	for k, aList := range b {
		bList, ok := other[k]
		if !ok {
			return false
		}

		aSorted := base.SortUint16List(aList)
		bSorted := base.SortUint16List(bList)
		if !base.AreSortedUint16ListsTheSame(aSorted, bSorted) {
			return false
		}
	}
	return true
}

func NewVBMasterCheckReq(common RequestCommon) *VBMasterCheckReq {
	req := &VBMasterCheckReq{RequestCommon: common}
	req.ReqType = ReqVBMasterChk
	return req
}

func (v *VBMasterCheckReq) Serialize() ([]byte, error) {
	bucketMapMarshalled, err := json.Marshal(v.bucketVBMap)
	if err != nil {
		return nil, err
	}

	v.BucketVBMapCompressed = snappy.Encode(nil, bucketMapMarshalled)

	return json.Marshal(v)
}

func (v *VBMasterCheckReq) DeSerialize(stream []byte) error {
	err := json.Unmarshal(stream, v)
	if err != nil {
		return err
	}

	if len(v.BucketVBMapCompressed) > 0 {
		marshalledVBMasterChkReq, snappyErr := snappy.Decode(nil, v.BucketVBMapCompressed)
		if snappyErr != nil {
			return snappyErr
		}
		err = json.Unmarshal(marshalledVBMasterChkReq, &v.bucketVBMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *VBMasterCheckReq) SameAs(otherRaw interface{}) (bool, error) {
	other, ok := otherRaw.(*VBMasterCheckReq)
	if !ok {
		return false, getWrongTypeErr("*VBMasterCheckReq", otherRaw)
	}
	if !v.bucketVBMap.SameAs(other.bucketVBMap) {
		return false, fmt.Errorf("BucketVBMap are different: %v vs %v", v.bucketVBMap, other.bucketVBMap)
	}
	if v.ReplicationId != other.ReplicationId {
		return false, nil
	}
	if v.PipelineType != other.PipelineType {
		return false, nil
	}
	return v.RequestCommon.SameAs(&other.RequestCommon)
}

func (v *VBMasterCheckReq) GenerateResponse() interface{} {
	responseCommon := NewResponseCommon(v.ReqType, v.RemoteLifeCycleId, v.LocalLifeCycleId, v.Opaque, v.TargetAddr)
	responseCommon.RespType = v.ReqType
	resp := &VBMasterCheckResp{
		ResponseCommon:     responseCommon,
		ReplicationPayload: NewReplicationPayload(v.ReplicationId, v.SourceBucketName, v.PipelineType, v.InternalSpecId),
	}
	return resp
}

type ReplicationPayload struct {
	mtx               sync.RWMutex
	payload           *BucketVBMPayloadType
	PayloadCompressed []byte
	ErrorMsg          string
	ReplicationSpecId string
	SourceBucketName  string
	PipelineType      common.PipelineType
	InternalSpecId    string
}

func NewReplicationPayload(specId, srcBucketName string, pipelineType common.PipelineType, internalSpecId string) ReplicationPayload {
	payload := make(BucketVBMPayloadType)
	return ReplicationPayload{
		ReplicationSpecId: specId,
		SourceBucketName:  srcBucketName,
		PipelineType:      pipelineType,
		payload:           &payload,
		InternalSpecId:    internalSpecId,
	}
}

func (v *ReplicationPayload) GetPayloadWithReadLock() (*BucketVBMPayloadType, func()) {
	unlockFunc := func() {
		v.mtx.RUnlock()
	}
	v.mtx.RLock()
	return v.payload, unlockFunc
}

func (v *ReplicationPayload) CompressPayload() error {
	if v == nil {
		return nil
	}
	v.mtx.Lock()
	defer v.mtx.Unlock()

	if v.payload == nil {
		v.PayloadCompressed = nil
		return nil
	}

	responsePayloadMarshalled, err := json.Marshal(v.payload)
	if err != nil {
		return err
	}

	v.PayloadCompressed = snappy.Encode(nil, responsePayloadMarshalled)
	return nil
}

func (v *ReplicationPayload) DecompressPayload() error {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	if len(v.PayloadCompressed) > 0 {
		marshalledPayload, snappyErr := snappy.Decode(nil, v.PayloadCompressed)
		if snappyErr != nil {
			return snappyErr
		}
		err := json.Unmarshal(marshalledPayload, &v.payload)
		if err != nil {
			return err
		}
		err = v.payload.PostDecompressInit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *ReplicationPayload) LoadPipelineCkpts(ckptDocs map[uint16]*metadata.CheckpointsDoc, srcBucketName string) error {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	payload, payloadFound := (*v.payload)[srcBucketName]
	if !payloadFound {
		return fmt.Errorf("Bucket %v not found from response payload", srcBucketName)
	}

	errMap := make(base.ErrorMap)
	for vb, ckptDoc := range ckptDocs {
		notMyVBMap := *payload.NotMyVBs
		vbPayload, found := notMyVBMap[vb]
		if found {
			vbPayload.CheckpointsDoc = ckptDoc
			continue
		}

		// If not found above, try next data structure
		conflictingVBMap := *payload.ConflictingVBs
		vbPayload2, found2 := conflictingVBMap[vb]
		if found2 {
			vbPayload2.CheckpointsDoc = ckptDoc
			continue
		}

		pushVBMap := *payload.PushVBs
		vbPayload3, found3 := pushVBMap[vb]
		if found3 {
			vbPayload3.CheckpointsDoc = ckptDoc
			continue
		}

		// Not found...
	}

	if len(errMap) > 0 {
		return fmt.Errorf(base.FlattenErrorMap(errMap))
	}
	return nil
}

func (v *ReplicationPayload) LoadManifests(srcManifests metadata.ManifestsCache, tgtManifests metadata.ManifestsCache, srcBucketName string) error {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	payload, found := (*v.payload)[srcBucketName]
	if !found {
		return fmt.Errorf("Bucket %v not found from response payload", srcBucketName)
	}

	payload.SrcManifests = &srcManifests
	payload.TgtManifests = &tgtManifests
	return nil
}

func (v *ReplicationPayload) LoadBrokenMappingDoc(brokenMappingDoc metadata.CollectionNsMappingsDoc, srcBucketName string) error {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	payload, found := (*v.payload)[srcBucketName]
	if !found {
		return fmt.Errorf("Bucket %v not found from response payload", srcBucketName)
	}

	payload.BrokenMappingDoc = &brokenMappingDoc
	return nil
}

func (v *ReplicationPayload) LoadBackfillTasks(backfillTasks *metadata.VBTasksMapType, srcBucketName string) error {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	if !backfillTasks.ContainsAtLeastOneTask() {
		// Nothing to do
		return nil
	}

	payload, found := (*v.payload)[srcBucketName]
	if !found {
		return fmt.Errorf("Bucket %v not found from response payload", srcBucketName)
	}

	backfillMapping := backfillTasks.GetAllCollectionNamespaceMappings()
	if len(backfillMapping) == 0 {
		return fmt.Errorf("backfill replication from source bucket %v contains at least one task but the mapping is empty", srcBucketName)
	}

	backfillMappingDoc := &metadata.CollectionNsMappingsDoc{}
	err := backfillMappingDoc.LoadShaMap(backfillMapping)
	if err != nil {
		return err
	}
	payload.BackfillMappingDoc = backfillMappingDoc

	// LoadPipelineCkpts has already been done so all the VBs struct would have been set up
	var tasksLoaded []uint16
	var taskEmpty []uint16
	var taskNotFound []uint16

	for vb, tasks := range backfillTasks.VBTasksMap {
		if tasks == nil || tasks.Len() == 0 {
			taskEmpty = append(taskEmpty, vb)
			continue
		}

		notMyVBMap := *payload.NotMyVBs
		vbPayload, found := notMyVBMap[vb]
		if found {
			vbPayload.BackfillTsks = tasks
			tasksLoaded = append(tasksLoaded, vb)
			continue
		}

		// If not found above, try next data structure
		conflictingVBMap := *payload.ConflictingVBs
		vbPayload2, found2 := conflictingVBMap[vb]
		if found2 {
			vbPayload2.BackfillTsks = tasks
			tasksLoaded = append(tasksLoaded, vb)
			continue
		}

		pushVBMap := *payload.PushVBs
		vbPayload3, found3 := pushVBMap[vb]
		if found3 {
			vbPayload3.BackfillTsks = tasks
			tasksLoaded = append(tasksLoaded, vb)
			continue
		}

		taskNotFound = append(taskNotFound, vb)
	}
	return nil
}

func (v *ReplicationPayload) GetSubsetBasedOnVBs(vbsList []uint16) *ReplicationPayload {
	if v == nil {
		return nil
	}

	v.mtx.RLock()
	defer v.mtx.RUnlock()

	retPayload := &ReplicationPayload{
		ReplicationSpecId: v.ReplicationSpecId,
		SourceBucketName:  v.SourceBucketName,
		PipelineType:      v.PipelineType,
	}

	payloadMap := make(BucketVBMPayloadType)
	if v.payload != nil {
		for bucketName, vbMasterPayload := range *v.payload {
			subsetPayload := vbMasterPayload.GetSubsetBasedOnVBs(vbsList)
			if subsetPayload.IsEmpty() {
				continue
			}
			payloadMap[bucketName] = subsetPayload
		}
	}
	retPayload.payload = &payloadMap
	return retPayload
}

func (v *ReplicationPayload) GetPushVBs() []uint16 {
	if v == nil {
		panic("Nil")
	}
	v.mtx.RLock()
	defer v.mtx.RUnlock()

	if v.payload == nil {
		panic("Nil payload")
	}

	var retList []uint16
	for _, vbMasterPayload := range *v.payload {
		pushVBs := *(vbMasterPayload.PushVBs)
		for vb, data := range pushVBs {
			if data.BackfillTsks.Len() > 0 || data.CheckpointsDoc.Len() > 0 {
				retList = append(retList, vb)
			}
		}
	}
	return retList
}

func (v *ReplicationPayload) GetSubsetBasedOnNonIntersectingVBs(vbsList []uint16) *ReplicationPayload {
	if v == nil {
		return nil
	}

	v.mtx.RLock()
	defer v.mtx.RUnlock()

	retPayload := &ReplicationPayload{
		ReplicationSpecId: v.ReplicationSpecId,
		SourceBucketName:  v.SourceBucketName,
		PipelineType:      v.PipelineType,
	}

	payloadMap := make(BucketVBMPayloadType)
	if v.payload != nil {
		for bucketName, vbMasterPayload := range *v.payload {
			subsetPayload := vbMasterPayload.GetSubsetBasedOnNonIntersectingVBs(vbsList)
			if subsetPayload.IsEmpty() {
				continue
			}
			payloadMap[bucketName] = subsetPayload
		}
	}
	retPayload.payload = &payloadMap
	return retPayload
}

func (v *ReplicationPayload) IsEmpty() bool {
	if v == nil {
		return true
	}

	v.mtx.RLock()
	defer v.mtx.RUnlock()

	if v.payload != nil {
		for _, vbMasterPayload := range *v.payload {
			if !vbMasterPayload.IsEmpty() {
				return false
			}
		}
	}
	return true
}

func (v *ReplicationPayload) ContainsBackfillCheckpoints() bool {
	if v == nil {
		return false
	}

	v.mtx.RLock()
	defer v.mtx.RUnlock()

	if v.payload == nil {
		return false
	}

	for _, vbMasterPayload := range *v.payload {
		if vbMasterPayload.ContainsBackfillCheckpoints() {
			return true
		}
	}
	return false
}

func (v *ReplicationPayload) SameAs(other *ReplicationPayload) bool {
	if v == nil && other != nil {
		return false
	} else if v != nil && other == nil {
		return false
	} else if v == nil && other == nil {
		return true
	}

	v.mtx.RLock()
	other.mtx.RLock()
	defer v.mtx.RUnlock()
	defer other.mtx.RUnlock()

	return v.payload.SameAs(other.payload) && v.ErrorMsg == other.ErrorMsg &&
		v.ReplicationSpecId == other.ReplicationSpecId && v.SourceBucketName == other.SourceBucketName &&
		v.PipelineType == other.PipelineType && v.InternalSpecId == other.InternalSpecId
}

type PeersVBMasterCheckRespMap map[string]*VBMasterCheckResp

type VBMasterCheckResp struct {
	ResponseCommon
	ReplicationPayload
}

func (v *VBMasterCheckResp) GetReponse() (*BucketVBMPayloadType, func()) {
	return v.GetPayloadWithReadLock()
}

// Unit test
func NewVBMasterCheckRespGivenPayload(payload BucketVBMPayloadType) *VBMasterCheckResp {
	return &VBMasterCheckResp{
		ResponseCommon:     ResponseCommon{},
		ReplicationPayload: ReplicationPayload{payload: &payload},
	}
}

// Unit test
func (v *VBMasterCheckResp) SetReponse(payload *BucketVBMPayloadType) {
	v.payload = payload
}

func (v *VBMasterCheckResp) Serialize() ([]byte, error) {
	err := v.ReplicationPayload.CompressPayload()
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func (v *VBMasterCheckResp) DeSerialize(bytes []byte) error {
	err := json.Unmarshal(bytes, v)
	if err != nil {
		return err
	}

	v.Init()
	err = v.DecompressPayload()
	if err != nil {
		return err
	}

	v.InitNilPts()
	return nil
}

func (v *VBMasterCheckResp) InitNilPts() {
	for bucket, payloadPerBucket := range *v.payload {
		if payloadPerBucket == nil {
			(*v.payload)[bucket] = NewVBMasterPayload()
		} else {
			if payloadPerBucket.NotMyVBs == nil {
				payloadPerBucket.NotMyVBs = NewVBsPayload(nil)
			}
			if payloadPerBucket.ConflictingVBs == nil {
				payloadPerBucket.ConflictingVBs = NewVBsPayload(nil)
			}
			if payloadPerBucket.PushVBs == nil {
				payloadPerBucket.PushVBs = NewVBsPayload(nil)
			}
		}
	}
}

// Key is bucket name
type BucketVBMPayloadType map[string]*VBMasterPayload

func (t *BucketVBMPayloadType) SameAs(other *BucketVBMPayloadType) bool {
	if t == nil && other == nil {
		return true
	} else if t != nil && other == nil {
		return false
	} else if t == nil && other != nil {
		return false
	}

	if len(*t) != len(*other) {
		return false
	}

	for k, v := range *t {
		otherV, exists := (*other)[k]
		if !exists {
			return false
		}
		if !v.SameAs(otherV) {
			return false
		}
	}
	return true
}

func (t *BucketVBMPayloadType) PostDecompressInit() error {
	if t == nil {
		return nil
	}

	for _, payloadPtr := range *t {
		if payloadPtr == nil {
			continue
		}
		err := payloadPtr.PostDecompressInit()
		if err != nil {
			return err
		}
	}
	return nil
}

type VBMasterPayload struct {
	mtx sync.RWMutex

	OverallPayloadErr string // If populated, the data below are invalid

	NotMyVBs       *VBsPayload // These VBs are not owned by requested node
	ConflictingVBs *VBsPayload // Requested node believes these VBs to be owned as does sender
	PushVBs        *VBsPayload // Use for push-model

	SrcManifests *metadata.ManifestsCache
	TgtManifests *metadata.ManifestsCache

	BrokenMappingDoc   *metadata.CollectionNsMappingsDoc // Shallow copied
	BackfillMappingDoc *metadata.CollectionNsMappingsDoc // Shallow copied
}

func (p *VBMasterPayload) GetSubsetBasedOnVBs(vbsList []uint16) *VBMasterPayload {
	if p == nil {
		return nil
	}
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	retPayload := &VBMasterPayload{
		NotMyVBs:           p.NotMyVBs.GetSubsetBasedOnVBs(vbsList),
		ConflictingVBs:     p.ConflictingVBs.GetSubsetBasedOnVBs(vbsList),
		PushVBs:            p.PushVBs.GetSubsetBasedOnVBs(vbsList),
		SrcManifests:       p.SrcManifests.Clone(),
		TgtManifests:       p.TgtManifests.Clone(),
		BrokenMappingDoc:   p.BrokenMappingDoc,
		BackfillMappingDoc: p.BackfillMappingDoc,
	}
	return retPayload
}

func (p *VBMasterPayload) GetSubsetBasedOnNonIntersectingVBs(vbsList []uint16) *VBMasterPayload {
	if p == nil {
		return nil
	}
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	retPayload := &VBMasterPayload{
		NotMyVBs:           p.NotMyVBs.GetSubsetBasedOnNonIntersectingVBs(vbsList),
		ConflictingVBs:     p.ConflictingVBs.GetSubsetBasedOnNonIntersectingVBs(vbsList),
		PushVBs:            p.PushVBs.GetSubsetBasedOnNonIntersectingVBs(vbsList),
		SrcManifests:       p.SrcManifests.Clone(),
		TgtManifests:       p.TgtManifests.Clone(),
		BrokenMappingDoc:   p.BrokenMappingDoc,
		BackfillMappingDoc: p.BackfillMappingDoc,
	}
	return retPayload
}

func (p *VBMasterPayload) IsEmpty() bool {
	if p == nil {
		return true
	}
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	return p.PushVBs.IsEmpty() && p.NotMyVBs.IsEmpty() && p.ConflictingVBs.IsEmpty()
}

func (p *VBMasterPayload) RegisterVbsIntersect(vbsIntersect []uint16) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	for _, vb := range vbsIntersect {
		(*p.ConflictingVBs)[vb] = NewPayload()
	}
}

func (p *VBMasterPayload) RegisterNotMyVBs(notMyVbs []uint16) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	for _, vb := range notMyVbs {
		(*p.NotMyVBs)[vb] = NewPayload()
	}
}

func (p *VBMasterPayload) RegisterPushVBs(pushVBs []uint16) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	for _, vb := range pushVBs {
		(*p.PushVBs)[vb] = NewPayload()
	}
}

func (p *VBMasterPayload) GetAllCheckpoints() map[uint16]*metadata.CheckpointsDoc {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	retMap := make(map[uint16]*metadata.CheckpointsDoc)

	if p.NotMyVBs != nil {
		for vb, payload := range *p.NotMyVBs {
			if payload.CheckpointsDoc != nil {
				retMap[vb] = payload.CheckpointsDoc
			}
		}
	}

	if p.ConflictingVBs != nil {
		for vb, payload := range *p.ConflictingVBs {
			if payload.CheckpointsDoc != nil {
				retMap[vb] = payload.CheckpointsDoc
			}
		}
	}

	if p.PushVBs != nil {
		for vb, payload := range *p.PushVBs {
			if payload.CheckpointsDoc != nil {
				retMap[vb] = payload.CheckpointsDoc
			}
		}
	}
	return retMap
}

func (p *VBMasterPayload) GetAllManifests() (srcManifests, tgtManifests *metadata.ManifestsCache) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	return p.SrcManifests.Clone(), p.TgtManifests.Clone()
}

// Shallow copy read only
func (p *VBMasterPayload) GetBrokenMappingDoc() *metadata.CollectionNsMappingsDoc {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	return p.BrokenMappingDoc
}

func (p *VBMasterPayload) GetBackfillMappingDoc() *metadata.CollectionNsMappingsDoc {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	return p.BackfillMappingDoc
}

func (p *VBMasterPayload) GetBackfillVBTasks() *metadata.VBTasksMapType {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	taskMap := metadata.NewVBTasksMap()

	if p.NotMyVBs != nil {
		for vb, payload := range *p.NotMyVBs {
			if payload.BackfillTsks != nil {
				taskMap.VBTasksMap[vb] = payload.BackfillTsks
				taskMap.VBTasksMap[vb].PostUnmarshalInit()
			}
		}
	}

	if p.ConflictingVBs != nil {
		for vb, payload := range *p.ConflictingVBs {
			if payload.BackfillTsks != nil {
				taskMap.VBTasksMap[vb] = payload.BackfillTsks
				taskMap.VBTasksMap[vb].PostUnmarshalInit()
			}
		}
	}

	if p.PushVBs != nil {
		for vb, payload := range *p.PushVBs {
			if payload.BackfillTsks != nil {
				taskMap.VBTasksMap[vb] = payload.BackfillTsks
				taskMap.VBTasksMap[vb].PostUnmarshalInit()
			}
		}
	}
	return taskMap
}

func (p *VBMasterPayload) SameAs(other *VBMasterPayload) bool {
	if p == nil && other == nil {
		return true
	} else if p == nil && other != nil {
		return false
	} else if p != nil && other == nil {
		return false
	}
	p.mtx.RLock()
	other.mtx.RLock()
	defer p.mtx.RUnlock()
	defer other.mtx.RUnlock()

	if p.OverallPayloadErr != "" && other.OverallPayloadErr != "" {
		return p.OverallPayloadErr == other.OverallPayloadErr
	} else {
		return p.NotMyVBs.SameAs(other.NotMyVBs) && p.ConflictingVBs.SameAs(other.ConflictingVBs) &&
			p.PushVBs.SameAs(other.PushVBs) && p.SrcManifests.SameAs(other.SrcManifests) &&
			p.TgtManifests.SameAs(other.TgtManifests) && p.BrokenMappingDoc.SameAs(other.BrokenMappingDoc) &&
			p.BackfillMappingDoc.SameAs(other.BackfillMappingDoc)
	}
}

func (p *VBMasterPayload) PostDecompressInit() error {
	if p == nil {
		return nil
	}

	var err error
	if p.NotMyVBs != nil {
		err = p.NotMyVBs.PostDecompressInit()
		if err != nil {
			return err
		}
	}
	if p.ConflictingVBs != nil {
		err = p.ConflictingVBs.PostDecompressInit()
		if err != nil {
			return err
		}
	}
	if p.PushVBs != nil {
		err = p.PushVBs.PostDecompressInit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *VBMasterPayload) ContainsBackfillCheckpoints() bool {
	if p == nil {
		return false
	}

	if p.NotMyVBs != nil && p.NotMyVBs.ContainsBackfillCheckpoints() {
		return true
	}

	if p.ConflictingVBs != nil && p.ConflictingVBs.ContainsBackfillCheckpoints() {
		return true
	}

	if p.PushVBs != nil && p.PushVBs.ContainsBackfillCheckpoints() {
		return true
	}
	return false
}

type VBsPayload map[uint16]*Payload

func NewVBsPayload(vbsList []uint16) *VBsPayload {
	retMap := make(VBsPayload)
	for _, vb := range vbsList {
		retMap[vb] = NewPayload()
	}
	return &retMap
}

func (v *VBsPayload) IsEmpty() bool {
	if v == nil {
		return true
	}
	return len(*v) == 0
}

// The data inside payload should be read only
func (v *VBsPayload) GetSubsetBasedOnVBs(vbsList []uint16) *VBsPayload {
	if v == nil {
		return nil
	}

	retMap := make(VBsPayload)
	for vbno, payload := range *v {
		if _, found := base.SearchUint16List(vbsList, vbno); found {
			retMap[vbno] = payload
		}
	}
	return &retMap
}

func (v *VBsPayload) GetSubsetBasedOnNonIntersectingVBs(vbsList []uint16) *VBsPayload {
	if v == nil {
		return nil
	}

	retMap := make(VBsPayload)
	for vbno, payload := range *v {
		if _, found := base.SearchUint16List(vbsList, vbno); !found {
			retMap[vbno] = payload
		}
	}
	return &retMap
}

func (v *VBsPayload) SameAs(other *VBsPayload) bool {
	if v == nil && other != nil {
		return false
	} else if v != nil && other == nil {
		return false
	} else if v == nil && other == nil {
		return true
	}

	if len(*v) != len(*other) {
		return false
	}

	for k, v := range *v {
		otherV, exists := (*other)[k]
		if !exists {
			return false
		}
		if !v.SameAs(otherV) {
			return false
		}
	}
	return true
}

func (v *VBsPayload) PostDecompressInit() error {
	if v == nil {
		return nil
	}

	for _, payload := range *v {
		if payload == nil || payload.BackfillTsks == nil {
			continue
		}
		payload.BackfillTsks.PostUnmarshalInit()
	}
	return nil
}

func (v *VBsPayload) ContainsBackfillCheckpoints() bool {
	if v == nil {
		return false
	}

	for _, payload := range *v {
		if payload == nil {
			continue
		}
		if payload.BackfillTsks != nil && payload.BackfillTsks.Len() > 0 &&
			payload.CheckpointsDoc != nil && payload.CheckpointsDoc.Len() > 0 {
			return true
		}
	}
	return false
}

func NewVBMasterPayload() *VBMasterPayload {
	notMyVbs := make(VBsPayload)
	conflictingVBs := make(VBsPayload)
	pushVBs := make(VBsPayload)
	return &VBMasterPayload{
		OverallPayloadErr: "",
		NotMyVBs:          &notMyVbs,
		ConflictingVBs:    &conflictingVBs,
		PushVBs:           &pushVBs,
	}
}

// Read-only
type Payload struct {
	CheckpointsDoc *metadata.CheckpointsDoc

	// Backfill replication is decomposed and just the VBTasksMap is transferred
	BackfillTsks *metadata.BackfillTasks
}

func (t *Payload) SameAs(other *Payload) bool {
	if t == nil && other == nil {
		return true
	} else if t != nil && other == nil {
		return false
	} else if t == nil && other != nil {
		return false
	}

	return t.CheckpointsDoc.SameAs(other.CheckpointsDoc) && t.BackfillTsks.SameAs(other.BackfillTsks)
}

func NewPayload() *Payload {
	return &Payload{}
}

func (v *VBMasterCheckResp) Init() {
	newMap := make(BucketVBMPayloadType)
	v.payload = &newMap
}

func (v *VBMasterCheckResp) InitBucket(bucketName string) {
	(*v.payload)[bucketName] = NewVBMasterPayload()
}

type VBPeriodicReplicateReq struct {
	MainReplication     *ReplicationPayload
	BackfillReplication *ReplicationPayload
}

func (v *VBPeriodicReplicateReq) IsEmpty() bool {
	return v.MainReplication == nil && v.BackfillReplication == nil
}

func (v *VBPeriodicReplicateReq) PushReqIsEmpty() bool {
	return v.IsEmpty() || (len(v.MainReplication.GetPushVBs()) == 0 && len(v.BackfillReplication.GetPushVBs()) == 0)
}

// Note - need to establish RequestCommon later
func NewVBPeriodicReplicateReq(specId, srcBucketName string, vbs []uint16, internalId string) *VBPeriodicReplicateReq {
	mainReplication := NewReplicationPayload(specId, srcBucketName, common.MainPipeline, internalId)
	backfillReplication := NewReplicationPayload(specId, srcBucketName, common.BackfillPipeline, internalId)

	(*mainReplication.payload)[srcBucketName] = NewVBMasterPayload()
	(*mainReplication.payload)[srcBucketName].RegisterPushVBs(vbs)
	(*backfillReplication.payload)[srcBucketName] = NewVBMasterPayload()
	(*backfillReplication.payload)[srcBucketName].RegisterPushVBs(vbs)

	return &VBPeriodicReplicateReq{
		MainReplication:     &mainReplication,
		BackfillReplication: &backfillReplication,
	}
}

func (v *VBPeriodicReplicateReq) LoadMainReplication(ckpts map[uint16]*metadata.CheckpointsDoc, srcManifests, tgtManifests map[uint64]*metadata.CollectionsManifest) error {
	err := v.MainReplication.LoadPipelineCkpts(ckpts, v.MainReplication.SourceBucketName)
	if err != nil {
		return err
	}
	err = v.MainReplication.LoadManifests(srcManifests, tgtManifests, v.MainReplication.SourceBucketName)
	if err != nil {
		return err
	}
	return nil
}

func (v *VBPeriodicReplicateReq) LoadBackfillReplication(vbTasks *metadata.VBTasksMapType, ckpts map[uint16]*metadata.CheckpointsDoc, srcManifests, tgtManifests map[uint64]*metadata.CollectionsManifest) error {
	err := v.BackfillReplication.LoadBackfillTasks(vbTasks, v.BackfillReplication.SourceBucketName)
	if err != nil {
		return err
	}
	err = v.BackfillReplication.LoadPipelineCkpts(ckpts, v.BackfillReplication.SourceBucketName)
	if err != nil {
		return err
	}
	err = v.BackfillReplication.LoadManifests(srcManifests, tgtManifests, v.BackfillReplication.SourceBucketName)
	if err != nil {
		return err
	}
	return nil
}

func (v *VBPeriodicReplicateReq) GetSubsetBasedOnVBList(vbsList []uint16) *VBPeriodicReplicateReq {
	newReq := &VBPeriodicReplicateReq{}
	newReq.MainReplication = v.MainReplication.GetSubsetBasedOnVBs(vbsList)
	newReq.BackfillReplication = v.BackfillReplication.GetSubsetBasedOnVBs(vbsList)

	return newReq
}

func (v *VBPeriodicReplicateReq) GetId() string {
	if v != nil && v.MainReplication != nil {
		return v.MainReplication.ReplicationSpecId
	} else if v != nil && v.BackfillReplication != nil {
		return v.BackfillReplication.ReplicationSpecId
	} else {
		return "unknown ID"
	}
}

func (v *VBPeriodicReplicateReq) PreSerlialize() error {
	// Nil ptr serialize to nothing
	if v == nil {
		return nil
	}

	err := v.MainReplication.CompressPayload()
	if err != nil {
		return err
	}
	err = v.BackfillReplication.CompressPayload()
	if err != nil {
		return err
	}
	return nil
}

func (v *VBPeriodicReplicateReq) PostSerialize() error {
	if v == nil {
		return nil
	}

	if v.MainReplication != nil {
		err := v.MainReplication.DecompressPayload()
		if err != nil {
			return err
		}
	}

	if v.BackfillReplication != nil {
		err := v.BackfillReplication.DecompressPayload()
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *VBPeriodicReplicateReq) SameAs(other *VBPeriodicReplicateReq) bool {
	if v == nil && other != nil {
		return false
	} else if v != nil && other == nil {
		return false
	} else if v == nil && other == nil {
		return true
	}

	return v.MainReplication.SameAs(other.MainReplication) && v.BackfillReplication.SameAs(other.BackfillReplication)
}

type VBPeriodicReplicateReqList []*VBPeriodicReplicateReq

func (l *VBPeriodicReplicateReqList) SameAs(other *VBPeriodicReplicateReqList) bool {
	if l == nil && other != nil {
		return false
	} else if l != nil && other == nil {
		return false
	} else if l == nil && other == nil {
		return true
	}

	if len(*l) != len(*other) {
		return false
	}

	for i, req := range *l {
		if !req.SameAs((*other)[i]) {
			return false
		}
	}
	return true
}

type PeersVBPeriodicReplicateReqs map[string]*VBPeriodicReplicateReqList

type PeerVBPeriodicPushReq struct {
	RequestCommon
	PushRequests *VBPeriodicReplicateReqList
}

func NewPeerVBPeriodicPushReq(common RequestCommon) *PeerVBPeriodicPushReq {
	req := &PeerVBPeriodicPushReq{RequestCommon: common}
	req.ReqType = ReqPeriodicPush
	return req
}

func (p *PeerVBPeriodicPushReq) Serialize() ([]byte, error) {
	if p == nil || p.PushRequests == nil {
		return nil, base.ErrorNilPtr
	}

	for _, req := range *p.PushRequests {
		if req == nil {
			continue
		}
		err := req.PreSerlialize()
		if err != nil {
			return nil, err
		}
	}

	return json.Marshal(p)
}

func (p *PeerVBPeriodicPushReq) DeSerialize(stream []byte) error {
	if p == nil {
		return base.ErrorNilPtr
	}

	err := json.Unmarshal(stream, p)
	if err != nil {
		return err
	}

	if p.PushRequests != nil {
		for _, req := range *p.PushRequests {
			if req == nil {
				continue
			}
			postSerializeErr := req.PostSerialize()
			if postSerializeErr != nil {
				return postSerializeErr
			}
		}
	}

	return nil
}

func (p *PeerVBPeriodicPushReq) SameAs(otherRaw interface{}) (bool, error) {
	other, ok := otherRaw.(*PeerVBPeriodicPushReq)
	if !ok {
		return false, getWrongTypeErr("*PeerVBPeriodicPushReq", otherRaw)
	}
	if !p.PushRequests.SameAs(other.PushRequests) {
		return false, fmt.Errorf("Pushrequests are different %v vs %v", p.PushRequests, other.PushRequests)
	}
	return p.RequestCommon.SameAs(&other.RequestCommon)
}

func (p *PeerVBPeriodicPushReq) GenerateResponse() interface{} {
	responseCommon := NewResponseCommon(p.ReqType, p.RemoteLifeCycleId, p.LocalLifeCycleId, p.Opaque, p.TargetAddr)
	responseCommon.RespType = p.ReqType
	resp := &PeerVBPeriodicPushResp{
		ResponseCommon: responseCommon,
	}
	return resp
}

type PeerVBPeriodicPushResp struct {
	ResponseCommon
}

func (p *PeerVBPeriodicPushResp) Serialize() ([]byte, error) {
	return json.Marshal(p)
}

func (p *PeerVBPeriodicPushResp) DeSerialize(stream []byte) error {
	return json.Unmarshal(stream, p)
}
