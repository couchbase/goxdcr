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
	utilities "github.com/couchbase/goxdcr/utils"
	"github.com/golang/snappy"
	"io/ioutil"
	"net/http"
	"reflect"
)

const (
	ReqDiscovery   OpCode = iota
	ReqVBMasterChk OpCode = iota
	ReqMaxInvalid  OpCode = iota
)

func (o OpCode) String() string {
	switch o {
	case ReqDiscovery:
		return "Discovery"
	case ReqVBMasterChk:
		return "VBMasterCheck"
	default:
		return "?? (InvalidRequest)"
	}
}

const OpcodeMin = ReqDiscovery
const OpcodeMax = ReqMaxInvalid

const ReqMagic = 0x001
const RespMagic = 0x100

const RequestType ReqRespType = iota
const ResponseType ReqRespType = iota

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
	DiscoveryErrString string
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
var reqMagicCheckFilter, _ = filter.NewFilter("magicCheckReq", fmt.Sprintf("Magic=%d", ReqMagic), filterUtils)
var respMagicCheckFilter, _ = filter.NewFilter("magicCheckResp", fmt.Sprintf("Magic=%d", RespMagic), filterUtils)

func GenerateP2PReqOrResp(httpReq *http.Request, utils utilities.UtilsIface) (ReqRespCommon, error) {
	body, err := ioutil.ReadAll(httpReq.Body)
	if err != nil {
		return nil, err
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
			return nil, err
		}
		return generateRequest(utils, reqCommon, err, body)
	} else {
		err = json.Unmarshal(body, &respCommon)
		if err != nil {
			return nil, err
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
			return nil, err
		}
		return respDisc, nil
	case ReqVBMasterChk:
		resp := &VBMasterCheckResp{}
		err = resp.DeSerialize(body)
		if len(resp.ResponsePayloadCompressed) > 0 && len(*resp.responsePayload) == 0 {
			panic("Should not be possible")
		}
		return resp, nil
	default:
		return nil, fmt.Errorf("Unknown response %v", respCommon.RespType)
	}
}

func generateRequest(utils utilities.UtilsIface, reqCommon RequestCommon, err error, body []byte) (ReqRespCommon, error) {
	cbFunc := func(resp Response) (HandlerResult, error) {
		payload, err := resp.Serialize()
		if err != nil {
			return &HandlerResultImpl{}, err
		}
		var out interface{}
		err, statusCode := utils.QueryRestApiWithAuth(reqCommon.GetSender(), getDevOnlyPath(), false, "", "", base.HttpAuthMechPlain, nil, false, nil, nil, base.MethodPost, base.JsonContentType,
			payload, 0, &out, nil, false, nil)
		result := &HandlerResultImpl{
			Err:            err,
			HttpStatusCode: statusCode,
		}
		return result, err
	}
	reqCommon.responseCb = cbFunc

	switch reqCommon.ReqType {
	case ReqDiscovery:
		reqDisc := &DiscoveryRequest{}
		err = reqDisc.DeSerialize(body)
		reqDisc.RequestCommon = reqCommon
		return reqDisc, err
	case ReqVBMasterChk:
		reqVBChk := &VBMasterCheckReq{}
		err = reqVBChk.DeSerialize(body)
		reqVBChk.RequestCommon = reqCommon
		return reqVBChk, err
	default:
		return nil, fmt.Errorf("Unknown request %v", reqCommon.ReqType)
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
		ResponseCommon:    responseCommon,
		ReplicationSpecId: v.ReplicationId,
		PipelineType:      v.PipelineType,
		SourceBucketName:  v.SourceBucketName,
	}
	return resp
}

type VBMasterCheckResp struct {
	ResponseCommon

	responsePayload           *BucketVBMPayloadType
	ResponsePayloadCompressed []byte
	ErrorMsg                  string

	ReplicationSpecId string
	SourceBucketName  string
	PipelineType      common.PipelineType
}

// Unit test
func NewVBMasterCheckRespGivenPayload(payload BucketVBMPayloadType) *VBMasterCheckResp {
	return &VBMasterCheckResp{
		ResponseCommon:            ResponseCommon{},
		responsePayload:           &payload,
		ResponsePayloadCompressed: nil,
	}
}

func (v *VBMasterCheckResp) GetReponse() *BucketVBMPayloadType {
	return v.responsePayload
}

func (v *VBMasterCheckResp) Serialize() ([]byte, error) {
	responsePayloadMarshalled, err := json.Marshal(v.responsePayload)
	if err != nil {
		return nil, err
	}

	v.ResponsePayloadCompressed = snappy.Encode(nil, responsePayloadMarshalled)
	return json.Marshal(v)
}

func (v *VBMasterCheckResp) DeSerialize(bytes []byte) error {
	err := json.Unmarshal(bytes, v)
	if err != nil {
		return err
	}

	v.Init()
	err = v.decompressPayload()
	if err != nil {
		return err
	}

	v.InitNilPts()
	return nil
}

func (v *VBMasterCheckResp) decompressPayload() error {
	if len(v.ResponsePayloadCompressed) > 0 {
		marshalledPayload, snappyErr := snappy.Decode(nil, v.ResponsePayloadCompressed)
		if snappyErr != nil {
			return snappyErr
		}
		err := json.Unmarshal(marshalledPayload, &v.responsePayload)
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *VBMasterCheckResp) InitNilPts() {
	for bucket, payloadPerBucket := range *v.responsePayload {
		if payloadPerBucket == nil {
			(*v.responsePayload)[bucket] = NewVBMasterPayload()
		} else {
			if payloadPerBucket.NotMyVBs == nil {
				payloadPerBucket.NotMyVBs = NewVBsPayload(nil)
			}
			if payloadPerBucket.ConflictingVBs == nil {
				payloadPerBucket.ConflictingVBs = NewVBsPayload(nil)
			}
		}
	}
}

// Key is bucket name
type BucketVBMPayloadType map[string]*VBMasterPayload

type VBMasterPayload struct {
	OverallPayloadErr string // If populated, the data below are invalid

	NotMyVBs       *VBsPayload // These VBs are not owned by requested node
	ConflictingVBs *VBsPayload // Requested node believes these VBs to be owned as does sender
}

func (p *VBMasterPayload) RegisterVbsIntersect(vbsIntersect []uint16) {
	for _, vb := range vbsIntersect {
		// TODO - actually populate payload?
		(*p.ConflictingVBs)[vb] = NewPayload()
	}
}

func (p *VBMasterPayload) RegisterNotMyVBs(notMyVbs []uint16) {
	for _, vb := range notMyVbs {
		(*p.NotMyVBs)[vb] = NewPayload()
	}
}

func (p *VBMasterPayload) GetAllCheckpoints() map[uint16]*metadata.CheckpointsDoc {
	retMap := make(map[uint16]*metadata.CheckpointsDoc)

	for vb, payload := range *p.NotMyVBs {
		if payload.CheckpointsDoc != nil {
			retMap[vb] = payload.CheckpointsDoc
		}
	}

	for vb, payload := range *p.ConflictingVBs {
		if payload.CheckpointsDoc != nil {
			retMap[vb] = payload.CheckpointsDoc
		}
	}

	return retMap
}

type VBsPayload map[uint16]*Payload

func NewVBsPayload(vbsList []uint16) *VBsPayload {
	retMap := make(VBsPayload)
	for _, vb := range vbsList {
		retMap[vb] = NewPayload()
	}
	return &retMap
}

func NewVBMasterPayload() *VBMasterPayload {
	notMyVbs := make(VBsPayload)
	conflictingVBs := make(VBsPayload)
	return &VBMasterPayload{
		OverallPayloadErr: "",
		NotMyVBs:          &notMyVbs,
		ConflictingVBs:    &conflictingVBs,
	}
}

type Payload struct {
	CheckpointsDoc *metadata.CheckpointsDoc
}

func NewPayload() *Payload {
	return &Payload{}
}

func (v *VBMasterCheckResp) Init() {
	newMap := make(BucketVBMPayloadType)
	v.responsePayload = &newMap
}

func (v *VBMasterCheckResp) InitBucket(bucketName string) {
	(*v.responsePayload)[bucketName] = NewVBMasterPayload()
}

func (v *VBMasterCheckResp) LoadPipelineCkpts(ckptDocs map[uint16]*metadata.CheckpointsDoc, srcBucketName string) error {
	payload, found := (*v.responsePayload)[srcBucketName]
	if !found {
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
		}
	}

	if len(errMap) > 0 {
		return fmt.Errorf(base.FlattenErrorMap(errMap))
	}
	return nil
}
