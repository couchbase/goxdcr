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

type DiscoveryResponse struct {
	ResponseCommon
	DiscoveryErrString string
}

func (d *DiscoveryResponse) GetOpcode() OpCode {
	return d.RespType
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
	//case ReqVBMasterChk:
	//resp := &VBMasterCheckResp{}
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

// TODO
type VBMasterChkRespType interface{}

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
	return v.RequestCommon.SameAs(&other.RequestCommon)
}

func (v *VBMasterCheckReq) GenerateResponse() interface{} {
	common := NewResponseCommon(v.ReqType, v.RemoteLifeCycleId, v.LocalLifeCycleId, v.Opaque, v.TargetAddr)
	common.RespType = v.ReqType
	resp := &VBMasterCheckResp{
		ResponseCommon: common,
	}
	return resp
}

type VBMasterCheckResp struct {
	ResponseCommon
}
