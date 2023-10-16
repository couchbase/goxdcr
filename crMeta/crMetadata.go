/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package crMeta

import (
	"encoding/binary"
	"fmt"
	"time"

	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/hlv"
	"github.com/couchbase/goxdcr/log"
)

// Custom CR related constants
const (
	HLV_CVCAS_FIELD = "cvCas" // This stores the version of the HLV
	// These are the fields in the HLV
	HLV_SRC_FIELD = "src" // src and ver combined is the cv in the design
	HLV_VER_FIELD = "ver" //
	HLV_MV_FIELD  = "mv"  // the MV field in _xdcr
	HLV_PV_FIELD  = "pv"  // The PV field in _xdcr

	XATTR_CVCAS_PATH = base.XATTR_HLV + "." + HLV_CVCAS_FIELD
	XATTR_SRC_PATH   = base.XATTR_HLV + "." + HLV_SRC_FIELD
	XATTR_VER_PATH   = base.XATTR_HLV + "." + HLV_VER_FIELD
	XATTR_MV_PATH    = base.XATTR_HLV + "." + HLV_MV_FIELD
	XATTR_PV_PATH    = base.XATTR_HLV + "." + HLV_PV_FIELD
)

type CRMetadata struct {
	docMeta *base.DocumentMetadata
	hlv     *hlv.HLV
	hadPv   bool
	hadMv   bool
}

func (m *CRMetadata) GetDocumentMetadata() *base.DocumentMetadata {
	if m == nil {
		return nil
	}
	return m.docMeta
}

func (m *CRMetadata) GetHLV() *hlv.HLV {
	if m == nil {
		return nil
	}
	return m.hlv
}

func (m *CRMetadata) Merge(other *CRMetadata) (*CRMetadata, error) {
	if m == nil || m.GetHLV() == nil || other == nil || other.GetHLV() == nil {
		return nil, fmt.Errorf("Cannot merge nil HLV")
	}
	h, err := m.hlv.Merge(other.hlv)
	if err != nil {
		return nil, err
	}
	ret := &CRMetadata{
		docMeta: m.docMeta,
		hlv:     h,
		hadPv:   m.hadPv,
		hadMv:   m.hadMv,
	}
	return ret, nil
}

type Document interface {
	GetMetadata() (*CRMetadata, error)
}

type SourceDocument struct {
	source hlv.DocumentSourceId // This would be the source bucket UUID converted to the DocumentSourceId format
	req    *base.WrappedMCRequest
}

func NewSourceDocument(req *base.WrappedMCRequest, source hlv.DocumentSourceId) *SourceDocument {
	return &SourceDocument{
		source: source,
		req:    req,
	}
}

func (doc *SourceDocument) GetMetadata() (*CRMetadata, error) {
	docMeta := base.DecodeSetMetaReq(doc.req.Req)
	cas, cvCas, cvSrc, cvVer, pvMap, mvMap, err := getMetaFromMCRequest(doc.req.Req)
	if err != nil {
		return nil, err
	}
	meta := CRMetadata{
		docMeta: &docMeta,
	}
	if len(pvMap) > 0 {
		meta.hadPv = true
	}
	if len(mvMap) > 0 {
		meta.hadMv = true
	}
	meta.hlv, err = hlv.NewHLV(doc.source, cas, cvCas, cvSrc, cvVer, pvMap, mvMap)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

type TargetDocument struct {
	source       hlv.DocumentSourceId // This would be the target bucket UUID converted to the DcoumentSource format
	resp         *base.SubdocLookupResponse
	key          []byte
	xattrEnabled bool // This affects how to interpret getMeta response.
}

func NewTargetDocument(key []byte, resp *mc.MCResponse, specs []base.SubdocLookupPathSpec, source hlv.DocumentSourceId, xattrEnabled bool) *TargetDocument {
	return &TargetDocument{
		source: source,
		resp: &base.SubdocLookupResponse{
			Specs: specs,
			Resp:  resp,
		},
		key:          key,
		xattrEnabled: xattrEnabled,
	}
}

func (doc *TargetDocument) GetMetadata() (*CRMetadata, error) {
	if doc.resp.Resp.Status == mc.KEY_ENOENT {
		meta := CRMetadata{
			docMeta: nil,
			hlv:     nil,
		}
		return &meta, nil
	}
	if doc.resp.Resp.Opcode == base.GET_WITH_META {
		// This is a getMeta response from target
		docMeta, err := base.DecodeGetMetaResp(doc.key, doc.resp.Resp, doc.xattrEnabled)
		if err != nil {
			return nil, err
		}
		meta := CRMetadata{
			docMeta: &docMeta,
			hlv:     nil,
		}
		return &meta, err
	} else if doc.resp.Specs != nil && doc.resp.Resp.Opcode == mc.SUBDOC_MULTI_LOOKUP {
		docMeta, err := base.DecodeSubDocResp(doc.key, doc.resp)
		if err != nil {
			return nil, err
		}
		cas, cvCas, cvSrc, cvVer, pvMap, mvMap, err := getMetaFromMCResponse(doc.resp)
		if err != nil {
			return nil, err
		}
		meta := CRMetadata{
			docMeta: &docMeta,
		}
		if len(pvMap) > 0 {
			meta.hadPv = true
		}
		if len(mvMap) > 0 {
			meta.hadMv = true
		}
		meta.hlv, err = hlv.NewHLV(doc.source, cas, cvCas, cvSrc, cvVer, pvMap, mvMap)
		if err != nil {
			return nil, err
		}
		return &meta, nil
	}
	return nil, fmt.Errorf("GetMetadata: bad target document opcode %v, specs %v", doc.resp.Resp.Opcode, doc.resp.Specs)
}

// This is called after getting the target document based on the lookup spec. We should have both source and target XATTRs
// If target doesn't exist (target.Status == KEY_ENOENT), return SourceDominate
func DetectConflictWithHLV(req *base.WrappedMCRequest, resp *mc.MCResponse, specs []base.SubdocLookupPathSpec, sourceId, targetId hlv.DocumentSourceId, xattrEnabled bool, logger *log.CommonLogger) (base.ConflictResult, error) {
	if resp.Status == mc.KEY_ENOENT {
		return base.SendToTarget, nil
	}
	sourceDoc := NewSourceDocument(req, sourceId)
	sourceMeta, err := sourceDoc.GetMetadata()
	if err != nil {
		return base.Error, err
	}
	targetDoc := NewTargetDocument(req.Req.Key, resp, specs, targetId, xattrEnabled)
	targetMeta, err := targetDoc.GetMetadata()
	if err != nil {
		return base.Error, err
	}
	targetDocMeta := targetMeta.GetDocumentMetadata()
	if targetDocMeta == nil {
		// Target document does not exist
		return base.SendToTarget, nil
	}
	sourceDocMeta := sourceMeta.GetDocumentMetadata()
	if sourceDocMeta.Cas < targetDocMeta.Cas {
		// We can only replicate from larger CAS to smaller
		return base.Skip, nil
	}

	sourceHLV := sourceMeta.GetHLV()
	targetHLV := targetMeta.GetHLV()
	conflictResult, err := sourceHLV.DetectConflict(targetHLV)
	if err != nil {
		return base.Error, err
	}

	logger.Debugf("DetectConflictWithHLV key=%q, result=%s, sourceHLV=%s, targetHL=%s", sourceMeta.docMeta.Key, conflictResult, sourceHLV, targetHLV)
	switch conflictResult {
	case hlv.Win:
		return base.SendToTarget, nil
	case hlv.Lose:
		if sourceDocMeta.Cas > targetDocMeta.Cas {
			return base.SetBackToSource, nil
		} else {
			return base.Skip, nil
		}
	case hlv.Conflict:
		return base.Merge, nil
	case hlv.Equal:
		if sourceDocMeta.Cas > targetDocMeta.Cas {
			// When HLV says equal because they contain each other, if source Cas is larger, we want to send so the CAS will converge
			return base.SendToTarget, nil
		} else {
			return base.Skip, nil
		}
	}
	return base.Error, fmt.Errorf("Bad result from ccrMeta.DetectConlict: %v, source meta: %s, %s, target meta: %s, %s",
		conflictResult, sourceDocMeta, sourceHLV, targetDocMeta, targetHLV)
}

func ResolveConflictByCAS(req *base.WrappedMCRequest, resp *mc.MCResponse, specs []base.SubdocLookupPathSpec, sourceId, targetId hlv.DocumentSourceId, xattrEnabled bool, logger *log.CommonLogger) (base.ConflictResult, error) {
	if resp.Status == mc.KEY_ENOENT {
		return base.SendToTarget, nil
	}
	doc_meta_source := base.DecodeSetMetaReq(req.Req)
	var doc_meta_target base.DocumentMetadata
	var err error
	if resp.Opcode == base.GET_WITH_META {
		doc_meta_target, err = base.DecodeGetMetaResp(req.Req.Key, resp, xattrEnabled)
		if err != nil {
			return base.Error, err
		}
	} else if resp.Opcode == mc.SUBDOC_MULTI_LOOKUP {
		target_doc := NewTargetDocument(req.Req.Key, resp, specs, targetId, xattrEnabled)
		target_meta, err := target_doc.GetMetadata()
		if err != nil {
			return base.Error, err
		}
		doc_meta_target = *target_meta.docMeta
	}
	sourceWin := true
	if doc_meta_target.Cas > doc_meta_source.Cas {
		sourceWin = false
	} else if doc_meta_target.Cas == doc_meta_source.Cas {
		if doc_meta_target.RevSeq > doc_meta_source.RevSeq {
			sourceWin = false
		} else if doc_meta_target.RevSeq == doc_meta_source.RevSeq {
			//if the outgoing mutation is deletion and its revSeq and cas are the
			//same as the target side document, it would lose the conflict resolution
			if doc_meta_source.Deletion || (doc_meta_target.Expiry > doc_meta_source.Expiry) {
				sourceWin = false
			} else if doc_meta_target.Expiry == doc_meta_source.Expiry {
				if doc_meta_target.Flags > doc_meta_source.Flags {
					sourceWin = false
				} else if doc_meta_target.Flags == doc_meta_source.Flags {
					sourceWin = resolveConflictByXattr(doc_meta_source, doc_meta_target, true)
				}
			}
		}
	}
	if sourceWin {
		return base.SendToTarget, nil
	} else {
		return base.Skip, nil
	}
}

func ResolveConflictByRevSeq(req *base.WrappedMCRequest, resp *mc.MCResponse, specs []base.SubdocLookupPathSpec, sourceId, targetId hlv.DocumentSourceId, xattrEnabled bool, logger *log.CommonLogger) (base.ConflictResult, error) {
	if resp.Status == mc.KEY_ENOENT {
		return base.SendToTarget, nil
	}
	doc_meta_source := base.DecodeSetMetaReq(req.Req)
	var doc_meta_target base.DocumentMetadata
	var err error
	if resp.Opcode == base.GET_WITH_META {
		doc_meta_target, err = base.DecodeGetMetaResp(req.Req.Key, resp, xattrEnabled)
		if err != nil {
			return base.Error, err
		}
	} else if resp.Opcode == mc.SUBDOC_MULTI_LOOKUP {
		target_doc := NewTargetDocument(req.Req.Key, resp, specs, targetId, xattrEnabled)
		target_meta, err := target_doc.GetMetadata()
		if err != nil {
			return base.Error, err
		}
		doc_meta_target = *target_meta.docMeta
	}
	sourceWin := true
	if doc_meta_target.RevSeq > doc_meta_source.RevSeq {
		sourceWin = false
	} else if doc_meta_target.RevSeq == doc_meta_source.RevSeq {
		if doc_meta_target.Cas > doc_meta_source.Cas {
			sourceWin = false
		} else if doc_meta_target.Cas == doc_meta_source.Cas {
			//if the outgoing mutation is deletion and its revSeq and cas are the
			//same as the target side document, it would lose the conflict resolution
			if doc_meta_source.Deletion || (doc_meta_target.Expiry > doc_meta_source.Expiry) {
				sourceWin = false
			} else if doc_meta_target.Expiry == doc_meta_source.Expiry {
				if doc_meta_target.Flags > doc_meta_source.Flags {
					sourceWin = false
				} else if doc_meta_target.Flags == doc_meta_source.Flags {
					sourceWin = resolveConflictByXattr(doc_meta_source, doc_meta_target, true)
				}
			}
		}
	}
	if sourceWin {
		return base.SendToTarget, nil
	} else {
		return base.Skip, nil
	}
}

func NeedToUpdateXattr(meta *CRMetadata, pruningWindow time.Duration) (bool, error) {
	if meta == nil || meta.GetHLV() == nil {
		return false, fmt.Errorf("HLV not available")
	}
	hlv := meta.hlv
	// We need to update if CAS has changed from ver.CAS
	if hlv.Updated {
		return true, nil
	}

	// The CAS has not changed. But do we need to prune PV?

	// No pruning if its setting is 0 or there is no hlv to prune
	if pruningWindow == 0 || hlv == nil || len(hlv.GetPV()) == 0 {
		return false, nil
	}
	pv := hlv.GetPV()
	for _, cas := range pv {
		// Check for pruning window
		if base.CasDuration(cas, meta.docMeta.Cas) >= pruningWindow {
			return true, nil
		}
	}
	return false, nil
}

// This routine construct XATTR _vv:{...} based on meta. The constructed XATTRs includes updates for
// new change (meta.cas > meta.ver) and pruning in PV
func ConstructCustomCRXattrForSetMeta(meta *CRMetadata, pruningWindow time.Duration, xattrComposer *base.XattrComposer) (int, error) {
	if meta == nil {
		return 0, fmt.Errorf("Metadata cannot be nil")
	}
	err := xattrComposer.StartRawMode()
	if err != nil {
		return -1, err
	}

	err = xattrComposer.RawWriteKey([]byte(base.XATTR_HLV))
	if err != nil {
		return -1, err
	}

	body, pos, err := xattrComposer.RawHijackValue()
	if err != nil {
		return -1, err
	}

	pruneFunc := base.GetHLVPruneFunction(meta.GetDocumentMetadata().Cas, pruningWindow)
	*pos = formatCv(meta, body, *pos)
	// Format MV
	mv := meta.GetHLV().GetMV()
	if len(mv) > 0 {
		// This is not the first since we have cv before this
		body, *pos = base.WriteJsonRawMsg(body, []byte(HLV_MV_FIELD), *pos, base.WriteJsonKey, len([]byte(HLV_MV_FIELD)), false)
		*pos = VersionMapToBytes(mv, body, *pos, nil)
	}
	// Format PV
	pv := meta.GetHLV().GetPV()
	if len(pv) > 0 {
		startPos := *pos
		body, *pos = base.WriteJsonRawMsg(body, []byte(HLV_PV_FIELD), *pos, base.WriteJsonKey, len([]byte(HLV_PV_FIELD)), false)
		afterKeyPos := *pos
		*pos = VersionMapToBytes(pv, body, *pos, &pruneFunc)
		if *pos == afterKeyPos {
			// Did not add PV, need to back of and remove the PV key
			*pos = startPos
		}
	}
	body[*pos] = '}'
	*pos++

	return xattrComposer.CommitRawKVPair()
}

type ConflictParams struct {
	Source         *base.WrappedMCRequest
	Target         *base.SubdocLookupResponse
	SourceId       hlv.DocumentSourceId
	TargetId       hlv.DocumentSourceId
	MergeFunction  string
	ResultNotifier MergeResultNotifier
	TimeoutMs      uint32
	ObjectRecycler func(request *base.WrappedMCRequest) // for source WrappedMCRequest cleanup
}

type MergeInputAndResult struct {
	Action base.ConflictManagerAction
	Input  *ConflictParams
	Result interface{}
	Err    error
}

type MergeResultNotifier interface {
	NotifyMergeResult(input *ConflictParams, mergeResult interface{}, mergeError error)
}

func VersionMapToBytes(vMap hlv.VersionsMap, body []byte, pos int, pruneFunction *base.PruningFunc) int {
	startPos := pos
	first := true
	for key, cas := range vMap {
		if (pruneFunction == nil) || ((*pruneFunction)(cas) == false) {
			// Not pruned.
			value := base.Uint64ToBase64(cas)
			body, pos = base.WriteJsonRawMsg(body, []byte(key), pos, base.WriteJsonKey, len(key), first /*firstKey*/)
			body, pos = base.WriteJsonRawMsg(body, value, pos, base.WriteJsonValue, len(value), false /*firstKey*/)
			first = false
		}
	}
	if first {
		// We haven't added anything
		return startPos
	}
	body[pos] = '}'
	pos++
	return pos
}

// {"cvCas":...,"src":...,"ver":...
func formatCv(meta *CRMetadata, body []byte, pos int) int {
	hlv := meta.GetHLV()
	cvCas := hlv.GetCvCas()
	cvCasHex := base.Uint64ToHexLittleEndian(cvCas)
	body, pos = base.WriteJsonRawMsg(body, []byte(HLV_CVCAS_FIELD), pos, base.WriteJsonKey, len(HLV_CVCAS_FIELD), true /*firstKey*/)
	body, pos = base.WriteJsonRawMsg(body, cvCasHex, pos, base.WriteJsonValue, len(cvCasHex), false /*firstKey*/)
	src := hlv.GetCvSrc()
	ver := hlv.GetCvVer()
	body, pos = base.WriteJsonRawMsg(body, []byte(HLV_SRC_FIELD), pos, base.WriteJsonKey, len(HLV_SRC_FIELD), false /*firstKey*/)
	body, pos = base.WriteJsonRawMsg(body, []byte(src), pos, base.WriteJsonValue, len(src), false /*firstKey*/)
	cvHex := base.Uint64ToHexLittleEndian(ver)
	body, pos = base.WriteJsonRawMsg(body, []byte(HLV_VER_FIELD), pos, base.WriteJsonKey, len(HLV_VER_FIELD), false /*firstKey*/)
	body, pos = base.WriteJsonRawMsg(body, cvHex, pos, base.WriteJsonValue, len(cvHex), false /*firstKey*/)
	return pos
}

func (meta *CRMetadata) HadMv() bool {
	return meta.hadMv
}

func (meta *CRMetadata) HadPv() bool {
	return meta.hadPv
}

func xattrVVtoMap(vvBytes []byte) (hlv.VersionsMap, error) {
	res := make(hlv.VersionsMap)
	if len(vvBytes) == 0 {
		return res, nil
	}
	it, err := base.NewCCRXattrFieldIterator(vvBytes)
	if err != nil {
		return nil, err
	}
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return nil, err
		}
		src := hlv.DocumentSourceId(k)
		ver, err := base.Base64ToUint64(v)
		if err != nil {
			return nil, err
		}
		res[src] = ver
	}
	return res, nil
}

// Called when target has a smaller CAS but its xattrs dominates
// This routine receives the target metadata and returns an updated pv and mv to be used to send subdoc_multi_mutation to source.
// Target with smaller CAS can only dominate source if source document is a merged document.
func (meta *CRMetadata) UpdateMetaForSetBack() (pvBytes, mvBytes []byte, err error) {
	pv := meta.hlv.GetPV()
	pvPos := 0
	if len(pv) > 0 {
		pvlen := 0
		for src, _ := range pv {
			pvlen = pvlen + len(src) + base.MaxBase64CASLength + 6 // quotes and sepeartors
		}
		// We may need to add cv and document CAS to it also
		pvlen = pvlen + len(meta.hlv.GetCvSrc()) + (base.MaxBase64CASLength + 6)
		pvlen = pvlen + 2 // { and }
		// TODO(MB-41808): data pool
		pvBytes = make([]byte, pvlen)
		firstKey := true
		for src, ver := range pv {
			value := base.Uint64ToBase64(ver)
			pvBytes, pvPos = base.WriteJsonRawMsg(pvBytes, []byte(src), pvPos, base.WriteJsonKey, len(src), firstKey /*firstKey*/)
			pvBytes, pvPos = base.WriteJsonRawMsg(pvBytes, value, pvPos, base.WriteJsonValue, len(value), false /*firstKey*/)
			firstKey = false
		}
		pvBytes[pvPos] = '}'
		pvPos++
	}
	mv := meta.hlv.GetMV()
	mvPos := 0
	if len(mv) > 0 {
		mvlen := 0
		for src, _ := range mv {
			mvlen = mvlen + len(src) + base.MaxBase64CASLength + 6 // quotes and sepeartors
		}
		mvlen = mvlen + 2 // { and }
		// TODO(MB-41808): data pool
		mvBytes = make([]byte, mvlen)
		firstKey := true
		for src, ver := range mv {
			value := base.Uint64ToBase64(ver)
			mvBytes, mvPos = base.WriteJsonRawMsg(mvBytes, []byte(src), mvPos, base.WriteJsonKey, len(src), firstKey /*firstKey*/)
			mvBytes, mvPos = base.WriteJsonRawMsg(mvBytes, value, mvPos, base.WriteJsonValue, len(value), false /*firstKey*/)
			firstKey = false
		}
		mvBytes[mvPos] = '}'
		mvPos++
	} else {
		// If there is no mv, then cv and document.CAS represent mutation events. It needs to be in pv
		first := false
		if pvPos == 0 {
			first = true
		} else {
			pvPos--
		}
		source := meta.hlv.GetCvSrc()
		version := base.Uint64ToBase64(meta.hlv.GetCvVer())
		pvBytes, pvPos = base.WriteJsonRawMsg(pvBytes, []byte(source), pvPos, base.WriteJsonKey, len(source), first /*firstKey*/)
		pvBytes, pvPos = base.WriteJsonRawMsg(pvBytes, version, pvPos, base.WriteJsonValue, len(version), false /*firstKey*/)
		pvBytes[pvPos] = '}'
		pvPos++
	}
	return pvBytes[:pvPos], mvBytes[:mvPos], nil

}

func parseHlvFields(cas uint64, xattr []byte) (cvCas uint64, src hlv.DocumentSourceId, cvVer uint64, pvMap, mvMap hlv.VersionsMap, err error) {
	var cvCasHex, verHex, pv, mv []byte
	var err1 error
	it, err := base.NewCCRXattrFieldIterator(xattr)
	if err != nil {
		return
	}
	for it.HasNext() {
		var key, value []byte
		key, value, err = it.Next()
		if err != nil {
			return
		}
		switch string(key) {
		case HLV_SRC_FIELD:
			src = hlv.DocumentSourceId(value)
		case HLV_VER_FIELD:
			verHex = value
		case HLV_PV_FIELD:
			pv = value
		case HLV_MV_FIELD:
			mv = value
		case HLV_CVCAS_FIELD:
			cvCasHex = value
		}
	}
	if len(verHex) > 0 {
		cvVer, err1 = base.HexLittleEndianToUint64(verHex)
		if err1 != nil {
			err = fmt.Errorf("failed to convert ver from hex %s to uint64. err: %v", verHex, err1)
			return
		}
	}
	if len(cvCasHex) > 0 {
		cvCas, err1 = base.HexLittleEndianToUint64(cvCasHex)
		if err1 != nil {
			err = fmt.Errorf("failed to convert cvCas from hex %s to uint64. err: %v", cvCasHex, err1)
			return
		}
	}
	if cvVer > cvCas || cvVer > cas {
		// ver should never be larger than cvCas.
		// For server
		err = fmt.Errorf("cvVer shoud not be greater than cvCas or cas, cvCas=%v,ver=%v,cvCasHex=%v,verHex=%s,pv=%s,mv=%s", cvCas, cvVer, cvCasHex, verHex, pv, mv)
		return
	}
	pvMap, err1 = xattrVVtoMap(pv)
	if err1 != nil {
		err = fmt.Errorf("failed to convert pv '%s' to map, error: %v", pv, err1)
		return
	}
	mvMap, err1 = xattrVVtoMap(mv)
	if err1 != nil {
		err = fmt.Errorf("failed to convert mv '%s' to map, error: %v", mv, err1)
		return
	}
	return
}

func getMetaFromMCResponse(lookupResp *base.SubdocLookupResponse) (cas, cvCas uint64, cvSrc hlv.DocumentSourceId, cvVer uint64,
	pvMap, mvMap hlv.VersionsMap, err error) {
	cas = lookupResp.Resp.Cas
	xattr, err1 := lookupResp.ResponseForAPath(base.XATTR_HLV)
	if err1 != nil {
		err = fmt.Errorf("failed to find subdoc_lookup result for path %s for document %s%q%s, error: %v", base.XATTR_HLV, base.UdTagBegin, lookupResp.Resp.Key, base.UdTagEnd, err1)
		return
	}
	if xattr == nil {
		return
	}
	cvCas, cvSrc, cvVer, pvMap, mvMap, err1 = parseHlvFields(cas, xattr)
	if err1 != nil {
		err = fmt.Errorf("failed to parse HLV fields for document %s%q%s, error: %v", base.UdTagBegin, lookupResp.Resp.Key, base.UdTagEnd, err)
		return
	}

	return
}

// This will find the custom CR XATTR from the req body
func getMetaFromMCRequest(req *mc.MCRequest) (cas, cvCas uint64, cvSrc hlv.DocumentSourceId, cvVer uint64, pvMap, mvMap hlv.VersionsMap, err error) {
	cas = binary.BigEndian.Uint64(req.Extras[16:24])
	if req.DataType&mcc.XattrDataType == 0 {
		return
	}
	body := req.Body
	var pos uint32 = 0
	pos = pos + 4
	xattrIter, err := base.NewXattrIterator(body)
	if err != nil {
		return
	}
	var key, value []byte
	var xattr []byte
	var err1 error
	for xattrIter.HasNext() {
		key, value, err = xattrIter.Next()
		if err != nil {
			return
		}
		if base.Equals(key, base.XATTR_HLV) {
			xattr = value
			break
		}
	}
	if xattr == nil {
		// Source does not have HLV XATTR
		return
	}
	// Found HLV XATTR. Now find the fields
	cvCas, cvSrc, cvVer, pvMap, mvMap, err1 = parseHlvFields(cas, xattr)
	if err1 != nil {
		err = fmt.Errorf("failed to parse HLV fields for document %s%q%s, error: %v", base.UdTagBegin, req.Key, base.UdTagEnd, err1)
		return
	}
	return
}

// if all other metadata fields are equal, use xattr field to decide whether source mutations should win
func resolveConflictByXattr(doc_meta_source base.DocumentMetadata,
	doc_meta_target base.DocumentMetadata, xattrEnabled bool) bool {
	if xattrEnabled {
		// if target is xattr enabled, source mutation has xattr, and target mutation does not have xattr
		// let source mutation win
		source_has_xattr := base.HasXattr(doc_meta_source.DataType)
		target_has_xattr := base.HasXattr(doc_meta_target.DataType)
		return source_has_xattr && !target_has_xattr
	} else {
		// if target is not xattr enabled, target mutation always does not have xattr
		// do not have let source mutation win even if source mutation has xattr,
		// otherwise source mutations need to be repeatly re-sent in backfill mode
		return false
	}
}
