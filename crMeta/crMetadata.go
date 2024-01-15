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

	XATTR_IMPORTCAS = base.XATTR_IMPORTCAS
)

// CRMetadata contains the metadata required to perform conflict resolution. It has two main parts:
//   - docMeta: It has cas, revId, etc,  used for LWW/revId CR. Its values are normally obtained from document metadata.
//   - hlv: This is used for custom conflict resolution. We also maintain it for mobile if bucket setting
//     EnableCrossClusterVersioningKey is true
//
// Mobile has a special mutation generated during import operation which we want to avoid replicating.
// When mobile imports a document, it will
// 1. Update its HLV, with cvCAS set to the pre-import document CAS.
// 2. Update mobile metadata (_sync XATTR)
// 3. Write back the document.
// This results in a new mutation with new CAS. Mobile will set an XATTR _importCAS to the same value as the new CAS
// using macro-expansion.
//
// The resulting import mutation has the following properties:
// 1. importCAS == document.CAS
// 2. cvCAS == pre-import document CAS
// We don't want import mutation to win over its pre-import predecessor. To achieve that,
// we will use pre-import CAS value for conflict resolution so docMeta.Cas will be set to this value.
// Pre-import revId is not saved. So if one of the mutation is import mutation, revId will not be used in LWW CR.
// revId CR cannot be supported either (unless mobile savs pre-import revId)
//
// If there is a new mutation after the import, the new mutation will have:
// - document.CAS > importCAS
// This document is no longer considered import mutation. It is a local mutation. When it is replicated to a
// target, the importCAS XATTR will be removed.
type CRMetadata struct {
	docMeta *base.DocumentMetadata
	// The HLV is always updated from what's stored in the document XATTR,
	// ie, MV may roll into PV if necessary, and PV may get pruned, and CV may be updated.
	hlv *hlv.HLV
	// The following indicate if the document has PV/MV in the XATTR. Since the HLV may be different from what's stored in
	// XATTR, we need the following to decide whether to send delete for PV or MV in the subdoc command.
	hadPv bool
	hadMv bool
	// If the document does not already have HLV, we will not generate one if the mutation is older than vbMaxCas
	hadHlv bool
	// It is import mutation if importCas == document.CAS. In that case, docMeta.Cas is the pre-import CAS value.
	isImport bool
	// This is the value parsed from the XATTR _importCAS.
	importCas uint64
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

func (m *CRMetadata) IsImportMutation() bool {
	return m.isImport
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

func (doc *SourceDocument) GetMetadata(uncompressFunc base.UncompressFunc) (*CRMetadata, error) {
	docMeta := base.DecodeSetMetaReq(doc.req.Req)
	cas, cvCas, cvSrc, cvVer, pvMap, mvMap, importCas, err := getHlvFromMCRequest(doc.req, uncompressFunc)
	if err != nil {
		return nil, err
	}
	meta := CRMetadata{
		docMeta: &docMeta,
	}
	if cvCas > 0 {
		// The document already has an HLV. In this case we need to keep it updated even if enableCrossClusterVersioning is off
		meta.hadHlv = true
	}
	if len(pvMap) > 0 {
		// This comes straight from source document XATTR. So source had it.  meta.hlv tells us if it still has it
		// after replication
		meta.hadPv = true
	}
	if len(mvMap) > 0 {
		// This comes straight from source document XATTR. So source had it.  meta.hlv tells us if it still has it
		// after replication
		meta.hadMv = true
	}
	if importCas == cas {
		// This is an import mutation. cvCas represents the pre-import CAS and that is used for CR
		// pre-import revId is not saved. We cannot use revId. set it to 0
		meta.isImport = true
		docMeta.Cas = cvCas
		docMeta.RevSeq = 0
	}
	doc.req.ImportMutation = meta.isImport
	// The docMeta.Cas below represents the last mutation that's not import mutation.
	meta.hlv, err = hlv.NewHLV(doc.source, docMeta.Cas, cvCas, cvSrc, cvVer, pvMap, mvMap)
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
	includeHlv   bool
}

func NewTargetDocument(key []byte, resp *mc.MCResponse, specs []base.SubdocLookupPathSpec, source hlv.DocumentSourceId, xattrEnabled, includeHlv bool) (*TargetDocument, error) {
	if resp.Status == mc.KEY_ENOENT {
		return nil, base.ErrorDocumentNotFound
	}
	return &TargetDocument{
		source: source,
		resp: &base.SubdocLookupResponse{
			Specs: specs,
			Resp:  resp,
		},
		key:          key,
		xattrEnabled: xattrEnabled,
		includeHlv:   includeHlv,
	}, nil
}

func (doc *TargetDocument) GetMetadata() (*CRMetadata, error) {
	if doc.resp.Resp.Status == mc.KEY_ENOENT {
		return nil, base.ErrorDocumentNotFound
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
		meta := CRMetadata{
			docMeta: &docMeta,
		}
		if doc.includeHlv {
			cas, cvCas, cvSrc, cvVer, pvMap, mvMap, importCas, err := getHlvFromMCResponse(doc.resp)
			if err != nil {
				return nil, err
			}
			if len(pvMap) > 0 {
				// This comes straight from target document XATTR. So target had it. meta.hlv tells us if it still has it after replication
				meta.hadPv = true
			}
			if len(mvMap) > 0 {
				// This comes straight from target document XATTR. So target had it.  meta.hlv tells us if it still has it
				meta.hadMv = true
			}
			if importCas == cas {
				// This is an import mutation. cvCas represents the pre-import CAS and that is used for CR
				// Pre-import revId is not used. So we cannot use revId. set it to 0
				meta.isImport = true
				docMeta.Cas = cvCas
				docMeta.RevSeq = 0
			}
			meta.hlv, err = hlv.NewHLV(doc.source, docMeta.Cas, cvCas, cvSrc, cvVer, pvMap, mvMap)
			if err != nil {
				return nil, err
			}
		}
		return &meta, nil
	}
	return nil, fmt.Errorf("GetMetadata: bad target document opcode %v, specs %v", doc.resp.Resp.Opcode, doc.resp.Specs)
}

// This is called after getting the target document based on the lookup spec. We should have both source and target XATTRs
// If target doesn't exist (target.Status == KEY_ENOENT), return SourceDominate
func DetectConflictWithHLV(req *base.WrappedMCRequest, resp *mc.MCResponse, specs []base.SubdocLookupPathSpec, sourceId, targetId hlv.DocumentSourceId, xattrEnabled bool, uncompressFunc base.UncompressFunc, logger *log.CommonLogger) (base.ConflictResult, error) {
	if resp.Status == mc.KEY_ENOENT {
		return base.SendToTarget, nil
	}
	sourceDoc := NewSourceDocument(req, sourceId)
	sourceMeta, err := sourceDoc.GetMetadata(uncompressFunc)
	if err != nil {
		return base.Error, err
	}
	targetDoc, err := NewTargetDocument(req.Req.Key, resp, specs, targetId, xattrEnabled, true)
	if err == base.ErrorDocumentNotFound {
		return base.SendToTarget, nil
	} else if err != nil {
		return base.Error, err
	}
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

func ResolveConflictByCAS(req *base.WrappedMCRequest, resp *mc.MCResponse, specs []base.SubdocLookupPathSpec, sourceId, targetId hlv.DocumentSourceId, xattrEnabled bool, uncompressFunc base.UncompressFunc, logger *log.CommonLogger) (base.ConflictResult, error) {
	if resp.Status == mc.KEY_ENOENT {
		return base.SendToTarget, nil
	}
	var doc_meta_source, doc_meta_target base.DocumentMetadata
	var err error
	if resp.Opcode == base.GET_WITH_META {
		doc_meta_source = base.DecodeSetMetaReq(req.Req)
		doc_meta_target, err = base.DecodeGetMetaResp(req.Req.Key, resp, xattrEnabled)
		if err != nil {
			return base.Error, err
		}
	} else if resp.Opcode == mc.SUBDOC_MULTI_LOOKUP {
		source_doc := NewSourceDocument(req, sourceId)
		source_meta, err := source_doc.GetMetadata(uncompressFunc)
		if err != nil {
			return base.Error, err
		}
		doc_meta_source = *source_meta.docMeta
		target_doc, err := NewTargetDocument(req.Req.Key, resp, specs, targetId, xattrEnabled, false)
		if err == base.ErrorDocumentNotFound {
			return base.SendToTarget, nil
		} else if err != nil {
			return base.Error, err
		}
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
		// We cannot use revId if they are not saved (set to 0) which is the case for import mutation.
		if doc_meta_target.RevSeq > doc_meta_source.RevSeq && doc_meta_source.RevSeq != 0 && doc_meta_target.RevSeq != 0 {
			sourceWin = false
		} else if doc_meta_target.RevSeq == doc_meta_source.RevSeq || doc_meta_source.RevSeq == 0 || doc_meta_target.RevSeq == 0 {
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

func ResolveConflictByRevSeq(req *base.WrappedMCRequest, resp *mc.MCResponse, specs []base.SubdocLookupPathSpec, sourceId, targetId hlv.DocumentSourceId, xattrEnabled bool, uncompressFunc base.UncompressFunc, logger *log.CommonLogger) (base.ConflictResult, error) {
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
		target_doc, err := NewTargetDocument(req.Req.Key, resp, specs, targetId, xattrEnabled, false)
		if err == base.ErrorDocumentNotFound {
			return base.SendToTarget, nil
		} else if err != nil {
			return base.Error, err
		}
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

func NeedToUpdateHlv(meta *CRMetadata, vbMaxCas uint64, pruningWindow time.Duration) bool {
	if meta == nil || meta.GetHLV() == nil {
		return false
	}
	if meta.isImport {
		// We have an import mutation that's winning CR. mobile has already updated HLV so it doesn't need update
		return false
	}
	if meta.hadHlv == false && meta.docMeta.Cas < vbMaxCas {
		// This is older mutation that doesn't already have an HLV
		return false
	}
	hlv := meta.hlv
	// We need to update if CAS has changed from ver.CAS
	if hlv.Updated {
		return true
	}

	// The CAS has not changed. But do we need to prune PV?

	// No pruning if its setting is 0 or there is no hlv to prune
	if pruningWindow == 0 || hlv == nil || len(hlv.GetPV()) == 0 {
		return false
	}
	pv := hlv.GetPV()
	for _, cas := range pv {
		// Check for pruning window
		if base.CasDuration(cas, meta.docMeta.Cas) >= pruningWindow {
			return true
		}
	}
	return false
}

// This routine construct XATTR _vv:{...} based on meta. The constructed XATTRs includes updates for
// new change (meta.cas > meta.ver) and pruning in PV
func ConstructXattrFromHlvForSetMeta(meta *CRMetadata, pruningWindow time.Duration, xattrComposer *base.XattrComposer) (int, bool, error) {
	if meta == nil {
		return 0, false, fmt.Errorf("Metadata cannot be nil")
	}
	err := xattrComposer.StartRawMode()
	if err != nil {
		return -1, false, err
	}

	err = xattrComposer.RawWriteKey([]byte(base.XATTR_HLV))
	if err != nil {
		return -1, false, err
	}

	body, pos, err := xattrComposer.RawHijackValue()
	if err != nil {
		return -1, false, err
	}

	pruneFunc := base.GetHLVPruneFunction(meta.GetDocumentMetadata().Cas, pruningWindow)
	*pos = formatCv(meta, body, *pos)
	// Format MV
	mv := meta.GetHLV().GetMV()
	if len(mv) > 0 {
		// This is not the first since we have cv before this
		body, *pos = base.WriteJsonRawMsg(body, []byte(HLV_MV_FIELD), *pos, base.WriteJsonKey, len([]byte(HLV_MV_FIELD)), false)
		*pos, _ = VersionMapToBytes(mv, body, *pos, nil)
	}
	// Format PV
	pv := meta.GetHLV().GetPV()
	var pruned bool
	if len(pv) > 0 {
		startPos := *pos
		body, *pos = base.WriteJsonRawMsg(body, []byte(HLV_PV_FIELD), *pos, base.WriteJsonKey, len([]byte(HLV_PV_FIELD)), false)
		afterKeyPos := *pos
		*pos, pruned = VersionMapToBytes(pv, body, *pos, &pruneFunc)
		if *pos == afterKeyPos {
			// Did not add PV, need to back of and remove the PV key
			*pos = startPos
		}
	}
	body[*pos] = '}'
	*pos++

	*pos, err = xattrComposer.CommitRawKVPair()
	return *pos, pruned, err
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
	UncompressFunc base.UncompressFunc
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

func VersionMapToBytes(vMap hlv.VersionsMap, body []byte, pos int, pruneFunction *base.PruningFunc) (int, bool) {
	startPos := pos
	first := true
	pruned := false
	for key, cas := range vMap {
		if (pruneFunction == nil) || ((*pruneFunction)(cas) == false) {
			// Not pruned.
			value := base.Uint64ToBase64(cas)
			body, pos = base.WriteJsonRawMsg(body, []byte(key), pos, base.WriteJsonKey, len(key), first /*firstKey*/)
			body, pos = base.WriteJsonRawMsg(body, value, pos, base.WriteJsonValue, len(value), false /*firstKey*/)
			first = false
		} else {
			pruned = true
		}
	}
	if first {
		// We haven't added anything
		return startPos, pruned
	}
	body[pos] = '}'
	pos++
	return pos, pruned
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
	prefixedSrc := append([]byte(base.SERVER_SRC_PREFIX), []byte(src)...)
	body, pos = base.WriteJsonRawMsg(body, prefixedSrc, pos, base.WriteJsonValue, len(prefixedSrc), false /*firstKey*/)
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
			src = hlv.ParseDocumentSource(value)
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

func getHlvFromMCResponse(lookupResp *base.SubdocLookupResponse) (cas, cvCas uint64, cvSrc hlv.DocumentSourceId, cvVer uint64,
	pvMap, mvMap hlv.VersionsMap, importCas uint64, err error) {
	cas = lookupResp.Resp.Cas
	xattr, err1 := lookupResp.ResponseForAPath(base.XATTR_HLV)
	if err1 != nil {
		err = fmt.Errorf("failed to find subdoc_lookup result for path %s for document %s%q%s, error: %v", base.XATTR_HLV, base.UdTagBegin, lookupResp.Resp.Key, base.UdTagEnd, err1)
		return
	}
	if xattr != nil {
		cvCas, cvSrc, cvVer, pvMap, mvMap, err1 = parseHlvFields(cas, xattr)
		if err1 != nil {
			err = fmt.Errorf("failed to parse HLV fields for document %s%q%s, error: %v", base.UdTagBegin, lookupResp.Resp.Key, base.UdTagEnd, err)
			return
		}
	}
	// It is ok to not find this path, since we may not be getting it if enableCrossClusterVersioning is not on, or target is not an import mutation.
	xattr, err1 = lookupResp.ResponseForAPath(XATTR_IMPORTCAS)
	xattrLen := len(xattr)
	if err1 == nil && xattrLen > 0 {
		// Strip the start/end quotes to get the importCas value
		importCas, err = base.HexLittleEndianToUint64(xattr[1 : xattrLen-1])
	}
	return
}

// This will find the custom CR XATTR from the req body, including HLV and _importCas
func getHlvFromMCRequest(wrappedReq *base.WrappedMCRequest, uncompressFunc base.UncompressFunc) (cas, cvCas uint64, cvSrc hlv.DocumentSourceId, cvVer uint64, pvMap, mvMap hlv.VersionsMap, importCas uint64, err error) {
	req := wrappedReq.Req
	cas = binary.BigEndian.Uint64(req.Extras[16:24])
	if req.DataType&mcc.XattrDataType == 0 {
		return
	}
	if err = uncompressFunc(wrappedReq); err != nil {
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
	var xattrHlv, xattrImportCas []byte
	var err1 error
	for xattrIter.HasNext() {
		key, value, err = xattrIter.Next()
		if err != nil {
			return
		}
		if base.Equals(key, base.XATTR_HLV) {
			xattrHlv = value
		}
		if base.Equals(key, XATTR_IMPORTCAS) {
			xattrImportCas = value
		}
	}
	if xattrImportCas != nil {
		// Remove the start/end quotes before converting it to uint64
		xattrLen := len(xattrImportCas)
		importCas, err = base.HexLittleEndianToUint64(xattrImportCas[1 : xattrLen-1])
		if err != nil {
			return
		}
	}
	if xattrHlv == nil {
		// Source does not have HLV XATTR
		return
	}
	// Found HLV XATTR. Now find the fields
	cvCas, cvSrc, cvVer, pvMap, mvMap, err1 = parseHlvFields(cas, xattrHlv)
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

func NewMetadataForTest(key, source []byte, cas, revId uint64, cvCasHex, cvSrc, verHex, pv, mv []byte) (*CRMetadata, error) {
	var cvCas, ver uint64
	var err error
	if len(verHex) == 0 {
		ver = 0
	} else {
		ver, err = base.HexLittleEndianToUint64(verHex)
		if err != nil {
			return nil, err
		}
	}
	if len(cvCasHex) == 0 {
		cvCas = 0
	} else {
		cvCas, err = base.HexLittleEndianToUint64(verHex)
		if err != nil {
			return nil, err
		}
	}
	pvMap, err := xattrVVtoMap(pv)
	if err != nil {
		return nil, err
	}
	mvMap, err := xattrVVtoMap(mv)
	if err != nil {
		return nil, err
	}
	hlv, err := hlv.NewHLV(hlv.DocumentSourceId(source), cas, cvCas, hlv.DocumentSourceId(cvSrc), ver, pvMap, mvMap)
	if err != nil {
		return nil, err
	}
	meta := CRMetadata{
		docMeta: &base.DocumentMetadata{
			Key:      key,
			RevSeq:   revId,
			Cas:      cas,
			Flags:    0,
			Expiry:   0,
			Deletion: false,
			DataType: 0,
		},
		hlv: hlv,
	}
	return &meta, nil
}
