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
	"strconv"
	"time"

	"github.com/couchbase/gomemcached"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/hlv"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbaselabs/gojsonsm"
)

// Custom CR related constants
const (
	HLV_CVCAS_FIELD = "cvCas" // This stores the version of the HLV
	// These are the fields in the HLV
	HLV_SRC_FIELD = "src" // src and ver combined is the cv in the design
	HLV_VER_FIELD = "ver" //
	HLV_MV_FIELD  = "mv"  // the MV field in _xdcr
	HLV_PV_FIELD  = "pv"  // The PV field in _xdcr

	XATTR_CVCAS_PATH = base.XATTR_HLV + base.PERIOD + HLV_CVCAS_FIELD
	XATTR_SRC_PATH   = base.XATTR_HLV + base.PERIOD + HLV_SRC_FIELD
	XATTR_VER_PATH   = base.XATTR_HLV + base.PERIOD + HLV_VER_FIELD
	XATTR_MV_PATH    = base.XATTR_HLV + base.PERIOD + HLV_MV_FIELD
	XATTR_PV_PATH    = base.XATTR_HLV + base.PERIOD + HLV_PV_FIELD

	XATTR_IMPORTCAS   = base.XATTR_IMPORTCAS
	XATTR_PREVIOUSREV = base.XATTR_PREVIOUSREV
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
	// If the document has _mou, we need the following to decide whether to delete _mou in the subdoc command.
	hadMou bool
	// It is import mutation if importCas == document.CAS. In that case, docMeta.Cas is the pre-import CAS value.
	isImport bool
	// This is the value parsed from the XATTR _importCAS.
	importCas uint64
	// if isImport is true, we will use this to store the actual Cas,
	// since we replace doc.Cas with pre-import Cas i.e. cvCas for conflict resolution
	actualCas uint64
}

func (m *CRMetadata) GetDocumentMetadata() *base.DocumentMetadata {
	if m == nil {
		return nil
	}
	return m.docMeta
}
func (m *CRMetadata) SetDocumentMetadata(docMeta *base.DocumentMetadata) {
	m.docMeta = docMeta
}

func (m *CRMetadata) GetHLV() *hlv.HLV {
	if m == nil {
		return nil
	}
	return m.hlv
}
func (m *CRMetadata) SetHLV(hlv *hlv.HLV) {
	m.hlv = hlv
}

func (m *CRMetadata) GetImportCas() uint64 {
	return m.importCas
}
func (m *CRMetadata) SetImportCas(importCas uint64) {
	m.importCas = importCas
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

func (m *CRMetadata) UpdateHLVIfNeeded(source hlv.DocumentSourceId, cas uint64, cvCas uint64, cvSrc hlv.DocumentSourceId, cvVer uint64, pvMap hlv.VersionsMap, mvMap hlv.VersionsMap, importCas uint64, pRev uint64) (err error) {
	if cvCas > 0 {
		// The document already has an HLV. In this case we need to keep it updated even if enableCrossClusterVersioning is off
		m.hadHlv = true
	}
	if len(pvMap) > 0 {
		// This comes straight from the document XATTR. So the document had it.  meta.hlv tells us if it still has it
		// after replication
		m.hadPv = true
	}
	if len(mvMap) > 0 {
		// This comes straight from the document XATTR. So the documentc had it.  meta.hlv tells us if it still has it
		// after replication
		m.hadMv = true
	}
	if importCas == cas {
		// This is an import mutation. cvCas represents the pre-import CAS and that is used for CR
		// pRev is the pre-import revId which is used for CR
		m.isImport = true
		m.docMeta.Cas = cvCas
		m.docMeta.RevSeq = pRev
	}

	// The docMeta.Cas below represents the last mutation that's not import mutation.
	m.hlv, err = hlv.NewHLV(source, m.docMeta.Cas, cvCas, cvSrc, cvVer, pvMap, mvMap)
	if err != nil {
		return err
	}

	return nil
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
	cas, cvCas, cvSrc, cvVer, pvMap, mvMap, importCas, pRev, err := getHlvFromMCRequest(doc.req, uncompressFunc)
	if err != nil {
		return nil, err
	}

	meta := CRMetadata{
		docMeta:   &docMeta,
		actualCas: docMeta.Cas,
		hadMou:    importCas > 0, // we will use the presence of importCAS to determine if we have _mou
	}

	err = meta.UpdateHLVIfNeeded(doc.source, cas, cvCas, cvSrc, cvVer, pvMap, mvMap, importCas, pRev)
	if err != nil {
		return nil, err
	}

	doc.req.ImportMutation = meta.isImport

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
			docMeta:   &docMeta,
			actualCas: docMeta.Cas,
		}
		if doc.includeHlv {
			cas, cvCas, cvSrc, cvVer, pvMap, mvMap, importCas, pRev, err := getHlvFromMCResponse(doc.resp)
			if err != nil {
				return nil, err
			}

			// We will use the presence of importCAS to determine if we have _mou
			meta.hadMou = importCas > 0

			err = meta.UpdateHLVIfNeeded(doc.source, cas, cvCas, cvSrc, cvVer, pvMap, mvMap, importCas, pRev)
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
	return base.Error, fmt.Errorf("bad result from ccrMeta.DetectConlict: %v, source meta: %s, %s, target meta: %s, %s",
		conflictResult, sourceDocMeta, sourceHLV, targetDocMeta, targetHLV)
}

func GetMetadataForCR(req *base.WrappedMCRequest, resp *mc.MCResponse, specs []base.SubdocLookupPathSpec, sourceId, targetId hlv.DocumentSourceId, xattrEnabled bool, uncompressFunc base.UncompressFunc) (base.DocumentMetadata, base.DocumentMetadata, error) {
	var doc_meta_source, doc_meta_target base.DocumentMetadata
	var target_meta, source_meta *CRMetadata
	var err error

	if resp.Opcode == base.GET_WITH_META {
		// GET_WITH_META will also be used when only ECCV is on (mobile is off) and cas < max_cas.
		// source HLV is not parsed and target HLV is not fetched.
		doc_meta_source = base.DecodeSetMetaReq(req.Req)
		doc_meta_target, err = base.DecodeGetMetaResp(req.Req.Key, resp, xattrEnabled)
		if err != nil {
			err = fmt.Errorf("error decoding GET_META response for key=%v%s%v, respBody=%v%v%v, err=%v", base.UdTagBegin, req.Req.Key, base.UdTagEnd, base.UdTagBegin, resp.Body, base.UdTagEnd, err)
			return doc_meta_source, doc_meta_target, err
		}
	} else if resp.Opcode == mc.SUBDOC_MULTI_LOOKUP {
		// We always parse the HLV in the source mutation for CR, if mobile is on, given that there could
		// be import on source cluster and it could be an active site in the mixed mode of the SGW+XDCR active-passive
		// setup, which is supported.
		// However for the target doc, we only fetch HLV if cas >= max_cas and if ECCV is on.
		// For the following cases, we don't fetch target HLV:
		// 1. mobile is on, ECCV is on and cas < max_cas (mixed mode) - this is fine as long as it was an
		// active-passive setup when this mutation was created.
		// 2. mobile is on, but ECCV is off - not supported and can lead to data loss as import on target can win CR.
		// Hence, target is not expected to have import in mixed mode, assuming that it is a passive site in the mixed
		// mode. Doing import on target, which is a passive site during mixed mode may cause data loss.

		source_doc := NewSourceDocument(req, sourceId)
		source_meta, err = source_doc.GetMetadata(uncompressFunc)
		if err != nil {
			err = fmt.Errorf("error decoding source mutation for key=%v%s%v, req=%v%v%v, reqBody=%v%v%v, err=%v", base.UdTagBegin, req.Req.Key, base.UdTagEnd, base.UdTagBegin, req.Req, base.UdTagEnd, base.UdTagBegin, req.Req.Body, base.UdTagEnd, err)
			return doc_meta_source, doc_meta_target, err
		}
		doc_meta_source = *source_meta.docMeta
		target_doc, err := NewTargetDocument(req.Req.Key, resp, specs, targetId, xattrEnabled, req.HLVModeOptions.IncludeTgtHlv)
		if err != nil {
			if err == base.ErrorDocumentNotFound {
				return doc_meta_source, doc_meta_target, err
			}
			err = fmt.Errorf("error creating target document for key=%v%s%v, respBody=%v%v%v, err=%v", base.UdTagBegin, req.Req.Key, base.UdTagEnd, base.UdTagBegin, resp.Body, base.UdTagEnd, err)
			return doc_meta_source, doc_meta_target, err
		}
		target_meta, err = target_doc.GetMetadata()
		if err != nil {
			err = fmt.Errorf("error decoding target SUBDOC_MULTI_LOOKUP response for key=%v%s%v, respBody=%v%v%v, err=%v", base.UdTagBegin, req.Req.Key, base.UdTagEnd, base.UdTagBegin, resp.Body, base.UdTagEnd, err)
			return doc_meta_source, doc_meta_target, err
		}
		doc_meta_target = *target_meta.docMeta

		if target_meta.IsImportMutation() && source_meta.actualCas < target_meta.actualCas {
			// This is the case when target CAS will rollback if source wins
			// So use subdoc command in this case instead of *_WITH_META commands
			req.SetSubdocOp()
			req.SubdocCmdOptions.TargetHasPv = target_meta.hadPv
			req.SubdocCmdOptions.TargetHasMv = target_meta.hadMv
			req.SubdocCmdOptions.TargetHasMou = target_meta.hadMou
			req.SubdocCmdOptions.TargetDocIsTombstone = doc_meta_target.Deletion
		}
	}

	return doc_meta_source, doc_meta_target, nil
}

func ResolveConflictByCAS(req *base.WrappedMCRequest, resp *mc.MCResponse, specs []base.SubdocLookupPathSpec, sourceId, targetId hlv.DocumentSourceId, xattrEnabled bool, uncompressFunc base.UncompressFunc, logger *log.CommonLogger) (base.ConflictResult, error) {
	if resp.Status == mc.KEY_ENOENT {
		return base.SendToTarget, nil
	}

	doc_meta_source, doc_meta_target, err := GetMetadataForCR(req, resp, specs, sourceId, targetId, xattrEnabled, uncompressFunc)
	if err == base.ErrorDocumentNotFound {
		return base.SendToTarget, nil
	} else if err != nil {
		return base.Error, err
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

	doc_meta_source, doc_meta_target, err := GetMetadataForCR(req, resp, specs, sourceId, targetId, xattrEnabled, uncompressFunc)
	if err == base.ErrorDocumentNotFound {
		return base.SendToTarget, nil
	} else if err != nil {
		return base.Error, err
	}

	sourceWin := true
	if doc_meta_target.RevSeq > doc_meta_source.RevSeq {
		sourceWin = false
		// TODO: Revisit MB-60346 for RevId rollback, after MB-60385 is done
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

// Check if HLV needs to be updated/stamped, i.e. when
// 1. ECCV is on and meta.cas >= vbMaxCas - stamp a new HLV or update the existing HLV.
// 2. ECCV is on and meta.cas < vbMaxCas - update if there is an existing HLV. Do not stamp a new HLV if it doesn't exist in the mutation already.
// 3. ECCV is off - update if there is an existing HLV. Do not stamp a new HLV if it doesn't exist in the mutation already.
func NeedToUpdateHlv(meta *CRMetadata, vbMaxCas uint64, pruningWindow time.Duration) bool {
	if meta == nil || meta.GetHLV() == nil {
		return false
	}
	if meta.isImport {
		// We have an import mutation that's winning CR. mobile has already updated HLV so it doesn't need update
		return false
	}
	if !meta.hadHlv && (vbMaxCas == 0 || meta.actualCas < vbMaxCas) {
		// This mutation doesn't already have an HLV, HLV is not newly stamped if
		// 1. ECCV is not on (i.e. vbMaxCas == 0).
		// 2. ECCV is on, but this is an older mutation (meta.docMeta.Cas < vbMaxCas).
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
		return 0, false, fmt.Errorf("metadata cannot be nil")
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
		*pos, _ = VersionMapToDeltasBytes(mv, body, *pos, nil)
	}
	// Format PV
	pv := meta.GetHLV().GetPV()
	var pruned bool
	if len(pv) > 0 {
		startPos := *pos
		body, *pos = base.WriteJsonRawMsg(body, []byte(HLV_PV_FIELD), *pos, base.WriteJsonKey, len([]byte(HLV_PV_FIELD)), false)
		afterKeyPos := *pos
		*pos, pruned = VersionMapToDeltasBytes(pv, body, *pos, &pruneFunc)
		if *pos == afterKeyPos {
			// Did not add PV, need to back off and remove the PV key
			*pos = startPos
		}
	}
	body[*pos] = '}'
	*pos++

	*pos, err = xattrComposer.CommitRawKVPair()
	return *pos, pruned, err
}

// This routine contructs HLV related operational specs for subdoc operation
func ConstructSpecsFromHlvForSubdocOp(meta *CRMetadata, pruningWindow time.Duration, specs *base.SubdocMutationPathSpecs, targetHasPV, targetHasMv bool, pvSlice, mvSlice []byte) (bool, error) {
	if meta == nil {
		return false, fmt.Errorf("metadata cannot be nil")
	}

	var spec base.SubdocMutationPathSpec
	var idx int

	pruneFunc := base.GetHLVPruneFunction(meta.GetDocumentMetadata().Cas, pruningWindow)
	HLV := meta.GetHLV()

	// set cvCas as regenerated Cas from mc.SET/DELETE using macro expansion.
	// MKDIR_P is not set, because we know for sure there exists cvCas on target since we used it for conflict resolution, otherwise there is something wrong
	spec = base.NewSubdocMutationPathSpec(uint8(base.SUBDOC_DICT_UPSERT), uint8(base.SUBDOC_FLAG_XATTR|base.SUBDOC_FLAG_EXPAND_MACROS), []byte(XATTR_CVCAS_PATH), []byte(base.CAS_MACRO_EXPANSION))
	*specs = append(*specs, spec)

	// format CV
	// src
	srcVal := HLV.GetCvSrc()
	src := []byte("\"" + string(srcVal) + "\"")
	spec = base.NewSubdocMutationPathSpec(uint8(base.SUBDOC_DICT_UPSERT), uint8(base.SUBDOC_FLAG_MKDIR_P|base.SUBDOC_FLAG_XATTR), []byte(XATTR_SRC_PATH), src)
	*specs = append(*specs, spec)

	// ver should also be macro expanded cas and should not need MKDIR_P
	spec = base.NewSubdocMutationPathSpec(uint8(base.SUBDOC_DICT_UPSERT), uint8(base.SUBDOC_FLAG_XATTR|base.SUBDOC_FLAG_EXPAND_MACROS), []byte(XATTR_VER_PATH), []byte(base.CAS_MACRO_EXPANSION))
	*specs = append(*specs, spec)

	// Format MV
	mv := HLV.GetMV()
	if len(mv) > 0 {
		idx = 0
		idx, _ = VersionMapToDeltasBytes(mv, mvSlice, idx, nil)
		spec = base.NewSubdocMutationPathSpec(uint8(base.SUBDOC_DICT_UPSERT), uint8(base.SUBDOC_FLAG_MKDIR_P|base.SUBDOC_FLAG_XATTR), []byte(XATTR_MV_PATH), mvSlice[:idx])
		*specs = append(*specs, spec)
	} else if targetHasMv {
		// no mv left after processing - remove from target if mv already exists
		spec = base.NewSubdocMutationPathSpec(uint8(base.SUBDOC_DELETE), uint8(base.SUBDOC_FLAG_XATTR), []byte(XATTR_MV_PATH), nil)
		*specs = append(*specs, spec)
	}

	// Format PV, after pruning
	pv := HLV.GetPV()
	var pruned bool
	if len(pv) > 0 {
		idx = 0
		afterKeyPos := idx
		idx, pruned = VersionMapToDeltasBytes(pv, pvSlice, idx, &pruneFunc)
		if idx == afterKeyPos {
			// PV is all pruned - remove from target
			if !targetHasPV {
				// _vv doesn't exist on target or pv doesn't exist already, no need to remove
				return pruned, nil
			}
			// pv exists on target, but the source mutation PV is all pruned, so delete the one on target.
			spec = base.NewSubdocMutationPathSpec(uint8(base.SUBDOC_DELETE), uint8(base.SUBDOC_FLAG_XATTR), []byte(XATTR_PV_PATH), nil)
			*specs = append(*specs, spec)
		} else {
			spec = base.NewSubdocMutationPathSpec(uint8(base.SUBDOC_DICT_UPSERT), uint8(base.SUBDOC_FLAG_MKDIR_P|base.SUBDOC_FLAG_XATTR), []byte(XATTR_PV_PATH), pvSlice[:idx])
			*specs = append(*specs, spec)
		}
	} else if targetHasPV {
		// no pv left after processing - remove from target if pv already exists
		spec = base.NewSubdocMutationPathSpec(uint8(base.SUBDOC_DELETE), uint8(base.SUBDOC_FLAG_XATTR), []byte(XATTR_PV_PATH), nil)
		*specs = append(*specs, spec)
	}

	return pruned, nil
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

// given a version map (PV or MV), this function,
// 1. applies the pruning function on top of the version entries if they are PVs i.e. if the pruning function is passed in.
// 2. converts the entries into version deltas, strips the leading zeroes of delta values and composes the PV or MV for the target doc.
func VersionMapToDeltasBytes(vMap hlv.VersionsMap, body []byte, pos int, pruneFunction *base.PruningFunc) (int, bool) {
	startPos := pos
	first := true
	pruned := false

	if pruneFunction != nil {
		// prune PVs if possible
		for key, cas := range vMap {
			if !((*pruneFunction)(cas)) {
				continue
			}
			// Pruned entry
			delete(vMap, key)
			pruned = true
		}
	}

	// deltas need to be recomputed from the non-pruned versions
	deltas := vMap.VersionsDeltas()
	firstEntry := true
	for _, delta := range deltas {
		key := delta.GetSource()
		ver := delta.GetVersion()
		var value []byte
		if firstEntry {
			value = base.Uint64ToHexLittleEndian(ver)
			firstEntry = false
		} else {
			value = base.Uint64ToHexLittleEndianAndStrip0s(ver)
		}
		body, pos = base.WriteJsonRawMsg(body, []byte(key), pos, base.WriteJsonKey, len(key), first /*firstKey*/)
		body, pos = base.WriteJsonRawMsg(body, value, pos, base.WriteJsonValue, len(value), false /*firstKey*/)
		first = false
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

func xattrVVtoDeltas(vvBytes []byte) (hlv.VersionsMap, error) {
	vv := make(hlv.VersionsMap)

	if len(vvBytes) == 0 {
		return vv, nil
	}

	it, err := base.NewCCRXattrFieldIterator(vvBytes)
	if err != nil {
		return nil, err
	}
	var lastEntryVersion uint64
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return nil, err
		}
		src := hlv.DocumentSourceId(k)
		ver, err := base.HexLittleEndianToUint64(v)
		if err != nil {
			return nil, err
		}

		lastEntryVersion = ver + lastEntryVersion
		vv[src] = lastEntryVersion
	}

	return vv, nil
}

// Called when target has a smaller CAS but its xattrs dominates
// This routine receives the target metadata and returns an updated pv and mv to be used to send subdoc_multi_mutation to source.
// Target with smaller CAS can only dominate source if source document is a merged document.
func (meta *CRMetadata) UpdateMetaForSetBack() (pvBytes, mvBytes []byte, err error) {
	pv := meta.hlv.GetPV()

	mv := meta.hlv.GetMV()
	mvPos := 0
	if len(mv) > 0 {
		mvlen := 0
		for src := range mv {
			mvlen = mvlen + len(src) + base.MaxHexCASLength + base.QuotesAndSepLenForVVEntry
		}
		mvlen = mvlen + 2 // { and }
		// TODO(MB-41808): data pool
		mvBytes = make([]byte, mvlen)
		mvDeltas := mv.VersionsDeltas()
		firstKey := true
		for _, delta := range mvDeltas {
			src := delta.GetSource()
			ver := delta.GetVersion()
			value := base.Uint64ToHexLittleEndianAndStrip0s(ver)
			mvBytes, mvPos = base.WriteJsonRawMsg(mvBytes, []byte(src), mvPos, base.WriteJsonKey, len(src), firstKey /*firstKey*/)
			mvBytes, mvPos = base.WriteJsonRawMsg(mvBytes, value, mvPos, base.WriteJsonValue, len(value), false /*firstKey*/)
			firstKey = false
		}
		mvBytes[mvPos] = '}'
		mvPos++
	} else {
		// If there is no mv, then cv and document.CAS represent mutation events. It needs to be in pv
		source := meta.hlv.GetCvSrc()
		version := meta.hlv.GetCvVer()

		if pv == nil {
			pv = hlv.VersionsMap{}
		}
		pv[source] = version
	}

	pvPos := 0
	if len(pv) > 0 {
		pvlen := 0
		for src := range pv {
			pvlen = pvlen + len(src) + base.MaxHexCASLength + base.QuotesAndSepLenForVVEntry
		}
		// We may need to add cv and document CAS to it also
		pvlen = pvlen + len(meta.hlv.GetCvSrc()) + (base.MaxHexCASLength + base.QuotesAndSepLenForVVEntry)
		pvlen = pvlen + len(base.EmptyJsonObject) // { and }

		// TODO(MB-41808): data pool
		pvBytes = make([]byte, pvlen)
		pvDeltas := pv.VersionsDeltas()
		firstKey := true
		for _, delta := range pvDeltas {
			src := delta.GetSource()
			ver := delta.GetVersion()
			value := base.Uint64ToHexLittleEndianAndStrip0s(ver)
			pvBytes, pvPos = base.WriteJsonRawMsg(pvBytes, []byte(src), pvPos, base.WriteJsonKey, len(src), firstKey /*firstKey*/)
			pvBytes, pvPos = base.WriteJsonRawMsg(pvBytes, value, pvPos, base.WriteJsonValue, len(value), false /*firstKey*/)
			firstKey = false
		}
		pvBytes[pvPos] = '}'
		pvPos++
	}

	return pvBytes[:pvPos], mvBytes[:mvPos], nil

}

func (source *CRMetadata) Diff(target *CRMetadata, sourcePruningFunc, targetPruningFunc base.PruningFunc) (bool, error) {
	if source.docMeta.Opcode != target.docMeta.Opcode {
		return false, nil
	}
	if source.docMeta.Opcode == gomemcached.UPR_MUTATION {
		if source.docMeta.RevSeq != target.docMeta.RevSeq || source.docMeta.Cas != target.docMeta.Cas || source.docMeta.Flags != target.docMeta.Flags ||
			(source.docMeta.DataType&base.JSONDataType != target.docMeta.DataType&base.JSONDataType) || (source.docMeta.DataType&base.XattrDataType != target.docMeta.DataType&base.XattrDataType) {
			return false, nil
		}
		same, err := source.hlv.SameAs(target.hlv, sourcePruningFunc, targetPruningFunc)
		if err != nil {
			return false, err
		}
		return same, nil
	}
	return true, nil
}

func ParseHlvFields(cas uint64, xattr []byte) (cvCas uint64, src hlv.DocumentSourceId, cvVer uint64, pvMap, mvMap hlv.VersionsMap, err error) {
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
	pvMap, err1 = xattrVVtoDeltas(pv)
	if err1 != nil {
		err = fmt.Errorf("failed to convert pv '%s' to deltas, error: %v", pv, err1)
		return
	}
	mvMap, err1 = xattrVVtoDeltas(mv)
	if err1 != nil {
		err = fmt.Errorf("failed to convert mv '%s' to map, error: %v", mv, err1)
		return
	}
	return
}

func getHlvFromMCResponse(lookupResp *base.SubdocLookupResponse) (cas, cvCas uint64, cvSrc hlv.DocumentSourceId, cvVer uint64,
	pvMap, mvMap hlv.VersionsMap, importCas uint64, pRev uint64, err error) {

	cas = lookupResp.Resp.Cas
	xattr, err1 := lookupResp.ResponseForAPath(base.XATTR_HLV)
	if err1 != nil {
		err = fmt.Errorf("failed to find subdoc_lookup result for path %s for document %s%q%s, error: %v", base.XATTR_HLV, base.UdTagBegin, lookupResp.Resp.Key, base.UdTagEnd, err1)
		return
	}
	if xattr != nil && !base.Equals(xattr, base.EmptyJsonObject) {
		cvCas, cvSrc, cvVer, pvMap, mvMap, err1 = ParseHlvFields(cas, xattr)
		if err1 != nil {
			err = fmt.Errorf("failed to parse HLV fields for document %s%q%s, error: %v", base.UdTagBegin, lookupResp.Resp.Key, base.UdTagEnd, err1)
			return
		}
	}

	// It is ok to not find _mou.importCAS or _mou.pRev, since we may not be getting it if enableCrossClusterVersioning is not on, or target is not an import mutation.
	xattr, err1 = lookupResp.ResponseForAPath(XATTR_IMPORTCAS)
	xattrLen := len(xattr)
	if err1 == nil && xattrLen == base.MaxHexCASLength {
		// Strip the start/end quotes to get the importCas value
		importCas, err = base.HexLittleEndianToUint64(xattr[1 : xattrLen-1])
	}

	xattr, err1 = lookupResp.ResponseForAPath(XATTR_PREVIOUSREV)
	xattrLen = len(xattr)
	if err1 == nil && xattrLen >= base.MinRevIdLengthWithQuotes {
		// Strip the start/end quotes to get the pRev value
		pRev, err = strconv.ParseUint(string(xattr[1:xattrLen-1]), 10, 64)
	}
	return
}

func GetImportCasAndPrevFromMou(mou []byte) (newMou []byte, atleastOneLeft bool, importCas uint64, pRev uint64, err error) {
	if base.Equals(mou, base.EmptyJsonObject) {
		return
	}
	var newMouLen int
	// TODO: MB-61748 - can use datapool + new pool for removedFromMou
	newMou = make([]byte, len(mou))
	removedFromMou := make(map[string][]byte)
	newMouLen, _, atleastOneLeft, err = gojsonsm.MatchAndRemoveItemsFromJsonObject(mou, base.MouXattrValuesForCR, newMou, removedFromMou)
	if err != nil {
		return
	}
	newMou = newMou[:newMouLen]

	xattrImportCas, foundImportCas := removedFromMou[base.IMPORTCAS]
	xattrPRev, foundPRev := removedFromMou[base.PREVIOUSREV]
	xattrImportCASLen := len(xattrImportCas)
	xattrPRevLen := len(xattrPRev)

	if foundImportCas && xattrImportCASLen == base.MaxHexCASLength {
		// Remove the start/end quotes before converting it to uint64
		importCas, err = base.HexLittleEndianToUint64(xattrImportCas[1 : xattrImportCASLen-1])
		if err != nil {
			return
		}
	}
	if foundPRev && xattrPRevLen >= base.MinRevIdLengthWithQuotes {
		// Remove the start/end quotes before converting it to uint64
		pRev, err = strconv.ParseUint(string(xattrPRev[1:xattrPRevLen-1]), 10, 64)
		if err != nil {
			return
		}
	}
	return
}

// This will find the custom CR XATTR from the req body, including HLV and _importCas
func getHlvFromMCRequest(wrappedReq *base.WrappedMCRequest, uncompressFunc base.UncompressFunc) (cas, cvCas uint64, cvSrc hlv.DocumentSourceId, cvVer uint64, pvMap, mvMap hlv.VersionsMap, importCas uint64, pRev uint64, err error) {

	req := wrappedReq.Req
	cas = binary.BigEndian.Uint64(req.Extras[16:24])
	if req.DataType&mcc.XattrDataType == 0 {
		return
	}
	if err = uncompressFunc(wrappedReq); err != nil {
		return
	}
	body := req.Body
	xattrIter, err := base.NewXattrIterator(body)
	if err != nil {
		return
	}
	var key, value, newMou []byte
	var xattrHlv []byte
	var err1 error
	var atleastOneLeft bool

	for xattrIter.HasNext() {
		key, value, err = xattrIter.Next()
		if err != nil {
			return
		}
		if base.Equals(key, base.XATTR_HLV) && !base.Equals(value, base.EmptyJsonObject) {
			xattrHlv = value
		}
		if base.Equals(key, base.XATTR_MOU) {
			newMou, atleastOneLeft, importCas, pRev, err = GetImportCasAndPrevFromMou(value)
			if err != nil {
				return
			}

			if atleastOneLeft {
				wrappedReq.MouAfterProcessing = newMou
			}
		}
	}

	if xattrHlv == nil {
		// Source does not have HLV XATTR
		return
	}
	// Found HLV XATTR. Now find the fields
	cvCas, cvSrc, cvVer, pvMap, mvMap, err1 = ParseHlvFields(cas, xattrHlv)
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
		cvCas, err = base.HexLittleEndianToUint64(cvCasHex)
		if err != nil {
			return nil, err
		}
	}
	pvMap, err := xattrVVtoDeltas(pv)
	if err != nil {
		return nil, err
	}
	mvMap, err := xattrVVtoDeltas(mv)
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
