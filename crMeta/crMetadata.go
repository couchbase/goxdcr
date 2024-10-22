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
	"fmt"

	mc "github.com/couchbase/gomemcached"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/hlv"
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
// This results in a new mutation with new CAS. Mobile will set an XATTR importCAS (_mou.cas) to the same value as the new CAS
// using macro-expansion.
//
// The resulting import mutation has the following properties:
// 1. importCAS (_mou.cas) == document.CAS
// 2. cvCAS == pre-import document CAS
// We don't want import mutation to win over its pre-import predecessor. To achieve that,
// we will use pre-import CAS value for conflict resolution so docMeta.Cas will be set to this value.
// Pre-import revId is not saved. So if one of the mutation is import mutation, revId will not be used in LWW CR.
// revId CR cannot be supported either (unless mobile savs pre-import revId)
//
// If there is a new mutation after the import, the new mutation will have:
// - document.CAS > importCAS (_mou.cas)
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
	// It is import mutation if importCas (_mou.cas) == document.CAS. In that case, docMeta.Cas is the pre-import CAS value.
	isImport bool
	// This is the value parsed from the XATTR importCAS (_mou.cas).
	importCas uint64
	// if isImport is true, we will use this to store the actual Cas,
	// since we replace doc.Cas with pre-import Cas i.e. cvCas for conflict resolution
	actualCas uint64
}

func (m CRMetadata) String() string {
	return fmt.Sprintf("{docMeta=%s,hlv=%s,had={%v,%v,%v,%v},isImport=%v,importCas=%v,actualCas=%v}",
		m.docMeta, m.hlv, m.hadPv, m.hadMv, m.hadHlv, m.hadMou, m.isImport, m.importCas, m.actualCas)
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
		return nil, fmt.Errorf("cannot merge nil HLV")
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
			mvlen = mvlen + len(src) + base.MaxHexCASLength + base.QuotesAndSepLenForHLVEntry
		}
		mvlen = mvlen + 2 // { and }
		// TODO(MB-41808): data pool
		mvBytes = make([]byte, mvlen)
		mvDeltas := mv.VersionsDeltas()
		firstKey := true
		for _, delta := range mvDeltas {
			src := delta.GetSource()
			ver := delta.GetVersion()
			var value []byte
			if firstKey {
				value = base.Uint64ToHexLittleEndian(ver)
			} else {
				value = base.Uint64ToHexLittleEndianAndStrip0s(ver)
			}

			hlvEntry, err := ComposeHLVEntry(src, value)
			if err != nil {
				return pvBytes, mvBytes, err
			}

			mvBytes, mvPos = base.WriteJsonRawMsg(mvBytes, hlvEntry, mvPos, base.WriteJsonArrayEntry, len(hlvEntry), firstKey /*firstKey*/)
			firstKey = false
		}
		mvBytes[mvPos] = ']'
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
			pvlen = pvlen + len(src) + base.MaxHexCASLength + base.QuotesAndSepLenForHLVEntry
		}
		// We may need to add cv and document CAS to it also
		pvlen = pvlen + len(meta.hlv.GetCvSrc()) + (base.MaxHexCASLength + base.QuotesAndSepLenForHLVEntry)
		pvlen = pvlen + len(base.EmptyJsonObject) // { and }

		// TODO(MB-41808): data pool
		pvBytes = make([]byte, pvlen)
		pvDeltas := pv.VersionsDeltas()
		firstKey := true
		for _, delta := range pvDeltas {
			src := delta.GetSource()
			ver := delta.GetVersion()
			var value []byte
			if firstKey {
				value = base.Uint64ToHexLittleEndian(ver)
			} else {
				value = base.Uint64ToHexLittleEndianAndStrip0s(ver)
			}

			hlvEntry, err := ComposeHLVEntry(src, value)
			if err != nil {
				return pvBytes, mvBytes, err
			}

			pvBytes, pvPos = base.WriteJsonRawMsg(pvBytes, hlvEntry, pvPos, base.WriteJsonArrayEntry, len(hlvEntry), firstKey /*firstKey*/)
			firstKey = false
		}
		pvBytes[pvPos] = ']'
		pvPos++
	}

	return pvBytes[:pvPos], mvBytes[:mvPos], nil

}

func (source *CRMetadata) Diff(target *CRMetadata, sourcePruningFunc, targetPruningFunc base.PruningFunc) (bool, error) {
	if source.docMeta.Opcode != target.docMeta.Opcode {
		return false, nil
	}
	if source.docMeta.Opcode == mc.UPR_MUTATION {
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
