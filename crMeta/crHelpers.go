/*
Copyright 2024-Present Couchbase, Inc.

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

	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/hlv"
	"github.com/couchbaselabs/gojsonsm"
)

// Hlv CR related constants
const (
	HLV_CVCAS_FIELD = "cvCas" // This stores the version of the HLV
	// These are the fields in the HLV
	HLV_SRC_FIELD = "src" // src and ver combined is the cv in the design
	HLV_VER_FIELD = "ver" //
	HLV_MV_FIELD  = "mv"  // the MV field in _vv
	HLV_PV_FIELD  = "pv"  // The PV field in _vv

	XATTR_CVCAS_PATH = base.XATTR_HLV + base.PERIOD + HLV_CVCAS_FIELD
	XATTR_SRC_PATH   = base.XATTR_HLV + base.PERIOD + HLV_SRC_FIELD
	XATTR_VER_PATH   = base.XATTR_HLV + base.PERIOD + HLV_VER_FIELD
	XATTR_MV_PATH    = base.XATTR_HLV + base.PERIOD + HLV_MV_FIELD
	XATTR_PV_PATH    = base.XATTR_HLV + base.PERIOD + HLV_PV_FIELD

	XATTR_IMPORTCAS   = base.XATTR_IMPORTCAS
	XATTR_PREVIOUSREV = base.XATTR_PREVIOUSREV

	// separator between source and version in version deltas entry
	// i.e <version>@<source>
	HLV_SEPARATOR = '@'
)

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
	// We need to update if CAS has changed from cvCas
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

	var pruned bool
	*pos, pruned, err = ConstructHlv(body, *pos, meta, pruningWindow)
	if err != nil {
		return *pos, pruned, err
	}

	*pos, err = xattrComposer.CommitRawKVPair()
	return *pos, pruned, err
}

// given a version map (PV or MV), this function,
// 1. applies the pruning function on top of the version entries if they are PVs i.e. if the pruning function is passed in.
// 2. converts the entries into version deltas, strips the leading zeroes of delta values and composes the PV or MV for the target doc.
// 3. PVs and MVs will be of the format of JSON arrays with individual string entries of the form "<version>@<source>".
func VersionMapToDeltasBytes(vMap hlv.VersionsMap, body []byte, pos int, pruneFunction *base.PruningFunc) (int, bool, error) {
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
	for _, delta := range deltas {
		key := delta.GetSource()
		ver := delta.GetVersion()
		var value []byte
		if first {
			value = base.Uint64ToHexLittleEndian(ver)
		} else {
			value = base.Uint64ToHexLittleEndianAndStrip0s(ver)
		}

		hlvEntry, err := ComposeHLVEntry(key, value)
		if err != nil {
			return startPos, pruned, err
		}

		body, pos = base.WriteJsonRawMsg(body, hlvEntry, pos, base.WriteJsonArrayEntry, len(hlvEntry), first /*firstKey*/)
		first = false
	}

	if first {
		// We haven't added anything
		return startPos, pruned, nil
	}
	body[pos] = ']'
	pos++
	return pos, pruned, nil
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

	it, err := base.NewArrayXattrFieldIterator(vvBytes)
	if err != nil {
		return nil, err
	}
	var lastEntryVersion uint64
	for it.HasNext() {
		v, err := it.Next()
		if err != nil {
			return nil, err
		}

		if len(v) <= 2 {
			err = fmt.Errorf("invalid vv entry=%v in vvBytes=%v", v, vvBytes)
			return nil, err
		}

		source, version, err := ParseOneVersionDeltaEntry(v[1 : len(v)-1])
		if err != nil {
			return nil, err
		}

		src := hlv.DocumentSourceId(source)
		ver, err := base.HexLittleEndianToUint64WO0x(version)
		if err != nil {
			return nil, err
		}

		lastEntryVersion = ver + lastEntryVersion
		vv[src] = lastEntryVersion
	}

	return vv, nil
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

	// It is ok to not find _mou.cas (importCas) or _mou.pRev, since we may not be getting it if enableCrossClusterVersioning is not on, or target is not an import mutation.
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

// This will find the custom CR XATTR from the req body, including HLV and importCas (_mou.cas)
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

// Decide if we need to use subdoc op instead of meta op
func setSubdocOpIfNeeded(sourceMeta, targetMeta *CRMetadata, req *base.WrappedMCRequest) {
	if req.HLVModeOptions.PreserveSync &&
		targetMeta.IsImportMutation() && sourceMeta.actualCas < targetMeta.actualCas {
		// This is the case when target CAS will rollback if source wins
		// So use subdoc command in this case instead of *_WITH_META commands
		req.SetSubdocOp()
		req.SubdocCmdOptions.TargetHasPv = targetMeta.hadPv
		req.SubdocCmdOptions.TargetHasMv = targetMeta.hadMv
		req.SubdocCmdOptions.TargetHasMou = targetMeta.hadMou
		req.SubdocCmdOptions.TargetDocIsTombstone = targetMeta.docMeta.Deletion
	}
}

// source should be of non zero length
// version should be atleast of length >= 3
// returns version@source, after stripping 0x of the version,
// which will be one entry of a PV or MV.
func ComposeHLVEntry(source hlv.DocumentSourceId, version []byte) ([]byte, error) {
	if len(source) == 0 {
		return nil, fmt.Errorf("invalid source=%v", source)
	}
	if len(version) < 3 {
		return nil, fmt.Errorf("invalid version=%v", version)
	}

	hlvEntryLen := len(version) + 1 + len(source)
	hlvEntry := make([]byte, hlvEntryLen)
	idx := 0
	// copy the version, without the 0x
	n := copy(hlvEntry[idx:], version[2:])
	idx += n
	// copy the separator
	hlvEntry[idx] = HLV_SEPARATOR
	idx++
	// copy the source
	for i := 0; i < len(source); i++ {
		hlvEntry[idx+i] = source[i]
	}
	idx += len(source)
	return hlvEntry[:idx], nil
}

// input is expected to be of the format <version>@<source>
// returns <source> and <version>
func ParseOneVersionDeltaEntry(entry []byte) (source, version []byte, err error) {
	sep := -1
	for idx := 0; idx < len(entry); idx++ {
		if entry[idx] == HLV_SEPARATOR {
			sep = idx
			break
		}
	}

	if sep == -1 {
		err = fmt.Errorf("invalid version delta entry=%s", entry)
		return
	}

	// sep points to the HLV_SEPARATOR now
	version = entry[:sep]
	source = entry[sep+1:]
	err = nil
	return
}

// constructs hlv content and writes it to "body" from "pos" index.
// Increments pos and returns the last position.
func ConstructHlv(body []byte, pos int, meta *CRMetadata, pruningWindow time.Duration) (int, bool, error) {
	var err error
	pruneFunc := base.GetHLVPruneFunction(meta.GetDocumentMetadata().Cas, pruningWindow)
	pos = formatCv(meta, body, pos)
	// Format MV
	mv := meta.GetHLV().GetMV()
	if len(mv) > 0 {
		// This is not the first since we have cv before this
		body, pos = base.WriteJsonRawMsg(body, []byte(HLV_MV_FIELD), pos, base.WriteJsonKey, len([]byte(HLV_MV_FIELD)), false)
		pos, _, err = VersionMapToDeltasBytes(mv, body, pos, nil)
		if err != nil {
			return pos, false, err
		}
	}
	// Format PV
	pv := meta.GetHLV().GetPV()
	var pruned bool
	if len(pv) > 0 {
		startPos := pos
		body, pos = base.WriteJsonRawMsg(body, []byte(HLV_PV_FIELD), pos, base.WriteJsonKey, len([]byte(HLV_PV_FIELD)), false)
		afterKeyPos := pos
		pos, pruned, err = VersionMapToDeltasBytes(pv, body, pos, &pruneFunc)
		if err != nil {
			return pos, pruned, err
		}
		if pos == afterKeyPos {
			// Did not add PV, need to back off and remove the PV key
			pos = startPos
		}
	}
	body[pos] = '}'
	pos++

	return pos, pruned, nil
}
