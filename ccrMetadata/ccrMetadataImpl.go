/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package ccrMetadata

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime/debug"
	"time"

	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/hlv"
)

type CcrMetadata struct {
	source []byte // The source bucket that XDCR got this document from
	cas    uint64 // The Cas in the document metadata
	src    []byte // _vv.src in XATTR
	ver    uint64 // _vv.ver in XATTR
	pv     []byte // _vv.pv in XATTR
	mv     []byte // _vv.mv in XATTR
	// This is the HLV generated from the fields above
	hlv *hlv.HLV
}

func NewCustomCRMeta(source []byte, cas uint64, cvSrc, verHex, pv, mv []byte) (*CcrMetadata, error) {
	var crMeta CcrMetadata
	if len(verHex) == 0 {
		crMeta = CcrMetadata{
			source: source,
			cas:    cas,
			src:    cvSrc,
			ver:    0,
			pv:     pv,
			mv:     mv,
		}
	} else {
		ver, err := base.HexLittleEndianToUint64(verHex)
		if err != nil {
			return nil, err
		}
		if ver > cas {
			// ver is initially the same as cas. Cas may increase later. So ver should never be greater than cas
			return nil, fmt.Errorf("ver>cas, ver=%v,cas=%v,cvHex=%s,pv=%s,mv=%s", ver, cas, verHex, pv, mv)
		}
		crMeta = CcrMetadata{
			source: source,
			cas:    cas,
			src:    cvSrc,
			ver:    ver,
			pv:     pv,
			mv:     mv,
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
	hlv, err := hlv.NewHLV(string(source), cas, string(cvSrc), crMeta.ver, pvMap, mvMap)
	if err != nil {
		return nil, err
	}
	crMeta.hlv = hlv
	if cas == 0 {
		debug.PrintStack()
	}
	return &crMeta, err
}

func (meta *CcrMetadata) GetMutationCas() uint64 {
	return meta.cas
}

func (meta *CcrMetadata) GetCvVer() uint64 {
	return meta.ver
}

func (meta *CcrMetadata) GetCvSrc() []byte {
	return meta.src
}

func (meta *CcrMetadata) GetMv() []byte {
	return meta.mv
}

func (meta *CcrMetadata) GetPv() []byte {
	return meta.pv
}

// Returns the id of the bucket that last updated this document
func (meta *CcrMetadata) GetMutationSource() []byte {
	if meta.cas > meta.ver {
		// New change in sending cluster so that's the id
		return meta.source
	} else {
		return meta.src
	}
}

func (meta *CcrMetadata) IsMergedDoc() bool {
	if meta.mv != nil && meta.cas == meta.ver {
		return true
	} else {
		return false
	}
}

func (meta *CcrMetadata) DetectConflict(other *CcrMetadata) (ConflictDetectionResult, error) {
	res, err := meta.hlv.DetectConflict(other.hlv)
	if err != nil {
		return NoResult, err
	}
	switch res {
	case hlv.Win:
		return Win, nil
	case hlv.Lose:
		if meta.cas > other.cas {
			// Target HLV wins but its CAS is smaller. Replication only happens on the larger CAS side
			// We need to set it back to source here so data will converge.
			return LoseWithLargerCas, nil
		}
		return Lose, nil
	case hlv.Conflict:
		return Conflict, nil
	case hlv.Equal:
		if meta.cas > other.cas {
			// Even though HLV is the same, source CAS is larger, so it still wins
			return Win, nil
		} else {
			return Equal, nil
		}
	default:
		return NoResult, fmt.Errorf("Back result from hlv.DetectConflict: %v", res)
	}
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
		src := string(k)
		ver, err := base.Base64ToUint64(v)
		if err != nil {
			return nil, err
		}
		res[src] = ver
	}
	return res, nil
}

// {"src":...;"ver":...
func (meta *CcrMetadata) formatCv(body []byte, pos int) int {
	body, pos = base.WriteJsonRawMsg(body, []byte(HLV_SRC_FIELD), pos, base.WriteJsonKey, len(HLV_SRC_FIELD), true /*firstKey*/)
	if meta.cas <= meta.ver {
		// Since there is no new update, id/ver stays the same
		body, pos = base.WriteJsonRawMsg(body, meta.src, pos, base.WriteJsonValue, len(meta.src), false /*firstKey*/)
		cvHex := base.Uint64ToHexLittleEndian(meta.ver)
		body, pos = base.WriteJsonRawMsg(body, []byte(HLV_VER_FIELD), pos, base.WriteJsonKey, len(HLV_VER_FIELD), false /*firstKey*/)
		body, pos = base.WriteJsonRawMsg(body, cvHex, pos, base.WriteJsonValue, len(cvHex), false /*firstKey*/)
	} else {
		// Set id to sender
		body, pos = base.WriteJsonRawMsg(body, meta.source, pos, base.WriteJsonValue, len(meta.source), false /*firstKey*/)
		// Set ver to the new CAS
		cvHex := base.Uint64ToHexLittleEndian(meta.cas)
		body, pos = base.WriteJsonRawMsg(body, []byte(HLV_VER_FIELD), pos, base.WriteJsonKey, len(HLV_VER_FIELD), false /*firstKey*/)
		body, pos = base.WriteJsonRawMsg(body, cvHex, pos, base.WriteJsonValue, len(cvHex), false /*firstKey*/)
	}
	return pos
}

// If key:value exceeds pruning window, it will not be added
// No pruning if pruningWindow == 0
// If an entry is pruned, its CAS will be returned.
func (meta *CcrMetadata) addEntryToBody(key, value, body []byte, pos int, empty bool, pruningWindow time.Duration) (uint64 /*cas*/, int /*pos*/, bool /*empty*/, error) {
	if pruningWindow > 0 {
		// Convert value to CAS to check for pruning
		cas, err := base.Base64ToUint64(value)
		if err != nil {
			return 0, pos, empty, err
		}
		if base.CasDuration(cas, meta.cas) >= pruningWindow {
			return cas, pos, empty, nil
		}
	}
	body, pos = base.WriteJsonRawMsg(body, key, pos, base.WriteJsonKey, len(key), empty /*firstKey*/)
	body, pos = base.WriteJsonRawMsg(body, value, pos, base.WriteJsonValue, len(value), false /*firstKey*/)
	return 0, pos, false, nil
}

func (meta *CcrMetadata) formatPv(body []byte, pos int, pruningWindow time.Duration) (int, error) {
	empty := true
	startPos := pos
	body, pos = base.WriteJsonRawMsg(body, []byte(HLV_PV_FIELD), pos, base.WriteJsonKey, len(HLV_PV_FIELD), false /*firstKey*/)
	if meta.cas > meta.ver {
		// New change, move mv into pv
		if meta.mv != nil {
			it, err := base.NewCCRXattrFieldIterator(meta.mv)
			if err != nil {
				return startPos, err
			}
			for it.HasNext() {
				key, value, err := it.Next()
				if err != nil {
					return startPos, err
				}
				// Use 0 for pruning window since items from MV will not be pruned
				_, pos, empty, err = meta.addEntryToBody(key, value, body, pos, empty, 0)
			}
		} else if len(meta.src) > 0 {
			// New change but no MV, put id/ver into PV
			var err error
			cvBase64 := base.Uint64ToBase64(meta.ver)
			_, pos, empty, err = meta.addEntryToBody(meta.src, cvBase64, body, pos, empty, 0)
			if err != nil {
				return startPos, err
			}
		}
	}
	if meta.pv != nil {
		var savedKey, savedVal []byte
		var maxCas uint64 = 0
		it, err := base.NewCCRXattrFieldIterator(meta.pv)
		if err != nil {
			return startPos, err
		}
		for it.HasNext() {
			key, value, err := it.Next()
			if err != nil {
				return startPos, err
			}
			// The document source will be set as id/ver, so it doesn't need to be in PV
			if bytes.Equal(meta.GetMutationSource(), key) {
				continue
			}
			var cas uint64
			cas, pos, empty, err = meta.addEntryToBody(key, value, body, pos, empty, pruningWindow)
			if err != nil {
				return startPos, err
			}
			// No value has been written to body and pruning is currently taking place
			if empty && cas > maxCas {
				maxCas = cas
				savedKey = key
				savedVal = value
			}
		}
		if empty && !meta.IsMergedDoc() && meta.pv != nil && savedKey != nil && savedVal != nil {
			// We have to have one PV entry for future CR
			_, pos, empty, err = meta.addEntryToBody(savedKey, savedVal, body, pos, empty, 0)
		}
	}
	if empty {
		return startPos, nil
	}
	body[pos] = '}'
	pos++
	return pos, nil
}

// This routine construct XATTR _xdcr{...} based on meta. The constructed XATTRs includes updates for
// new change (meta.cas > meta.ver) and pruning in PV
func (meta *CcrMetadata) ConstructCustomCRXattrForSetMeta(pruningWindow time.Duration, xattrComposer *base.XattrComposer) (int, error) {
	err := xattrComposer.StartRawMode()
	if err != nil {
		return -1, err
	}

	err = xattrComposer.RawWriteKey([]byte(XATTR_HLV))
	if err != nil {
		return -1, err
	}

	body, pos, err := xattrComposer.RawHijackValue()
	if err != nil {
		return -1, err
	}

	*pos = meta.formatCv(body, *pos)
	if meta.cas <= meta.ver && meta.mv != nil {
		// Since there is no new change, mv stays the same
		body, *pos = base.WriteJsonRawMsg(body, []byte(HLV_MV_FIELD), *pos, base.WriteJsonKey, len(HLV_MV_FIELD), false /*firstKey*/)
		// copy the mv since it is already in json format
		copy(body[*pos:], meta.mv)
		*pos = *pos + len(meta.mv)
	}
	*pos, err = meta.formatPv(body, *pos, pruningWindow)
	if err != nil {
		return -1, err
	}
	body[*pos] = '}'
	*pos++

	return xattrComposer.CommitRawKVPair()
}

// Called when target has a smaller CAS but its xattrs dominates
// This routine receives the target CcrMetadata and returns an updated pv and mv to be used to send subdoc_multi_mutation to source.
// Target with smaller CAS can only dominate source if source document is a merged document.
func (meta *CcrMetadata) UpdateMetaForSetBack() (pvBytes, mvBytes []byte, err error) {
	pv := meta.hlv.GetPV()
	pvPos := 0
	if len(pv) > 0 {
		pvlen := 0
		for src, _ := range pv {
			pvlen = pvlen + len(src) + base.MaxBase64CASLength + 6 // quotes and sepeartors
		}
		// We may need to add cv and document CAS to it also
		pvlen = pvlen + len(meta.hlv.GetCvSrc()) + len(meta.source) + 2*(base.MaxBase64CASLength+6)
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
		casHex := base.Uint64ToBase64(meta.cas)
		version := base.Uint64ToBase64(meta.hlv.GetCvVer())
		pvBytes, pvPos = base.WriteJsonRawMsg(pvBytes, []byte(source), pvPos, base.WriteJsonKey, len(source), first /*firstKey*/)
		pvBytes, pvPos = base.WriteJsonRawMsg(pvBytes, version, pvPos, base.WriteJsonValue, len(version), false /*firstKey*/)
		pvBytes, pvPos = base.WriteJsonRawMsg(pvBytes, []byte(meta.source), pvPos, base.WriteJsonKey, len(meta.source), false /*firstKey*/)
		pvBytes, pvPos = base.WriteJsonRawMsg(pvBytes, casHex, pvPos, base.WriteJsonValue, len(casHex), false /*firstKey*/)
		pvBytes[pvPos] = '}'
		pvPos++
	}
	return pvBytes[:pvPos], mvBytes[:mvPos], nil

}

// This routine combines source and target meta and puts the new MV/PCAS in the mergedMvSlice/mergedPcasSlice
//
//	It finds the current versions (MV) of the two documents, combine them into new MV.
//	It finds the previous versions (PCAS) of the two documents, combine them into new PCAS
//	It then remove any duplicates and format them in the provided slices
func (meta *CcrMetadata) MergeCustomCRMetadata(other *CcrMetadata, mergedMvSlice, mergedPvSlice []byte, pruningWindow time.Duration) (mvlen int, pvlen int, err error) {
	mergedHLV, err := meta.hlv.Merge(other.hlv)
	if err != nil {
		return 0, 0, fmt.Errorf("hlv.Merge returned error %v for source %v and target %v", err, meta, other)
	}

	// Construct MV
	firstKey := true
	for key, cas := range mergedHLV.GetMV() {
		value := base.Uint64ToBase64(cas)
		mergedMvSlice, mvlen = base.WriteJsonRawMsg(mergedMvSlice, []byte(key), mvlen, base.WriteJsonKey, len(key), firstKey /*firstKey*/)
		mergedMvSlice, mvlen = base.WriteJsonRawMsg(mergedMvSlice, value, mvlen, base.WriteJsonValue, len(value), false /*firstKey*/)
		firstKey = false
	}
	if mvlen > 0 {
		// We have MV. Finish it with '}'
		mergedMvSlice[mvlen] = '}'
		mvlen++
	}
	// Construct PV
	firstKey = true
	for key, cas := range mergedHLV.GetPV() {
		value := base.Uint64ToBase64(cas)
		mergedPvSlice, pvlen = base.WriteJsonRawMsg(mergedPvSlice, []byte(key), pvlen, base.WriteJsonKey, len(key), firstKey /*firstKey*/)
		mergedPvSlice, pvlen = base.WriteJsonRawMsg(mergedPvSlice, value, pvlen, base.WriteJsonValue, len(value), false /*firstKey*/)
		firstKey = false
	}
	if pvlen > 0 {
		// We have PV. Finish it with '}
		mergedPvSlice[pvlen] = '}'
		pvlen++
	}
	return mvlen, pvlen, nil
}

func (meta *CcrMetadata) NeedUpdate(pruningWindow time.Duration) (bool, error) {
	// We need to update if CAS has changed from ver.CAS
	if meta.GetMutationCas() > meta.GetCvVer() {
		return true, nil
	}

	// The CAS has not changed. But do we need to prune PV?

	// No pruning if its setting is 0 or there is no pv to prune
	if pruningWindow == 0 || meta.pv == nil {
		return false, nil
	}
	it, err := base.NewCCRXattrFieldIterator(meta.pv)
	if err != nil {
		return false, err
	}
	for it.HasNext() {
		_, v, err := it.Next()
		if err != nil {
			return false, err
		}
		cas, err := base.Base64ToUint64(v)
		if err != nil {
			return false, err
		}
		// Check for pruning window
		if base.CasDuration(cas, meta.cas) >= pruningWindow {
			return true, nil
		}
	}
	return false, nil
}

func (meta *CcrMetadata) String() string {
	return fmt.Sprintf("source: %s, cas: %v, src: %s, ver: %v, pv: %s, mv: %s, hlv: %s",
		meta.source, meta.cas, meta.src, meta.ver, meta.pv, meta.mv, meta.hlv)
}

func MergedPvLength(sourceMeta, targetMeta *CcrMetadata) int {
	return len(sourceMeta.GetPv()) + len(targetMeta.GetPv()) + 2 + // maximum combined PV + 2 seperators ','
		len(sourceMeta.GetMv()) + len(targetMeta.GetMv()) + 2 + // maximum combined MV rolled into to PV + 2 seperators ','
		len(sourceMeta.GetCvSrc()) + 3 + // quotes and separators "...":
		len(targetMeta.GetCvSrc()) + 3 + // quotes and separators "...":
		2*(base.MaxBase64CASLength+3) + 2 // quotes and separators "...", plus '{' and '}'
}

func MergedMvLength(sourceMeta, targetMeta *CcrMetadata) int {
	return len(sourceMeta.GetMv()) + len(targetMeta.GetMv()) + 2 + // maximumn combined MV + 2 seperators ','
		len(sourceMeta.GetMutationSource()) + 3 + // quotes and separators "...":
		len(targetMeta.GetMutationSource()) + 3 + // quotes and separators "...":
		2*(base.MaxBase64CASLength+3) + 2 // quotes and separators "...", plus '{' and '}'
}

func findCustomCRXattrFields(xattr []byte) (src, ver, pv, mv []byte, err error) {
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
		if base.Equals(key, HLV_SRC_FIELD) {
			src = value
		} else if base.Equals(key, HLV_VER_FIELD) {
			ver = value
		} else if base.Equals(key, HLV_PV_FIELD) {
			pv = value
		} else if base.Equals(key, HLV_MV_FIELD) {
			mv = value
		}
	}
	return
}

func GetTargetCustomCRXattr(lookupResp *base.SubdocLookupResponse, targetId []byte) (crMeta *CcrMetadata, err error) {
	cas := lookupResp.Resp.Cas
	xattr, err := lookupResp.ResponseForAPath(XATTR_HLV)
	if err != nil {
		return &CcrMetadata{}, fmt.Errorf("error %v for %v", err, XATTR_HLV)
	}
	if xattr == nil {
		return NewCustomCRMeta(targetId, cas, nil, nil, nil, nil)
	}

	src, ver, pv, mv, err := findCustomCRXattrFields(xattr)
	if err != nil {
		return &CcrMetadata{}, err
	}
	return NewCustomCRMeta(targetId, cas, src, ver, pv, mv)
}

// This will find the custom CR XATTR from the req body
func GetSourceCustomCRXattr(req *mc.MCRequest, sourceId []byte) (crMeta *CcrMetadata, err error) {
	cas := binary.BigEndian.Uint64(req.Extras[16:24])
	if req.DataType&mcc.XattrDataType == 0 {
		return NewCustomCRMeta(sourceId, cas, nil, nil, nil, nil)
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
	for xattrIter.HasNext() {
		key, value, err = xattrIter.Next()
		if err != nil {
			return
		}
		if base.Equals(key, XATTR_HLV) {
			xattr = value
			break
		}
	}
	if xattr == nil {
		// Source does not have _xdcr XATTR
		return NewCustomCRMeta(sourceId, cas, nil, nil, nil, nil)
	}
	// Found _xdcr XATTR. Now find the fields
	src, ver, pv, mv, err := findCustomCRXattrFields(xattr)
	if err != nil {
		// TODO: MB-40143: Remove before CC shipping
		panic(err.Error())
	}
	return NewCustomCRMeta(sourceId, cas, src, ver, pv, mv)
}
