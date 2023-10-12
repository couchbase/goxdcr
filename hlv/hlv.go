/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package hlv

import (
	"fmt"

	"github.com/couchbase/goxdcr/base"
)

type ConflictDetectionResult uint32

const (
	Win      ConflictDetectionResult = iota
	Lose     ConflictDetectionResult = iota
	Conflict ConflictDetectionResult = iota
	Equal    ConflictDetectionResult = iota
	Error    ConflictDetectionResult = iota
)

func (cr ConflictDetectionResult) String() string {
	switch cr {
	case Win:
		return "Win"
	case Lose:
		return "Lose"
	case Conflict:
		return "Conflict"
	case Equal:
		return "Equal"
	}
	return "Unknown"
}

type DocumentSourceId string

func UUIDtoDocumentSource(uuid string) (DocumentSourceId, error) {
	ret, err := base.HexToBase64(uuid)
	return DocumentSourceId(ret), err
}

type VersionsMap map[DocumentSourceId]uint64

// Add the other {src,ver} to vm if it is not there or if other has higher ver
func (vm VersionsMap) Add(other VersionsMap) {
	for src, ver := range other {
		v, ok := vm[src]
		if !ok || ver > v {
			vm[src] = ver
		}
	}
}

type HLV struct {
	cvCAS   uint64
	cv      currentVersion
	pv      VersionsMap
	mv      VersionsMap
	Updated bool // Whether this HLV is different from what's stored in the source XATTR
	// Whether there was a PV or MV in the input document body. This is needed when we merge and may have to remove that path
}

// This is the mutation current version
type currentVersion struct {
	source  DocumentSourceId
	version uint64
}

// This creates a new updated HLV based on XATTR and document.CAS
// Input:
// - cas: this is the document CAS
// - source: this is the bucket XDCR get this mutation from, either through DCP or subdoc_get
// - {src,ver} these combined is the input cv
// If cas > ver, then the document has been mutated since replication.
// - If there is no mv, the input cv will be rolled into pv
// - If there is an mv, the mv will be rolled into pv
func NewHLV(source DocumentSourceId, cas uint64, cvCas uint64, src DocumentSourceId, ver uint64, pv, mv VersionsMap) (*HLV, error) {
	hlv := HLV{
		cvCAS: cas,
	}
	if cas < cvCas {
		// cvCas is initially the same as cas. Cas may increase later. So cvCas should never be greater than cas
		return nil, fmt.Errorf("cas < cvCas, cas=%v,cvCas=%v,ver=%v,src=%v,pv=%s,mv=%s", cas, cvCas, ver, src, pv, mv)
	} else if cas == cvCas {
		// The HLV did not change.
		hlv.cvCAS = cvCas
		hlv.cv = currentVersion{src, ver}
		hlv.pv = pv
		hlv.mv = mv
		hlv.Updated = false
	} else {
		// CAS has Updated. This new CAS is the current version. Everything else is rolled into pv
		hlv.Updated = true
		hlv.cvCAS = cas
		hlv.cv = currentVersion{source, cas}
		hlv.pv = pv
		if len(mv) > 0 {
			// Add mv to pv, no need to add cv to history because it represents a merge event
			for k, v := range mv {
				hlv.pv[k] = v
			}
		} else if len(src) > 0 {
			// Add cv to pv only if mv does not exist.
			// When there is no mv, cv represents a mutation and needs to be added to vrsion history
			hlv.pv[src] = ver
		}
		hlv.mv = nil
	}
	// Make sure the cv is not repeated in pv
	delete(hlv.pv, hlv.cv.source)
	return &hlv, nil
}

func (h *HLV) GetMV() VersionsMap {
	if h == nil {
		return nil
	}
	return h.mv
}

func (h *HLV) GetPV() VersionsMap {
	if h == nil {
		return nil
	}
	return h.pv
}

func (h *HLV) GetCvCas() uint64 {
	if h == nil {
		return 0
	}
	return h.cvCAS
}
func (h *HLV) GetCvSrc() DocumentSourceId {
	if h == nil {
		return ""
	}
	return h.cv.source
}

func (h *HLV) GetCvVer() uint64 {
	if h == nil {
		return 0
	}
	return h.cv.version
}

func (h *HLV) DetectConflict(other *HLV) (ConflictDetectionResult, error) {
	if h == nil || other == nil {
		return Error, fmt.Errorf("cannot detect conflict with nil HLV")
	}
	c1 := h.contain(other)
	c2 := other.contain(h)
	if c1 && c2 {
		return Equal, nil
	} else if c1 {
		return Win, nil
	} else if c2 {
		return Lose, nil
	}
	return Conflict, nil
}

func (h *HLV) contain(other *HLV) bool {
	if other == nil {
		return true
	}
	if h.containVersion(other.cv.source, other.cv.version) {
		// cv is the most recent change. If it contains cv, then it contains the other HLV
		return true
	}
	if len(other.mv) > 0 {
		// For merged document, as long as it contains each version in the merge, then it contains the whole
		for k, v := range other.mv {
			if h.containVersion(k, v) == false {
				return false
			}
		}
		return true
	}
	return false
}

// Generate a merged HLV from two HLVs
// Only pv and mv values are filled. cvCAS and cv.ver will be filled by macro-expansion when it is sent to KV.
// cv.src will be set depending on the bucket this is sent to.
func (h *HLV) Merge(other *HLV) (*HLV, error) {
	if h == nil || other == nil {
		return nil, fmt.Errorf("cannot merge nil HLV")
	}
	res := HLV{
		cvCAS: 0,
		cv:    currentVersion{},
		pv:    make(VersionsMap),
		mv:    make(VersionsMap),
	}
	// Combine pv
	res.pv.Add(h.pv)
	res.pv.Add(other.pv)
	// Combine mv or cv to get the new mv
	if len(h.mv) > 0 {
		res.mv.Add(h.mv)
	} else {
		if len(h.cv.source) == 0 {
			return nil, fmt.Errorf("Cannot merge HLVs. Missing cv.src for the first HLV: %v", h)
		}
		if h.cv.version <= 0 {
			return nil, fmt.Errorf("Cannot merge HLVs. bad cv.ver value for the first HLV: %v", h)
		}
		res.mv[h.cv.source] = h.cv.version
	}
	if len(other.mv) > 0 {
		res.mv.Add(other.mv)
	} else {
		if len(other.cv.source) == 0 {
			return nil, fmt.Errorf("Cannot merge HLVs. Missing cv.src for the second HLV: %v", other)
		}
		if other.cv.version <= 0 {
			return nil, fmt.Errorf("Cannot merge HLVs. bad cv.ver value for the second HLV: %v", other)
		}
		res.mv[other.cv.source] = other.cv.version
	}

	// Remove any entries in pv that are already in mv
	for src, _ := range res.mv {
		if _, ok := res.pv[src]; ok {
			delete(res.pv, src)
		}
	}
	return &res, nil
}

func (h *HLV) containVersion(src DocumentSourceId, ver uint64) bool {
	if h.cv.source == src {
		if h.cv.version >= ver {
			return true
		} else {
			return false
		}
	}
	if len(h.mv) > 0 {
		if v, ok := h.mv[src]; ok {
			if v >= ver {
				return true
			} else {
				return false
			}
		}
	}
	if len(h.pv) > 0 {
		if v, ok := h.pv[src]; ok {
			if v >= ver {
				return true
			} else {
				return false
			}
		}
	}
	return false
}

func (h *HLV) String() string {
	if h == nil {
		return ""
	}
	return fmt.Sprintf("{cvCAS: %v, cv: {%v,%v}, pv: %v, mv: %v}", h.cvCAS, h.cv.source, h.cv.version, h.pv, h.mv)
}

// Bytes required when formatting the map to XATTR
func BytesRequired(vMap VersionsMap) int {
	if vMap == nil {
		return 0
	}
	res := 0
	for k, _ := range vMap {
		res = res + len(k) + 3                  // quotes and column
		res = res + base.MaxBase64CASLength + 3 // quotes and comma
	}
	if res != 0 {
		res = res + 3 // Add { and } and nil terminator
	}
	return res
}
