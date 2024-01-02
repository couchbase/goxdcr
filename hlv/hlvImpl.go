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
)

type HLV struct {
	cvCAS uint64
	cv    currentVersion
	pv    VersionsMap
	mv    VersionsMap
}

// This is the mutation current version
type currentVersion struct {
	source  string
	version uint64
}

type VersionsMap map[string]uint64

// Add the other {src,ver} to vm if it is not there or if other has higher ver
func (vm VersionsMap) add(other VersionsMap) {
	for src, ver := range other {
		v, ok := vm[src]
		if !ok || ver > v {
			vm[src] = ver
		}
	}
}

// This creates a new updated HLV based on XATTR and document.CAS
// Input:
// - cas: this is the document CAS
// - source: this is the bucket XDCR get this mutation from, either through DCP or subdoc_get
// - {src,ver} these combined is the input cv
// If cas > ver, then the document has been mutated since replication.
// - If there is no mv, the input cv will be rolled into pv
// - If there is an mv, the mv will be rolled into pv
func NewHLV(source string, cas uint64, src string, ver uint64, pv, mv VersionsMap) (*HLV, error) {
	hlv := HLV{
		cvCAS: cas,
	}
	if cas < ver {
		// ver is initially the same as cas. Cas may increase later. So ver should never be greater than cas
		return nil, fmt.Errorf("cas < ver, cas=%v,ver=%v,src=%v,pv=%s,mv=%s", cas, ver, src, pv, mv)
	} else if cas == ver {
		// The HLV did not change.
		hlv.cv = currentVersion{src, ver}
		hlv.pv = pv
		hlv.mv = mv
	} else {
		// CAS has changed. This new CAS is the current version. Everything else is rolled into pv
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
	return h.mv
}

func (h *HLV) GetPV() VersionsMap {
	return h.pv
}

func (h *HLV) GetCvSrc() string {
	return h.cv.source
}

func (h *HLV) GetCvVer() uint64 {
	return h.cv.version
}

func (h *HLV) DetectConflict(other *HLV) (ConflictDetectionResult, error) {
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
	res := HLV{
		cvCAS: 0,
		cv:    currentVersion{},
		pv:    make(VersionsMap),
		mv:    make(VersionsMap),
	}
	// Combine pv
	res.pv.add(h.pv)
	res.pv.add(other.pv)
	// Combine mv or cv to get the new mv
	if len(h.mv) > 0 {
		res.mv.add(h.mv)
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
		res.mv.add(other.mv)
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

func (h *HLV) containVersion(src string, ver uint64) bool {
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
	return fmt.Sprintf("{cvCAS: %v, cv: {%v,%v}, pv: %v, mv: %v", h.cvCAS, h.cv.source, h.cv.version, h.pv, h.mv)
}
