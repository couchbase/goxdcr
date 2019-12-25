package metadata

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
)

type BackfillType int

const (
	IncrementalMainBackfill BackfillType = iota
	CollectionBackfill      BackfillType = iota
)

type BackfillRequest struct {
	Type BackfillType

	BackfillMap VBucketBackfillMap

	//	SeqnoRange           [2]uint64
	//	Manifests            CollectionsManifestPair

	// For collection backfill - if it is in the mapping, then it should be backfilled
	RequestedCollections CollectionToCollectionMapping
}

func NewIncrementalBackfillRequest(backfillMap VBucketBackfillMap) *BackfillRequest {
	return &BackfillRequest{
		Type:        IncrementalMainBackfill,
		BackfillMap: backfillMap,
	}
}

func NewCollectionBackfillRequest(manifests CollectionsManifestPair, mapping CollectionToCollectionMapping, beginSeqno, highSeqno uint64) *BackfillRequest {
	req := &BackfillRequest{
		Type:                 CollectionBackfill,
		RequestedCollections: mapping,
		BackfillMap:          make(VBucketBackfillMap),
	}

	var i uint16
	for i = 0; i < 1024; i++ {
		// Everything else is 0 to start streaming from the beginning
		startTs := &base.VBTimestamp{Vbno: i}
		endTs := &base.VBTimestamp{
			Vbno:        i,
			Seqno:       highSeqno,
			ManifestIDs: base.CollectionsManifestIdPair{manifests.Source.Uid(), manifests.Target.Uid()},
		}
		var tsRange [2]*base.VBTimestamp
		tsRange[0] = startTs
		tsRange[1] = endTs
		req.BackfillMap[i] = tsRange
	}

	return req
}

type BackfillPersistInfo struct {
	// List of backfill requests
	Requests []*BackfillRequest
}

func (b *BackfillPersistInfo) GetHighestEndSeqno() (uint64, error) {
	if b == nil || len(b.Requests) == 0 {
		return 0, base.ErrorInvalidInput
	}

	var highSeqno uint64
	for _, req := range b.Requests {
		highSeqnoForReq := req.BackfillMap.GetHighestEndSeqno()
		if highSeqnoForReq > highSeqno {
			highSeqno = highSeqnoForReq
		}
	}

	return highSeqno, nil
}

//func (b *BackfillPersistInfo) GetStreamType() DcpStreamType {
//	return DcpStreamTypeBackfill
//}

func (b *BackfillPersistInfo) Same(other *BackfillPersistInfo) bool {
	if b == nil && other != nil || b != nil && other == nil {
		return false
	} else if b == nil && other == nil {
		return true
	}

	if len(b.Requests) != len(other.Requests) {
		return false
	}

	for i := 0; i < len(b.Requests); i++ {
		if !((b.Requests)[i].Same(other.Requests[i])) {
			return false
		}
	}

	return true
}

func (b *BackfillPersistInfo) Clone() *BackfillPersistInfo {
	if b == nil {
		return nil
	}
	var requests []*BackfillRequest
	if b.Requests != nil {
		for i := 0; i < len(b.Requests); i++ {
			requests = append(requests, b.Requests[i].Clone())
		}
	}
	return &BackfillPersistInfo{
		Requests: requests,
	}
}

func (b *BackfillRequest) Same(other *BackfillRequest) bool {
	if b == nil && other != nil || b != nil && other == nil {
		return false
	} else if b == nil && other == nil {
		return true
	}

	return b.RequestedCollections.Same(&other.RequestedCollections) && b.Type == other.Type && b.BackfillMap.Same(&other.BackfillMap)
}

func (b *BackfillRequest) Clone() *BackfillRequest {
	return &BackfillRequest{
		Type:                 b.Type,
		RequestedCollections: b.RequestedCollections,
		BackfillMap:          b.BackfillMap.Clone(),
	}
}

// A map of vbucket - start and end sequence numbers
type VBucketBackfillMap map[uint16][2]*base.VBTimestamp

func (v VBucketBackfillMap) Clone() VBucketBackfillMap {
	retMap := make(VBucketBackfillMap)
	for k, v := range v {
		retMap[k] = v
	}
	return retMap
}

func (v *VBucketBackfillMap) Same(other *VBucketBackfillMap) bool {
	if v == nil && other == nil {
		return true
	} else if v == nil && other != nil || v != nil && other == nil {
		return false
	} else if len(*v) != len(*other) {
		return false
	}

	for vbno, tsRange := range *v {
		otherRange, exists := (*other)[vbno]
		if !exists {
			return false
		}
		if tsRange[0] != otherRange[0] || tsRange[1] != otherRange[1] {
			return false
		}
	}

	return true
}

func (v *VBucketBackfillMap) GetHighestEndSeqno() (highestSeqno uint64) {
	if v == nil {
		return
	}

	for _, tsRange := range *v {
		if tsRange[1].Seqno > highestSeqno {
			highestSeqno = tsRange[1].Seqno
		}
	}

	return
}

// NOT SERIALIZED
// These functions are to ensure that a maximum of range is backfilled
func (v *VBucketBackfillMap) AddBackfillRange(vbno uint16, begin, end *base.VBTimestamp) (err error) {
	backfillRange, exists := (*v)[vbno]
	if !exists || backfillRange[1].Empty() { // It's ok for backfillRange[0] to be empty (i.e. backfill from 0)
		backfillRange[0] = begin
		backfillRange[1] = end
		(*v)[vbno] = backfillRange
	} else {
		// Compare beginning
		result, valid := backfillRange[0].Compare(begin)
		if !valid {
			err = fmt.Errorf("Invalid begin comparison between %v and %v", backfillRange[0], begin)
			return
		}
		if result < 0 {
			backfillRange[0] = begin
		}
		// Compare end
		result, valid = backfillRange[1].Compare(end)
		if !valid {
			err = fmt.Errorf("Invalid end comparison between %v and %v", backfillRange[0], begin)
			return
		}
		if result > 0 {
			backfillRange[1] = end
		}
	}
	return
}

func (v *VBucketBackfillMap) TotalMutations() (result uint64) {
	for _, timestamps := range *v {
		seqnoDiff := timestamps[1].Seqno - timestamps[0].Seqno
		result += seqnoDiff
	}
	return
}

// Given where latest VBMap is (i.e. checkpoint), produce an incremental map if needed
func (v *VBucketBackfillMap) GetSpecificBackfillMap(latestVBMap map[uint16]*base.VBTimestamp) VBucketBackfillMap {
	if v == nil {
		return nil
	}
	diffMap := make(VBucketBackfillMap)
	for vbno, tsRange := range *v {
		latestTs, exists := latestVBMap[vbno]
		if !exists {
			diffMap[vbno] = tsRange
		} else {
			result, valid := latestTs.Compare(tsRange[1])
			if !valid {
				panic("Should not be invalid")
				diffMap[vbno] = tsRange
			} else if result < 0 {
				var newRange [2]*base.VBTimestamp
				newRange[0] = latestTs
				newRange[1] = tsRange[1]
				diffMap[vbno] = newRange
			}
		}
	}
	return diffMap
}
