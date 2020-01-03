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

func (b *BackfillPersistInfo) AlreadyIncludes(other *BackfillRequest) bool {
	for _, req := range b.Requests {
		if req.Same(other) {
			return true
		}
	}
	return false
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

// This will leave the first request alone
func (b *BackfillPersistInfo) InsertNonActiveRequest(task *BackfillRequest) {
	if len(b.Requests) == 1 {
		b.Requests = append(b.Requests, task)
		return
	}

	var replaceIdx int = -1
	// Check to see if anyone else can absorb this request
	for i, req := range b.Requests {
		// TODO - need a better idea of what is active and what is not
		if i == 0 {
			continue
		}
		if req.Contains(task) {
			// Nothing need to be done
			return
		} else if task.Contains(req) {
			// NOT active
			replaceIdx = i
			break
		}
		// TODO - try to implement "absorb"
		if req.NonConflictVBuckets(task) {
			req.VBucketsCombine(task)
			return
		}
	}

	if replaceIdx > -1 {
		b.Requests[replaceIdx] = task
		return
	}

	b.Requests = append(b.Requests, task)
	return
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

// 150k io all going to one vbucket
var threshold uint64 = 150000

// Returns true if the backfill request can accomodate the "other" request piggybacking on it
func (b *BackfillRequest) Contains(other *BackfillRequest) bool {
	switch b.Type {
	case IncrementalMainBackfill:
		switch other.Type {
		case CollectionBackfill:
			fallthrough
		case IncrementalMainBackfill:
			return b.BackfillMap.Contains(other.BackfillMap)
		default:
			panic("Need to implement")
		}
	case CollectionBackfill:
		switch other.Type {
		case IncrementalMainBackfill:
			return false
		case CollectionBackfill:
			// TODO - do this later
			return true
		default:
			panic("Need to implement")
		}

	default:
		panic("Need to implement")
	}
}

func (b *BackfillRequest) ContainsWThreshold(other *BackfillRequest, currentSeqnos map[uint16]uint64) bool {
	switch b.Type {
	case IncrementalMainBackfill:
		switch other.Type {
		case CollectionBackfill:
			fallthrough
		case IncrementalMainBackfill:
			// Simply check the current ongoing seqno + buffer against the requested
			return b.BackfillMap.ContainsWThreshold(other.BackfillMap, currentSeqnos, threshold)
		default:
			panic("Need to implement")
		}
	case CollectionBackfill:
		switch other.Type {
		case IncrementalMainBackfill:
			return false
		case CollectionBackfill:
			// TODO - do this later
			return true
		default:
			panic("Need to implement")
		}

	default:
		panic("Need to implement")
	}
}

// If the two requests do not have vbucket intersection, return true
func (b *BackfillRequest) NonConflictVBuckets(other *BackfillRequest) bool {
	if b == nil || other == nil {
		return false
	}

	// If job types are different, don't even check
	if b.Type != other.Type {
		return false
	}

	for vbno, _ := range b.BackfillMap {
		_, vbExists := other.BackfillMap[vbno]
		if vbExists {
			return false
		}
	}

	return true
}

// If complements, then absorb
func (b *BackfillRequest) VBucketsCombine(other *BackfillRequest) error {
	if b == nil || other == nil || !b.NonConflictVBuckets(other) {
		return base.ErrorInvalidInput
	}

	for k, v := range other.BackfillMap {
		b.BackfillMap[k] = v
	}
	return nil
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
		// TODO _ check for nil tsRange
		if *(tsRange[0]) != *(otherRange[0]) || *(tsRange[1]) != *(otherRange[1]) {
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

func (v VBucketBackfillMap) Contains(other VBucketBackfillMap) bool {
	currentLen := len(v)
	otherLen := len(other)
	if currentLen < otherLen {
		return false
	}

	for vbno, tsRange := range v {
		otherTsRange, exists := other[vbno]
		if !exists {
			if currentLen == otherLen {
				return false
			} else {
				continue
			}
		}
		if otherTsRange[0].Vbuuid != 0 && tsRange[0].Vbuuid != 0 && otherTsRange[0].Vbuuid != tsRange[0].Vbuuid {
			panic(fmt.Sprintf("NEIL DEBUG - vbuuid is different - curr %v other %v", tsRange[0].Vbuuid, otherTsRange[0].Vbuuid))
			return false
		}
		if tsRange[0].Seqno > otherTsRange[0].Seqno {
			// Other is requesting a start range earlier than current request
			return false
		}
		if tsRange[1].Seqno < otherTsRange[1].Seqno {
			// Other is requesting a longer endSeqno of this one
			return false
		}
	}
	return true
}

func (v VBucketBackfillMap) ContainsWThreshold(other VBucketBackfillMap, currentSeqnos map[uint16]uint64, threshold uint64) bool {
	currentLen := len(v)
	otherLen := len(other)
	if currentLen < otherLen {
		return false
	}

	for vbno, tsRange := range v {
		otherTsRange, exists := other[vbno]
		if !exists {
			if currentLen == otherLen {
				return false
			} else {
				continue
			}
		}
		currentSeqno, exists := currentSeqnos[vbno]
		if !exists {
			panic("Should exist")
		}
		if otherTsRange[0].Vbuuid != 0 && tsRange[0].Vbuuid != 0 && otherTsRange[0].Vbuuid != tsRange[0].Vbuuid {
			panic(fmt.Sprintf("NEIL DEBUG - vbuuid is different - curr %v other %v", tsRange[0].Vbuuid, otherTsRange[0].Vbuuid))
			return false
		}
		if currentSeqno+threshold > otherTsRange[0].Seqno {
			// This backfill has currently moved past the requested start of the other
			return false
		}
		if tsRange[1].Seqno < otherTsRange[1].Seqno {
			// Other is requesting a longer endSeqno of this one
			return false
		}
	}
	return true
}
