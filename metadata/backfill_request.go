package metadata

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
)

type BackfillType int

const (
	MainPipelineBackfill BackfillType = iota
	CollectionBackfill   BackfillType = iota
)

type BackfillRequest struct {
	Type BackfillType

	SeqnoRange           [2]uint64
	Manifests            CollectionsManifestPair
	RequestedCollections CollectionToCollectionMapping
}

func NewBackfillRequest(manifests CollectionsManifestPair, mapping CollectionToCollectionMapping, beginSeqno, highSeqno uint64) *BackfillRequest {
	return &BackfillRequest{
		Type:                 CollectionBackfill,
		Manifests:            manifests,
		RequestedCollections: mapping,
		SeqnoRange:           [2]uint64{beginSeqno, highSeqno},
	}
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
		if req.SeqnoRange[1] > highSeqno {
			highSeqno = req.SeqnoRange[1]
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

	return b.SeqnoRange[1] == other.SeqnoRange[1] && b.RequestedCollections.Same(&other.RequestedCollections) &&
		b.Manifests.Same(&other.Manifests)
}

func (b *BackfillRequest) Clone() *BackfillRequest {
	return &BackfillRequest{
		Manifests:            b.Manifests,
		RequestedCollections: b.RequestedCollections,
		SeqnoRange:           b.SeqnoRange,
	}
}

// A map of vbucket - start and end sequence numbers
type VBucketBackfillMap map[uint16][2]*base.VBTimestamp

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
