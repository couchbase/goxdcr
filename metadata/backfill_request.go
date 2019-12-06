package metadata

import (
	"github.com/couchbase/goxdcr/base"
)

type BackfillRequest struct {
	Manifests            CollectionsManifestPair
	RequestedCollections CollectionToCollectionMapping
	HighestSeqno         uint64
}

func NewBackfillRequest(manifests CollectionsManifestPair, mapping CollectionToCollectionMapping, highSeqno uint64) *BackfillRequest {
	return &BackfillRequest{
		Manifests:            manifests,
		RequestedCollections: mapping,
		HighestSeqno:         highSeqno,
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
		if req.HighestSeqno > highSeqno {
			highSeqno = req.HighestSeqno
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

	return b.HighestSeqno == other.HighestSeqno && b.RequestedCollections.Same(&other.RequestedCollections) &&
		b.Manifests.Same(&other.Manifests)
}

func (b *BackfillRequest) Clone() *BackfillRequest {
	return &BackfillRequest{
		Manifests:            b.Manifests,
		RequestedCollections: b.RequestedCollections,
		HighestSeqno:         b.HighestSeqno,
	}
}
