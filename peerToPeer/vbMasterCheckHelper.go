package peerToPeer

import "sync"

// TODO - need to hook up to repl spec or bucket service

type VbMasterCheckHelper interface {
	GetUnverifiedSubset(requestedSet BucketVBMapType) BucketVBMapType
}

type VbMasterCheckHelperImpl struct {
	bucketVbsMap    map[string]verifiedVbs
	bucketVBsMapMtx sync.RWMutex
}

type verifiedVbs struct {
	verifiedVbs map[uint16]bool
	mtx         sync.RWMutex
}

func (h *VbMasterCheckHelperImpl) GetUnverifiedSubset(requestedSet BucketVBMapType) BucketVBMapType {
	// TODO - fill this in
	return requestedSet
}

func NewVBMasterCheckHelper() *VbMasterCheckHelperImpl {
	helper := &VbMasterCheckHelperImpl{
		bucketVbsMap: map[string]verifiedVbs{},
	}
	return helper
}
