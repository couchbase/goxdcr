/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package peerToPeer

import (
	"fmt"
	"github.com/couchbase/goxdcr/v8/metadata"
	"sync"
)

type VbMasterCheckHelper interface {
	GetUnverifiedSubset(requestedSet BucketVBMapType) (BucketVBMapType, error)
	HandleSpecCreation(spec *metadata.ReplicationSpecification)
	HandleSpecDeletion(spec *metadata.ReplicationSpecification)
	HandleSpecChange(oldSpec, newSpec *metadata.ReplicationSpecification)
}

type VbMasterCheckHelperImpl struct {
	bucketRefCnt    map[string]int
	bucketVbsMap    map[string]verifiedVbs
	bucketVBsMapMtx sync.RWMutex
}

func (h *VbMasterCheckHelperImpl) HandleSpecCreation(spec *metadata.ReplicationSpecification) {
	h.bucketVBsMapMtx.Lock()
	defer h.bucketVBsMapMtx.Unlock()
	_, exists := h.bucketRefCnt[spec.SourceBucketName]
	if !exists {
		h.bucketRefCnt[spec.SourceBucketName] = 1
		h.bucketVbsMap[spec.SourceBucketName] = verifiedVbs{verifiedVbs: map[uint16]bool{}}
	} else {
		h.bucketRefCnt[spec.SourceBucketName]++
	}
}

func (h *VbMasterCheckHelperImpl) HandleSpecDeletion(spec *metadata.ReplicationSpecification) {
	h.bucketVBsMapMtx.Lock()
	defer h.bucketVBsMapMtx.Unlock()
	_, exists := h.bucketRefCnt[spec.SourceBucketName]
	if exists {
		h.bucketRefCnt[spec.SourceBucketName]--
		if h.bucketRefCnt[spec.SourceBucketName] == 0 {
			// clean up
			delete(h.bucketRefCnt, spec.SourceBucketName)
			delete(h.bucketVbsMap, spec.SourceBucketName)
		}
	} else {
		panic(fmt.Sprintf("Source bucket %v deleted when it never existed - refCnt Error", spec.SourceBucketName))
	}
}

func (h *VbMasterCheckHelperImpl) HandleSpecChange(oldSpec, newSpec *metadata.ReplicationSpecification) {
	// no-op
}

type verifiedVbs struct {
	verifiedVbs map[uint16]bool
	mtx         sync.RWMutex
}

func (h *VbMasterCheckHelperImpl) GetUnverifiedSubset(requestedSet BucketVBMapType) (BucketVBMapType, error) {
	filteredSet := make(BucketVBMapType)

	for bucket, requestedVBs := range requestedSet {
		h.bucketVBsMapMtx.RLock()
		verifiedVbsMap, exists := h.bucketVbsMap[bucket]
		h.bucketVBsMapMtx.RUnlock()
		if !exists {
			return nil, fmt.Errorf("source bucket %v wasn't registered", bucket)
		}

		verifiedVbsMap.mtx.RLock()
		for _, vb := range requestedVBs {
			verified, exists2 := verifiedVbsMap.verifiedVbs[vb]
			if !exists2 || !verified {
				filteredSet[bucket] = append(filteredSet[bucket], vb)
			}
		}
		verifiedVbsMap.mtx.RUnlock()
	}
	return filteredSet, nil
}

func NewVBMasterCheckHelper() *VbMasterCheckHelperImpl {
	helper := &VbMasterCheckHelperImpl{
		bucketVbsMap: map[string]verifiedVbs{},
		bucketRefCnt: map[string]int{},
	}
	return helper
}
