// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package base

import (
	"errors"
	"fmt"
)

// ErrTopologyTypeMismatch is returned when comparing topologies of different types
var ErrTopologyTypeMismatch = errors.New("topology type mismatch: cannot compare different topology types")

// ErrBaselineNotSet is returned when attempting to compare against an uninitialized baseline
var ErrBaselineNotSet = errors.New("baseline topology not set")

// ErrLastObservedNotSet is returned when checking stability against an uninitialized last observed topology
var ErrLastObservedNotSet = errors.New("last observed topology not set")

// TargetTopologyData represents abstracted topology data for change detection.
// For traditional clusters: tracks vb->server mapping
// For CNG: tracks vb->uuid mapping
type TargetTopologyData interface {
	// GetDiffVBList returns VBs that differ between this and other topology.
	// Returns ErrTopologyTypeMismatch if other is not the same concrete type.
	GetDiffVBList(vblist []uint16, other TargetTopologyData) ([]uint16, error)
	// IsSame returns true if topologies are equivalent.
	// Returns ErrTopologyTypeMismatch if other is not the same concrete type.
	IsSame(other TargetTopologyData) (bool, error)
}

// VBServerTopology implements TargetTopologyData for traditional clusters.
// It tracks vbucket to server hostname mapping.
type VBServerTopology map[uint16]string

// NewVBServerTopology creates a VBServerTopology from a vbList and server->vb map.
// It filters the serverVBMap to only include VBs in vbList.
func NewVBServerTopology(vbList []uint16, serverVBMap KvVBMapType) VBServerTopology {
	return ConstructVbServerMap(vbList, serverVBMap)
}

// GetDiffVBList returns VBs that have different server assignments between this and other topology.
// Returns ErrTopologyTypeMismatch if other is not a VBServerTopology.
func (t VBServerTopology) GetDiffVBList(vblist []uint16, other TargetTopologyData) ([]uint16, error) {
	otherServer, ok := other.(VBServerTopology)
	if !ok {
		return nil, ErrTopologyTypeMismatch
	}
	return GetDiffVBList(vblist, t, otherServer), nil
}

// IsSame returns true if both topologies have identical vb->server mappings.
// Returns ErrTopologyTypeMismatch if other is not a VBServerTopology.
func (t VBServerTopology) IsSame(other TargetTopologyData) (bool, error) {
	otherServer, ok := other.(VBServerTopology)
	if !ok {
		return false, ErrTopologyTypeMismatch
	}
	return AreVBServerMapsTheSame(t, otherServer), nil
}

// VBUuidTopology implements TargetTopologyData for CNG clusters.
// It tracks vbucket to vbuuid mapping.
type VBUuidTopology map[uint16]uint64

// NewVBUuidTopology creates a VBUuidTopology from a vbList and vb->uuid map.
// Returns an error if any VBs in vbList are not found in allVBUuids
func NewVBUuidTopology(vbList []uint16, allVBUuids map[uint16]uint64) (VBUuidTopology, error) {
	if len(allVBUuids) == 0 {
		return nil, ErrorVbUUIDMapEmpty
	}

	vbUuidMap := make(VBUuidTopology, len(vbList))
	var missingVBs []uint16
	for _, vb := range vbList {
		if uuid, ok := allVBUuids[vb]; ok {
			vbUuidMap[vb] = uuid
		} else {
			missingVBs = append(missingVBs, vb)
		}
	}
	if len(missingVBs) > 0 {
		return nil, fmt.Errorf("vbuckets %v not found in uuid map", missingVBs)
	}
	return vbUuidMap, nil
}

// GetDiffVBList returns VBs that have different uuids between this and other topology.
// Returns ErrTopologyTypeMismatch if other is not a VBUuidTopology.
func (t VBUuidTopology) GetDiffVBList(vblist []uint16, other TargetTopologyData) ([]uint16, error) {
	otherUuid, ok := other.(VBUuidTopology)
	if !ok {
		return nil, ErrTopologyTypeMismatch
	}
	return GetDiffVBListByUuid(vblist, t, otherUuid), nil
}

// IsSame returns true if both topologies have identical vb->uuid mappings.
// Returns ErrTopologyTypeMismatch if other is not a VBUuidTopology.
func (t VBUuidTopology) IsSame(other TargetTopologyData) (bool, error) {
	otherUuid, ok := other.(VBUuidTopology)
	if !ok {
		return false, ErrTopologyTypeMismatch
	}
	return AreVBUuidMapsTheSame(t, otherUuid), nil
}

// TargetTopologyView holds baseline and last observed topology for change detection.
// It provides methods to compare current topology against baseline and track stability.
type TargetTopologyView struct {
	baseline     TargetTopologyData
	lastObserved TargetTopologyData
}

// SetBaseline sets the baseline topology (captured at pipeline start).
func (v *TargetTopologyView) SetBaseline(data TargetTopologyData) {
	v.baseline = data
}

// SetLastObserved sets the last observed topology (for stability tracking).
func (v *TargetTopologyView) SetLastObserved(data TargetTopologyData) {
	v.lastObserved = data
}

// GetBaseline returns the baseline topology.
func (v *TargetTopologyView) GetBaseline() TargetTopologyData {
	return v.baseline
}

// GetLastObserved returns the last observed topology.
func (v *TargetTopologyView) GetLastObserved() TargetTopologyData {
	return v.lastObserved
}

// GetDiffFromBaseline returns VBs that differ between baseline and current topology.
// Returns ErrBaselineNotSet if baseline is not set.
// Returns ErrTopologyTypeMismatch if current and baseline have different types.
func (v *TargetTopologyView) GetDiffFromBaseline(vblist []uint16, current TargetTopologyData) ([]uint16, error) {
	if v.baseline == nil {
		return nil, ErrBaselineNotSet
	}
	return v.baseline.GetDiffVBList(vblist, current)
}

// IsStable returns true if current topology matches the last observed topology.
// This is used to determine if topology changes have stabilized.
// Returns ErrLastObservedNotSet if lastObserved is not set.
// Returns ErrTopologyTypeMismatch if current and lastObserved have different types.
func (v *TargetTopologyView) IsStable(current TargetTopologyData) (bool, error) {
	if v.lastObserved == nil {
		return false, ErrLastObservedNotSet
	}
	return v.lastObserved.IsSame(current)
}
