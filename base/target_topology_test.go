// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package base

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVBServerTopology_GetDiffVBList_NoDiff(t *testing.T) {
	serverVBMap := KvVBMapType{
		"node1:8091": []uint16{0, 1, 2},
		"node2:8091": []uint16{3, 4, 5},
	}
	vbList := []uint16{0, 1, 2, 3, 4, 5}

	topo1 := NewVBServerTopology(vbList, serverVBMap)
	topo2 := NewVBServerTopology(vbList, serverVBMap)

	diffList, err := topo1.GetDiffVBList(vbList, topo2)
	require.NoError(t, err)
	assert.Empty(t, diffList)
}

func TestVBServerTopology_GetDiffVBList_WithDiff(t *testing.T) {
	serverVBMap1 := KvVBMapType{
		"node1:8091": []uint16{0, 1, 2},
		"node2:8091": []uint16{3, 4, 5},
	}
	serverVBMap2 := KvVBMapType{
		"node1:8091": []uint16{0, 1},
		"node2:8091": []uint16{2, 3, 4, 5}, // VB 2 moved to node2
	}
	vbList := []uint16{0, 1, 2, 3, 4, 5}

	topo1 := NewVBServerTopology(vbList, serverVBMap1)
	topo2 := NewVBServerTopology(vbList, serverVBMap2)

	diffList, err := topo1.GetDiffVBList(vbList, topo2)
	require.NoError(t, err)
	assert.Len(t, diffList, 1)
	assert.Contains(t, diffList, uint16(2))
}

func TestVBServerTopology_GetDiffVBList_TypeMismatch(t *testing.T) {
	serverVBMap := KvVBMapType{
		"node1:8091": []uint16{0, 1, 2},
	}
	vbList := []uint16{0, 1, 2}

	serverTopo := NewVBServerTopology(vbList, serverVBMap)
	uuidTopo, err := NewVBUuidTopology(vbList, map[uint16]uint64{0: 111, 1: 222, 2: 333})
	require.NoError(t, err)

	diffList, err := serverTopo.GetDiffVBList(vbList, uuidTopo)
	assert.ErrorIs(t, err, ErrTopologyTypeMismatch)
	assert.Nil(t, diffList)
}

func TestVBServerTopology_IsSame_True(t *testing.T) {
	serverVBMap := KvVBMapType{
		"node1:8091": []uint16{0, 1, 2},
		"node2:8091": []uint16{3, 4, 5},
	}
	vbList := []uint16{0, 1, 2, 3, 4, 5}

	topo1 := NewVBServerTopology(vbList, serverVBMap)
	topo2 := NewVBServerTopology(vbList, serverVBMap)

	same, err := topo1.IsSame(topo2)
	require.NoError(t, err)
	assert.True(t, same)
}

func TestVBServerTopology_IsSame_False(t *testing.T) {
	serverVBMap1 := KvVBMapType{
		"node1:8091": []uint16{0, 1, 2},
		"node2:8091": []uint16{3, 4, 5},
	}
	serverVBMap2 := KvVBMapType{
		"node1:8091": []uint16{0, 1},
		"node2:8091": []uint16{2, 3, 4, 5},
	}
	vbList := []uint16{0, 1, 2, 3, 4, 5}

	topo1 := NewVBServerTopology(vbList, serverVBMap1)
	topo2 := NewVBServerTopology(vbList, serverVBMap2)

	same, err := topo1.IsSame(topo2)
	require.NoError(t, err)
	assert.False(t, same)
}

func TestVBServerTopology_IsSame_TypeMismatch(t *testing.T) {
	serverVBMap := KvVBMapType{
		"node1:8091": []uint16{0, 1, 2},
	}
	vbList := []uint16{0, 1, 2}

	serverTopo := NewVBServerTopology(vbList, serverVBMap)
	uuidTopo, err := NewVBUuidTopology(vbList, map[uint16]uint64{0: 111, 1: 222, 2: 333})
	require.NoError(t, err)

	same, err := serverTopo.IsSame(uuidTopo)
	assert.ErrorIs(t, err, ErrTopologyTypeMismatch)
	assert.False(t, same)
}

func TestVBUuidTopology_GetDiffVBList_NoDiff(t *testing.T) {
	uuidMap := map[uint16]uint64{0: 111, 1: 222, 2: 333}
	vbList := []uint16{0, 1, 2}

	topo1, err := NewVBUuidTopology(vbList, uuidMap)
	require.NoError(t, err)
	topo2, err := NewVBUuidTopology(vbList, uuidMap)
	require.NoError(t, err)

	diffList, err := topo1.GetDiffVBList(vbList, topo2)
	require.NoError(t, err)
	assert.Empty(t, diffList)
}

func TestVBUuidTopology_GetDiffVBList_WithDiff(t *testing.T) {
	uuidMap1 := map[uint16]uint64{0: 111, 1: 222, 2: 333}
	uuidMap2 := map[uint16]uint64{0: 111, 1: 999, 2: 333} // VB 1 uuid changed
	vbList := []uint16{0, 1, 2}

	topo1, err := NewVBUuidTopology(vbList, uuidMap1)
	require.NoError(t, err)
	topo2, err := NewVBUuidTopology(vbList, uuidMap2)
	require.NoError(t, err)

	diffList, err := topo1.GetDiffVBList(vbList, topo2)
	require.NoError(t, err)
	assert.Len(t, diffList, 1)
	assert.Contains(t, diffList, uint16(1))
}

func TestVBUuidTopology_GetDiffVBList_TypeMismatch(t *testing.T) {
	vbList := []uint16{0, 1, 2}

	uuidTopo, err := NewVBUuidTopology(vbList, map[uint16]uint64{0: 111, 1: 222, 2: 333})
	require.NoError(t, err)
	serverTopo := NewVBServerTopology(vbList, KvVBMapType{"node1:8091": vbList})

	diffList, err := uuidTopo.GetDiffVBList(vbList, serverTopo)
	assert.ErrorIs(t, err, ErrTopologyTypeMismatch)
	assert.Nil(t, diffList)
}

func TestVBUuidTopology_IsSame_True(t *testing.T) {
	uuidMap := map[uint16]uint64{0: 111, 1: 222, 2: 333}
	vbList := []uint16{0, 1, 2}

	topo1, err := NewVBUuidTopology(vbList, uuidMap)
	require.NoError(t, err)
	topo2, err := NewVBUuidTopology(vbList, uuidMap)
	require.NoError(t, err)

	same, err := topo1.IsSame(topo2)
	require.NoError(t, err)
	assert.True(t, same)
}

func TestVBUuidTopology_IsSame_False(t *testing.T) {
	uuidMap1 := map[uint16]uint64{0: 111, 1: 222, 2: 333}
	uuidMap2 := map[uint16]uint64{0: 111, 1: 999, 2: 333}
	vbList := []uint16{0, 1, 2}

	topo1, err := NewVBUuidTopology(vbList, uuidMap1)
	require.NoError(t, err)
	topo2, err := NewVBUuidTopology(vbList, uuidMap2)
	require.NoError(t, err)

	same, err := topo1.IsSame(topo2)
	require.NoError(t, err)
	assert.False(t, same)
}

func TestVBUuidTopology_IsSame_TypeMismatch(t *testing.T) {
	vbList := []uint16{0, 1, 2}
	uuidTopo, err := NewVBUuidTopology(vbList, map[uint16]uint64{0: 111, 1: 222, 2: 333})
	require.NoError(t, err)
	serverTopo := VBServerTopology{0: "node1", 1: "node1", 2: "node2"}

	same, err := uuidTopo.IsSame(serverTopo)
	assert.ErrorIs(t, err, ErrTopologyTypeMismatch)
	assert.False(t, same)
}

func TestTargetTopologyView_GetDiffFromBaseline(t *testing.T) {
	serverVBMapBaseline := KvVBMapType{
		"node1:8091": []uint16{0, 1, 2},
		"node2:8091": []uint16{3, 4, 5},
	}
	serverVBMapCurrent := KvVBMapType{
		"node1:8091": []uint16{0, 1},
		"node2:8091": []uint16{2, 3, 4, 5},
	}
	vbList := []uint16{0, 1, 2, 3, 4, 5}

	baseline := NewVBServerTopology(vbList, serverVBMapBaseline)
	current := NewVBServerTopology(vbList, serverVBMapCurrent)

	view := &TargetTopologyView{}
	view.SetBaseline(baseline)

	diffList, err := view.GetDiffFromBaseline(vbList, current)
	require.NoError(t, err)
	assert.Len(t, diffList, 1)
	assert.Contains(t, diffList, uint16(2))
}

func TestTargetTopologyView_GetDiffFromBaseline_NilBaseline(t *testing.T) {
	vbList := []uint16{0}
	current := VBServerTopology{0: "node1"}

	view := &TargetTopologyView{}
	// baseline is nil

	diffList, err := view.GetDiffFromBaseline(vbList, current)
	assert.ErrorIs(t, err, ErrBaselineNotSet)
	assert.Nil(t, diffList)
}

func TestTargetTopologyView_GetDiffFromBaseline_TypeMismatch(t *testing.T) {
	vbList := []uint16{0, 1, 2}

	serverVBMap := KvVBMapType{
		"node1:8091": []uint16{0, 1, 2},
	}
	baseline := NewVBServerTopology(vbList, serverVBMap)
	current, err := NewVBUuidTopology(vbList, map[uint16]uint64{0: 111, 1: 222, 2: 333})
	require.NoError(t, err)

	view := &TargetTopologyView{}
	view.SetBaseline(baseline)

	diffList, err := view.GetDiffFromBaseline(vbList, current)
	assert.ErrorIs(t, err, ErrTopologyTypeMismatch)
	assert.Nil(t, diffList)
}

func TestTargetTopologyView_IsStable_True(t *testing.T) {
	serverVBMap := KvVBMapType{
		"node1:8091": []uint16{0, 1, 2},
	}
	vbList := []uint16{0, 1, 2}

	last := NewVBServerTopology(vbList, serverVBMap)
	current := NewVBServerTopology(vbList, serverVBMap)

	view := &TargetTopologyView{}
	view.SetLastObserved(last)

	stable, err := view.IsStable(current)
	require.NoError(t, err)
	assert.True(t, stable)
}

func TestTargetTopologyView_IsStable_False(t *testing.T) {
	serverVBMapLast := KvVBMapType{
		"node1:8091": []uint16{0, 1, 2},
	}
	serverVBMapCurrent := KvVBMapType{
		"node1:8091": []uint16{0, 1},
		"node2:8091": []uint16{2},
	}
	vbList := []uint16{0, 1, 2}

	last := NewVBServerTopology(vbList, serverVBMapLast)
	current := NewVBServerTopology(vbList, serverVBMapCurrent)

	view := &TargetTopologyView{}
	view.SetLastObserved(last)

	stable, err := view.IsStable(current)
	require.NoError(t, err)
	assert.False(t, stable)
}

func TestTargetTopologyView_IsStable_NilLast(t *testing.T) {
	current := VBServerTopology{0: "node1"}

	view := &TargetTopologyView{}
	// lastObserved is nil

	stable, err := view.IsStable(current)
	assert.ErrorIs(t, err, ErrLastObservedNotSet)
	assert.False(t, stable)
}

func TestTargetTopologyView_IsStable_TypeMismatch(t *testing.T) {
	vbList := []uint16{0, 1, 2}

	serverVBMap := KvVBMapType{
		"node1:8091": []uint16{0, 1, 2},
	}
	lastObserved := NewVBServerTopology(vbList, serverVBMap)
	current, err := NewVBUuidTopology(vbList, map[uint16]uint64{0: 111, 1: 222, 2: 333})
	require.NoError(t, err)

	view := &TargetTopologyView{}
	view.SetLastObserved(lastObserved)

	stable, err := view.IsStable(current)
	assert.ErrorIs(t, err, ErrTopologyTypeMismatch)
	assert.False(t, stable)
}

func TestVBUuidTopology_FilterVBList(t *testing.T) {
	// All UUIDs available for requested VBs
	allUuids := map[uint16]uint64{0: 111, 1: 222, 2: 333, 3: 444, 4: 555}
	// Only request VBs 0, 1, 2
	vbList := []uint16{0, 1, 2}

	topo, err := NewVBUuidTopology(vbList, allUuids)
	require.NoError(t, err)

	// Verify only requested VBs are included in the map
	assert.Len(t, topo, 3)
	_, ok0 := topo[0]
	_, ok1 := topo[1]
	_, ok2 := topo[2]
	_, ok3 := topo[3]
	_, ok4 := topo[4]
	assert.True(t, ok0)
	assert.True(t, ok1)
	assert.True(t, ok2)
	assert.False(t, ok3)
	assert.False(t, ok4)
}

func TestVBUuidTopology_MissingVBs_ReturnsError(t *testing.T) {
	// UUID map is missing some VBs that are in vbList
	allUuids := map[uint16]uint64{0: 111, 2: 333} // missing VB 1 and 3
	vbList := []uint16{0, 1, 2, 3}

	topo, err := NewVBUuidTopology(vbList, allUuids)
	assert.Error(t, err)
	assert.Nil(t, topo)
	assert.Contains(t, err.Error(), "1")
	assert.Contains(t, err.Error(), "3")
}

func TestTargetTopologyView_WithVBUuidTopology(t *testing.T) {
	vbList := []uint16{0, 1, 2}
	baselineUuids := map[uint16]uint64{0: 111, 1: 222, 2: 333}
	currentUuids := map[uint16]uint64{0: 111, 1: 999, 2: 333} // VB 1 changed

	baseline, err := NewVBUuidTopology(vbList, baselineUuids)
	require.NoError(t, err)
	current, err := NewVBUuidTopology(vbList, currentUuids)
	require.NoError(t, err)

	view := &TargetTopologyView{}
	view.SetBaseline(baseline)

	diffList, err := view.GetDiffFromBaseline(vbList, current)
	require.NoError(t, err)
	assert.Len(t, diffList, 1)
	assert.Contains(t, diffList, uint16(1))
}

func TestTargetTopologyView_GettersAndSetters(t *testing.T) {
	vbList := []uint16{0, 1, 2}
	baselineMap := map[uint16]uint64{0: 111, 1: 222, 2: 333}
	lastObservedMap := map[uint16]uint64{0: 444, 1: 555, 2: 666}

	baseline, err := NewVBUuidTopology(vbList, baselineMap)
	require.NoError(t, err)
	lastObserved, err := NewVBUuidTopology(vbList, lastObservedMap)
	require.NoError(t, err)

	view := &TargetTopologyView{}

	// Initially nil
	assert.Nil(t, view.GetBaseline())
	assert.Nil(t, view.GetLastObserved())

	// Set and verify
	view.SetBaseline(baseline)
	view.SetLastObserved(lastObserved)

	assert.Equal(t, baseline, view.GetBaseline())
	assert.Equal(t, lastObserved, view.GetLastObserved())
}
