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
	"math/rand"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/stretchr/testify/assert"
)

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}
func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}
func Test_compareHlv(t *testing.T) {
	sourcePruningWindow := time.Duration(5) * time.Nanosecond
	targetPruningWindow := time.Duration(5) * time.Nanosecond
	sourceBucketUUID := DocumentSourceId(randomString(10))
	targetBucketUUID := DocumentSourceId(randomString(10))
	type args struct {
		hlv1              *HLV
		hlv2              *HLV
		sourcePruningFunc base.PruningFunc
		targetPruningFunc base.PruningFunc
	}
	tests := []struct {
		name       string
		args       args
		same       bool
		errPresent bool
	}{
		//Test1 : both source and target HLVs are nil
		{
			name: "HLVs Absent",
			args: args{
				hlv1:              nil,
				hlv2:              nil,
				sourcePruningFunc: base.GetHLVPruneFunction(rand.Uint64(), sourcePruningWindow),
				targetPruningFunc: base.GetHLVPruneFunction(rand.Uint64(), targetPruningWindow),
			},
			same:       false,
			errPresent: true,
		},
		//Test2 : HLV is present at the source
		{
			name: "HLV present at Source",
			args: args{
				hlv1:              generateHLV(sourceBucketUUID, 10, 10, targetBucketUUID, 10, nil, nil),
				hlv2:              nil,
				sourcePruningFunc: base.GetHLVPruneFunction(10, sourcePruningWindow),
				targetPruningFunc: base.GetHLVPruneFunction(10, targetPruningWindow),
			},
			same:       false,
			errPresent: true,
		},
		//Test3 : HLV is present at the target
		{
			name: "HLV present at Target",
			args: args{
				hlv1:              nil,
				hlv2:              generateHLV(targetBucketUUID, 20, 20, sourceBucketUUID, 20, nil, nil),
				sourcePruningFunc: base.GetHLVPruneFunction(20, sourcePruningWindow),
				targetPruningFunc: base.GetHLVPruneFunction(20, targetPruningWindow),
			},
			same:       false,
			errPresent: true,
		},
		//Test4 : both the HLVs are present - with one of them outdated(This case involves implicit construction of HLV)
		{
			name: "HLV present at both source and target but outdated at source",
			args: args{
				hlv1:              generateHLV(sourceBucketUUID, 30, 20, targetBucketUUID, 20, VersionsMap{}, nil),
				hlv2:              generateHLV(targetBucketUUID, 30, 30, sourceBucketUUID, 30, VersionsMap{targetBucketUUID: 20}, nil),
				sourcePruningFunc: base.GetHLVPruneFunction(30, sourcePruningWindow),
				targetPruningFunc: base.GetHLVPruneFunction(30, targetPruningWindow),
			},
			same:       true,
			errPresent: false,
		},
		//Test5 : both the HLVs are present - with PV pruned in one of them
		{
			name: "HLV present at both source and target with Missing PVs(Pruned version at Target) ",
			args: args{
				hlv1:              generateHLV(sourceBucketUUID, 30, 20, sourceBucketUUID, 20, VersionsMap{DocumentSourceId(randomString(10)): 1}, nil),
				hlv2:              generateHLV(targetBucketUUID, 30, 30, sourceBucketUUID, 30, VersionsMap{}, nil),
				sourcePruningFunc: base.GetHLVPruneFunction(30, sourcePruningWindow),
				targetPruningFunc: base.GetHLVPruneFunction(30, targetPruningWindow),
			},
			same:       true,
			errPresent: false,
		},
		//Test6: HLV cvCASs missmatch
		{
			name: "HLV cvCAS mismatch",
			args: args{
				hlv1:              generateHLV(sourceBucketUUID, 40, 40, sourceBucketUUID, 40, nil, nil),
				hlv2:              generateHLV(targetBucketUUID, 20, 20, sourceBucketUUID, 20, nil, nil),
				sourcePruningFunc: base.GetHLVPruneFunction(40, sourcePruningWindow),
				targetPruningFunc: base.GetHLVPruneFunction(20, targetPruningWindow),
			},
			same:       false,
			errPresent: false,
		},
		//Test7: PVs present on both source and target with pruning and data mismatch
		{
			name: "PV pruning+mismatch",
			args: args{
				hlv1:              generateHLV(sourceBucketUUID, 30, 30, sourceBucketUUID, 30, VersionsMap{targetBucketUUID: 10, DocumentSourceId(randomString(10)): 1}, nil),
				hlv2:              generateHLV(targetBucketUUID, 30, 30, sourceBucketUUID, 30, VersionsMap{targetBucketUUID: 20}, nil),
				sourcePruningFunc: base.GetHLVPruneFunction(30, sourcePruningWindow),
				targetPruningFunc: base.GetHLVPruneFunction(30, targetPruningWindow),
			},
			same:       false,
			errPresent: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.args.hlv1.SameAs(tt.args.hlv2, tt.args.sourcePruningFunc, tt.args.targetPruningFunc)
			if (err != nil) != tt.errPresent {
				t.Errorf("HLV.Diff() error = %v, wantErr %v", err, tt.errPresent)
				return
			}
			if got != tt.same {
				t.Errorf("CRMetadata.Diff() = %v, want %v", got, tt.same)
			}
		})
	}
}

func generateHLV(source DocumentSourceId, cas uint64, cvCas uint64, src DocumentSourceId, ver uint64, pv VersionsMap, mv VersionsMap) *HLV {
	HLV, _ := NewHLV(source, cas, cvCas, src, ver, pv, mv)
	return HLV
}

func Test_VersionsMapAndDeltas(t *testing.T) {
	a := assert.New(t)

	s1 := DocumentSourceId("src1")
	s2 := DocumentSourceId("src2")
	s3 := DocumentSourceId("src3")
	s4 := DocumentSourceId("src4")
	s5 := DocumentSourceId("src5")

	// 1. version map to version deltas
	now := int(time.Now().UnixNano())
	v1 := uint64(now - rand.Intn(1000000000000))
	v2 := uint64(now - rand.Intn(1000000000000))
	v3 := uint64(now - rand.Intn(1000000000000))
	v4 := uint64(now - rand.Intn(1000000000000))
	v5 := uint64(now - rand.Intn(1000000000000))

	// 1a. multiple entries
	vm := make(VersionsMap)
	vm[s1] = v1
	vm[s2] = v2
	vm[s3] = v3
	vm[s4] = v4
	vm[s5] = v5
	vdm := vm.VersionsDeltas()
	vm1 := vdm.VersionsMap()
	a.Equal(vm[s1], vm1[s1])
	a.Equal(vm[s2], vm1[s2])
	a.Equal(vm[s3], vm1[s3])
	a.Equal(vm[s4], vm1[s4])
	a.Equal(vm[s5], vm1[s5])

	// 1b. 1 entry
	vm = make(VersionsMap)
	vm[s1] = v1
	vdm = vm.VersionsDeltas()
	vm1 = vdm.VersionsMap()
	a.Equal(vm[s1], vm1[s1])

	// 1c. 0 entries
	vm = make(VersionsMap)
	vdm = vm.VersionsDeltas()
	vm1 = vdm.VersionsMap()
	a.Equal(len(vm1), 0)
	a.Equal(len(vdm), 0)
	a.NotNil(vdm)
	a.NotNil(vm1)

	// 1d. nil map
	vm = nil
	vdm = vm.VersionsDeltas()
	vm1 = vdm.VersionsMap()
	a.Nil(vdm)
	a.Nil(vm1)

	// 2. version delta to version map
	now = int(time.Now().UnixNano())
	d1 := uint64(now)
	d2 := uint64(rand.Intn(1000000000000))
	d3 := uint64(rand.Intn(1000000000000))
	d4 := uint64(rand.Intn(1000000000000))
	d5 := uint64(rand.Intn(1000000000000))

	// 2a. multiple entries
	vdm = make(VersionsDeltas, 0, 5)
	vdm = append(vdm, versionSourcePair{s1, d1})
	vdm = append(vdm, versionSourcePair{s2, d2})
	vdm = append(vdm, versionSourcePair{s3, d3})
	vdm = append(vdm, versionSourcePair{s4, d4})
	vdm = append(vdm, versionSourcePair{s5, d5})
	vm = vdm.VersionsMap()
	vdm1 := vm.VersionsDeltas()
	a.Equal(vdm1[0], vdm[0])
	a.Equal(vdm1[1], vdm[1])
	a.Equal(vdm1[2], vdm[2])
	a.Equal(vdm1[3], vdm[3])
	a.Equal(vdm1[4], vdm[4])

	// 2b. one entry
	vdm = make(VersionsDeltas, 0, 5)
	vdm = append(vdm, versionSourcePair{s1, d1})
	vm = vdm.VersionsMap()
	vdm1 = vm.VersionsDeltas()
	a.Equal(vdm1[0], vdm[0])
	a.Equal(vdm[0].version, vm[s1])

	// 2c. 0 entries
	vdm = make(VersionsDeltas, 0, 5)
	vm = vdm.VersionsMap()
	vdm1 = vm.VersionsDeltas()
	a.Equal(len(vm), 0)
	a.Equal(len(vdm1), 0)
	a.NotNil(vm)
	a.NotNil(vdm1)

	// 2d. nil list
	vdm = nil
	vm = vdm.VersionsMap()
	vdm1 = vm.VersionsDeltas()
	a.Nil(vm)
	a.Nil(vdm1)
}

func Test_versionsDeltas(t *testing.T) {
	a := assert.New(t)

	// Helper to create a pruner that prunes versions older than threshold
	makePruner := func(threshold uint64) base.PruningFunc {
		return func(ver uint64) bool {
			return ver < threshold
		}
	}

	s1 := DocumentSourceId("source1")
	s2 := DocumentSourceId("source2")
	s3 := DocumentSourceId("source3")
	s4 := DocumentSourceId("source4")
	s5 := DocumentSourceId("source5")

	// Test 1: Empty map
	t.Run("EmptyMap", func(t *testing.T) {
		vm := VersionsMap{}
		vdm, pruned := vm.versionsDeltas(nil, 0)
		a.False(pruned)
		a.Equal(0, len(vdm))
	})

	// Test 2: Nil map
	t.Run("NilMap", func(t *testing.T) {
		var vm VersionsMap
		vdm, pruned := vm.versionsDeltas(nil, 0)
		a.False(pruned)
		a.Nil(vdm)
	})

	// Test 3: Single entry, no pruning
	t.Run("SingleEntryNoPruning", func(t *testing.T) {
		vm := VersionsMap{s1: 100}
		vdm, pruned := vm.versionsDeltas(nil, 0)
		a.False(pruned)
		a.Equal(1, len(vdm))
		a.Equal(s1, vdm[0].source)
		a.Equal(uint64(100), vdm[0].version) // First entry is absolute, not delta
	})

	// Test 4: Multiple entries - verify delta conversion
	t.Run("MultipleEntriesDeltaConversion", func(t *testing.T) {
		vm := VersionsMap{s1: 100, s2: 300, s3: 600}
		vdm, pruned := vm.versionsDeltas(nil, 0)
		a.False(pruned)
		a.Equal(3, len(vdm))

		// After sorting by version: s1(100), s2(300), s3(600)
		// After delta conversion:
		// - vdm[0] = 100 (absolute)
		// - vdm[1] = 300 - 100 = 200 (delta)
		// - vdm[2] = 600 - 300 = 300 (delta)
		a.Equal(s1, vdm[0].source)
		a.Equal(uint64(100), vdm[0].version)
		a.Equal(s2, vdm[1].source)
		a.Equal(uint64(200), vdm[1].version)
		a.Equal(s3, vdm[2].source)
		a.Equal(uint64(300), vdm[2].version)
	})

	// Test 5: Entries are sorted by version
	t.Run("EntriesSortedByVersion", func(t *testing.T) {
		// Insert in reverse order
		vm := VersionsMap{s3: 300, s1: 100, s2: 200}
		vdm, _ := vm.versionsDeltas(nil, 0)
		a.Equal(3, len(vdm))

		// Should be sorted: s1(100), s2(200), s3(300)
		a.Equal(s1, vdm[0].source)
		a.Equal(s2, vdm[1].source)
		a.Equal(s3, vdm[2].source)
	})

	// Test 6: With pruning - all pruneable, minPVLen=0
	t.Run("AllPruneableminPVLenZero", func(t *testing.T) {
		vm := VersionsMap{s1: 100, s2: 200, s3: 300}
		pruner := makePruner(500)
		vdm, pruned := vm.versionsDeltas(&pruner, 0)
		a.True(pruned)
		a.Equal(0, len(vdm))
		a.Equal(0, len(vm)) // Map should be empty after pruning
	})

	// Test 7: With pruning - some pruneable
	t.Run("SomePruneable", func(t *testing.T) {
		vm := VersionsMap{s1: 100, s2: 200, s3: 300, s4: 400, s5: 500}
		pruner := makePruner(250) // s1(100) and s2(200) are pruneable
		vdm, pruned := vm.versionsDeltas(&pruner, 0)
		a.True(pruned)
		a.Equal(3, len(vdm))
		a.Equal(3, len(vm))

		// Remaining: s3(300), s4(400), s5(500)
		// After delta: 300, 100, 100
		a.Equal(s3, vdm[0].source)
		a.Equal(uint64(300), vdm[0].version)
		a.Equal(s4, vdm[1].source)
		a.Equal(uint64(100), vdm[1].version)
		a.Equal(s5, vdm[2].source)
		a.Equal(uint64(100), vdm[2].version)
	})

	// Test 8: With pruning - minPVLen prevents pruning
	t.Run("minPVLenPreventsPruning", func(t *testing.T) {
		vm := VersionsMap{s1: 100, s2: 200, s3: 300}
		pruner := makePruner(500)                    // All would be pruneable
		vdm, pruned := vm.versionsDeltas(&pruner, 5) // But minPVLen=5 > len(vm)
		a.False(pruned)
		a.Equal(3, len(vdm))
		a.Equal(3, len(vm))
	})

	// Test 9: With pruning - 6 entries, minPVLen=5, only 1 can be pruned
	t.Run("SixEntriesMinFiveOnlyOnePruned", func(t *testing.T) {
		vm := VersionsMap{s1: 100, s2: 200, s3: 300, s4: 400, s5: 500, "source6": 600}
		pruner := makePruner(1000) // All are pruneable by threshold
		vdm, pruned := vm.versionsDeltas(&pruner, 5)
		a.True(pruned)
		a.Equal(5, len(vdm))
		a.Equal(5, len(vm))

		// s1 (smallest) should be pruned
		_, exists := vm[s1]
		a.False(exists, "s1 should be pruned")
	})

	// Test 10: Delta conversion with equal versions
	t.Run("EqualVersionsDeltaZero", func(t *testing.T) {
		vm := VersionsMap{s1: 100, s2: 100, s3: 100}
		vdm, _ := vm.versionsDeltas(nil, 0)
		a.Equal(3, len(vdm))

		// All same version, deltas should be 0 except first
		a.Equal(uint64(100), vdm[0].version)
		a.Equal(uint64(0), vdm[1].version)
		a.Equal(uint64(0), vdm[2].version)
	})

	// Test 11: Large values for delta conversion
	t.Run("LargeValuesDelta", func(t *testing.T) {
		vm := VersionsMap{
			s1: 1000000000000,
			s2: 2000000000000,
			s3: 3000000000000,
		}
		vdm, _ := vm.versionsDeltas(nil, 0)
		a.Equal(3, len(vdm))

		a.Equal(uint64(1000000000000), vdm[0].version)
		a.Equal(uint64(1000000000000), vdm[1].version) // 2T - 1T = 1T
		a.Equal(uint64(1000000000000), vdm[2].version) // 3T - 2T = 1T
	})

	// Test 12: Verify map is modified when pruning
	t.Run("MapModifiedOnPruning", func(t *testing.T) {
		vm := VersionsMap{s1: 100, s2: 200, s3: 300, s4: 400, s5: 500}
		originalLen := len(vm)
		pruner := makePruner(250)
		_, pruned := vm.versionsDeltas(&pruner, 0)
		a.True(pruned)
		a.Less(len(vm), originalLen)
		a.Equal(3, len(vm))

		// Verify correct entries remain
		_, exists1 := vm[s1]
		_, exists2 := vm[s2]
		a.False(exists1)
		a.False(exists2)
		a.Equal(uint64(300), vm[s3])
		a.Equal(uint64(400), vm[s4])
		a.Equal(uint64(500), vm[s5])
	})

	// Test 13: Nil pruner should not prune
	t.Run("NilPrunerNoPruning", func(t *testing.T) {
		vm := VersionsMap{s1: 100, s2: 200, s3: 300}
		vdm, pruned := vm.versionsDeltas(nil, 0)
		a.False(pruned)
		a.Equal(3, len(vdm))
		a.Equal(3, len(vm))
	})

	// Test 14: 10 entries, all pruneable, minPVLen=5 - verify lowest 5 are deleted
	t.Run("TenEntriesAllPruneableMinFiveLowestDeleted", func(t *testing.T) {
		s6 := DocumentSourceId("source6")
		s7 := DocumentSourceId("source7")
		s8 := DocumentSourceId("source8")
		s9 := DocumentSourceId("source9")
		s10 := DocumentSourceId("source10")

		vm := VersionsMap{
			s1: 100, s2: 200, s3: 300, s4: 400, s5: 500,
			s6: 600, s7: 700, s8: 800, s9: 900, s10: 1000,
		}
		pruner := makePruner(2000) // All entries are pruneable (all < 2000)
		vdm, pruned := vm.versionsDeltas(&pruner, 5)

		a.True(pruned)
		a.Equal(5, len(vdm))
		a.Equal(5, len(vm))

		// Lowest 5 versions (100, 200, 300, 400, 500) should be pruned
		_, exists1 := vm[s1]
		_, exists2 := vm[s2]
		_, exists3 := vm[s3]
		_, exists4 := vm[s4]
		_, exists5 := vm[s5]
		a.False(exists1, "s1 (100) should be pruned")
		a.False(exists2, "s2 (200) should be pruned")
		a.False(exists3, "s3 (300) should be pruned")
		a.False(exists4, "s4 (400) should be pruned")
		a.False(exists5, "s5 (500) should be pruned")

		// Highest 5 versions (600, 700, 800, 900, 1000) should remain
		a.Equal(uint64(600), vm[s6], "s6 (600) should remain")
		a.Equal(uint64(700), vm[s7], "s7 (700) should remain")
		a.Equal(uint64(800), vm[s8], "s8 (800) should remain")
		a.Equal(uint64(900), vm[s9], "s9 (900) should remain")
		a.Equal(uint64(1000), vm[s10], "s10 (1000) should remain")

		// Verify delta conversion is correct for remaining entries
		// After sorting: s6(600), s7(700), s8(800), s9(900), s10(1000)
		// After delta: 600, 100, 100, 100, 100
		a.Equal(s6, vdm[0].source)
		a.Equal(uint64(600), vdm[0].version)
		a.Equal(s7, vdm[1].source)
		a.Equal(uint64(100), vdm[1].version)
		a.Equal(s8, vdm[2].source)
		a.Equal(uint64(100), vdm[2].version)
		a.Equal(s9, vdm[3].source)
		a.Equal(uint64(100), vdm[3].version)
		a.Equal(s10, vdm[4].source)
		a.Equal(uint64(100), vdm[4].version)
	})

	// Test 15: Equal versions - source is tiebreaker for pruning order
	// When versions are equal, entries are sorted by source (alphabetically)
	// So the entry with the "smallest" source alphabetically gets pruned first
	t.Run("EqualVersionsSourceTiebreaker", func(t *testing.T) {
		// All have version 100, sources are "aaa", "bbb", "ccc"
		sA := DocumentSourceId("aaa")
		sB := DocumentSourceId("bbb")
		sC := DocumentSourceId("ccc")

		vm := VersionsMap{sA: 100, sB: 100, sC: 100}
		pruner := makePruner(200) // All entries are pruneable (all < 200)
		vdm, pruned := vm.versionsDeltas(&pruner, 2)

		a.True(pruned)
		a.Equal(2, len(vdm))
		a.Equal(2, len(vm))

		// After sorting by version (all equal), then by source:
		// Order is: sA("aaa"), sB("bbb"), sC("ccc")
		// With minPVLen=2, maxPrunableIdx = 3 - 2 = 1
		// So only index 0 (sA) can be pruned
		_, existsA := vm[sA]
		a.False(existsA, "sA ('aaa') should be pruned - smallest source alphabetically")

		// sB and sC should remain
		a.Equal(uint64(100), vm[sB], "sB ('bbb') should remain")
		a.Equal(uint64(100), vm[sC], "sC ('ccc') should remain")

		// Verify the order in vdm: sB first, then sC
		a.Equal(sB, vdm[0].source)
		a.Equal(sC, vdm[1].source)
	})
}
