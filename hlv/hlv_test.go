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
