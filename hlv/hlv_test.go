/*
Copyright 2024-Present Couchbase, Inc.

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

	"github.com/stretchr/testify/assert"
)

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
