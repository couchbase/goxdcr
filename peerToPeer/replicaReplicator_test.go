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
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestReplicatorWithSpecAndIntervalChange(t *testing.T) {
	fmt.Println("============== Test case start: TestReplicatorWithSpecAndIntervalChange =================")
	defer fmt.Println("============== Test case end: TestReplicatorWithSpecAndIntervalChange =================")
	assert := assert.New(t)

	bucketName := "bucketName"
	bucketName2 := "bucketName2"
	spec, _ := metadata.NewReplicationSpecification(bucketName, "", "", "", "")
	spec.Settings.Values[metadata.ReplicateCkptIntervalKey] = 1 // 1 minute
	spec2, _ := metadata.NewReplicationSpecification(bucketName2, "", "", "", "")
	spec2.Settings.Values[metadata.ReplicateCkptIntervalKey] = 5 // 5 minute
	specList := []*metadata.ReplicationSpecification{spec, spec2}

	xdcrComp, utilsMock, bucketSvc, replSvc, utilsReal, queryResultErrs, queryResultsStatusCode, peerNodes, myHostAddr, srcCh, ckptSvc, backfillSpecSvc, colManifestSvc, securitySvc, _ := setupBoilerPlate()
	setupMocks(utilsMock, utilsReal, xdcrComp, peerNodes, myHostAddr, specList, replSvc, queryResultErrs, queryResultsStatusCode, srcCh, nil, bucketSvc, ckptSvc, backfillSpecSvc, colManifestSvc, securitySvc, true)

	dummyFunc := func(reqs PeersVBPeriodicReplicateReqs) error {
		return nil
	}
	replicator := NewReplicaReplicator(bucketSvc, nil, ckptSvc, backfillSpecSvc, utilsMock, nil, replSvc, dummyFunc, xdcrComp)
	replicator.unitTest = true
	assert.NotNil(replicator)
	replicator.Start()

	replicator.HandleSpecCreation(spec)
	replicator.minIntervalMtx.Lock()
	assert.Equal(float64(1), replicator.minInterval.Minutes())
	replicator.minIntervalMtx.Unlock()

	newSpec := spec.Clone()
	newSpec.Settings.Values[metadata.ReplicateCkptIntervalKey] = 30 // 30 min
	_, _, _, replSvc2, _, _, _, _, _, _, _, _, _, _, _ := setupBoilerPlate()
	specList2 := []*metadata.ReplicationSpecification{newSpec, spec2}
	setupMocks(utilsMock, utilsReal, xdcrComp, peerNodes, myHostAddr, specList2, replSvc2, queryResultErrs, queryResultsStatusCode, srcCh, nil, bucketSvc, ckptSvc, backfillSpecSvc, colManifestSvc, securitySvc, true)
	replicator.replicationSpecSvc = replSvc2
	replicator.agentMapMtx.Lock()
	for _, agent := range replicator.agentMap {
		agent.replSpecSvc = replSvc2
	}
	replicator.agentMapMtx.Unlock()
	replicator.HandleSpecChange(spec, newSpec)
	time.Sleep(50 * time.Millisecond)
	replicator.minIntervalMtx.Lock()
	assert.Equal(float64(30), replicator.minInterval.Minutes())
	replicator.minIntervalMtx.Unlock()

	replicator.HandleSpecCreation(spec2)
	replicator.minIntervalMtx.Lock()
	assert.Equal(float64(5), replicator.minInterval.Minutes())
	replicator.minIntervalMtx.Unlock()

	// Deleting 5 will leave the last remaining one, 30
	replicator.HandleSpecDeletion(spec2)
	replicator.minIntervalMtx.Lock()
	assert.Equal(float64(30), replicator.minInterval.Minutes())
	replicator.minIntervalMtx.Unlock()
}

func TestReplicatorChangeSpecDuringInitWait(t *testing.T) {
	fmt.Println("============== Test case start: TestReplicatorChangeSpecDuringInitWait =================")
	defer fmt.Println("============== Test case end: TestReplicatorChangeSpecDuringInitWait =================")
	assert := assert.New(t)

	bucketName := "bucketName"
	spec, _ := metadata.NewReplicationSpecification(bucketName, "", "", "", "")
	spec.Settings.Values[metadata.ReplicateCkptIntervalKey] = 20 // by default, 20 min

	specList := []*metadata.ReplicationSpecification{spec}
	xdcrComp, utilsMock, bucketSvc, replSvc, utilsReal, queryResultErrs, queryResultsStatusCode, peerNodes, myHostAddr, srcCh, ckptSvc, backfillSpecSvc, colManifestSvc, securitySvc, _ := setupBoilerPlate()
	setupMocks(utilsMock, utilsReal, xdcrComp, peerNodes, myHostAddr, specList, replSvc, queryResultErrs, queryResultsStatusCode, srcCh, nil, bucketSvc, ckptSvc, backfillSpecSvc, colManifestSvc, securitySvc, true)

	dummyFunc := func(reqs PeersVBPeriodicReplicateReqs) error {
		return nil
	}
	replicator := NewReplicaReplicator(bucketSvc, nil, ckptSvc, backfillSpecSvc, utilsMock, nil, replSvc, dummyFunc, xdcrComp)

	assert.NotNil(replicator)
	go replicator.Start()

	replicator.unitTest = true
	replicator.unitTestInitDelaySec = 5
	replicator.HandleSpecCreation(spec)
	replicator.minIntervalMtx.Lock()
	assert.Equal(float64(20), replicator.minInterval.Minutes())
	replicator.minIntervalMtx.Unlock()

	// Now spec is changed to 1 min
	// First set up the mocks
	specChangeTo1 := spec.Clone()
	specChangeTo1.Settings.Values[metadata.ReplicateCkptIntervalKey] = 1 // 30 min
	_, _, _, replSvc2, _, _, _, _, _, _, _, _, _, _, _ := setupBoilerPlate()
	specList2 := []*metadata.ReplicationSpecification{specChangeTo1}
	setupMocks(utilsMock, utilsReal, xdcrComp, peerNodes, myHostAddr, specList2, replSvc2, queryResultErrs, queryResultsStatusCode, srcCh, nil, bucketSvc, ckptSvc, backfillSpecSvc, colManifestSvc, securitySvc, true)
	replicator.replicationSpecSvc = replSvc2
	replicator.agentMapMtx.Lock()
	for _, agent := range replicator.agentMap {
		agent.replSpecSvc = replSvc2
		// The replicator is still sleeping, so agent not running yet
		assert.False(agent.IsRunning())
	}
	replicator.agentMapMtx.Unlock()

	// Immediately change spec - this call should return immediately without changing the replicator's minInterval
	replicator.HandleSpecChange(spec, specChangeTo1)
	replicator.minIntervalMtx.Lock()
	assert.Equal(float64(20), replicator.minInterval.Minutes())
	assert.NotEqual(float64(1), replicator.minInterval.Minutes())
	replicator.minIntervalMtx.Unlock()

	// Let the agent finish booting up
	for _, agent := range replicator.agentMap {
		for !agent.IsRunning() {
			time.Sleep(100 * time.Millisecond)
		}
	}

	// After the agent finished running, the minInterval should change
	replicator.minIntervalMtx.Lock()
	assert.NotEqual(float64(20), replicator.minInterval.Minutes())
	assert.Equal(float64(1), replicator.minInterval.Minutes())
	replicator.minIntervalMtx.Unlock()

}

func TestReplicatorWithSpecAndIntervalChangeNonKVNode(t *testing.T) {
	fmt.Println("============== Test case start: TestReplicatorWithSpecAndIntervalChangeNonKVNode =================")
	defer fmt.Println("============== Test case end: TestReplicatorWithSpecAndIntervalChangeNonKVNode =================")
	assert := assert.New(t)

	bucketName := "bucketName"
	bucketName2 := "bucketName2"
	spec, _ := metadata.NewReplicationSpecification(bucketName, "", "", "", "")
	spec.Settings.Values[metadata.ReplicateCkptIntervalKey] = 1 // 1 minute
	spec2, _ := metadata.NewReplicationSpecification(bucketName2, "", "", "", "")
	spec2.Settings.Values[metadata.ReplicateCkptIntervalKey] = 5 // 5 minute
	specList := []*metadata.ReplicationSpecification{spec, spec2}

	xdcrComp, utilsMock, bucketSvc, replSvc, utilsReal, queryResultErrs, queryResultsStatusCode, peerNodes, myHostAddr, _, ckptSvc, backfillSpecSvc, colManifestSvc, securitySvc, _ := setupBoilerPlate()
	setupMocks(utilsMock, utilsReal, xdcrComp, peerNodes, myHostAddr, specList, replSvc, queryResultErrs, queryResultsStatusCode, nil, base.ErrorNoSourceNozzle, bucketSvc, ckptSvc, backfillSpecSvc, colManifestSvc, securitySvc, false)

	dummyFunc := func(reqs PeersVBPeriodicReplicateReqs) error {
		return nil
	}
	replicator := NewReplicaReplicator(bucketSvc, nil, ckptSvc, backfillSpecSvc, utilsMock, nil, replSvc, dummyFunc, xdcrComp)
	replicator.unitTest = true
	assert.NotNil(replicator)
	replicator.Start()

	replicator.HandleSpecCreation(spec)
	replicator.minIntervalMtx.Lock()
	assert.Equal(float64(1), replicator.minInterval.Minutes())
	replicator.minIntervalMtx.Unlock()

	// Do multiple spec changes to ensure that things don't lock up
	newSpec := spec.Clone()
	newSpec.Settings.Values[metadata.ReplicateCkptIntervalKey] = 30 // 30 min

	// Without the fixes, the following will lock up
	for i := 0; i < base.P2PReplicaReplicatorReloadChSize+2; i++ {
		replicator.HandleSpecChange(spec, newSpec)
	}
}
