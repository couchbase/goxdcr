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
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
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

	xdcrComp, utilsMock, bucketSvc, replSvc, utilsReal, queryResultErrs, queryResultsStatusCode, peerNodes, myHostAddr, srcCh, ckptSvc, backfillSpecSvc, colManifestSvc, securitySvc, _, remClusterSvc := setupBoilerPlate()
	setupMocks(utilsMock, utilsReal, xdcrComp, peerNodes, myHostAddr, specList, replSvc, queryResultErrs, queryResultsStatusCode, srcCh, nil, bucketSvc, ckptSvc, backfillSpecSvc, colManifestSvc, securitySvc, remClusterSvc)

	dummyFunc := func(reqs PeersVBPeriodicReplicateReqs) error {
		return nil
	}
	replicator := NewReplicaReplicator(bucketSvc, nil, ckptSvc, backfillSpecSvc, utilsMock, nil, replSvc, dummyFunc)
	replicator.unitTest = true
	assert.NotNil(replicator)
	replicator.Start()

	replicator.HandleSpecCreation(spec)
	replicator.minIntervalMtx.Lock()
	assert.Equal(float64(1), replicator.minInterval.Minutes())
	replicator.minIntervalMtx.Unlock()

	newSpec := spec.Clone()
	newSpec.Settings.Values[metadata.ReplicateCkptIntervalKey] = 30 // 30 min
	_, _, _, replSvc2, _, _, _, _, _, _, _, _, _, _, _, _ := setupBoilerPlate()
	specList2 := []*metadata.ReplicationSpecification{newSpec, spec2}
	setupMocks(utilsMock, utilsReal, xdcrComp, peerNodes, myHostAddr, specList2, replSvc2, queryResultErrs, queryResultsStatusCode, srcCh, nil, bucketSvc, ckptSvc, backfillSpecSvc, colManifestSvc, securitySvc, remClusterSvc)
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

	xdcrComp, utilsMock, bucketSvc, replSvc, utilsReal, queryResultErrs, queryResultsStatusCode, peerNodes, myHostAddr, _, ckptSvc, backfillSpecSvc, colManifestSvc, securitySvc, _, remClusterSvc := setupBoilerPlate()
	setupMocks(utilsMock, utilsReal, xdcrComp, peerNodes, myHostAddr, specList, replSvc, queryResultErrs, queryResultsStatusCode, nil, base.ErrorNoSourceNozzle, bucketSvc, ckptSvc, backfillSpecSvc, colManifestSvc, securitySvc, remClusterSvc)

	dummyFunc := func(reqs PeersVBPeriodicReplicateReqs) error {
		return nil
	}
	replicator := NewReplicaReplicator(bucketSvc, nil, ckptSvc, backfillSpecSvc, utilsMock, nil, replSvc, dummyFunc)
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
