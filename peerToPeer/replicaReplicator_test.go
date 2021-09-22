package peerToPeer

import (
	"fmt"
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

	xdcrComp, utilsMock, bucketSvc, replSvc, utilsReal, queryResultErrs, queryResultsStatusCode, peerNodes, myHostAddr, srcCh, ckptSvc, backfillSpecSvc, colManifestSvc := setupBoilerPlate()
	setupMocks(utilsMock, utilsReal, xdcrComp, peerNodes, myHostAddr, specList, replSvc, queryResultErrs, queryResultsStatusCode, srcCh, bucketSvc, ckptSvc, backfillSpecSvc, colManifestSvc)

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
	_, _, _, replSvc2, _, _, _, _, _, _, _, _, _ := setupBoilerPlate()
	specList2 := []*metadata.ReplicationSpecification{newSpec, spec2}
	setupMocks(utilsMock, utilsReal, xdcrComp, peerNodes, myHostAddr, specList2, replSvc2, queryResultErrs, queryResultsStatusCode, srcCh, bucketSvc, ckptSvc, backfillSpecSvc, colManifestSvc)
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
