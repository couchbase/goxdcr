package peerToPeer

import (
	"fmt"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReplicatorWithSpecAndIntervalChange(t *testing.T) {
	fmt.Println("============== Test case start: TestReplicatorWithSpecAndIntervalChange =================")
	defer fmt.Println("============== Test case end: TestReplicatorWithSpecAndIntervalChange =================")
	assert := assert.New(t)

	bucketName := "bucketName"
	spec, _ := metadata.NewReplicationSpecification(bucketName, "", "", "", "")
	spec.Settings.Values[metadata.ReplicateCkptIntervalKey] = 1 // 1 minute
	specList := []*metadata.ReplicationSpecification{spec}

	xdcrComp, utilsMock, bucketSvc, replSvc, utilsReal, queryResultErrs, queryResultsStatusCode, peerNodes, myHostAddr, srcCh, ckptSvc, backfillSpecSvc := setupBoilerPlate()
	setupMocks(utilsMock, utilsReal, xdcrComp, peerNodes, myHostAddr, specList, replSvc, queryResultErrs, queryResultsStatusCode, srcCh, bucketSvc, ckptSvc, backfillSpecSvc)

	commAPI := NewP2pCommAPIHelper(nil, utilsMock, xdcrComp)
	replicator := NewReplicaReplicator(bucketSvc, nil, ckptSvc, backfillSpecSvc, commAPI, utilsMock)
	assert.NotNil(replicator)
	replicator.HandleSpecCreation(spec)

	newSpec := spec.Clone()
	newSpec.Settings.Values[metadata.ReplicateCkptIntervalKey] = 30 // 30 min
	replicator.HandleSpecChange(spec, newSpec)
}
