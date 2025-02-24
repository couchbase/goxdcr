// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata_svc

import (
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	serviceDefMocks "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/couchbase/goxdcr/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func setupCommonMocks() (*serviceDefMocks.RemoteClusterSvc, *serviceDefMocks.CheckpointsService, *serviceDefMocks.BucketTopologySvc, *serviceDefMocks.CollectionsManifestOps, *serviceDefMocks.ManifestsService, *serviceDefMocks.PeerManifestsGetter, *serviceDefMocks.SourceNotification, *utils.Utilities) {
	remoteClusterSvc := &serviceDefMocks.RemoteClusterSvc{}
	ckptSvc := &serviceDefMocks.CheckpointsService{}
	bucketTopSvc := &serviceDefMocks.BucketTopologySvc{}
	manifestOps := &serviceDefMocks.CollectionsManifestOps{}
	manifestSvc := &serviceDefMocks.ManifestsService{}
	peerManifestGetter := &serviceDefMocks.PeerManifestsGetter{}
	notification := &serviceDefMocks.SourceNotification{}
	utils := utils.NewUtilities()

	return remoteClusterSvc, ckptSvc, bucketTopSvc, manifestOps, manifestSvc, peerManifestGetter, notification, utils
}

func setupMocksForCBSE19371() (*serviceDefMocks.RemoteClusterSvc, *serviceDefMocks.CheckpointsService, *serviceDefMocks.BucketTopologySvc, *serviceDefMocks.CollectionsManifestOps, *serviceDefMocks.ManifestsService, *serviceDefMocks.PeerManifestsGetter, *metadata.ReplicationSpecification, *utils.Utilities) {
	remoteClusterSvc, ckptSvc, bucketTopSvc, manifestOps, manifestSvc, peerManifestGetter, notification, utils := setupCommonMocks()

	spec, _ := metadata.NewReplicationSpecification("B1", "B1", "T", "B2", "B2")
	// shorten the test time of runPeriodicRefresh
	base.TopologyChangeCheckInterval = 3 * time.Second
	var subscribers map[string]chan service_def.SourceNotification = make(map[string]chan service_def.SourceNotification)

	manifestSvc.On("GetSourceManifests", mock.Anything).Return(nil, nil)
	manifestSvc.On("GetTargetManifests", mock.Anything).Return(nil, nil)

	remoteClusterSvc.On("SetBucketTopologySvc", mock.Anything).Return(nil)
	remoteClusterSvc.On("GetCapability", mock.Anything).Return(metadata.Capability{}, nil)
	remoteClusterSvc.On("RemoteClusterByUuid", mock.Anything, mock.Anything).Return(&metadata.RemoteClusterReference{}, nil)

	notification.On("GetSourceCollectionManifestUid").Return(uint64(0))
	notification.On("Recycle").Return()

	firstCh := make(chan service_def.SourceNotification, base.BucketTopologyWatcherChanLen)
	secondCh := make(chan service_def.SourceNotification, base.BucketTopologyWatcherChanLen)

	// in the CBSE this subscribe is by refreshAndNotify
	bucketTopSvc.On("SubscribeToLocalBucketFeed", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		// simulate what bucketTopologySvc keeps tracks of subscriber channels.
		subId := args.Get(1).(string)
		fmt.Printf("SubscribeToLocalBucketFeed called for refreshAndNotify\n")
		subscribers[subId] = firstCh

		fmt.Printf("Sending initial notification after %v\n", base.TopologyChangeCheckInterval+2*time.Second)
		time.AfterFunc(base.TopologyChangeCheckInterval+2*time.Second, func() {
			// base.TopologyChangeCheckInterval+2*time.Second is thought of ample time for one iteration of
			// runPeriodicRefresh to kick in.
			fmt.Printf("Initial notification generated\n")
			subscribers[subId] <- notification
		})
	}).Return(firstCh, nil).Once()

	// in the CBSE this subscribe is by runPeriodicRefresh
	bucketTopSvc.On("SubscribeToLocalBucketFeed", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		// simulate what bucketTopologySvc keeps tracks of subscriber channels.
		subId := args.Get(1).(string)
		fmt.Printf("SubscribeToLocalBucketFeed called for runPeriodicRefresh\n")
		subscribers[subId] = secondCh
	}).Return(secondCh, nil)

	bucketTopSvc.On("UnSubscribeLocalBucketFeed", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		subId := args.Get(1).(string)
		delete(subscribers, subId)
	}).Return(nil)

	return remoteClusterSvc, ckptSvc, bucketTopSvc, manifestOps, manifestSvc, peerManifestGetter, spec, utils
}

func TestCollectionsManifestAgent_Start(t *testing.T) {
	a := assert.New(t)

	remoteClusterSvc, checkpointsSvc, bucketTopoSvc, manifestOps, manifestSvc, peerManifestGetter, spec, utils := setupMocksForCBSE19371()
	logger := log.NewLogger("test", log.DefaultLoggerContext)
	manifestGetter := NewBucketManifestGetter(spec.SourceBucketName, manifestOps, time.Duration(base.ManifestRefreshSrcInterval)*time.Second, spec)
	metadataCallback := func(metadataId string, oldMetadata, newMetadata interface{}) error { return nil }

	agent := NewCollectionsManifestAgent("test", "testId", remoteClusterSvc, checkpointsSvc, bucketTopoSvc, logger, utils, spec, manifestOps, manifestGetter.GetManifest, manifestSvc, metadataCallback, peerManifestGetter.Execute)
	a.Nil(agent.Start())
	time.Sleep(2 * base.TopologyChangeCheckInterval)
	agent.Stop()
}

func TestCollectionsManifestAgent_remoteClusterHasNoCollectionsCapability(t *testing.T) {
	remoteClusterSvc, ckptSvc, bucketTopSvc, manifestOps, manifestSvc, peerManifestGetter, _, utils := setupCommonMocks()

	manifestSvc.On("GetSourceManifests", mock.Anything).Return(nil, nil)
	manifestSvc.On("GetTargetManifests", mock.Anything).Return(nil, nil)
	remoteClusterSvc.On("SetBucketTopologySvc", mock.Anything).Return(nil)
	remoteClusterSvc.On("GetCapability", mock.Anything).Return(metadata.Capability{}, nil)
	remoteClusterSvc.On("RemoteClusterByUuid", mock.Anything, mock.Anything).Return(&metadata.RemoteClusterReference{}, nil)
	notificationCh := make(chan service_def.SourceNotification, 1)
	bucketTopSvc.On("SubscribeToLocalBucketFeed", mock.Anything, mock.Anything).Return(notificationCh, nil)
	bucketTopSvc.On("UnSubscribeLocalBucketFeed", mock.Anything, mock.Anything).Return(nil)

	spec, _ := metadata.NewReplicationSpecification("B1", "B1", "T", "B2", "B2")
	logger := log.NewLogger("test", log.DefaultLoggerContext)
	manifestGetter := NewBucketManifestGetter(spec.SourceBucketName, manifestOps, time.Duration(base.ManifestRefreshSrcInterval)*time.Second, spec)
	metadataCallback := func(metadataId string, oldMetadata, newMetadata interface{}) error { return nil }
	agent := NewCollectionsManifestAgent("test", "testId", remoteClusterSvc, ckptSvc, bucketTopSvc, logger, utils, spec, manifestOps, manifestGetter.GetManifest, manifestSvc, metadataCallback, peerManifestGetter.Execute)
	var finch = make(chan bool)
	var startTime time.Time
	go func() {
		startTime = time.Now()
		agent.remoteClusterHasNoCollectionsCapability()
		close(finch)
	}()

	time.AfterFunc(10*time.Second, func() {
		agent.Start()
	})

	<-finch
	elapsedTime := time.Since(startTime)

	// agent.remoteClusterHasNoCollectionsCapability() will be blocking on `remoteClusterRefInitDone`
	// `remoteClusterRefInitDone` is closed by the agent.Start() code path which starts after 20sec
	// so if the finch is closed before 20sec then we are not achieving the required synchronization
	if elapsedTime < 10*time.Second {
		t.Fatalf("Test failed: finch closed too early after %v", elapsedTime)
	}

}
