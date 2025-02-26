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

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	serviceDefMocks "github.com/couchbase/goxdcr/v8/service_def/mocks"
	"github.com/couchbase/goxdcr/v8/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func setupCommonMocks() (*serviceDefMocks.RemoteClusterSvc, *serviceDefMocks.CheckpointsService, *serviceDefMocks.BucketTopologySvc, *serviceDefMocks.CollectionsManifestOps, *serviceDefMocks.ManifestsService, *serviceDefMocks.PeerManifestsGetter, *serviceDefMocks.SourceNotification) {
	remoteClusterSvc := &serviceDefMocks.RemoteClusterSvc{}
	ckptSvc := &serviceDefMocks.CheckpointsService{}
	bucketTopSvc := &serviceDefMocks.BucketTopologySvc{}
	manifestOps := &serviceDefMocks.CollectionsManifestOps{}
	manifestSvc := &serviceDefMocks.ManifestsService{}
	peerManifestGetter := &serviceDefMocks.PeerManifestsGetter{}
	notification := &serviceDefMocks.SourceNotification{}

	return remoteClusterSvc, ckptSvc, bucketTopSvc, manifestOps, manifestSvc, peerManifestGetter, notification
}

func setupMocksForCBSE19371() (*serviceDefMocks.RemoteClusterSvc, *serviceDefMocks.CheckpointsService, *serviceDefMocks.BucketTopologySvc, *serviceDefMocks.CollectionsManifestOps, *serviceDefMocks.ManifestsService, *serviceDefMocks.PeerManifestsGetter, *metadata.ReplicationSpecification) {
	remoteClusterSvc, ckptSvc, bucketTopSvc, manifestOps, manifestSvc, peerManifestGetter, notification := setupCommonMocks()

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

	return remoteClusterSvc, ckptSvc, bucketTopSvc, manifestOps, manifestSvc, peerManifestGetter, spec
}

func TestCollectionsManifestAgent_Start(t *testing.T) {
	a := assert.New(t)

	remoteClusterSvc, checkpointsSvc, bucketTopoSvc, manifestOps, manifestSvc, peerManifestGetter, spec := setupMocksForCBSE19371()
	logger := log.NewLogger("test", log.DefaultLoggerContext)
	utils := utils.NewUtilities()
	manifestGetter := NewBucketManifestGetter(spec.SourceBucketName, manifestOps, time.Duration(base.ManifestRefreshSrcInterval)*time.Second, spec)
	metadataCallback := func(metadataId string, oldMetadata, newMetadata interface{}) error { return nil }

	agent := NewCollectionsManifestAgent("test", "testId", remoteClusterSvc, checkpointsSvc, bucketTopoSvc, logger, utils, spec, manifestOps, manifestGetter.GetManifest, manifestSvc, metadataCallback, peerManifestGetter.Execute)
	a.Nil(agent.Start())
	time.Sleep(2 * base.TopologyChangeCheckInterval)
	agent.Stop()
}
