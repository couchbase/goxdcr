// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package cngAgent

import (
	"context"
	"errors"
	"fmt"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/metadata_svc"
	"github.com/couchbase/goxdcr/v8/streamApiWatcher/cngWatcher"
	"google.golang.org/grpc/codes"
)

var _ metadata_svc.RemoteAgentManifestOps = &RemoteCngAgent{}

func (agent *RemoteCngAgent) OneTimeGetRemoteBucketManifest(requestOpts *base.GetManifestOpts) (*metadata.CollectionsManifest, error) {
	if err := requestOpts.Validate(); err != nil {
		return nil, err
	}

	// Validate if the remote bucket actually exists
	grpcOpts := agent.GetGrpcOpts()
	conn, err := base.NewCngConn(grpcOpts)
	if err != nil {
		agent.logger.Errorf("OneTimeGetRemoteBucketManifest: failed to create CNG connection: %v", err)
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}
	defer conn.Close()

	bucketInfoContext, bucketInfoCancel := context.WithTimeout(context.Background(), base.ShortHttpTimeout)
	defer bucketInfoCancel()

	request := &base.GrpcRequest[*internal_xdcr_v1.GetBucketInfoRequest]{
		Context: bucketInfoContext,
		Request: &internal_xdcr_v1.GetBucketInfoRequest{
			BucketName: requestOpts.BucketName,
		},
	}
	response := agent.services.utils.CngGetBucketInfo(conn.Client(), request)
	if response.Code() != codes.OK {
		agent.logger.Errorf("OneTimeGetRemoteBucketManifest: failed to get bucket info: %v", response.Message())
		return nil, fmt.Errorf("failed to verify the bucket for get manifest: %v", response.Message())
	}

	// Get the manifest from the remote cluster
	collectionsWatcher := cngWatcher.NewCollectionsWatcher(requestOpts.BucketName, agent.GetGrpcOpts, agent.services.utils, base.CollectionsWatcherWaitTime,
		base.CollectionsWatcherBackoffFactor, base.CollectionsWatcherMaxWaitTime, false, agent.logger)
	collectionsWatcher.Start()
	defer collectionsWatcher.Stop()

	context, cancel := context.WithTimeout(context.Background(), base.ShortHttpTimeout)
	defer cancel()

	res, err := collectionsWatcher.GetResult(context)
	if err != nil {
		agent.logger.Errorf("failed to get manifest for bucket %v: %v", requestOpts.BucketName, err)
		defaultManifest := metadata.NewDefaultCollectionsManifest()
		return &defaultManifest, nil
	}
	return res, nil
}

func (agent *RemoteCngAgent) GetManifest(requestOpts *base.GetManifestOpts) (*metadata.CollectionsManifest, error) {
	if err := requestOpts.Validate(); err != nil {
		return nil, err
	}

	agent.remoteDataProvider.mutex.RLock()
	watcher, ok := agent.remoteDataProvider.bucketManifestWatcher[requestOpts.BucketName]
	if !ok && !requestOpts.RestAPIQuery {
		errMsg := fmt.Sprintf("Unable to find manifest getter for bucket %v", requestOpts.BucketName)
		agent.logger.Warnf(errMsg)
		agent.remoteDataProvider.mutex.RUnlock()
		return nil, errors.New(errMsg)
	}
	agent.remoteDataProvider.mutex.RUnlock()

	if !ok && requestOpts.RestAPIQuery {
		// A manifest request via REST usually comes from the UI to display details.
		// Since replication to the target bucket probably doesn’t exist yet,
		// we need a one-time manifest fetch operation.
		return agent.OneTimeGetRemoteBucketManifest(requestOpts)
	}

	// Use an already-cancelled context for non-blocking behavior.
	// This returns immediately with the cached manifest if available, or an error if its not available yet.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := watcher.GetResult(ctx)
	if err != nil {
		agent.logger.Errorf("failed to get manifest for bucket %v: %v", requestOpts.BucketName, err)
		defaultManifest := metadata.NewDefaultCollectionsManifest()
		return &defaultManifest, nil
	}
	return result, nil
}
