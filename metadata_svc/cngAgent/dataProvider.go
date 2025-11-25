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
	"time"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/metadata_svc"
)

var _ metadata_svc.RemoteAgentManifestOps = &RemoteCngAgent{}

func (agent *RemoteCngAgent) OneTimeGetRemoteBucketManifest(requestOpts *base.GetManifestOpts) (*metadata.CollectionsManifest, error) {
	// CNG TODO: Temporary implementation for one-time manifest fetch using WatchCollections RPC.
	// This is due to a bug in CngCollections watcher
	if err := requestOpts.Validate(); err != nil {
		return nil, err
	}

	ref, err := agent.GetReferenceClone(false)
	if err != nil {
		return nil, err
	}

	conn, err := ref.NewCNGConn()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// CNG TODO: Use a proper timeout value
	// Its hardcoded because this is a temporary implementation
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()

	stream, err := conn.Client().WatchCollections(ctx, &internal_xdcr_v1.WatchCollectionsRequest{
		BucketName: requestOpts.BucketName,
	})
	if err != nil {
		err = fmt.Errorf("failed to create stream to get one time remote manifest: %w", err)
		return nil, err
	}

	var msg internal_xdcr_v1.WatchCollectionsResponse
	if err := stream.RecvMsg(&msg); err != nil {
		err = fmt.Errorf("failed to receive watch collections response for one time remote manifest: %w", err)
		return nil, err
	}

	agent.logger.Infof("successfully received one time watch collections response for bucket %v msg:%v", requestOpts.BucketName, msg)

	manifest := &metadata.CollectionsManifest{}
	manifest.LoadFromWatchCollectionsResp(&msg)

	return manifest, nil
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
	return watcher.GetResult(), nil
}
