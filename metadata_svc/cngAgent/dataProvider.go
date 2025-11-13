// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package cngAgent

import (
	"errors"
	"fmt"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/metadata_svc"
	"github.com/couchbase/goxdcr/v8/streamApiWatcher/cngWatcher"
)

var _ metadata_svc.RemoteAgentManifestOps = &RemoteCngAgent{}

func (agent *RemoteCngAgent) OneTimeGetRemoteBucketManifest(requestOpts *base.GetManifestOpts) (*metadata.CollectionsManifest, error) {
	if err := requestOpts.Validate(); err != nil {
		return nil, err
	}

	collectionsWatcher := cngWatcher.NewCollectionsWatcher(requestOpts.BucketName, agent.GetGrpcOpts, agent.services.utils, base.CollectionsWatcherWaitTime,
		base.CollectionsWatcherBackoffFactor, base.CollectionsWatcherMaxWaitTime, false, agent.logger)
	collectionsWatcher.Start()
	defer collectionsWatcher.Stop()
	res := collectionsWatcher.GetResult()
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
	return watcher.GetResult(), nil
}
