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

var _ metadata_svc.RemoteCngAgentDataProvider = &RemoteCngAgent{}

func (agent *RemoteCngAgent) OneTimeGetRemoteBucketManifest(bucketName string) (*metadata.CollectionsManifest, error) {
	collectionsWatcher := cngWatcher.NewCollectionsWatcher(bucketName, agent.GetGrpcOpts, agent.services.utils, base.CollectionsWatcherWaitTime,
		base.CollectionsWatcherBackoffFactor, base.CollectionsWatcherMaxWaitTime, false, agent.logger)
	collectionsWatcher.Start()
	defer collectionsWatcher.Stop()
	res := collectionsWatcher.GetResult()
	if res == nil {
		return nil, fmt.Errorf("failed to get manifest for the target bucket %v", bucketName)
	}
	return res, nil
}

func (agent *RemoteCngAgent) GetManifest(bucketName string, restAPIQuery bool) (*metadata.CollectionsManifest, error) {
	agent.remoteDataProvider.mutex.RLock()
	watcher, ok := agent.remoteDataProvider.bucketManifestWatcher[bucketName]
	if !ok && !restAPIQuery {
		errMsg := fmt.Sprintf("Unable to find manifest getter for bucket %v", bucketName)
		agent.logger.Warnf(errMsg)
		agent.remoteDataProvider.mutex.RUnlock()
		return nil, errors.New(errMsg)
	}
	agent.remoteDataProvider.mutex.RUnlock()

	if restAPIQuery {
		// A manifest request via REST usually comes from the UI to display details.
		// Since replication to the target bucket probably doesn’t exist yet,
		// we need a one-time manifest fetch operation.
		return agent.OneTimeGetRemoteBucketManifest(bucketName)
	}
	return watcher.GetResult(), nil
}
