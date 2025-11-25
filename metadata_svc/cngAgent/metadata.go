// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package cngAgent

import (
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/metadata_svc"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/couchbase/goxdcr/v8/streamApiWatcher/cngWatcher"
)

var _ metadata_svc.RemoteAgentMetadata = &RemoteCngAgent{}

func (agent *RemoteCngAgent) Id() string {
	agent.refCache.mutex.RLock()
	defer agent.refCache.mutex.RUnlock()
	return agent.refCache.reference.Id()
}

func (agent *RemoteCngAgent) Name() string {
	agent.refCache.mutex.RLock()
	defer agent.refCache.mutex.RUnlock()
	return agent.refCache.reference.Name()
}

func (agent *RemoteCngAgent) HostName() string {
	agent.refCache.mutex.RLock()
	defer agent.refCache.mutex.RUnlock()
	return agent.refCache.reference.HostName()
}

func (agent *RemoteCngAgent) Uuid() string {
	agent.refCache.mutex.RLock()
	defer agent.refCache.mutex.RUnlock()
	return agent.refCache.reference.Uuid()
}

func (agent *RemoteCngAgent) GetReferenceClone(refresh bool) (*metadata.RemoteClusterReference, error) {
	if !agent.InitDone() {
		// If init is not done, return an error
		// Since some of the callers check for this exact error, we'll stick to returning this
		// in case of init not done
		return nil, metadata_svc.RefreshNotEnabledYet
	}

	getClone := func() *metadata.RemoteClusterReference {
		agent.refCache.mutex.RLock()
		defer agent.refCache.mutex.RUnlock()
		return agent.refCache.reference.Clone()
	}

	if refresh {
		if err := agent.Refresh(); err != nil {
			// Log the error and return the last known reference
			agent.logger.Warnf("%v: GetReferenceClone encountered error during refresh: %v", agent.Name(), err)
			return getClone(), nil
		}
	}

	return getClone(), nil
}

func (agent *RemoteCngAgent) GetReferenceAndStatusClone() *metadata.RemoteClusterReference {
	for !agent.InitDone() {
		time.Sleep(10 * time.Millisecond)
	}

	ref, _ := agent.GetReferenceClone(false)
	if _, err := ref.MyConnectionStr(); err != nil && strings.Contains(err.Error(), base.IpFamilyOnlyErrorMessage) {
		agent.healthTracker.connectivityHelper.MarkIpFamilyError(true)
	} else {
		agent.healthTracker.connectivityHelper.MarkIpFamilyError(false)
	}

	connectivityStatus := agent.GetConnectivityStatus()
	ref.SetConnectivityStatus(connectivityStatus.String())
	if connectivityStatus == metadata.ConnIniting || connectivityStatus == metadata.ConnValid {
		ref.ClearConnErrs()
	}
	return ref
}

func (agent *RemoteCngAgent) GetCapability() (metadata.Capability, error) {
	// Darshan TODO: implement MB-68864
	// CNG TODO: temparily return all enabled capability
	return *metadata.AllEnabledCapability(), nil
}

// CAPI is not used anymore and will not be supported for CNG targets
func (agent *RemoteCngAgent) GetConnectionStringForCAPIRemoteCluster() (string, error) {
	// no-op
	return "", nil
}

func (agent *RemoteCngAgent) IsSame(ref *metadata.RemoteClusterReference) bool {
	agent.refCache.mutex.RLock()
	defer agent.refCache.mutex.RUnlock()
	return agent.refCache.reference.IsSame(ref)
}

// There isn't a concept of alternate address in case of CNG
func (agent *RemoteCngAgent) UsesAlternateAddress() (bool, error) {
	// no-op
	return false, nil
}

// -------- RemoteAgentConfig Methods ----------
var _ metadata_svc.RemoteAgentConfig = &RemoteCngAgent{}

func (agent *RemoteCngAgent) SetCapability(capability metadata.Capability) {
	// Darshan TODO: implement MB-68864
}

func (agent *RemoteCngAgent) SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback) {
	agent.mutex.Lock()
	defer agent.mutex.Unlock()
	agent.metadataChangeCallback = callBack
}

func (agent *RemoteCngAgent) RegisterBucketRequest(bucketName string) error {
	agent.remoteDataProvider.mutex.Lock()
	defer agent.remoteDataProvider.mutex.Unlock()
	if _, ok := agent.remoteDataProvider.bucketManifestWatcher[bucketName]; !ok {
		agent.remoteDataProvider.bucketManifestWatcher[bucketName] = cngWatcher.NewCollectionsWatcher(bucketName, agent.GetGrpcOpts, agent.services.utils, base.CollectionsWatcherWaitTime,
			base.CollectionsWatcherBackoffFactor, base.CollectionsWatcherMaxWaitTime, true, agent.logger)
		agent.remoteDataProvider.bucketManifestWatcher[bucketName].Start()
	}
	agent.remoteDataProvider.refCount[bucketName]++
	return nil
}

func (agent *RemoteCngAgent) UnRegisterBucketRefresh(bucketName string) error {
	agent.remoteDataProvider.mutex.Lock()
	defer agent.remoteDataProvider.mutex.Unlock()

	_, ok := agent.remoteDataProvider.refCount[bucketName]
	if !ok {
		return fmt.Errorf("unRegisterBucketRefresh() called for bucket %v but it is not registered", bucketName)
	}

	if agent.remoteDataProvider.refCount[bucketName] > uint32(0) {
		agent.remoteDataProvider.refCount[bucketName]--
	}

	if agent.remoteDataProvider.refCount[bucketName] == uint32(0) {
		agent.remoteDataProvider.bucketManifestWatcher[bucketName].Stop()
		delete(agent.remoteDataProvider.bucketManifestWatcher, bucketName)
	}
	return nil
}

func (agent *RemoteCngAgent) SetHeartbeatApi(api service_def.ClusterHeartbeatAPI) {
	// no-op
}

func (agent *RemoteCngAgent) SetReplReader(reader service_def.ReplicationSpecReader) {
	agent.heartbeatManager.mutex.Lock()
	defer agent.heartbeatManager.mutex.Unlock()

	agent.heartbeatManager.specsReader = reader
}
