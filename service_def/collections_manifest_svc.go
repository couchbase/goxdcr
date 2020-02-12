// Copyright (c) 2013-2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_def

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
)

/*
 * Manifest service is responsible for the following things:
 *
 * 1. Manifest Persistence
 * ------------------------
 * It needs to ensure that the necessary manifests are persisted and can be recalled when needed.
 *
 * 2. Manifest Deltas
 * -------------------
 * It needs to be able to detect manifest changes and produce a delta of the changes so the correct
 * actions can be taken
 *
 * 3. Manifest Provider
 * --------------------
 * Each DCP Nozzle and Xmem nozzle will be receiving a manifest for source and target.
 * When new manifests are present, they will receive the UID of the manifests, and utilize
 * this service for getting the actual manifests
 *
 * It utilizes agents to divvy up the responsibilities for each replication
 * Each agent is in charge of pulling ns_server to see if there are new manifests
 * It's possible that multiple agents share a same source bucket, which is what the bucketGetters
 * are for.
 * The Service itself is responsible for book-keeping and persisting all the needed manifests
 * to its local metakv for restartability needs
 *
 * 4. Manifest Getter
 * --------------------
 * The service itself is source-aware and is responsible for getting the manifest from the node's ns_server
 */

// Directionally agnostic function type to retrieve a manifest given a manifest UID
type CollectionsManifestReqFunc func(manifestUid uint64) (*metadata.CollectionsManifest, error)

type CollectionsManifestSvc interface {
	// Responsible for the retrieving manifests for source buckets
	CollectionsManifestOps

	// When pipelines start without resuming from checkpoints, use this to load the latest manifests
	GetLatestManifests(spec *metadata.ReplicationSpecification) (src, tgt *metadata.CollectionsManifest, err error)

	// Persist all manifests that are needed by this replication
	PersistNeededManifests(spec *metadata.ReplicationSpecification) error

	// API for other services (i.e. Backfill Manager) to pull for latest/changed manifests
	GetLastPersistedManifests(spec *metadata.ReplicationSpecification) (*metadata.CollectionsManifestPair, error)

	// APIs for Nozzles
	GetSpecificSourceManifest(spec *metadata.ReplicationSpecification, manifestVersion uint64) (*metadata.CollectionsManifest, error)
	GetSpecificTargetManifest(spec *metadata.ReplicationSpecification, manifestVersion uint64) (*metadata.CollectionsManifest, error)

	// This service should allow multiple calls and each call should be append
	SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback)
	ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}) error
}

type CollectionsManifestOps interface {
	// Interface agnostic of source or target to retrieve a manifests given a bucket name
	CollectionManifestGetter(bucketName string) (*metadata.CollectionsManifest, error)
}

type CollectionsManifestAgentIface interface {
	Start() error
	Stop()
	GetSourceManifest() (*metadata.CollectionsManifest, error)
	GetTargetManifest() (*metadata.CollectionsManifest, error)
	GetSpecificSourceManifest(manifestVersion uint64) (*metadata.CollectionsManifest, error)
	GetSpecificTargetManifest(manifestVersion uint64) (*metadata.CollectionsManifest, error)
	PersistNeededManifests() (error, error, bool, bool)
	GetLastPersistedManifests() (*metadata.CollectionsManifestPair, error)
}
