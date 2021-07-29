// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
	"sync"
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
	// Also for validating mapping when creating/changing specs, use specMayNotExist
	GetLatestManifests(spec *metadata.ReplicationSpecification, specMayNotExist bool) (src, tgt *metadata.CollectionsManifest, err error)

	// Persist all manifests that are needed by this replication
	PersistNeededManifests(spec *metadata.ReplicationSpecification) error

	// API for other services (i.e. Backfill Manager) to pull for latest/changed manifests
	GetLastPersistedManifests(spec *metadata.ReplicationSpecification) (*metadata.CollectionsManifestPair, error)

	// APIs for Nozzles
	GetSpecificSourceManifest(spec *metadata.ReplicationSpecification, manifestVersion uint64) (*metadata.CollectionsManifest, error)
	GetSpecificTargetManifest(spec *metadata.ReplicationSpecification, manifestVersion uint64) (*metadata.CollectionsManifest, error)

	// Error handling conditions requiring more system work
	ForceTargetManifestRefresh(spec *metadata.ReplicationSpecification) error

	// This service should allow multiple calls and each call should be append
	SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback)
	ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}, wg *sync.WaitGroup) error

	// When a node passes manifests in, store them
	PersistReceivedManifests(spec *metadata.ReplicationSpecification, srcManifests, tgtManifests map[uint64]*metadata.CollectionsManifest) error

	GetAllCachedManifests(spec *metadata.ReplicationSpecification) (map[uint64]*metadata.CollectionsManifest, map[uint64]*metadata.CollectionsManifest, error)
}

type CollectionsManifestOps interface {
	// Interface agnostic of source or target to retrieve a manifests given a bucket name
	CollectionManifestGetter(bucketName string) (*metadata.CollectionsManifest, error)
}

type CollectionsManifestAgentIface interface {
	Start() error
	Stop()
	ForceTargetManifestRefresh() error
	GetSourceManifest() (*metadata.CollectionsManifest, error)
	GetTargetManifest() (*metadata.CollectionsManifest, error)
	GetSpecificSourceManifest(manifestVersion uint64) (*metadata.CollectionsManifest, error)
	GetSpecificTargetManifest(manifestVersion uint64) (*metadata.CollectionsManifest, error)
	PersistNeededManifests() (error, error, bool, bool)
	GetLastPersistedManifests() (*metadata.CollectionsManifestPair, error)
	SetTempAgent()
}
