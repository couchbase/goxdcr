// Copyright (c) 2013-2019 Couchbase, Inc.
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
 * Source and target each has 1024 vb's, and the manifest service needs to keep track of each vb for each
 * source and target of which manifest uid each vb is currently on.
 *
 * When ckpt manager perform checkpoints for each vb, it will ask the the collectionsManifestSvc for the manifest UID
 * corresponding to the vb stream. It is vital that the manifest service returns the currently active manifest
 * being used for the vb stream. If the manifest returns an "ahead" manifest for checkpointing, then it could
 * lead to missing backfill.
 * <example>
 * Imagine manifest uid 1 with target collection uid 3. During checkpointing, target collection gets recreated
 * with uid 4 and target manifest rolls to uid 2, and it gets pulled and checkpointed instead of the 3 xmem orig
 * knows.
 * When resuming, xmem will erroneously send target manifest uid 4 instead of 3, and target kv will accept it.
 * The correct thing is to send manifest uid 3 (with collection uid of 3), and have target reject it, which will
 * cause backfill manager to start a backfill for the new target collection.
 *
 * It utilizes agents to divvy up the responsibilities for each replication
 * Each agent is in charge of pulling ns_server to see if there are new manifests
 * It's possible that multiple agents share a same source bucket, which is what the bucketGetters
 * are for.
 * The Service itself is responsible for book-keeping and persisting all the needed manifests
 * to its local metakv for restartability needs
 */

type CollectionsManifestPartsFunc func(vblist []uint16) *metadata.CollectionsManifest
type CollectionsManifestReqFunc func(manifestUid uint64) (*metadata.CollectionsManifest, error)

type CollectionsManifestSvc interface {
	CollectionsManifestOps

	GetLatestManifests(spec *metadata.ReplicationSpecification) (src, tgt *metadata.CollectionsManifest, err error)

	// Checkpointing
	PersistNeededManifests(spec *metadata.ReplicationSpecification) error
	GetOngoingManifests(spec *metadata.ReplicationSpecification, vb uint16) (*metadata.CollectionsManifestPair, error)

	// APIs for Nozzles
	GetSourceManifestForNozzle(spec *metadata.ReplicationSpecification, vblist []uint16) *metadata.CollectionsManifest
	GetTargetManifestForNozzle(spec *metadata.ReplicationSpecification, vblist []uint16) *metadata.CollectionsManifest
	GetSpecificSourceManifest(spec *metadata.ReplicationSpecification, manifestVersion uint64) (*metadata.CollectionsManifest, error)
	GetSpecificTargetManifest(spec *metadata.ReplicationSpecification, manifestVersion uint64) (*metadata.CollectionsManifest, error)

	// This service should allow multiple calls and each call should be append
	SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback)
	ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}) error
}

type CollectionsManifestOps interface {
	CollectionManifestGetter(bucketName string) (*metadata.CollectionsManifest, error)
}

type CollectionsManifestAgentIface interface {
	Start() error
	Stop()
	GetSourceManifest() *metadata.CollectionsManifest
	GetAndRecordSourceManifest(vblist []uint16) *metadata.CollectionsManifest
	GetTargetManifest() *metadata.CollectionsManifest
	GetOngoingManifests(vb uint16) *metadata.CollectionsManifestPair
	PersistNeededManifests() (error, error, bool, bool)
}
