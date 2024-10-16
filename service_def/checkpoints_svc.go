// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import (
	"sync"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
)

type CheckpointsService interface {
	CheckpointsDoc(replicationId string, vbno uint16) (*metadata.CheckpointsDoc, error)
	DelCheckpointsDoc(replicationId string, vbno uint16, specInternalId string) error
	DelCheckpointsDocs(replicationId string) error
	UpsertCheckpoints(replicationId string, specInternalId string, vbno uint16, ckpt_record *metadata.CheckpointRecord) (int, error)
	UpsertCheckpointsDone(replicationId string, internalId string) error // Used by ckmgr to notify that it is done with individual VBs
	CheckpointsDocs(replicationId string, brokenMappingsNeeded bool) (map[uint16]*metadata.CheckpointsDoc, error)
	GetVbnosFromCheckpointDocs(replicationId string) ([]uint16, error)
	PreUpsertBrokenMapping(replicationId string, specInternalId string, oneBrokenMapping *metadata.CollectionNamespaceMapping) error
	UpsertBrokenMapping(replicationId string, specInternalId string) error

	CollectionsManifestChangeCb(metadataId string, oldMetadata interface{}, newMetadata interface{}) error
	ReplicationSpecChangeCallback(metadataId string, oldMetadata interface{}, newMetadata interface{}, wg *sync.WaitGroup) error
	BackfillReplicationSpecChangeCallback(metadataId string, oldMetadata interface{}, newMetadata interface{}) error

	GetCkptsMappingsCleanupCallback(specId, specInternalId string, toBeRemoved metadata.ScopesMap) (base.StoppedPipelineCallback, base.StoppedPipelineErrCallback)

	LoadBrokenMappings(replicationId string) (metadata.ShaToCollectionNamespaceMap, *metadata.CollectionNsMappingsDoc, IncrementerFunc, bool, error)
	UpsertAndReloadCheckpointCompleteSet(replicationId string, mappingDoc *metadata.CollectionNsMappingsDoc, ckptDoc map[uint16]*metadata.CheckpointsDoc, internalId string) error
	DisableRefCntDecrement(topic string)
	EnableRefCntDecrement(topic string)
}

type IncrementerFunc func(shaString string, valueToCount interface{})

type DecrementerFunc func(shaString string)
