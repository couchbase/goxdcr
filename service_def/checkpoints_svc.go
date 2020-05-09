// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_def

import (
	"github.com/couchbase/goxdcr/metadata"
)

type CheckpointsService interface {
	CheckpointsDoc(replicationId string, vbno uint16) (*metadata.CheckpointsDoc, error)
	DelCheckpointsDoc(replicationId string, vbno uint16) error
	PostDelCheckpointsDoc(replicationId string, doc *metadata.CheckpointsDoc) (modified bool, err error)
	DelCheckpointsDocs(replicationId string) error
	UpsertCheckpoints(replicationId string, specInternalId string, vbno uint16, ckpt_record *metadata.CheckpointRecord,
		xattr_seqno uint64, targetClusterVersion int) (int, error)
	CheckpointsDocs(replicationId string, brokenMappingsNeeded bool) (map[uint16]*metadata.CheckpointsDoc, error)
	GetVbnosFromCheckpointDocs(replicationId string) ([]uint16, error)
	PreUpsertBrokenMapping(replicationId string, specInternalId string, oneBrokenMapping *metadata.CollectionNamespaceMapping) error
	UpsertBrokenMapping(replicationId string, specInternalId string) error
}
