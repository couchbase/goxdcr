// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"sync"
)

type BackfillReplSvc interface {
	BackfillReplSpec(replicationId string) (*metadata.BackfillReplicationSpec, error)
	AddBackfillReplSpec(spec *metadata.BackfillReplicationSpec) error
	SetBackfillReplSpec(spec *metadata.BackfillReplicationSpec) error
	DelBackfillReplSpec(replicationId string) (*metadata.BackfillReplicationSpec, error)
	AllActiveBackfillSpecsReadOnly() (map[string]*metadata.BackfillReplicationSpec, error)

	SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback)

	// Callback for any replication spec changes from repl monitor to ensure synchronization between backfillRepl and its parent
	ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}, wg *sync.WaitGroup) error

	// In case of backfill mapping corruption, the backfillCallback is needed to capture a wide net of backfill
	SetCompleteBackfillRaiser(backfillCallback func(specId string) error) error
}
