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
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/metadata"
	"sync"
)

type BackfillMgrIface interface {
	Start() error
	Stop()
	ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}, wg *sync.WaitGroup) error

	// Backfill manager can act as a pipeline service and handle specific pipeline needs
	GetPipelineSvc() common.PipelineService

	// When explicit mapping changes, this callback should be used in between updates once pipelines have stopped
	GetExplicitMappingChangeHandler(specId, internalSpecId string, oldSettings, newSettings *metadata.ReplicationSettings) (base.StoppedPipelineCallback, base.StoppedPipelineErrCallback)
	GetRouterMappingChangeHandler(specId, internalSpecId string, diff metadata.CollectionNamespaceMappingsDiffPair) (base.StoppedPipelineCallback, base.StoppedPipelineErrCallback)
	GetLastSuccessfulSourceManifestId(specId string) (uint64, error)
	SetLastSuccessfulSourceManifestId(specId string, manifestId uint64, dcpRollbackScenario bool, finCh chan bool) error
}

type BackfillMgrComponentListenerGetter interface {
	// Backfill manager can act as a component event listener given a pipeline
	GetComponentEventListener(pipeline common.Pipeline) (common.ComponentEventListener, error)
}
