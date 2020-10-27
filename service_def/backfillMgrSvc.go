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
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/metadata"
)

type BackfillMgrIface interface {
	Start() error
	Stop()
	ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}) error

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
