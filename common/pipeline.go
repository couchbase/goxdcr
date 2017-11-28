// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package common

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
)

type PipelineState int

const (
	Pipeline_Initial  PipelineState = iota
	Pipeline_Starting PipelineState = iota
	Pipeline_Running  PipelineState = iota
	Pipeline_Stopping PipelineState = iota
	Pipeline_Stopped  PipelineState = iota
	Pipeline_Error    PipelineState = iota
)

type PipelineProgressRecorder func(progress string)

//interface for Pipeline
type Pipeline interface {
	//Name of the Pipeline
	Topic() string

	Sources() map[string]Nozzle
	Targets() map[string]Nozzle

	//getter\setter of the runtime environment
	RuntimeContext() PipelineRuntimeContext
	SetRuntimeContext(ctx PipelineRuntimeContext)

	//start the data exchange
	Start(settings map[string]interface{}) base.ErrorMap
	//stop the data exchange
	Stop() base.ErrorMap

	Specification() *metadata.ReplicationSpecification
	Settings() map[string]interface{}

	State() PipelineState
	SetState(state PipelineState) error
	InstanceId() string

	SetProgressRecorder(recorder PipelineProgressRecorder)
	ReportProgress(progress string)

	UpdateSettings(settings map[string]interface{}) error
}
