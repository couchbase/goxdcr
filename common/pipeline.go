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
	"fmt"
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

const backfillPipelineTopicPrefix = "backfill_"

type PipelineType int

const (
	MainPipeline     PipelineType = iota
	BackfillPipeline PipelineType = iota
)

func (p PipelineType) String() string {
	switch p {
	case MainPipeline:
		return "MainPipeline"
	case BackfillPipeline:
		return "BackfillPipeline"
	default:
		return "?? (PipelineType)"
	}
}

func ComposeFullTopic(topic string, t PipelineType) string {
	switch t {
	case MainPipeline:
		return topic
	case BackfillPipeline:
		return fmt.Sprintf("%v%v", backfillPipelineTopicPrefix, topic)
	default:
		return "Unable to composeFullTopic"
	}
}

type PipelineProgressRecorder func(progress string)

//interface for Pipeline
type Pipeline interface {
	//Name of the Pipeline
	Topic() string
	// Name of the pipeline and inferred type
	FullTopic() string

	Type() PipelineType

	Sources() map[string]Nozzle
	Targets() map[string]Nozzle

	//getter\setter of the runtime environment
	RuntimeContext() PipelineRuntimeContext
	SetRuntimeContext(ctx PipelineRuntimeContext)

	//start the data exchange
	Start(settings metadata.ReplicationSettingsMap) base.ErrorMap
	//stop the data exchange
	Stop() base.ErrorMap

	// Return the genericSpec that can be further used depending on pipeline type
	Specification() metadata.GenericSpecification
	Settings() metadata.ReplicationSettingsMap

	State() PipelineState
	SetState(state PipelineState) error
	InstanceId() string

	SetProgressRecorder(recorder PipelineProgressRecorder)
	ReportProgress(progress string)

	UpdateSettings(settings metadata.ReplicationSettingsMap) error

	GetAsyncListenerMap() map[string]AsyncComponentEventListener
	SetAsyncListenerMap(map[string]AsyncComponentEventListener)
}
