// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package common

import (
	"strings"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/conflictlog"
	"github.com/couchbase/goxdcr/v8/metadata"
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

type PipelineType int

const (
	MainPipeline     PipelineType = iota
	BackfillPipeline PipelineType = iota

	PipelineTypeBegin      = MainPipeline
	PipelineTypeInvalidEnd = BackfillPipeline + 1
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
		return base.CompileBackfillPipelineSpecId(topic)
	default:
		return "Unable to composeFullTopic"
	}
}

func DecomposeFullTopic(fullTopic string) (string, PipelineType) {
	if strings.HasPrefix(fullTopic, base.BackfillPipelineTopicPrefix) {
		return base.GetMainPipelineSpecIdFromBackfillId(fullTopic), BackfillPipeline
	} else {
		return fullTopic, MainPipeline
	}
}

type PipelineProgressRecorder func(progress string)

const (
	ProgressStartConstruction = "Start pipeline construction"
	ProgressConstructionDone  = "Pipeline is constructed"
	ProgressNozzlesWired      = "Source nozzles have been wired to target nozzles"
	ProgressP2PComm           = "Performing PeerToPeer communication"
	ProgressP2PMerge          = "Performing PeerToPeer metadata merging"
	ProgressP2PDoneMerge      = "Done PeerToPeer communication and metadata merging"
	ProgressRuntimeCtxStarted = "The runtime context has been started"
	ProgressPartsStarted      = "All parts have been started"
	ProgressOutNozzleOpened   = "All outgoing nozzles have been opened"
	ProgressInNozzleOpened    = "All incoming nozzles have been opened"
	ProgressPipelineRunning   = "Pipeline is running"

	ProgressAsyncStopped      = "Async listeners have been stopped"
	ProgressRuntimeCtxStopped = "Runtime context has been stopped"
	ProgressSrcNozzleClose    = "Source nozzles have been closed"
	ProgressPipelineStopped   = "Pipeline has been stopped"
)

// interface for Pipeline
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

	GetBrokenMapRO() (brokenMapReadOnly metadata.CollectionNamespaceMapping, callOnceDone func())
	SetBrokenMap(brokenMap metadata.CollectionNamespaceMapping)

	GetRebalanceProgress() (string, string)

	ConflictLogger() conflictlog.Logger
}
