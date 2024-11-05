// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package common

import (
	"github.com/couchbase/goxdcr/v8/metadata"
)

// PipelineService can be any component that monitors, does logging, keeps state for the pipeline
// Each PipelineService is a goroutine that run parallelly
type PipelineService interface {
	Attach(pipeline Pipeline) error

	Start(metadata.ReplicationSettingsMap) error
	Stop() error

	UpdateSettings(settings metadata.ReplicationSettingsMap) error

	// If a service can be shared by more than one pipeline in the same replication
	IsSharable() bool
	// If sharable, allow service to be detached
	Detach(pipeline Pipeline) error
}
type PipelineSupervisorSvc interface {
	PipelineService
	ComponentEventListener
}
