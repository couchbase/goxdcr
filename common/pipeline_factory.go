// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package common

import (
	"github.com/couchbase/goxdcr/v8/base"
)

type PipelineFactory interface {
	NewPipeline(topic string, progressRecorder PipelineProgressRecorder) (Pipeline, error)
	NewSecondaryPipeline(topic string, primaryPipeline Pipeline, progress_recorder PipelineProgressRecorder, pipelineType PipelineType) (Pipeline, error)

	// Due to inability to have import cycle, these APIs are called by Factory Consumers to set consumer APIs for callbacks
	SetPipelineStopCallback(cbType base.PipelineMgrStopCbType)
}
