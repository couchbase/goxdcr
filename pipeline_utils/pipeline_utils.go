// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package pipeline_utils

import (
	"strconv"
	"strings"

	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/parts"
)

func GetSourceVBListPerPipeline(pipeline common.Pipeline) []uint16 {
	ret := []uint16{}
	sourceNozzles := pipeline.Sources()
	for _, sourceNozzle := range sourceNozzles {
		responsibleVBs := sourceNozzle.ResponsibleVBs()
		ret = append(ret, responsibleVBs...)
	}
	return ret
}

func GetTargetVBListPerPipeline(pipeline common.Pipeline) []uint16 {
	ret := []uint16{}
	targetNozzles := pipeline.Targets()
	for _, targetNozzle := range targetNozzles {
		responsibleVBs := targetNozzle.ResponsibleVBs()
		ret = append(ret, responsibleVBs...)
	}
	return ret
}

func GetElementIdFromName(pipeline common.Pipeline, name string) string {
	return pipeline.FullTopic() + "_" + name
}

func GetElementIdFromNameAndIndex(pipeline common.Pipeline, name string, index int) string {
	return pipeline.FullTopic() + "_" + name + "_" + strconv.Itoa(index)
}

// as the name indicates, this method works only on element ids with index at the end
func GetElementNameFromIdWithIndex(id string) string {
	parts := strings.Split(id, "_")
	if len(parts) > 2 {
		return parts[len(parts)-2]
	}
	return ""
}

func RegisterAsyncComponentEventHandler(listenerMap map[string]common.AsyncComponentEventListener, listenerName string, handler common.AsyncComponentEventHandler) {
	for id, listener := range listenerMap {
		if listenerName == GetElementNameFromIdWithIndex(id) {
			listener.RegisterComponentEventHandler(handler)
		}
	}
}

func IsPipelineUsingCapi(pipeline common.Pipeline) bool {
	isCapi := false
	for _, target := range pipeline.Targets() {
		_, isCapi = target.(*parts.CapiNozzle)
		break
	}
	return isCapi
}

// check if the specified state indicates that the pipeline is still running
// many components, e.g., topology change detector, use this to decide if they need to abort operation
func IsPipelineRunning(state common.PipelineState) bool {
	return state == common.Pipeline_Initial || state == common.Pipeline_Starting || state == common.Pipeline_Running
}

func IsPipelineStopping(state common.PipelineState) bool {
	return state == common.Pipeline_Stopping || state == common.Pipeline_Stopped
}
