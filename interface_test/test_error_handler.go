// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package interface_test

import (
	common "github.com/couchbase/couchbase_goxdcr/common"
	pipeline "github.com/couchbase/couchbase_goxdcr/pipeline"
	log "github.com/couchbase/couchbase_goxdcr/log"
)

var logger = log.NewLogger ("testErrorHandler", log.DefaultLoggerContext)

type testErrorHandler struct {
	pipeline common.Pipeline

	bStarted bool
}

func NewErrorHandler() *testErrorHandler {
	return &testErrorHandler{nil, false}
}

func (h *testErrorHandler) Attach(pipeline common.Pipeline) error {
	h.pipeline = pipeline
	return nil
}

func (h *testErrorHandler) Start(settings metadata.ReplicationSettingsMap) error {
	h.hookup()
	h.bStarted = true
	return nil
}

func (h *testErrorHandler) hookup() {
	parts := pipeline.GetAllParts(h.pipeline)
	for _, part := range parts {
		part.RegisterComponentEventListener(common.ErrorEncountered, h)
	}
}

func (h *testErrorHandler) Stop() error {
	h.cleanup()
	h.bStarted = false
	return nil

}

func (h *testErrorHandler) cleanup() {
	parts := pipeline.GetAllParts(h.pipeline)
	for _, part := range parts {
		part.UnRegisterComponentEventListener(common.ErrorEncountered, h)
	}
}

func (h *testErrorHandler) OnEvent(eventType common.ComponentEventType, item interface{}, component common.Component, derivedItems []interface{}, otherInfos map[string]interface{}) {
	if eventType == common.ErrorEncountered {
		logger.Errorf("Error encountered when processing %b at component %s", item, component.Id())
	}
}
