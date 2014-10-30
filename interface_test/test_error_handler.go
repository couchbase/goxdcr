// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package interface_test

import (
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	pipeline "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
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

func (h *testErrorHandler) Start(settings map[string]interface{}) error {
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
