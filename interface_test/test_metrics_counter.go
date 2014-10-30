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
	common "github.com/Xiaomei-Zhang/goxdcr/common"
	"github.com/Xiaomei-Zhang/goxdcr/log"
	"sync"
)

var logger_metrics = log.NewLogger ("testMetricsCollector", log.DefaultLoggerContext)

type testMetricsCollector struct {
	pipeline common.Pipeline

	bStarted bool

	counter int
	counterLock sync.Mutex
}

func NewMetricsCollector() *testMetricsCollector {
	return &testMetricsCollector{nil, false, 0, sync.Mutex{}}
}

func (h *testMetricsCollector) Attach(pipeline common.Pipeline) error {
	h.pipeline = pipeline
	return nil
}

func (h *testMetricsCollector) Start(settings map[string]interface{}) error {
	h.hookup()
	h.bStarted = true
	return nil
}

func (h *testMetricsCollector) hookup() {
	h.counter = 0
	targets := h.pipeline.Targets()
	for _, target := range targets {
		target.RegisterComponentEventListener(common.DataSent, h)
	}
}

func (h *testMetricsCollector) Stop() error {
	h.cleanup()
	h.bStarted = false
	return nil

}

func (h *testMetricsCollector) cleanup() {
	targets := h.pipeline.Targets()
	for _, target := range targets {
		target.UnRegisterComponentEventListener(common.DataSent, h)
	}
}

func (h *testMetricsCollector) OnEvent(eventType common.ComponentEventType, item interface{}, component common.Component, derivedItems []interface{}, otherInfos map[string]interface{}) {
	h.counterLock.Lock()
	defer h.counterLock.Unlock()
	
	if eventType == common.DataSent {
		h.counter++
	}
}

func (h *testMetricsCollector) MetricsValue() int {
	h.counterLock.Lock()
	defer h.counterLock.Unlock()
	
	return h.counter
}
