// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package interface_test

import (
	common "github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
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

func (h *testMetricsCollector) Start(settings metadata.ReplicationSettingsMap) error {
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
