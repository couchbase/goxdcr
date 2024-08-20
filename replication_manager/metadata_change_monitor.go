// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package replication_manager

import (
	"fmt"
	"github.com/couchbase/goxdcr/v8/base"
	"time"
)

type MetadataChangeMonitor struct {
	// order of listeners matters
	// it is the order that listeners are added to the monitor
	// listeners are started in the same order
	// hence if listener A needs to be started after listener B
	// all we need to do is to register listener A after listener B
	listeners []base.MetadataChangeListener
}

func NewMetadataChangeMonitor() *MetadataChangeMonitor {
	return &MetadataChangeMonitor{
		listeners: make([]base.MetadataChangeListener, 0),
	}
}

func (mcm *MetadataChangeMonitor) Start() {
	for _, listener := range mcm.listeners {
		listener.Start()
		time.Sleep(base.WaitTimeBetweenMetadataChangeListeners)
	}
}

func (mcm *MetadataChangeMonitor) RegisterListener(listener base.MetadataChangeListener) error {
	for _, existing_listener := range mcm.listeners {
		if existing_listener.Id() == listener.Id() {
			return fmt.Errorf("listener with the same Id, %v, already exists", listener.Id())
		}
	}

	mcm.listeners = append(mcm.listeners, listener)
	return nil
}
