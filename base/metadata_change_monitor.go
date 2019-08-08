// Copyright (c) 2013-2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package base

import (
	"fmt"
	"time"
)

type MetadataChangeMonitor struct {
	// order of listeners matters
	// it is the order that listeners are added to the monitor
	// listeners are started in the same order
	// hence if listener A needs to be started after listener B
	// all we need to do is to register listener A after listener B
	listeners []MetadataChangeListener
}

func NewMetadataChangeMonitor() *MetadataChangeMonitor {
	return &MetadataChangeMonitor{
		listeners: make([]MetadataChangeListener, 0),
	}
}

func (mcm *MetadataChangeMonitor) Start() {
	for _, listener := range mcm.listeners {
		listener.Start()
		time.Sleep(WaitTimeBetweenMetadataChangeListeners)
	}
}

func (mcm *MetadataChangeMonitor) RegisterListener(listener MetadataChangeListener) error {
	for _, existing_listener := range mcm.listeners {
		if existing_listener.Id() == listener.Id() {
			return fmt.Errorf("listener with the same Id, %v, already exists", listener.Id())
		}
	}

	mcm.listeners = append(mcm.listeners, listener)
	return nil
}
