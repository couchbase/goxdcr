// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package replication_manager

import (
	"errors"
	"github.com/couchbase/goxdcr/base"
)

type MetadataChangeMonitor struct {
	//registered runtime pipeline service
	listeners map[string]base.MetadataChangeListener
}

func NewMetadataChangeMonitor() *MetadataChangeMonitor {
	return &MetadataChangeMonitor{
		listeners: make(map[string]base.MetadataChangeListener),
	}
}

func (mcm *MetadataChangeMonitor) Start() {
	for _, listener := range mcm.listeners {
		listener.Start()
	}
}

func (mcm *MetadataChangeMonitor) RegisterListener(listener base.MetadataChangeListener) error {
	if _, ok := mcm.listeners[listener.Id()]; ok {
		return errors.New("listener with the same Id already exists")
	}
	mcm.listeners[listener.Id()] = listener
	return nil
}
