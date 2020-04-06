// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package common

import (
	"github.com/couchbase/goxdcr/metadata"
)

//Connector abstracts the logic which moves data from one processing steps to another
type Connector interface {
	Component

	Forward(data interface{}) error

	//get this node's down stream nodes
	DownStreams() map[string]Part

	//add a node to its existing set of downstream nodes
	AddDownStream(partId string, part Part) error

	UpdateSettings(settings metadata.ReplicationSettingsMap) error

	IsStartable() bool
	Start() error
	Stop() error
}
