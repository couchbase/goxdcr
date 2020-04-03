// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package common

import ()

//Nozzle models the openning that data streaming out of the source system
//and the outlet where the data flowing into the target system.
//
//Nozzle can be opened or closed. An closed nozzle will not allow data flow through
//
//Each nozzle is a goroutine
type Nozzle interface {
	Part

	//Open opens the Nozzle
	//
	//Data can be passed to the downstream
	Open() error

	//Close closes the Nozzle
	//
	//Data can get to this nozzle, but would not be passed to the downstream
	Close() error

	//IsOpen returns true if the nozzle is open; returns false if the nozzle is closed
	IsOpen() bool

	// Returns a list of its responsible VBs (Read-only)
	ResponsibleVBs() []uint16

	// To avoid garbage
	RecycleDataObj(obj interface{})

	// Valid for outgoing nozzles
	SetUpstreamObjRecycler(func(interface{}))
}
