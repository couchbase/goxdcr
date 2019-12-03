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

//Connectable abstracts the step that can be connected to one or more downstream
//steps.
//
//Any processing step which has a downstream step would need to implement this
//
//The processing step that implements Connectable would delegate the job of passing
//data to its downstream steps to its Connector. Different connector can have
//different data passing schem
type Connectable interface {
	Connector() Connector
	SetConnector(connector Connector) error
	//	UnsetConnector(connector Connector) error
}
