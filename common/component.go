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

)

//ComponentEventType is the common event type that Component can raise during its lifecycle
//It is not required for Component to raise all those event
type ComponentEventType int
const (
	DataReceived ComponentEventType = iota
	DataProcessed ComponentEventType = iota
	DataSent ComponentEventType = iota
	DataFiltered ComponentEventType = iota
	ErrorEncountered ComponentEventType = iota
)

//ComponentEventListener abstracts anybody who is interested in an event of a component
type ComponentEventListener interface {
	//OnEvent is the callback function that component would notify listener on an event
	//event - the type of component event
	//item - the data item
	//derivedItems - the data items derived from the original item. This only used by DataProcessed event
	//otherinformation - any other information the event might be able to supply to its listener
	OnEvent (eventType ComponentEventType, item interface{}, component Component, derivedItems []interface{}, otherInfos map[string]interface{})
}

type Component interface {
	// id of component
	Id() string
	
	//RegisterComponentEventListener registers a listener for component event
	//
	//if the eventType is not supported by the component, an error would be thrown
	RegisterComponentEventListener (eventType ComponentEventType, listener ComponentEventListener) error
	UnRegisterComponentEventListener (eventType ComponentEventType, listener ComponentEventListener) error	
	
	// raise event for a component
	RaiseEvent(eventType ComponentEventType, data interface{}, component Component, derivedData []interface{}, otherInfos map[string]interface{})
}