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
	//data streaming starts
	StreamingStart	ComponentEventType = iota
	//data received by the component
	DataReceived ComponentEventType = iota
	//data is processed by the component 
	//and passed down to the downstream
	DataProcessed ComponentEventType = iota
	//data is fully passed through the 
	//pipeline to the target system
	DataSent ComponentEventType = iota
	//data is filtered out by the component
	DataFiltered ComponentEventType = iota
	//error encountered by the component
	ErrorEncountered ComponentEventType = iota
	//checkpointing suceeded for vb
	CheckpointDoneForVB	ComponentEventType = iota
	//checkpointing succeed 
	CheckpointDone		ComponentEventType = iota
	// get meta request sent to target cluster
	GetMetaSent ComponentEventType = iota
	// get meta response received from target cluster
	GetMetaReceived ComponentEventType = iota
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