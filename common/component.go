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

//ComponentEventType is the common event type that Component can raise during its lifecycle
//It is not required for Component to raise all those event
type ComponentEventType int

const (
	//data streaming starts
	StreamingStart ComponentEventType = iota
	//data received by the component
	DataReceived ComponentEventType = iota
	//data is processed by the component
	//and passed down to the downstream
	DataProcessed ComponentEventType = iota
	//data sent to and acknowledged by the target system
	DataSent ComponentEventType = iota
	//data is filtered out by the component
	DataFiltered ComponentEventType = iota
	//data is unable to be successfully parsed to be filtered
	DataUnableToFilter ComponentEventType = iota
	//fatal error encountered by the component
	ErrorEncountered ComponentEventType = iota
	//error encountered in a particular vb by the component
	VBErrorEncountered ComponentEventType = iota
	//checkpointing suceeded for vb
	CheckpointDoneForVB ComponentEventType = iota
	//checkpointing succeed
	CheckpointDone ComponentEventType = iota
	// get meta response received from target cluster
	GetMetaReceived ComponentEventType = iota
	//data failed conflict resolution on source cluster side due to optimistic replication
	DataFailedCRSource ComponentEventType = iota
	// generic stats update event for the component
	StatsUpdate ComponentEventType = iota
	//received snapshot marker from dcp
	SnapshotMarkerReceived ComponentEventType = iota
	//data sending is throttled due to bandwidth usage limit being reached
	DataThrottled ComponentEventType = iota
	// Unable to get recycled data from datapool
	DataPoolGetFail ComponentEventType = iota
	//data sending is throttled due to throughput limit being reached
	DataThroughputThrottled ComponentEventType = iota
	// Expiry field has been stripped
	ExpiryFieldStripped ComponentEventType = iota
	// When DCP sends down a system event
	SystemEventReceived ComponentEventType = iota
)

type Event struct {
	//the type of component event
	EventType ComponentEventType
	//the data item
	Data interface{}
	//the originating component
	Component Component
	//the data items derived from the original item.
	DerivedData []interface{}
	//any other information the event might be able to supply to its listener
	OtherInfos interface{}
}

func NewEvent(eventType ComponentEventType, data interface{}, component Component, derivedData []interface{}, otherInfos interface{}) *Event {
	return &Event{eventType, data, component, derivedData, otherInfos}
}

//ComponentEventListener abstracts anybody who is interested in an event of a component
type ComponentEventListener interface {
	//OnEvent is the callback function that component would notify listener on an event
	OnEvent(event *Event)
}

//AsyncComponentEventListener is a subtype of ComponentEventListener which processes events asynchonously
type AsyncComponentEventListener interface {
	ComponentEventListener
	// id of event listener
	Id() string
	Start() error
	Stop() error
	RegisterComponentEventHandler(handler AsyncComponentEventHandler)
}

type AsyncComponentEventHandler interface {
	Id() string
	//ProcessEvent does the actual handling of the component event for async event listener
	//it is called from a separate go routine from OnEvent()
	ProcessEvent(event *Event) error
}

type Component interface {
	// id of component
	Id() string

	//RegisterComponentEventListener registers a listener for component event
	//
	//if the eventType is not supported by the component, an error would be thrown
	RegisterComponentEventListener(eventType ComponentEventType, listener ComponentEventListener) error
	UnRegisterComponentEventListener(eventType ComponentEventType, listener ComponentEventListener) error

	// returns the map of async event listeners registered on this component
	AsyncComponentEventListeners() map[string]AsyncComponentEventListener

	// raise event for a component
	RaiseEvent(event *Event)
}
