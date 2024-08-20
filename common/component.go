// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package common

import "github.com/couchbase/goxdcr/v8/base"

// ComponentEventType is the common event type that Component can raise during its lifecycle
// It is not required for Component to raise all those event
type ComponentEventType int

const (
	//data streaming starts
	StreamingStart ComponentEventType = iota
	// data streaming ends
	StreamingEnd ComponentEventType = iota
	// data streaming bypass - if a backfill taks does not have this VB
	// This is necessary because it is possible that this node could pull VB task from peer nodes that
	// have not been GC'ed - and no work is to be done
	StreamingBypassed ComponentEventType = iota
	//data received by the component
	DataReceived ComponentEventType = iota
	//data is processed by the component
	//and passed down to the downstream
	DataProcessed ComponentEventType = iota
	//data sent to and acknowledged by the target system
	DataSent ComponentEventType = iota
	//data sent failed because of Cas change
	DataSentCasChanged ComponentEventType = iota
	// Data merged and send to and acknowledged by source bucket
	DataMerged ComponentEventType = iota
	// set merge result failed because source Cas changed
	MergeCasChanged ComponentEventType = iota
	// Merge failed.
	MergeFailed ComponentEventType = iota
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
	// response received from target cluster for subdoc_multi_lookup for XATTR and document body
	GetDocReceived ComponentEventType = iota
	// get meta response received from target cluster, or if custom CR, subdoc_multi_lookup for XATTR received
	GetMetaReceived ComponentEventType = iota
	//data failed conflict resolution on source cluster side due to optimistic replication
	DataFailedCRSource ComponentEventType = iota
	// data skipped because it is from target
	TargetDataSkipped ComponentEventType = iota
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
	// RoutingUpdate event
	BrokenRoutingUpdateEvent ComponentEventType = iota
	// When previously broken mappings have been fixed and should be backfilled
	FixedRoutingUpdateEvent ComponentEventType = iota
	// Data not replicated due to broken collection mapping
	// NOTE: This event is only supposed to be raised after ensuring the data will be
	// re-replicated later in another backfill
	DataNotReplicated ComponentEventType = iota
	// When a backfill pipeline ends, throughSeqnoTrackerSvc will raise this once the last seen DCP seqno has been processed
	LastSeenSeqnoDoneProcessed ComponentEventType = iota
	// When a source mutation is translated into multiple target mutations for different target namespaces
	DataCloned ComponentEventType = iota
	// DCP may send OSO snapshot marker if XDCR requested only one collection
	OsoSnapshotReceived ComponentEventType = iota
	// When target rejected writes
	DataSentFailed ComponentEventType = iota
	// When target guardrail prevents successful writes
	DataSentHitGuardrail ComponentEventType = iota
	// When target KV returned a status code that this XDCR does not understand
	DataSentFailedUnknownStatus ComponentEventType = iota
	// When mobile is active, source _sync XATTR is removed
	SourceSyncXattrRemoved ComponentEventType = iota
	// When mobile is active, target _sync XATTR is preserved if it exists
	TargetSyncXattrPreserved ComponentEventType = iota
	HlvUpdated               ComponentEventType = iota
	HlvPruned                ComponentEventType = iota
	HlvPrunedAtMerge         ComponentEventType = iota
	// DCP SeqnoAdv
	SeqnoAdvReceived ComponentEventType = iota
	// We use subdoc multipath sets and deletes when we have a specific mobile/xdcr case to avoid cas rollback on target
	DocsSentWithSubdocCmd   ComponentEventType = iota
	DocsSentWithPoisonedCas ComponentEventType = iota
)

func (c ComponentEventType) IsOutNozzleThroughSeqnoRelated() bool {
	switch c {
	case DataFailedCRSource:
		return true
	case TargetDataSkipped:
		return true
	case DataSent:
		return true
	case DataNotReplicated:
		return true
	default:
		return false
	}
}

func (c ComponentEventType) IsSynchronousEvent() bool {
	switch c {
	case FixedRoutingUpdateEvent:
		return true
	case BrokenRoutingUpdateEvent:
		return true
	default:
		return false
	}
}

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

// ComponentEventListener abstracts anybody who is interested in an event of a component
type ComponentEventListener interface {
	//OnEvent is the callback function that component would notify listener on an event
	OnEvent(event *Event)
}

// AsyncComponentEventListener is a subtype of ComponentEventListener which processes events asynchonously
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

type PipelineEventsProducer interface {
	// If there are elements in the eventExtras, the key (eventIDs) will be re-keyed to ensure global event ID uniqueness
	AddEvent(eventType base.EventInfoType, eventDesc string, eventExtras base.EventsMap, hint interface{}) (eventId int64)
	DismissEvent(eventId int) error
	// UpdateEvent should not modify the event type. If needed in the future, need to analyze the reason why it needs to be changed
	// Updating an event will update the time to the time this method is being called
	// eventExtras can be nil, and the existing information won't be changed
	// If eventExtras is not nil, the newEventExtras will be taken, re-keyed and merged with whatever is there currently
	UpdateEvent(oldEventId int64, newEventDesc string, newEventExtras *base.EventsMap) error
}
