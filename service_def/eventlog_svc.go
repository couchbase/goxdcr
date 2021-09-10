// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

type EventIdType uint

// All System Events have the following fields:
// {
//	timestamp:		string (exact date and time of the event in the format YYYY-MM-DDThh:mm:ss:SSSZ)
//	component:		string ("xdcr")
//	severity:		string (info/warn/error/fatal)
//	event_id: 		uint (7168-8191 for XDCR)
//	description: 	string (a short description of the event
//	uuid:			string (uuid of the event in UUIDv4)
//	extra_attributes: json (optional key/value pair for additional info)
// }
//
// When writing to system event log, the caller will provide the event_id and extra_attributes map. The EventLogSvc will fill in the rest.
// The map key/value must be 1-80 bytes
//
// No sensitive information can be logged. The system event log WILL NOT be redacted.

const (
	// XDCR event id block is 7168-8191
	MinSystemEventId                             EventIdType = 7168
	CreateRemoteClusterRefSystemEventId          EventIdType = 7168
	UpdateRemoteClusterRefSystemEventId          EventIdType = 7169
	DeleteRemoteClusterRefSystemEventId          EventIdType = 7170
	CreateReplicationSystemEventId               EventIdType = 7171
	PauseReplicationSystemEventId                EventIdType = 7172
	ResumeReplicationSystemEventId               EventIdType = 7173
	DeleteReplicationSystemEventId               EventIdType = 7174
	UpdateDefaultReplicationSettingSystemEventId EventIdType = 7175
	UpdateReplicationSettingSystemEventId        EventIdType = 7176
	MaxSystemEventId                             EventIdType = 7176
)

const (
	// These are some of the keys used in event log
	SourceBucketKey  = "sourceBucket"
	TargetBucketKey  = "targetBucket"
	RemoteClusterKey = "remoteClusterName"
)

type EventLogSvc interface {
	WriteEvent(eventId EventIdType, args map[string]string)
}
