// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_def

import (
	"github.com/couchbase/goxdcr/base"
)

const (
	CreateRemoteClusterRefEventId           uint32 = 16384
	UpdateRemoteClusterRefEventId           uint32 = 16385
	DeleteRemoteClusterRefEventId           uint32 = 16386
	CreateReplicationEventId                uint32 = 16387
	PauseReplicationEventId                 uint32 = 16388
	ResumeReplicationEventId                uint32 = 16389
	CancelReplicationEventId                uint32 = 16390
	UpdateDefaultReplicationSettingsEventId uint32 = 16391
	UpdateReplicationSettingsEventId        uint32 = 16392
	UpdateBucketSettingsEventId             uint32 = 16393
)

var ErrorWritingAudit = "Could not write audit logs."

// used in the place where a remote cluster referenced by a replication can
// no longer be found, e.g., when the cluster has been deleted prior
var UnknownRemoteClusterName = "Unknown"

type RemoteClusterRefEvent struct {
	GenericFields
	RemoteClusterName     string `json:"cluster_name"`
	RemoteClusterHostname string `json:"cluster_hostname"`
	IsEncrypted           bool   `json:"is_encrypted"`
	EncryptionType        string `json:"encryption_type"`
}

type CreateReplicationEvent struct {
	GenericReplicationEvent
	FilterExpression string `json:"filter_expression,omitempty"`
}

type UpdateDefaultReplicationSettingsEvent struct {
	GenericReplicationFields
	UpdatedSettings map[string]interface{} `json:"updated_settings"`
}

type UpdateReplicationSettingsEvent struct {
	ReplicationSpecificFields
	UpdateDefaultReplicationSettingsEvent
}

type UpdateBucketSettingsEvent struct {
	GenericFields
	BucketName      string                 `json:"bucket_name"`
	UpdatedSettings map[string]interface{} `json:"updated_settings"`
}

type GenericReplicationEvent struct {
	GenericReplicationFields
	ReplicationSpecificFields
}

// fields applicable to all events
type GenericFields struct {
	Timestamp  string     `json:"timestamp"`
	RealUserid RealUserId `json:"real_userid"`
}

// fields applicable to all replication related events
type GenericReplicationFields struct {
	GenericFields
	LocalClusterName string `json:"local_cluster_name"`
}

// fields applicable to individual replications
type ReplicationSpecificFields struct {
	SourceBucketName  string `json:"source_bucket_name"`
	RemoteClusterName string `json:"remote_cluster_name"`
	TargetBucketName  string `json:"target_bucket_name"`
}

type RealUserId struct {
	Source   string `json:"source"`
	Username string `json:"user"`
}

func (userId *RealUserId) Redact() {
	if !base.IsStringRedacted(userId.Username) {
		userId.Username = base.TagUD(userId.Username)
	}
}

func (generics *GenericFields) Redact() {
	generics.RealUserid.Redact()
}

func (event *RemoteClusterRefEvent) Redact() AuditEventIface {
	event.GenericFields.Redact()
	return event
}

func (event *RemoteClusterRefEvent) Clone() AuditEventIface {
	clonedEvent := &RemoteClusterRefEvent{}
	*clonedEvent = *event
	return clonedEvent
}

func (event *CreateReplicationEvent) Redact() AuditEventIface {
	event.GenericFields.Redact()
	return event
}

func (event *CreateReplicationEvent) Clone() AuditEventIface {
	clonedEvent := &CreateReplicationEvent{}
	*clonedEvent = *event
	return clonedEvent
}

func (event *UpdateDefaultReplicationSettingsEvent) Redact() AuditEventIface {
	event.GenericFields.Redact()
	return event
}

func (event *UpdateDefaultReplicationSettingsEvent) Clone() AuditEventIface {
	clonedEvent := &UpdateDefaultReplicationSettingsEvent{}
	*clonedEvent = *event
	return clonedEvent
}

func (event *UpdateBucketSettingsEvent) Redact() AuditEventIface {
	event.GenericFields.Redact()
	return event
}

func (event *UpdateBucketSettingsEvent) Clone() AuditEventIface {
	clonedEvent := &UpdateBucketSettingsEvent{}
	*clonedEvent = *event
	return clonedEvent
}

func (event *GenericReplicationEvent) Redact() AuditEventIface {
	event.GenericFields.Redact()
	return event
}

func (event *GenericReplicationEvent) Clone() AuditEventIface {
	clonedEvent := &GenericReplicationEvent{}
	*clonedEvent = *event
	return clonedEvent
}
