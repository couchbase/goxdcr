// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import (
	"github.com/couchbase/goxdcr/v8/base"
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
	CreateRemoteAccessDeniedEventId         uint32 = 16394
	UpdateRemoteAccessDeniedEventId         uint32 = 16395
	LocalAccessDeniedEventId                uint32 = 16396
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

type LocalClusterAccessDeniedEvent struct {
	GenericFields
	Request      string `json:"request"`
	Method       string `json:"method"`
	ErrorMessage string `json:"error_message"`
}

type RemoteClusterAccessDeniedEvent struct {
	GenericFields
	RemoteClusterName     string `json:"cluster_name"`
	RemoteClusterHostname string `json:"cluster_hostname"`
	RemoteUserName        string `json:"remote_username"`
	EncryptionType        string `json:"encryption_type"`
	ErrorMessage          string `json:"error_message"`
}

// fields applicable to all events
type GenericFields struct {
	Timestamp  string     `json:"timestamp"`
	RealUserid RealUserId `json:"real_userid"`
	LocalRemoteIPs
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
	Domain   string `json:"domain"`
	Username string `json:"user"`
}

func (userId *RealUserId) Redact() {
	if !base.IsStringRedacted(userId.Username) {
		userId.Username = base.TagUD(userId.Username)
	}
}

type IpAndPort struct {
	Ip   string `json:"ip""`
	Port uint16 `json:"port""`
}

type LocalRemoteIPs struct {
	Remote *IpAndPort `json:"remote"`
	Local  *IpAndPort `json:"local"`
}

func (generics *GenericFields) Redact() {
	generics.RealUserid.Redact()
}

func (event *RemoteClusterRefEvent) Redact() AuditEventIface {
	event.GenericFields.Redact()
	return event
}

func (event *RemoteClusterRefEvent) Clone() AuditEventIface {
	clonedEvent := *event
	return &clonedEvent
}

func (event *CreateReplicationEvent) Redact() AuditEventIface {
	event.GenericFields.Redact()
	return event
}

func (event *CreateReplicationEvent) Clone() AuditEventIface {
	clonedEvent := *event
	return &clonedEvent
}

func (event *UpdateDefaultReplicationSettingsEvent) Redact() AuditEventIface {
	event.GenericFields.Redact()
	return event
}

func (event *UpdateDefaultReplicationSettingsEvent) Clone() AuditEventIface {
	clonedEvent := *event
	return &clonedEvent
}

func (event *UpdateBucketSettingsEvent) Redact() AuditEventIface {
	event.GenericFields.Redact()
	return event
}

func (event *UpdateBucketSettingsEvent) Clone() AuditEventIface {
	clonedEvent := *event
	return &clonedEvent
}

func (event *GenericReplicationEvent) Redact() AuditEventIface {
	event.GenericFields.Redact()
	return event
}

func (event *GenericReplicationEvent) Clone() AuditEventIface {
	clonedEvent := *event
	return &clonedEvent
}

func (event *RemoteClusterAccessDeniedEvent) Redact() AuditEventIface {
	event.GenericFields.Redact()
	return event
}

func (event *RemoteClusterAccessDeniedEvent) Clone() AuditEventIface {
	clonedEvent := *event
	return &clonedEvent
}

func (event *LocalClusterAccessDeniedEvent) Redact() AuditEventIface {
	event.GenericFields.Redact()
	return event
}

func (event *LocalClusterAccessDeniedEvent) Clone() AuditEventIface {
	clonedEvent := *event
	return &clonedEvent
}
