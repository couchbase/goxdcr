// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package common

import (
	"github.com/couchbase/goxdcr/v8/metadata"
	"time"
)

//interfaces for Supervisor and related entities

type Supervisor interface {
	Id() string
	AddChild(child Supervisable) error
	RemoveChild(childId string) error
	Child(childId string) (Supervisable, error)
	Start(settings metadata.ReplicationSettingsMap) error
	Stop() error
	ReportFailure(errors map[string]error)
}

// Components that can be supervised, e.g., parts, replication manager, etc.
type Supervisable interface {
	Id() string
	IsReadyForHeartBeat() bool
	HeartBeat_sync() bool
	HeartBeat_async(respchan chan []interface{}, timestamp time.Time) error
}

// Handler for failures reported by Supervisor
type SupervisorFailureHandler interface {
	OnError(supervisor Supervisor, errors map[string]error)
}
