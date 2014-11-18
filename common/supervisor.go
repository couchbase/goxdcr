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
	"time"
)

//interfaces for Supervisor and related entities

type Supervisor interface {
	Id() string
	AddChild(child Supervisable) error
	RemoveChild(childId string) error
	Child(childId string) (Supervisable, error)
	Start(settings map[string]interface{}) error
	Stop() error
	ReportFailure(errors map[string]error)
}

// Components that can be supervised, e.g., parts, replication manager, etc.
type Supervisable interface {
	Id()  string
	HeartBeat_sync() bool
	HeartBeat_async(respchan chan []interface{}, timestamp time.Time) error
}

// Handler for failures reported by Supervisor
type SupervisorFailureHandler interface {
	OnError(supervisor Supervisor, errors map[string]error)
}

