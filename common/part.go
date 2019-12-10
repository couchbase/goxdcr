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
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
)

type PartState int

//This is the error message any part goroutine would throw when it finds out
//the part is already requested to stop and it is left as orphan. The caller
//see this error message, it should stop itself and exit
var PartStoppedError = errors.New("Part is stopping or already stopped, exit")

var PartAlreadyStartedError = errors.New("Part has already been started before")

const (
	Part_Initial  PartState = iota
	Part_Starting PartState = iota
	Part_Running  PartState = iota
	Part_Stopping PartState = iota
	Part_Stopped  PartState = iota
	Part_Error    PartState = iota
)

func (s *PartState) Get() PartState {
	if s == nil {
		// ??
		return Part_Error
	}
	return *s
}

func (s *PartState) Set(state PartState, id string) error {
	if s == nil {
		return base.ErrorInvalidInput
	}

	//validate the state transition
	switch *s {
	case Part_Initial:
		if state != Part_Starting && state != Part_Stopping {
			return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, id, "Initial", "Started, Stopping"))
		}
	case Part_Starting:
		if state == Part_Starting {
			// return a special error since caller likely needs to distinguish it from other errors
			return PartAlreadyStartedError
		}
		if state != Part_Running && state != Part_Stopping && state != Part_Error {
			return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, id, "Starting", "Running, Stopping"))
		}
	case Part_Running:
		if state == Part_Starting {
			// return a special error since caller likely needs to distinguish it from other errors
			return PartAlreadyStartedError
		}
		if state != Part_Stopping && state != Part_Error {
			return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, id, "Running", "Stopping"))
		}
	case Part_Stopping:
		if state == Part_Starting {
			// return a special error since caller likely needs to distinguish it from other errors
			return PartAlreadyStartedError
		}
		if state != Part_Stopped {
			return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, id, "Stopping", "Stopped"))
		}
	case Part_Stopped:
		if state == Part_Starting {
			// return a special error since caller likely needs to distinguish it from other errors
			return PartAlreadyStartedError
		}
		return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, id, "Stopped", ""))
	case Part_Error:
		if state == Part_Starting {
			// return a special error since caller likely needs to distinguish it from other errors
			return PartAlreadyStartedError
		}
		if state != Part_Stopping {
			return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, id, "Error", "Stopping"))
		}
	}

	*s = state
	return nil
}

type PartType int

const (
	RegularPart  PartType = iota
	AdvancedPart PartType = iota
)

type PartCommon interface {
	Connectable

	GetType() PartType

	//Start makes goroutine for the part working
	Start(settings metadata.ReplicationSettingsMap) error

	//Stop stops the part,
	Stop() error

	//Receive accepts data passed down from its upstream
	Receive(data interface{}) error

	//return the state of the part
	State() PartState

	UpdateSettings(settings metadata.ReplicationSettingsMap) error
}

type Part interface {
	PartCommon
	Component
}

type AdvPart interface {
	PartCommon
	AdvComponent
}
