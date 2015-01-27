// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package parts

import (
	"errors"
	"fmt"
	common "github.com/couchbase/goxdcr/common"
	component "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"sync"
)

var invalidStateTransitionErrMsg = "Can't move to state %v - Part's current state is %v, can only move to state [%v]"
//This is the error message any part goroutine would throw when it finds out 
//the part is already requested to stop and it is left as orphan. The caller 
//see this error message, it should stop itself and exit
var PartStoppedError = errors.New("Part is stopping or already stopped, exit")

type AbstractPart struct {
	*component.AbstractComponent
	connector common.Connector
	stateLock sync.RWMutex
	state     common.PartState
	logger    *log.CommonLogger
}

func NewAbstractPartWithLogger(id string,
	logger *log.CommonLogger) AbstractPart {
	return AbstractPart{
		AbstractComponent: component.NewAbstractComponentWithLogger(id, logger),
		state:             common.Part_Initial,
		connector:         nil,
	}
}

func NewAbstractPart(id string) AbstractPart {
	return NewAbstractPartWithLogger(id, log.NewLogger("AbstractPart", log.DefaultLoggerContext))
}

func (p *AbstractPart) Connector() common.Connector {
	p.stateLock.RLock()
	defer p.stateLock.RUnlock()

	return p.connector
}

func (p *AbstractPart) SetConnector(connector common.Connector) error {
	if p.State() != common.Part_Initial {
		return errors.New("Cannot set connector on part" + p.Id() + " since its state is not Part_Initial")
	}

	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	p.connector = connector
	return nil
}

func (p *AbstractPart) State() common.PartState {
	return p.state
}

func (p *AbstractPart) SetState(state common.PartState) error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	//validate the state transition
	switch p.State() {
	case common.Part_Initial:
		if state != common.Part_Starting && state != common.Part_Stopping {
			return errors.New(fmt.Sprintf(invalidStateTransitionErrMsg, state, "Initial", "Started, Stopping"))
		}
	case common.Part_Starting:
		if state != common.Part_Running && state != common.Part_Stopping {
			return errors.New(fmt.Sprintf(invalidStateTransitionErrMsg, state, "Starting", "Running, Stopping"))
		}
	case common.Part_Running:
		if state != common.Part_Stopping {
			return errors.New(fmt.Sprintf(invalidStateTransitionErrMsg, state, "Running", "Stopping"))
		}
	case common.Part_Stopping:
		if state != common.Part_Stopped {
			return errors.New(fmt.Sprintf(invalidStateTransitionErrMsg, state, "Stopping", "Stopped"))
		}
	case common.Part_Stopped:
		return errors.New(fmt.Sprintf(invalidStateTransitionErrMsg, state, "Stopped", ""))
	}
	p.state = state
	return nil
}

//func (p *AbstractPart) Logger() *log.CommonLogger {
//	return p.logger
//}
