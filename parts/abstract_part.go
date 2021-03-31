// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package parts

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	component "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"sync"
)

//This is the error message any part goroutine would throw when it finds out
//the part is already requested to stop and it is left as orphan. The caller
//see this error message, it should stop itself and exit
var PartStoppedError = errors.New("Part is stopping or already stopped, exit")

var PartAlreadyStartedError = errors.New("Part has already been started before")
var PartAlreadyStoppedError = errors.New("Part has already been stopped before")

type AbstractPart struct {
	*component.AbstractComponent
	connector common.Connector
	stateLock sync.RWMutex
	state     common.PartState
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
	p.stateLock.Lock()
	defer p.stateLock.Unlock()
	if p.state != common.Part_Initial {
		return errors.New("Cannot set connector on part" + p.Id() + " since its state is not Part_Initial")
	}

	p.connector = connector
	return nil
}

func (p *AbstractPart) State() common.PartState {
	p.stateLock.RLock()
	defer p.stateLock.RUnlock()
	return p.state
}

func (p *AbstractPart) SetState(state common.PartState) error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	//validate the state transition
	switch p.state {
	case common.Part_Initial:
		if state != common.Part_Starting && state != common.Part_Stopping {
			return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, p.Id(), "Initial", "Started, Stopping"))
		}
	case common.Part_Starting:
		if state == common.Part_Starting {
			// return a special error since caller likely needs to distinguish it from other errors
			return PartAlreadyStartedError
		}
		if state != common.Part_Running && state != common.Part_Stopping && state != common.Part_Error {
			return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, p.Id(), "Starting", "Running, Stopping"))
		}
	case common.Part_Running:
		if state == common.Part_Starting {
			// return a special error since caller likely needs to distinguish it from other errors
			return PartAlreadyStartedError
		}
		if state != common.Part_Stopping && state != common.Part_Error {
			return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, p.Id(), "Running", "Stopping"))
		}
	case common.Part_Stopping:
		if state == common.Part_Starting {
			// return a special error since caller likely needs to distinguish it from other errors
			return PartAlreadyStartedError
		}
		if state != common.Part_Stopped {
			return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, p.Id(), "Stopping", "Stopped"))
		}
	case common.Part_Stopped:
		if state == common.Part_Starting {
			// return a special error since caller likely needs to distinguish it from other errors
			return PartAlreadyStartedError
		}
		return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, p.Id(), "Stopped", ""))
	case common.Part_Error:
		if state == common.Part_Starting {
			// return a special error since caller likely needs to distinguish it from other errors
			return PartAlreadyStartedError
		}
		if state != common.Part_Stopping {
			return errors.New(fmt.Sprintf(base.InvalidStateTransitionErrMsg, state, p.Id(), "Error", "Stopping"))
		}
	}
	p.state = state
	return nil
}

func (p *AbstractPart) IsReadyForHeartBeat() bool {
	p.stateLock.RLock()
	defer p.stateLock.RUnlock()
	return p.state == common.Part_Running
}

func (p *AbstractPart) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	return nil
}
