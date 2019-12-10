// Copyright (c) 2013-2020 Couchbase, Inc.
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
	"github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	component "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"sync"
)

type AbstractPart struct {
	*component.AbstractComponent
	stateLock sync.RWMutex
	state     common.PartState
	connector common.Connector
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

func (p *AbstractPart) GetType() common.PartType {
	return common.RegularPart
}

func (p *AbstractPart) Connector() common.Connector {
	p.stateLock.RLock()
	defer p.stateLock.RUnlock()

	return p.connector
}

func (p *AbstractPart) SetConnector(connector common.Connector) error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()
	if p.state.Get() != common.Part_Initial {
		return errors.New("Cannot set connector on part" + p.Id() + " since its state is not Part_Initial")
	}

	p.connector = connector
	return nil
}

func (p *AbstractPart) State() common.PartState {
	p.stateLock.RLock()
	defer p.stateLock.RUnlock()
	return p.state.Get()
}

func (p *AbstractPart) SetState(state common.PartState) error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	return p.state.Set(state, p.Id())
}

func (p *AbstractPart) IsReadyForHeartBeat() bool {
	p.stateLock.RLock()
	defer p.stateLock.RUnlock()
	return p.state == common.Part_Running
}

func (p *AbstractPart) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	return nil
}

func (p *AbstractPart) SetAdvConnector(connector common.AdvConnector) error {
	return base.ErrorNotImplemented
}

func (p *AbstractPart) AdvConnector() (common.AdvConnector, error) {
	return nil, base.ErrorNotImplemented
}
