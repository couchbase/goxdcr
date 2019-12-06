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
	"github.com/couchbase/goxdcr/common"
	component "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"sync"
)

type AdvAbstractPart struct {
	*component.AdvAbstractComponent
	connector common.Connector
	stateLock sync.RWMutex
	state     common.PartState
}

func NewAdvAbstractPartWithLogger(id string, logger *log.CommonLogger) AdvAbstractPart {
	return AdvAbstractPart{
		AdvAbstractComponent: component.NewAdvAbstractComponentWithLogger(id, logger),
		state:                common.Part_Initial,
		connector:            nil,
	}
}

func (p *AdvAbstractPart) Connector() common.Connector {
	p.stateLock.RLock()
	defer p.stateLock.RUnlock()

	return p.connector
}

func (p *AdvAbstractPart) SetConnector(connector common.Connector) error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()
	if p.state.Get() != common.Part_Initial {
		return errors.New("Cannot set connector on part" + p.Id() + " since its state is not Part_Initial")
	}

	p.connector = connector
	p.AdvAbstractComponent.Logger().Infof("NEIL DEBUG set connector to %v\n", connector.Id())
	return nil
}

func (p *AdvAbstractPart) State() common.PartState {
	p.stateLock.RLock()
	defer p.stateLock.RUnlock()
	return p.state.Get()
}

func (p *AdvAbstractPart) SetState(state common.PartState) error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	return p.state.Set(state, p.Id())
}
