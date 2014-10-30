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
	common "github.com/Xiaomei-Zhang/goxdcr/common"
	component "github.com/Xiaomei-Zhang/goxdcr/component"
	"github.com/Xiaomei-Zhang/goxdcr/log"
	"sync"
)

//var logger = log.NewLogger("AbstractPart", log.LogLevelInfo)

type IsStarted_Callback_Func func() bool

type AbstractPart struct {
	component.AbstractComponent
	connector common.Connector

	isStarted_callback *IsStarted_Callback_Func

	stateLock sync.RWMutex
	logger    *log.CommonLogger
}

func NewAbstractPartWithLogger(id string,
	isStarted_callback *IsStarted_Callback_Func,
	logger *log.CommonLogger) AbstractPart {
	return AbstractPart{
		AbstractComponent: component.NewAbstractComponentWithLogger(id, logger),
		connector:          nil,
		isStarted_callback: isStarted_callback,
	}
}

func NewAbstractPart(id string,
	isStarted_callback *IsStarted_Callback_Func) AbstractPart {
	return NewAbstractPartWithLogger(id, isStarted_callback, log.NewLogger("AbstractPart", log.DefaultLoggerContext))
}


func (p *AbstractPart) Connector() common.Connector {
	p.stateLock.RLock()
	defer p.stateLock.RUnlock()

	return p.connector
}

func (p *AbstractPart) SetConnector(connector common.Connector) error {
	if p.isStarted_callback == nil || (*p.isStarted_callback) == nil {
		return errors.New("IsStarted() call back func has not been defined for part " + p.Id())
	}
	if (*p.isStarted_callback)() {
		return errors.New("Cannot set connector on part" + p.Id() + " since the part is still running.")
	}

	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	p.connector = connector
	return nil
}

//func (p *AbstractPart) Logger() *log.CommonLogger {
//	return p.logger
//}
