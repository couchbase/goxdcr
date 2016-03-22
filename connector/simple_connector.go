// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package connector

import (
	//	"errors"
	"sync"
	common "github.com/couchbase/goxdcr/common"
	component "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
)


//SimpleConnector connects one source to one downstream
type SimpleConnector struct {
	*component.AbstractComponent
	downStreamPart common.Part
	stateLock sync.RWMutex
}

func NewSimpleConnector (id string, downstreamPart common.Part, logger_context *log.LoggerContext) *SimpleConnector {
	logger := log.NewLogger("SimpleConnector", logger_context)
	return &SimpleConnector{component.NewAbstractComponentWithLogger(id, logger), downstreamPart, sync.RWMutex{}}
}

func (con *SimpleConnector) Forward(data interface{}) error {
	con.stateLock.RLock()
	defer con.stateLock.RUnlock()
	
	con.Logger().Debugf("%v forwarding to downstream part %s", con.Id(), con.downStreamPart.Id())
	return con.downStreamPart.Receive(data)
}

func (con *SimpleConnector) DownStreams() map[string]common.Part {
	con.stateLock.RLock()
	defer con.stateLock.RUnlock()
	
	downStreams := make(map[string]common.Part)
	downStreams[con.downStreamPart.Id()] = con.downStreamPart
	return downStreams
}

//add a node to its existing set of downstream nodes
func (con *SimpleConnector) AddDownStream(partId string, part common.Part) error {
	con.stateLock.Lock()
	defer con.stateLock.Unlock()
	
	if con.downStreamPart == nil {
		con.downStreamPart = part
	} else {
		//TODO: log
		//replace the new Part with the existing one
		con.downStreamPart = part
	}
	return nil

}
