// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package connector

import (
	//	"errors"
	common "github.com/couchbase/goxdcr/v8/common"
	component "github.com/couchbase/goxdcr/v8/component"
	"github.com/couchbase/goxdcr/v8/log"
	"sync"
)

//SimpleConnector connects one source to one downstream
type SimpleConnector struct {
	*component.AbstractComponent
	downStreamPart common.Part
	stateLock      sync.RWMutex
}

func NewSimpleConnector(id string, downstreamPart common.Part, logger_context *log.LoggerContext) *SimpleConnector {
	logger := log.NewLogger("SimpleConn", logger_context)
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
