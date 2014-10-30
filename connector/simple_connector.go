package connector

import (
	//	"errors"
	"sync"
	common "github.com/Xiaomei-Zhang/goxdcr/common"
	component "github.com/Xiaomei-Zhang/goxdcr/component"
	"github.com/Xiaomei-Zhang/goxdcr/log"
)


//SimpleConnector connects one source to one downstream
type SimpleConnector struct {
	component.AbstractComponent
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
	
	con.Logger().Debugf("Try to forward to downstream part %s", con.downStreamPart.Id())
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
