package common

import (

)

//Connectable abstracts the step that can be connected to one or more downstream 
//steps. 
//
//Any processing step which has a downstream step would need to implement this
//for example secondary index's KVFeed would need to implement this
//
//The processing step that implements Connectable would delegate the job of passing
//data to its downstream steps to its Connector. Different connector can have 
//different data passing schem
type Connectable interface {
	
	Connector() Connector
	SetConnector(connector Connector) error
}
