package common

import (

)

//Nozzle models the openning that data streaming out of the source system 
//and the outlet where the data flowing into the target system.
//
//Nozzle can be opened or closed. An closed nozzle will not allow data flow through
//
//Each nozzle is a goroutine
type Nozzle interface {
	Part

	//Open opens the Nozzle
	//
	//Data can be passed to the downstream
	Open() error
	
	//Close closes the Nozzle
	//
	//Data can get to this nozzle, but would not be passed to the downstream
	Close() error
		
	
	//IsOpen returns true if the nozzle is open; returns false if the nozzle is closed
	IsOpen () bool
	
}