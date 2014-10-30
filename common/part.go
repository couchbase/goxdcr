package common

import (

)

type Part interface {
	Component
	Connectable
	
	//Start makes goroutine for the part working
	Start (settings map[string]interface{} ) error
	
	//Stop stops the part,
	Stop () error
	
	//Receive accepts data passed down from its upstream
	Receive (data interface {}) error

	//IsStarted returns true if the part is started; otherwise returns false
	IsStarted () bool
	
}