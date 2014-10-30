package common

import (

)

//interface for Pipeline

type Pipeline interface {
	//Name of the Pipeline
	Topic() string

	Sources () map[string]Nozzle
	Targets () map[string]Nozzle
	
	//getter\setter of the runtime environment
	RuntimeContext() PipelineRuntimeContext
	SetRuntimeContext (ctx PipelineRuntimeContext)

	//start the data exchange
	Start(settings map[string]interface{}) error
	//stop the data exchange
	Stop() error
}
