package common

import (

)

//PipelineService can be any component that monitors, does logging, keeps state for the pipeline
//Each PipelineService is a goroutine that run parallelly
type PipelineService interface {
	Attach (pipeline Pipeline) error
	
	Start(map[string]interface{}) error
	Stop() error
}
