package common

import (

)

//PipelineRuntimeContext manages all the services that attaches to the feed
type PipelineRuntimeContext interface {

	Start(map[string]interface{}) error
	Stop() error
	
	Pipeline () Pipeline
	
	//return a service handle
	Service (svc_name string) PipelineService
	
	//register a new service
	//if the feed is active, the service would be started rightway
	RegisterService (svc_name string, svc PipelineService) error
	
	//attach the service from the feed and stop the service's goroutine
	UnregisterService (srv_name string) error
	
}

