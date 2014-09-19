package pipeline_svc

import (
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
)

type PipelineSupervisor struct {
}

func (supervisor *PipelineSupervisor) Attach(pipeline common.Pipeline) error {
	//TODO:
	return nil
}

func (supervisor *PipelineSupervisor) Start(settings map[string]interface{}) error {
	//TODO:
	return nil
}

func (supervisor *PipelineSupervisor) Stop() error {
	//TODO:
	return nil
}
