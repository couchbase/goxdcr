// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package pipeline_ctx

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
)

type ServiceSettingsConstructor func(pipeline common.Pipeline, service common.PipelineService, pipeline_settings map[string]interface{}, ts map[uint16]*base.VBTimestamp) (map[string]interface{}, error)
type StartSeqnoConstructor func(pipeline common.Pipeline) error

type PipelineRuntimeCtx struct {
	//registered runtime pipeline service
	runtime_svcs map[string]common.PipelineService

	//pipeline
	pipeline common.Pipeline


	isRunning                    bool
	logger                       *log.CommonLogger
	service_settings_constructor ServiceSettingsConstructor
}

func NewWithSettingConstructor(p common.Pipeline, service_settings_constructor ServiceSettingsConstructor, logger_context *log.LoggerContext) (*PipelineRuntimeCtx, error) {
	ctx := &PipelineRuntimeCtx{
		runtime_svcs: make(map[string]common.PipelineService),
		pipeline:     p,
		isRunning:    false,
		logger:       log.NewLogger("PipelineRuntimeCtx", logger_context),
		service_settings_constructor: service_settings_constructor}

	return ctx, nil
}

func New(p common.Pipeline) (*PipelineRuntimeCtx, error) {
	return NewWithSettingConstructor(p, nil, log.DefaultLoggerContext)
}

func (ctx *PipelineRuntimeCtx) Start(params map[string]interface{}) error {

	var err error = nil
	//start all registered services
	for name, svc := range ctx.runtime_svcs {
		settings := params
		if ctx.service_settings_constructor != nil {
			ts, ok := params["VBTimestamps"]
			if !ok {
				ctx.logger.Errorf("VBTimestamps is missing. params=%v\n", params)
				return errors.New("VBTimestamps is missing")
			}
			settings, err = ctx.service_settings_constructor(ctx.pipeline, svc, params, ts.(map[uint16]*base.VBTimestamp))
			if err != nil {
				return err
			}
		}
		err = svc.Start(settings)
		if err != nil {
			ctx.logger.Errorf("Failed to start service %s", name)
			break
		}
		ctx.logger.Infof("Service %s has been started", name)
	}

	if err == nil {
		ctx.isRunning = true
	} else {
		//clean up
		err2 := ctx.Stop()
		if err2 != nil {
			//failed to clean up
			panic("Pipeline runtime context failed to start up, try to clean up, failed again")
		}
	}
	return err
}

func (ctx *PipelineRuntimeCtx) Stop() error {
	ctx.logger.Infof("Pipeline context is stopping...")
	var err error = nil
	errMap := make (map[string]error)
	
	services_stopped := []string {}
	//stop all registered services
	for name, _ := range ctx.runtime_svcs {
		err1 := ctx.UnregisterService(name)
		if err1 != nil {
			errMap[name] = err1
			ctx.logger.Errorf("Failed to stop service %v - %v", name, err1)
		}else {
			services_stopped = append (services_stopped, name)
		}
		
	}

	for _, service_stopped_name := range services_stopped {
		delete (ctx.runtime_svcs, service_stopped_name)
	}
	
	if len(errMap) == 0 {
		ctx.isRunning = false
		ctx.logger.Infof("Pipeline context for %v has been stopped", ctx.pipeline.InstanceId())
	} else {
		err = errors.New(fmt.Sprintf("Pipeline runtime context failed to stop, err = %v\n", errMap))
	}
	return err
}

func (ctx *PipelineRuntimeCtx) Pipeline() common.Pipeline {
	return ctx.pipeline
}

func (ctx *PipelineRuntimeCtx) Service(svc_name string) common.PipelineService {
	return ctx.runtime_svcs[svc_name]
}

func (ctx *PipelineRuntimeCtx) RegisterService(svc_name string, svc common.PipelineService) error {

	if ctx.isRunning {
		return errors.New("Can't register service when PipelineRuntimeContext is already running")
	}

	ctx.logger.Infof("Try to attach %v to pipeline %v\n", svc_name, ctx.pipeline.InstanceId())
	ctx.runtime_svcs[svc_name] = svc

	return svc.Attach(ctx.pipeline)

}

func (ctx *PipelineRuntimeCtx) UnregisterService(srv_name string) error {
	var err error
	svc := ctx.runtime_svcs[srv_name]

	if svc != nil && ctx.isRunning {
		err = svc.Stop()
		if err != nil {
			//log error
			ctx.logger.Errorf(fmt.Sprintf("Failed to stop service %v", srv_name))
		}
	}


	return err
}

//enforcer for PipelineRuntimeCtx to implement PipelineRuntimeContext
var _ common.PipelineRuntimeContext = (*PipelineRuntimeCtx)(nil)
