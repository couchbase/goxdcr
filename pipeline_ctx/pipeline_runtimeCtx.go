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
	common "github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
)

type ServiceSettingsConstructor func(pipeline common.Pipeline, service common.PipelineService, pipeline_settings map[string]interface{}) (map[string]interface{}, error)

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
			settings, err = ctx.service_settings_constructor (ctx.pipeline, svc, params)
			if err != nil {
				return err
			}
		}
		err = svc.Start(settings)
		if err != nil {
			ctx.logger.Errorf("Failed to start service %s", name)
		}
		ctx.logger.Debugf("Service %s has been started", name)
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

	var err error = nil
	//stop all registered services
	for _, svc := range ctx.runtime_svcs {
		err = svc.Stop()
		if err != nil {
			//TODO: log error
		}
	}

	if err == nil {
		ctx.isRunning = false
	} else {
		panic("Pipeline runtime context failed to stop")
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
		}
	}

	//remove it from the map
	delete(ctx.runtime_svcs, srv_name)

	return err
}

//enforcer for PipelineRuntimeCtx to implement PipelineRuntimeContext
var _ common.PipelineRuntimeContext = (*PipelineRuntimeCtx)(nil)
