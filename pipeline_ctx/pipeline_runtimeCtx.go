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
	"github.com/couchbase/goxdcr/metadata"
	"sync"
)

type ServiceSettingsConstructor func(pipeline common.Pipeline, service common.PipelineService, pipeline_settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error)
type ServiceUpdateSettingsConstructor func(pipeline common.Pipeline, service common.PipelineService, pipeline_settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error)
type StartSeqnoConstructor func(pipeline common.Pipeline) error

const RuntimeContext = "RuntimeCtx"

type PipelineRuntimeCtx struct {
	//registered runtime pipeline service
	runtime_svcs      map[string]common.PipelineService
	runtime_svcs_lock *sync.RWMutex

	//pipeline
	pipeline common.Pipeline

	isRunning                           bool
	logger                              *log.CommonLogger
	service_settings_constructor        ServiceSettingsConstructor
	service_update_settings_constructor ServiceUpdateSettingsConstructor
}

func NewWithSettingConstructor(p common.Pipeline, service_settings_constructor ServiceSettingsConstructor,
	service_update_settings_constructor ServiceUpdateSettingsConstructor, logger_context *log.LoggerContext) (*PipelineRuntimeCtx, error) {
	ctx := &PipelineRuntimeCtx{
		runtime_svcs:                        make(map[string]common.PipelineService),
		pipeline:                            p,
		isRunning:                           false,
		logger:                              log.NewLogger(RuntimeContext, logger_context),
		service_settings_constructor:        service_settings_constructor,
		runtime_svcs_lock:                   &sync.RWMutex{},
		service_update_settings_constructor: service_update_settings_constructor}

	return ctx, nil
}

func New(p common.Pipeline) (*PipelineRuntimeCtx, error) {
	return NewWithSettingConstructor(p, nil, nil, log.DefaultLoggerContext)
}

func (ctx *PipelineRuntimeCtx) Start(params metadata.ReplicationSettingsMap) error {
	ctx.runtime_svcs_lock.RLock()
	defer ctx.runtime_svcs_lock.RUnlock()

	topic := ""
	if ctx.pipeline != nil {
		topic = ctx.pipeline.Topic()
	}

	startServicesFunc := func() error {
		var err error = nil
		//start all registered services
		for name, svc := range ctx.runtime_svcs {
			settings := params
			if ctx.service_settings_constructor != nil {
				settings, err = ctx.service_settings_constructor(ctx.pipeline, svc, params)
				if err != nil {
					return err
				}
			}
			err = svc.Start(settings)
			if err != nil {
				err1 := fmt.Errorf("Failed to start service %v for %v. err=%v", name, topic, err)
				ctx.logger.Errorf("%v", err1)
				return err1
			}
			ctx.logger.Infof("Service %v has been started for %v", name, topic)
		}
		return nil
	}

	// put a timeout around service starting to avoid being stuck
	err := base.ExecWithTimeout(startServicesFunc, base.TimeoutRuntimeContextStart, ctx.logger)
	if err != nil {
		ctx.logger.Errorf("%v error starting pipeline context. err=%v", topic, err)
	} else {
		ctx.logger.Infof("%v pipeline context has started successfully", topic)
	}

	if err == nil {
		ctx.isRunning = true
	}

	return err
}

func (ctx *PipelineRuntimeCtx) Stop() base.ErrorMap {
	topic := ""
	if ctx.pipeline != nil {
		topic = ctx.pipeline.Topic()
	}

	errMap := make(base.ErrorMap)

	// Before stopping service, detach
	detachFunc := func() error {
		ctx.runtime_svcs_lock.RLock()
		defer ctx.runtime_svcs_lock.RUnlock()

		waitGrp := &sync.WaitGroup{}
		errCh := make(chan base.ComponentError, len(ctx.runtime_svcs))

		// detach all registered services if they support it
		for name, service := range ctx.runtime_svcs {
			// detach services in parallel to ensure that all services get their turns
			waitGrp.Add(1)
			go ctx.detachService(name, service, waitGrp, errCh)
		}

		waitGrp.Wait()

		if len(errCh) != 0 {
			errMap = base.FormatErrMsg(errCh)
		}
		return nil
	}

	// Only detach non-main pipelines
	if ctx.Pipeline().Type() != common.MainPipeline {
		// put a timeout around service stopping to avoid being stuck
		err := base.ExecWithTimeout(detachFunc, base.TimeoutRuntimeContextStop, ctx.logger)
		if err != nil {
			// if err is not nill, it is possible that stopServicesFunc is still running and may still access errMap
			// return errMap1 instead of errMap to avoid race conditions
			ctx.logger.Warnf("%v Error detaching pipeline context. err=%v", topic, err)
			errMap1 := make(base.ErrorMap)
			errMap1[RuntimeContext] = err
			return errMap1
		}
	}

	if ctx.Pipeline().Type() != common.MainPipeline {
		ctx.logger.Infof("%v Pipeline context is attached to %v, not a main pipeline. Not stopping services", ctx.Pipeline().Type().String, topic)
		return nil
	}
	ctx.logger.Infof("%v Pipeline context is stopping...", topic)

	stopServicesFunc := func() error {
		ctx.runtime_svcs_lock.RLock()
		defer ctx.runtime_svcs_lock.RUnlock()

		waitGrp := &sync.WaitGroup{}
		errCh := make(chan base.ComponentError, len(ctx.runtime_svcs))

		//stop all registered services
		for name, service := range ctx.runtime_svcs {
			// stop services in parallel to ensure that all services get their turns
			waitGrp.Add(1)
			go ctx.stopService(name, service, waitGrp, errCh)
		}

		waitGrp.Wait()

		if len(errCh) != 0 {
			errMap = base.FormatErrMsg(errCh)
		}
		return nil
	}

	// put a timeout around service stopping to avoid being stuck
	err := base.ExecWithTimeout(stopServicesFunc, base.TimeoutRuntimeContextStop, ctx.logger)
	if err != nil {
		// if err is not nill, it is possible that stopServicesFunc is still running and may still access errMap
		// return errMap1 instead of errMap to avoid race conditions
		ctx.logger.Warnf("%v Error stopping pipeline context. err=%v", topic, err)
		errMap1 := make(base.ErrorMap)
		errMap1[RuntimeContext] = err
		return errMap1
	}

	ctx.logger.Infof("%v pipeline context has stopped successfully", topic)
	return errMap
}

func (ctx *PipelineRuntimeCtx) stopService(name string, service common.PipelineService, waitGrp *sync.WaitGroup, errCh chan base.ComponentError) {
	defer waitGrp.Done()

	topic := ""
	if ctx.pipeline != nil {
		topic = ctx.pipeline.Topic()
	}

	err := service.Stop()
	if err != nil {
		ctx.logger.Warnf("%v failed to stop service %s. err=%v", topic, name, err)
		errCh <- base.ComponentError{name, err}
	} else {
		ctx.logger.Infof("%v successfully stopped service %s.", topic, name)
	}
}

func (ctx *PipelineRuntimeCtx) detachService(name string, service common.PipelineService, waitGrp *sync.WaitGroup, errCh chan base.ComponentError) {
	defer waitGrp.Done()

	topic := ""
	if ctx.pipeline != nil {
		topic = ctx.pipeline.Topic()
	}

	if !service.IsSharable() {
		return
	}

	err := service.Detach(ctx.pipeline)
	if err != nil {
		ctx.logger.Warnf("%v failed to detach service %s. err=%v", topic, name, err)
		errCh <- base.ComponentError{name, err}
	} else {
		ctx.logger.Infof("%v successfully detached service %s.", topic, name)
	}
}

func (ctx *PipelineRuntimeCtx) Pipeline() common.Pipeline {
	return ctx.pipeline
}

func (ctx *PipelineRuntimeCtx) Service(svc_name string) common.PipelineService {
	ctx.runtime_svcs_lock.RLock()
	defer ctx.runtime_svcs_lock.RUnlock()
	return ctx.runtime_svcs[svc_name]
}

func (ctx *PipelineRuntimeCtx) RegisterService(svc_name string, svc common.PipelineService) error {
	if ctx.isRunning {
		return errors.New("Can't register service when PipelineRuntimeContext is already running")
	}

	ctx.logger.Infof("Try to attach %v to pipeline %v\n", svc_name, ctx.pipeline.InstanceId())
	ctx.addService(svc_name, svc)
	return svc.Attach(ctx.pipeline)

}

func (ctx *PipelineRuntimeCtx) addService(svc_name string, svc common.PipelineService) {
	ctx.runtime_svcs_lock.Lock()
	defer ctx.runtime_svcs_lock.Unlock()
	ctx.runtime_svcs[svc_name] = svc
}

func (ctx *PipelineRuntimeCtx) UnregisterService(srv_name string) error {
	ctx.runtime_svcs_lock.Lock()
	defer ctx.runtime_svcs_lock.Unlock()

	var err error
	svc := ctx.runtime_svcs[srv_name]

	if svc != nil && ctx.isRunning {
		err = svc.Stop()
		if err != nil {
			//log error
			ctx.logger.Errorf(fmt.Sprintf("Failed to stop service %v", srv_name))
		}
	}

	delete(ctx.runtime_svcs, srv_name)

	return err
}

func (ctx *PipelineRuntimeCtx) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	ctx.runtime_svcs_lock.RLock()
	defer ctx.runtime_svcs_lock.RUnlock()

	if ctx.service_settings_constructor == nil {
		return nil
	}

	for name, svc := range ctx.runtime_svcs {
		if svc != nil {
			service_settings, err := ctx.service_update_settings_constructor(ctx.pipeline, svc, settings)
			if err != nil {
				ctx.logger.Errorf("Error constructing update settings for service %v. err=%v", name, err)
				return err

			}
			err = svc.UpdateSettings(service_settings)
			if err != nil {
				ctx.logger.Errorf("Error updating settings for service %v. settings=%v, err=%v", name, service_settings.CloneAndRedact(), err)
				return err
			}
		}
	}

	return nil
}

//enforcer for PipelineRuntimeCtx to implement PipelineRuntimeContext
var _ common.PipelineRuntimeContext = (*PipelineRuntimeCtx)(nil)
