// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package pipeline_ctx

import (
	"errors"
	"fmt"
	"sync"

	"github.com/couchbase/goxdcr/v8/base"
	common "github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
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
		topic = ctx.pipeline.FullTopic()
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
			ctx.logger.Infof("Service %v has been started", name)
		}
		return nil
	}

	// put a timeout around service starting to avoid being stuck
	err := base.ExecWithTimeout(startServicesFunc, base.TimeoutRuntimeContextStart, ctx.logger)
	if err != nil {
		ctx.logger.Errorf("error starting pipeline context. err=%v", err)
	} else {
		ctx.logger.Infof("pipeline context has started successfully")
	}

	if err == nil {
		ctx.isRunning = true
	}

	return err
}

func (ctx *PipelineRuntimeCtx) Stop() base.ErrorMap {
	topic := ""
	if ctx.pipeline != nil {
		topic = ctx.pipeline.FullTopic()
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

	// put a timeout around service stopping to avoid being stuck
	err := base.ExecWithTimeout(detachFunc, base.TimeoutRuntimeContextStop, ctx.logger)
	if err != nil {
		// if err is not nill, it is possible that stopServicesFunc is still running and may still access errMap
		// return errMap1 instead of errMap to avoid race conditions
		ctx.logger.Warnf("Error detaching pipeline context. err=%v", err)
		errMap1 := make(base.ErrorMap)
		errMap1[RuntimeContext] = err
		return errMap1
	}

	ctx.logger.Infof("Pipeline context is stopping...")

	stopServicesFunc := func() error {
		ctx.runtime_svcs_lock.RLock()
		defer ctx.runtime_svcs_lock.RUnlock()

		waitGrp := &sync.WaitGroup{}
		errCh := make(chan base.ComponentError, len(ctx.runtime_svcs))

		//stop all registered services
		for name, service := range ctx.runtime_svcs {
			if service.IsSharable() && ctx.Pipeline().Type() != common.MainPipeline {
				// If it is sharable, only the main pipeline can stop the service
				ctx.logger.Infof("%v service %v is sharable. This context is not based on Main Pipeline. Skip stopping",
					topic, name)
				continue
			}
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
	err = base.ExecWithTimeout(stopServicesFunc, base.TimeoutRuntimeContextStop, ctx.logger)
	if err != nil {
		// if err is not nill, it is possible that stopServicesFunc is still running and may still access errMap
		// return errMap1 instead of errMap to avoid race conditions
		ctx.logger.Warnf("Error stopping pipeline context. err=%v", err)
		errMap1 := make(base.ErrorMap)
		errMap1[RuntimeContext] = err
		return errMap1
	}

	ctx.logger.Infof("pipeline context has stopped successfully")
	return errMap
}

func (ctx *PipelineRuntimeCtx) stopService(name string, service common.PipelineService, waitGrp *sync.WaitGroup, errCh chan base.ComponentError) {
	defer waitGrp.Done()

	err := service.Stop()
	if err != nil {
		ctx.logger.Warnf("failed to stop service %s. err=%v", name, err)
		errCh <- base.ComponentError{name, err}
	} else {
		ctx.logger.Infof("successfully stopped service %s.", name)
	}
}

func (ctx *PipelineRuntimeCtx) detachService(name string, service common.PipelineService, waitGrp *sync.WaitGroup, errCh chan base.ComponentError) {
	defer waitGrp.Done()

	if !service.IsSharable() {
		return
	}

	err := service.Detach(ctx.pipeline)
	if err != nil && err != base.ErrorNotSupported {
		ctx.logger.Warnf("failed to detach service %s. err=%v", name, err)
		errCh <- base.ComponentError{name, err}
	} else {
		ctx.logger.Infof("successfully detached service %s.", name)
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

// enforcer for PipelineRuntimeCtx to implement PipelineRuntimeContext
var _ common.PipelineRuntimeContext = (*PipelineRuntimeCtx)(nil)
