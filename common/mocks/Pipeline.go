package mocks

import common "github.com/couchbase/goxdcr/common"
import metadata "github.com/couchbase/goxdcr/metadata"
import mock "github.com/stretchr/testify/mock"

// Pipeline is an autogenerated mock type for the Pipeline type
type Pipeline struct {
	mock.Mock
}

// InstanceId provides a mock function with given fields:
func (_m *Pipeline) InstanceId() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// ReportProgress provides a mock function with given fields: progress
func (_m *Pipeline) ReportProgress(progress string) {
	_m.Called(progress)
}

// RuntimeContext provides a mock function with given fields:
func (_m *Pipeline) RuntimeContext() common.PipelineRuntimeContext {
	ret := _m.Called()

	var r0 common.PipelineRuntimeContext
	if rf, ok := ret.Get(0).(func() common.PipelineRuntimeContext); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.PipelineRuntimeContext)
		}
	}

	return r0
}

// SetProgressRecorder provides a mock function with given fields: recorder
func (_m *Pipeline) SetProgressRecorder(recorder common.PipelineProgressRecorder) {
	_m.Called(recorder)
}

// SetRuntimeContext provides a mock function with given fields: ctx
func (_m *Pipeline) SetRuntimeContext(ctx common.PipelineRuntimeContext) {
	_m.Called(ctx)
}

// SetState provides a mock function with given fields: state
func (_m *Pipeline) SetState(state common.PipelineState) error {
	ret := _m.Called(state)

	var r0 error
	if rf, ok := ret.Get(0).(func(common.PipelineState) error); ok {
		r0 = rf(state)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Settings provides a mock function with given fields:
func (_m *Pipeline) Settings() map[string]interface{} {
	ret := _m.Called()

	var r0 map[string]interface{}
	if rf, ok := ret.Get(0).(func() map[string]interface{}); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]interface{})
		}
	}

	return r0
}

// Sources provides a mock function with given fields:
func (_m *Pipeline) Sources() map[string]common.Nozzle {
	ret := _m.Called()

	var r0 map[string]common.Nozzle
	if rf, ok := ret.Get(0).(func() map[string]common.Nozzle); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]common.Nozzle)
		}
	}

	return r0
}

// Specification provides a mock function with given fields:
func (_m *Pipeline) Specification() *metadata.ReplicationSpecification {
	ret := _m.Called()

	var r0 *metadata.ReplicationSpecification
	if rf, ok := ret.Get(0).(func() *metadata.ReplicationSpecification); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReplicationSpecification)
		}
	}

	return r0
}

// Start provides a mock function with given fields: settings
func (_m *Pipeline) Start(settings map[string]interface{}) error {
	ret := _m.Called(settings)

	var r0 error
	if rf, ok := ret.Get(0).(func(map[string]interface{}) error); ok {
		r0 = rf(settings)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// State provides a mock function with given fields:
func (_m *Pipeline) State() common.PipelineState {
	ret := _m.Called()

	var r0 common.PipelineState
	if rf, ok := ret.Get(0).(func() common.PipelineState); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(common.PipelineState)
	}

	return r0
}

// Stop provides a mock function with given fields:
func (_m *Pipeline) Stop() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Targets provides a mock function with given fields:
func (_m *Pipeline) Targets() map[string]common.Nozzle {
	ret := _m.Called()

	var r0 map[string]common.Nozzle
	if rf, ok := ret.Get(0).(func() map[string]common.Nozzle); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]common.Nozzle)
		}
	}

	return r0
}

// Topic provides a mock function with given fields:
func (_m *Pipeline) Topic() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// UpdateSettings provides a mock function with given fields: settings
func (_m *Pipeline) UpdateSettings(settings map[string]interface{}) error {
	ret := _m.Called(settings)

	var r0 error
	if rf, ok := ret.Get(0).(func(map[string]interface{}) error); ok {
		r0 = rf(settings)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
