package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"

	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"
)

// Pipeline is an autogenerated mock type for the Pipeline type
type Pipeline struct {
	mock.Mock
}

// FullTopic provides a mock function with given fields:
func (_m *Pipeline) FullTopic() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// GetAsyncListenerMap provides a mock function with given fields:
func (_m *Pipeline) GetAsyncListenerMap() map[string]common.AsyncComponentEventListener {
	ret := _m.Called()

	var r0 map[string]common.AsyncComponentEventListener
	if rf, ok := ret.Get(0).(func() map[string]common.AsyncComponentEventListener); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]common.AsyncComponentEventListener)
		}
	}

	return r0
}

// GetBrokenMapRO provides a mock function with given fields:
func (_m *Pipeline) GetBrokenMapRO() (metadata.CollectionNamespaceMapping, func()) {
	ret := _m.Called()

	var r0 metadata.CollectionNamespaceMapping
	if rf, ok := ret.Get(0).(func() metadata.CollectionNamespaceMapping); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(metadata.CollectionNamespaceMapping)
		}
	}

	var r1 func()
	if rf, ok := ret.Get(1).(func() func()); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(func())
		}
	}

	return r0, r1
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

// SetAsyncListenerMap provides a mock function with given fields: _a0
func (_m *Pipeline) SetAsyncListenerMap(_a0 map[string]common.AsyncComponentEventListener) {
	_m.Called(_a0)
}

// SetBrokenMap provides a mock function with given fields: brokenMap
func (_m *Pipeline) SetBrokenMap(brokenMap metadata.CollectionNamespaceMapping) {
	_m.Called(brokenMap)
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
func (_m *Pipeline) Settings() metadata.ReplicationSettingsMap {
	ret := _m.Called()

	var r0 metadata.ReplicationSettingsMap
	if rf, ok := ret.Get(0).(func() metadata.ReplicationSettingsMap); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(metadata.ReplicationSettingsMap)
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
func (_m *Pipeline) Specification() metadata.GenericSpecification {
	ret := _m.Called()

	var r0 metadata.GenericSpecification
	if rf, ok := ret.Get(0).(func() metadata.GenericSpecification); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(metadata.GenericSpecification)
		}
	}

	return r0
}

// Start provides a mock function with given fields: settings
func (_m *Pipeline) Start(settings metadata.ReplicationSettingsMap) base.ErrorMap {
	ret := _m.Called(settings)

	var r0 base.ErrorMap
	if rf, ok := ret.Get(0).(func(metadata.ReplicationSettingsMap) base.ErrorMap); ok {
		r0 = rf(settings)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.ErrorMap)
		}
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
func (_m *Pipeline) Stop() base.ErrorMap {
	ret := _m.Called()

	var r0 base.ErrorMap
	if rf, ok := ret.Get(0).(func() base.ErrorMap); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.ErrorMap)
		}
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

// Type provides a mock function with given fields:
func (_m *Pipeline) Type() common.PipelineType {
	ret := _m.Called()

	var r0 common.PipelineType
	if rf, ok := ret.Get(0).(func() common.PipelineType); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(common.PipelineType)
	}

	return r0
}

// UpdateSettings provides a mock function with given fields: settings
func (_m *Pipeline) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	ret := _m.Called(settings)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.ReplicationSettingsMap) error); ok {
		r0 = rf(settings)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
