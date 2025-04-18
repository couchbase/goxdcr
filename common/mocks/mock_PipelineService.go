// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	common "github.com/couchbase/goxdcr/v8/common"
	metadata "github.com/couchbase/goxdcr/v8/metadata"

	mock "github.com/stretchr/testify/mock"
)

// PipelineService is an autogenerated mock type for the PipelineService type
type PipelineService struct {
	mock.Mock
}

type PipelineService_Expecter struct {
	mock *mock.Mock
}

func (_m *PipelineService) EXPECT() *PipelineService_Expecter {
	return &PipelineService_Expecter{mock: &_m.Mock}
}

// Attach provides a mock function with given fields: pipeline
func (_m *PipelineService) Attach(pipeline common.Pipeline) error {
	ret := _m.Called(pipeline)

	if len(ret) == 0 {
		panic("no return value specified for Attach")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(common.Pipeline) error); ok {
		r0 = rf(pipeline)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineService_Attach_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Attach'
type PipelineService_Attach_Call struct {
	*mock.Call
}

// Attach is a helper method to define mock.On call
//   - pipeline common.Pipeline
func (_e *PipelineService_Expecter) Attach(pipeline interface{}) *PipelineService_Attach_Call {
	return &PipelineService_Attach_Call{Call: _e.mock.On("Attach", pipeline)}
}

func (_c *PipelineService_Attach_Call) Run(run func(pipeline common.Pipeline)) *PipelineService_Attach_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(common.Pipeline))
	})
	return _c
}

func (_c *PipelineService_Attach_Call) Return(_a0 error) *PipelineService_Attach_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineService_Attach_Call) RunAndReturn(run func(common.Pipeline) error) *PipelineService_Attach_Call {
	_c.Call.Return(run)
	return _c
}

// Detach provides a mock function with given fields: pipeline
func (_m *PipelineService) Detach(pipeline common.Pipeline) error {
	ret := _m.Called(pipeline)

	if len(ret) == 0 {
		panic("no return value specified for Detach")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(common.Pipeline) error); ok {
		r0 = rf(pipeline)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineService_Detach_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Detach'
type PipelineService_Detach_Call struct {
	*mock.Call
}

// Detach is a helper method to define mock.On call
//   - pipeline common.Pipeline
func (_e *PipelineService_Expecter) Detach(pipeline interface{}) *PipelineService_Detach_Call {
	return &PipelineService_Detach_Call{Call: _e.mock.On("Detach", pipeline)}
}

func (_c *PipelineService_Detach_Call) Run(run func(pipeline common.Pipeline)) *PipelineService_Detach_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(common.Pipeline))
	})
	return _c
}

func (_c *PipelineService_Detach_Call) Return(_a0 error) *PipelineService_Detach_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineService_Detach_Call) RunAndReturn(run func(common.Pipeline) error) *PipelineService_Detach_Call {
	_c.Call.Return(run)
	return _c
}

// IsSharable provides a mock function with no fields
func (_m *PipelineService) IsSharable() bool {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for IsSharable")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// PipelineService_IsSharable_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'IsSharable'
type PipelineService_IsSharable_Call struct {
	*mock.Call
}

// IsSharable is a helper method to define mock.On call
func (_e *PipelineService_Expecter) IsSharable() *PipelineService_IsSharable_Call {
	return &PipelineService_IsSharable_Call{Call: _e.mock.On("IsSharable")}
}

func (_c *PipelineService_IsSharable_Call) Run(run func()) *PipelineService_IsSharable_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineService_IsSharable_Call) Return(_a0 bool) *PipelineService_IsSharable_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineService_IsSharable_Call) RunAndReturn(run func() bool) *PipelineService_IsSharable_Call {
	_c.Call.Return(run)
	return _c
}

// Start provides a mock function with given fields: _a0
func (_m *PipelineService) Start(_a0 metadata.ReplicationSettingsMap) error {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for Start")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.ReplicationSettingsMap) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineService_Start_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Start'
type PipelineService_Start_Call struct {
	*mock.Call
}

// Start is a helper method to define mock.On call
//   - _a0 metadata.ReplicationSettingsMap
func (_e *PipelineService_Expecter) Start(_a0 interface{}) *PipelineService_Start_Call {
	return &PipelineService_Start_Call{Call: _e.mock.On("Start", _a0)}
}

func (_c *PipelineService_Start_Call) Run(run func(_a0 metadata.ReplicationSettingsMap)) *PipelineService_Start_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(metadata.ReplicationSettingsMap))
	})
	return _c
}

func (_c *PipelineService_Start_Call) Return(_a0 error) *PipelineService_Start_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineService_Start_Call) RunAndReturn(run func(metadata.ReplicationSettingsMap) error) *PipelineService_Start_Call {
	_c.Call.Return(run)
	return _c
}

// Stop provides a mock function with no fields
func (_m *PipelineService) Stop() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Stop")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineService_Stop_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Stop'
type PipelineService_Stop_Call struct {
	*mock.Call
}

// Stop is a helper method to define mock.On call
func (_e *PipelineService_Expecter) Stop() *PipelineService_Stop_Call {
	return &PipelineService_Stop_Call{Call: _e.mock.On("Stop")}
}

func (_c *PipelineService_Stop_Call) Run(run func()) *PipelineService_Stop_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineService_Stop_Call) Return(_a0 error) *PipelineService_Stop_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineService_Stop_Call) RunAndReturn(run func() error) *PipelineService_Stop_Call {
	_c.Call.Return(run)
	return _c
}

// UpdateSettings provides a mock function with given fields: settings
func (_m *PipelineService) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	ret := _m.Called(settings)

	if len(ret) == 0 {
		panic("no return value specified for UpdateSettings")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.ReplicationSettingsMap) error); ok {
		r0 = rf(settings)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineService_UpdateSettings_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UpdateSettings'
type PipelineService_UpdateSettings_Call struct {
	*mock.Call
}

// UpdateSettings is a helper method to define mock.On call
//   - settings metadata.ReplicationSettingsMap
func (_e *PipelineService_Expecter) UpdateSettings(settings interface{}) *PipelineService_UpdateSettings_Call {
	return &PipelineService_UpdateSettings_Call{Call: _e.mock.On("UpdateSettings", settings)}
}

func (_c *PipelineService_UpdateSettings_Call) Run(run func(settings metadata.ReplicationSettingsMap)) *PipelineService_UpdateSettings_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(metadata.ReplicationSettingsMap))
	})
	return _c
}

func (_c *PipelineService_UpdateSettings_Call) Return(_a0 error) *PipelineService_UpdateSettings_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineService_UpdateSettings_Call) RunAndReturn(run func(metadata.ReplicationSettingsMap) error) *PipelineService_UpdateSettings_Call {
	_c.Call.Return(run)
	return _c
}

// NewPipelineService creates a new instance of PipelineService. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewPipelineService(t interface {
	mock.TestingT
	Cleanup(func())
}) *PipelineService {
	mock := &PipelineService{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
