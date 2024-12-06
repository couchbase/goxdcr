// Code generated by mockery v2.43.2. DO NOT EDIT.

package mocks

import (
	common "github.com/couchbase/goxdcr/common"
	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"
)

// CheckpointMgrSvc is an autogenerated mock type for the CheckpointMgrSvc type
type CheckpointMgrSvc struct {
	mock.Mock
}

type CheckpointMgrSvc_Expecter struct {
	mock *mock.Mock
}

func (_m *CheckpointMgrSvc) EXPECT() *CheckpointMgrSvc_Expecter {
	return &CheckpointMgrSvc_Expecter{mock: &_m.Mock}
}

// Attach provides a mock function with given fields: pipeline
func (_m *CheckpointMgrSvc) Attach(pipeline common.Pipeline) error {
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

// CheckpointMgrSvc_Attach_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Attach'
type CheckpointMgrSvc_Attach_Call struct {
	*mock.Call
}

// Attach is a helper method to define mock.On call
//   - pipeline common.Pipeline
func (_e *CheckpointMgrSvc_Expecter) Attach(pipeline interface{}) *CheckpointMgrSvc_Attach_Call {
	return &CheckpointMgrSvc_Attach_Call{Call: _e.mock.On("Attach", pipeline)}
}

func (_c *CheckpointMgrSvc_Attach_Call) Run(run func(pipeline common.Pipeline)) *CheckpointMgrSvc_Attach_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(common.Pipeline))
	})
	return _c
}

func (_c *CheckpointMgrSvc_Attach_Call) Return(_a0 error) *CheckpointMgrSvc_Attach_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointMgrSvc_Attach_Call) RunAndReturn(run func(common.Pipeline) error) *CheckpointMgrSvc_Attach_Call {
	_c.Call.Return(run)
	return _c
}

// CheckpointsExist provides a mock function with given fields: topic
func (_m *CheckpointMgrSvc) CheckpointsExist(topic string) (bool, error) {
	ret := _m.Called(topic)

	if len(ret) == 0 {
		panic("no return value specified for CheckpointsExist")
	}

	var r0 bool
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (bool, error)); ok {
		return rf(topic)
	}
	if rf, ok := ret.Get(0).(func(string) bool); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Get(0).(bool)
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(topic)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CheckpointMgrSvc_CheckpointsExist_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CheckpointsExist'
type CheckpointMgrSvc_CheckpointsExist_Call struct {
	*mock.Call
}

// CheckpointsExist is a helper method to define mock.On call
//   - topic string
func (_e *CheckpointMgrSvc_Expecter) CheckpointsExist(topic interface{}) *CheckpointMgrSvc_CheckpointsExist_Call {
	return &CheckpointMgrSvc_CheckpointsExist_Call{Call: _e.mock.On("CheckpointsExist", topic)}
}

func (_c *CheckpointMgrSvc_CheckpointsExist_Call) Run(run func(topic string)) *CheckpointMgrSvc_CheckpointsExist_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *CheckpointMgrSvc_CheckpointsExist_Call) Return(_a0 bool, _a1 error) *CheckpointMgrSvc_CheckpointsExist_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CheckpointMgrSvc_CheckpointsExist_Call) RunAndReturn(run func(string) (bool, error)) *CheckpointMgrSvc_CheckpointsExist_Call {
	_c.Call.Return(run)
	return _c
}

// DelSingleVBCheckpoint provides a mock function with given fields: topic, vbno, internalId
func (_m *CheckpointMgrSvc) DelSingleVBCheckpoint(topic string, vbno uint16, internalId string) error {
	ret := _m.Called(topic, vbno, internalId)

	if len(ret) == 0 {
		panic("no return value specified for DelSingleVBCheckpoint")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, uint16, string) error); ok {
		r0 = rf(topic, vbno, internalId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CheckpointMgrSvc_DelSingleVBCheckpoint_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DelSingleVBCheckpoint'
type CheckpointMgrSvc_DelSingleVBCheckpoint_Call struct {
	*mock.Call
}

// DelSingleVBCheckpoint is a helper method to define mock.On call
//   - topic string
//   - vbno uint16
//   - internalId string
func (_e *CheckpointMgrSvc_Expecter) DelSingleVBCheckpoint(topic interface{}, vbno interface{}, internalId interface{}) *CheckpointMgrSvc_DelSingleVBCheckpoint_Call {
	return &CheckpointMgrSvc_DelSingleVBCheckpoint_Call{Call: _e.mock.On("DelSingleVBCheckpoint", topic, vbno, internalId)}
}

func (_c *CheckpointMgrSvc_DelSingleVBCheckpoint_Call) Run(run func(topic string, vbno uint16, internalId string)) *CheckpointMgrSvc_DelSingleVBCheckpoint_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(uint16), args[2].(string))
	})
	return _c
}

func (_c *CheckpointMgrSvc_DelSingleVBCheckpoint_Call) Return(_a0 error) *CheckpointMgrSvc_DelSingleVBCheckpoint_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointMgrSvc_DelSingleVBCheckpoint_Call) RunAndReturn(run func(string, uint16, string) error) *CheckpointMgrSvc_DelSingleVBCheckpoint_Call {
	_c.Call.Return(run)
	return _c
}

// Detach provides a mock function with given fields: pipeline
func (_m *CheckpointMgrSvc) Detach(pipeline common.Pipeline) error {
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

// CheckpointMgrSvc_Detach_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Detach'
type CheckpointMgrSvc_Detach_Call struct {
	*mock.Call
}

// Detach is a helper method to define mock.On call
//   - pipeline common.Pipeline
func (_e *CheckpointMgrSvc_Expecter) Detach(pipeline interface{}) *CheckpointMgrSvc_Detach_Call {
	return &CheckpointMgrSvc_Detach_Call{Call: _e.mock.On("Detach", pipeline)}
}

func (_c *CheckpointMgrSvc_Detach_Call) Run(run func(pipeline common.Pipeline)) *CheckpointMgrSvc_Detach_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(common.Pipeline))
	})
	return _c
}

func (_c *CheckpointMgrSvc_Detach_Call) Return(_a0 error) *CheckpointMgrSvc_Detach_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointMgrSvc_Detach_Call) RunAndReturn(run func(common.Pipeline) error) *CheckpointMgrSvc_Detach_Call {
	_c.Call.Return(run)
	return _c
}

// IsSharable provides a mock function with given fields:
func (_m *CheckpointMgrSvc) IsSharable() bool {
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

// CheckpointMgrSvc_IsSharable_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'IsSharable'
type CheckpointMgrSvc_IsSharable_Call struct {
	*mock.Call
}

// IsSharable is a helper method to define mock.On call
func (_e *CheckpointMgrSvc_Expecter) IsSharable() *CheckpointMgrSvc_IsSharable_Call {
	return &CheckpointMgrSvc_IsSharable_Call{Call: _e.mock.On("IsSharable")}
}

func (_c *CheckpointMgrSvc_IsSharable_Call) Run(run func()) *CheckpointMgrSvc_IsSharable_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CheckpointMgrSvc_IsSharable_Call) Return(_a0 bool) *CheckpointMgrSvc_IsSharable_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointMgrSvc_IsSharable_Call) RunAndReturn(run func() bool) *CheckpointMgrSvc_IsSharable_Call {
	_c.Call.Return(run)
	return _c
}

// MergePeerNodesCkptInfo provides a mock function with given fields: genericResponse
func (_m *CheckpointMgrSvc) MergePeerNodesCkptInfo(genericResponse interface{}) error {
	ret := _m.Called(genericResponse)

	if len(ret) == 0 {
		panic("no return value specified for MergePeerNodesCkptInfo")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(genericResponse)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CheckpointMgrSvc_MergePeerNodesCkptInfo_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'MergePeerNodesCkptInfo'
type CheckpointMgrSvc_MergePeerNodesCkptInfo_Call struct {
	*mock.Call
}

// MergePeerNodesCkptInfo is a helper method to define mock.On call
//   - genericResponse interface{}
func (_e *CheckpointMgrSvc_Expecter) MergePeerNodesCkptInfo(genericResponse interface{}) *CheckpointMgrSvc_MergePeerNodesCkptInfo_Call {
	return &CheckpointMgrSvc_MergePeerNodesCkptInfo_Call{Call: _e.mock.On("MergePeerNodesCkptInfo", genericResponse)}
}

func (_c *CheckpointMgrSvc_MergePeerNodesCkptInfo_Call) Run(run func(genericResponse interface{})) *CheckpointMgrSvc_MergePeerNodesCkptInfo_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(interface{}))
	})
	return _c
}

func (_c *CheckpointMgrSvc_MergePeerNodesCkptInfo_Call) Return(_a0 error) *CheckpointMgrSvc_MergePeerNodesCkptInfo_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointMgrSvc_MergePeerNodesCkptInfo_Call) RunAndReturn(run func(interface{}) error) *CheckpointMgrSvc_MergePeerNodesCkptInfo_Call {
	_c.Call.Return(run)
	return _c
}

// Start provides a mock function with given fields: _a0
func (_m *CheckpointMgrSvc) Start(_a0 metadata.ReplicationSettingsMap) error {
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

// CheckpointMgrSvc_Start_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Start'
type CheckpointMgrSvc_Start_Call struct {
	*mock.Call
}

// Start is a helper method to define mock.On call
//   - _a0 metadata.ReplicationSettingsMap
func (_e *CheckpointMgrSvc_Expecter) Start(_a0 interface{}) *CheckpointMgrSvc_Start_Call {
	return &CheckpointMgrSvc_Start_Call{Call: _e.mock.On("Start", _a0)}
}

func (_c *CheckpointMgrSvc_Start_Call) Run(run func(_a0 metadata.ReplicationSettingsMap)) *CheckpointMgrSvc_Start_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(metadata.ReplicationSettingsMap))
	})
	return _c
}

func (_c *CheckpointMgrSvc_Start_Call) Return(_a0 error) *CheckpointMgrSvc_Start_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointMgrSvc_Start_Call) RunAndReturn(run func(metadata.ReplicationSettingsMap) error) *CheckpointMgrSvc_Start_Call {
	_c.Call.Return(run)
	return _c
}

// Stop provides a mock function with given fields:
func (_m *CheckpointMgrSvc) Stop() error {
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

// CheckpointMgrSvc_Stop_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Stop'
type CheckpointMgrSvc_Stop_Call struct {
	*mock.Call
}

// Stop is a helper method to define mock.On call
func (_e *CheckpointMgrSvc_Expecter) Stop() *CheckpointMgrSvc_Stop_Call {
	return &CheckpointMgrSvc_Stop_Call{Call: _e.mock.On("Stop")}
}

func (_c *CheckpointMgrSvc_Stop_Call) Run(run func()) *CheckpointMgrSvc_Stop_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CheckpointMgrSvc_Stop_Call) Return(_a0 error) *CheckpointMgrSvc_Stop_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointMgrSvc_Stop_Call) RunAndReturn(run func() error) *CheckpointMgrSvc_Stop_Call {
	_c.Call.Return(run)
	return _c
}

// UpdateSettings provides a mock function with given fields: settings
func (_m *CheckpointMgrSvc) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
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

// CheckpointMgrSvc_UpdateSettings_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UpdateSettings'
type CheckpointMgrSvc_UpdateSettings_Call struct {
	*mock.Call
}

// UpdateSettings is a helper method to define mock.On call
//   - settings metadata.ReplicationSettingsMap
func (_e *CheckpointMgrSvc_Expecter) UpdateSettings(settings interface{}) *CheckpointMgrSvc_UpdateSettings_Call {
	return &CheckpointMgrSvc_UpdateSettings_Call{Call: _e.mock.On("UpdateSettings", settings)}
}

func (_c *CheckpointMgrSvc_UpdateSettings_Call) Run(run func(settings metadata.ReplicationSettingsMap)) *CheckpointMgrSvc_UpdateSettings_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(metadata.ReplicationSettingsMap))
	})
	return _c
}

func (_c *CheckpointMgrSvc_UpdateSettings_Call) Return(_a0 error) *CheckpointMgrSvc_UpdateSettings_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointMgrSvc_UpdateSettings_Call) RunAndReturn(run func(metadata.ReplicationSettingsMap) error) *CheckpointMgrSvc_UpdateSettings_Call {
	_c.Call.Return(run)
	return _c
}

// NewCheckpointMgrSvc creates a new instance of CheckpointMgrSvc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewCheckpointMgrSvc(t interface {
	mock.TestingT
	Cleanup(func())
}) *CheckpointMgrSvc {
	mock := &CheckpointMgrSvc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
