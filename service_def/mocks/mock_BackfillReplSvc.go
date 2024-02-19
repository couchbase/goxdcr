// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"

	sync "sync"
)

// BackfillReplSvc is an autogenerated mock type for the BackfillReplSvc type
type BackfillReplSvc struct {
	mock.Mock
}

type BackfillReplSvc_Expecter struct {
	mock *mock.Mock
}

func (_m *BackfillReplSvc) EXPECT() *BackfillReplSvc_Expecter {
	return &BackfillReplSvc_Expecter{mock: &_m.Mock}
}

// AddBackfillReplSpec provides a mock function with given fields: spec
func (_m *BackfillReplSvc) AddBackfillReplSpec(spec *metadata.BackfillReplicationSpec) error {
	ret := _m.Called(spec)

	if len(ret) == 0 {
		panic("no return value specified for AddBackfillReplSpec")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.BackfillReplicationSpec) error); ok {
		r0 = rf(spec)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// BackfillReplSvc_AddBackfillReplSpec_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AddBackfillReplSpec'
type BackfillReplSvc_AddBackfillReplSpec_Call struct {
	*mock.Call
}

// AddBackfillReplSpec is a helper method to define mock.On call
//   - spec *metadata.BackfillReplicationSpec
func (_e *BackfillReplSvc_Expecter) AddBackfillReplSpec(spec interface{}) *BackfillReplSvc_AddBackfillReplSpec_Call {
	return &BackfillReplSvc_AddBackfillReplSpec_Call{Call: _e.mock.On("AddBackfillReplSpec", spec)}
}

func (_c *BackfillReplSvc_AddBackfillReplSpec_Call) Run(run func(spec *metadata.BackfillReplicationSpec)) *BackfillReplSvc_AddBackfillReplSpec_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.BackfillReplicationSpec))
	})
	return _c
}

func (_c *BackfillReplSvc_AddBackfillReplSpec_Call) Return(_a0 error) *BackfillReplSvc_AddBackfillReplSpec_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *BackfillReplSvc_AddBackfillReplSpec_Call) RunAndReturn(run func(*metadata.BackfillReplicationSpec) error) *BackfillReplSvc_AddBackfillReplSpec_Call {
	_c.Call.Return(run)
	return _c
}

// AllActiveBackfillSpecsReadOnly provides a mock function with given fields:
func (_m *BackfillReplSvc) AllActiveBackfillSpecsReadOnly() (map[string]*metadata.BackfillReplicationSpec, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for AllActiveBackfillSpecsReadOnly")
	}

	var r0 map[string]*metadata.BackfillReplicationSpec
	var r1 error
	if rf, ok := ret.Get(0).(func() (map[string]*metadata.BackfillReplicationSpec, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() map[string]*metadata.BackfillReplicationSpec); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]*metadata.BackfillReplicationSpec)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BackfillReplSvc_AllActiveBackfillSpecsReadOnly_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AllActiveBackfillSpecsReadOnly'
type BackfillReplSvc_AllActiveBackfillSpecsReadOnly_Call struct {
	*mock.Call
}

// AllActiveBackfillSpecsReadOnly is a helper method to define mock.On call
func (_e *BackfillReplSvc_Expecter) AllActiveBackfillSpecsReadOnly() *BackfillReplSvc_AllActiveBackfillSpecsReadOnly_Call {
	return &BackfillReplSvc_AllActiveBackfillSpecsReadOnly_Call{Call: _e.mock.On("AllActiveBackfillSpecsReadOnly")}
}

func (_c *BackfillReplSvc_AllActiveBackfillSpecsReadOnly_Call) Run(run func()) *BackfillReplSvc_AllActiveBackfillSpecsReadOnly_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *BackfillReplSvc_AllActiveBackfillSpecsReadOnly_Call) Return(_a0 map[string]*metadata.BackfillReplicationSpec, _a1 error) *BackfillReplSvc_AllActiveBackfillSpecsReadOnly_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *BackfillReplSvc_AllActiveBackfillSpecsReadOnly_Call) RunAndReturn(run func() (map[string]*metadata.BackfillReplicationSpec, error)) *BackfillReplSvc_AllActiveBackfillSpecsReadOnly_Call {
	_c.Call.Return(run)
	return _c
}

// BackfillReplSpec provides a mock function with given fields: replicationId
func (_m *BackfillReplSvc) BackfillReplSpec(replicationId string) (*metadata.BackfillReplicationSpec, error) {
	ret := _m.Called(replicationId)

	if len(ret) == 0 {
		panic("no return value specified for BackfillReplSpec")
	}

	var r0 *metadata.BackfillReplicationSpec
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (*metadata.BackfillReplicationSpec, error)); ok {
		return rf(replicationId)
	}
	if rf, ok := ret.Get(0).(func(string) *metadata.BackfillReplicationSpec); ok {
		r0 = rf(replicationId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.BackfillReplicationSpec)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(replicationId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BackfillReplSvc_BackfillReplSpec_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'BackfillReplSpec'
type BackfillReplSvc_BackfillReplSpec_Call struct {
	*mock.Call
}

// BackfillReplSpec is a helper method to define mock.On call
//   - replicationId string
func (_e *BackfillReplSvc_Expecter) BackfillReplSpec(replicationId interface{}) *BackfillReplSvc_BackfillReplSpec_Call {
	return &BackfillReplSvc_BackfillReplSpec_Call{Call: _e.mock.On("BackfillReplSpec", replicationId)}
}

func (_c *BackfillReplSvc_BackfillReplSpec_Call) Run(run func(replicationId string)) *BackfillReplSvc_BackfillReplSpec_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *BackfillReplSvc_BackfillReplSpec_Call) Return(_a0 *metadata.BackfillReplicationSpec, _a1 error) *BackfillReplSvc_BackfillReplSpec_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *BackfillReplSvc_BackfillReplSpec_Call) RunAndReturn(run func(string) (*metadata.BackfillReplicationSpec, error)) *BackfillReplSvc_BackfillReplSpec_Call {
	_c.Call.Return(run)
	return _c
}

// DelBackfillReplSpec provides a mock function with given fields: replicationId
func (_m *BackfillReplSvc) DelBackfillReplSpec(replicationId string) (*metadata.BackfillReplicationSpec, error) {
	ret := _m.Called(replicationId)

	if len(ret) == 0 {
		panic("no return value specified for DelBackfillReplSpec")
	}

	var r0 *metadata.BackfillReplicationSpec
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (*metadata.BackfillReplicationSpec, error)); ok {
		return rf(replicationId)
	}
	if rf, ok := ret.Get(0).(func(string) *metadata.BackfillReplicationSpec); ok {
		r0 = rf(replicationId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.BackfillReplicationSpec)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(replicationId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BackfillReplSvc_DelBackfillReplSpec_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DelBackfillReplSpec'
type BackfillReplSvc_DelBackfillReplSpec_Call struct {
	*mock.Call
}

// DelBackfillReplSpec is a helper method to define mock.On call
//   - replicationId string
func (_e *BackfillReplSvc_Expecter) DelBackfillReplSpec(replicationId interface{}) *BackfillReplSvc_DelBackfillReplSpec_Call {
	return &BackfillReplSvc_DelBackfillReplSpec_Call{Call: _e.mock.On("DelBackfillReplSpec", replicationId)}
}

func (_c *BackfillReplSvc_DelBackfillReplSpec_Call) Run(run func(replicationId string)) *BackfillReplSvc_DelBackfillReplSpec_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *BackfillReplSvc_DelBackfillReplSpec_Call) Return(_a0 *metadata.BackfillReplicationSpec, _a1 error) *BackfillReplSvc_DelBackfillReplSpec_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *BackfillReplSvc_DelBackfillReplSpec_Call) RunAndReturn(run func(string) (*metadata.BackfillReplicationSpec, error)) *BackfillReplSvc_DelBackfillReplSpec_Call {
	_c.Call.Return(run)
	return _c
}

// ReplicationSpecChangeCallback provides a mock function with given fields: id, oldVal, newVal, wg
func (_m *BackfillReplSvc) ReplicationSpecChangeCallback(id string, oldVal interface{}, newVal interface{}, wg *sync.WaitGroup) error {
	ret := _m.Called(id, oldVal, newVal, wg)

	if len(ret) == 0 {
		panic("no return value specified for ReplicationSpecChangeCallback")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, interface{}, interface{}, *sync.WaitGroup) error); ok {
		r0 = rf(id, oldVal, newVal, wg)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// BackfillReplSvc_ReplicationSpecChangeCallback_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ReplicationSpecChangeCallback'
type BackfillReplSvc_ReplicationSpecChangeCallback_Call struct {
	*mock.Call
}

// ReplicationSpecChangeCallback is a helper method to define mock.On call
//   - id string
//   - oldVal interface{}
//   - newVal interface{}
//   - wg *sync.WaitGroup
func (_e *BackfillReplSvc_Expecter) ReplicationSpecChangeCallback(id interface{}, oldVal interface{}, newVal interface{}, wg interface{}) *BackfillReplSvc_ReplicationSpecChangeCallback_Call {
	return &BackfillReplSvc_ReplicationSpecChangeCallback_Call{Call: _e.mock.On("ReplicationSpecChangeCallback", id, oldVal, newVal, wg)}
}

func (_c *BackfillReplSvc_ReplicationSpecChangeCallback_Call) Run(run func(id string, oldVal interface{}, newVal interface{}, wg *sync.WaitGroup)) *BackfillReplSvc_ReplicationSpecChangeCallback_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(interface{}), args[2].(interface{}), args[3].(*sync.WaitGroup))
	})
	return _c
}

func (_c *BackfillReplSvc_ReplicationSpecChangeCallback_Call) Return(_a0 error) *BackfillReplSvc_ReplicationSpecChangeCallback_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *BackfillReplSvc_ReplicationSpecChangeCallback_Call) RunAndReturn(run func(string, interface{}, interface{}, *sync.WaitGroup) error) *BackfillReplSvc_ReplicationSpecChangeCallback_Call {
	_c.Call.Return(run)
	return _c
}

// SetBackfillReplSpec provides a mock function with given fields: spec
func (_m *BackfillReplSvc) SetBackfillReplSpec(spec *metadata.BackfillReplicationSpec) error {
	ret := _m.Called(spec)

	if len(ret) == 0 {
		panic("no return value specified for SetBackfillReplSpec")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.BackfillReplicationSpec) error); ok {
		r0 = rf(spec)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// BackfillReplSvc_SetBackfillReplSpec_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetBackfillReplSpec'
type BackfillReplSvc_SetBackfillReplSpec_Call struct {
	*mock.Call
}

// SetBackfillReplSpec is a helper method to define mock.On call
//   - spec *metadata.BackfillReplicationSpec
func (_e *BackfillReplSvc_Expecter) SetBackfillReplSpec(spec interface{}) *BackfillReplSvc_SetBackfillReplSpec_Call {
	return &BackfillReplSvc_SetBackfillReplSpec_Call{Call: _e.mock.On("SetBackfillReplSpec", spec)}
}

func (_c *BackfillReplSvc_SetBackfillReplSpec_Call) Run(run func(spec *metadata.BackfillReplicationSpec)) *BackfillReplSvc_SetBackfillReplSpec_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.BackfillReplicationSpec))
	})
	return _c
}

func (_c *BackfillReplSvc_SetBackfillReplSpec_Call) Return(_a0 error) *BackfillReplSvc_SetBackfillReplSpec_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *BackfillReplSvc_SetBackfillReplSpec_Call) RunAndReturn(run func(*metadata.BackfillReplicationSpec) error) *BackfillReplSvc_SetBackfillReplSpec_Call {
	_c.Call.Return(run)
	return _c
}

// SetCompleteBackfillRaiser provides a mock function with given fields: backfillCallback
func (_m *BackfillReplSvc) SetCompleteBackfillRaiser(backfillCallback func(string) error) error {
	ret := _m.Called(backfillCallback)

	if len(ret) == 0 {
		panic("no return value specified for SetCompleteBackfillRaiser")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(func(string) error) error); ok {
		r0 = rf(backfillCallback)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// BackfillReplSvc_SetCompleteBackfillRaiser_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetCompleteBackfillRaiser'
type BackfillReplSvc_SetCompleteBackfillRaiser_Call struct {
	*mock.Call
}

// SetCompleteBackfillRaiser is a helper method to define mock.On call
//   - backfillCallback func(string) error
func (_e *BackfillReplSvc_Expecter) SetCompleteBackfillRaiser(backfillCallback interface{}) *BackfillReplSvc_SetCompleteBackfillRaiser_Call {
	return &BackfillReplSvc_SetCompleteBackfillRaiser_Call{Call: _e.mock.On("SetCompleteBackfillRaiser", backfillCallback)}
}

func (_c *BackfillReplSvc_SetCompleteBackfillRaiser_Call) Run(run func(backfillCallback func(string) error)) *BackfillReplSvc_SetCompleteBackfillRaiser_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(func(string) error))
	})
	return _c
}

func (_c *BackfillReplSvc_SetCompleteBackfillRaiser_Call) Return(_a0 error) *BackfillReplSvc_SetCompleteBackfillRaiser_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *BackfillReplSvc_SetCompleteBackfillRaiser_Call) RunAndReturn(run func(func(string) error) error) *BackfillReplSvc_SetCompleteBackfillRaiser_Call {
	_c.Call.Return(run)
	return _c
}

// SetMetadataChangeHandlerCallback provides a mock function with given fields: callBack
func (_m *BackfillReplSvc) SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback) {
	_m.Called(callBack)
}

// BackfillReplSvc_SetMetadataChangeHandlerCallback_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetMetadataChangeHandlerCallback'
type BackfillReplSvc_SetMetadataChangeHandlerCallback_Call struct {
	*mock.Call
}

// SetMetadataChangeHandlerCallback is a helper method to define mock.On call
//   - callBack base.MetadataChangeHandlerCallback
func (_e *BackfillReplSvc_Expecter) SetMetadataChangeHandlerCallback(callBack interface{}) *BackfillReplSvc_SetMetadataChangeHandlerCallback_Call {
	return &BackfillReplSvc_SetMetadataChangeHandlerCallback_Call{Call: _e.mock.On("SetMetadataChangeHandlerCallback", callBack)}
}

func (_c *BackfillReplSvc_SetMetadataChangeHandlerCallback_Call) Run(run func(callBack base.MetadataChangeHandlerCallback)) *BackfillReplSvc_SetMetadataChangeHandlerCallback_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(base.MetadataChangeHandlerCallback))
	})
	return _c
}

func (_c *BackfillReplSvc_SetMetadataChangeHandlerCallback_Call) Return() *BackfillReplSvc_SetMetadataChangeHandlerCallback_Call {
	_c.Call.Return()
	return _c
}

func (_c *BackfillReplSvc_SetMetadataChangeHandlerCallback_Call) RunAndReturn(run func(base.MetadataChangeHandlerCallback)) *BackfillReplSvc_SetMetadataChangeHandlerCallback_Call {
	_c.Call.Return(run)
	return _c
}

// NewBackfillReplSvc creates a new instance of BackfillReplSvc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewBackfillReplSvc(t interface {
	mock.TestingT
	Cleanup(func())
}) *BackfillReplSvc {
	mock := &BackfillReplSvc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
