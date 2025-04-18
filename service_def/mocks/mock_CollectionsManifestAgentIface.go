// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/v8/metadata"
	mock "github.com/stretchr/testify/mock"
)

// CollectionsManifestAgentIface is an autogenerated mock type for the CollectionsManifestAgentIface type
type CollectionsManifestAgentIface struct {
	mock.Mock
}

type CollectionsManifestAgentIface_Expecter struct {
	mock *mock.Mock
}

func (_m *CollectionsManifestAgentIface) EXPECT() *CollectionsManifestAgentIface_Expecter {
	return &CollectionsManifestAgentIface_Expecter{mock: &_m.Mock}
}

// ForceTargetManifestRefresh provides a mock function with no fields
func (_m *CollectionsManifestAgentIface) ForceTargetManifestRefresh() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for ForceTargetManifestRefresh")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CollectionsManifestAgentIface_ForceTargetManifestRefresh_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ForceTargetManifestRefresh'
type CollectionsManifestAgentIface_ForceTargetManifestRefresh_Call struct {
	*mock.Call
}

// ForceTargetManifestRefresh is a helper method to define mock.On call
func (_e *CollectionsManifestAgentIface_Expecter) ForceTargetManifestRefresh() *CollectionsManifestAgentIface_ForceTargetManifestRefresh_Call {
	return &CollectionsManifestAgentIface_ForceTargetManifestRefresh_Call{Call: _e.mock.On("ForceTargetManifestRefresh")}
}

func (_c *CollectionsManifestAgentIface_ForceTargetManifestRefresh_Call) Run(run func()) *CollectionsManifestAgentIface_ForceTargetManifestRefresh_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CollectionsManifestAgentIface_ForceTargetManifestRefresh_Call) Return(_a0 error) *CollectionsManifestAgentIface_ForceTargetManifestRefresh_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CollectionsManifestAgentIface_ForceTargetManifestRefresh_Call) RunAndReturn(run func() error) *CollectionsManifestAgentIface_ForceTargetManifestRefresh_Call {
	_c.Call.Return(run)
	return _c
}

// GetLastPersistedManifests provides a mock function with no fields
func (_m *CollectionsManifestAgentIface) GetLastPersistedManifests() (*metadata.CollectionsManifestPair, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetLastPersistedManifests")
	}

	var r0 *metadata.CollectionsManifestPair
	var r1 error
	if rf, ok := ret.Get(0).(func() (*metadata.CollectionsManifestPair, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() *metadata.CollectionsManifestPair); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifestPair)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CollectionsManifestAgentIface_GetLastPersistedManifests_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetLastPersistedManifests'
type CollectionsManifestAgentIface_GetLastPersistedManifests_Call struct {
	*mock.Call
}

// GetLastPersistedManifests is a helper method to define mock.On call
func (_e *CollectionsManifestAgentIface_Expecter) GetLastPersistedManifests() *CollectionsManifestAgentIface_GetLastPersistedManifests_Call {
	return &CollectionsManifestAgentIface_GetLastPersistedManifests_Call{Call: _e.mock.On("GetLastPersistedManifests")}
}

func (_c *CollectionsManifestAgentIface_GetLastPersistedManifests_Call) Run(run func()) *CollectionsManifestAgentIface_GetLastPersistedManifests_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CollectionsManifestAgentIface_GetLastPersistedManifests_Call) Return(_a0 *metadata.CollectionsManifestPair, _a1 error) *CollectionsManifestAgentIface_GetLastPersistedManifests_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CollectionsManifestAgentIface_GetLastPersistedManifests_Call) RunAndReturn(run func() (*metadata.CollectionsManifestPair, error)) *CollectionsManifestAgentIface_GetLastPersistedManifests_Call {
	_c.Call.Return(run)
	return _c
}

// GetSourceManifest provides a mock function with no fields
func (_m *CollectionsManifestAgentIface) GetSourceManifest() (*metadata.CollectionsManifest, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetSourceManifest")
	}

	var r0 *metadata.CollectionsManifest
	var r1 error
	if rf, ok := ret.Get(0).(func() (*metadata.CollectionsManifest, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() *metadata.CollectionsManifest); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifest)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CollectionsManifestAgentIface_GetSourceManifest_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSourceManifest'
type CollectionsManifestAgentIface_GetSourceManifest_Call struct {
	*mock.Call
}

// GetSourceManifest is a helper method to define mock.On call
func (_e *CollectionsManifestAgentIface_Expecter) GetSourceManifest() *CollectionsManifestAgentIface_GetSourceManifest_Call {
	return &CollectionsManifestAgentIface_GetSourceManifest_Call{Call: _e.mock.On("GetSourceManifest")}
}

func (_c *CollectionsManifestAgentIface_GetSourceManifest_Call) Run(run func()) *CollectionsManifestAgentIface_GetSourceManifest_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CollectionsManifestAgentIface_GetSourceManifest_Call) Return(_a0 *metadata.CollectionsManifest, _a1 error) *CollectionsManifestAgentIface_GetSourceManifest_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CollectionsManifestAgentIface_GetSourceManifest_Call) RunAndReturn(run func() (*metadata.CollectionsManifest, error)) *CollectionsManifestAgentIface_GetSourceManifest_Call {
	_c.Call.Return(run)
	return _c
}

// GetSpecificSourceManifest provides a mock function with given fields: manifestVersion
func (_m *CollectionsManifestAgentIface) GetSpecificSourceManifest(manifestVersion uint64) (*metadata.CollectionsManifest, error) {
	ret := _m.Called(manifestVersion)

	if len(ret) == 0 {
		panic("no return value specified for GetSpecificSourceManifest")
	}

	var r0 *metadata.CollectionsManifest
	var r1 error
	if rf, ok := ret.Get(0).(func(uint64) (*metadata.CollectionsManifest, error)); ok {
		return rf(manifestVersion)
	}
	if rf, ok := ret.Get(0).(func(uint64) *metadata.CollectionsManifest); ok {
		r0 = rf(manifestVersion)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifest)
		}
	}

	if rf, ok := ret.Get(1).(func(uint64) error); ok {
		r1 = rf(manifestVersion)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CollectionsManifestAgentIface_GetSpecificSourceManifest_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSpecificSourceManifest'
type CollectionsManifestAgentIface_GetSpecificSourceManifest_Call struct {
	*mock.Call
}

// GetSpecificSourceManifest is a helper method to define mock.On call
//   - manifestVersion uint64
func (_e *CollectionsManifestAgentIface_Expecter) GetSpecificSourceManifest(manifestVersion interface{}) *CollectionsManifestAgentIface_GetSpecificSourceManifest_Call {
	return &CollectionsManifestAgentIface_GetSpecificSourceManifest_Call{Call: _e.mock.On("GetSpecificSourceManifest", manifestVersion)}
}

func (_c *CollectionsManifestAgentIface_GetSpecificSourceManifest_Call) Run(run func(manifestVersion uint64)) *CollectionsManifestAgentIface_GetSpecificSourceManifest_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint64))
	})
	return _c
}

func (_c *CollectionsManifestAgentIface_GetSpecificSourceManifest_Call) Return(_a0 *metadata.CollectionsManifest, _a1 error) *CollectionsManifestAgentIface_GetSpecificSourceManifest_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CollectionsManifestAgentIface_GetSpecificSourceManifest_Call) RunAndReturn(run func(uint64) (*metadata.CollectionsManifest, error)) *CollectionsManifestAgentIface_GetSpecificSourceManifest_Call {
	_c.Call.Return(run)
	return _c
}

// GetSpecificTargetManifest provides a mock function with given fields: manifestVersion
func (_m *CollectionsManifestAgentIface) GetSpecificTargetManifest(manifestVersion uint64) (*metadata.CollectionsManifest, error) {
	ret := _m.Called(manifestVersion)

	if len(ret) == 0 {
		panic("no return value specified for GetSpecificTargetManifest")
	}

	var r0 *metadata.CollectionsManifest
	var r1 error
	if rf, ok := ret.Get(0).(func(uint64) (*metadata.CollectionsManifest, error)); ok {
		return rf(manifestVersion)
	}
	if rf, ok := ret.Get(0).(func(uint64) *metadata.CollectionsManifest); ok {
		r0 = rf(manifestVersion)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifest)
		}
	}

	if rf, ok := ret.Get(1).(func(uint64) error); ok {
		r1 = rf(manifestVersion)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CollectionsManifestAgentIface_GetSpecificTargetManifest_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSpecificTargetManifest'
type CollectionsManifestAgentIface_GetSpecificTargetManifest_Call struct {
	*mock.Call
}

// GetSpecificTargetManifest is a helper method to define mock.On call
//   - manifestVersion uint64
func (_e *CollectionsManifestAgentIface_Expecter) GetSpecificTargetManifest(manifestVersion interface{}) *CollectionsManifestAgentIface_GetSpecificTargetManifest_Call {
	return &CollectionsManifestAgentIface_GetSpecificTargetManifest_Call{Call: _e.mock.On("GetSpecificTargetManifest", manifestVersion)}
}

func (_c *CollectionsManifestAgentIface_GetSpecificTargetManifest_Call) Run(run func(manifestVersion uint64)) *CollectionsManifestAgentIface_GetSpecificTargetManifest_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint64))
	})
	return _c
}

func (_c *CollectionsManifestAgentIface_GetSpecificTargetManifest_Call) Return(_a0 *metadata.CollectionsManifest, _a1 error) *CollectionsManifestAgentIface_GetSpecificTargetManifest_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CollectionsManifestAgentIface_GetSpecificTargetManifest_Call) RunAndReturn(run func(uint64) (*metadata.CollectionsManifest, error)) *CollectionsManifestAgentIface_GetSpecificTargetManifest_Call {
	_c.Call.Return(run)
	return _c
}

// GetTargetManifest provides a mock function with no fields
func (_m *CollectionsManifestAgentIface) GetTargetManifest() (*metadata.CollectionsManifest, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetTargetManifest")
	}

	var r0 *metadata.CollectionsManifest
	var r1 error
	if rf, ok := ret.Get(0).(func() (*metadata.CollectionsManifest, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() *metadata.CollectionsManifest); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifest)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CollectionsManifestAgentIface_GetTargetManifest_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetTargetManifest'
type CollectionsManifestAgentIface_GetTargetManifest_Call struct {
	*mock.Call
}

// GetTargetManifest is a helper method to define mock.On call
func (_e *CollectionsManifestAgentIface_Expecter) GetTargetManifest() *CollectionsManifestAgentIface_GetTargetManifest_Call {
	return &CollectionsManifestAgentIface_GetTargetManifest_Call{Call: _e.mock.On("GetTargetManifest")}
}

func (_c *CollectionsManifestAgentIface_GetTargetManifest_Call) Run(run func()) *CollectionsManifestAgentIface_GetTargetManifest_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CollectionsManifestAgentIface_GetTargetManifest_Call) Return(_a0 *metadata.CollectionsManifest, _a1 error) *CollectionsManifestAgentIface_GetTargetManifest_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CollectionsManifestAgentIface_GetTargetManifest_Call) RunAndReturn(run func() (*metadata.CollectionsManifest, error)) *CollectionsManifestAgentIface_GetTargetManifest_Call {
	_c.Call.Return(run)
	return _c
}

// PersistNeededManifests provides a mock function with no fields
func (_m *CollectionsManifestAgentIface) PersistNeededManifests() (error, error, bool, bool) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for PersistNeededManifests")
	}

	var r0 error
	var r1 error
	var r2 bool
	var r3 bool
	if rf, ok := ret.Get(0).(func() (error, error, bool, bool)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	if rf, ok := ret.Get(2).(func() bool); ok {
		r2 = rf()
	} else {
		r2 = ret.Get(2).(bool)
	}

	if rf, ok := ret.Get(3).(func() bool); ok {
		r3 = rf()
	} else {
		r3 = ret.Get(3).(bool)
	}

	return r0, r1, r2, r3
}

// CollectionsManifestAgentIface_PersistNeededManifests_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'PersistNeededManifests'
type CollectionsManifestAgentIface_PersistNeededManifests_Call struct {
	*mock.Call
}

// PersistNeededManifests is a helper method to define mock.On call
func (_e *CollectionsManifestAgentIface_Expecter) PersistNeededManifests() *CollectionsManifestAgentIface_PersistNeededManifests_Call {
	return &CollectionsManifestAgentIface_PersistNeededManifests_Call{Call: _e.mock.On("PersistNeededManifests")}
}

func (_c *CollectionsManifestAgentIface_PersistNeededManifests_Call) Run(run func()) *CollectionsManifestAgentIface_PersistNeededManifests_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CollectionsManifestAgentIface_PersistNeededManifests_Call) Return(_a0 error, _a1 error, _a2 bool, _a3 bool) *CollectionsManifestAgentIface_PersistNeededManifests_Call {
	_c.Call.Return(_a0, _a1, _a2, _a3)
	return _c
}

func (_c *CollectionsManifestAgentIface_PersistNeededManifests_Call) RunAndReturn(run func() (error, error, bool, bool)) *CollectionsManifestAgentIface_PersistNeededManifests_Call {
	_c.Call.Return(run)
	return _c
}

// SetTempAgent provides a mock function with no fields
func (_m *CollectionsManifestAgentIface) SetTempAgent() {
	_m.Called()
}

// CollectionsManifestAgentIface_SetTempAgent_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetTempAgent'
type CollectionsManifestAgentIface_SetTempAgent_Call struct {
	*mock.Call
}

// SetTempAgent is a helper method to define mock.On call
func (_e *CollectionsManifestAgentIface_Expecter) SetTempAgent() *CollectionsManifestAgentIface_SetTempAgent_Call {
	return &CollectionsManifestAgentIface_SetTempAgent_Call{Call: _e.mock.On("SetTempAgent")}
}

func (_c *CollectionsManifestAgentIface_SetTempAgent_Call) Run(run func()) *CollectionsManifestAgentIface_SetTempAgent_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CollectionsManifestAgentIface_SetTempAgent_Call) Return() *CollectionsManifestAgentIface_SetTempAgent_Call {
	_c.Call.Return()
	return _c
}

func (_c *CollectionsManifestAgentIface_SetTempAgent_Call) RunAndReturn(run func()) *CollectionsManifestAgentIface_SetTempAgent_Call {
	_c.Run(run)
	return _c
}

// Start provides a mock function with no fields
func (_m *CollectionsManifestAgentIface) Start() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Start")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CollectionsManifestAgentIface_Start_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Start'
type CollectionsManifestAgentIface_Start_Call struct {
	*mock.Call
}

// Start is a helper method to define mock.On call
func (_e *CollectionsManifestAgentIface_Expecter) Start() *CollectionsManifestAgentIface_Start_Call {
	return &CollectionsManifestAgentIface_Start_Call{Call: _e.mock.On("Start")}
}

func (_c *CollectionsManifestAgentIface_Start_Call) Run(run func()) *CollectionsManifestAgentIface_Start_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CollectionsManifestAgentIface_Start_Call) Return(_a0 error) *CollectionsManifestAgentIface_Start_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CollectionsManifestAgentIface_Start_Call) RunAndReturn(run func() error) *CollectionsManifestAgentIface_Start_Call {
	_c.Call.Return(run)
	return _c
}

// Stop provides a mock function with no fields
func (_m *CollectionsManifestAgentIface) Stop() {
	_m.Called()
}

// CollectionsManifestAgentIface_Stop_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Stop'
type CollectionsManifestAgentIface_Stop_Call struct {
	*mock.Call
}

// Stop is a helper method to define mock.On call
func (_e *CollectionsManifestAgentIface_Expecter) Stop() *CollectionsManifestAgentIface_Stop_Call {
	return &CollectionsManifestAgentIface_Stop_Call{Call: _e.mock.On("Stop")}
}

func (_c *CollectionsManifestAgentIface_Stop_Call) Run(run func()) *CollectionsManifestAgentIface_Stop_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CollectionsManifestAgentIface_Stop_Call) Return() *CollectionsManifestAgentIface_Stop_Call {
	_c.Call.Return()
	return _c
}

func (_c *CollectionsManifestAgentIface_Stop_Call) RunAndReturn(run func()) *CollectionsManifestAgentIface_Stop_Call {
	_c.Run(run)
	return _c
}

// NewCollectionsManifestAgentIface creates a new instance of CollectionsManifestAgentIface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewCollectionsManifestAgentIface(t interface {
	mock.TestingT
	Cleanup(func())
}) *CollectionsManifestAgentIface {
	mock := &CollectionsManifestAgentIface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
