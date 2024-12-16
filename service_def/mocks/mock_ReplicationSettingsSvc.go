// Code generated by mockery v2.50.0. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/metadata"
	mock "github.com/stretchr/testify/mock"
)

// ReplicationSettingsSvc is an autogenerated mock type for the ReplicationSettingsSvc type
type ReplicationSettingsSvc struct {
	mock.Mock
}

type ReplicationSettingsSvc_Expecter struct {
	mock *mock.Mock
}

func (_m *ReplicationSettingsSvc) EXPECT() *ReplicationSettingsSvc_Expecter {
	return &ReplicationSettingsSvc_Expecter{mock: &_m.Mock}
}

// GetDefaultReplicationSettings provides a mock function with no fields
func (_m *ReplicationSettingsSvc) GetDefaultReplicationSettings() (*metadata.ReplicationSettings, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetDefaultReplicationSettings")
	}

	var r0 *metadata.ReplicationSettings
	var r1 error
	if rf, ok := ret.Get(0).(func() (*metadata.ReplicationSettings, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() *metadata.ReplicationSettings); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReplicationSettings)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReplicationSettingsSvc_GetDefaultReplicationSettings_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetDefaultReplicationSettings'
type ReplicationSettingsSvc_GetDefaultReplicationSettings_Call struct {
	*mock.Call
}

// GetDefaultReplicationSettings is a helper method to define mock.On call
func (_e *ReplicationSettingsSvc_Expecter) GetDefaultReplicationSettings() *ReplicationSettingsSvc_GetDefaultReplicationSettings_Call {
	return &ReplicationSettingsSvc_GetDefaultReplicationSettings_Call{Call: _e.mock.On("GetDefaultReplicationSettings")}
}

func (_c *ReplicationSettingsSvc_GetDefaultReplicationSettings_Call) Run(run func()) *ReplicationSettingsSvc_GetDefaultReplicationSettings_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ReplicationSettingsSvc_GetDefaultReplicationSettings_Call) Return(_a0 *metadata.ReplicationSettings, _a1 error) *ReplicationSettingsSvc_GetDefaultReplicationSettings_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *ReplicationSettingsSvc_GetDefaultReplicationSettings_Call) RunAndReturn(run func() (*metadata.ReplicationSettings, error)) *ReplicationSettingsSvc_GetDefaultReplicationSettings_Call {
	_c.Call.Return(run)
	return _c
}

// UpdateDefaultReplicationSettings provides a mock function with given fields: _a0
func (_m *ReplicationSettingsSvc) UpdateDefaultReplicationSettings(_a0 map[string]interface{}) (metadata.ReplicationSettingsMap, map[string]error, error) {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for UpdateDefaultReplicationSettings")
	}

	var r0 metadata.ReplicationSettingsMap
	var r1 map[string]error
	var r2 error
	if rf, ok := ret.Get(0).(func(map[string]interface{}) (metadata.ReplicationSettingsMap, map[string]error, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(map[string]interface{}) metadata.ReplicationSettingsMap); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(metadata.ReplicationSettingsMap)
		}
	}

	if rf, ok := ret.Get(1).(func(map[string]interface{}) map[string]error); ok {
		r1 = rf(_a0)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(map[string]error)
		}
	}

	if rf, ok := ret.Get(2).(func(map[string]interface{}) error); ok {
		r2 = rf(_a0)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// ReplicationSettingsSvc_UpdateDefaultReplicationSettings_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UpdateDefaultReplicationSettings'
type ReplicationSettingsSvc_UpdateDefaultReplicationSettings_Call struct {
	*mock.Call
}

// UpdateDefaultReplicationSettings is a helper method to define mock.On call
//   - _a0 map[string]interface{}
func (_e *ReplicationSettingsSvc_Expecter) UpdateDefaultReplicationSettings(_a0 interface{}) *ReplicationSettingsSvc_UpdateDefaultReplicationSettings_Call {
	return &ReplicationSettingsSvc_UpdateDefaultReplicationSettings_Call{Call: _e.mock.On("UpdateDefaultReplicationSettings", _a0)}
}

func (_c *ReplicationSettingsSvc_UpdateDefaultReplicationSettings_Call) Run(run func(_a0 map[string]interface{})) *ReplicationSettingsSvc_UpdateDefaultReplicationSettings_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(map[string]interface{}))
	})
	return _c
}

func (_c *ReplicationSettingsSvc_UpdateDefaultReplicationSettings_Call) Return(_a0 metadata.ReplicationSettingsMap, _a1 map[string]error, _a2 error) *ReplicationSettingsSvc_UpdateDefaultReplicationSettings_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *ReplicationSettingsSvc_UpdateDefaultReplicationSettings_Call) RunAndReturn(run func(map[string]interface{}) (metadata.ReplicationSettingsMap, map[string]error, error)) *ReplicationSettingsSvc_UpdateDefaultReplicationSettings_Call {
	_c.Call.Return(run)
	return _c
}

// NewReplicationSettingsSvc creates a new instance of ReplicationSettingsSvc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewReplicationSettingsSvc(t interface {
	mock.TestingT
	Cleanup(func())
}) *ReplicationSettingsSvc {
	mock := &ReplicationSettingsSvc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
