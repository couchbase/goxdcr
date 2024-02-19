// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"
)

// GlobalSettingsSvc is an autogenerated mock type for the GlobalSettingsSvc type
type GlobalSettingsSvc struct {
	mock.Mock
}

type GlobalSettingsSvc_Expecter struct {
	mock *mock.Mock
}

func (_m *GlobalSettingsSvc) EXPECT() *GlobalSettingsSvc_Expecter {
	return &GlobalSettingsSvc_Expecter{mock: &_m.Mock}
}

// GetGlobalSettings provides a mock function with given fields:
func (_m *GlobalSettingsSvc) GetGlobalSettings() (*metadata.GlobalSettings, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetGlobalSettings")
	}

	var r0 *metadata.GlobalSettings
	var r1 error
	if rf, ok := ret.Get(0).(func() (*metadata.GlobalSettings, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() *metadata.GlobalSettings); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.GlobalSettings)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GlobalSettingsSvc_GetGlobalSettings_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetGlobalSettings'
type GlobalSettingsSvc_GetGlobalSettings_Call struct {
	*mock.Call
}

// GetGlobalSettings is a helper method to define mock.On call
func (_e *GlobalSettingsSvc_Expecter) GetGlobalSettings() *GlobalSettingsSvc_GetGlobalSettings_Call {
	return &GlobalSettingsSvc_GetGlobalSettings_Call{Call: _e.mock.On("GetGlobalSettings")}
}

func (_c *GlobalSettingsSvc_GetGlobalSettings_Call) Run(run func()) *GlobalSettingsSvc_GetGlobalSettings_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *GlobalSettingsSvc_GetGlobalSettings_Call) Return(_a0 *metadata.GlobalSettings, _a1 error) *GlobalSettingsSvc_GetGlobalSettings_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *GlobalSettingsSvc_GetGlobalSettings_Call) RunAndReturn(run func() (*metadata.GlobalSettings, error)) *GlobalSettingsSvc_GetGlobalSettings_Call {
	_c.Call.Return(run)
	return _c
}

// GlobalSettingsServiceCallback provides a mock function with given fields: path, value, rev
func (_m *GlobalSettingsSvc) GlobalSettingsServiceCallback(path string, value []byte, rev interface{}) error {
	ret := _m.Called(path, value, rev)

	if len(ret) == 0 {
		panic("no return value specified for GlobalSettingsServiceCallback")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []byte, interface{}) error); ok {
		r0 = rf(path, value, rev)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GlobalSettingsSvc_GlobalSettingsServiceCallback_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GlobalSettingsServiceCallback'
type GlobalSettingsSvc_GlobalSettingsServiceCallback_Call struct {
	*mock.Call
}

// GlobalSettingsServiceCallback is a helper method to define mock.On call
//   - path string
//   - value []byte
//   - rev interface{}
func (_e *GlobalSettingsSvc_Expecter) GlobalSettingsServiceCallback(path interface{}, value interface{}, rev interface{}) *GlobalSettingsSvc_GlobalSettingsServiceCallback_Call {
	return &GlobalSettingsSvc_GlobalSettingsServiceCallback_Call{Call: _e.mock.On("GlobalSettingsServiceCallback", path, value, rev)}
}

func (_c *GlobalSettingsSvc_GlobalSettingsServiceCallback_Call) Run(run func(path string, value []byte, rev interface{})) *GlobalSettingsSvc_GlobalSettingsServiceCallback_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].([]byte), args[2].(interface{}))
	})
	return _c
}

func (_c *GlobalSettingsSvc_GlobalSettingsServiceCallback_Call) Return(_a0 error) *GlobalSettingsSvc_GlobalSettingsServiceCallback_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *GlobalSettingsSvc_GlobalSettingsServiceCallback_Call) RunAndReturn(run func(string, []byte, interface{}) error) *GlobalSettingsSvc_GlobalSettingsServiceCallback_Call {
	_c.Call.Return(run)
	return _c
}

// SetMetadataChangeHandlerCallback provides a mock function with given fields: callBack
func (_m *GlobalSettingsSvc) SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback) {
	_m.Called(callBack)
}

// GlobalSettingsSvc_SetMetadataChangeHandlerCallback_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetMetadataChangeHandlerCallback'
type GlobalSettingsSvc_SetMetadataChangeHandlerCallback_Call struct {
	*mock.Call
}

// SetMetadataChangeHandlerCallback is a helper method to define mock.On call
//   - callBack base.MetadataChangeHandlerCallback
func (_e *GlobalSettingsSvc_Expecter) SetMetadataChangeHandlerCallback(callBack interface{}) *GlobalSettingsSvc_SetMetadataChangeHandlerCallback_Call {
	return &GlobalSettingsSvc_SetMetadataChangeHandlerCallback_Call{Call: _e.mock.On("SetMetadataChangeHandlerCallback", callBack)}
}

func (_c *GlobalSettingsSvc_SetMetadataChangeHandlerCallback_Call) Run(run func(callBack base.MetadataChangeHandlerCallback)) *GlobalSettingsSvc_SetMetadataChangeHandlerCallback_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(base.MetadataChangeHandlerCallback))
	})
	return _c
}

func (_c *GlobalSettingsSvc_SetMetadataChangeHandlerCallback_Call) Return() *GlobalSettingsSvc_SetMetadataChangeHandlerCallback_Call {
	_c.Call.Return()
	return _c
}

func (_c *GlobalSettingsSvc_SetMetadataChangeHandlerCallback_Call) RunAndReturn(run func(base.MetadataChangeHandlerCallback)) *GlobalSettingsSvc_SetMetadataChangeHandlerCallback_Call {
	_c.Call.Return(run)
	return _c
}

// UpdateGlobalSettings provides a mock function with given fields: _a0
func (_m *GlobalSettingsSvc) UpdateGlobalSettings(_a0 metadata.ReplicationSettingsMap) (map[string]error, error) {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for UpdateGlobalSettings")
	}

	var r0 map[string]error
	var r1 error
	if rf, ok := ret.Get(0).(func(metadata.ReplicationSettingsMap) (map[string]error, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(metadata.ReplicationSettingsMap) map[string]error); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]error)
		}
	}

	if rf, ok := ret.Get(1).(func(metadata.ReplicationSettingsMap) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GlobalSettingsSvc_UpdateGlobalSettings_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UpdateGlobalSettings'
type GlobalSettingsSvc_UpdateGlobalSettings_Call struct {
	*mock.Call
}

// UpdateGlobalSettings is a helper method to define mock.On call
//   - _a0 metadata.ReplicationSettingsMap
func (_e *GlobalSettingsSvc_Expecter) UpdateGlobalSettings(_a0 interface{}) *GlobalSettingsSvc_UpdateGlobalSettings_Call {
	return &GlobalSettingsSvc_UpdateGlobalSettings_Call{Call: _e.mock.On("UpdateGlobalSettings", _a0)}
}

func (_c *GlobalSettingsSvc_UpdateGlobalSettings_Call) Run(run func(_a0 metadata.ReplicationSettingsMap)) *GlobalSettingsSvc_UpdateGlobalSettings_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(metadata.ReplicationSettingsMap))
	})
	return _c
}

func (_c *GlobalSettingsSvc_UpdateGlobalSettings_Call) Return(_a0 map[string]error, _a1 error) *GlobalSettingsSvc_UpdateGlobalSettings_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *GlobalSettingsSvc_UpdateGlobalSettings_Call) RunAndReturn(run func(metadata.ReplicationSettingsMap) (map[string]error, error)) *GlobalSettingsSvc_UpdateGlobalSettings_Call {
	_c.Call.Return(run)
	return _c
}

// NewGlobalSettingsSvc creates a new instance of GlobalSettingsSvc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewGlobalSettingsSvc(t interface {
	mock.TestingT
	Cleanup(func())
}) *GlobalSettingsSvc {
	mock := &GlobalSettingsSvc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
