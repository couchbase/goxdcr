// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/v8/base"
	metadata "github.com/couchbase/goxdcr/v8/metadata"

	mock "github.com/stretchr/testify/mock"
)

// InternalSettingsSvc is an autogenerated mock type for the InternalSettingsSvc type
type InternalSettingsSvc struct {
	mock.Mock
}

type InternalSettingsSvc_Expecter struct {
	mock *mock.Mock
}

func (_m *InternalSettingsSvc) EXPECT() *InternalSettingsSvc_Expecter {
	return &InternalSettingsSvc_Expecter{mock: &_m.Mock}
}

// GetInternalSettings provides a mock function with no fields
func (_m *InternalSettingsSvc) GetInternalSettings() *metadata.InternalSettings {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetInternalSettings")
	}

	var r0 *metadata.InternalSettings
	if rf, ok := ret.Get(0).(func() *metadata.InternalSettings); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.InternalSettings)
		}
	}

	return r0
}

// InternalSettingsSvc_GetInternalSettings_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetInternalSettings'
type InternalSettingsSvc_GetInternalSettings_Call struct {
	*mock.Call
}

// GetInternalSettings is a helper method to define mock.On call
func (_e *InternalSettingsSvc_Expecter) GetInternalSettings() *InternalSettingsSvc_GetInternalSettings_Call {
	return &InternalSettingsSvc_GetInternalSettings_Call{Call: _e.mock.On("GetInternalSettings")}
}

func (_c *InternalSettingsSvc_GetInternalSettings_Call) Run(run func()) *InternalSettingsSvc_GetInternalSettings_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *InternalSettingsSvc_GetInternalSettings_Call) Return(_a0 *metadata.InternalSettings) *InternalSettingsSvc_GetInternalSettings_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *InternalSettingsSvc_GetInternalSettings_Call) RunAndReturn(run func() *metadata.InternalSettings) *InternalSettingsSvc_GetInternalSettings_Call {
	_c.Call.Return(run)
	return _c
}

// InternalSettingsServiceCallback provides a mock function with given fields: path, value, rev
func (_m *InternalSettingsSvc) InternalSettingsServiceCallback(path string, value []byte, rev interface{}) error {
	ret := _m.Called(path, value, rev)

	if len(ret) == 0 {
		panic("no return value specified for InternalSettingsServiceCallback")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []byte, interface{}) error); ok {
		r0 = rf(path, value, rev)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// InternalSettingsSvc_InternalSettingsServiceCallback_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'InternalSettingsServiceCallback'
type InternalSettingsSvc_InternalSettingsServiceCallback_Call struct {
	*mock.Call
}

// InternalSettingsServiceCallback is a helper method to define mock.On call
//   - path string
//   - value []byte
//   - rev interface{}
func (_e *InternalSettingsSvc_Expecter) InternalSettingsServiceCallback(path interface{}, value interface{}, rev interface{}) *InternalSettingsSvc_InternalSettingsServiceCallback_Call {
	return &InternalSettingsSvc_InternalSettingsServiceCallback_Call{Call: _e.mock.On("InternalSettingsServiceCallback", path, value, rev)}
}

func (_c *InternalSettingsSvc_InternalSettingsServiceCallback_Call) Run(run func(path string, value []byte, rev interface{})) *InternalSettingsSvc_InternalSettingsServiceCallback_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].([]byte), args[2].(interface{}))
	})
	return _c
}

func (_c *InternalSettingsSvc_InternalSettingsServiceCallback_Call) Return(_a0 error) *InternalSettingsSvc_InternalSettingsServiceCallback_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *InternalSettingsSvc_InternalSettingsServiceCallback_Call) RunAndReturn(run func(string, []byte, interface{}) error) *InternalSettingsSvc_InternalSettingsServiceCallback_Call {
	_c.Call.Return(run)
	return _c
}

// SetMetadataChangeHandlerCallback provides a mock function with given fields: callBack
func (_m *InternalSettingsSvc) SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback) {
	_m.Called(callBack)
}

// InternalSettingsSvc_SetMetadataChangeHandlerCallback_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetMetadataChangeHandlerCallback'
type InternalSettingsSvc_SetMetadataChangeHandlerCallback_Call struct {
	*mock.Call
}

// SetMetadataChangeHandlerCallback is a helper method to define mock.On call
//   - callBack base.MetadataChangeHandlerCallback
func (_e *InternalSettingsSvc_Expecter) SetMetadataChangeHandlerCallback(callBack interface{}) *InternalSettingsSvc_SetMetadataChangeHandlerCallback_Call {
	return &InternalSettingsSvc_SetMetadataChangeHandlerCallback_Call{Call: _e.mock.On("SetMetadataChangeHandlerCallback", callBack)}
}

func (_c *InternalSettingsSvc_SetMetadataChangeHandlerCallback_Call) Run(run func(callBack base.MetadataChangeHandlerCallback)) *InternalSettingsSvc_SetMetadataChangeHandlerCallback_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(base.MetadataChangeHandlerCallback))
	})
	return _c
}

func (_c *InternalSettingsSvc_SetMetadataChangeHandlerCallback_Call) Return() *InternalSettingsSvc_SetMetadataChangeHandlerCallback_Call {
	_c.Call.Return()
	return _c
}

func (_c *InternalSettingsSvc_SetMetadataChangeHandlerCallback_Call) RunAndReturn(run func(base.MetadataChangeHandlerCallback)) *InternalSettingsSvc_SetMetadataChangeHandlerCallback_Call {
	_c.Run(run)
	return _c
}

// UpdateInternalSettings provides a mock function with given fields: settingsMap
func (_m *InternalSettingsSvc) UpdateInternalSettings(settingsMap map[string]interface{}) (*metadata.InternalSettings, map[string]error, error) {
	ret := _m.Called(settingsMap)

	if len(ret) == 0 {
		panic("no return value specified for UpdateInternalSettings")
	}

	var r0 *metadata.InternalSettings
	var r1 map[string]error
	var r2 error
	if rf, ok := ret.Get(0).(func(map[string]interface{}) (*metadata.InternalSettings, map[string]error, error)); ok {
		return rf(settingsMap)
	}
	if rf, ok := ret.Get(0).(func(map[string]interface{}) *metadata.InternalSettings); ok {
		r0 = rf(settingsMap)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.InternalSettings)
		}
	}

	if rf, ok := ret.Get(1).(func(map[string]interface{}) map[string]error); ok {
		r1 = rf(settingsMap)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(map[string]error)
		}
	}

	if rf, ok := ret.Get(2).(func(map[string]interface{}) error); ok {
		r2 = rf(settingsMap)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// InternalSettingsSvc_UpdateInternalSettings_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UpdateInternalSettings'
type InternalSettingsSvc_UpdateInternalSettings_Call struct {
	*mock.Call
}

// UpdateInternalSettings is a helper method to define mock.On call
//   - settingsMap map[string]interface{}
func (_e *InternalSettingsSvc_Expecter) UpdateInternalSettings(settingsMap interface{}) *InternalSettingsSvc_UpdateInternalSettings_Call {
	return &InternalSettingsSvc_UpdateInternalSettings_Call{Call: _e.mock.On("UpdateInternalSettings", settingsMap)}
}

func (_c *InternalSettingsSvc_UpdateInternalSettings_Call) Run(run func(settingsMap map[string]interface{})) *InternalSettingsSvc_UpdateInternalSettings_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(map[string]interface{}))
	})
	return _c
}

func (_c *InternalSettingsSvc_UpdateInternalSettings_Call) Return(_a0 *metadata.InternalSettings, _a1 map[string]error, _a2 error) *InternalSettingsSvc_UpdateInternalSettings_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *InternalSettingsSvc_UpdateInternalSettings_Call) RunAndReturn(run func(map[string]interface{}) (*metadata.InternalSettings, map[string]error, error)) *InternalSettingsSvc_UpdateInternalSettings_Call {
	_c.Call.Return(run)
	return _c
}

// NewInternalSettingsSvc creates a new instance of InternalSettingsSvc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewInternalSettingsSvc(t interface {
	mock.TestingT
	Cleanup(func())
}) *InternalSettingsSvc {
	mock := &InternalSettingsSvc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
