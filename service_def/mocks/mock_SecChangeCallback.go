// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	service_def "github.com/couchbase/goxdcr/v8/service_def"
	mock "github.com/stretchr/testify/mock"
)

// SecChangeCallback is an autogenerated mock type for the SecChangeCallback type
type SecChangeCallback struct {
	mock.Mock
}

type SecChangeCallback_Expecter struct {
	mock *mock.Mock
}

func (_m *SecChangeCallback) EXPECT() *SecChangeCallback_Expecter {
	return &SecChangeCallback_Expecter{mock: &_m.Mock}
}

// Execute provides a mock function with given fields: old, new
func (_m *SecChangeCallback) Execute(old service_def.EncryptionSettingIface, new service_def.EncryptionSettingIface) {
	_m.Called(old, new)
}

// SecChangeCallback_Execute_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Execute'
type SecChangeCallback_Execute_Call struct {
	*mock.Call
}

// Execute is a helper method to define mock.On call
//   - old service_def.EncryptionSettingIface
//   - new service_def.EncryptionSettingIface
func (_e *SecChangeCallback_Expecter) Execute(old interface{}, new interface{}) *SecChangeCallback_Execute_Call {
	return &SecChangeCallback_Execute_Call{Call: _e.mock.On("Execute", old, new)}
}

func (_c *SecChangeCallback_Execute_Call) Run(run func(old service_def.EncryptionSettingIface, new service_def.EncryptionSettingIface)) *SecChangeCallback_Execute_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(service_def.EncryptionSettingIface), args[1].(service_def.EncryptionSettingIface))
	})
	return _c
}

func (_c *SecChangeCallback_Execute_Call) Return() *SecChangeCallback_Execute_Call {
	_c.Call.Return()
	return _c
}

func (_c *SecChangeCallback_Execute_Call) RunAndReturn(run func(service_def.EncryptionSettingIface, service_def.EncryptionSettingIface)) *SecChangeCallback_Execute_Call {
	_c.Run(run)
	return _c
}

// NewSecChangeCallback creates a new instance of SecChangeCallback. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewSecChangeCallback(t interface {
	mock.TestingT
	Cleanup(func())
}) *SecChangeCallback {
	mock := &SecChangeCallback{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
