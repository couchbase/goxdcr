// Code generated by mockery v2.50.0. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/metadata"
	mock "github.com/stretchr/testify/mock"
)

// ConfigMapRetriever is an autogenerated mock type for the ConfigMapRetriever type
type ConfigMapRetriever struct {
	mock.Mock
}

type ConfigMapRetriever_Expecter struct {
	mock *mock.Mock
}

func (_m *ConfigMapRetriever) EXPECT() *ConfigMapRetriever_Expecter {
	return &ConfigMapRetriever_Expecter{mock: &_m.Mock}
}

// Execute provides a mock function with no fields
func (_m *ConfigMapRetriever) Execute() map[string]*metadata.SettingsConfig {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Execute")
	}

	var r0 map[string]*metadata.SettingsConfig
	if rf, ok := ret.Get(0).(func() map[string]*metadata.SettingsConfig); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]*metadata.SettingsConfig)
		}
	}

	return r0
}

// ConfigMapRetriever_Execute_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Execute'
type ConfigMapRetriever_Execute_Call struct {
	*mock.Call
}

// Execute is a helper method to define mock.On call
func (_e *ConfigMapRetriever_Expecter) Execute() *ConfigMapRetriever_Execute_Call {
	return &ConfigMapRetriever_Execute_Call{Call: _e.mock.On("Execute")}
}

func (_c *ConfigMapRetriever_Execute_Call) Run(run func()) *ConfigMapRetriever_Execute_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ConfigMapRetriever_Execute_Call) Return(_a0 map[string]*metadata.SettingsConfig) *ConfigMapRetriever_Execute_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ConfigMapRetriever_Execute_Call) RunAndReturn(run func() map[string]*metadata.SettingsConfig) *ConfigMapRetriever_Execute_Call {
	_c.Call.Return(run)
	return _c
}

// NewConfigMapRetriever creates a new instance of ConfigMapRetriever. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewConfigMapRetriever(t interface {
	mock.TestingT
	Cleanup(func())
}) *ConfigMapRetriever {
	mock := &ConfigMapRetriever{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
