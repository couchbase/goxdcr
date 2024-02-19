// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	service_def "github.com/couchbase/goxdcr/service_def"
	mock "github.com/stretchr/testify/mock"

	x509 "crypto/x509"
)

// SecuritySvc is an autogenerated mock type for the SecuritySvc type
type SecuritySvc struct {
	mock.Mock
}

type SecuritySvc_Expecter struct {
	mock *mock.Mock
}

func (_m *SecuritySvc) EXPECT() *SecuritySvc_Expecter {
	return &SecuritySvc_Expecter{mock: &_m.Mock}
}

// EncryptData provides a mock function with given fields:
func (_m *SecuritySvc) EncryptData() bool {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for EncryptData")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// SecuritySvc_EncryptData_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'EncryptData'
type SecuritySvc_EncryptData_Call struct {
	*mock.Call
}

// EncryptData is a helper method to define mock.On call
func (_e *SecuritySvc_Expecter) EncryptData() *SecuritySvc_EncryptData_Call {
	return &SecuritySvc_EncryptData_Call{Call: _e.mock.On("EncryptData")}
}

func (_c *SecuritySvc_EncryptData_Call) Run(run func()) *SecuritySvc_EncryptData_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SecuritySvc_EncryptData_Call) Return(_a0 bool) *SecuritySvc_EncryptData_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *SecuritySvc_EncryptData_Call) RunAndReturn(run func() bool) *SecuritySvc_EncryptData_Call {
	_c.Call.Return(run)
	return _c
}

// GetCACertificates provides a mock function with given fields:
func (_m *SecuritySvc) GetCACertificates() []byte {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetCACertificates")
	}

	var r0 []byte
	if rf, ok := ret.Get(0).(func() []byte); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	return r0
}

// SecuritySvc_GetCACertificates_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetCACertificates'
type SecuritySvc_GetCACertificates_Call struct {
	*mock.Call
}

// GetCACertificates is a helper method to define mock.On call
func (_e *SecuritySvc_Expecter) GetCACertificates() *SecuritySvc_GetCACertificates_Call {
	return &SecuritySvc_GetCACertificates_Call{Call: _e.mock.On("GetCACertificates")}
}

func (_c *SecuritySvc_GetCACertificates_Call) Run(run func()) *SecuritySvc_GetCACertificates_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SecuritySvc_GetCACertificates_Call) Return(_a0 []byte) *SecuritySvc_GetCACertificates_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *SecuritySvc_GetCACertificates_Call) RunAndReturn(run func() []byte) *SecuritySvc_GetCACertificates_Call {
	_c.Call.Return(run)
	return _c
}

// GetCaPool provides a mock function with given fields:
func (_m *SecuritySvc) GetCaPool() *x509.CertPool {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetCaPool")
	}

	var r0 *x509.CertPool
	if rf, ok := ret.Get(0).(func() *x509.CertPool); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*x509.CertPool)
		}
	}

	return r0
}

// SecuritySvc_GetCaPool_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetCaPool'
type SecuritySvc_GetCaPool_Call struct {
	*mock.Call
}

// GetCaPool is a helper method to define mock.On call
func (_e *SecuritySvc_Expecter) GetCaPool() *SecuritySvc_GetCaPool_Call {
	return &SecuritySvc_GetCaPool_Call{Call: _e.mock.On("GetCaPool")}
}

func (_c *SecuritySvc_GetCaPool_Call) Run(run func()) *SecuritySvc_GetCaPool_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SecuritySvc_GetCaPool_Call) Return(_a0 *x509.CertPool) *SecuritySvc_GetCaPool_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *SecuritySvc_GetCaPool_Call) RunAndReturn(run func() *x509.CertPool) *SecuritySvc_GetCaPool_Call {
	_c.Call.Return(run)
	return _c
}

// IsClusterEncryptionLevelStrict provides a mock function with given fields:
func (_m *SecuritySvc) IsClusterEncryptionLevelStrict() bool {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for IsClusterEncryptionLevelStrict")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// SecuritySvc_IsClusterEncryptionLevelStrict_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'IsClusterEncryptionLevelStrict'
type SecuritySvc_IsClusterEncryptionLevelStrict_Call struct {
	*mock.Call
}

// IsClusterEncryptionLevelStrict is a helper method to define mock.On call
func (_e *SecuritySvc_Expecter) IsClusterEncryptionLevelStrict() *SecuritySvc_IsClusterEncryptionLevelStrict_Call {
	return &SecuritySvc_IsClusterEncryptionLevelStrict_Call{Call: _e.mock.On("IsClusterEncryptionLevelStrict")}
}

func (_c *SecuritySvc_IsClusterEncryptionLevelStrict_Call) Run(run func()) *SecuritySvc_IsClusterEncryptionLevelStrict_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SecuritySvc_IsClusterEncryptionLevelStrict_Call) Return(_a0 bool) *SecuritySvc_IsClusterEncryptionLevelStrict_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *SecuritySvc_IsClusterEncryptionLevelStrict_Call) RunAndReturn(run func() bool) *SecuritySvc_IsClusterEncryptionLevelStrict_Call {
	_c.Call.Return(run)
	return _c
}

// SetEncryptionLevelChangeCallback provides a mock function with given fields: key, callback
func (_m *SecuritySvc) SetEncryptionLevelChangeCallback(key string, callback service_def.SecChangeCallback) {
	_m.Called(key, callback)
}

// SecuritySvc_SetEncryptionLevelChangeCallback_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetEncryptionLevelChangeCallback'
type SecuritySvc_SetEncryptionLevelChangeCallback_Call struct {
	*mock.Call
}

// SetEncryptionLevelChangeCallback is a helper method to define mock.On call
//   - key string
//   - callback service_def.SecChangeCallback
func (_e *SecuritySvc_Expecter) SetEncryptionLevelChangeCallback(key interface{}, callback interface{}) *SecuritySvc_SetEncryptionLevelChangeCallback_Call {
	return &SecuritySvc_SetEncryptionLevelChangeCallback_Call{Call: _e.mock.On("SetEncryptionLevelChangeCallback", key, callback)}
}

func (_c *SecuritySvc_SetEncryptionLevelChangeCallback_Call) Run(run func(key string, callback service_def.SecChangeCallback)) *SecuritySvc_SetEncryptionLevelChangeCallback_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(service_def.SecChangeCallback))
	})
	return _c
}

func (_c *SecuritySvc_SetEncryptionLevelChangeCallback_Call) Return() *SecuritySvc_SetEncryptionLevelChangeCallback_Call {
	_c.Call.Return()
	return _c
}

func (_c *SecuritySvc_SetEncryptionLevelChangeCallback_Call) RunAndReturn(run func(string, service_def.SecChangeCallback)) *SecuritySvc_SetEncryptionLevelChangeCallback_Call {
	_c.Call.Return(run)
	return _c
}

// Start provides a mock function with given fields:
func (_m *SecuritySvc) Start() {
	_m.Called()
}

// SecuritySvc_Start_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Start'
type SecuritySvc_Start_Call struct {
	*mock.Call
}

// Start is a helper method to define mock.On call
func (_e *SecuritySvc_Expecter) Start() *SecuritySvc_Start_Call {
	return &SecuritySvc_Start_Call{Call: _e.mock.On("Start")}
}

func (_c *SecuritySvc_Start_Call) Run(run func()) *SecuritySvc_Start_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SecuritySvc_Start_Call) Return() *SecuritySvc_Start_Call {
	_c.Call.Return()
	return _c
}

func (_c *SecuritySvc_Start_Call) RunAndReturn(run func()) *SecuritySvc_Start_Call {
	_c.Call.Return(run)
	return _c
}

// NewSecuritySvc creates a new instance of SecuritySvc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewSecuritySvc(t interface {
	mock.TestingT
	Cleanup(func())
}) *SecuritySvc {
	mock := &SecuritySvc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
