// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	service_def "github.com/couchbase/goxdcr/service_def"
	mock "github.com/stretchr/testify/mock"
)

// AuditEventIface is an autogenerated mock type for the AuditEventIface type
type AuditEventIface struct {
	mock.Mock
}

type AuditEventIface_Expecter struct {
	mock *mock.Mock
}

func (_m *AuditEventIface) EXPECT() *AuditEventIface_Expecter {
	return &AuditEventIface_Expecter{mock: &_m.Mock}
}

// Clone provides a mock function with given fields:
func (_m *AuditEventIface) Clone() service_def.AuditEventIface {
	ret := _m.Called()

	var r0 service_def.AuditEventIface
	if rf, ok := ret.Get(0).(func() service_def.AuditEventIface); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(service_def.AuditEventIface)
		}
	}

	return r0
}

// AuditEventIface_Clone_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Clone'
type AuditEventIface_Clone_Call struct {
	*mock.Call
}

// Clone is a helper method to define mock.On call
func (_e *AuditEventIface_Expecter) Clone() *AuditEventIface_Clone_Call {
	return &AuditEventIface_Clone_Call{Call: _e.mock.On("Clone")}
}

func (_c *AuditEventIface_Clone_Call) Run(run func()) *AuditEventIface_Clone_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *AuditEventIface_Clone_Call) Return(_a0 service_def.AuditEventIface) *AuditEventIface_Clone_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *AuditEventIface_Clone_Call) RunAndReturn(run func() service_def.AuditEventIface) *AuditEventIface_Clone_Call {
	_c.Call.Return(run)
	return _c
}

// Redact provides a mock function with given fields:
func (_m *AuditEventIface) Redact() service_def.AuditEventIface {
	ret := _m.Called()

	var r0 service_def.AuditEventIface
	if rf, ok := ret.Get(0).(func() service_def.AuditEventIface); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(service_def.AuditEventIface)
		}
	}

	return r0
}

// AuditEventIface_Redact_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Redact'
type AuditEventIface_Redact_Call struct {
	*mock.Call
}

// Redact is a helper method to define mock.On call
func (_e *AuditEventIface_Expecter) Redact() *AuditEventIface_Redact_Call {
	return &AuditEventIface_Redact_Call{Call: _e.mock.On("Redact")}
}

func (_c *AuditEventIface_Redact_Call) Run(run func()) *AuditEventIface_Redact_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *AuditEventIface_Redact_Call) Return(_a0 service_def.AuditEventIface) *AuditEventIface_Redact_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *AuditEventIface_Redact_Call) RunAndReturn(run func() service_def.AuditEventIface) *AuditEventIface_Redact_Call {
	_c.Call.Return(run)
	return _c
}

// NewAuditEventIface creates a new instance of AuditEventIface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewAuditEventIface(t interface {
	mock.TestingT
	Cleanup(func())
}) *AuditEventIface {
	mock := &AuditEventIface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
