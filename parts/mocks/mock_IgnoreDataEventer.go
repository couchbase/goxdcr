// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	mock "github.com/stretchr/testify/mock"
)

// IgnoreDataEventer is an autogenerated mock type for the IgnoreDataEventer type
type IgnoreDataEventer struct {
	mock.Mock
}

type IgnoreDataEventer_Expecter struct {
	mock *mock.Mock
}

func (_m *IgnoreDataEventer) EXPECT() *IgnoreDataEventer_Expecter {
	return &IgnoreDataEventer_Expecter{mock: &_m.Mock}
}

// Execute provides a mock function with given fields: _a0
func (_m *IgnoreDataEventer) Execute(_a0 *base.WrappedMCRequest) {
	_m.Called(_a0)
}

// IgnoreDataEventer_Execute_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Execute'
type IgnoreDataEventer_Execute_Call struct {
	*mock.Call
}

// Execute is a helper method to define mock.On call
//   - _a0 *base.WrappedMCRequest
func (_e *IgnoreDataEventer_Expecter) Execute(_a0 interface{}) *IgnoreDataEventer_Execute_Call {
	return &IgnoreDataEventer_Execute_Call{Call: _e.mock.On("Execute", _a0)}
}

func (_c *IgnoreDataEventer_Execute_Call) Run(run func(_a0 *base.WrappedMCRequest)) *IgnoreDataEventer_Execute_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*base.WrappedMCRequest))
	})
	return _c
}

func (_c *IgnoreDataEventer_Execute_Call) Return() *IgnoreDataEventer_Execute_Call {
	_c.Call.Return()
	return _c
}

func (_c *IgnoreDataEventer_Execute_Call) RunAndReturn(run func(*base.WrappedMCRequest)) *IgnoreDataEventer_Execute_Call {
	_c.Call.Return(run)
	return _c
}

// NewIgnoreDataEventer creates a new instance of IgnoreDataEventer. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewIgnoreDataEventer(t interface {
	mock.TestingT
	Cleanup(func())
}) *IgnoreDataEventer {
	mock := &IgnoreDataEventer{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
