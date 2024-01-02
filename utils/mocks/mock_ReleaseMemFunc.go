// Code generated by mockery. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// ReleaseMemFunc is an autogenerated mock type for the ReleaseMemFunc type
type ReleaseMemFunc struct {
	mock.Mock
}

type ReleaseMemFunc_Expecter struct {
	mock *mock.Mock
}

func (_m *ReleaseMemFunc) EXPECT() *ReleaseMemFunc_Expecter {
	return &ReleaseMemFunc_Expecter{mock: &_m.Mock}
}

// Execute provides a mock function with given fields:
func (_m *ReleaseMemFunc) Execute() {
	_m.Called()
}

// ReleaseMemFunc_Execute_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Execute'
type ReleaseMemFunc_Execute_Call struct {
	*mock.Call
}

// Execute is a helper method to define mock.On call
func (_e *ReleaseMemFunc_Expecter) Execute() *ReleaseMemFunc_Execute_Call {
	return &ReleaseMemFunc_Execute_Call{Call: _e.mock.On("Execute")}
}

func (_c *ReleaseMemFunc_Execute_Call) Run(run func()) *ReleaseMemFunc_Execute_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ReleaseMemFunc_Execute_Call) Return() *ReleaseMemFunc_Execute_Call {
	_c.Call.Return()
	return _c
}

func (_c *ReleaseMemFunc_Execute_Call) RunAndReturn(run func()) *ReleaseMemFunc_Execute_Call {
	_c.Call.Return(run)
	return _c
}

// NewReleaseMemFunc creates a new instance of ReleaseMemFunc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewReleaseMemFunc(t interface {
	mock.TestingT
	Cleanup(func())
}) *ReleaseMemFunc {
	mock := &ReleaseMemFunc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}