// Code generated by mockery v2.50.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// ErrReportFunc is an autogenerated mock type for the ErrReportFunc type
type ErrReportFunc struct {
	mock.Mock
}

type ErrReportFunc_Expecter struct {
	mock *mock.Mock
}

func (_m *ErrReportFunc) EXPECT() *ErrReportFunc_Expecter {
	return &ErrReportFunc_Expecter{mock: &_m.Mock}
}

// Execute provides a mock function with given fields: obj
func (_m *ErrReportFunc) Execute(obj interface{}) {
	_m.Called(obj)
}

// ErrReportFunc_Execute_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Execute'
type ErrReportFunc_Execute_Call struct {
	*mock.Call
}

// Execute is a helper method to define mock.On call
//   - obj interface{}
func (_e *ErrReportFunc_Expecter) Execute(obj interface{}) *ErrReportFunc_Execute_Call {
	return &ErrReportFunc_Execute_Call{Call: _e.mock.On("Execute", obj)}
}

func (_c *ErrReportFunc_Execute_Call) Run(run func(obj interface{})) *ErrReportFunc_Execute_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(interface{}))
	})
	return _c
}

func (_c *ErrReportFunc_Execute_Call) Return() *ErrReportFunc_Execute_Call {
	_c.Call.Return()
	return _c
}

func (_c *ErrReportFunc_Execute_Call) RunAndReturn(run func(interface{})) *ErrReportFunc_Execute_Call {
	_c.Run(run)
	return _c
}

// NewErrReportFunc creates a new instance of ErrReportFunc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewErrReportFunc(t interface {
	mock.TestingT
	Cleanup(func())
}) *ErrReportFunc {
	mock := &ErrReportFunc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
