// Code generated by mockery. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// ExponentialOpFunc is an autogenerated mock type for the ExponentialOpFunc type
type ExponentialOpFunc struct {
	mock.Mock
}

type ExponentialOpFunc_Expecter struct {
	mock *mock.Mock
}

func (_m *ExponentialOpFunc) EXPECT() *ExponentialOpFunc_Expecter {
	return &ExponentialOpFunc_Expecter{mock: &_m.Mock}
}

// Execute provides a mock function with no fields
func (_m *ExponentialOpFunc) Execute() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Execute")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ExponentialOpFunc_Execute_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Execute'
type ExponentialOpFunc_Execute_Call struct {
	*mock.Call
}

// Execute is a helper method to define mock.On call
func (_e *ExponentialOpFunc_Expecter) Execute() *ExponentialOpFunc_Execute_Call {
	return &ExponentialOpFunc_Execute_Call{Call: _e.mock.On("Execute")}
}

func (_c *ExponentialOpFunc_Execute_Call) Run(run func()) *ExponentialOpFunc_Execute_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ExponentialOpFunc_Execute_Call) Return(_a0 error) *ExponentialOpFunc_Execute_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ExponentialOpFunc_Execute_Call) RunAndReturn(run func() error) *ExponentialOpFunc_Execute_Call {
	_c.Call.Return(run)
	return _c
}

// NewExponentialOpFunc creates a new instance of ExponentialOpFunc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewExponentialOpFunc(t interface {
	mock.TestingT
	Cleanup(func())
}) *ExponentialOpFunc {
	mock := &ExponentialOpFunc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
