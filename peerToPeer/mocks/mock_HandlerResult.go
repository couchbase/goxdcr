// Code generated by mockery v2.43.2. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// HandlerResult is an autogenerated mock type for the HandlerResult type
type HandlerResult struct {
	mock.Mock
}

type HandlerResult_Expecter struct {
	mock *mock.Mock
}

func (_m *HandlerResult) EXPECT() *HandlerResult_Expecter {
	return &HandlerResult_Expecter{mock: &_m.Mock}
}

// GetError provides a mock function with given fields:
func (_m *HandlerResult) GetError() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetError")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// HandlerResult_GetError_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetError'
type HandlerResult_GetError_Call struct {
	*mock.Call
}

// GetError is a helper method to define mock.On call
func (_e *HandlerResult_Expecter) GetError() *HandlerResult_GetError_Call {
	return &HandlerResult_GetError_Call{Call: _e.mock.On("GetError")}
}

func (_c *HandlerResult_GetError_Call) Run(run func()) *HandlerResult_GetError_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *HandlerResult_GetError_Call) Return(_a0 error) *HandlerResult_GetError_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *HandlerResult_GetError_Call) RunAndReturn(run func() error) *HandlerResult_GetError_Call {
	_c.Call.Return(run)
	return _c
}

// GetHttpStatusCode provides a mock function with given fields:
func (_m *HandlerResult) GetHttpStatusCode() int {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetHttpStatusCode")
	}

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// HandlerResult_GetHttpStatusCode_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetHttpStatusCode'
type HandlerResult_GetHttpStatusCode_Call struct {
	*mock.Call
}

// GetHttpStatusCode is a helper method to define mock.On call
func (_e *HandlerResult_Expecter) GetHttpStatusCode() *HandlerResult_GetHttpStatusCode_Call {
	return &HandlerResult_GetHttpStatusCode_Call{Call: _e.mock.On("GetHttpStatusCode")}
}

func (_c *HandlerResult_GetHttpStatusCode_Call) Run(run func()) *HandlerResult_GetHttpStatusCode_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *HandlerResult_GetHttpStatusCode_Call) Return(_a0 int) *HandlerResult_GetHttpStatusCode_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *HandlerResult_GetHttpStatusCode_Call) RunAndReturn(run func() int) *HandlerResult_GetHttpStatusCode_Call {
	_c.Call.Return(run)
	return _c
}

// NewHandlerResult creates a new instance of HandlerResult. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewHandlerResult(t interface {
	mock.TestingT
	Cleanup(func())
}) *HandlerResult {
	mock := &HandlerResult{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
