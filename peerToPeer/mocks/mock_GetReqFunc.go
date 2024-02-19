// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	peerToPeer "github.com/couchbase/goxdcr/peerToPeer"
	mock "github.com/stretchr/testify/mock"
)

// GetReqFunc is an autogenerated mock type for the GetReqFunc type
type GetReqFunc struct {
	mock.Mock
}

type GetReqFunc_Expecter struct {
	mock *mock.Mock
}

func (_m *GetReqFunc) EXPECT() *GetReqFunc_Expecter {
	return &GetReqFunc_Expecter{mock: &_m.Mock}
}

// Execute provides a mock function with given fields: source, target
func (_m *GetReqFunc) Execute(source string, target string) peerToPeer.Request {
	ret := _m.Called(source, target)

	if len(ret) == 0 {
		panic("no return value specified for Execute")
	}

	var r0 peerToPeer.Request
	if rf, ok := ret.Get(0).(func(string, string) peerToPeer.Request); ok {
		r0 = rf(source, target)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(peerToPeer.Request)
		}
	}

	return r0
}

// GetReqFunc_Execute_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Execute'
type GetReqFunc_Execute_Call struct {
	*mock.Call
}

// Execute is a helper method to define mock.On call
//   - source string
//   - target string
func (_e *GetReqFunc_Expecter) Execute(source interface{}, target interface{}) *GetReqFunc_Execute_Call {
	return &GetReqFunc_Execute_Call{Call: _e.mock.On("Execute", source, target)}
}

func (_c *GetReqFunc_Execute_Call) Run(run func(source string, target string)) *GetReqFunc_Execute_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string))
	})
	return _c
}

func (_c *GetReqFunc_Execute_Call) Return(_a0 peerToPeer.Request) *GetReqFunc_Execute_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *GetReqFunc_Execute_Call) RunAndReturn(run func(string, string) peerToPeer.Request) *GetReqFunc_Execute_Call {
	_c.Call.Return(run)
	return _c
}

// NewGetReqFunc creates a new instance of GetReqFunc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewGetReqFunc(t interface {
	mock.TestingT
	Cleanup(func())
}) *GetReqFunc {
	mock := &GetReqFunc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
