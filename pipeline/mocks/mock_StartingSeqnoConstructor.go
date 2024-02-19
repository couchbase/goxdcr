// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	common "github.com/couchbase/goxdcr/common"
	mock "github.com/stretchr/testify/mock"
)

// StartingSeqnoConstructor is an autogenerated mock type for the StartingSeqnoConstructor type
type StartingSeqnoConstructor struct {
	mock.Mock
}

type StartingSeqnoConstructor_Expecter struct {
	mock *mock.Mock
}

func (_m *StartingSeqnoConstructor) EXPECT() *StartingSeqnoConstructor_Expecter {
	return &StartingSeqnoConstructor_Expecter{mock: &_m.Mock}
}

// Execute provides a mock function with given fields: _a0
func (_m *StartingSeqnoConstructor) Execute(_a0 common.Pipeline) error {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for Execute")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(common.Pipeline) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// StartingSeqnoConstructor_Execute_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Execute'
type StartingSeqnoConstructor_Execute_Call struct {
	*mock.Call
}

// Execute is a helper method to define mock.On call
//   - _a0 common.Pipeline
func (_e *StartingSeqnoConstructor_Expecter) Execute(_a0 interface{}) *StartingSeqnoConstructor_Execute_Call {
	return &StartingSeqnoConstructor_Execute_Call{Call: _e.mock.On("Execute", _a0)}
}

func (_c *StartingSeqnoConstructor_Execute_Call) Run(run func(_a0 common.Pipeline)) *StartingSeqnoConstructor_Execute_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(common.Pipeline))
	})
	return _c
}

func (_c *StartingSeqnoConstructor_Execute_Call) Return(_a0 error) *StartingSeqnoConstructor_Execute_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *StartingSeqnoConstructor_Execute_Call) RunAndReturn(run func(common.Pipeline) error) *StartingSeqnoConstructor_Execute_Call {
	_c.Call.Return(run)
	return _c
}

// NewStartingSeqnoConstructor creates a new instance of StartingSeqnoConstructor. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewStartingSeqnoConstructor(t interface {
	mock.TestingT
	Cleanup(func())
}) *StartingSeqnoConstructor {
	mock := &StartingSeqnoConstructor{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
