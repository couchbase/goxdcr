// Code generated by mockery. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// TargetTimestamp is an autogenerated mock type for the TargetTimestamp type
type TargetTimestamp struct {
	mock.Mock
}

type TargetTimestamp_Expecter struct {
	mock *mock.Mock
}

func (_m *TargetTimestamp) EXPECT() *TargetTimestamp_Expecter {
	return &TargetTimestamp_Expecter{mock: &_m.Mock}
}

// GetValue provides a mock function with no fields
func (_m *TargetTimestamp) GetValue() interface{} {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetValue")
	}

	var r0 interface{}
	if rf, ok := ret.Get(0).(func() interface{}); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	return r0
}

// TargetTimestamp_GetValue_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetValue'
type TargetTimestamp_GetValue_Call struct {
	*mock.Call
}

// GetValue is a helper method to define mock.On call
func (_e *TargetTimestamp_Expecter) GetValue() *TargetTimestamp_GetValue_Call {
	return &TargetTimestamp_GetValue_Call{Call: _e.mock.On("GetValue")}
}

func (_c *TargetTimestamp_GetValue_Call) Run(run func()) *TargetTimestamp_GetValue_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *TargetTimestamp_GetValue_Call) Return(_a0 interface{}) *TargetTimestamp_GetValue_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *TargetTimestamp_GetValue_Call) RunAndReturn(run func() interface{}) *TargetTimestamp_GetValue_Call {
	_c.Call.Return(run)
	return _c
}

// IsTraditional provides a mock function with no fields
func (_m *TargetTimestamp) IsTraditional() bool {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for IsTraditional")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// TargetTimestamp_IsTraditional_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'IsTraditional'
type TargetTimestamp_IsTraditional_Call struct {
	*mock.Call
}

// IsTraditional is a helper method to define mock.On call
func (_e *TargetTimestamp_Expecter) IsTraditional() *TargetTimestamp_IsTraditional_Call {
	return &TargetTimestamp_IsTraditional_Call{Call: _e.mock.On("IsTraditional")}
}

func (_c *TargetTimestamp_IsTraditional_Call) Run(run func()) *TargetTimestamp_IsTraditional_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *TargetTimestamp_IsTraditional_Call) Return(_a0 bool) *TargetTimestamp_IsTraditional_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *TargetTimestamp_IsTraditional_Call) RunAndReturn(run func() bool) *TargetTimestamp_IsTraditional_Call {
	_c.Call.Return(run)
	return _c
}

// NewTargetTimestamp creates a new instance of TargetTimestamp. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewTargetTimestamp(t interface {
	mock.TestingT
	Cleanup(func())
}) *TargetTimestamp {
	mock := &TargetTimestamp{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
