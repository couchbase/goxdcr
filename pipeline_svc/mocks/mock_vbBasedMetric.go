// Code generated by mockery. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// vbBasedMetric is an autogenerated mock type for the vbBasedMetric type
type vbBasedMetric struct {
	mock.Mock
}

type vbBasedMetric_Expecter struct {
	mock *mock.Mock
}

func (_m *vbBasedMetric) EXPECT() *vbBasedMetric_Expecter {
	return &vbBasedMetric_Expecter{mock: &_m.Mock}
}

// Getter provides a mock function with given fields: _a0
func (_m *vbBasedMetric) Getter(_a0 uint16) (interface{}, error) {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for Getter")
	}

	var r0 interface{}
	var r1 error
	if rf, ok := ret.Get(0).(func(uint16) (interface{}, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(uint16) interface{}); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	if rf, ok := ret.Get(1).(func(uint16) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// vbBasedMetric_Getter_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Getter'
type vbBasedMetric_Getter_Call struct {
	*mock.Call
}

// Getter is a helper method to define mock.On call
//   - _a0 uint16
func (_e *vbBasedMetric_Expecter) Getter(_a0 interface{}) *vbBasedMetric_Getter_Call {
	return &vbBasedMetric_Getter_Call{Call: _e.mock.On("Getter", _a0)}
}

func (_c *vbBasedMetric_Getter_Call) Run(run func(_a0 uint16)) *vbBasedMetric_Getter_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint16))
	})
	return _c
}

func (_c *vbBasedMetric_Getter_Call) Return(_a0 interface{}, _a1 error) *vbBasedMetric_Getter_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *vbBasedMetric_Getter_Call) RunAndReturn(run func(uint16) (interface{}, error)) *vbBasedMetric_Getter_Call {
	_c.Call.Return(run)
	return _c
}

// IsTraditional provides a mock function with given fields:
func (_m *vbBasedMetric) IsTraditional() bool {
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

// vbBasedMetric_IsTraditional_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'IsTraditional'
type vbBasedMetric_IsTraditional_Call struct {
	*mock.Call
}

// IsTraditional is a helper method to define mock.On call
func (_e *vbBasedMetric_Expecter) IsTraditional() *vbBasedMetric_IsTraditional_Call {
	return &vbBasedMetric_IsTraditional_Call{Call: _e.mock.On("IsTraditional")}
}

func (_c *vbBasedMetric_IsTraditional_Call) Run(run func()) *vbBasedMetric_IsTraditional_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *vbBasedMetric_IsTraditional_Call) Return(_a0 bool) *vbBasedMetric_IsTraditional_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *vbBasedMetric_IsTraditional_Call) RunAndReturn(run func() bool) *vbBasedMetric_IsTraditional_Call {
	_c.Call.Return(run)
	return _c
}

// Len provides a mock function with given fields:
func (_m *vbBasedMetric) Len() int {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Len")
	}

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// vbBasedMetric_Len_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Len'
type vbBasedMetric_Len_Call struct {
	*mock.Call
}

// Len is a helper method to define mock.On call
func (_e *vbBasedMetric_Expecter) Len() *vbBasedMetric_Len_Call {
	return &vbBasedMetric_Len_Call{Call: _e.mock.On("Len")}
}

func (_c *vbBasedMetric_Len_Call) Run(run func()) *vbBasedMetric_Len_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *vbBasedMetric_Len_Call) Return(_a0 int) *vbBasedMetric_Len_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *vbBasedMetric_Len_Call) RunAndReturn(run func() int) *vbBasedMetric_Len_Call {
	_c.Call.Return(run)
	return _c
}

// Setter provides a mock function with given fields: _a0, _a1
func (_m *vbBasedMetric) Setter(_a0 uint16, _a1 interface{}) error {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for Setter")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(uint16, interface{}) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// vbBasedMetric_Setter_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Setter'
type vbBasedMetric_Setter_Call struct {
	*mock.Call
}

// Setter is a helper method to define mock.On call
//   - _a0 uint16
//   - _a1 interface{}
func (_e *vbBasedMetric_Expecter) Setter(_a0 interface{}, _a1 interface{}) *vbBasedMetric_Setter_Call {
	return &vbBasedMetric_Setter_Call{Call: _e.mock.On("Setter", _a0, _a1)}
}

func (_c *vbBasedMetric_Setter_Call) Run(run func(_a0 uint16, _a1 interface{})) *vbBasedMetric_Setter_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint16), args[1].(interface{}))
	})
	return _c
}

func (_c *vbBasedMetric_Setter_Call) Return(_a0 error) *vbBasedMetric_Setter_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *vbBasedMetric_Setter_Call) RunAndReturn(run func(uint16, interface{}) error) *vbBasedMetric_Setter_Call {
	_c.Call.Return(run)
	return _c
}

// newVbBasedMetric creates a new instance of vbBasedMetric. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newVbBasedMetric(t interface {
	mock.TestingT
	Cleanup(func())
}) *vbBasedMetric {
	mock := &vbBasedMetric{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}