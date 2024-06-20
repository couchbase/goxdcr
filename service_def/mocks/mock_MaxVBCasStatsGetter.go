// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	mock "github.com/stretchr/testify/mock"
)

// MaxVBCasStatsGetter is an autogenerated mock type for the MaxVBCasStatsGetter type
type MaxVBCasStatsGetter struct {
	mock.Mock
}

type MaxVBCasStatsGetter_Expecter struct {
	mock *mock.Mock
}

func (_m *MaxVBCasStatsGetter) EXPECT() *MaxVBCasStatsGetter_Expecter {
	return &MaxVBCasStatsGetter_Expecter{mock: &_m.Mock}
}

// Execute provides a mock function with given fields:
func (_m *MaxVBCasStatsGetter) Execute() (base.HighSeqnosMapType, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Execute")
	}

	var r0 base.HighSeqnosMapType
	var r1 error
	if rf, ok := ret.Get(0).(func() (base.HighSeqnosMapType, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() base.HighSeqnosMapType); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.HighSeqnosMapType)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MaxVBCasStatsGetter_Execute_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Execute'
type MaxVBCasStatsGetter_Execute_Call struct {
	*mock.Call
}

// Execute is a helper method to define mock.On call
func (_e *MaxVBCasStatsGetter_Expecter) Execute() *MaxVBCasStatsGetter_Execute_Call {
	return &MaxVBCasStatsGetter_Execute_Call{Call: _e.mock.On("Execute")}
}

func (_c *MaxVBCasStatsGetter_Execute_Call) Run(run func()) *MaxVBCasStatsGetter_Execute_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MaxVBCasStatsGetter_Execute_Call) Return(_a0 base.HighSeqnosMapType, _a1 error) *MaxVBCasStatsGetter_Execute_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MaxVBCasStatsGetter_Execute_Call) RunAndReturn(run func() (base.HighSeqnosMapType, error)) *MaxVBCasStatsGetter_Execute_Call {
	_c.Call.Return(run)
	return _c
}

// NewMaxVBCasStatsGetter creates a new instance of MaxVBCasStatsGetter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMaxVBCasStatsGetter(t interface {
	mock.TestingT
	Cleanup(func())
}) *MaxVBCasStatsGetter {
	mock := &MaxVBCasStatsGetter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
