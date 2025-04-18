// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	service_def "github.com/couchbase/goxdcr/v8/service_def"
	mock "github.com/stretchr/testify/mock"
)

// BackfillMgrGetter is an autogenerated mock type for the BackfillMgrGetter type
type BackfillMgrGetter struct {
	mock.Mock
}

type BackfillMgrGetter_Expecter struct {
	mock *mock.Mock
}

func (_m *BackfillMgrGetter) EXPECT() *BackfillMgrGetter_Expecter {
	return &BackfillMgrGetter_Expecter{mock: &_m.Mock}
}

// Execute provides a mock function with no fields
func (_m *BackfillMgrGetter) Execute() service_def.BackfillMgrIface {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Execute")
	}

	var r0 service_def.BackfillMgrIface
	if rf, ok := ret.Get(0).(func() service_def.BackfillMgrIface); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(service_def.BackfillMgrIface)
		}
	}

	return r0
}

// BackfillMgrGetter_Execute_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Execute'
type BackfillMgrGetter_Execute_Call struct {
	*mock.Call
}

// Execute is a helper method to define mock.On call
func (_e *BackfillMgrGetter_Expecter) Execute() *BackfillMgrGetter_Execute_Call {
	return &BackfillMgrGetter_Execute_Call{Call: _e.mock.On("Execute")}
}

func (_c *BackfillMgrGetter_Execute_Call) Run(run func()) *BackfillMgrGetter_Execute_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *BackfillMgrGetter_Execute_Call) Return(_a0 service_def.BackfillMgrIface) *BackfillMgrGetter_Execute_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *BackfillMgrGetter_Execute_Call) RunAndReturn(run func() service_def.BackfillMgrIface) *BackfillMgrGetter_Execute_Call {
	_c.Call.Return(run)
	return _c
}

// NewBackfillMgrGetter creates a new instance of BackfillMgrGetter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewBackfillMgrGetter(t interface {
	mock.TestingT
	Cleanup(func())
}) *BackfillMgrGetter {
	mock := &BackfillMgrGetter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
