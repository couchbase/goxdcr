// Code generated by mockery v2.50.0. DO NOT EDIT.

package mocks

import (
	common "github.com/couchbase/goxdcr/common"
	mock "github.com/stretchr/testify/mock"
)

// BackfillMgrComponentListenerGetter is an autogenerated mock type for the BackfillMgrComponentListenerGetter type
type BackfillMgrComponentListenerGetter struct {
	mock.Mock
}

type BackfillMgrComponentListenerGetter_Expecter struct {
	mock *mock.Mock
}

func (_m *BackfillMgrComponentListenerGetter) EXPECT() *BackfillMgrComponentListenerGetter_Expecter {
	return &BackfillMgrComponentListenerGetter_Expecter{mock: &_m.Mock}
}

// GetComponentEventListener provides a mock function with given fields: pipeline
func (_m *BackfillMgrComponentListenerGetter) GetComponentEventListener(pipeline common.Pipeline) (common.ComponentEventListener, error) {
	ret := _m.Called(pipeline)

	if len(ret) == 0 {
		panic("no return value specified for GetComponentEventListener")
	}

	var r0 common.ComponentEventListener
	var r1 error
	if rf, ok := ret.Get(0).(func(common.Pipeline) (common.ComponentEventListener, error)); ok {
		return rf(pipeline)
	}
	if rf, ok := ret.Get(0).(func(common.Pipeline) common.ComponentEventListener); ok {
		r0 = rf(pipeline)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.ComponentEventListener)
		}
	}

	if rf, ok := ret.Get(1).(func(common.Pipeline) error); ok {
		r1 = rf(pipeline)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BackfillMgrComponentListenerGetter_GetComponentEventListener_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetComponentEventListener'
type BackfillMgrComponentListenerGetter_GetComponentEventListener_Call struct {
	*mock.Call
}

// GetComponentEventListener is a helper method to define mock.On call
//   - pipeline common.Pipeline
func (_e *BackfillMgrComponentListenerGetter_Expecter) GetComponentEventListener(pipeline interface{}) *BackfillMgrComponentListenerGetter_GetComponentEventListener_Call {
	return &BackfillMgrComponentListenerGetter_GetComponentEventListener_Call{Call: _e.mock.On("GetComponentEventListener", pipeline)}
}

func (_c *BackfillMgrComponentListenerGetter_GetComponentEventListener_Call) Run(run func(pipeline common.Pipeline)) *BackfillMgrComponentListenerGetter_GetComponentEventListener_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(common.Pipeline))
	})
	return _c
}

func (_c *BackfillMgrComponentListenerGetter_GetComponentEventListener_Call) Return(_a0 common.ComponentEventListener, _a1 error) *BackfillMgrComponentListenerGetter_GetComponentEventListener_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *BackfillMgrComponentListenerGetter_GetComponentEventListener_Call) RunAndReturn(run func(common.Pipeline) (common.ComponentEventListener, error)) *BackfillMgrComponentListenerGetter_GetComponentEventListener_Call {
	_c.Call.Return(run)
	return _c
}

// NewBackfillMgrComponentListenerGetter creates a new instance of BackfillMgrComponentListenerGetter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewBackfillMgrComponentListenerGetter(t interface {
	mock.TestingT
	Cleanup(func())
}) *BackfillMgrComponentListenerGetter {
	mock := &BackfillMgrComponentListenerGetter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
