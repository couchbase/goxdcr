// Code generated by mockery v2.42.1. DO NOT EDIT.

package mocks

import (
	common "github.com/couchbase/goxdcr/common"
	mock "github.com/stretchr/testify/mock"
)

// AsyncComponentEventHandler is an autogenerated mock type for the AsyncComponentEventHandler type
type AsyncComponentEventHandler struct {
	mock.Mock
}

// Id provides a mock function with given fields:
func (_m *AsyncComponentEventHandler) Id() string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Id")
	}

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// ProcessEvent provides a mock function with given fields: event
func (_m *AsyncComponentEventHandler) ProcessEvent(event *common.Event) error {
	ret := _m.Called(event)

	if len(ret) == 0 {
		panic("no return value specified for ProcessEvent")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*common.Event) error); ok {
		r0 = rf(event)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewAsyncComponentEventHandler creates a new instance of AsyncComponentEventHandler. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewAsyncComponentEventHandler(t interface {
	mock.TestingT
	Cleanup(func())
}) *AsyncComponentEventHandler {
	mock := &AsyncComponentEventHandler{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
