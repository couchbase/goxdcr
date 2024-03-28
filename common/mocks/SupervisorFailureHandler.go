// Code generated by mockery v2.42.1. DO NOT EDIT.

package mocks

import (
	common "github.com/couchbase/goxdcr/common"
	mock "github.com/stretchr/testify/mock"
)

// SupervisorFailureHandler is an autogenerated mock type for the SupervisorFailureHandler type
type SupervisorFailureHandler struct {
	mock.Mock
}

// OnError provides a mock function with given fields: supervisor, errors
func (_m *SupervisorFailureHandler) OnError(supervisor common.Supervisor, errors map[string]error) {
	_m.Called(supervisor, errors)
}

// NewSupervisorFailureHandler creates a new instance of SupervisorFailureHandler. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewSupervisorFailureHandler(t interface {
	mock.TestingT
	Cleanup(func())
}) *SupervisorFailureHandler {
	mock := &SupervisorFailureHandler{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
