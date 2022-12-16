// Code generated by mockery v2.14.0. DO NOT EDIT.

package mocks

import (
	service_def "github.com/couchbase/goxdcr/service_def"
	mock "github.com/stretchr/testify/mock"
)

// AuditSvc is an autogenerated mock type for the AuditSvc type
type AuditSvc struct {
	mock.Mock
}

// Write provides a mock function with given fields: eventId, event
func (_m *AuditSvc) Write(eventId uint32, event service_def.AuditEventIface) error {
	ret := _m.Called(eventId, event)

	var r0 error
	if rf, ok := ret.Get(0).(func(uint32, service_def.AuditEventIface) error); ok {
		r0 = rf(eventId, event)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTNewAuditSvc interface {
	mock.TestingT
	Cleanup(func())
}

// NewAuditSvc creates a new instance of AuditSvc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewAuditSvc(t mockConstructorTestingTNewAuditSvc) *AuditSvc {
	mock := &AuditSvc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
