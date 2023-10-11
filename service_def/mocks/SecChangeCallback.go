// Code generated by mockery v2.32.0. DO NOT EDIT.

package mocks

import (
	service_def "github.com/couchbase/goxdcr/service_def"
	mock "github.com/stretchr/testify/mock"
)

// SecChangeCallback is an autogenerated mock type for the SecChangeCallback type
type SecChangeCallback struct {
	mock.Mock
}

// Execute provides a mock function with given fields: old, new
func (_m *SecChangeCallback) Execute(old service_def.EncryptionSettingIface, new service_def.EncryptionSettingIface) {
	_m.Called(old, new)
}

// NewSecChangeCallback creates a new instance of SecChangeCallback. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewSecChangeCallback(t interface {
	mock.TestingT
	Cleanup(func())
}) *SecChangeCallback {
	mock := &SecChangeCallback{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
