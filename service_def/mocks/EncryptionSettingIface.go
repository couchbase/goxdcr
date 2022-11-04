// Code generated by mockery v2.14.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// EncryptionSettingIface is an autogenerated mock type for the EncryptionSettingIface type
type EncryptionSettingIface struct {
	mock.Mock
}

// IsStrictEncryption provides a mock function with given fields:
func (_m *EncryptionSettingIface) IsStrictEncryption() bool {
	ret := _m.Called()

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

type mockConstructorTestingTNewEncryptionSettingIface interface {
	mock.TestingT
	Cleanup(func())
}

// NewEncryptionSettingIface creates a new instance of EncryptionSettingIface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewEncryptionSettingIface(t mockConstructorTestingTNewEncryptionSettingIface) *EncryptionSettingIface {
	mock := &EncryptionSettingIface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
