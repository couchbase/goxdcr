// Code generated by mockery v2.20.0. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/metadata"
	mock "github.com/stretchr/testify/mock"
)

// ReplicationSettingsSvc is an autogenerated mock type for the ReplicationSettingsSvc type
type ReplicationSettingsSvc struct {
	mock.Mock
}

// GetDefaultReplicationSettings provides a mock function with given fields:
func (_m *ReplicationSettingsSvc) GetDefaultReplicationSettings() (*metadata.ReplicationSettings, error) {
	ret := _m.Called()

	var r0 *metadata.ReplicationSettings
	var r1 error
	if rf, ok := ret.Get(0).(func() (*metadata.ReplicationSettings, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() *metadata.ReplicationSettings); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReplicationSettings)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpdateDefaultReplicationSettings provides a mock function with given fields: _a0
func (_m *ReplicationSettingsSvc) UpdateDefaultReplicationSettings(_a0 map[string]interface{}) (metadata.ReplicationSettingsMap, map[string]error, error) {
	ret := _m.Called(_a0)

	var r0 metadata.ReplicationSettingsMap
	var r1 map[string]error
	var r2 error
	if rf, ok := ret.Get(0).(func(map[string]interface{}) (metadata.ReplicationSettingsMap, map[string]error, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(map[string]interface{}) metadata.ReplicationSettingsMap); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(metadata.ReplicationSettingsMap)
		}
	}

	if rf, ok := ret.Get(1).(func(map[string]interface{}) map[string]error); ok {
		r1 = rf(_a0)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(map[string]error)
		}
	}

	if rf, ok := ret.Get(2).(func(map[string]interface{}) error); ok {
		r2 = rf(_a0)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

type mockConstructorTestingTNewReplicationSettingsSvc interface {
	mock.TestingT
	Cleanup(func())
}

// NewReplicationSettingsSvc creates a new instance of ReplicationSettingsSvc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewReplicationSettingsSvc(t mockConstructorTestingTNewReplicationSettingsSvc) *ReplicationSettingsSvc {
	mock := &ReplicationSettingsSvc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
