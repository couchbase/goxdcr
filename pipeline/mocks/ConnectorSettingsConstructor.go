// Code generated by mockery v2.42.1. DO NOT EDIT.

package mocks

import (
	common "github.com/couchbase/goxdcr/common"
	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"
)

// ConnectorSettingsConstructor is an autogenerated mock type for the ConnectorSettingsConstructor type
type ConnectorSettingsConstructor struct {
	mock.Mock
}

// Execute provides a mock function with given fields: _a0, connector, pipeline_settings
func (_m *ConnectorSettingsConstructor) Execute(_a0 common.Pipeline, connector common.Connector, pipeline_settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error) {
	ret := _m.Called(_a0, connector, pipeline_settings)

	if len(ret) == 0 {
		panic("no return value specified for Execute")
	}

	var r0 metadata.ReplicationSettingsMap
	var r1 error
	if rf, ok := ret.Get(0).(func(common.Pipeline, common.Connector, metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error)); ok {
		return rf(_a0, connector, pipeline_settings)
	}
	if rf, ok := ret.Get(0).(func(common.Pipeline, common.Connector, metadata.ReplicationSettingsMap) metadata.ReplicationSettingsMap); ok {
		r0 = rf(_a0, connector, pipeline_settings)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(metadata.ReplicationSettingsMap)
		}
	}

	if rf, ok := ret.Get(1).(func(common.Pipeline, common.Connector, metadata.ReplicationSettingsMap) error); ok {
		r1 = rf(_a0, connector, pipeline_settings)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewConnectorSettingsConstructor creates a new instance of ConnectorSettingsConstructor. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewConnectorSettingsConstructor(t interface {
	mock.TestingT
	Cleanup(func())
}) *ConnectorSettingsConstructor {
	mock := &ConnectorSettingsConstructor{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
