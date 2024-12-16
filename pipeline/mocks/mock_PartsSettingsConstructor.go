// Code generated by mockery v2.50.0. DO NOT EDIT.

package mocks

import (
	common "github.com/couchbase/goxdcr/common"
	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"
)

// PartsSettingsConstructor is an autogenerated mock type for the PartsSettingsConstructor type
type PartsSettingsConstructor struct {
	mock.Mock
}

type PartsSettingsConstructor_Expecter struct {
	mock *mock.Mock
}

func (_m *PartsSettingsConstructor) EXPECT() *PartsSettingsConstructor_Expecter {
	return &PartsSettingsConstructor_Expecter{mock: &_m.Mock}
}

// Execute provides a mock function with given fields: _a0, part, pipeline_settings, targetClusterref, ssl_port_map
func (_m *PartsSettingsConstructor) Execute(_a0 common.Pipeline, part common.Part, pipeline_settings metadata.ReplicationSettingsMap, targetClusterref *metadata.RemoteClusterReference, ssl_port_map map[string]uint16) (metadata.ReplicationSettingsMap, error) {
	ret := _m.Called(_a0, part, pipeline_settings, targetClusterref, ssl_port_map)

	if len(ret) == 0 {
		panic("no return value specified for Execute")
	}

	var r0 metadata.ReplicationSettingsMap
	var r1 error
	if rf, ok := ret.Get(0).(func(common.Pipeline, common.Part, metadata.ReplicationSettingsMap, *metadata.RemoteClusterReference, map[string]uint16) (metadata.ReplicationSettingsMap, error)); ok {
		return rf(_a0, part, pipeline_settings, targetClusterref, ssl_port_map)
	}
	if rf, ok := ret.Get(0).(func(common.Pipeline, common.Part, metadata.ReplicationSettingsMap, *metadata.RemoteClusterReference, map[string]uint16) metadata.ReplicationSettingsMap); ok {
		r0 = rf(_a0, part, pipeline_settings, targetClusterref, ssl_port_map)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(metadata.ReplicationSettingsMap)
		}
	}

	if rf, ok := ret.Get(1).(func(common.Pipeline, common.Part, metadata.ReplicationSettingsMap, *metadata.RemoteClusterReference, map[string]uint16) error); ok {
		r1 = rf(_a0, part, pipeline_settings, targetClusterref, ssl_port_map)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// PartsSettingsConstructor_Execute_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Execute'
type PartsSettingsConstructor_Execute_Call struct {
	*mock.Call
}

// Execute is a helper method to define mock.On call
//   - _a0 common.Pipeline
//   - part common.Part
//   - pipeline_settings metadata.ReplicationSettingsMap
//   - targetClusterref *metadata.RemoteClusterReference
//   - ssl_port_map map[string]uint16
func (_e *PartsSettingsConstructor_Expecter) Execute(_a0 interface{}, part interface{}, pipeline_settings interface{}, targetClusterref interface{}, ssl_port_map interface{}) *PartsSettingsConstructor_Execute_Call {
	return &PartsSettingsConstructor_Execute_Call{Call: _e.mock.On("Execute", _a0, part, pipeline_settings, targetClusterref, ssl_port_map)}
}

func (_c *PartsSettingsConstructor_Execute_Call) Run(run func(_a0 common.Pipeline, part common.Part, pipeline_settings metadata.ReplicationSettingsMap, targetClusterref *metadata.RemoteClusterReference, ssl_port_map map[string]uint16)) *PartsSettingsConstructor_Execute_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(common.Pipeline), args[1].(common.Part), args[2].(metadata.ReplicationSettingsMap), args[3].(*metadata.RemoteClusterReference), args[4].(map[string]uint16))
	})
	return _c
}

func (_c *PartsSettingsConstructor_Execute_Call) Return(_a0 metadata.ReplicationSettingsMap, _a1 error) *PartsSettingsConstructor_Execute_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *PartsSettingsConstructor_Execute_Call) RunAndReturn(run func(common.Pipeline, common.Part, metadata.ReplicationSettingsMap, *metadata.RemoteClusterReference, map[string]uint16) (metadata.ReplicationSettingsMap, error)) *PartsSettingsConstructor_Execute_Call {
	_c.Call.Return(run)
	return _c
}

// NewPartsSettingsConstructor creates a new instance of PartsSettingsConstructor. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewPartsSettingsConstructor(t interface {
	mock.TestingT
	Cleanup(func())
}) *PartsSettingsConstructor {
	mock := &PartsSettingsConstructor{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
