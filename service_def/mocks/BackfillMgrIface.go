// Code generated by mockery v2.42.1. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"

	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"

	sync "sync"
)

// BackfillMgrIface is an autogenerated mock type for the BackfillMgrIface type
type BackfillMgrIface struct {
	mock.Mock
}

// GetExplicitMappingChangeHandler provides a mock function with given fields: specId, internalSpecId, oldSettings, newSettings
func (_m *BackfillMgrIface) GetExplicitMappingChangeHandler(specId string, internalSpecId string, oldSettings *metadata.ReplicationSettings, newSettings *metadata.ReplicationSettings) (base.StoppedPipelineCallback, base.StoppedPipelineErrCallback) {
	ret := _m.Called(specId, internalSpecId, oldSettings, newSettings)

	if len(ret) == 0 {
		panic("no return value specified for GetExplicitMappingChangeHandler")
	}

	var r0 base.StoppedPipelineCallback
	var r1 base.StoppedPipelineErrCallback
	if rf, ok := ret.Get(0).(func(string, string, *metadata.ReplicationSettings, *metadata.ReplicationSettings) (base.StoppedPipelineCallback, base.StoppedPipelineErrCallback)); ok {
		return rf(specId, internalSpecId, oldSettings, newSettings)
	}
	if rf, ok := ret.Get(0).(func(string, string, *metadata.ReplicationSettings, *metadata.ReplicationSettings) base.StoppedPipelineCallback); ok {
		r0 = rf(specId, internalSpecId, oldSettings, newSettings)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.StoppedPipelineCallback)
		}
	}

	if rf, ok := ret.Get(1).(func(string, string, *metadata.ReplicationSettings, *metadata.ReplicationSettings) base.StoppedPipelineErrCallback); ok {
		r1 = rf(specId, internalSpecId, oldSettings, newSettings)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(base.StoppedPipelineErrCallback)
		}
	}

	return r0, r1
}

// GetLastSuccessfulSourceManifestId provides a mock function with given fields: specId
func (_m *BackfillMgrIface) GetLastSuccessfulSourceManifestId(specId string) (uint64, error) {
	ret := _m.Called(specId)

	if len(ret) == 0 {
		panic("no return value specified for GetLastSuccessfulSourceManifestId")
	}

	var r0 uint64
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (uint64, error)); ok {
		return rf(specId)
	}
	if rf, ok := ret.Get(0).(func(string) uint64); ok {
		r0 = rf(specId)
	} else {
		r0 = ret.Get(0).(uint64)
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(specId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetPipelineSvc provides a mock function with given fields:
func (_m *BackfillMgrIface) GetPipelineSvc() common.PipelineService {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetPipelineSvc")
	}

	var r0 common.PipelineService
	if rf, ok := ret.Get(0).(func() common.PipelineService); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.PipelineService)
		}
	}

	return r0
}

// GetRouterMappingChangeHandler provides a mock function with given fields: specId, internalSpecId, diff
func (_m *BackfillMgrIface) GetRouterMappingChangeHandler(specId string, internalSpecId string, diff metadata.CollectionNamespaceMappingsDiffPair) (base.StoppedPipelineCallback, base.StoppedPipelineErrCallback) {
	ret := _m.Called(specId, internalSpecId, diff)

	if len(ret) == 0 {
		panic("no return value specified for GetRouterMappingChangeHandler")
	}

	var r0 base.StoppedPipelineCallback
	var r1 base.StoppedPipelineErrCallback
	if rf, ok := ret.Get(0).(func(string, string, metadata.CollectionNamespaceMappingsDiffPair) (base.StoppedPipelineCallback, base.StoppedPipelineErrCallback)); ok {
		return rf(specId, internalSpecId, diff)
	}
	if rf, ok := ret.Get(0).(func(string, string, metadata.CollectionNamespaceMappingsDiffPair) base.StoppedPipelineCallback); ok {
		r0 = rf(specId, internalSpecId, diff)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.StoppedPipelineCallback)
		}
	}

	if rf, ok := ret.Get(1).(func(string, string, metadata.CollectionNamespaceMappingsDiffPair) base.StoppedPipelineErrCallback); ok {
		r1 = rf(specId, internalSpecId, diff)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(base.StoppedPipelineErrCallback)
		}
	}

	return r0, r1
}

// ReplicationSpecChangeCallback provides a mock function with given fields: id, oldVal, newVal, wg
func (_m *BackfillMgrIface) ReplicationSpecChangeCallback(id string, oldVal interface{}, newVal interface{}, wg *sync.WaitGroup) error {
	ret := _m.Called(id, oldVal, newVal, wg)

	if len(ret) == 0 {
		panic("no return value specified for ReplicationSpecChangeCallback")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, interface{}, interface{}, *sync.WaitGroup) error); ok {
		r0 = rf(id, oldVal, newVal, wg)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetLastSuccessfulSourceManifestId provides a mock function with given fields: specId, manifestId, dcpRollbackScenario, finCh
func (_m *BackfillMgrIface) SetLastSuccessfulSourceManifestId(specId string, manifestId uint64, dcpRollbackScenario bool, finCh chan bool) error {
	ret := _m.Called(specId, manifestId, dcpRollbackScenario, finCh)

	if len(ret) == 0 {
		panic("no return value specified for SetLastSuccessfulSourceManifestId")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, uint64, bool, chan bool) error); ok {
		r0 = rf(specId, manifestId, dcpRollbackScenario, finCh)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Start provides a mock function with given fields:
func (_m *BackfillMgrIface) Start() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Start")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Stop provides a mock function with given fields:
func (_m *BackfillMgrIface) Stop() {
	_m.Called()
}

// NewBackfillMgrIface creates a new instance of BackfillMgrIface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewBackfillMgrIface(t interface {
	mock.TestingT
	Cleanup(func())
}) *BackfillMgrIface {
	mock := &BackfillMgrIface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
