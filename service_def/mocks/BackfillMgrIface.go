package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"

	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"
)

// BackfillMgrIface is an autogenerated mock type for the BackfillMgrIface type
type BackfillMgrIface struct {
	mock.Mock
}

// GetExplicitMappingChangeHandler provides a mock function with given fields: specId, internalSpecId, oldSettings, newSettings
func (_m *BackfillMgrIface) GetExplicitMappingChangeHandler(specId string, internalSpecId string, oldSettings *metadata.ReplicationSettings, newSettings *metadata.ReplicationSettings) (base.StoppedPipelineCallback, base.StoppedPipelineErrCallback) {
	ret := _m.Called(specId, internalSpecId, oldSettings, newSettings)

	var r0 base.StoppedPipelineCallback
	if rf, ok := ret.Get(0).(func(string, string, *metadata.ReplicationSettings, *metadata.ReplicationSettings) base.StoppedPipelineCallback); ok {
		r0 = rf(specId, internalSpecId, oldSettings, newSettings)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.StoppedPipelineCallback)
		}
	}

	var r1 base.StoppedPipelineErrCallback
	if rf, ok := ret.Get(1).(func(string, string, *metadata.ReplicationSettings, *metadata.ReplicationSettings) base.StoppedPipelineErrCallback); ok {
		r1 = rf(specId, internalSpecId, oldSettings, newSettings)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(base.StoppedPipelineErrCallback)
		}
	}

	return r0, r1
}

// GetPipelineSvc provides a mock function with given fields:
func (_m *BackfillMgrIface) GetPipelineSvc() common.PipelineService {
	ret := _m.Called()

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

	var r0 base.StoppedPipelineCallback
	if rf, ok := ret.Get(0).(func(string, string, metadata.CollectionNamespaceMappingsDiffPair) base.StoppedPipelineCallback); ok {
		r0 = rf(specId, internalSpecId, diff)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.StoppedPipelineCallback)
		}
	}

	var r1 base.StoppedPipelineErrCallback
	if rf, ok := ret.Get(1).(func(string, string, metadata.CollectionNamespaceMappingsDiffPair) base.StoppedPipelineErrCallback); ok {
		r1 = rf(specId, internalSpecId, diff)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(base.StoppedPipelineErrCallback)
		}
	}

	return r0, r1
}

// ReplicationSpecChangeCallback provides a mock function with given fields: id, oldVal, newVal
func (_m *BackfillMgrIface) ReplicationSpecChangeCallback(id string, oldVal interface{}, newVal interface{}) error {
	ret := _m.Called(id, oldVal, newVal)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, interface{}, interface{}) error); ok {
		r0 = rf(id, oldVal, newVal)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Start provides a mock function with given fields:
func (_m *BackfillMgrIface) Start() error {
	ret := _m.Called()

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
