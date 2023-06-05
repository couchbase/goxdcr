// Code generated by mockery v2.20.0. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"

	pipeline "github.com/couchbase/goxdcr/pipeline"

	service_def "github.com/couchbase/goxdcr/service_def"
)

// PipelineMgrForSerializer is an autogenerated mock type for the PipelineMgrForSerializer type
type PipelineMgrForSerializer struct {
	mock.Mock
}

// AutoPauseReplication provides a mock function with given fields: topic
func (_m *PipelineMgrForSerializer) AutoPauseReplication(topic string) error {
	ret := _m.Called(topic)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// BackfillMappingUpdate provides a mock function with given fields: topic, diffPair, srcManifestsDelta
func (_m *PipelineMgrForSerializer) BackfillMappingUpdate(topic string, diffPair *metadata.CollectionNamespaceMappingsDiffPair, srcManifestsDelta []*metadata.CollectionsManifest) error {
	ret := _m.Called(topic, diffPair, srcManifestsDelta)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, *metadata.CollectionNamespaceMappingsDiffPair, []*metadata.CollectionsManifest) error); ok {
		r0 = rf(topic, diffPair, srcManifestsDelta)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CleanupBackfillPipeline provides a mock function with given fields: topic
func (_m *PipelineMgrForSerializer) CleanupBackfillPipeline(topic string) error {
	ret := _m.Called(topic)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CleanupPipeline provides a mock function with given fields: topic
func (_m *PipelineMgrForSerializer) CleanupPipeline(topic string) error {
	ret := _m.Called(topic)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DismissEvent provides a mock function with given fields: eventId
func (_m *PipelineMgrForSerializer) DismissEvent(eventId int) error {
	ret := _m.Called(eventId)

	var r0 error
	if rf, ok := ret.Get(0).(func(int) error); ok {
		r0 = rf(eventId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ForceTargetRefreshManifest provides a mock function with given fields: spec
func (_m *PipelineMgrForSerializer) ForceTargetRefreshManifest(spec *metadata.ReplicationSpecification) error {
	ret := _m.Called(spec)

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification) error); ok {
		r0 = rf(spec)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetLastUpdateResult provides a mock function with given fields: topic
func (_m *PipelineMgrForSerializer) GetLastUpdateResult(topic string) bool {
	ret := _m.Called(topic)

	var r0 bool
	if rf, ok := ret.Get(0).(func(string) bool); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// GetLogSvc provides a mock function with given fields:
func (_m *PipelineMgrForSerializer) GetLogSvc() service_def.UILogSvc {
	ret := _m.Called()

	var r0 service_def.UILogSvc
	if rf, ok := ret.Get(0).(func() service_def.UILogSvc); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(service_def.UILogSvc)
		}
	}

	return r0
}

// GetOrCreateReplicationStatus provides a mock function with given fields: topic, cur_err
func (_m *PipelineMgrForSerializer) GetOrCreateReplicationStatus(topic string, cur_err error) (*pipeline.ReplicationStatus, error) {
	ret := _m.Called(topic, cur_err)

	var r0 *pipeline.ReplicationStatus
	var r1 error
	if rf, ok := ret.Get(0).(func(string, error) (*pipeline.ReplicationStatus, error)); ok {
		return rf(topic, cur_err)
	}
	if rf, ok := ret.Get(0).(func(string, error) *pipeline.ReplicationStatus); ok {
		r0 = rf(topic, cur_err)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*pipeline.ReplicationStatus)
		}
	}

	if rf, ok := ret.Get(1).(func(string, error) error); ok {
		r1 = rf(topic, cur_err)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetRemoteClusterSvc provides a mock function with given fields:
func (_m *PipelineMgrForSerializer) GetRemoteClusterSvc() service_def.RemoteClusterSvc {
	ret := _m.Called()

	var r0 service_def.RemoteClusterSvc
	if rf, ok := ret.Get(0).(func() service_def.RemoteClusterSvc); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(service_def.RemoteClusterSvc)
		}
	}

	return r0
}

// GetReplSpecSvc provides a mock function with given fields:
func (_m *PipelineMgrForSerializer) GetReplSpecSvc() service_def.ReplicationSpecSvc {
	ret := _m.Called()

	var r0 service_def.ReplicationSpecSvc
	if rf, ok := ret.Get(0).(func() service_def.ReplicationSpecSvc); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(service_def.ReplicationSpecSvc)
		}
	}

	return r0
}

// GetXDCRTopologySvc provides a mock function with given fields:
func (_m *PipelineMgrForSerializer) GetXDCRTopologySvc() service_def.XDCRCompTopologySvc {
	ret := _m.Called()

	var r0 service_def.XDCRCompTopologySvc
	if rf, ok := ret.Get(0).(func() service_def.XDCRCompTopologySvc); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(service_def.XDCRCompTopologySvc)
		}
	}

	return r0
}

// PauseReplication provides a mock function with given fields: topic
func (_m *PipelineMgrForSerializer) PauseReplication(topic string) error {
	ret := _m.Called(topic)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RemoveReplicationStatus provides a mock function with given fields: topic
func (_m *PipelineMgrForSerializer) RemoveReplicationStatus(topic string) error {
	ret := _m.Called(topic)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// StartBackfill provides a mock function with given fields: topic
func (_m *PipelineMgrForSerializer) StartBackfill(topic string) error {
	ret := _m.Called(topic)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// StartPipeline provides a mock function with given fields: topic
func (_m *PipelineMgrForSerializer) StartPipeline(topic string) base.ErrorMap {
	ret := _m.Called(topic)

	var r0 base.ErrorMap
	if rf, ok := ret.Get(0).(func(string) base.ErrorMap); ok {
		r0 = rf(topic)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.ErrorMap)
		}
	}

	return r0
}

// StopAllUpdaters provides a mock function with given fields:
func (_m *PipelineMgrForSerializer) StopAllUpdaters() {
	_m.Called()
}

// StopBackfill provides a mock function with given fields: topic, skipCkpt
func (_m *PipelineMgrForSerializer) StopBackfill(topic string, skipCkpt bool) error {
	ret := _m.Called(topic, skipCkpt)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, bool) error); ok {
		r0 = rf(topic, skipCkpt)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// StopBackfillWithStoppedCb provides a mock function with given fields: topic, cb, errCb, skipCkpt
func (_m *PipelineMgrForSerializer) StopBackfillWithStoppedCb(topic string, cb base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback, skipCkpt bool) error {
	ret := _m.Called(topic, cb, errCb, skipCkpt)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, base.StoppedPipelineCallback, base.StoppedPipelineErrCallback, bool) error); ok {
		r0 = rf(topic, cb, errCb, skipCkpt)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// StopPipeline provides a mock function with given fields: rep_status
func (_m *PipelineMgrForSerializer) StopPipeline(rep_status pipeline.ReplicationStatusIface) base.ErrorMap {
	ret := _m.Called(rep_status)

	var r0 base.ErrorMap
	if rf, ok := ret.Get(0).(func(pipeline.ReplicationStatusIface) base.ErrorMap); ok {
		r0 = rf(rep_status)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.ErrorMap)
		}
	}

	return r0
}

// Update provides a mock function with given fields: topic, cur_err
func (_m *PipelineMgrForSerializer) Update(topic string, cur_err error) error {
	ret := _m.Called(topic, cur_err)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, error) error); ok {
		r0 = rf(topic, cur_err)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateWithStoppedCb provides a mock function with given fields: topic, callback, errCb
func (_m *PipelineMgrForSerializer) UpdateWithStoppedCb(topic string, callback base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback) error {
	ret := _m.Called(topic, callback, errCb)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, base.StoppedPipelineCallback, base.StoppedPipelineErrCallback) error); ok {
		r0 = rf(topic, callback, errCb)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTNewPipelineMgrForSerializer interface {
	mock.TestingT
	Cleanup(func())
}

// NewPipelineMgrForSerializer creates a new instance of PipelineMgrForSerializer. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewPipelineMgrForSerializer(t mockConstructorTestingTNewPipelineMgrForSerializer) *PipelineMgrForSerializer {
	mock := &PipelineMgrForSerializer{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
