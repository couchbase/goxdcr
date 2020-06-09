package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"

	pipeline "github.com/couchbase/goxdcr/pipeline"

	service_def "github.com/couchbase/goxdcr/service_def"
)

// PipelineMgrForUpdater is an autogenerated mock type for the PipelineMgrForUpdater type
type PipelineMgrForUpdater struct {
	mock.Mock
}

// AutoPauseReplication provides a mock function with given fields: topic
func (_m *PipelineMgrForUpdater) AutoPauseReplication(topic string) error {
	ret := _m.Called(topic)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ForceTargetRefreshManifest provides a mock function with given fields: spec
func (_m *PipelineMgrForUpdater) ForceTargetRefreshManifest(spec *metadata.ReplicationSpecification) error {
	ret := _m.Called(spec)

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification) error); ok {
		r0 = rf(spec)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetClusterInfoSvc provides a mock function with given fields:
func (_m *PipelineMgrForUpdater) GetClusterInfoSvc() service_def.ClusterInfoSvc {
	ret := _m.Called()

	var r0 service_def.ClusterInfoSvc
	if rf, ok := ret.Get(0).(func() service_def.ClusterInfoSvc); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(service_def.ClusterInfoSvc)
		}
	}

	return r0
}

// GetLastUpdateResult provides a mock function with given fields: topic
func (_m *PipelineMgrForUpdater) GetLastUpdateResult(topic string) bool {
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
func (_m *PipelineMgrForUpdater) GetLogSvc() service_def.UILogSvc {
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
func (_m *PipelineMgrForUpdater) GetOrCreateReplicationStatus(topic string, cur_err error) (*pipeline.ReplicationStatus, error) {
	ret := _m.Called(topic, cur_err)

	var r0 *pipeline.ReplicationStatus
	if rf, ok := ret.Get(0).(func(string, error) *pipeline.ReplicationStatus); ok {
		r0 = rf(topic, cur_err)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*pipeline.ReplicationStatus)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, error) error); ok {
		r1 = rf(topic, cur_err)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetRemoteClusterSvc provides a mock function with given fields:
func (_m *PipelineMgrForUpdater) GetRemoteClusterSvc() service_def.RemoteClusterSvc {
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
func (_m *PipelineMgrForUpdater) GetReplSpecSvc() service_def.ReplicationSpecSvc {
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
func (_m *PipelineMgrForUpdater) GetXDCRTopologySvc() service_def.XDCRCompTopologySvc {
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

// RemoveReplicationStatus provides a mock function with given fields: topic
func (_m *PipelineMgrForUpdater) RemoveReplicationStatus(topic string) error {
	ret := _m.Called(topic)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// StartBackfillPipeline provides a mock function with given fields: topic
func (_m *PipelineMgrForUpdater) StartBackfillPipeline(topic string) base.ErrorMap {
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

// StartPipeline provides a mock function with given fields: topic
func (_m *PipelineMgrForUpdater) StartPipeline(topic string) base.ErrorMap {
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
func (_m *PipelineMgrForUpdater) StopAllUpdaters() {
	_m.Called()
}

// StopBackfillPipeline provides a mock function with given fields: topic
func (_m *PipelineMgrForUpdater) StopBackfillPipeline(topic string) base.ErrorMap {
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

// StopPipeline provides a mock function with given fields: rep_status
func (_m *PipelineMgrForUpdater) StopPipeline(rep_status pipeline.ReplicationStatusIface) base.ErrorMap {
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
func (_m *PipelineMgrForUpdater) Update(topic string, cur_err error) error {
	ret := _m.Called(topic, cur_err)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, error) error); ok {
		r0 = rf(topic, cur_err)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
