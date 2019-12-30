package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"

	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"

	pipeline "github.com/couchbase/goxdcr/pipeline"

	service_def "github.com/couchbase/goxdcr/service_def"
)

// Pipeline_mgr_iface is an autogenerated mock type for the Pipeline_mgr_iface type
type Pipeline_mgr_iface struct {
	mock.Mock
}

// AllReplicationSpecsForTargetCluster provides a mock function with given fields: targetClusterUuid
func (_m *Pipeline_mgr_iface) AllReplicationSpecsForTargetCluster(targetClusterUuid string) map[string]*metadata.ReplicationSpecification {
	ret := _m.Called(targetClusterUuid)

	var r0 map[string]*metadata.ReplicationSpecification
	if rf, ok := ret.Get(0).(func(string) map[string]*metadata.ReplicationSpecification); ok {
		r0 = rf(targetClusterUuid)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]*metadata.ReplicationSpecification)
		}
	}

	return r0
}

// AllReplications provides a mock function with given fields:
func (_m *Pipeline_mgr_iface) AllReplications() []string {
	ret := _m.Called()

	var r0 []string
	if rf, ok := ret.Get(0).(func() []string); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	return r0
}

// AllReplicationsForBucket provides a mock function with given fields: bucket
func (_m *Pipeline_mgr_iface) AllReplicationsForBucket(bucket string) []string {
	ret := _m.Called(bucket)

	var r0 []string
	if rf, ok := ret.Get(0).(func(string) []string); ok {
		r0 = rf(bucket)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	return r0
}

// AllReplicationsForTargetCluster provides a mock function with given fields: targetClusterUuid
func (_m *Pipeline_mgr_iface) AllReplicationsForTargetCluster(targetClusterUuid string) []string {
	ret := _m.Called(targetClusterUuid)

	var r0 []string
	if rf, ok := ret.Get(0).(func(string) []string); ok {
		r0 = rf(targetClusterUuid)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	return r0
}

// CheckPipelines provides a mock function with given fields:
func (_m *Pipeline_mgr_iface) CheckPipelines() {
	_m.Called()
}

// CleanupPipeline provides a mock function with given fields: topic
func (_m *Pipeline_mgr_iface) CleanupPipeline(topic string) error {
	ret := _m.Called(topic)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeletePipeline provides a mock function with given fields: pipelineName
func (_m *Pipeline_mgr_iface) DeletePipeline(pipelineName string) error {
	ret := _m.Called(pipelineName)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(pipelineName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetClusterInfoSvc provides a mock function with given fields:
func (_m *Pipeline_mgr_iface) GetClusterInfoSvc() service_def.ClusterInfoSvc {
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
func (_m *Pipeline_mgr_iface) GetLastUpdateResult(topic string) bool {
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
func (_m *Pipeline_mgr_iface) GetLogSvc() service_def.UILogSvc {
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
func (_m *Pipeline_mgr_iface) GetOrCreateReplicationStatus(topic string, cur_err error) (*pipeline.ReplicationStatus, error) {
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

// GetPipelineFactory provides a mock function with given fields:
func (_m *Pipeline_mgr_iface) GetPipelineFactory() common.PipelineFactory {
	ret := _m.Called()

	var r0 common.PipelineFactory
	if rf, ok := ret.Get(0).(func() common.PipelineFactory); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.PipelineFactory)
		}
	}

	return r0
}

// GetRemoteClusterSvc provides a mock function with given fields:
func (_m *Pipeline_mgr_iface) GetRemoteClusterSvc() service_def.RemoteClusterSvc {
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
func (_m *Pipeline_mgr_iface) GetReplSpecSvc() service_def.ReplicationSpecSvc {
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
func (_m *Pipeline_mgr_iface) GetXDCRTopologySvc() service_def.XDCRCompTopologySvc {
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

// InitiateRepStatus provides a mock function with given fields: pipelineName
func (_m *Pipeline_mgr_iface) InitiateRepStatus(pipelineName string) error {
	ret := _m.Called(pipelineName)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(pipelineName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// OnExit provides a mock function with given fields:
func (_m *Pipeline_mgr_iface) OnExit() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReInitStreams provides a mock function with given fields: pipelineName
func (_m *Pipeline_mgr_iface) ReInitStreams(pipelineName string) error {
	ret := _m.Called(pipelineName)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(pipelineName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RemoveReplicationCheckpoints provides a mock function with given fields: topic
func (_m *Pipeline_mgr_iface) RemoveReplicationCheckpoints(topic string) error {
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
func (_m *Pipeline_mgr_iface) RemoveReplicationStatus(topic string) error {
	ret := _m.Called(topic)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReplicationStatus provides a mock function with given fields: topic
func (_m *Pipeline_mgr_iface) ReplicationStatus(topic string) (*pipeline.ReplicationStatus, error) {
	ret := _m.Called(topic)

	var r0 *pipeline.ReplicationStatus
	if rf, ok := ret.Get(0).(func(string) *pipeline.ReplicationStatus); ok {
		r0 = rf(topic)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*pipeline.ReplicationStatus)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(topic)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReplicationStatusMap provides a mock function with given fields:
func (_m *Pipeline_mgr_iface) ReplicationStatusMap() map[string]*pipeline.ReplicationStatus {
	ret := _m.Called()

	var r0 map[string]*pipeline.ReplicationStatus
	if rf, ok := ret.Get(0).(func() map[string]*pipeline.ReplicationStatus); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]*pipeline.ReplicationStatus)
		}
	}

	return r0
}

// StartPipeline provides a mock function with given fields: topic
func (_m *Pipeline_mgr_iface) StartPipeline(topic string) base.ErrorMap {
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
func (_m *Pipeline_mgr_iface) StopAllUpdaters() {
	_m.Called()
}

// StopPipeline provides a mock function with given fields: rep_status, pipelineName
func (_m *Pipeline_mgr_iface) StopPipeline(rep_status pipeline.ReplicationStatusIface, pipelineName string) base.ErrorMap {
	ret := _m.Called(rep_status, pipelineName)

	var r0 base.ErrorMap
	if rf, ok := ret.Get(0).(func(pipeline.ReplicationStatusIface, string) base.ErrorMap); ok {
		r0 = rf(rep_status, pipelineName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.ErrorMap)
		}
	}

	return r0
}

// Update provides a mock function with given fields: topic, cur_err
func (_m *Pipeline_mgr_iface) Update(topic string, cur_err error) error {
	ret := _m.Called(topic, cur_err)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, error) error); ok {
		r0 = rf(topic, cur_err)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdatePipeline provides a mock function with given fields: pipelineName, cur_err
func (_m *Pipeline_mgr_iface) UpdatePipeline(pipelineName string, cur_err error) error {
	ret := _m.Called(pipelineName, cur_err)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, error) error); ok {
		r0 = rf(pipelineName, cur_err)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
