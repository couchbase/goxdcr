// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/v8/base"
	metadata "github.com/couchbase/goxdcr/v8/metadata"

	mock "github.com/stretchr/testify/mock"

	pipeline "github.com/couchbase/goxdcr/v8/pipeline"

	service_def "github.com/couchbase/goxdcr/v8/service_def"
)

// PipelineMgrForSerializer is an autogenerated mock type for the PipelineMgrForSerializer type
type PipelineMgrForSerializer struct {
	mock.Mock
}

type PipelineMgrForSerializer_Expecter struct {
	mock *mock.Mock
}

func (_m *PipelineMgrForSerializer) EXPECT() *PipelineMgrForSerializer_Expecter {
	return &PipelineMgrForSerializer_Expecter{mock: &_m.Mock}
}

// AutoPauseReplication provides a mock function with given fields: topic
func (_m *PipelineMgrForSerializer) AutoPauseReplication(topic string) error {
	ret := _m.Called(topic)

	if len(ret) == 0 {
		panic("no return value specified for AutoPauseReplication")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrForSerializer_AutoPauseReplication_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AutoPauseReplication'
type PipelineMgrForSerializer_AutoPauseReplication_Call struct {
	*mock.Call
}

// AutoPauseReplication is a helper method to define mock.On call
//   - topic string
func (_e *PipelineMgrForSerializer_Expecter) AutoPauseReplication(topic interface{}) *PipelineMgrForSerializer_AutoPauseReplication_Call {
	return &PipelineMgrForSerializer_AutoPauseReplication_Call{Call: _e.mock.On("AutoPauseReplication", topic)}
}

func (_c *PipelineMgrForSerializer_AutoPauseReplication_Call) Run(run func(topic string)) *PipelineMgrForSerializer_AutoPauseReplication_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_AutoPauseReplication_Call) Return(_a0 error) *PipelineMgrForSerializer_AutoPauseReplication_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_AutoPauseReplication_Call) RunAndReturn(run func(string) error) *PipelineMgrForSerializer_AutoPauseReplication_Call {
	_c.Call.Return(run)
	return _c
}

// BackfillMappingUpdate provides a mock function with given fields: topic, diffPair, srcManifestsDelta
func (_m *PipelineMgrForSerializer) BackfillMappingUpdate(topic string, diffPair *metadata.CollectionNamespaceMappingsDiffPair, srcManifestsDelta []*metadata.CollectionsManifest) error {
	ret := _m.Called(topic, diffPair, srcManifestsDelta)

	if len(ret) == 0 {
		panic("no return value specified for BackfillMappingUpdate")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, *metadata.CollectionNamespaceMappingsDiffPair, []*metadata.CollectionsManifest) error); ok {
		r0 = rf(topic, diffPair, srcManifestsDelta)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrForSerializer_BackfillMappingUpdate_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'BackfillMappingUpdate'
type PipelineMgrForSerializer_BackfillMappingUpdate_Call struct {
	*mock.Call
}

// BackfillMappingUpdate is a helper method to define mock.On call
//   - topic string
//   - diffPair *metadata.CollectionNamespaceMappingsDiffPair
//   - srcManifestsDelta []*metadata.CollectionsManifest
func (_e *PipelineMgrForSerializer_Expecter) BackfillMappingUpdate(topic interface{}, diffPair interface{}, srcManifestsDelta interface{}) *PipelineMgrForSerializer_BackfillMappingUpdate_Call {
	return &PipelineMgrForSerializer_BackfillMappingUpdate_Call{Call: _e.mock.On("BackfillMappingUpdate", topic, diffPair, srcManifestsDelta)}
}

func (_c *PipelineMgrForSerializer_BackfillMappingUpdate_Call) Run(run func(topic string, diffPair *metadata.CollectionNamespaceMappingsDiffPair, srcManifestsDelta []*metadata.CollectionsManifest)) *PipelineMgrForSerializer_BackfillMappingUpdate_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(*metadata.CollectionNamespaceMappingsDiffPair), args[2].([]*metadata.CollectionsManifest))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_BackfillMappingUpdate_Call) Return(_a0 error) *PipelineMgrForSerializer_BackfillMappingUpdate_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_BackfillMappingUpdate_Call) RunAndReturn(run func(string, *metadata.CollectionNamespaceMappingsDiffPair, []*metadata.CollectionsManifest) error) *PipelineMgrForSerializer_BackfillMappingUpdate_Call {
	_c.Call.Return(run)
	return _c
}

// CleanupBackfillPipeline provides a mock function with given fields: topic
func (_m *PipelineMgrForSerializer) CleanupBackfillPipeline(topic string) error {
	ret := _m.Called(topic)

	if len(ret) == 0 {
		panic("no return value specified for CleanupBackfillPipeline")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrForSerializer_CleanupBackfillPipeline_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CleanupBackfillPipeline'
type PipelineMgrForSerializer_CleanupBackfillPipeline_Call struct {
	*mock.Call
}

// CleanupBackfillPipeline is a helper method to define mock.On call
//   - topic string
func (_e *PipelineMgrForSerializer_Expecter) CleanupBackfillPipeline(topic interface{}) *PipelineMgrForSerializer_CleanupBackfillPipeline_Call {
	return &PipelineMgrForSerializer_CleanupBackfillPipeline_Call{Call: _e.mock.On("CleanupBackfillPipeline", topic)}
}

func (_c *PipelineMgrForSerializer_CleanupBackfillPipeline_Call) Run(run func(topic string)) *PipelineMgrForSerializer_CleanupBackfillPipeline_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_CleanupBackfillPipeline_Call) Return(_a0 error) *PipelineMgrForSerializer_CleanupBackfillPipeline_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_CleanupBackfillPipeline_Call) RunAndReturn(run func(string) error) *PipelineMgrForSerializer_CleanupBackfillPipeline_Call {
	_c.Call.Return(run)
	return _c
}

// CleanupPipeline provides a mock function with given fields: topic
func (_m *PipelineMgrForSerializer) CleanupPipeline(topic string) error {
	ret := _m.Called(topic)

	if len(ret) == 0 {
		panic("no return value specified for CleanupPipeline")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrForSerializer_CleanupPipeline_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CleanupPipeline'
type PipelineMgrForSerializer_CleanupPipeline_Call struct {
	*mock.Call
}

// CleanupPipeline is a helper method to define mock.On call
//   - topic string
func (_e *PipelineMgrForSerializer_Expecter) CleanupPipeline(topic interface{}) *PipelineMgrForSerializer_CleanupPipeline_Call {
	return &PipelineMgrForSerializer_CleanupPipeline_Call{Call: _e.mock.On("CleanupPipeline", topic)}
}

func (_c *PipelineMgrForSerializer_CleanupPipeline_Call) Run(run func(topic string)) *PipelineMgrForSerializer_CleanupPipeline_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_CleanupPipeline_Call) Return(_a0 error) *PipelineMgrForSerializer_CleanupPipeline_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_CleanupPipeline_Call) RunAndReturn(run func(string) error) *PipelineMgrForSerializer_CleanupPipeline_Call {
	_c.Call.Return(run)
	return _c
}

// DismissEvent provides a mock function with given fields: eventId
func (_m *PipelineMgrForSerializer) DismissEvent(eventId int) error {
	ret := _m.Called(eventId)

	if len(ret) == 0 {
		panic("no return value specified for DismissEvent")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(int) error); ok {
		r0 = rf(eventId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrForSerializer_DismissEvent_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DismissEvent'
type PipelineMgrForSerializer_DismissEvent_Call struct {
	*mock.Call
}

// DismissEvent is a helper method to define mock.On call
//   - eventId int
func (_e *PipelineMgrForSerializer_Expecter) DismissEvent(eventId interface{}) *PipelineMgrForSerializer_DismissEvent_Call {
	return &PipelineMgrForSerializer_DismissEvent_Call{Call: _e.mock.On("DismissEvent", eventId)}
}

func (_c *PipelineMgrForSerializer_DismissEvent_Call) Run(run func(eventId int)) *PipelineMgrForSerializer_DismissEvent_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(int))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_DismissEvent_Call) Return(_a0 error) *PipelineMgrForSerializer_DismissEvent_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_DismissEvent_Call) RunAndReturn(run func(int) error) *PipelineMgrForSerializer_DismissEvent_Call {
	_c.Call.Return(run)
	return _c
}

// ForceTargetRefreshManifest provides a mock function with given fields: spec
func (_m *PipelineMgrForSerializer) ForceTargetRefreshManifest(spec *metadata.ReplicationSpecification) error {
	ret := _m.Called(spec)

	if len(ret) == 0 {
		panic("no return value specified for ForceTargetRefreshManifest")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification) error); ok {
		r0 = rf(spec)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrForSerializer_ForceTargetRefreshManifest_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ForceTargetRefreshManifest'
type PipelineMgrForSerializer_ForceTargetRefreshManifest_Call struct {
	*mock.Call
}

// ForceTargetRefreshManifest is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
func (_e *PipelineMgrForSerializer_Expecter) ForceTargetRefreshManifest(spec interface{}) *PipelineMgrForSerializer_ForceTargetRefreshManifest_Call {
	return &PipelineMgrForSerializer_ForceTargetRefreshManifest_Call{Call: _e.mock.On("ForceTargetRefreshManifest", spec)}
}

func (_c *PipelineMgrForSerializer_ForceTargetRefreshManifest_Call) Run(run func(spec *metadata.ReplicationSpecification)) *PipelineMgrForSerializer_ForceTargetRefreshManifest_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_ForceTargetRefreshManifest_Call) Return(_a0 error) *PipelineMgrForSerializer_ForceTargetRefreshManifest_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_ForceTargetRefreshManifest_Call) RunAndReturn(run func(*metadata.ReplicationSpecification) error) *PipelineMgrForSerializer_ForceTargetRefreshManifest_Call {
	_c.Call.Return(run)
	return _c
}

// GetLastUpdateResult provides a mock function with given fields: topic
func (_m *PipelineMgrForSerializer) GetLastUpdateResult(topic string) bool {
	ret := _m.Called(topic)

	if len(ret) == 0 {
		panic("no return value specified for GetLastUpdateResult")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func(string) bool); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// PipelineMgrForSerializer_GetLastUpdateResult_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetLastUpdateResult'
type PipelineMgrForSerializer_GetLastUpdateResult_Call struct {
	*mock.Call
}

// GetLastUpdateResult is a helper method to define mock.On call
//   - topic string
func (_e *PipelineMgrForSerializer_Expecter) GetLastUpdateResult(topic interface{}) *PipelineMgrForSerializer_GetLastUpdateResult_Call {
	return &PipelineMgrForSerializer_GetLastUpdateResult_Call{Call: _e.mock.On("GetLastUpdateResult", topic)}
}

func (_c *PipelineMgrForSerializer_GetLastUpdateResult_Call) Run(run func(topic string)) *PipelineMgrForSerializer_GetLastUpdateResult_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_GetLastUpdateResult_Call) Return(_a0 bool) *PipelineMgrForSerializer_GetLastUpdateResult_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_GetLastUpdateResult_Call) RunAndReturn(run func(string) bool) *PipelineMgrForSerializer_GetLastUpdateResult_Call {
	_c.Call.Return(run)
	return _c
}

// GetLogSvc provides a mock function with given fields:
func (_m *PipelineMgrForSerializer) GetLogSvc() service_def.UILogSvc {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetLogSvc")
	}

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

// PipelineMgrForSerializer_GetLogSvc_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetLogSvc'
type PipelineMgrForSerializer_GetLogSvc_Call struct {
	*mock.Call
}

// GetLogSvc is a helper method to define mock.On call
func (_e *PipelineMgrForSerializer_Expecter) GetLogSvc() *PipelineMgrForSerializer_GetLogSvc_Call {
	return &PipelineMgrForSerializer_GetLogSvc_Call{Call: _e.mock.On("GetLogSvc")}
}

func (_c *PipelineMgrForSerializer_GetLogSvc_Call) Run(run func()) *PipelineMgrForSerializer_GetLogSvc_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineMgrForSerializer_GetLogSvc_Call) Return(_a0 service_def.UILogSvc) *PipelineMgrForSerializer_GetLogSvc_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_GetLogSvc_Call) RunAndReturn(run func() service_def.UILogSvc) *PipelineMgrForSerializer_GetLogSvc_Call {
	_c.Call.Return(run)
	return _c
}

// GetOrCreateReplicationStatus provides a mock function with given fields: topic, cur_err
func (_m *PipelineMgrForSerializer) GetOrCreateReplicationStatus(topic string, cur_err error) (*pipeline.ReplicationStatus, error) {
	ret := _m.Called(topic, cur_err)

	if len(ret) == 0 {
		panic("no return value specified for GetOrCreateReplicationStatus")
	}

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

// PipelineMgrForSerializer_GetOrCreateReplicationStatus_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetOrCreateReplicationStatus'
type PipelineMgrForSerializer_GetOrCreateReplicationStatus_Call struct {
	*mock.Call
}

// GetOrCreateReplicationStatus is a helper method to define mock.On call
//   - topic string
//   - cur_err error
func (_e *PipelineMgrForSerializer_Expecter) GetOrCreateReplicationStatus(topic interface{}, cur_err interface{}) *PipelineMgrForSerializer_GetOrCreateReplicationStatus_Call {
	return &PipelineMgrForSerializer_GetOrCreateReplicationStatus_Call{Call: _e.mock.On("GetOrCreateReplicationStatus", topic, cur_err)}
}

func (_c *PipelineMgrForSerializer_GetOrCreateReplicationStatus_Call) Run(run func(topic string, cur_err error)) *PipelineMgrForSerializer_GetOrCreateReplicationStatus_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(error))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_GetOrCreateReplicationStatus_Call) Return(_a0 *pipeline.ReplicationStatus, _a1 error) *PipelineMgrForSerializer_GetOrCreateReplicationStatus_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *PipelineMgrForSerializer_GetOrCreateReplicationStatus_Call) RunAndReturn(run func(string, error) (*pipeline.ReplicationStatus, error)) *PipelineMgrForSerializer_GetOrCreateReplicationStatus_Call {
	_c.Call.Return(run)
	return _c
}

// GetRemoteClusterSvc provides a mock function with given fields:
func (_m *PipelineMgrForSerializer) GetRemoteClusterSvc() service_def.RemoteClusterSvc {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetRemoteClusterSvc")
	}

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

// PipelineMgrForSerializer_GetRemoteClusterSvc_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetRemoteClusterSvc'
type PipelineMgrForSerializer_GetRemoteClusterSvc_Call struct {
	*mock.Call
}

// GetRemoteClusterSvc is a helper method to define mock.On call
func (_e *PipelineMgrForSerializer_Expecter) GetRemoteClusterSvc() *PipelineMgrForSerializer_GetRemoteClusterSvc_Call {
	return &PipelineMgrForSerializer_GetRemoteClusterSvc_Call{Call: _e.mock.On("GetRemoteClusterSvc")}
}

func (_c *PipelineMgrForSerializer_GetRemoteClusterSvc_Call) Run(run func()) *PipelineMgrForSerializer_GetRemoteClusterSvc_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineMgrForSerializer_GetRemoteClusterSvc_Call) Return(_a0 service_def.RemoteClusterSvc) *PipelineMgrForSerializer_GetRemoteClusterSvc_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_GetRemoteClusterSvc_Call) RunAndReturn(run func() service_def.RemoteClusterSvc) *PipelineMgrForSerializer_GetRemoteClusterSvc_Call {
	_c.Call.Return(run)
	return _c
}

// GetReplSpecSvc provides a mock function with given fields:
func (_m *PipelineMgrForSerializer) GetReplSpecSvc() service_def.ReplicationSpecSvc {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetReplSpecSvc")
	}

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

// PipelineMgrForSerializer_GetReplSpecSvc_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetReplSpecSvc'
type PipelineMgrForSerializer_GetReplSpecSvc_Call struct {
	*mock.Call
}

// GetReplSpecSvc is a helper method to define mock.On call
func (_e *PipelineMgrForSerializer_Expecter) GetReplSpecSvc() *PipelineMgrForSerializer_GetReplSpecSvc_Call {
	return &PipelineMgrForSerializer_GetReplSpecSvc_Call{Call: _e.mock.On("GetReplSpecSvc")}
}

func (_c *PipelineMgrForSerializer_GetReplSpecSvc_Call) Run(run func()) *PipelineMgrForSerializer_GetReplSpecSvc_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineMgrForSerializer_GetReplSpecSvc_Call) Return(_a0 service_def.ReplicationSpecSvc) *PipelineMgrForSerializer_GetReplSpecSvc_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_GetReplSpecSvc_Call) RunAndReturn(run func() service_def.ReplicationSpecSvc) *PipelineMgrForSerializer_GetReplSpecSvc_Call {
	_c.Call.Return(run)
	return _c
}

// GetXDCRTopologySvc provides a mock function with given fields:
func (_m *PipelineMgrForSerializer) GetXDCRTopologySvc() service_def.XDCRCompTopologySvc {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetXDCRTopologySvc")
	}

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

// PipelineMgrForSerializer_GetXDCRTopologySvc_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetXDCRTopologySvc'
type PipelineMgrForSerializer_GetXDCRTopologySvc_Call struct {
	*mock.Call
}

// GetXDCRTopologySvc is a helper method to define mock.On call
func (_e *PipelineMgrForSerializer_Expecter) GetXDCRTopologySvc() *PipelineMgrForSerializer_GetXDCRTopologySvc_Call {
	return &PipelineMgrForSerializer_GetXDCRTopologySvc_Call{Call: _e.mock.On("GetXDCRTopologySvc")}
}

func (_c *PipelineMgrForSerializer_GetXDCRTopologySvc_Call) Run(run func()) *PipelineMgrForSerializer_GetXDCRTopologySvc_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineMgrForSerializer_GetXDCRTopologySvc_Call) Return(_a0 service_def.XDCRCompTopologySvc) *PipelineMgrForSerializer_GetXDCRTopologySvc_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_GetXDCRTopologySvc_Call) RunAndReturn(run func() service_def.XDCRCompTopologySvc) *PipelineMgrForSerializer_GetXDCRTopologySvc_Call {
	_c.Call.Return(run)
	return _c
}

// PauseReplication provides a mock function with given fields: topic
func (_m *PipelineMgrForSerializer) PauseReplication(topic string) error {
	ret := _m.Called(topic)

	if len(ret) == 0 {
		panic("no return value specified for PauseReplication")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrForSerializer_PauseReplication_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'PauseReplication'
type PipelineMgrForSerializer_PauseReplication_Call struct {
	*mock.Call
}

// PauseReplication is a helper method to define mock.On call
//   - topic string
func (_e *PipelineMgrForSerializer_Expecter) PauseReplication(topic interface{}) *PipelineMgrForSerializer_PauseReplication_Call {
	return &PipelineMgrForSerializer_PauseReplication_Call{Call: _e.mock.On("PauseReplication", topic)}
}

func (_c *PipelineMgrForSerializer_PauseReplication_Call) Run(run func(topic string)) *PipelineMgrForSerializer_PauseReplication_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_PauseReplication_Call) Return(_a0 error) *PipelineMgrForSerializer_PauseReplication_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_PauseReplication_Call) RunAndReturn(run func(string) error) *PipelineMgrForSerializer_PauseReplication_Call {
	_c.Call.Return(run)
	return _c
}

// PostTopologyStatus provides a mock function with given fields:
func (_m *PipelineMgrForSerializer) PostTopologyStatus() {
	_m.Called()
}

// PipelineMgrForSerializer_PostTopologyStatus_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'PostTopologyStatus'
type PipelineMgrForSerializer_PostTopologyStatus_Call struct {
	*mock.Call
}

// PostTopologyStatus is a helper method to define mock.On call
func (_e *PipelineMgrForSerializer_Expecter) PostTopologyStatus() *PipelineMgrForSerializer_PostTopologyStatus_Call {
	return &PipelineMgrForSerializer_PostTopologyStatus_Call{Call: _e.mock.On("PostTopologyStatus")}
}

func (_c *PipelineMgrForSerializer_PostTopologyStatus_Call) Run(run func()) *PipelineMgrForSerializer_PostTopologyStatus_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineMgrForSerializer_PostTopologyStatus_Call) Return() *PipelineMgrForSerializer_PostTopologyStatus_Call {
	_c.Call.Return()
	return _c
}

func (_c *PipelineMgrForSerializer_PostTopologyStatus_Call) RunAndReturn(run func()) *PipelineMgrForSerializer_PostTopologyStatus_Call {
	_c.Call.Return(run)
	return _c
}

// RemoveReplicationStatus provides a mock function with given fields: topic
func (_m *PipelineMgrForSerializer) RemoveReplicationStatus(topic string) error {
	ret := _m.Called(topic)

	if len(ret) == 0 {
		panic("no return value specified for RemoveReplicationStatus")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrForSerializer_RemoveReplicationStatus_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RemoveReplicationStatus'
type PipelineMgrForSerializer_RemoveReplicationStatus_Call struct {
	*mock.Call
}

// RemoveReplicationStatus is a helper method to define mock.On call
//   - topic string
func (_e *PipelineMgrForSerializer_Expecter) RemoveReplicationStatus(topic interface{}) *PipelineMgrForSerializer_RemoveReplicationStatus_Call {
	return &PipelineMgrForSerializer_RemoveReplicationStatus_Call{Call: _e.mock.On("RemoveReplicationStatus", topic)}
}

func (_c *PipelineMgrForSerializer_RemoveReplicationStatus_Call) Run(run func(topic string)) *PipelineMgrForSerializer_RemoveReplicationStatus_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_RemoveReplicationStatus_Call) Return(_a0 error) *PipelineMgrForSerializer_RemoveReplicationStatus_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_RemoveReplicationStatus_Call) RunAndReturn(run func(string) error) *PipelineMgrForSerializer_RemoveReplicationStatus_Call {
	_c.Call.Return(run)
	return _c
}

// StartBackfill provides a mock function with given fields: topic
func (_m *PipelineMgrForSerializer) StartBackfill(topic string) error {
	ret := _m.Called(topic)

	if len(ret) == 0 {
		panic("no return value specified for StartBackfill")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrForSerializer_StartBackfill_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StartBackfill'
type PipelineMgrForSerializer_StartBackfill_Call struct {
	*mock.Call
}

// StartBackfill is a helper method to define mock.On call
//   - topic string
func (_e *PipelineMgrForSerializer_Expecter) StartBackfill(topic interface{}) *PipelineMgrForSerializer_StartBackfill_Call {
	return &PipelineMgrForSerializer_StartBackfill_Call{Call: _e.mock.On("StartBackfill", topic)}
}

func (_c *PipelineMgrForSerializer_StartBackfill_Call) Run(run func(topic string)) *PipelineMgrForSerializer_StartBackfill_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_StartBackfill_Call) Return(_a0 error) *PipelineMgrForSerializer_StartBackfill_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_StartBackfill_Call) RunAndReturn(run func(string) error) *PipelineMgrForSerializer_StartBackfill_Call {
	_c.Call.Return(run)
	return _c
}

// StartPipeline provides a mock function with given fields: topic
func (_m *PipelineMgrForSerializer) StartPipeline(topic string) base.ErrorMap {
	ret := _m.Called(topic)

	if len(ret) == 0 {
		panic("no return value specified for StartPipeline")
	}

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

// PipelineMgrForSerializer_StartPipeline_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StartPipeline'
type PipelineMgrForSerializer_StartPipeline_Call struct {
	*mock.Call
}

// StartPipeline is a helper method to define mock.On call
//   - topic string
func (_e *PipelineMgrForSerializer_Expecter) StartPipeline(topic interface{}) *PipelineMgrForSerializer_StartPipeline_Call {
	return &PipelineMgrForSerializer_StartPipeline_Call{Call: _e.mock.On("StartPipeline", topic)}
}

func (_c *PipelineMgrForSerializer_StartPipeline_Call) Run(run func(topic string)) *PipelineMgrForSerializer_StartPipeline_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_StartPipeline_Call) Return(_a0 base.ErrorMap) *PipelineMgrForSerializer_StartPipeline_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_StartPipeline_Call) RunAndReturn(run func(string) base.ErrorMap) *PipelineMgrForSerializer_StartPipeline_Call {
	_c.Call.Return(run)
	return _c
}

// StopAllUpdaters provides a mock function with given fields:
func (_m *PipelineMgrForSerializer) StopAllUpdaters() {
	_m.Called()
}

// PipelineMgrForSerializer_StopAllUpdaters_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StopAllUpdaters'
type PipelineMgrForSerializer_StopAllUpdaters_Call struct {
	*mock.Call
}

// StopAllUpdaters is a helper method to define mock.On call
func (_e *PipelineMgrForSerializer_Expecter) StopAllUpdaters() *PipelineMgrForSerializer_StopAllUpdaters_Call {
	return &PipelineMgrForSerializer_StopAllUpdaters_Call{Call: _e.mock.On("StopAllUpdaters")}
}

func (_c *PipelineMgrForSerializer_StopAllUpdaters_Call) Run(run func()) *PipelineMgrForSerializer_StopAllUpdaters_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineMgrForSerializer_StopAllUpdaters_Call) Return() *PipelineMgrForSerializer_StopAllUpdaters_Call {
	_c.Call.Return()
	return _c
}

func (_c *PipelineMgrForSerializer_StopAllUpdaters_Call) RunAndReturn(run func()) *PipelineMgrForSerializer_StopAllUpdaters_Call {
	_c.Call.Return(run)
	return _c
}

// StopBackfill provides a mock function with given fields: topic, skipCkpt
func (_m *PipelineMgrForSerializer) StopBackfill(topic string, skipCkpt bool) error {
	ret := _m.Called(topic, skipCkpt)

	if len(ret) == 0 {
		panic("no return value specified for StopBackfill")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, bool) error); ok {
		r0 = rf(topic, skipCkpt)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrForSerializer_StopBackfill_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StopBackfill'
type PipelineMgrForSerializer_StopBackfill_Call struct {
	*mock.Call
}

// StopBackfill is a helper method to define mock.On call
//   - topic string
//   - skipCkpt bool
func (_e *PipelineMgrForSerializer_Expecter) StopBackfill(topic interface{}, skipCkpt interface{}) *PipelineMgrForSerializer_StopBackfill_Call {
	return &PipelineMgrForSerializer_StopBackfill_Call{Call: _e.mock.On("StopBackfill", topic, skipCkpt)}
}

func (_c *PipelineMgrForSerializer_StopBackfill_Call) Run(run func(topic string, skipCkpt bool)) *PipelineMgrForSerializer_StopBackfill_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(bool))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_StopBackfill_Call) Return(_a0 error) *PipelineMgrForSerializer_StopBackfill_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_StopBackfill_Call) RunAndReturn(run func(string, bool) error) *PipelineMgrForSerializer_StopBackfill_Call {
	_c.Call.Return(run)
	return _c
}

// StopBackfillWithStoppedCb provides a mock function with given fields: topic, cb, errCb, skipCkpt
func (_m *PipelineMgrForSerializer) StopBackfillWithStoppedCb(topic string, cb base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback, skipCkpt bool) error {
	ret := _m.Called(topic, cb, errCb, skipCkpt)

	if len(ret) == 0 {
		panic("no return value specified for StopBackfillWithStoppedCb")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, base.StoppedPipelineCallback, base.StoppedPipelineErrCallback, bool) error); ok {
		r0 = rf(topic, cb, errCb, skipCkpt)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrForSerializer_StopBackfillWithStoppedCb_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StopBackfillWithStoppedCb'
type PipelineMgrForSerializer_StopBackfillWithStoppedCb_Call struct {
	*mock.Call
}

// StopBackfillWithStoppedCb is a helper method to define mock.On call
//   - topic string
//   - cb base.StoppedPipelineCallback
//   - errCb base.StoppedPipelineErrCallback
//   - skipCkpt bool
func (_e *PipelineMgrForSerializer_Expecter) StopBackfillWithStoppedCb(topic interface{}, cb interface{}, errCb interface{}, skipCkpt interface{}) *PipelineMgrForSerializer_StopBackfillWithStoppedCb_Call {
	return &PipelineMgrForSerializer_StopBackfillWithStoppedCb_Call{Call: _e.mock.On("StopBackfillWithStoppedCb", topic, cb, errCb, skipCkpt)}
}

func (_c *PipelineMgrForSerializer_StopBackfillWithStoppedCb_Call) Run(run func(topic string, cb base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback, skipCkpt bool)) *PipelineMgrForSerializer_StopBackfillWithStoppedCb_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(base.StoppedPipelineCallback), args[2].(base.StoppedPipelineErrCallback), args[3].(bool))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_StopBackfillWithStoppedCb_Call) Return(_a0 error) *PipelineMgrForSerializer_StopBackfillWithStoppedCb_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_StopBackfillWithStoppedCb_Call) RunAndReturn(run func(string, base.StoppedPipelineCallback, base.StoppedPipelineErrCallback, bool) error) *PipelineMgrForSerializer_StopBackfillWithStoppedCb_Call {
	_c.Call.Return(run)
	return _c
}

// StopPipeline provides a mock function with given fields: rep_status
func (_m *PipelineMgrForSerializer) StopPipeline(rep_status pipeline.ReplicationStatusIface) base.ErrorMap {
	ret := _m.Called(rep_status)

	if len(ret) == 0 {
		panic("no return value specified for StopPipeline")
	}

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

// PipelineMgrForSerializer_StopPipeline_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StopPipeline'
type PipelineMgrForSerializer_StopPipeline_Call struct {
	*mock.Call
}

// StopPipeline is a helper method to define mock.On call
//   - rep_status pipeline.ReplicationStatusIface
func (_e *PipelineMgrForSerializer_Expecter) StopPipeline(rep_status interface{}) *PipelineMgrForSerializer_StopPipeline_Call {
	return &PipelineMgrForSerializer_StopPipeline_Call{Call: _e.mock.On("StopPipeline", rep_status)}
}

func (_c *PipelineMgrForSerializer_StopPipeline_Call) Run(run func(rep_status pipeline.ReplicationStatusIface)) *PipelineMgrForSerializer_StopPipeline_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(pipeline.ReplicationStatusIface))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_StopPipeline_Call) Return(_a0 base.ErrorMap) *PipelineMgrForSerializer_StopPipeline_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_StopPipeline_Call) RunAndReturn(run func(pipeline.ReplicationStatusIface) base.ErrorMap) *PipelineMgrForSerializer_StopPipeline_Call {
	_c.Call.Return(run)
	return _c
}

// Update provides a mock function with given fields: topic, cur_err
func (_m *PipelineMgrForSerializer) Update(topic string, cur_err error) error {
	ret := _m.Called(topic, cur_err)

	if len(ret) == 0 {
		panic("no return value specified for Update")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, error) error); ok {
		r0 = rf(topic, cur_err)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrForSerializer_Update_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Update'
type PipelineMgrForSerializer_Update_Call struct {
	*mock.Call
}

// Update is a helper method to define mock.On call
//   - topic string
//   - cur_err error
func (_e *PipelineMgrForSerializer_Expecter) Update(topic interface{}, cur_err interface{}) *PipelineMgrForSerializer_Update_Call {
	return &PipelineMgrForSerializer_Update_Call{Call: _e.mock.On("Update", topic, cur_err)}
}

func (_c *PipelineMgrForSerializer_Update_Call) Run(run func(topic string, cur_err error)) *PipelineMgrForSerializer_Update_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(error))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_Update_Call) Return(_a0 error) *PipelineMgrForSerializer_Update_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_Update_Call) RunAndReturn(run func(string, error) error) *PipelineMgrForSerializer_Update_Call {
	_c.Call.Return(run)
	return _c
}

// UpdateWithStoppedCb provides a mock function with given fields: topic, callback, errCb
func (_m *PipelineMgrForSerializer) UpdateWithStoppedCb(topic string, callback base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback) error {
	ret := _m.Called(topic, callback, errCb)

	if len(ret) == 0 {
		panic("no return value specified for UpdateWithStoppedCb")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, base.StoppedPipelineCallback, base.StoppedPipelineErrCallback) error); ok {
		r0 = rf(topic, callback, errCb)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrForSerializer_UpdateWithStoppedCb_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UpdateWithStoppedCb'
type PipelineMgrForSerializer_UpdateWithStoppedCb_Call struct {
	*mock.Call
}

// UpdateWithStoppedCb is a helper method to define mock.On call
//   - topic string
//   - callback base.StoppedPipelineCallback
//   - errCb base.StoppedPipelineErrCallback
func (_e *PipelineMgrForSerializer_Expecter) UpdateWithStoppedCb(topic interface{}, callback interface{}, errCb interface{}) *PipelineMgrForSerializer_UpdateWithStoppedCb_Call {
	return &PipelineMgrForSerializer_UpdateWithStoppedCb_Call{Call: _e.mock.On("UpdateWithStoppedCb", topic, callback, errCb)}
}

func (_c *PipelineMgrForSerializer_UpdateWithStoppedCb_Call) Run(run func(topic string, callback base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback)) *PipelineMgrForSerializer_UpdateWithStoppedCb_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(base.StoppedPipelineCallback), args[2].(base.StoppedPipelineErrCallback))
	})
	return _c
}

func (_c *PipelineMgrForSerializer_UpdateWithStoppedCb_Call) Return(_a0 error) *PipelineMgrForSerializer_UpdateWithStoppedCb_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrForSerializer_UpdateWithStoppedCb_Call) RunAndReturn(run func(string, base.StoppedPipelineCallback, base.StoppedPipelineErrCallback) error) *PipelineMgrForSerializer_UpdateWithStoppedCb_Call {
	_c.Call.Return(run)
	return _c
}

// NewPipelineMgrForSerializer creates a new instance of PipelineMgrForSerializer. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewPipelineMgrForSerializer(t interface {
	mock.TestingT
	Cleanup(func())
}) *PipelineMgrForSerializer {
	mock := &PipelineMgrForSerializer{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
