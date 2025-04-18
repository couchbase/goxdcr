// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/v8/base"
	metadata "github.com/couchbase/goxdcr/v8/metadata"

	mock "github.com/stretchr/testify/mock"

	pipeline "github.com/couchbase/goxdcr/v8/pipeline"

	service_def "github.com/couchbase/goxdcr/v8/service_def"
)

// PipelineMgrInternalIface is an autogenerated mock type for the PipelineMgrInternalIface type
type PipelineMgrInternalIface struct {
	mock.Mock
}

type PipelineMgrInternalIface_Expecter struct {
	mock *mock.Mock
}

func (_m *PipelineMgrInternalIface) EXPECT() *PipelineMgrInternalIface_Expecter {
	return &PipelineMgrInternalIface_Expecter{mock: &_m.Mock}
}

// AutoPauseReplication provides a mock function with given fields: topic
func (_m *PipelineMgrInternalIface) AutoPauseReplication(topic string) error {
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

// PipelineMgrInternalIface_AutoPauseReplication_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AutoPauseReplication'
type PipelineMgrInternalIface_AutoPauseReplication_Call struct {
	*mock.Call
}

// AutoPauseReplication is a helper method to define mock.On call
//   - topic string
func (_e *PipelineMgrInternalIface_Expecter) AutoPauseReplication(topic interface{}) *PipelineMgrInternalIface_AutoPauseReplication_Call {
	return &PipelineMgrInternalIface_AutoPauseReplication_Call{Call: _e.mock.On("AutoPauseReplication", topic)}
}

func (_c *PipelineMgrInternalIface_AutoPauseReplication_Call) Run(run func(topic string)) *PipelineMgrInternalIface_AutoPauseReplication_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrInternalIface_AutoPauseReplication_Call) Return(_a0 error) *PipelineMgrInternalIface_AutoPauseReplication_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrInternalIface_AutoPauseReplication_Call) RunAndReturn(run func(string) error) *PipelineMgrInternalIface_AutoPauseReplication_Call {
	_c.Call.Return(run)
	return _c
}

// ForceTargetRefreshManifest provides a mock function with given fields: spec
func (_m *PipelineMgrInternalIface) ForceTargetRefreshManifest(spec *metadata.ReplicationSpecification) error {
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

// PipelineMgrInternalIface_ForceTargetRefreshManifest_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ForceTargetRefreshManifest'
type PipelineMgrInternalIface_ForceTargetRefreshManifest_Call struct {
	*mock.Call
}

// ForceTargetRefreshManifest is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
func (_e *PipelineMgrInternalIface_Expecter) ForceTargetRefreshManifest(spec interface{}) *PipelineMgrInternalIface_ForceTargetRefreshManifest_Call {
	return &PipelineMgrInternalIface_ForceTargetRefreshManifest_Call{Call: _e.mock.On("ForceTargetRefreshManifest", spec)}
}

func (_c *PipelineMgrInternalIface_ForceTargetRefreshManifest_Call) Run(run func(spec *metadata.ReplicationSpecification)) *PipelineMgrInternalIface_ForceTargetRefreshManifest_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification))
	})
	return _c
}

func (_c *PipelineMgrInternalIface_ForceTargetRefreshManifest_Call) Return(_a0 error) *PipelineMgrInternalIface_ForceTargetRefreshManifest_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrInternalIface_ForceTargetRefreshManifest_Call) RunAndReturn(run func(*metadata.ReplicationSpecification) error) *PipelineMgrInternalIface_ForceTargetRefreshManifest_Call {
	_c.Call.Return(run)
	return _c
}

// GetLastUpdateResult provides a mock function with given fields: topic
func (_m *PipelineMgrInternalIface) GetLastUpdateResult(topic string) bool {
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

// PipelineMgrInternalIface_GetLastUpdateResult_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetLastUpdateResult'
type PipelineMgrInternalIface_GetLastUpdateResult_Call struct {
	*mock.Call
}

// GetLastUpdateResult is a helper method to define mock.On call
//   - topic string
func (_e *PipelineMgrInternalIface_Expecter) GetLastUpdateResult(topic interface{}) *PipelineMgrInternalIface_GetLastUpdateResult_Call {
	return &PipelineMgrInternalIface_GetLastUpdateResult_Call{Call: _e.mock.On("GetLastUpdateResult", topic)}
}

func (_c *PipelineMgrInternalIface_GetLastUpdateResult_Call) Run(run func(topic string)) *PipelineMgrInternalIface_GetLastUpdateResult_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrInternalIface_GetLastUpdateResult_Call) Return(_a0 bool) *PipelineMgrInternalIface_GetLastUpdateResult_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrInternalIface_GetLastUpdateResult_Call) RunAndReturn(run func(string) bool) *PipelineMgrInternalIface_GetLastUpdateResult_Call {
	_c.Call.Return(run)
	return _c
}

// GetLogSvc provides a mock function with no fields
func (_m *PipelineMgrInternalIface) GetLogSvc() service_def.UILogSvc {
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

// PipelineMgrInternalIface_GetLogSvc_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetLogSvc'
type PipelineMgrInternalIface_GetLogSvc_Call struct {
	*mock.Call
}

// GetLogSvc is a helper method to define mock.On call
func (_e *PipelineMgrInternalIface_Expecter) GetLogSvc() *PipelineMgrInternalIface_GetLogSvc_Call {
	return &PipelineMgrInternalIface_GetLogSvc_Call{Call: _e.mock.On("GetLogSvc")}
}

func (_c *PipelineMgrInternalIface_GetLogSvc_Call) Run(run func()) *PipelineMgrInternalIface_GetLogSvc_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineMgrInternalIface_GetLogSvc_Call) Return(_a0 service_def.UILogSvc) *PipelineMgrInternalIface_GetLogSvc_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrInternalIface_GetLogSvc_Call) RunAndReturn(run func() service_def.UILogSvc) *PipelineMgrInternalIface_GetLogSvc_Call {
	_c.Call.Return(run)
	return _c
}

// GetOrCreateReplicationStatus provides a mock function with given fields: topic, cur_err
func (_m *PipelineMgrInternalIface) GetOrCreateReplicationStatus(topic string, cur_err error) (*pipeline.ReplicationStatus, error) {
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

// PipelineMgrInternalIface_GetOrCreateReplicationStatus_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetOrCreateReplicationStatus'
type PipelineMgrInternalIface_GetOrCreateReplicationStatus_Call struct {
	*mock.Call
}

// GetOrCreateReplicationStatus is a helper method to define mock.On call
//   - topic string
//   - cur_err error
func (_e *PipelineMgrInternalIface_Expecter) GetOrCreateReplicationStatus(topic interface{}, cur_err interface{}) *PipelineMgrInternalIface_GetOrCreateReplicationStatus_Call {
	return &PipelineMgrInternalIface_GetOrCreateReplicationStatus_Call{Call: _e.mock.On("GetOrCreateReplicationStatus", topic, cur_err)}
}

func (_c *PipelineMgrInternalIface_GetOrCreateReplicationStatus_Call) Run(run func(topic string, cur_err error)) *PipelineMgrInternalIface_GetOrCreateReplicationStatus_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(error))
	})
	return _c
}

func (_c *PipelineMgrInternalIface_GetOrCreateReplicationStatus_Call) Return(_a0 *pipeline.ReplicationStatus, _a1 error) *PipelineMgrInternalIface_GetOrCreateReplicationStatus_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *PipelineMgrInternalIface_GetOrCreateReplicationStatus_Call) RunAndReturn(run func(string, error) (*pipeline.ReplicationStatus, error)) *PipelineMgrInternalIface_GetOrCreateReplicationStatus_Call {
	_c.Call.Return(run)
	return _c
}

// GetRemoteClusterSvc provides a mock function with no fields
func (_m *PipelineMgrInternalIface) GetRemoteClusterSvc() service_def.RemoteClusterSvc {
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

// PipelineMgrInternalIface_GetRemoteClusterSvc_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetRemoteClusterSvc'
type PipelineMgrInternalIface_GetRemoteClusterSvc_Call struct {
	*mock.Call
}

// GetRemoteClusterSvc is a helper method to define mock.On call
func (_e *PipelineMgrInternalIface_Expecter) GetRemoteClusterSvc() *PipelineMgrInternalIface_GetRemoteClusterSvc_Call {
	return &PipelineMgrInternalIface_GetRemoteClusterSvc_Call{Call: _e.mock.On("GetRemoteClusterSvc")}
}

func (_c *PipelineMgrInternalIface_GetRemoteClusterSvc_Call) Run(run func()) *PipelineMgrInternalIface_GetRemoteClusterSvc_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineMgrInternalIface_GetRemoteClusterSvc_Call) Return(_a0 service_def.RemoteClusterSvc) *PipelineMgrInternalIface_GetRemoteClusterSvc_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrInternalIface_GetRemoteClusterSvc_Call) RunAndReturn(run func() service_def.RemoteClusterSvc) *PipelineMgrInternalIface_GetRemoteClusterSvc_Call {
	_c.Call.Return(run)
	return _c
}

// GetReplSpecSvc provides a mock function with no fields
func (_m *PipelineMgrInternalIface) GetReplSpecSvc() service_def.ReplicationSpecSvc {
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

// PipelineMgrInternalIface_GetReplSpecSvc_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetReplSpecSvc'
type PipelineMgrInternalIface_GetReplSpecSvc_Call struct {
	*mock.Call
}

// GetReplSpecSvc is a helper method to define mock.On call
func (_e *PipelineMgrInternalIface_Expecter) GetReplSpecSvc() *PipelineMgrInternalIface_GetReplSpecSvc_Call {
	return &PipelineMgrInternalIface_GetReplSpecSvc_Call{Call: _e.mock.On("GetReplSpecSvc")}
}

func (_c *PipelineMgrInternalIface_GetReplSpecSvc_Call) Run(run func()) *PipelineMgrInternalIface_GetReplSpecSvc_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineMgrInternalIface_GetReplSpecSvc_Call) Return(_a0 service_def.ReplicationSpecSvc) *PipelineMgrInternalIface_GetReplSpecSvc_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrInternalIface_GetReplSpecSvc_Call) RunAndReturn(run func() service_def.ReplicationSpecSvc) *PipelineMgrInternalIface_GetReplSpecSvc_Call {
	_c.Call.Return(run)
	return _c
}

// GetXDCRTopologySvc provides a mock function with no fields
func (_m *PipelineMgrInternalIface) GetXDCRTopologySvc() service_def.XDCRCompTopologySvc {
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

// PipelineMgrInternalIface_GetXDCRTopologySvc_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetXDCRTopologySvc'
type PipelineMgrInternalIface_GetXDCRTopologySvc_Call struct {
	*mock.Call
}

// GetXDCRTopologySvc is a helper method to define mock.On call
func (_e *PipelineMgrInternalIface_Expecter) GetXDCRTopologySvc() *PipelineMgrInternalIface_GetXDCRTopologySvc_Call {
	return &PipelineMgrInternalIface_GetXDCRTopologySvc_Call{Call: _e.mock.On("GetXDCRTopologySvc")}
}

func (_c *PipelineMgrInternalIface_GetXDCRTopologySvc_Call) Run(run func()) *PipelineMgrInternalIface_GetXDCRTopologySvc_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineMgrInternalIface_GetXDCRTopologySvc_Call) Return(_a0 service_def.XDCRCompTopologySvc) *PipelineMgrInternalIface_GetXDCRTopologySvc_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrInternalIface_GetXDCRTopologySvc_Call) RunAndReturn(run func() service_def.XDCRCompTopologySvc) *PipelineMgrInternalIface_GetXDCRTopologySvc_Call {
	_c.Call.Return(run)
	return _c
}

// PostTopologyStatus provides a mock function with no fields
func (_m *PipelineMgrInternalIface) PostTopologyStatus() {
	_m.Called()
}

// PipelineMgrInternalIface_PostTopologyStatus_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'PostTopologyStatus'
type PipelineMgrInternalIface_PostTopologyStatus_Call struct {
	*mock.Call
}

// PostTopologyStatus is a helper method to define mock.On call
func (_e *PipelineMgrInternalIface_Expecter) PostTopologyStatus() *PipelineMgrInternalIface_PostTopologyStatus_Call {
	return &PipelineMgrInternalIface_PostTopologyStatus_Call{Call: _e.mock.On("PostTopologyStatus")}
}

func (_c *PipelineMgrInternalIface_PostTopologyStatus_Call) Run(run func()) *PipelineMgrInternalIface_PostTopologyStatus_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineMgrInternalIface_PostTopologyStatus_Call) Return() *PipelineMgrInternalIface_PostTopologyStatus_Call {
	_c.Call.Return()
	return _c
}

func (_c *PipelineMgrInternalIface_PostTopologyStatus_Call) RunAndReturn(run func()) *PipelineMgrInternalIface_PostTopologyStatus_Call {
	_c.Run(run)
	return _c
}

// RemoveReplicationStatus provides a mock function with given fields: topic
func (_m *PipelineMgrInternalIface) RemoveReplicationStatus(topic string) error {
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

// PipelineMgrInternalIface_RemoveReplicationStatus_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RemoveReplicationStatus'
type PipelineMgrInternalIface_RemoveReplicationStatus_Call struct {
	*mock.Call
}

// RemoveReplicationStatus is a helper method to define mock.On call
//   - topic string
func (_e *PipelineMgrInternalIface_Expecter) RemoveReplicationStatus(topic interface{}) *PipelineMgrInternalIface_RemoveReplicationStatus_Call {
	return &PipelineMgrInternalIface_RemoveReplicationStatus_Call{Call: _e.mock.On("RemoveReplicationStatus", topic)}
}

func (_c *PipelineMgrInternalIface_RemoveReplicationStatus_Call) Run(run func(topic string)) *PipelineMgrInternalIface_RemoveReplicationStatus_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrInternalIface_RemoveReplicationStatus_Call) Return(_a0 error) *PipelineMgrInternalIface_RemoveReplicationStatus_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrInternalIface_RemoveReplicationStatus_Call) RunAndReturn(run func(string) error) *PipelineMgrInternalIface_RemoveReplicationStatus_Call {
	_c.Call.Return(run)
	return _c
}

// StartPipeline provides a mock function with given fields: topic
func (_m *PipelineMgrInternalIface) StartPipeline(topic string) base.ErrorMap {
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

// PipelineMgrInternalIface_StartPipeline_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StartPipeline'
type PipelineMgrInternalIface_StartPipeline_Call struct {
	*mock.Call
}

// StartPipeline is a helper method to define mock.On call
//   - topic string
func (_e *PipelineMgrInternalIface_Expecter) StartPipeline(topic interface{}) *PipelineMgrInternalIface_StartPipeline_Call {
	return &PipelineMgrInternalIface_StartPipeline_Call{Call: _e.mock.On("StartPipeline", topic)}
}

func (_c *PipelineMgrInternalIface_StartPipeline_Call) Run(run func(topic string)) *PipelineMgrInternalIface_StartPipeline_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrInternalIface_StartPipeline_Call) Return(_a0 base.ErrorMap) *PipelineMgrInternalIface_StartPipeline_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrInternalIface_StartPipeline_Call) RunAndReturn(run func(string) base.ErrorMap) *PipelineMgrInternalIface_StartPipeline_Call {
	_c.Call.Return(run)
	return _c
}

// StopAllUpdaters provides a mock function with no fields
func (_m *PipelineMgrInternalIface) StopAllUpdaters() {
	_m.Called()
}

// PipelineMgrInternalIface_StopAllUpdaters_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StopAllUpdaters'
type PipelineMgrInternalIface_StopAllUpdaters_Call struct {
	*mock.Call
}

// StopAllUpdaters is a helper method to define mock.On call
func (_e *PipelineMgrInternalIface_Expecter) StopAllUpdaters() *PipelineMgrInternalIface_StopAllUpdaters_Call {
	return &PipelineMgrInternalIface_StopAllUpdaters_Call{Call: _e.mock.On("StopAllUpdaters")}
}

func (_c *PipelineMgrInternalIface_StopAllUpdaters_Call) Run(run func()) *PipelineMgrInternalIface_StopAllUpdaters_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineMgrInternalIface_StopAllUpdaters_Call) Return() *PipelineMgrInternalIface_StopAllUpdaters_Call {
	_c.Call.Return()
	return _c
}

func (_c *PipelineMgrInternalIface_StopAllUpdaters_Call) RunAndReturn(run func()) *PipelineMgrInternalIface_StopAllUpdaters_Call {
	_c.Run(run)
	return _c
}

// StopPipeline provides a mock function with given fields: rep_status
func (_m *PipelineMgrInternalIface) StopPipeline(rep_status pipeline.ReplicationStatusIface) base.ErrorMap {
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

// PipelineMgrInternalIface_StopPipeline_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StopPipeline'
type PipelineMgrInternalIface_StopPipeline_Call struct {
	*mock.Call
}

// StopPipeline is a helper method to define mock.On call
//   - rep_status pipeline.ReplicationStatusIface
func (_e *PipelineMgrInternalIface_Expecter) StopPipeline(rep_status interface{}) *PipelineMgrInternalIface_StopPipeline_Call {
	return &PipelineMgrInternalIface_StopPipeline_Call{Call: _e.mock.On("StopPipeline", rep_status)}
}

func (_c *PipelineMgrInternalIface_StopPipeline_Call) Run(run func(rep_status pipeline.ReplicationStatusIface)) *PipelineMgrInternalIface_StopPipeline_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(pipeline.ReplicationStatusIface))
	})
	return _c
}

func (_c *PipelineMgrInternalIface_StopPipeline_Call) Return(_a0 base.ErrorMap) *PipelineMgrInternalIface_StopPipeline_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrInternalIface_StopPipeline_Call) RunAndReturn(run func(pipeline.ReplicationStatusIface) base.ErrorMap) *PipelineMgrInternalIface_StopPipeline_Call {
	_c.Call.Return(run)
	return _c
}

// Update provides a mock function with given fields: topic, cur_err
func (_m *PipelineMgrInternalIface) Update(topic string, cur_err error) error {
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

// PipelineMgrInternalIface_Update_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Update'
type PipelineMgrInternalIface_Update_Call struct {
	*mock.Call
}

// Update is a helper method to define mock.On call
//   - topic string
//   - cur_err error
func (_e *PipelineMgrInternalIface_Expecter) Update(topic interface{}, cur_err interface{}) *PipelineMgrInternalIface_Update_Call {
	return &PipelineMgrInternalIface_Update_Call{Call: _e.mock.On("Update", topic, cur_err)}
}

func (_c *PipelineMgrInternalIface_Update_Call) Run(run func(topic string, cur_err error)) *PipelineMgrInternalIface_Update_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(error))
	})
	return _c
}

func (_c *PipelineMgrInternalIface_Update_Call) Return(_a0 error) *PipelineMgrInternalIface_Update_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrInternalIface_Update_Call) RunAndReturn(run func(string, error) error) *PipelineMgrInternalIface_Update_Call {
	_c.Call.Return(run)
	return _c
}

// NewPipelineMgrInternalIface creates a new instance of PipelineMgrInternalIface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewPipelineMgrInternalIface(t interface {
	mock.TestingT
	Cleanup(func())
}) *PipelineMgrInternalIface {
	mock := &PipelineMgrInternalIface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
