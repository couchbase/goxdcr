// Code generated by mockery v2.43.2. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"

	pipeline "github.com/couchbase/goxdcr/pipeline"

	service_def "github.com/couchbase/goxdcr/service_def"
)

// PipelineMgrIface is an autogenerated mock type for the PipelineMgrIface type
type PipelineMgrIface struct {
	mock.Mock
}

type PipelineMgrIface_Expecter struct {
	mock *mock.Mock
}

func (_m *PipelineMgrIface) EXPECT() *PipelineMgrIface_Expecter {
	return &PipelineMgrIface_Expecter{mock: &_m.Mock}
}

// AllReplicationSpecsForTargetCluster provides a mock function with given fields: targetClusterUuid
func (_m *PipelineMgrIface) AllReplicationSpecsForTargetCluster(targetClusterUuid string) map[string]*metadata.ReplicationSpecification {
	ret := _m.Called(targetClusterUuid)

	if len(ret) == 0 {
		panic("no return value specified for AllReplicationSpecsForTargetCluster")
	}

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

// PipelineMgrIface_AllReplicationSpecsForTargetCluster_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AllReplicationSpecsForTargetCluster'
type PipelineMgrIface_AllReplicationSpecsForTargetCluster_Call struct {
	*mock.Call
}

// AllReplicationSpecsForTargetCluster is a helper method to define mock.On call
//   - targetClusterUuid string
func (_e *PipelineMgrIface_Expecter) AllReplicationSpecsForTargetCluster(targetClusterUuid interface{}) *PipelineMgrIface_AllReplicationSpecsForTargetCluster_Call {
	return &PipelineMgrIface_AllReplicationSpecsForTargetCluster_Call{Call: _e.mock.On("AllReplicationSpecsForTargetCluster", targetClusterUuid)}
}

func (_c *PipelineMgrIface_AllReplicationSpecsForTargetCluster_Call) Run(run func(targetClusterUuid string)) *PipelineMgrIface_AllReplicationSpecsForTargetCluster_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrIface_AllReplicationSpecsForTargetCluster_Call) Return(_a0 map[string]*metadata.ReplicationSpecification) *PipelineMgrIface_AllReplicationSpecsForTargetCluster_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrIface_AllReplicationSpecsForTargetCluster_Call) RunAndReturn(run func(string) map[string]*metadata.ReplicationSpecification) *PipelineMgrIface_AllReplicationSpecsForTargetCluster_Call {
	_c.Call.Return(run)
	return _c
}

// AllReplications provides a mock function with given fields:
func (_m *PipelineMgrIface) AllReplications() []string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for AllReplications")
	}

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

// PipelineMgrIface_AllReplications_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AllReplications'
type PipelineMgrIface_AllReplications_Call struct {
	*mock.Call
}

// AllReplications is a helper method to define mock.On call
func (_e *PipelineMgrIface_Expecter) AllReplications() *PipelineMgrIface_AllReplications_Call {
	return &PipelineMgrIface_AllReplications_Call{Call: _e.mock.On("AllReplications")}
}

func (_c *PipelineMgrIface_AllReplications_Call) Run(run func()) *PipelineMgrIface_AllReplications_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineMgrIface_AllReplications_Call) Return(_a0 []string) *PipelineMgrIface_AllReplications_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrIface_AllReplications_Call) RunAndReturn(run func() []string) *PipelineMgrIface_AllReplications_Call {
	_c.Call.Return(run)
	return _c
}

// AllReplicationsForBucket provides a mock function with given fields: bucket
func (_m *PipelineMgrIface) AllReplicationsForBucket(bucket string) []string {
	ret := _m.Called(bucket)

	if len(ret) == 0 {
		panic("no return value specified for AllReplicationsForBucket")
	}

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

// PipelineMgrIface_AllReplicationsForBucket_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AllReplicationsForBucket'
type PipelineMgrIface_AllReplicationsForBucket_Call struct {
	*mock.Call
}

// AllReplicationsForBucket is a helper method to define mock.On call
//   - bucket string
func (_e *PipelineMgrIface_Expecter) AllReplicationsForBucket(bucket interface{}) *PipelineMgrIface_AllReplicationsForBucket_Call {
	return &PipelineMgrIface_AllReplicationsForBucket_Call{Call: _e.mock.On("AllReplicationsForBucket", bucket)}
}

func (_c *PipelineMgrIface_AllReplicationsForBucket_Call) Run(run func(bucket string)) *PipelineMgrIface_AllReplicationsForBucket_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrIface_AllReplicationsForBucket_Call) Return(_a0 []string) *PipelineMgrIface_AllReplicationsForBucket_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrIface_AllReplicationsForBucket_Call) RunAndReturn(run func(string) []string) *PipelineMgrIface_AllReplicationsForBucket_Call {
	_c.Call.Return(run)
	return _c
}

// AllReplicationsForTargetCluster provides a mock function with given fields: targetClusterUuid
func (_m *PipelineMgrIface) AllReplicationsForTargetCluster(targetClusterUuid string) []string {
	ret := _m.Called(targetClusterUuid)

	if len(ret) == 0 {
		panic("no return value specified for AllReplicationsForTargetCluster")
	}

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

// PipelineMgrIface_AllReplicationsForTargetCluster_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AllReplicationsForTargetCluster'
type PipelineMgrIface_AllReplicationsForTargetCluster_Call struct {
	*mock.Call
}

// AllReplicationsForTargetCluster is a helper method to define mock.On call
//   - targetClusterUuid string
func (_e *PipelineMgrIface_Expecter) AllReplicationsForTargetCluster(targetClusterUuid interface{}) *PipelineMgrIface_AllReplicationsForTargetCluster_Call {
	return &PipelineMgrIface_AllReplicationsForTargetCluster_Call{Call: _e.mock.On("AllReplicationsForTargetCluster", targetClusterUuid)}
}

func (_c *PipelineMgrIface_AllReplicationsForTargetCluster_Call) Run(run func(targetClusterUuid string)) *PipelineMgrIface_AllReplicationsForTargetCluster_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrIface_AllReplicationsForTargetCluster_Call) Return(_a0 []string) *PipelineMgrIface_AllReplicationsForTargetCluster_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrIface_AllReplicationsForTargetCluster_Call) RunAndReturn(run func(string) []string) *PipelineMgrIface_AllReplicationsForTargetCluster_Call {
	_c.Call.Return(run)
	return _c
}

// CheckPipelines provides a mock function with given fields:
func (_m *PipelineMgrIface) CheckPipelines() {
	_m.Called()
}

// PipelineMgrIface_CheckPipelines_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CheckPipelines'
type PipelineMgrIface_CheckPipelines_Call struct {
	*mock.Call
}

// CheckPipelines is a helper method to define mock.On call
func (_e *PipelineMgrIface_Expecter) CheckPipelines() *PipelineMgrIface_CheckPipelines_Call {
	return &PipelineMgrIface_CheckPipelines_Call{Call: _e.mock.On("CheckPipelines")}
}

func (_c *PipelineMgrIface_CheckPipelines_Call) Run(run func()) *PipelineMgrIface_CheckPipelines_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineMgrIface_CheckPipelines_Call) Return() *PipelineMgrIface_CheckPipelines_Call {
	_c.Call.Return()
	return _c
}

func (_c *PipelineMgrIface_CheckPipelines_Call) RunAndReturn(run func()) *PipelineMgrIface_CheckPipelines_Call {
	_c.Call.Return(run)
	return _c
}

// DeletePipeline provides a mock function with given fields: pipelineName
func (_m *PipelineMgrIface) DeletePipeline(pipelineName string) error {
	ret := _m.Called(pipelineName)

	if len(ret) == 0 {
		panic("no return value specified for DeletePipeline")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(pipelineName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrIface_DeletePipeline_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DeletePipeline'
type PipelineMgrIface_DeletePipeline_Call struct {
	*mock.Call
}

// DeletePipeline is a helper method to define mock.On call
//   - pipelineName string
func (_e *PipelineMgrIface_Expecter) DeletePipeline(pipelineName interface{}) *PipelineMgrIface_DeletePipeline_Call {
	return &PipelineMgrIface_DeletePipeline_Call{Call: _e.mock.On("DeletePipeline", pipelineName)}
}

func (_c *PipelineMgrIface_DeletePipeline_Call) Run(run func(pipelineName string)) *PipelineMgrIface_DeletePipeline_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrIface_DeletePipeline_Call) Return(_a0 error) *PipelineMgrIface_DeletePipeline_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrIface_DeletePipeline_Call) RunAndReturn(run func(string) error) *PipelineMgrIface_DeletePipeline_Call {
	_c.Call.Return(run)
	return _c
}

// DismissEventForPipeline provides a mock function with given fields: pipelineName, eventId
func (_m *PipelineMgrIface) DismissEventForPipeline(pipelineName string, eventId int) error {
	ret := _m.Called(pipelineName, eventId)

	if len(ret) == 0 {
		panic("no return value specified for DismissEventForPipeline")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, int) error); ok {
		r0 = rf(pipelineName, eventId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrIface_DismissEventForPipeline_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DismissEventForPipeline'
type PipelineMgrIface_DismissEventForPipeline_Call struct {
	*mock.Call
}

// DismissEventForPipeline is a helper method to define mock.On call
//   - pipelineName string
//   - eventId int
func (_e *PipelineMgrIface_Expecter) DismissEventForPipeline(pipelineName interface{}, eventId interface{}) *PipelineMgrIface_DismissEventForPipeline_Call {
	return &PipelineMgrIface_DismissEventForPipeline_Call{Call: _e.mock.On("DismissEventForPipeline", pipelineName, eventId)}
}

func (_c *PipelineMgrIface_DismissEventForPipeline_Call) Run(run func(pipelineName string, eventId int)) *PipelineMgrIface_DismissEventForPipeline_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(int))
	})
	return _c
}

func (_c *PipelineMgrIface_DismissEventForPipeline_Call) Return(_a0 error) *PipelineMgrIface_DismissEventForPipeline_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrIface_DismissEventForPipeline_Call) RunAndReturn(run func(string, int) error) *PipelineMgrIface_DismissEventForPipeline_Call {
	_c.Call.Return(run)
	return _c
}

// HandleClusterEncryptionLevelChange provides a mock function with given fields: old, new
func (_m *PipelineMgrIface) HandleClusterEncryptionLevelChange(old service_def.EncryptionSettingIface, new service_def.EncryptionSettingIface) {
	_m.Called(old, new)
}

// PipelineMgrIface_HandleClusterEncryptionLevelChange_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'HandleClusterEncryptionLevelChange'
type PipelineMgrIface_HandleClusterEncryptionLevelChange_Call struct {
	*mock.Call
}

// HandleClusterEncryptionLevelChange is a helper method to define mock.On call
//   - old service_def.EncryptionSettingIface
//   - new service_def.EncryptionSettingIface
func (_e *PipelineMgrIface_Expecter) HandleClusterEncryptionLevelChange(old interface{}, new interface{}) *PipelineMgrIface_HandleClusterEncryptionLevelChange_Call {
	return &PipelineMgrIface_HandleClusterEncryptionLevelChange_Call{Call: _e.mock.On("HandleClusterEncryptionLevelChange", old, new)}
}

func (_c *PipelineMgrIface_HandleClusterEncryptionLevelChange_Call) Run(run func(old service_def.EncryptionSettingIface, new service_def.EncryptionSettingIface)) *PipelineMgrIface_HandleClusterEncryptionLevelChange_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(service_def.EncryptionSettingIface), args[1].(service_def.EncryptionSettingIface))
	})
	return _c
}

func (_c *PipelineMgrIface_HandleClusterEncryptionLevelChange_Call) Return() *PipelineMgrIface_HandleClusterEncryptionLevelChange_Call {
	_c.Call.Return()
	return _c
}

func (_c *PipelineMgrIface_HandleClusterEncryptionLevelChange_Call) RunAndReturn(run func(service_def.EncryptionSettingIface, service_def.EncryptionSettingIface)) *PipelineMgrIface_HandleClusterEncryptionLevelChange_Call {
	_c.Call.Return(run)
	return _c
}

// HandlePeerCkptPush provides a mock function with given fields: fullTopic, sender, dynamicEvt
func (_m *PipelineMgrIface) HandlePeerCkptPush(fullTopic string, sender string, dynamicEvt interface{}) error {
	ret := _m.Called(fullTopic, sender, dynamicEvt)

	if len(ret) == 0 {
		panic("no return value specified for HandlePeerCkptPush")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, interface{}) error); ok {
		r0 = rf(fullTopic, sender, dynamicEvt)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrIface_HandlePeerCkptPush_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'HandlePeerCkptPush'
type PipelineMgrIface_HandlePeerCkptPush_Call struct {
	*mock.Call
}

// HandlePeerCkptPush is a helper method to define mock.On call
//   - fullTopic string
//   - sender string
//   - dynamicEvt interface{}
func (_e *PipelineMgrIface_Expecter) HandlePeerCkptPush(fullTopic interface{}, sender interface{}, dynamicEvt interface{}) *PipelineMgrIface_HandlePeerCkptPush_Call {
	return &PipelineMgrIface_HandlePeerCkptPush_Call{Call: _e.mock.On("HandlePeerCkptPush", fullTopic, sender, dynamicEvt)}
}

func (_c *PipelineMgrIface_HandlePeerCkptPush_Call) Run(run func(fullTopic string, sender string, dynamicEvt interface{})) *PipelineMgrIface_HandlePeerCkptPush_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string), args[2].(interface{}))
	})
	return _c
}

func (_c *PipelineMgrIface_HandlePeerCkptPush_Call) Return(_a0 error) *PipelineMgrIface_HandlePeerCkptPush_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrIface_HandlePeerCkptPush_Call) RunAndReturn(run func(string, string, interface{}) error) *PipelineMgrIface_HandlePeerCkptPush_Call {
	_c.Call.Return(run)
	return _c
}

// InitiateRepStatus provides a mock function with given fields: pipelineName
func (_m *PipelineMgrIface) InitiateRepStatus(pipelineName string) error {
	ret := _m.Called(pipelineName)

	if len(ret) == 0 {
		panic("no return value specified for InitiateRepStatus")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(pipelineName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrIface_InitiateRepStatus_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'InitiateRepStatus'
type PipelineMgrIface_InitiateRepStatus_Call struct {
	*mock.Call
}

// InitiateRepStatus is a helper method to define mock.On call
//   - pipelineName string
func (_e *PipelineMgrIface_Expecter) InitiateRepStatus(pipelineName interface{}) *PipelineMgrIface_InitiateRepStatus_Call {
	return &PipelineMgrIface_InitiateRepStatus_Call{Call: _e.mock.On("InitiateRepStatus", pipelineName)}
}

func (_c *PipelineMgrIface_InitiateRepStatus_Call) Run(run func(pipelineName string)) *PipelineMgrIface_InitiateRepStatus_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrIface_InitiateRepStatus_Call) Return(_a0 error) *PipelineMgrIface_InitiateRepStatus_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrIface_InitiateRepStatus_Call) RunAndReturn(run func(string) error) *PipelineMgrIface_InitiateRepStatus_Call {
	_c.Call.Return(run)
	return _c
}

// OnExit provides a mock function with given fields:
func (_m *PipelineMgrIface) OnExit() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for OnExit")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrIface_OnExit_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'OnExit'
type PipelineMgrIface_OnExit_Call struct {
	*mock.Call
}

// OnExit is a helper method to define mock.On call
func (_e *PipelineMgrIface_Expecter) OnExit() *PipelineMgrIface_OnExit_Call {
	return &PipelineMgrIface_OnExit_Call{Call: _e.mock.On("OnExit")}
}

func (_c *PipelineMgrIface_OnExit_Call) Run(run func()) *PipelineMgrIface_OnExit_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineMgrIface_OnExit_Call) Return(_a0 error) *PipelineMgrIface_OnExit_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrIface_OnExit_Call) RunAndReturn(run func() error) *PipelineMgrIface_OnExit_Call {
	_c.Call.Return(run)
	return _c
}

// ReInitStreams provides a mock function with given fields: pipelineName
func (_m *PipelineMgrIface) ReInitStreams(pipelineName string) error {
	ret := _m.Called(pipelineName)

	if len(ret) == 0 {
		panic("no return value specified for ReInitStreams")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(pipelineName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrIface_ReInitStreams_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ReInitStreams'
type PipelineMgrIface_ReInitStreams_Call struct {
	*mock.Call
}

// ReInitStreams is a helper method to define mock.On call
//   - pipelineName string
func (_e *PipelineMgrIface_Expecter) ReInitStreams(pipelineName interface{}) *PipelineMgrIface_ReInitStreams_Call {
	return &PipelineMgrIface_ReInitStreams_Call{Call: _e.mock.On("ReInitStreams", pipelineName)}
}

func (_c *PipelineMgrIface_ReInitStreams_Call) Run(run func(pipelineName string)) *PipelineMgrIface_ReInitStreams_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrIface_ReInitStreams_Call) Return(_a0 error) *PipelineMgrIface_ReInitStreams_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrIface_ReInitStreams_Call) RunAndReturn(run func(string) error) *PipelineMgrIface_ReInitStreams_Call {
	_c.Call.Return(run)
	return _c
}

// ReplicationStatus provides a mock function with given fields: topic
func (_m *PipelineMgrIface) ReplicationStatus(topic string) (pipeline.ReplicationStatusIface, error) {
	ret := _m.Called(topic)

	if len(ret) == 0 {
		panic("no return value specified for ReplicationStatus")
	}

	var r0 pipeline.ReplicationStatusIface
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (pipeline.ReplicationStatusIface, error)); ok {
		return rf(topic)
	}
	if rf, ok := ret.Get(0).(func(string) pipeline.ReplicationStatusIface); ok {
		r0 = rf(topic)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(pipeline.ReplicationStatusIface)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(topic)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// PipelineMgrIface_ReplicationStatus_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ReplicationStatus'
type PipelineMgrIface_ReplicationStatus_Call struct {
	*mock.Call
}

// ReplicationStatus is a helper method to define mock.On call
//   - topic string
func (_e *PipelineMgrIface_Expecter) ReplicationStatus(topic interface{}) *PipelineMgrIface_ReplicationStatus_Call {
	return &PipelineMgrIface_ReplicationStatus_Call{Call: _e.mock.On("ReplicationStatus", topic)}
}

func (_c *PipelineMgrIface_ReplicationStatus_Call) Run(run func(topic string)) *PipelineMgrIface_ReplicationStatus_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *PipelineMgrIface_ReplicationStatus_Call) Return(_a0 pipeline.ReplicationStatusIface, _a1 error) *PipelineMgrIface_ReplicationStatus_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *PipelineMgrIface_ReplicationStatus_Call) RunAndReturn(run func(string) (pipeline.ReplicationStatusIface, error)) *PipelineMgrIface_ReplicationStatus_Call {
	_c.Call.Return(run)
	return _c
}

// ReplicationStatusMap provides a mock function with given fields:
func (_m *PipelineMgrIface) ReplicationStatusMap() map[string]pipeline.ReplicationStatusIface {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for ReplicationStatusMap")
	}

	var r0 map[string]pipeline.ReplicationStatusIface
	if rf, ok := ret.Get(0).(func() map[string]pipeline.ReplicationStatusIface); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]pipeline.ReplicationStatusIface)
		}
	}

	return r0
}

// PipelineMgrIface_ReplicationStatusMap_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ReplicationStatusMap'
type PipelineMgrIface_ReplicationStatusMap_Call struct {
	*mock.Call
}

// ReplicationStatusMap is a helper method to define mock.On call
func (_e *PipelineMgrIface_Expecter) ReplicationStatusMap() *PipelineMgrIface_ReplicationStatusMap_Call {
	return &PipelineMgrIface_ReplicationStatusMap_Call{Call: _e.mock.On("ReplicationStatusMap")}
}

func (_c *PipelineMgrIface_ReplicationStatusMap_Call) Run(run func()) *PipelineMgrIface_ReplicationStatusMap_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *PipelineMgrIface_ReplicationStatusMap_Call) Return(_a0 map[string]pipeline.ReplicationStatusIface) *PipelineMgrIface_ReplicationStatusMap_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrIface_ReplicationStatusMap_Call) RunAndReturn(run func() map[string]pipeline.ReplicationStatusIface) *PipelineMgrIface_ReplicationStatusMap_Call {
	_c.Call.Return(run)
	return _c
}

// UpdatePipeline provides a mock function with given fields: pipelineName, cur_err
func (_m *PipelineMgrIface) UpdatePipeline(pipelineName string, cur_err error) error {
	ret := _m.Called(pipelineName, cur_err)

	if len(ret) == 0 {
		panic("no return value specified for UpdatePipeline")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, error) error); ok {
		r0 = rf(pipelineName, cur_err)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrIface_UpdatePipeline_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UpdatePipeline'
type PipelineMgrIface_UpdatePipeline_Call struct {
	*mock.Call
}

// UpdatePipeline is a helper method to define mock.On call
//   - pipelineName string
//   - cur_err error
func (_e *PipelineMgrIface_Expecter) UpdatePipeline(pipelineName interface{}, cur_err interface{}) *PipelineMgrIface_UpdatePipeline_Call {
	return &PipelineMgrIface_UpdatePipeline_Call{Call: _e.mock.On("UpdatePipeline", pipelineName, cur_err)}
}

func (_c *PipelineMgrIface_UpdatePipeline_Call) Run(run func(pipelineName string, cur_err error)) *PipelineMgrIface_UpdatePipeline_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(error))
	})
	return _c
}

func (_c *PipelineMgrIface_UpdatePipeline_Call) Return(_a0 error) *PipelineMgrIface_UpdatePipeline_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrIface_UpdatePipeline_Call) RunAndReturn(run func(string, error) error) *PipelineMgrIface_UpdatePipeline_Call {
	_c.Call.Return(run)
	return _c
}

// UpdatePipelineWithStoppedCb provides a mock function with given fields: topic, callback, errCb
func (_m *PipelineMgrIface) UpdatePipelineWithStoppedCb(topic string, callback base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback) error {
	ret := _m.Called(topic, callback, errCb)

	if len(ret) == 0 {
		panic("no return value specified for UpdatePipelineWithStoppedCb")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, base.StoppedPipelineCallback, base.StoppedPipelineErrCallback) error); ok {
		r0 = rf(topic, callback, errCb)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PipelineMgrIface_UpdatePipelineWithStoppedCb_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UpdatePipelineWithStoppedCb'
type PipelineMgrIface_UpdatePipelineWithStoppedCb_Call struct {
	*mock.Call
}

// UpdatePipelineWithStoppedCb is a helper method to define mock.On call
//   - topic string
//   - callback base.StoppedPipelineCallback
//   - errCb base.StoppedPipelineErrCallback
func (_e *PipelineMgrIface_Expecter) UpdatePipelineWithStoppedCb(topic interface{}, callback interface{}, errCb interface{}) *PipelineMgrIface_UpdatePipelineWithStoppedCb_Call {
	return &PipelineMgrIface_UpdatePipelineWithStoppedCb_Call{Call: _e.mock.On("UpdatePipelineWithStoppedCb", topic, callback, errCb)}
}

func (_c *PipelineMgrIface_UpdatePipelineWithStoppedCb_Call) Run(run func(topic string, callback base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback)) *PipelineMgrIface_UpdatePipelineWithStoppedCb_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(base.StoppedPipelineCallback), args[2].(base.StoppedPipelineErrCallback))
	})
	return _c
}

func (_c *PipelineMgrIface_UpdatePipelineWithStoppedCb_Call) Return(_a0 error) *PipelineMgrIface_UpdatePipelineWithStoppedCb_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *PipelineMgrIface_UpdatePipelineWithStoppedCb_Call) RunAndReturn(run func(string, base.StoppedPipelineCallback, base.StoppedPipelineErrCallback) error) *PipelineMgrIface_UpdatePipelineWithStoppedCb_Call {
	_c.Call.Return(run)
	return _c
}

// NewPipelineMgrIface creates a new instance of PipelineMgrIface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewPipelineMgrIface(t interface {
	mock.TestingT
	Cleanup(func())
}) *PipelineMgrIface {
	mock := &PipelineMgrIface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
