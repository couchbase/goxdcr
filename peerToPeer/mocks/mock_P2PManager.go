// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/v8/base"
	common "github.com/couchbase/goxdcr/v8/common"

	metadata "github.com/couchbase/goxdcr/v8/metadata"

	mock "github.com/stretchr/testify/mock"

	peerToPeer "github.com/couchbase/goxdcr/v8/peerToPeer"

	sync "sync"
)

// P2PManager is an autogenerated mock type for the P2PManager type
type P2PManager struct {
	mock.Mock
}

type P2PManager_Expecter struct {
	mock *mock.Mock
}

func (_m *P2PManager) EXPECT() *P2PManager_Expecter {
	return &P2PManager_Expecter{mock: &_m.Mock}
}

// CheckVBMaster provides a mock function with given fields: _a0, _a1
func (_m *P2PManager) CheckVBMaster(_a0 peerToPeer.BucketVBMapType, _a1 common.Pipeline) (map[string]*peerToPeer.VBMasterCheckResp, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for CheckVBMaster")
	}

	var r0 map[string]*peerToPeer.VBMasterCheckResp
	var r1 error
	if rf, ok := ret.Get(0).(func(peerToPeer.BucketVBMapType, common.Pipeline) (map[string]*peerToPeer.VBMasterCheckResp, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(peerToPeer.BucketVBMapType, common.Pipeline) map[string]*peerToPeer.VBMasterCheckResp); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]*peerToPeer.VBMasterCheckResp)
		}
	}

	if rf, ok := ret.Get(1).(func(peerToPeer.BucketVBMapType, common.Pipeline) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// P2PManager_CheckVBMaster_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CheckVBMaster'
type P2PManager_CheckVBMaster_Call struct {
	*mock.Call
}

// CheckVBMaster is a helper method to define mock.On call
//   - _a0 peerToPeer.BucketVBMapType
//   - _a1 common.Pipeline
func (_e *P2PManager_Expecter) CheckVBMaster(_a0 interface{}, _a1 interface{}) *P2PManager_CheckVBMaster_Call {
	return &P2PManager_CheckVBMaster_Call{Call: _e.mock.On("CheckVBMaster", _a0, _a1)}
}

func (_c *P2PManager_CheckVBMaster_Call) Run(run func(_a0 peerToPeer.BucketVBMapType, _a1 common.Pipeline)) *P2PManager_CheckVBMaster_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(peerToPeer.BucketVBMapType), args[1].(common.Pipeline))
	})
	return _c
}

func (_c *P2PManager_CheckVBMaster_Call) Return(_a0 map[string]*peerToPeer.VBMasterCheckResp, _a1 error) *P2PManager_CheckVBMaster_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *P2PManager_CheckVBMaster_Call) RunAndReturn(run func(peerToPeer.BucketVBMapType, common.Pipeline) (map[string]*peerToPeer.VBMasterCheckResp, error)) *P2PManager_CheckVBMaster_Call {
	_c.Call.Return(run)
	return _c
}

// GetHeartbeatsReceivedV1 provides a mock function with given fields:
func (_m *P2PManager) GetHeartbeatsReceivedV1() (map[string][]*metadata.ReplicationSpecification, map[string][]string, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetHeartbeatsReceivedV1")
	}

	var r0 map[string][]*metadata.ReplicationSpecification
	var r1 map[string][]string
	var r2 error
	if rf, ok := ret.Get(0).(func() (map[string][]*metadata.ReplicationSpecification, map[string][]string, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() map[string][]*metadata.ReplicationSpecification); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string][]*metadata.ReplicationSpecification)
		}
	}

	if rf, ok := ret.Get(1).(func() map[string][]string); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(map[string][]string)
		}
	}

	if rf, ok := ret.Get(2).(func() error); ok {
		r2 = rf()
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// P2PManager_GetHeartbeatsReceivedV1_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetHeartbeatsReceivedV1'
type P2PManager_GetHeartbeatsReceivedV1_Call struct {
	*mock.Call
}

// GetHeartbeatsReceivedV1 is a helper method to define mock.On call
func (_e *P2PManager_Expecter) GetHeartbeatsReceivedV1() *P2PManager_GetHeartbeatsReceivedV1_Call {
	return &P2PManager_GetHeartbeatsReceivedV1_Call{Call: _e.mock.On("GetHeartbeatsReceivedV1")}
}

func (_c *P2PManager_GetHeartbeatsReceivedV1_Call) Run(run func()) *P2PManager_GetHeartbeatsReceivedV1_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *P2PManager_GetHeartbeatsReceivedV1_Call) Return(_a0 map[string][]*metadata.ReplicationSpecification, _a1 map[string][]string, _a2 error) *P2PManager_GetHeartbeatsReceivedV1_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *P2PManager_GetHeartbeatsReceivedV1_Call) RunAndReturn(run func() (map[string][]*metadata.ReplicationSpecification, map[string][]string, error)) *P2PManager_GetHeartbeatsReceivedV1_Call {
	_c.Call.Return(run)
	return _c
}

// GetLifecycleId provides a mock function with given fields:
func (_m *P2PManager) GetLifecycleId() string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetLifecycleId")
	}

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// P2PManager_GetLifecycleId_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetLifecycleId'
type P2PManager_GetLifecycleId_Call struct {
	*mock.Call
}

// GetLifecycleId is a helper method to define mock.On call
func (_e *P2PManager_Expecter) GetLifecycleId() *P2PManager_GetLifecycleId_Call {
	return &P2PManager_GetLifecycleId_Call{Call: _e.mock.On("GetLifecycleId")}
}

func (_c *P2PManager_GetLifecycleId_Call) Run(run func()) *P2PManager_GetLifecycleId_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *P2PManager_GetLifecycleId_Call) Return(_a0 string) *P2PManager_GetLifecycleId_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *P2PManager_GetLifecycleId_Call) RunAndReturn(run func() string) *P2PManager_GetLifecycleId_Call {
	_c.Call.Return(run)
	return _c
}

// ReplicationSpecChangeCallback provides a mock function with given fields: id, oldVal, newVal, wg
func (_m *P2PManager) ReplicationSpecChangeCallback(id string, oldVal interface{}, newVal interface{}, wg *sync.WaitGroup) error {
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

// P2PManager_ReplicationSpecChangeCallback_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ReplicationSpecChangeCallback'
type P2PManager_ReplicationSpecChangeCallback_Call struct {
	*mock.Call
}

// ReplicationSpecChangeCallback is a helper method to define mock.On call
//   - id string
//   - oldVal interface{}
//   - newVal interface{}
//   - wg *sync.WaitGroup
func (_e *P2PManager_Expecter) ReplicationSpecChangeCallback(id interface{}, oldVal interface{}, newVal interface{}, wg interface{}) *P2PManager_ReplicationSpecChangeCallback_Call {
	return &P2PManager_ReplicationSpecChangeCallback_Call{Call: _e.mock.On("ReplicationSpecChangeCallback", id, oldVal, newVal, wg)}
}

func (_c *P2PManager_ReplicationSpecChangeCallback_Call) Run(run func(id string, oldVal interface{}, newVal interface{}, wg *sync.WaitGroup)) *P2PManager_ReplicationSpecChangeCallback_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(interface{}), args[2].(interface{}), args[3].(*sync.WaitGroup))
	})
	return _c
}

func (_c *P2PManager_ReplicationSpecChangeCallback_Call) Return(_a0 error) *P2PManager_ReplicationSpecChangeCallback_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *P2PManager_ReplicationSpecChangeCallback_Call) RunAndReturn(run func(string, interface{}, interface{}, *sync.WaitGroup) error) *P2PManager_ReplicationSpecChangeCallback_Call {
	_c.Call.Return(run)
	return _c
}

// RequestImmediateCkptBkfillPush provides a mock function with given fields: replicationId
func (_m *P2PManager) RequestImmediateCkptBkfillPush(replicationId string) error {
	ret := _m.Called(replicationId)

	if len(ret) == 0 {
		panic("no return value specified for RequestImmediateCkptBkfillPush")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(replicationId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// P2PManager_RequestImmediateCkptBkfillPush_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RequestImmediateCkptBkfillPush'
type P2PManager_RequestImmediateCkptBkfillPush_Call struct {
	*mock.Call
}

// RequestImmediateCkptBkfillPush is a helper method to define mock.On call
//   - replicationId string
func (_e *P2PManager_Expecter) RequestImmediateCkptBkfillPush(replicationId interface{}) *P2PManager_RequestImmediateCkptBkfillPush_Call {
	return &P2PManager_RequestImmediateCkptBkfillPush_Call{Call: _e.mock.On("RequestImmediateCkptBkfillPush", replicationId)}
}

func (_c *P2PManager_RequestImmediateCkptBkfillPush_Call) Run(run func(replicationId string)) *P2PManager_RequestImmediateCkptBkfillPush_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *P2PManager_RequestImmediateCkptBkfillPush_Call) Return(_a0 error) *P2PManager_RequestImmediateCkptBkfillPush_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *P2PManager_RequestImmediateCkptBkfillPush_Call) RunAndReturn(run func(string) error) *P2PManager_RequestImmediateCkptBkfillPush_Call {
	_c.Call.Return(run)
	return _c
}

// RetrieveConnectionPreCheckResult provides a mock function with given fields: _a0
func (_m *P2PManager) RetrieveConnectionPreCheckResult(_a0 string) (base.ConnectionErrMapType, bool, error) {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for RetrieveConnectionPreCheckResult")
	}

	var r0 base.ConnectionErrMapType
	var r1 bool
	var r2 error
	if rf, ok := ret.Get(0).(func(string) (base.ConnectionErrMapType, bool, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(string) base.ConnectionErrMapType); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.ConnectionErrMapType)
		}
	}

	if rf, ok := ret.Get(1).(func(string) bool); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Get(1).(bool)
	}

	if rf, ok := ret.Get(2).(func(string) error); ok {
		r2 = rf(_a0)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// P2PManager_RetrieveConnectionPreCheckResult_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RetrieveConnectionPreCheckResult'
type P2PManager_RetrieveConnectionPreCheckResult_Call struct {
	*mock.Call
}

// RetrieveConnectionPreCheckResult is a helper method to define mock.On call
//   - _a0 string
func (_e *P2PManager_Expecter) RetrieveConnectionPreCheckResult(_a0 interface{}) *P2PManager_RetrieveConnectionPreCheckResult_Call {
	return &P2PManager_RetrieveConnectionPreCheckResult_Call{Call: _e.mock.On("RetrieveConnectionPreCheckResult", _a0)}
}

func (_c *P2PManager_RetrieveConnectionPreCheckResult_Call) Run(run func(_a0 string)) *P2PManager_RetrieveConnectionPreCheckResult_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *P2PManager_RetrieveConnectionPreCheckResult_Call) Return(_a0 base.ConnectionErrMapType, _a1 bool, _a2 error) *P2PManager_RetrieveConnectionPreCheckResult_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *P2PManager_RetrieveConnectionPreCheckResult_Call) RunAndReturn(run func(string) (base.ConnectionErrMapType, bool, error)) *P2PManager_RetrieveConnectionPreCheckResult_Call {
	_c.Call.Return(run)
	return _c
}

// SendConnectionPreCheckRequest provides a mock function with given fields: _a0, _a1, _a2
func (_m *P2PManager) SendConnectionPreCheckRequest(_a0 *metadata.RemoteClusterReference, _a1 peerToPeer.InitRemoteClusterRefFunc, _a2 string) {
	_m.Called(_a0, _a1, _a2)
}

// P2PManager_SendConnectionPreCheckRequest_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SendConnectionPreCheckRequest'
type P2PManager_SendConnectionPreCheckRequest_Call struct {
	*mock.Call
}

// SendConnectionPreCheckRequest is a helper method to define mock.On call
//   - _a0 *metadata.RemoteClusterReference
//   - _a1 peerToPeer.InitRemoteClusterRefFunc
//   - _a2 string
func (_e *P2PManager_Expecter) SendConnectionPreCheckRequest(_a0 interface{}, _a1 interface{}, _a2 interface{}) *P2PManager_SendConnectionPreCheckRequest_Call {
	return &P2PManager_SendConnectionPreCheckRequest_Call{Call: _e.mock.On("SendConnectionPreCheckRequest", _a0, _a1, _a2)}
}

func (_c *P2PManager_SendConnectionPreCheckRequest_Call) Run(run func(_a0 *metadata.RemoteClusterReference, _a1 peerToPeer.InitRemoteClusterRefFunc, _a2 string)) *P2PManager_SendConnectionPreCheckRequest_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.RemoteClusterReference), args[1].(peerToPeer.InitRemoteClusterRefFunc), args[2].(string))
	})
	return _c
}

func (_c *P2PManager_SendConnectionPreCheckRequest_Call) Return() *P2PManager_SendConnectionPreCheckRequest_Call {
	_c.Call.Return()
	return _c
}

func (_c *P2PManager_SendConnectionPreCheckRequest_Call) RunAndReturn(run func(*metadata.RemoteClusterReference, peerToPeer.InitRemoteClusterRefFunc, string)) *P2PManager_SendConnectionPreCheckRequest_Call {
	_c.Call.Return(run)
	return _c
}

// SendDelBackfill provides a mock function with given fields: specId
func (_m *P2PManager) SendDelBackfill(specId string) error {
	ret := _m.Called(specId)

	if len(ret) == 0 {
		panic("no return value specified for SendDelBackfill")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(specId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// P2PManager_SendDelBackfill_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SendDelBackfill'
type P2PManager_SendDelBackfill_Call struct {
	*mock.Call
}

// SendDelBackfill is a helper method to define mock.On call
//   - specId string
func (_e *P2PManager_Expecter) SendDelBackfill(specId interface{}) *P2PManager_SendDelBackfill_Call {
	return &P2PManager_SendDelBackfill_Call{Call: _e.mock.On("SendDelBackfill", specId)}
}

func (_c *P2PManager_SendDelBackfill_Call) Run(run func(specId string)) *P2PManager_SendDelBackfill_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *P2PManager_SendDelBackfill_Call) Return(_a0 error) *P2PManager_SendDelBackfill_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *P2PManager_SendDelBackfill_Call) RunAndReturn(run func(string) error) *P2PManager_SendDelBackfill_Call {
	_c.Call.Return(run)
	return _c
}

// SendHeartbeatToRemoteV1 provides a mock function with given fields: reference, specs
func (_m *P2PManager) SendHeartbeatToRemoteV1(reference *metadata.RemoteClusterReference, specs []*metadata.ReplicationSpecification) error {
	ret := _m.Called(reference, specs)

	if len(ret) == 0 {
		panic("no return value specified for SendHeartbeatToRemoteV1")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.RemoteClusterReference, []*metadata.ReplicationSpecification) error); ok {
		r0 = rf(reference, specs)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// P2PManager_SendHeartbeatToRemoteV1_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SendHeartbeatToRemoteV1'
type P2PManager_SendHeartbeatToRemoteV1_Call struct {
	*mock.Call
}

// SendHeartbeatToRemoteV1 is a helper method to define mock.On call
//   - reference *metadata.RemoteClusterReference
//   - specs []*metadata.ReplicationSpecification
func (_e *P2PManager_Expecter) SendHeartbeatToRemoteV1(reference interface{}, specs interface{}) *P2PManager_SendHeartbeatToRemoteV1_Call {
	return &P2PManager_SendHeartbeatToRemoteV1_Call{Call: _e.mock.On("SendHeartbeatToRemoteV1", reference, specs)}
}

func (_c *P2PManager_SendHeartbeatToRemoteV1_Call) Run(run func(reference *metadata.RemoteClusterReference, specs []*metadata.ReplicationSpecification)) *P2PManager_SendHeartbeatToRemoteV1_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.RemoteClusterReference), args[1].([]*metadata.ReplicationSpecification))
	})
	return _c
}

func (_c *P2PManager_SendHeartbeatToRemoteV1_Call) Return(_a0 error) *P2PManager_SendHeartbeatToRemoteV1_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *P2PManager_SendHeartbeatToRemoteV1_Call) RunAndReturn(run func(*metadata.RemoteClusterReference, []*metadata.ReplicationSpecification) error) *P2PManager_SendHeartbeatToRemoteV1_Call {
	_c.Call.Return(run)
	return _c
}

// SendManifests provides a mock function with given fields: spec, manifests
func (_m *P2PManager) SendManifests(spec *metadata.ReplicationSpecification, manifests *metadata.CollectionsManifestPair) error {
	ret := _m.Called(spec, manifests)

	if len(ret) == 0 {
		panic("no return value specified for SendManifests")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, *metadata.CollectionsManifestPair) error); ok {
		r0 = rf(spec, manifests)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// P2PManager_SendManifests_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SendManifests'
type P2PManager_SendManifests_Call struct {
	*mock.Call
}

// SendManifests is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - manifests *metadata.CollectionsManifestPair
func (_e *P2PManager_Expecter) SendManifests(spec interface{}, manifests interface{}) *P2PManager_SendManifests_Call {
	return &P2PManager_SendManifests_Call{Call: _e.mock.On("SendManifests", spec, manifests)}
}

func (_c *P2PManager_SendManifests_Call) Run(run func(spec *metadata.ReplicationSpecification, manifests *metadata.CollectionsManifestPair)) *P2PManager_SendManifests_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(*metadata.CollectionsManifestPair))
	})
	return _c
}

func (_c *P2PManager_SendManifests_Call) Return(_a0 error) *P2PManager_SendManifests_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *P2PManager_SendManifests_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, *metadata.CollectionsManifestPair) error) *P2PManager_SendManifests_Call {
	_c.Call.Return(run)
	return _c
}

// SendManualBackfill provides a mock function with given fields: specId, request
func (_m *P2PManager) SendManualBackfill(specId string, request string) error {
	ret := _m.Called(specId, request)

	if len(ret) == 0 {
		panic("no return value specified for SendManualBackfill")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string) error); ok {
		r0 = rf(specId, request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// P2PManager_SendManualBackfill_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SendManualBackfill'
type P2PManager_SendManualBackfill_Call struct {
	*mock.Call
}

// SendManualBackfill is a helper method to define mock.On call
//   - specId string
//   - request string
func (_e *P2PManager_Expecter) SendManualBackfill(specId interface{}, request interface{}) *P2PManager_SendManualBackfill_Call {
	return &P2PManager_SendManualBackfill_Call{Call: _e.mock.On("SendManualBackfill", specId, request)}
}

func (_c *P2PManager_SendManualBackfill_Call) Run(run func(specId string, request string)) *P2PManager_SendManualBackfill_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string))
	})
	return _c
}

func (_c *P2PManager_SendManualBackfill_Call) Return(_a0 error) *P2PManager_SendManualBackfill_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *P2PManager_SendManualBackfill_Call) RunAndReturn(run func(string, string) error) *P2PManager_SendManualBackfill_Call {
	_c.Call.Return(run)
	return _c
}

// SetPushReqMergerOnce provides a mock function with given fields: pm
func (_m *P2PManager) SetPushReqMergerOnce(pm func(string, string, interface{}) error) {
	_m.Called(pm)
}

// P2PManager_SetPushReqMergerOnce_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetPushReqMergerOnce'
type P2PManager_SetPushReqMergerOnce_Call struct {
	*mock.Call
}

// SetPushReqMergerOnce is a helper method to define mock.On call
//   - pm func(string , string , interface{}) error
func (_e *P2PManager_Expecter) SetPushReqMergerOnce(pm interface{}) *P2PManager_SetPushReqMergerOnce_Call {
	return &P2PManager_SetPushReqMergerOnce_Call{Call: _e.mock.On("SetPushReqMergerOnce", pm)}
}

func (_c *P2PManager_SetPushReqMergerOnce_Call) Run(run func(pm func(string, string, interface{}) error)) *P2PManager_SetPushReqMergerOnce_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(func(string, string, interface{}) error))
	})
	return _c
}

func (_c *P2PManager_SetPushReqMergerOnce_Call) Return() *P2PManager_SetPushReqMergerOnce_Call {
	_c.Call.Return()
	return _c
}

func (_c *P2PManager_SetPushReqMergerOnce_Call) RunAndReturn(run func(func(string, string, interface{}) error)) *P2PManager_SetPushReqMergerOnce_Call {
	_c.Call.Return(run)
	return _c
}

// Start provides a mock function with given fields:
func (_m *P2PManager) Start() (peerToPeer.PeerToPeerCommAPI, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Start")
	}

	var r0 peerToPeer.PeerToPeerCommAPI
	var r1 error
	if rf, ok := ret.Get(0).(func() (peerToPeer.PeerToPeerCommAPI, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() peerToPeer.PeerToPeerCommAPI); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(peerToPeer.PeerToPeerCommAPI)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// P2PManager_Start_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Start'
type P2PManager_Start_Call struct {
	*mock.Call
}

// Start is a helper method to define mock.On call
func (_e *P2PManager_Expecter) Start() *P2PManager_Start_Call {
	return &P2PManager_Start_Call{Call: _e.mock.On("Start")}
}

func (_c *P2PManager_Start_Call) Run(run func()) *P2PManager_Start_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *P2PManager_Start_Call) Return(_a0 peerToPeer.PeerToPeerCommAPI, _a1 error) *P2PManager_Start_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *P2PManager_Start_Call) RunAndReturn(run func() (peerToPeer.PeerToPeerCommAPI, error)) *P2PManager_Start_Call {
	_c.Call.Return(run)
	return _c
}

// Stop provides a mock function with given fields:
func (_m *P2PManager) Stop() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Stop")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// P2PManager_Stop_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Stop'
type P2PManager_Stop_Call struct {
	*mock.Call
}

// Stop is a helper method to define mock.On call
func (_e *P2PManager_Expecter) Stop() *P2PManager_Stop_Call {
	return &P2PManager_Stop_Call{Call: _e.mock.On("Stop")}
}

func (_c *P2PManager_Stop_Call) Run(run func()) *P2PManager_Stop_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *P2PManager_Stop_Call) Return(_a0 error) *P2PManager_Stop_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *P2PManager_Stop_Call) RunAndReturn(run func() error) *P2PManager_Stop_Call {
	_c.Call.Return(run)
	return _c
}

// NewP2PManager creates a new instance of P2PManager. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewP2PManager(t interface {
	mock.TestingT
	Cleanup(func())
}) *P2PManager {
	mock := &P2PManager{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
