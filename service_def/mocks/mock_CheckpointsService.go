// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"

	service_def "github.com/couchbase/goxdcr/service_def"

	sync "sync"
)

// CheckpointsService is an autogenerated mock type for the CheckpointsService type
type CheckpointsService struct {
	mock.Mock
}

type CheckpointsService_Expecter struct {
	mock *mock.Mock
}

func (_m *CheckpointsService) EXPECT() *CheckpointsService_Expecter {
	return &CheckpointsService_Expecter{mock: &_m.Mock}
}

// BackfillReplicationSpecChangeCallback provides a mock function with given fields: metadataId, oldMetadata, newMetadata
func (_m *CheckpointsService) BackfillReplicationSpecChangeCallback(metadataId string, oldMetadata interface{}, newMetadata interface{}) error {
	ret := _m.Called(metadataId, oldMetadata, newMetadata)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, interface{}, interface{}) error); ok {
		r0 = rf(metadataId, oldMetadata, newMetadata)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CheckpointsService_BackfillReplicationSpecChangeCallback_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'BackfillReplicationSpecChangeCallback'
type CheckpointsService_BackfillReplicationSpecChangeCallback_Call struct {
	*mock.Call
}

// BackfillReplicationSpecChangeCallback is a helper method to define mock.On call
//   - metadataId string
//   - oldMetadata interface{}
//   - newMetadata interface{}
func (_e *CheckpointsService_Expecter) BackfillReplicationSpecChangeCallback(metadataId interface{}, oldMetadata interface{}, newMetadata interface{}) *CheckpointsService_BackfillReplicationSpecChangeCallback_Call {
	return &CheckpointsService_BackfillReplicationSpecChangeCallback_Call{Call: _e.mock.On("BackfillReplicationSpecChangeCallback", metadataId, oldMetadata, newMetadata)}
}

func (_c *CheckpointsService_BackfillReplicationSpecChangeCallback_Call) Run(run func(metadataId string, oldMetadata interface{}, newMetadata interface{})) *CheckpointsService_BackfillReplicationSpecChangeCallback_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(interface{}), args[2].(interface{}))
	})
	return _c
}

func (_c *CheckpointsService_BackfillReplicationSpecChangeCallback_Call) Return(_a0 error) *CheckpointsService_BackfillReplicationSpecChangeCallback_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointsService_BackfillReplicationSpecChangeCallback_Call) RunAndReturn(run func(string, interface{}, interface{}) error) *CheckpointsService_BackfillReplicationSpecChangeCallback_Call {
	_c.Call.Return(run)
	return _c
}

// CheckpointsDoc provides a mock function with given fields: replicationId, vbno
func (_m *CheckpointsService) CheckpointsDoc(replicationId string, vbno uint16) (*metadata.CheckpointsDoc, error) {
	ret := _m.Called(replicationId, vbno)

	var r0 *metadata.CheckpointsDoc
	var r1 error
	if rf, ok := ret.Get(0).(func(string, uint16) (*metadata.CheckpointsDoc, error)); ok {
		return rf(replicationId, vbno)
	}
	if rf, ok := ret.Get(0).(func(string, uint16) *metadata.CheckpointsDoc); ok {
		r0 = rf(replicationId, vbno)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CheckpointsDoc)
		}
	}

	if rf, ok := ret.Get(1).(func(string, uint16) error); ok {
		r1 = rf(replicationId, vbno)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CheckpointsService_CheckpointsDoc_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CheckpointsDoc'
type CheckpointsService_CheckpointsDoc_Call struct {
	*mock.Call
}

// CheckpointsDoc is a helper method to define mock.On call
//   - replicationId string
//   - vbno uint16
func (_e *CheckpointsService_Expecter) CheckpointsDoc(replicationId interface{}, vbno interface{}) *CheckpointsService_CheckpointsDoc_Call {
	return &CheckpointsService_CheckpointsDoc_Call{Call: _e.mock.On("CheckpointsDoc", replicationId, vbno)}
}

func (_c *CheckpointsService_CheckpointsDoc_Call) Run(run func(replicationId string, vbno uint16)) *CheckpointsService_CheckpointsDoc_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(uint16))
	})
	return _c
}

func (_c *CheckpointsService_CheckpointsDoc_Call) Return(_a0 *metadata.CheckpointsDoc, _a1 error) *CheckpointsService_CheckpointsDoc_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CheckpointsService_CheckpointsDoc_Call) RunAndReturn(run func(string, uint16) (*metadata.CheckpointsDoc, error)) *CheckpointsService_CheckpointsDoc_Call {
	_c.Call.Return(run)
	return _c
}

// CheckpointsDocs provides a mock function with given fields: replicationId, brokenMappingsNeeded
func (_m *CheckpointsService) CheckpointsDocs(replicationId string, brokenMappingsNeeded bool) (map[uint16]*metadata.CheckpointsDoc, error) {
	ret := _m.Called(replicationId, brokenMappingsNeeded)

	var r0 map[uint16]*metadata.CheckpointsDoc
	var r1 error
	if rf, ok := ret.Get(0).(func(string, bool) (map[uint16]*metadata.CheckpointsDoc, error)); ok {
		return rf(replicationId, brokenMappingsNeeded)
	}
	if rf, ok := ret.Get(0).(func(string, bool) map[uint16]*metadata.CheckpointsDoc); ok {
		r0 = rf(replicationId, brokenMappingsNeeded)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[uint16]*metadata.CheckpointsDoc)
		}
	}

	if rf, ok := ret.Get(1).(func(string, bool) error); ok {
		r1 = rf(replicationId, brokenMappingsNeeded)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CheckpointsService_CheckpointsDocs_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CheckpointsDocs'
type CheckpointsService_CheckpointsDocs_Call struct {
	*mock.Call
}

// CheckpointsDocs is a helper method to define mock.On call
//   - replicationId string
//   - brokenMappingsNeeded bool
func (_e *CheckpointsService_Expecter) CheckpointsDocs(replicationId interface{}, brokenMappingsNeeded interface{}) *CheckpointsService_CheckpointsDocs_Call {
	return &CheckpointsService_CheckpointsDocs_Call{Call: _e.mock.On("CheckpointsDocs", replicationId, brokenMappingsNeeded)}
}

func (_c *CheckpointsService_CheckpointsDocs_Call) Run(run func(replicationId string, brokenMappingsNeeded bool)) *CheckpointsService_CheckpointsDocs_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(bool))
	})
	return _c
}

func (_c *CheckpointsService_CheckpointsDocs_Call) Return(_a0 map[uint16]*metadata.CheckpointsDoc, _a1 error) *CheckpointsService_CheckpointsDocs_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CheckpointsService_CheckpointsDocs_Call) RunAndReturn(run func(string, bool) (map[uint16]*metadata.CheckpointsDoc, error)) *CheckpointsService_CheckpointsDocs_Call {
	_c.Call.Return(run)
	return _c
}

// CollectionsManifestChangeCb provides a mock function with given fields: metadataId, oldMetadata, newMetadata
func (_m *CheckpointsService) CollectionsManifestChangeCb(metadataId string, oldMetadata interface{}, newMetadata interface{}) error {
	ret := _m.Called(metadataId, oldMetadata, newMetadata)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, interface{}, interface{}) error); ok {
		r0 = rf(metadataId, oldMetadata, newMetadata)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CheckpointsService_CollectionsManifestChangeCb_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CollectionsManifestChangeCb'
type CheckpointsService_CollectionsManifestChangeCb_Call struct {
	*mock.Call
}

// CollectionsManifestChangeCb is a helper method to define mock.On call
//   - metadataId string
//   - oldMetadata interface{}
//   - newMetadata interface{}
func (_e *CheckpointsService_Expecter) CollectionsManifestChangeCb(metadataId interface{}, oldMetadata interface{}, newMetadata interface{}) *CheckpointsService_CollectionsManifestChangeCb_Call {
	return &CheckpointsService_CollectionsManifestChangeCb_Call{Call: _e.mock.On("CollectionsManifestChangeCb", metadataId, oldMetadata, newMetadata)}
}

func (_c *CheckpointsService_CollectionsManifestChangeCb_Call) Run(run func(metadataId string, oldMetadata interface{}, newMetadata interface{})) *CheckpointsService_CollectionsManifestChangeCb_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(interface{}), args[2].(interface{}))
	})
	return _c
}

func (_c *CheckpointsService_CollectionsManifestChangeCb_Call) Return(_a0 error) *CheckpointsService_CollectionsManifestChangeCb_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointsService_CollectionsManifestChangeCb_Call) RunAndReturn(run func(string, interface{}, interface{}) error) *CheckpointsService_CollectionsManifestChangeCb_Call {
	_c.Call.Return(run)
	return _c
}

// DelCheckpointsDoc provides a mock function with given fields: replicationId, vbno, specInternalId
func (_m *CheckpointsService) DelCheckpointsDoc(replicationId string, vbno uint16, specInternalId string) error {
	ret := _m.Called(replicationId, vbno, specInternalId)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, uint16, string) error); ok {
		r0 = rf(replicationId, vbno, specInternalId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CheckpointsService_DelCheckpointsDoc_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DelCheckpointsDoc'
type CheckpointsService_DelCheckpointsDoc_Call struct {
	*mock.Call
}

// DelCheckpointsDoc is a helper method to define mock.On call
//   - replicationId string
//   - vbno uint16
//   - specInternalId string
func (_e *CheckpointsService_Expecter) DelCheckpointsDoc(replicationId interface{}, vbno interface{}, specInternalId interface{}) *CheckpointsService_DelCheckpointsDoc_Call {
	return &CheckpointsService_DelCheckpointsDoc_Call{Call: _e.mock.On("DelCheckpointsDoc", replicationId, vbno, specInternalId)}
}

func (_c *CheckpointsService_DelCheckpointsDoc_Call) Run(run func(replicationId string, vbno uint16, specInternalId string)) *CheckpointsService_DelCheckpointsDoc_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(uint16), args[2].(string))
	})
	return _c
}

func (_c *CheckpointsService_DelCheckpointsDoc_Call) Return(_a0 error) *CheckpointsService_DelCheckpointsDoc_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointsService_DelCheckpointsDoc_Call) RunAndReturn(run func(string, uint16, string) error) *CheckpointsService_DelCheckpointsDoc_Call {
	_c.Call.Return(run)
	return _c
}

// DelCheckpointsDocs provides a mock function with given fields: replicationId
func (_m *CheckpointsService) DelCheckpointsDocs(replicationId string) error {
	ret := _m.Called(replicationId)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(replicationId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CheckpointsService_DelCheckpointsDocs_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DelCheckpointsDocs'
type CheckpointsService_DelCheckpointsDocs_Call struct {
	*mock.Call
}

// DelCheckpointsDocs is a helper method to define mock.On call
//   - replicationId string
func (_e *CheckpointsService_Expecter) DelCheckpointsDocs(replicationId interface{}) *CheckpointsService_DelCheckpointsDocs_Call {
	return &CheckpointsService_DelCheckpointsDocs_Call{Call: _e.mock.On("DelCheckpointsDocs", replicationId)}
}

func (_c *CheckpointsService_DelCheckpointsDocs_Call) Run(run func(replicationId string)) *CheckpointsService_DelCheckpointsDocs_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *CheckpointsService_DelCheckpointsDocs_Call) Return(_a0 error) *CheckpointsService_DelCheckpointsDocs_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointsService_DelCheckpointsDocs_Call) RunAndReturn(run func(string) error) *CheckpointsService_DelCheckpointsDocs_Call {
	_c.Call.Return(run)
	return _c
}

// DisableRefCntDecrement provides a mock function with given fields: topic
func (_m *CheckpointsService) DisableRefCntDecrement(topic string) {
	_m.Called(topic)
}

// CheckpointsService_DisableRefCntDecrement_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DisableRefCntDecrement'
type CheckpointsService_DisableRefCntDecrement_Call struct {
	*mock.Call
}

// DisableRefCntDecrement is a helper method to define mock.On call
//   - topic string
func (_e *CheckpointsService_Expecter) DisableRefCntDecrement(topic interface{}) *CheckpointsService_DisableRefCntDecrement_Call {
	return &CheckpointsService_DisableRefCntDecrement_Call{Call: _e.mock.On("DisableRefCntDecrement", topic)}
}

func (_c *CheckpointsService_DisableRefCntDecrement_Call) Run(run func(topic string)) *CheckpointsService_DisableRefCntDecrement_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *CheckpointsService_DisableRefCntDecrement_Call) Return() *CheckpointsService_DisableRefCntDecrement_Call {
	_c.Call.Return()
	return _c
}

func (_c *CheckpointsService_DisableRefCntDecrement_Call) RunAndReturn(run func(string)) *CheckpointsService_DisableRefCntDecrement_Call {
	_c.Call.Return(run)
	return _c
}

// EnableRefCntDecrement provides a mock function with given fields: topic
func (_m *CheckpointsService) EnableRefCntDecrement(topic string) {
	_m.Called(topic)
}

// CheckpointsService_EnableRefCntDecrement_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'EnableRefCntDecrement'
type CheckpointsService_EnableRefCntDecrement_Call struct {
	*mock.Call
}

// EnableRefCntDecrement is a helper method to define mock.On call
//   - topic string
func (_e *CheckpointsService_Expecter) EnableRefCntDecrement(topic interface{}) *CheckpointsService_EnableRefCntDecrement_Call {
	return &CheckpointsService_EnableRefCntDecrement_Call{Call: _e.mock.On("EnableRefCntDecrement", topic)}
}

func (_c *CheckpointsService_EnableRefCntDecrement_Call) Run(run func(topic string)) *CheckpointsService_EnableRefCntDecrement_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *CheckpointsService_EnableRefCntDecrement_Call) Return() *CheckpointsService_EnableRefCntDecrement_Call {
	_c.Call.Return()
	return _c
}

func (_c *CheckpointsService_EnableRefCntDecrement_Call) RunAndReturn(run func(string)) *CheckpointsService_EnableRefCntDecrement_Call {
	_c.Call.Return(run)
	return _c
}

// GetCkptsMappingsCleanupCallback provides a mock function with given fields: specId, specInternalId, toBeRemoved
func (_m *CheckpointsService) GetCkptsMappingsCleanupCallback(specId string, specInternalId string, toBeRemoved metadata.ScopesMap) (base.StoppedPipelineCallback, base.StoppedPipelineErrCallback) {
	ret := _m.Called(specId, specInternalId, toBeRemoved)

	var r0 base.StoppedPipelineCallback
	var r1 base.StoppedPipelineErrCallback
	if rf, ok := ret.Get(0).(func(string, string, metadata.ScopesMap) (base.StoppedPipelineCallback, base.StoppedPipelineErrCallback)); ok {
		return rf(specId, specInternalId, toBeRemoved)
	}
	if rf, ok := ret.Get(0).(func(string, string, metadata.ScopesMap) base.StoppedPipelineCallback); ok {
		r0 = rf(specId, specInternalId, toBeRemoved)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.StoppedPipelineCallback)
		}
	}

	if rf, ok := ret.Get(1).(func(string, string, metadata.ScopesMap) base.StoppedPipelineErrCallback); ok {
		r1 = rf(specId, specInternalId, toBeRemoved)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(base.StoppedPipelineErrCallback)
		}
	}

	return r0, r1
}

// CheckpointsService_GetCkptsMappingsCleanupCallback_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetCkptsMappingsCleanupCallback'
type CheckpointsService_GetCkptsMappingsCleanupCallback_Call struct {
	*mock.Call
}

// GetCkptsMappingsCleanupCallback is a helper method to define mock.On call
//   - specId string
//   - specInternalId string
//   - toBeRemoved metadata.ScopesMap
func (_e *CheckpointsService_Expecter) GetCkptsMappingsCleanupCallback(specId interface{}, specInternalId interface{}, toBeRemoved interface{}) *CheckpointsService_GetCkptsMappingsCleanupCallback_Call {
	return &CheckpointsService_GetCkptsMappingsCleanupCallback_Call{Call: _e.mock.On("GetCkptsMappingsCleanupCallback", specId, specInternalId, toBeRemoved)}
}

func (_c *CheckpointsService_GetCkptsMappingsCleanupCallback_Call) Run(run func(specId string, specInternalId string, toBeRemoved metadata.ScopesMap)) *CheckpointsService_GetCkptsMappingsCleanupCallback_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string), args[2].(metadata.ScopesMap))
	})
	return _c
}

func (_c *CheckpointsService_GetCkptsMappingsCleanupCallback_Call) Return(_a0 base.StoppedPipelineCallback, _a1 base.StoppedPipelineErrCallback) *CheckpointsService_GetCkptsMappingsCleanupCallback_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CheckpointsService_GetCkptsMappingsCleanupCallback_Call) RunAndReturn(run func(string, string, metadata.ScopesMap) (base.StoppedPipelineCallback, base.StoppedPipelineErrCallback)) *CheckpointsService_GetCkptsMappingsCleanupCallback_Call {
	_c.Call.Return(run)
	return _c
}

// GetVbnosFromCheckpointDocs provides a mock function with given fields: replicationId
func (_m *CheckpointsService) GetVbnosFromCheckpointDocs(replicationId string) ([]uint16, error) {
	ret := _m.Called(replicationId)

	var r0 []uint16
	var r1 error
	if rf, ok := ret.Get(0).(func(string) ([]uint16, error)); ok {
		return rf(replicationId)
	}
	if rf, ok := ret.Get(0).(func(string) []uint16); ok {
		r0 = rf(replicationId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]uint16)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(replicationId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CheckpointsService_GetVbnosFromCheckpointDocs_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetVbnosFromCheckpointDocs'
type CheckpointsService_GetVbnosFromCheckpointDocs_Call struct {
	*mock.Call
}

// GetVbnosFromCheckpointDocs is a helper method to define mock.On call
//   - replicationId string
func (_e *CheckpointsService_Expecter) GetVbnosFromCheckpointDocs(replicationId interface{}) *CheckpointsService_GetVbnosFromCheckpointDocs_Call {
	return &CheckpointsService_GetVbnosFromCheckpointDocs_Call{Call: _e.mock.On("GetVbnosFromCheckpointDocs", replicationId)}
}

func (_c *CheckpointsService_GetVbnosFromCheckpointDocs_Call) Run(run func(replicationId string)) *CheckpointsService_GetVbnosFromCheckpointDocs_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *CheckpointsService_GetVbnosFromCheckpointDocs_Call) Return(_a0 []uint16, _a1 error) *CheckpointsService_GetVbnosFromCheckpointDocs_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CheckpointsService_GetVbnosFromCheckpointDocs_Call) RunAndReturn(run func(string) ([]uint16, error)) *CheckpointsService_GetVbnosFromCheckpointDocs_Call {
	_c.Call.Return(run)
	return _c
}

// LoadBrokenMappings provides a mock function with given fields: replicationId
func (_m *CheckpointsService) LoadBrokenMappings(replicationId string) (metadata.ShaToCollectionNamespaceMap, *metadata.CollectionNsMappingsDoc, service_def.IncrementerFunc, bool, error) {
	ret := _m.Called(replicationId)

	var r0 metadata.ShaToCollectionNamespaceMap
	var r1 *metadata.CollectionNsMappingsDoc
	var r2 service_def.IncrementerFunc
	var r3 bool
	var r4 error
	if rf, ok := ret.Get(0).(func(string) (metadata.ShaToCollectionNamespaceMap, *metadata.CollectionNsMappingsDoc, service_def.IncrementerFunc, bool, error)); ok {
		return rf(replicationId)
	}
	if rf, ok := ret.Get(0).(func(string) metadata.ShaToCollectionNamespaceMap); ok {
		r0 = rf(replicationId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(metadata.ShaToCollectionNamespaceMap)
		}
	}

	if rf, ok := ret.Get(1).(func(string) *metadata.CollectionNsMappingsDoc); ok {
		r1 = rf(replicationId)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(*metadata.CollectionNsMappingsDoc)
		}
	}

	if rf, ok := ret.Get(2).(func(string) service_def.IncrementerFunc); ok {
		r2 = rf(replicationId)
	} else {
		if ret.Get(2) != nil {
			r2 = ret.Get(2).(service_def.IncrementerFunc)
		}
	}

	if rf, ok := ret.Get(3).(func(string) bool); ok {
		r3 = rf(replicationId)
	} else {
		r3 = ret.Get(3).(bool)
	}

	if rf, ok := ret.Get(4).(func(string) error); ok {
		r4 = rf(replicationId)
	} else {
		r4 = ret.Error(4)
	}

	return r0, r1, r2, r3, r4
}

// CheckpointsService_LoadBrokenMappings_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'LoadBrokenMappings'
type CheckpointsService_LoadBrokenMappings_Call struct {
	*mock.Call
}

// LoadBrokenMappings is a helper method to define mock.On call
//   - replicationId string
func (_e *CheckpointsService_Expecter) LoadBrokenMappings(replicationId interface{}) *CheckpointsService_LoadBrokenMappings_Call {
	return &CheckpointsService_LoadBrokenMappings_Call{Call: _e.mock.On("LoadBrokenMappings", replicationId)}
}

func (_c *CheckpointsService_LoadBrokenMappings_Call) Run(run func(replicationId string)) *CheckpointsService_LoadBrokenMappings_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *CheckpointsService_LoadBrokenMappings_Call) Return(_a0 metadata.ShaToCollectionNamespaceMap, _a1 *metadata.CollectionNsMappingsDoc, _a2 service_def.IncrementerFunc, _a3 bool, _a4 error) *CheckpointsService_LoadBrokenMappings_Call {
	_c.Call.Return(_a0, _a1, _a2, _a3, _a4)
	return _c
}

func (_c *CheckpointsService_LoadBrokenMappings_Call) RunAndReturn(run func(string) (metadata.ShaToCollectionNamespaceMap, *metadata.CollectionNsMappingsDoc, service_def.IncrementerFunc, bool, error)) *CheckpointsService_LoadBrokenMappings_Call {
	_c.Call.Return(run)
	return _c
}

// PreUpsertBrokenMapping provides a mock function with given fields: replicationId, specInternalId, oneBrokenMapping
func (_m *CheckpointsService) PreUpsertBrokenMapping(replicationId string, specInternalId string, oneBrokenMapping *metadata.CollectionNamespaceMapping) error {
	ret := _m.Called(replicationId, specInternalId, oneBrokenMapping)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, *metadata.CollectionNamespaceMapping) error); ok {
		r0 = rf(replicationId, specInternalId, oneBrokenMapping)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CheckpointsService_PreUpsertBrokenMapping_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'PreUpsertBrokenMapping'
type CheckpointsService_PreUpsertBrokenMapping_Call struct {
	*mock.Call
}

// PreUpsertBrokenMapping is a helper method to define mock.On call
//   - replicationId string
//   - specInternalId string
//   - oneBrokenMapping *metadata.CollectionNamespaceMapping
func (_e *CheckpointsService_Expecter) PreUpsertBrokenMapping(replicationId interface{}, specInternalId interface{}, oneBrokenMapping interface{}) *CheckpointsService_PreUpsertBrokenMapping_Call {
	return &CheckpointsService_PreUpsertBrokenMapping_Call{Call: _e.mock.On("PreUpsertBrokenMapping", replicationId, specInternalId, oneBrokenMapping)}
}

func (_c *CheckpointsService_PreUpsertBrokenMapping_Call) Run(run func(replicationId string, specInternalId string, oneBrokenMapping *metadata.CollectionNamespaceMapping)) *CheckpointsService_PreUpsertBrokenMapping_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string), args[2].(*metadata.CollectionNamespaceMapping))
	})
	return _c
}

func (_c *CheckpointsService_PreUpsertBrokenMapping_Call) Return(_a0 error) *CheckpointsService_PreUpsertBrokenMapping_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointsService_PreUpsertBrokenMapping_Call) RunAndReturn(run func(string, string, *metadata.CollectionNamespaceMapping) error) *CheckpointsService_PreUpsertBrokenMapping_Call {
	_c.Call.Return(run)
	return _c
}

// ReplicationSpecChangeCallback provides a mock function with given fields: metadataId, oldMetadata, newMetadata, wg
func (_m *CheckpointsService) ReplicationSpecChangeCallback(metadataId string, oldMetadata interface{}, newMetadata interface{}, wg *sync.WaitGroup) error {
	ret := _m.Called(metadataId, oldMetadata, newMetadata, wg)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, interface{}, interface{}, *sync.WaitGroup) error); ok {
		r0 = rf(metadataId, oldMetadata, newMetadata, wg)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CheckpointsService_ReplicationSpecChangeCallback_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ReplicationSpecChangeCallback'
type CheckpointsService_ReplicationSpecChangeCallback_Call struct {
	*mock.Call
}

// ReplicationSpecChangeCallback is a helper method to define mock.On call
//   - metadataId string
//   - oldMetadata interface{}
//   - newMetadata interface{}
//   - wg *sync.WaitGroup
func (_e *CheckpointsService_Expecter) ReplicationSpecChangeCallback(metadataId interface{}, oldMetadata interface{}, newMetadata interface{}, wg interface{}) *CheckpointsService_ReplicationSpecChangeCallback_Call {
	return &CheckpointsService_ReplicationSpecChangeCallback_Call{Call: _e.mock.On("ReplicationSpecChangeCallback", metadataId, oldMetadata, newMetadata, wg)}
}

func (_c *CheckpointsService_ReplicationSpecChangeCallback_Call) Run(run func(metadataId string, oldMetadata interface{}, newMetadata interface{}, wg *sync.WaitGroup)) *CheckpointsService_ReplicationSpecChangeCallback_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(interface{}), args[2].(interface{}), args[3].(*sync.WaitGroup))
	})
	return _c
}

func (_c *CheckpointsService_ReplicationSpecChangeCallback_Call) Return(_a0 error) *CheckpointsService_ReplicationSpecChangeCallback_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointsService_ReplicationSpecChangeCallback_Call) RunAndReturn(run func(string, interface{}, interface{}, *sync.WaitGroup) error) *CheckpointsService_ReplicationSpecChangeCallback_Call {
	_c.Call.Return(run)
	return _c
}

// UpsertAndReloadCheckpointCompleteSet provides a mock function with given fields: replicationId, mappingDoc, ckptDoc, internalId
func (_m *CheckpointsService) UpsertAndReloadCheckpointCompleteSet(replicationId string, mappingDoc *metadata.CollectionNsMappingsDoc, ckptDoc map[uint16]*metadata.CheckpointsDoc, internalId string) error {
	ret := _m.Called(replicationId, mappingDoc, ckptDoc, internalId)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, *metadata.CollectionNsMappingsDoc, map[uint16]*metadata.CheckpointsDoc, string) error); ok {
		r0 = rf(replicationId, mappingDoc, ckptDoc, internalId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CheckpointsService_UpsertAndReloadCheckpointCompleteSet_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UpsertAndReloadCheckpointCompleteSet'
type CheckpointsService_UpsertAndReloadCheckpointCompleteSet_Call struct {
	*mock.Call
}

// UpsertAndReloadCheckpointCompleteSet is a helper method to define mock.On call
//   - replicationId string
//   - mappingDoc *metadata.CollectionNsMappingsDoc
//   - ckptDoc map[uint16]*metadata.CheckpointsDoc
//   - internalId string
func (_e *CheckpointsService_Expecter) UpsertAndReloadCheckpointCompleteSet(replicationId interface{}, mappingDoc interface{}, ckptDoc interface{}, internalId interface{}) *CheckpointsService_UpsertAndReloadCheckpointCompleteSet_Call {
	return &CheckpointsService_UpsertAndReloadCheckpointCompleteSet_Call{Call: _e.mock.On("UpsertAndReloadCheckpointCompleteSet", replicationId, mappingDoc, ckptDoc, internalId)}
}

func (_c *CheckpointsService_UpsertAndReloadCheckpointCompleteSet_Call) Run(run func(replicationId string, mappingDoc *metadata.CollectionNsMappingsDoc, ckptDoc map[uint16]*metadata.CheckpointsDoc, internalId string)) *CheckpointsService_UpsertAndReloadCheckpointCompleteSet_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(*metadata.CollectionNsMappingsDoc), args[2].(map[uint16]*metadata.CheckpointsDoc), args[3].(string))
	})
	return _c
}

func (_c *CheckpointsService_UpsertAndReloadCheckpointCompleteSet_Call) Return(_a0 error) *CheckpointsService_UpsertAndReloadCheckpointCompleteSet_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointsService_UpsertAndReloadCheckpointCompleteSet_Call) RunAndReturn(run func(string, *metadata.CollectionNsMappingsDoc, map[uint16]*metadata.CheckpointsDoc, string) error) *CheckpointsService_UpsertAndReloadCheckpointCompleteSet_Call {
	_c.Call.Return(run)
	return _c
}

// UpsertBrokenMapping provides a mock function with given fields: replicationId, specInternalId
func (_m *CheckpointsService) UpsertBrokenMapping(replicationId string, specInternalId string) error {
	ret := _m.Called(replicationId, specInternalId)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string) error); ok {
		r0 = rf(replicationId, specInternalId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CheckpointsService_UpsertBrokenMapping_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UpsertBrokenMapping'
type CheckpointsService_UpsertBrokenMapping_Call struct {
	*mock.Call
}

// UpsertBrokenMapping is a helper method to define mock.On call
//   - replicationId string
//   - specInternalId string
func (_e *CheckpointsService_Expecter) UpsertBrokenMapping(replicationId interface{}, specInternalId interface{}) *CheckpointsService_UpsertBrokenMapping_Call {
	return &CheckpointsService_UpsertBrokenMapping_Call{Call: _e.mock.On("UpsertBrokenMapping", replicationId, specInternalId)}
}

func (_c *CheckpointsService_UpsertBrokenMapping_Call) Run(run func(replicationId string, specInternalId string)) *CheckpointsService_UpsertBrokenMapping_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string))
	})
	return _c
}

func (_c *CheckpointsService_UpsertBrokenMapping_Call) Return(_a0 error) *CheckpointsService_UpsertBrokenMapping_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointsService_UpsertBrokenMapping_Call) RunAndReturn(run func(string, string) error) *CheckpointsService_UpsertBrokenMapping_Call {
	_c.Call.Return(run)
	return _c
}

// UpsertCheckpoints provides a mock function with given fields: replicationId, specInternalId, vbno, ckpt_record
func (_m *CheckpointsService) UpsertCheckpoints(replicationId string, specInternalId string, vbno uint16, ckpt_record *metadata.CheckpointRecord) (int, error) {
	ret := _m.Called(replicationId, specInternalId, vbno, ckpt_record)

	var r0 int
	var r1 error
	if rf, ok := ret.Get(0).(func(string, string, uint16, *metadata.CheckpointRecord) (int, error)); ok {
		return rf(replicationId, specInternalId, vbno, ckpt_record)
	}
	if rf, ok := ret.Get(0).(func(string, string, uint16, *metadata.CheckpointRecord) int); ok {
		r0 = rf(replicationId, specInternalId, vbno, ckpt_record)
	} else {
		r0 = ret.Get(0).(int)
	}

	if rf, ok := ret.Get(1).(func(string, string, uint16, *metadata.CheckpointRecord) error); ok {
		r1 = rf(replicationId, specInternalId, vbno, ckpt_record)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CheckpointsService_UpsertCheckpoints_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UpsertCheckpoints'
type CheckpointsService_UpsertCheckpoints_Call struct {
	*mock.Call
}

// UpsertCheckpoints is a helper method to define mock.On call
//   - replicationId string
//   - specInternalId string
//   - vbno uint16
//   - ckpt_record *metadata.CheckpointRecord
func (_e *CheckpointsService_Expecter) UpsertCheckpoints(replicationId interface{}, specInternalId interface{}, vbno interface{}, ckpt_record interface{}) *CheckpointsService_UpsertCheckpoints_Call {
	return &CheckpointsService_UpsertCheckpoints_Call{Call: _e.mock.On("UpsertCheckpoints", replicationId, specInternalId, vbno, ckpt_record)}
}

func (_c *CheckpointsService_UpsertCheckpoints_Call) Run(run func(replicationId string, specInternalId string, vbno uint16, ckpt_record *metadata.CheckpointRecord)) *CheckpointsService_UpsertCheckpoints_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string), args[2].(uint16), args[3].(*metadata.CheckpointRecord))
	})
	return _c
}

func (_c *CheckpointsService_UpsertCheckpoints_Call) Return(_a0 int, _a1 error) *CheckpointsService_UpsertCheckpoints_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CheckpointsService_UpsertCheckpoints_Call) RunAndReturn(run func(string, string, uint16, *metadata.CheckpointRecord) (int, error)) *CheckpointsService_UpsertCheckpoints_Call {
	_c.Call.Return(run)
	return _c
}

// UpsertCheckpointsDone provides a mock function with given fields: replicationId, internalId
func (_m *CheckpointsService) UpsertCheckpointsDone(replicationId string, internalId string) error {
	ret := _m.Called(replicationId, internalId)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string) error); ok {
		r0 = rf(replicationId, internalId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CheckpointsService_UpsertCheckpointsDone_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UpsertCheckpointsDone'
type CheckpointsService_UpsertCheckpointsDone_Call struct {
	*mock.Call
}

// UpsertCheckpointsDone is a helper method to define mock.On call
//   - replicationId string
//   - internalId string
func (_e *CheckpointsService_Expecter) UpsertCheckpointsDone(replicationId interface{}, internalId interface{}) *CheckpointsService_UpsertCheckpointsDone_Call {
	return &CheckpointsService_UpsertCheckpointsDone_Call{Call: _e.mock.On("UpsertCheckpointsDone", replicationId, internalId)}
}

func (_c *CheckpointsService_UpsertCheckpointsDone_Call) Run(run func(replicationId string, internalId string)) *CheckpointsService_UpsertCheckpointsDone_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string))
	})
	return _c
}

func (_c *CheckpointsService_UpsertCheckpointsDone_Call) Return(_a0 error) *CheckpointsService_UpsertCheckpointsDone_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointsService_UpsertCheckpointsDone_Call) RunAndReturn(run func(string, string) error) *CheckpointsService_UpsertCheckpointsDone_Call {
	_c.Call.Return(run)
	return _c
}

// NewCheckpointsService creates a new instance of CheckpointsService. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewCheckpointsService(t interface {
	mock.TestingT
	Cleanup(func())
}) *CheckpointsService {
	mock := &CheckpointsService{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}