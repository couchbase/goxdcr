// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	mock "github.com/stretchr/testify/mock"

	time "time"
)

// SourceNotification is an autogenerated mock type for the SourceNotification type
type SourceNotification struct {
	mock.Mock
}

type SourceNotification_Expecter struct {
	mock *mock.Mock
}

func (_m *SourceNotification) EXPECT() *SourceNotification_Expecter {
	return &SourceNotification_Expecter{mock: &_m.Mock}
}

// Clone provides a mock function with given fields: numOfReaders
func (_m *SourceNotification) Clone(numOfReaders int) interface{} {
	ret := _m.Called(numOfReaders)

	var r0 interface{}
	if rf, ok := ret.Get(0).(func(int) interface{}); ok {
		r0 = rf(numOfReaders)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	return r0
}

// SourceNotification_Clone_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Clone'
type SourceNotification_Clone_Call struct {
	*mock.Call
}

// Clone is a helper method to define mock.On call
//   - numOfReaders int
func (_e *SourceNotification_Expecter) Clone(numOfReaders interface{}) *SourceNotification_Clone_Call {
	return &SourceNotification_Clone_Call{Call: _e.mock.On("Clone", numOfReaders)}
}

func (_c *SourceNotification_Clone_Call) Run(run func(numOfReaders int)) *SourceNotification_Clone_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(int))
	})
	return _c
}

func (_c *SourceNotification_Clone_Call) Return(_a0 interface{}) *SourceNotification_Clone_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *SourceNotification_Clone_Call) RunAndReturn(run func(int) interface{}) *SourceNotification_Clone_Call {
	_c.Call.Return(run)
	return _c
}

// GetDcpStatsMap provides a mock function with given fields:
func (_m *SourceNotification) GetDcpStatsMap() base.DcpStatsMapType {
	ret := _m.Called()

	var r0 base.DcpStatsMapType
	if rf, ok := ret.Get(0).(func() base.DcpStatsMapType); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.DcpStatsMapType)
		}
	}

	return r0
}

// SourceNotification_GetDcpStatsMap_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetDcpStatsMap'
type SourceNotification_GetDcpStatsMap_Call struct {
	*mock.Call
}

// GetDcpStatsMap is a helper method to define mock.On call
func (_e *SourceNotification_Expecter) GetDcpStatsMap() *SourceNotification_GetDcpStatsMap_Call {
	return &SourceNotification_GetDcpStatsMap_Call{Call: _e.mock.On("GetDcpStatsMap")}
}

func (_c *SourceNotification_GetDcpStatsMap_Call) Run(run func()) *SourceNotification_GetDcpStatsMap_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SourceNotification_GetDcpStatsMap_Call) Return(_a0 base.DcpStatsMapType) *SourceNotification_GetDcpStatsMap_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *SourceNotification_GetDcpStatsMap_Call) RunAndReturn(run func() base.DcpStatsMapType) *SourceNotification_GetDcpStatsMap_Call {
	_c.Call.Return(run)
	return _c
}

// GetDcpStatsMapLegacy provides a mock function with given fields:
func (_m *SourceNotification) GetDcpStatsMapLegacy() base.DcpStatsMapType {
	ret := _m.Called()

	var r0 base.DcpStatsMapType
	if rf, ok := ret.Get(0).(func() base.DcpStatsMapType); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.DcpStatsMapType)
		}
	}

	return r0
}

// SourceNotification_GetDcpStatsMapLegacy_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetDcpStatsMapLegacy'
type SourceNotification_GetDcpStatsMapLegacy_Call struct {
	*mock.Call
}

// GetDcpStatsMapLegacy is a helper method to define mock.On call
func (_e *SourceNotification_Expecter) GetDcpStatsMapLegacy() *SourceNotification_GetDcpStatsMapLegacy_Call {
	return &SourceNotification_GetDcpStatsMapLegacy_Call{Call: _e.mock.On("GetDcpStatsMapLegacy")}
}

func (_c *SourceNotification_GetDcpStatsMapLegacy_Call) Run(run func()) *SourceNotification_GetDcpStatsMapLegacy_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SourceNotification_GetDcpStatsMapLegacy_Call) Return(_a0 base.DcpStatsMapType) *SourceNotification_GetDcpStatsMapLegacy_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *SourceNotification_GetDcpStatsMapLegacy_Call) RunAndReturn(run func() base.DcpStatsMapType) *SourceNotification_GetDcpStatsMapLegacy_Call {
	_c.Call.Return(run)
	return _c
}

// GetHighSeqnosMap provides a mock function with given fields:
func (_m *SourceNotification) GetHighSeqnosMap() base.HighSeqnosMapType {
	ret := _m.Called()

	var r0 base.HighSeqnosMapType
	if rf, ok := ret.Get(0).(func() base.HighSeqnosMapType); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.HighSeqnosMapType)
		}
	}

	return r0
}

// SourceNotification_GetHighSeqnosMap_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetHighSeqnosMap'
type SourceNotification_GetHighSeqnosMap_Call struct {
	*mock.Call
}

// GetHighSeqnosMap is a helper method to define mock.On call
func (_e *SourceNotification_Expecter) GetHighSeqnosMap() *SourceNotification_GetHighSeqnosMap_Call {
	return &SourceNotification_GetHighSeqnosMap_Call{Call: _e.mock.On("GetHighSeqnosMap")}
}

func (_c *SourceNotification_GetHighSeqnosMap_Call) Run(run func()) *SourceNotification_GetHighSeqnosMap_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SourceNotification_GetHighSeqnosMap_Call) Return(_a0 base.HighSeqnosMapType) *SourceNotification_GetHighSeqnosMap_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *SourceNotification_GetHighSeqnosMap_Call) RunAndReturn(run func() base.HighSeqnosMapType) *SourceNotification_GetHighSeqnosMap_Call {
	_c.Call.Return(run)
	return _c
}

// GetHighSeqnosMapLegacy provides a mock function with given fields:
func (_m *SourceNotification) GetHighSeqnosMapLegacy() base.HighSeqnosMapType {
	ret := _m.Called()

	var r0 base.HighSeqnosMapType
	if rf, ok := ret.Get(0).(func() base.HighSeqnosMapType); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.HighSeqnosMapType)
		}
	}

	return r0
}

// SourceNotification_GetHighSeqnosMapLegacy_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetHighSeqnosMapLegacy'
type SourceNotification_GetHighSeqnosMapLegacy_Call struct {
	*mock.Call
}

// GetHighSeqnosMapLegacy is a helper method to define mock.On call
func (_e *SourceNotification_Expecter) GetHighSeqnosMapLegacy() *SourceNotification_GetHighSeqnosMapLegacy_Call {
	return &SourceNotification_GetHighSeqnosMapLegacy_Call{Call: _e.mock.On("GetHighSeqnosMapLegacy")}
}

func (_c *SourceNotification_GetHighSeqnosMapLegacy_Call) Run(run func()) *SourceNotification_GetHighSeqnosMapLegacy_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SourceNotification_GetHighSeqnosMapLegacy_Call) Return(_a0 base.HighSeqnosMapType) *SourceNotification_GetHighSeqnosMapLegacy_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *SourceNotification_GetHighSeqnosMapLegacy_Call) RunAndReturn(run func() base.HighSeqnosMapType) *SourceNotification_GetHighSeqnosMapLegacy_Call {
	_c.Call.Return(run)
	return _c
}

// GetKvVbMapRO provides a mock function with given fields:
func (_m *SourceNotification) GetKvVbMapRO() base.KvVBMapType {
	ret := _m.Called()

	var r0 base.KvVBMapType
	if rf, ok := ret.Get(0).(func() base.KvVBMapType); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.KvVBMapType)
		}
	}

	return r0
}

// SourceNotification_GetKvVbMapRO_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetKvVbMapRO'
type SourceNotification_GetKvVbMapRO_Call struct {
	*mock.Call
}

// GetKvVbMapRO is a helper method to define mock.On call
func (_e *SourceNotification_Expecter) GetKvVbMapRO() *SourceNotification_GetKvVbMapRO_Call {
	return &SourceNotification_GetKvVbMapRO_Call{Call: _e.mock.On("GetKvVbMapRO")}
}

func (_c *SourceNotification_GetKvVbMapRO_Call) Run(run func()) *SourceNotification_GetKvVbMapRO_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SourceNotification_GetKvVbMapRO_Call) Return(_a0 base.KvVBMapType) *SourceNotification_GetKvVbMapRO_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *SourceNotification_GetKvVbMapRO_Call) RunAndReturn(run func() base.KvVBMapType) *SourceNotification_GetKvVbMapRO_Call {
	_c.Call.Return(run)
	return _c
}

// GetLocalTopologyUpdatedTime provides a mock function with given fields:
func (_m *SourceNotification) GetLocalTopologyUpdatedTime() time.Time {
	ret := _m.Called()

	var r0 time.Time
	if rf, ok := ret.Get(0).(func() time.Time); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(time.Time)
	}

	return r0
}

// SourceNotification_GetLocalTopologyUpdatedTime_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetLocalTopologyUpdatedTime'
type SourceNotification_GetLocalTopologyUpdatedTime_Call struct {
	*mock.Call
}

// GetLocalTopologyUpdatedTime is a helper method to define mock.On call
func (_e *SourceNotification_Expecter) GetLocalTopologyUpdatedTime() *SourceNotification_GetLocalTopologyUpdatedTime_Call {
	return &SourceNotification_GetLocalTopologyUpdatedTime_Call{Call: _e.mock.On("GetLocalTopologyUpdatedTime")}
}

func (_c *SourceNotification_GetLocalTopologyUpdatedTime_Call) Run(run func()) *SourceNotification_GetLocalTopologyUpdatedTime_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SourceNotification_GetLocalTopologyUpdatedTime_Call) Return(_a0 time.Time) *SourceNotification_GetLocalTopologyUpdatedTime_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *SourceNotification_GetLocalTopologyUpdatedTime_Call) RunAndReturn(run func() time.Time) *SourceNotification_GetLocalTopologyUpdatedTime_Call {
	_c.Call.Return(run)
	return _c
}

// GetNumberOfSourceNodes provides a mock function with given fields:
func (_m *SourceNotification) GetNumberOfSourceNodes() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// SourceNotification_GetNumberOfSourceNodes_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetNumberOfSourceNodes'
type SourceNotification_GetNumberOfSourceNodes_Call struct {
	*mock.Call
}

// GetNumberOfSourceNodes is a helper method to define mock.On call
func (_e *SourceNotification_Expecter) GetNumberOfSourceNodes() *SourceNotification_GetNumberOfSourceNodes_Call {
	return &SourceNotification_GetNumberOfSourceNodes_Call{Call: _e.mock.On("GetNumberOfSourceNodes")}
}

func (_c *SourceNotification_GetNumberOfSourceNodes_Call) Run(run func()) *SourceNotification_GetNumberOfSourceNodes_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SourceNotification_GetNumberOfSourceNodes_Call) Return(_a0 int) *SourceNotification_GetNumberOfSourceNodes_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *SourceNotification_GetNumberOfSourceNodes_Call) RunAndReturn(run func() int) *SourceNotification_GetNumberOfSourceNodes_Call {
	_c.Call.Return(run)
	return _c
}

// GetReplicasInfo provides a mock function with given fields:
func (_m *SourceNotification) GetReplicasInfo() (int, *base.VbHostsMapType, *base.StringStringMap, []uint16) {
	ret := _m.Called()

	var r0 int
	var r1 *base.VbHostsMapType
	var r2 *base.StringStringMap
	var r3 []uint16
	if rf, ok := ret.Get(0).(func() (int, *base.VbHostsMapType, *base.StringStringMap, []uint16)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	if rf, ok := ret.Get(1).(func() *base.VbHostsMapType); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(*base.VbHostsMapType)
		}
	}

	if rf, ok := ret.Get(2).(func() *base.StringStringMap); ok {
		r2 = rf()
	} else {
		if ret.Get(2) != nil {
			r2 = ret.Get(2).(*base.StringStringMap)
		}
	}

	if rf, ok := ret.Get(3).(func() []uint16); ok {
		r3 = rf()
	} else {
		if ret.Get(3) != nil {
			r3 = ret.Get(3).([]uint16)
		}
	}

	return r0, r1, r2, r3
}

// SourceNotification_GetReplicasInfo_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetReplicasInfo'
type SourceNotification_GetReplicasInfo_Call struct {
	*mock.Call
}

// GetReplicasInfo is a helper method to define mock.On call
func (_e *SourceNotification_Expecter) GetReplicasInfo() *SourceNotification_GetReplicasInfo_Call {
	return &SourceNotification_GetReplicasInfo_Call{Call: _e.mock.On("GetReplicasInfo")}
}

func (_c *SourceNotification_GetReplicasInfo_Call) Run(run func()) *SourceNotification_GetReplicasInfo_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SourceNotification_GetReplicasInfo_Call) Return(_a0 int, _a1 *base.VbHostsMapType, _a2 *base.StringStringMap, _a3 []uint16) *SourceNotification_GetReplicasInfo_Call {
	_c.Call.Return(_a0, _a1, _a2, _a3)
	return _c
}

func (_c *SourceNotification_GetReplicasInfo_Call) RunAndReturn(run func() (int, *base.VbHostsMapType, *base.StringStringMap, []uint16)) *SourceNotification_GetReplicasInfo_Call {
	_c.Call.Return(run)
	return _c
}

// GetSourceCollectionManifestUid provides a mock function with given fields:
func (_m *SourceNotification) GetSourceCollectionManifestUid() uint64 {
	ret := _m.Called()

	var r0 uint64
	if rf, ok := ret.Get(0).(func() uint64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint64)
	}

	return r0
}

// SourceNotification_GetSourceCollectionManifestUid_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSourceCollectionManifestUid'
type SourceNotification_GetSourceCollectionManifestUid_Call struct {
	*mock.Call
}

// GetSourceCollectionManifestUid is a helper method to define mock.On call
func (_e *SourceNotification_Expecter) GetSourceCollectionManifestUid() *SourceNotification_GetSourceCollectionManifestUid_Call {
	return &SourceNotification_GetSourceCollectionManifestUid_Call{Call: _e.mock.On("GetSourceCollectionManifestUid")}
}

func (_c *SourceNotification_GetSourceCollectionManifestUid_Call) Run(run func()) *SourceNotification_GetSourceCollectionManifestUid_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SourceNotification_GetSourceCollectionManifestUid_Call) Return(_a0 uint64) *SourceNotification_GetSourceCollectionManifestUid_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *SourceNotification_GetSourceCollectionManifestUid_Call) RunAndReturn(run func() uint64) *SourceNotification_GetSourceCollectionManifestUid_Call {
	_c.Call.Return(run)
	return _c
}

// GetSourceStorageBackend provides a mock function with given fields:
func (_m *SourceNotification) GetSourceStorageBackend() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// SourceNotification_GetSourceStorageBackend_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSourceStorageBackend'
type SourceNotification_GetSourceStorageBackend_Call struct {
	*mock.Call
}

// GetSourceStorageBackend is a helper method to define mock.On call
func (_e *SourceNotification_Expecter) GetSourceStorageBackend() *SourceNotification_GetSourceStorageBackend_Call {
	return &SourceNotification_GetSourceStorageBackend_Call{Call: _e.mock.On("GetSourceStorageBackend")}
}

func (_c *SourceNotification_GetSourceStorageBackend_Call) Run(run func()) *SourceNotification_GetSourceStorageBackend_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SourceNotification_GetSourceStorageBackend_Call) Return(_a0 string) *SourceNotification_GetSourceStorageBackend_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *SourceNotification_GetSourceStorageBackend_Call) RunAndReturn(run func() string) *SourceNotification_GetSourceStorageBackend_Call {
	_c.Call.Return(run)
	return _c
}

// GetSourceVBMapRO provides a mock function with given fields:
func (_m *SourceNotification) GetSourceVBMapRO() base.KvVBMapType {
	ret := _m.Called()

	var r0 base.KvVBMapType
	if rf, ok := ret.Get(0).(func() base.KvVBMapType); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.KvVBMapType)
		}
	}

	return r0
}

// SourceNotification_GetSourceVBMapRO_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSourceVBMapRO'
type SourceNotification_GetSourceVBMapRO_Call struct {
	*mock.Call
}

// GetSourceVBMapRO is a helper method to define mock.On call
func (_e *SourceNotification_Expecter) GetSourceVBMapRO() *SourceNotification_GetSourceVBMapRO_Call {
	return &SourceNotification_GetSourceVBMapRO_Call{Call: _e.mock.On("GetSourceVBMapRO")}
}

func (_c *SourceNotification_GetSourceVBMapRO_Call) Run(run func()) *SourceNotification_GetSourceVBMapRO_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SourceNotification_GetSourceVBMapRO_Call) Return(_a0 base.KvVBMapType) *SourceNotification_GetSourceVBMapRO_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *SourceNotification_GetSourceVBMapRO_Call) RunAndReturn(run func() base.KvVBMapType) *SourceNotification_GetSourceVBMapRO_Call {
	_c.Call.Return(run)
	return _c
}

// IsSourceNotification provides a mock function with given fields:
func (_m *SourceNotification) IsSourceNotification() bool {
	ret := _m.Called()

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// SourceNotification_IsSourceNotification_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'IsSourceNotification'
type SourceNotification_IsSourceNotification_Call struct {
	*mock.Call
}

// IsSourceNotification is a helper method to define mock.On call
func (_e *SourceNotification_Expecter) IsSourceNotification() *SourceNotification_IsSourceNotification_Call {
	return &SourceNotification_IsSourceNotification_Call{Call: _e.mock.On("IsSourceNotification")}
}

func (_c *SourceNotification_IsSourceNotification_Call) Run(run func()) *SourceNotification_IsSourceNotification_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SourceNotification_IsSourceNotification_Call) Return(_a0 bool) *SourceNotification_IsSourceNotification_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *SourceNotification_IsSourceNotification_Call) RunAndReturn(run func() bool) *SourceNotification_IsSourceNotification_Call {
	_c.Call.Return(run)
	return _c
}

// Recycle provides a mock function with given fields:
func (_m *SourceNotification) Recycle() {
	_m.Called()
}

// SourceNotification_Recycle_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Recycle'
type SourceNotification_Recycle_Call struct {
	*mock.Call
}

// Recycle is a helper method to define mock.On call
func (_e *SourceNotification_Expecter) Recycle() *SourceNotification_Recycle_Call {
	return &SourceNotification_Recycle_Call{Call: _e.mock.On("Recycle")}
}

func (_c *SourceNotification_Recycle_Call) Run(run func()) *SourceNotification_Recycle_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SourceNotification_Recycle_Call) Return() *SourceNotification_Recycle_Call {
	_c.Call.Return()
	return _c
}

func (_c *SourceNotification_Recycle_Call) RunAndReturn(run func()) *SourceNotification_Recycle_Call {
	_c.Call.Return(run)
	return _c
}

// NewSourceNotification creates a new instance of SourceNotification. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewSourceNotification(t interface {
	mock.TestingT
	Cleanup(func())
}) *SourceNotification {
	mock := &SourceNotification{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
