// Code generated by mockery v2.35.4. DO NOT EDIT.

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

// Recycle provides a mock function with given fields:
func (_m *SourceNotification) Recycle() {
	_m.Called()
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
