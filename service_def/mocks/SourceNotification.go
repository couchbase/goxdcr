// Code generated by mockery (devel). DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	mock "github.com/stretchr/testify/mock"
)

// SourceNotification is an autogenerated mock type for the SourceNotification type
type SourceNotification struct {
	mock.Mock
}

// Clone provides a mock function with given fields:
func (_m *SourceNotification) Clone() interface{} {
	ret := _m.Called()

	var r0 interface{}
	if rf, ok := ret.Get(0).(func() interface{}); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	return r0
}

// CloneRO provides a mock function with given fields:
func (_m *SourceNotification) CloneRO() interface{} {
	ret := _m.Called()

	var r0 interface{}
	if rf, ok := ret.Get(0).(func() interface{}); ok {
		r0 = rf()
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