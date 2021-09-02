// Code generated by mockery (devel). DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	mock "github.com/stretchr/testify/mock"
)

// TargetNotification is an autogenerated mock type for the TargetNotification type
type TargetNotification struct {
	mock.Mock
}

// Clone provides a mock function with given fields:
func (_m *TargetNotification) Clone() interface{} {
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
func (_m *TargetNotification) CloneRO() interface{} {
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

// GetTargetBucketInfo provides a mock function with given fields:
func (_m *TargetNotification) GetTargetBucketInfo() base.BucketInfoMapType {
	ret := _m.Called()

	var r0 base.BucketInfoMapType
	if rf, ok := ret.Get(0).(func() base.BucketInfoMapType); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.BucketInfoMapType)
		}
	}

	return r0
}

// GetTargetBucketUUID provides a mock function with given fields:
func (_m *TargetNotification) GetTargetBucketUUID() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// GetTargetServerVBMap provides a mock function with given fields:
func (_m *TargetNotification) GetTargetServerVBMap() base.KvVBMapType {
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
func (_m *TargetNotification) IsSourceNotification() bool {
	ret := _m.Called()

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}