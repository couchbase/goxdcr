// Code generated by mockery v2.5.1. DO NOT EDIT.

package mocks

import (
	memcached "github.com/couchbase/gomemcached/client"
	mock "github.com/stretchr/testify/mock"
)

// Filter is an autogenerated mock type for the Filter type
type Filter struct {
	mock.Mock
}

// FilterUprEvent provides a mock function with given fields: uprEvent
func (_m *Filter) FilterUprEvent(uprEvent *memcached.UprEvent) (bool, error, string, int64) {
	ret := _m.Called(uprEvent)

	var r0 bool
	if rf, ok := ret.Get(0).(func(*memcached.UprEvent) bool); ok {
		r0 = rf(uprEvent)
	} else {
		r0 = ret.Get(0).(bool)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*memcached.UprEvent) error); ok {
		r1 = rf(uprEvent)
	} else {
		r1 = ret.Error(1)
	}

	var r2 string
	if rf, ok := ret.Get(2).(func(*memcached.UprEvent) string); ok {
		r2 = rf(uprEvent)
	} else {
		r2 = ret.Get(2).(string)
	}

	var r3 int64
	if rf, ok := ret.Get(3).(func(*memcached.UprEvent) int64); ok {
		r3 = rf(uprEvent)
	} else {
		r3 = ret.Get(3).(int64)
	}

	return r0, r1, r2, r3
}

// SetShouldSkipUncommittedTxn provides a mock function with given fields: val
func (_m *Filter) SetShouldSkipUncommittedTxn(val bool) {
	_m.Called(val)
}
