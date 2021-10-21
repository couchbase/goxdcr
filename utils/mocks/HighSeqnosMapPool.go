// Code generated by mockery (devel). DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	mock "github.com/stretchr/testify/mock"
)

// HighSeqnosMapPool is an autogenerated mock type for the HighSeqnosMapPool type
type HighSeqnosMapPool struct {
	mock.Mock
}

// Get provides a mock function with given fields: keys
func (_m *HighSeqnosMapPool) Get(keys []string) base.HighSeqnosMapType {
	ret := _m.Called(keys)

	var r0 base.HighSeqnosMapType
	if rf, ok := ret.Get(0).(func([]string) base.HighSeqnosMapType); ok {
		r0 = rf(keys)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.HighSeqnosMapType)
		}
	}

	return r0
}

// Put provides a mock function with given fields: _a0
func (_m *HighSeqnosMapPool) Put(_a0 base.HighSeqnosMapType) {
	_m.Called(_a0)
}