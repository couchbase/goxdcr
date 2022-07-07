// Code generated by mockery (devel). DO NOT EDIT.

package mocks

import (
	memcached "github.com/couchbase/gomemcached/client"
	mock "github.com/stretchr/testify/mock"
)

// CasFunc is an autogenerated mock type for the CasFunc type
type CasFunc struct {
	mock.Mock
}

// Execute provides a mock function with given fields: current
func (_m *CasFunc) Execute(current []byte) ([]byte, memcached.CasOp) {
	ret := _m.Called(current)

	var r0 []byte
	if rf, ok := ret.Get(0).(func([]byte) []byte); ok {
		r0 = rf(current)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	var r1 memcached.CasOp
	if rf, ok := ret.Get(1).(func([]byte) memcached.CasOp); ok {
		r1 = rf(current)
	} else {
		r1 = ret.Get(1).(memcached.CasOp)
	}

	return r0, r1
}