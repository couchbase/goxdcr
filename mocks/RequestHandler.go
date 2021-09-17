// Code generated by mockery (devel). DO NOT EDIT.

package mocks

import (
	io "io"

	gomemcached "github.com/couchbase/gomemcached"

	mock "github.com/stretchr/testify/mock"
)

// RequestHandler is an autogenerated mock type for the RequestHandler type
type RequestHandler struct {
	mock.Mock
}

// HandleMessage provides a mock function with given fields: _a0, _a1
func (_m *RequestHandler) HandleMessage(_a0 io.Writer, _a1 *gomemcached.MCRequest) *gomemcached.MCResponse {
	ret := _m.Called(_a0, _a1)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(io.Writer, *gomemcached.MCRequest) *gomemcached.MCResponse); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	return r0
}
