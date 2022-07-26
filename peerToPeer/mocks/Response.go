// Code generated by mockery (devel). DO NOT EDIT.

package mocks

import (
	peerToPeer "github.com/couchbase/goxdcr/peerToPeer"
	mock "github.com/stretchr/testify/mock"

	time "time"
)

// Response is an autogenerated mock type for the Response type
type Response struct {
	mock.Mock
}

// DeSerialize provides a mock function with given fields: _a0
func (_m *Response) DeSerialize(_a0 []byte) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func([]byte) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetEnqueuedTime provides a mock function with given fields:
func (_m *Response) GetEnqueuedTime() time.Time {
	ret := _m.Called()

	var r0 time.Time
	if rf, ok := ret.Get(0).(func() time.Time); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(time.Time)
	}

	return r0
}

// GetErrorString provides a mock function with given fields:
func (_m *Response) GetErrorString() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// GetOpaque provides a mock function with given fields:
func (_m *Response) GetOpaque() uint32 {
	ret := _m.Called()

	var r0 uint32
	if rf, ok := ret.Get(0).(func() uint32); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint32)
	}

	return r0
}

// GetOpcode provides a mock function with given fields:
func (_m *Response) GetOpcode() peerToPeer.OpCode {
	ret := _m.Called()

	var r0 peerToPeer.OpCode
	if rf, ok := ret.Get(0).(func() peerToPeer.OpCode); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(peerToPeer.OpCode)
	}

	return r0
}

// GetSender provides a mock function with given fields:
func (_m *Response) GetSender() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// GetType provides a mock function with given fields:
func (_m *Response) GetType() peerToPeer.ReqRespType {
	ret := _m.Called()

	var r0 peerToPeer.ReqRespType
	if rf, ok := ret.Get(0).(func() peerToPeer.ReqRespType); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(peerToPeer.ReqRespType)
	}

	return r0
}

// RecordEnqueuedTime provides a mock function with given fields:
func (_m *Response) RecordEnqueuedTime() {
	_m.Called()
}

// Serialize provides a mock function with given fields:
func (_m *Response) Serialize() ([]byte, error) {
	ret := _m.Called()

	var r0 []byte
	if rf, ok := ret.Get(0).(func() []byte); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}