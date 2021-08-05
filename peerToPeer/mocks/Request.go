// Code generated by mockery v2.5.1. DO NOT EDIT.

package mocks

import (
	peerToPeer "github.com/couchbase/goxdcr/peerToPeer"
	mock "github.com/stretchr/testify/mock"
)

// Request is an autogenerated mock type for the Request type
type Request struct {
	mock.Mock
}

// CallBack provides a mock function with given fields: resp
func (_m *Request) CallBack(resp peerToPeer.Response) (peerToPeer.HandlerResult, error) {
	ret := _m.Called(resp)

	var r0 peerToPeer.HandlerResult
	if rf, ok := ret.Get(0).(func(peerToPeer.Response) peerToPeer.HandlerResult); ok {
		r0 = rf(resp)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(peerToPeer.HandlerResult)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(peerToPeer.Response) error); ok {
		r1 = rf(resp)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// DeSerialize provides a mock function with given fields: _a0
func (_m *Request) DeSerialize(_a0 []byte) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func([]byte) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GenerateResponse provides a mock function with given fields:
func (_m *Request) GenerateResponse() interface{} {
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

// GetOpaque provides a mock function with given fields:
func (_m *Request) GetOpaque() uint32 {
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
func (_m *Request) GetOpcode() peerToPeer.OpCode {
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
func (_m *Request) GetSender() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// GetTarget provides a mock function with given fields:
func (_m *Request) GetTarget() string {
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
func (_m *Request) GetType() peerToPeer.ReqRespType {
	ret := _m.Called()

	var r0 peerToPeer.ReqRespType
	if rf, ok := ret.Get(0).(func() peerToPeer.ReqRespType); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(peerToPeer.ReqRespType)
	}

	return r0
}

// SameAs provides a mock function with given fields: other
func (_m *Request) SameAs(other interface{}) (bool, error) {
	ret := _m.Called(other)

	var r0 bool
	if rf, ok := ret.Get(0).(func(interface{}) bool); ok {
		r0 = rf(other)
	} else {
		r0 = ret.Get(0).(bool)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(interface{}) error); ok {
		r1 = rf(other)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Serialize provides a mock function with given fields:
func (_m *Request) Serialize() ([]byte, error) {
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
