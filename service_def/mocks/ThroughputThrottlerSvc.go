// Code generated by mockery v1.0.0
package mocks

import mock "github.com/stretchr/testify/mock"

// ThroughputThrottlerSvc is an autogenerated mock type for the ThroughputThrottlerSvc type
type ThroughputThrottlerSvc struct {
	mock.Mock
}

// CanSend provides a mock function with given fields:
func (_m *ThroughputThrottlerSvc) CanSend() bool {
	ret := _m.Called()

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// SetThroughputLimit provides a mock function with given fields: limit
func (_m *ThroughputThrottlerSvc) SetThroughputLimit(limit int64) error {
	ret := _m.Called(limit)

	var r0 error
	if rf, ok := ret.Get(0).(func(int64) error); ok {
		r0 = rf(limit)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Start provides a mock function with given fields:
func (_m *ThroughputThrottlerSvc) Start() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Stop provides a mock function with given fields:
func (_m *ThroughputThrottlerSvc) Stop() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Wait provides a mock function with given fields:
func (_m *ThroughputThrottlerSvc) Wait() {
	_m.Called()
}