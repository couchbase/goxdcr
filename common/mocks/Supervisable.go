package mocks

import mock "github.com/stretchr/testify/mock"
import time "time"

// Supervisable is an autogenerated mock type for the Supervisable type
type Supervisable struct {
	mock.Mock
}

// HeartBeat_async provides a mock function with given fields: respchan, timestamp
func (_m *Supervisable) HeartBeat_async(respchan chan []interface{}, timestamp time.Time) error {
	ret := _m.Called(respchan, timestamp)

	var r0 error
	if rf, ok := ret.Get(0).(func(chan []interface{}, time.Time) error); ok {
		r0 = rf(respchan, timestamp)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// HeartBeat_sync provides a mock function with given fields:
func (_m *Supervisable) HeartBeat_sync() bool {
	ret := _m.Called()

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// Id provides a mock function with given fields:
func (_m *Supervisable) Id() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// IsReadyForHeartBeat provides a mock function with given fields:
func (_m *Supervisable) IsReadyForHeartBeat() bool {
	ret := _m.Called()

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}
