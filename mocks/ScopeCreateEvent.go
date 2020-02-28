package mocks

import mock "github.com/stretchr/testify/mock"

// ScopeCreateEvent is an autogenerated mock type for the ScopeCreateEvent type
type ScopeCreateEvent struct {
	mock.Mock
}

// GetManifestId provides a mock function with given fields:
func (_m *ScopeCreateEvent) GetManifestId() (uint64, error) {
	ret := _m.Called()

	var r0 uint64
	if rf, ok := ret.Get(0).(func() uint64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetScopeId provides a mock function with given fields:
func (_m *ScopeCreateEvent) GetScopeId() (uint32, error) {
	ret := _m.Called()

	var r0 uint32
	if rf, ok := ret.Get(0).(func() uint32); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint32)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetSystemEventName provides a mock function with given fields:
func (_m *ScopeCreateEvent) GetSystemEventName() (string, error) {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
