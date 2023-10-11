// Code generated by mockery v2.32.0. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"

	sync "sync"
)

// BackfillReplSvc is an autogenerated mock type for the BackfillReplSvc type
type BackfillReplSvc struct {
	mock.Mock
}

// AddBackfillReplSpec provides a mock function with given fields: spec
func (_m *BackfillReplSvc) AddBackfillReplSpec(spec *metadata.BackfillReplicationSpec) error {
	ret := _m.Called(spec)

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.BackfillReplicationSpec) error); ok {
		r0 = rf(spec)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// AllActiveBackfillSpecsReadOnly provides a mock function with given fields:
func (_m *BackfillReplSvc) AllActiveBackfillSpecsReadOnly() (map[string]*metadata.BackfillReplicationSpec, error) {
	ret := _m.Called()

	var r0 map[string]*metadata.BackfillReplicationSpec
	var r1 error
	if rf, ok := ret.Get(0).(func() (map[string]*metadata.BackfillReplicationSpec, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() map[string]*metadata.BackfillReplicationSpec); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]*metadata.BackfillReplicationSpec)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BackfillReplSpec provides a mock function with given fields: replicationId
func (_m *BackfillReplSvc) BackfillReplSpec(replicationId string) (*metadata.BackfillReplicationSpec, error) {
	ret := _m.Called(replicationId)

	var r0 *metadata.BackfillReplicationSpec
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (*metadata.BackfillReplicationSpec, error)); ok {
		return rf(replicationId)
	}
	if rf, ok := ret.Get(0).(func(string) *metadata.BackfillReplicationSpec); ok {
		r0 = rf(replicationId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.BackfillReplicationSpec)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(replicationId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// DelBackfillReplSpec provides a mock function with given fields: replicationId
func (_m *BackfillReplSvc) DelBackfillReplSpec(replicationId string) (*metadata.BackfillReplicationSpec, error) {
	ret := _m.Called(replicationId)

	var r0 *metadata.BackfillReplicationSpec
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (*metadata.BackfillReplicationSpec, error)); ok {
		return rf(replicationId)
	}
	if rf, ok := ret.Get(0).(func(string) *metadata.BackfillReplicationSpec); ok {
		r0 = rf(replicationId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.BackfillReplicationSpec)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(replicationId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReplicationSpecChangeCallback provides a mock function with given fields: id, oldVal, newVal, wg
func (_m *BackfillReplSvc) ReplicationSpecChangeCallback(id string, oldVal interface{}, newVal interface{}, wg *sync.WaitGroup) error {
	ret := _m.Called(id, oldVal, newVal, wg)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, interface{}, interface{}, *sync.WaitGroup) error); ok {
		r0 = rf(id, oldVal, newVal, wg)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetBackfillReplSpec provides a mock function with given fields: spec
func (_m *BackfillReplSvc) SetBackfillReplSpec(spec *metadata.BackfillReplicationSpec) error {
	ret := _m.Called(spec)

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.BackfillReplicationSpec) error); ok {
		r0 = rf(spec)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetCompleteBackfillRaiser provides a mock function with given fields: backfillCallback
func (_m *BackfillReplSvc) SetCompleteBackfillRaiser(backfillCallback func(string) error) error {
	ret := _m.Called(backfillCallback)

	var r0 error
	if rf, ok := ret.Get(0).(func(func(string) error) error); ok {
		r0 = rf(backfillCallback)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetMetadataChangeHandlerCallback provides a mock function with given fields: callBack
func (_m *BackfillReplSvc) SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback) {
	_m.Called(callBack)
}

// NewBackfillReplSvc creates a new instance of BackfillReplSvc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewBackfillReplSvc(t interface {
	mock.TestingT
	Cleanup(func())
}) *BackfillReplSvc {
	mock := &BackfillReplSvc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
