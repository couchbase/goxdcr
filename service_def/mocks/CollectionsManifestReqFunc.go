// Code generated by mockery v2.32.0. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/metadata"
	mock "github.com/stretchr/testify/mock"
)

// CollectionsManifestReqFunc is an autogenerated mock type for the CollectionsManifestReqFunc type
type CollectionsManifestReqFunc struct {
	mock.Mock
}

// Execute provides a mock function with given fields: manifestUid
func (_m *CollectionsManifestReqFunc) Execute(manifestUid uint64) (*metadata.CollectionsManifest, error) {
	ret := _m.Called(manifestUid)

	var r0 *metadata.CollectionsManifest
	var r1 error
	if rf, ok := ret.Get(0).(func(uint64) (*metadata.CollectionsManifest, error)); ok {
		return rf(manifestUid)
	}
	if rf, ok := ret.Get(0).(func(uint64) *metadata.CollectionsManifest); ok {
		r0 = rf(manifestUid)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifest)
		}
	}

	if rf, ok := ret.Get(1).(func(uint64) error); ok {
		r1 = rf(manifestUid)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewCollectionsManifestReqFunc creates a new instance of CollectionsManifestReqFunc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewCollectionsManifestReqFunc(t interface {
	mock.TestingT
	Cleanup(func())
}) *CollectionsManifestReqFunc {
	mock := &CollectionsManifestReqFunc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
