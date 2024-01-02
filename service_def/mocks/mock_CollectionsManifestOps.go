// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/metadata"
	mock "github.com/stretchr/testify/mock"
)

// CollectionsManifestOps is an autogenerated mock type for the CollectionsManifestOps type
type CollectionsManifestOps struct {
	mock.Mock
}

type CollectionsManifestOps_Expecter struct {
	mock *mock.Mock
}

func (_m *CollectionsManifestOps) EXPECT() *CollectionsManifestOps_Expecter {
	return &CollectionsManifestOps_Expecter{mock: &_m.Mock}
}

// CollectionManifestGetter provides a mock function with given fields: bucketName, hasStoredManifest, storedManifestUid, spec
func (_m *CollectionsManifestOps) CollectionManifestGetter(bucketName string, hasStoredManifest bool, storedManifestUid uint64, spec *metadata.ReplicationSpecification) (*metadata.CollectionsManifest, error) {
	ret := _m.Called(bucketName, hasStoredManifest, storedManifestUid, spec)

	var r0 *metadata.CollectionsManifest
	var r1 error
	if rf, ok := ret.Get(0).(func(string, bool, uint64, *metadata.ReplicationSpecification) (*metadata.CollectionsManifest, error)); ok {
		return rf(bucketName, hasStoredManifest, storedManifestUid, spec)
	}
	if rf, ok := ret.Get(0).(func(string, bool, uint64, *metadata.ReplicationSpecification) *metadata.CollectionsManifest); ok {
		r0 = rf(bucketName, hasStoredManifest, storedManifestUid, spec)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifest)
		}
	}

	if rf, ok := ret.Get(1).(func(string, bool, uint64, *metadata.ReplicationSpecification) error); ok {
		r1 = rf(bucketName, hasStoredManifest, storedManifestUid, spec)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CollectionsManifestOps_CollectionManifestGetter_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CollectionManifestGetter'
type CollectionsManifestOps_CollectionManifestGetter_Call struct {
	*mock.Call
}

// CollectionManifestGetter is a helper method to define mock.On call
//   - bucketName string
//   - hasStoredManifest bool
//   - storedManifestUid uint64
//   - spec *metadata.ReplicationSpecification
func (_e *CollectionsManifestOps_Expecter) CollectionManifestGetter(bucketName interface{}, hasStoredManifest interface{}, storedManifestUid interface{}, spec interface{}) *CollectionsManifestOps_CollectionManifestGetter_Call {
	return &CollectionsManifestOps_CollectionManifestGetter_Call{Call: _e.mock.On("CollectionManifestGetter", bucketName, hasStoredManifest, storedManifestUid, spec)}
}

func (_c *CollectionsManifestOps_CollectionManifestGetter_Call) Run(run func(bucketName string, hasStoredManifest bool, storedManifestUid uint64, spec *metadata.ReplicationSpecification)) *CollectionsManifestOps_CollectionManifestGetter_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(bool), args[2].(uint64), args[3].(*metadata.ReplicationSpecification))
	})
	return _c
}

func (_c *CollectionsManifestOps_CollectionManifestGetter_Call) Return(_a0 *metadata.CollectionsManifest, _a1 error) *CollectionsManifestOps_CollectionManifestGetter_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CollectionsManifestOps_CollectionManifestGetter_Call) RunAndReturn(run func(string, bool, uint64, *metadata.ReplicationSpecification) (*metadata.CollectionsManifest, error)) *CollectionsManifestOps_CollectionManifestGetter_Call {
	_c.Call.Return(run)
	return _c
}

// NewCollectionsManifestOps creates a new instance of CollectionsManifestOps. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewCollectionsManifestOps(t interface {
	mock.TestingT
	Cleanup(func())
}) *CollectionsManifestOps {
	mock := &CollectionsManifestOps{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}