// Code generated by mockery v2.43.2. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"

	sync "sync"
)

// CollectionsManifestSvc is an autogenerated mock type for the CollectionsManifestSvc type
type CollectionsManifestSvc struct {
	mock.Mock
}

type CollectionsManifestSvc_Expecter struct {
	mock *mock.Mock
}

func (_m *CollectionsManifestSvc) EXPECT() *CollectionsManifestSvc_Expecter {
	return &CollectionsManifestSvc_Expecter{mock: &_m.Mock}
}

// CollectionManifestGetter provides a mock function with given fields: bucketName
func (_m *CollectionsManifestSvc) CollectionManifestGetter(bucketName string) (*metadata.CollectionsManifest, error) {
	ret := _m.Called(bucketName)

	if len(ret) == 0 {
		panic("no return value specified for CollectionManifestGetter")
	}

	var r0 *metadata.CollectionsManifest
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (*metadata.CollectionsManifest, error)); ok {
		return rf(bucketName)
	}
	if rf, ok := ret.Get(0).(func(string) *metadata.CollectionsManifest); ok {
		r0 = rf(bucketName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifest)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(bucketName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CollectionsManifestSvc_CollectionManifestGetter_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CollectionManifestGetter'
type CollectionsManifestSvc_CollectionManifestGetter_Call struct {
	*mock.Call
}

// CollectionManifestGetter is a helper method to define mock.On call
//   - bucketName string
func (_e *CollectionsManifestSvc_Expecter) CollectionManifestGetter(bucketName interface{}) *CollectionsManifestSvc_CollectionManifestGetter_Call {
	return &CollectionsManifestSvc_CollectionManifestGetter_Call{Call: _e.mock.On("CollectionManifestGetter", bucketName)}
}

func (_c *CollectionsManifestSvc_CollectionManifestGetter_Call) Run(run func(bucketName string)) *CollectionsManifestSvc_CollectionManifestGetter_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *CollectionsManifestSvc_CollectionManifestGetter_Call) Return(_a0 *metadata.CollectionsManifest, _a1 error) *CollectionsManifestSvc_CollectionManifestGetter_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CollectionsManifestSvc_CollectionManifestGetter_Call) RunAndReturn(run func(string) (*metadata.CollectionsManifest, error)) *CollectionsManifestSvc_CollectionManifestGetter_Call {
	_c.Call.Return(run)
	return _c
}

// ForceTargetManifestRefresh provides a mock function with given fields: spec
func (_m *CollectionsManifestSvc) ForceTargetManifestRefresh(spec *metadata.ReplicationSpecification) error {
	ret := _m.Called(spec)

	if len(ret) == 0 {
		panic("no return value specified for ForceTargetManifestRefresh")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification) error); ok {
		r0 = rf(spec)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CollectionsManifestSvc_ForceTargetManifestRefresh_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ForceTargetManifestRefresh'
type CollectionsManifestSvc_ForceTargetManifestRefresh_Call struct {
	*mock.Call
}

// ForceTargetManifestRefresh is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
func (_e *CollectionsManifestSvc_Expecter) ForceTargetManifestRefresh(spec interface{}) *CollectionsManifestSvc_ForceTargetManifestRefresh_Call {
	return &CollectionsManifestSvc_ForceTargetManifestRefresh_Call{Call: _e.mock.On("ForceTargetManifestRefresh", spec)}
}

func (_c *CollectionsManifestSvc_ForceTargetManifestRefresh_Call) Run(run func(spec *metadata.ReplicationSpecification)) *CollectionsManifestSvc_ForceTargetManifestRefresh_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification))
	})
	return _c
}

func (_c *CollectionsManifestSvc_ForceTargetManifestRefresh_Call) Return(_a0 error) *CollectionsManifestSvc_ForceTargetManifestRefresh_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CollectionsManifestSvc_ForceTargetManifestRefresh_Call) RunAndReturn(run func(*metadata.ReplicationSpecification) error) *CollectionsManifestSvc_ForceTargetManifestRefresh_Call {
	_c.Call.Return(run)
	return _c
}

// GetAllCachedManifests provides a mock function with given fields: spec
func (_m *CollectionsManifestSvc) GetAllCachedManifests(spec *metadata.ReplicationSpecification) (map[uint64]*metadata.CollectionsManifest, map[uint64]*metadata.CollectionsManifest, error) {
	ret := _m.Called(spec)

	if len(ret) == 0 {
		panic("no return value specified for GetAllCachedManifests")
	}

	var r0 map[uint64]*metadata.CollectionsManifest
	var r1 map[uint64]*metadata.CollectionsManifest
	var r2 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification) (map[uint64]*metadata.CollectionsManifest, map[uint64]*metadata.CollectionsManifest, error)); ok {
		return rf(spec)
	}
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification) map[uint64]*metadata.CollectionsManifest); ok {
		r0 = rf(spec)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[uint64]*metadata.CollectionsManifest)
		}
	}

	if rf, ok := ret.Get(1).(func(*metadata.ReplicationSpecification) map[uint64]*metadata.CollectionsManifest); ok {
		r1 = rf(spec)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(map[uint64]*metadata.CollectionsManifest)
		}
	}

	if rf, ok := ret.Get(2).(func(*metadata.ReplicationSpecification) error); ok {
		r2 = rf(spec)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// CollectionsManifestSvc_GetAllCachedManifests_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetAllCachedManifests'
type CollectionsManifestSvc_GetAllCachedManifests_Call struct {
	*mock.Call
}

// GetAllCachedManifests is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
func (_e *CollectionsManifestSvc_Expecter) GetAllCachedManifests(spec interface{}) *CollectionsManifestSvc_GetAllCachedManifests_Call {
	return &CollectionsManifestSvc_GetAllCachedManifests_Call{Call: _e.mock.On("GetAllCachedManifests", spec)}
}

func (_c *CollectionsManifestSvc_GetAllCachedManifests_Call) Run(run func(spec *metadata.ReplicationSpecification)) *CollectionsManifestSvc_GetAllCachedManifests_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification))
	})
	return _c
}

func (_c *CollectionsManifestSvc_GetAllCachedManifests_Call) Return(_a0 map[uint64]*metadata.CollectionsManifest, _a1 map[uint64]*metadata.CollectionsManifest, _a2 error) *CollectionsManifestSvc_GetAllCachedManifests_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *CollectionsManifestSvc_GetAllCachedManifests_Call) RunAndReturn(run func(*metadata.ReplicationSpecification) (map[uint64]*metadata.CollectionsManifest, map[uint64]*metadata.CollectionsManifest, error)) *CollectionsManifestSvc_GetAllCachedManifests_Call {
	_c.Call.Return(run)
	return _c
}

// GetLastPersistedManifests provides a mock function with given fields: spec
func (_m *CollectionsManifestSvc) GetLastPersistedManifests(spec *metadata.ReplicationSpecification) (*metadata.CollectionsManifestPair, error) {
	ret := _m.Called(spec)

	if len(ret) == 0 {
		panic("no return value specified for GetLastPersistedManifests")
	}

	var r0 *metadata.CollectionsManifestPair
	var r1 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification) (*metadata.CollectionsManifestPair, error)); ok {
		return rf(spec)
	}
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification) *metadata.CollectionsManifestPair); ok {
		r0 = rf(spec)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifestPair)
		}
	}

	if rf, ok := ret.Get(1).(func(*metadata.ReplicationSpecification) error); ok {
		r1 = rf(spec)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CollectionsManifestSvc_GetLastPersistedManifests_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetLastPersistedManifests'
type CollectionsManifestSvc_GetLastPersistedManifests_Call struct {
	*mock.Call
}

// GetLastPersistedManifests is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
func (_e *CollectionsManifestSvc_Expecter) GetLastPersistedManifests(spec interface{}) *CollectionsManifestSvc_GetLastPersistedManifests_Call {
	return &CollectionsManifestSvc_GetLastPersistedManifests_Call{Call: _e.mock.On("GetLastPersistedManifests", spec)}
}

func (_c *CollectionsManifestSvc_GetLastPersistedManifests_Call) Run(run func(spec *metadata.ReplicationSpecification)) *CollectionsManifestSvc_GetLastPersistedManifests_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification))
	})
	return _c
}

func (_c *CollectionsManifestSvc_GetLastPersistedManifests_Call) Return(_a0 *metadata.CollectionsManifestPair, _a1 error) *CollectionsManifestSvc_GetLastPersistedManifests_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CollectionsManifestSvc_GetLastPersistedManifests_Call) RunAndReturn(run func(*metadata.ReplicationSpecification) (*metadata.CollectionsManifestPair, error)) *CollectionsManifestSvc_GetLastPersistedManifests_Call {
	_c.Call.Return(run)
	return _c
}

// GetLatestManifests provides a mock function with given fields: spec, specMayNotExist
func (_m *CollectionsManifestSvc) GetLatestManifests(spec *metadata.ReplicationSpecification, specMayNotExist bool) (*metadata.CollectionsManifest, *metadata.CollectionsManifest, error) {
	ret := _m.Called(spec, specMayNotExist)

	if len(ret) == 0 {
		panic("no return value specified for GetLatestManifests")
	}

	var r0 *metadata.CollectionsManifest
	var r1 *metadata.CollectionsManifest
	var r2 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, bool) (*metadata.CollectionsManifest, *metadata.CollectionsManifest, error)); ok {
		return rf(spec, specMayNotExist)
	}
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, bool) *metadata.CollectionsManifest); ok {
		r0 = rf(spec, specMayNotExist)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifest)
		}
	}

	if rf, ok := ret.Get(1).(func(*metadata.ReplicationSpecification, bool) *metadata.CollectionsManifest); ok {
		r1 = rf(spec, specMayNotExist)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(*metadata.CollectionsManifest)
		}
	}

	if rf, ok := ret.Get(2).(func(*metadata.ReplicationSpecification, bool) error); ok {
		r2 = rf(spec, specMayNotExist)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// CollectionsManifestSvc_GetLatestManifests_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetLatestManifests'
type CollectionsManifestSvc_GetLatestManifests_Call struct {
	*mock.Call
}

// GetLatestManifests is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - specMayNotExist bool
func (_e *CollectionsManifestSvc_Expecter) GetLatestManifests(spec interface{}, specMayNotExist interface{}) *CollectionsManifestSvc_GetLatestManifests_Call {
	return &CollectionsManifestSvc_GetLatestManifests_Call{Call: _e.mock.On("GetLatestManifests", spec, specMayNotExist)}
}

func (_c *CollectionsManifestSvc_GetLatestManifests_Call) Run(run func(spec *metadata.ReplicationSpecification, specMayNotExist bool)) *CollectionsManifestSvc_GetLatestManifests_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(bool))
	})
	return _c
}

func (_c *CollectionsManifestSvc_GetLatestManifests_Call) Return(src *metadata.CollectionsManifest, tgt *metadata.CollectionsManifest, err error) *CollectionsManifestSvc_GetLatestManifests_Call {
	_c.Call.Return(src, tgt, err)
	return _c
}

func (_c *CollectionsManifestSvc_GetLatestManifests_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, bool) (*metadata.CollectionsManifest, *metadata.CollectionsManifest, error)) *CollectionsManifestSvc_GetLatestManifests_Call {
	_c.Call.Return(run)
	return _c
}

// GetSpecificSourceManifest provides a mock function with given fields: spec, manifestVersion
func (_m *CollectionsManifestSvc) GetSpecificSourceManifest(spec *metadata.ReplicationSpecification, manifestVersion uint64) (*metadata.CollectionsManifest, error) {
	ret := _m.Called(spec, manifestVersion)

	if len(ret) == 0 {
		panic("no return value specified for GetSpecificSourceManifest")
	}

	var r0 *metadata.CollectionsManifest
	var r1 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, uint64) (*metadata.CollectionsManifest, error)); ok {
		return rf(spec, manifestVersion)
	}
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, uint64) *metadata.CollectionsManifest); ok {
		r0 = rf(spec, manifestVersion)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifest)
		}
	}

	if rf, ok := ret.Get(1).(func(*metadata.ReplicationSpecification, uint64) error); ok {
		r1 = rf(spec, manifestVersion)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CollectionsManifestSvc_GetSpecificSourceManifest_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSpecificSourceManifest'
type CollectionsManifestSvc_GetSpecificSourceManifest_Call struct {
	*mock.Call
}

// GetSpecificSourceManifest is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - manifestVersion uint64
func (_e *CollectionsManifestSvc_Expecter) GetSpecificSourceManifest(spec interface{}, manifestVersion interface{}) *CollectionsManifestSvc_GetSpecificSourceManifest_Call {
	return &CollectionsManifestSvc_GetSpecificSourceManifest_Call{Call: _e.mock.On("GetSpecificSourceManifest", spec, manifestVersion)}
}

func (_c *CollectionsManifestSvc_GetSpecificSourceManifest_Call) Run(run func(spec *metadata.ReplicationSpecification, manifestVersion uint64)) *CollectionsManifestSvc_GetSpecificSourceManifest_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(uint64))
	})
	return _c
}

func (_c *CollectionsManifestSvc_GetSpecificSourceManifest_Call) Return(_a0 *metadata.CollectionsManifest, _a1 error) *CollectionsManifestSvc_GetSpecificSourceManifest_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CollectionsManifestSvc_GetSpecificSourceManifest_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, uint64) (*metadata.CollectionsManifest, error)) *CollectionsManifestSvc_GetSpecificSourceManifest_Call {
	_c.Call.Return(run)
	return _c
}

// GetSpecificTargetManifest provides a mock function with given fields: spec, manifestVersion
func (_m *CollectionsManifestSvc) GetSpecificTargetManifest(spec *metadata.ReplicationSpecification, manifestVersion uint64) (*metadata.CollectionsManifest, error) {
	ret := _m.Called(spec, manifestVersion)

	if len(ret) == 0 {
		panic("no return value specified for GetSpecificTargetManifest")
	}

	var r0 *metadata.CollectionsManifest
	var r1 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, uint64) (*metadata.CollectionsManifest, error)); ok {
		return rf(spec, manifestVersion)
	}
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, uint64) *metadata.CollectionsManifest); ok {
		r0 = rf(spec, manifestVersion)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifest)
		}
	}

	if rf, ok := ret.Get(1).(func(*metadata.ReplicationSpecification, uint64) error); ok {
		r1 = rf(spec, manifestVersion)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CollectionsManifestSvc_GetSpecificTargetManifest_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSpecificTargetManifest'
type CollectionsManifestSvc_GetSpecificTargetManifest_Call struct {
	*mock.Call
}

// GetSpecificTargetManifest is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - manifestVersion uint64
func (_e *CollectionsManifestSvc_Expecter) GetSpecificTargetManifest(spec interface{}, manifestVersion interface{}) *CollectionsManifestSvc_GetSpecificTargetManifest_Call {
	return &CollectionsManifestSvc_GetSpecificTargetManifest_Call{Call: _e.mock.On("GetSpecificTargetManifest", spec, manifestVersion)}
}

func (_c *CollectionsManifestSvc_GetSpecificTargetManifest_Call) Run(run func(spec *metadata.ReplicationSpecification, manifestVersion uint64)) *CollectionsManifestSvc_GetSpecificTargetManifest_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(uint64))
	})
	return _c
}

func (_c *CollectionsManifestSvc_GetSpecificTargetManifest_Call) Return(_a0 *metadata.CollectionsManifest, _a1 error) *CollectionsManifestSvc_GetSpecificTargetManifest_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CollectionsManifestSvc_GetSpecificTargetManifest_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, uint64) (*metadata.CollectionsManifest, error)) *CollectionsManifestSvc_GetSpecificTargetManifest_Call {
	_c.Call.Return(run)
	return _c
}

// PersistNeededManifests provides a mock function with given fields: spec
func (_m *CollectionsManifestSvc) PersistNeededManifests(spec *metadata.ReplicationSpecification) error {
	ret := _m.Called(spec)

	if len(ret) == 0 {
		panic("no return value specified for PersistNeededManifests")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification) error); ok {
		r0 = rf(spec)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CollectionsManifestSvc_PersistNeededManifests_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'PersistNeededManifests'
type CollectionsManifestSvc_PersistNeededManifests_Call struct {
	*mock.Call
}

// PersistNeededManifests is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
func (_e *CollectionsManifestSvc_Expecter) PersistNeededManifests(spec interface{}) *CollectionsManifestSvc_PersistNeededManifests_Call {
	return &CollectionsManifestSvc_PersistNeededManifests_Call{Call: _e.mock.On("PersistNeededManifests", spec)}
}

func (_c *CollectionsManifestSvc_PersistNeededManifests_Call) Run(run func(spec *metadata.ReplicationSpecification)) *CollectionsManifestSvc_PersistNeededManifests_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification))
	})
	return _c
}

func (_c *CollectionsManifestSvc_PersistNeededManifests_Call) Return(_a0 error) *CollectionsManifestSvc_PersistNeededManifests_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CollectionsManifestSvc_PersistNeededManifests_Call) RunAndReturn(run func(*metadata.ReplicationSpecification) error) *CollectionsManifestSvc_PersistNeededManifests_Call {
	_c.Call.Return(run)
	return _c
}

// PersistReceivedManifests provides a mock function with given fields: spec, srcManifests, tgtManifests
func (_m *CollectionsManifestSvc) PersistReceivedManifests(spec *metadata.ReplicationSpecification, srcManifests map[uint64]*metadata.CollectionsManifest, tgtManifests map[uint64]*metadata.CollectionsManifest) error {
	ret := _m.Called(spec, srcManifests, tgtManifests)

	if len(ret) == 0 {
		panic("no return value specified for PersistReceivedManifests")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, map[uint64]*metadata.CollectionsManifest, map[uint64]*metadata.CollectionsManifest) error); ok {
		r0 = rf(spec, srcManifests, tgtManifests)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CollectionsManifestSvc_PersistReceivedManifests_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'PersistReceivedManifests'
type CollectionsManifestSvc_PersistReceivedManifests_Call struct {
	*mock.Call
}

// PersistReceivedManifests is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - srcManifests map[uint64]*metadata.CollectionsManifest
//   - tgtManifests map[uint64]*metadata.CollectionsManifest
func (_e *CollectionsManifestSvc_Expecter) PersistReceivedManifests(spec interface{}, srcManifests interface{}, tgtManifests interface{}) *CollectionsManifestSvc_PersistReceivedManifests_Call {
	return &CollectionsManifestSvc_PersistReceivedManifests_Call{Call: _e.mock.On("PersistReceivedManifests", spec, srcManifests, tgtManifests)}
}

func (_c *CollectionsManifestSvc_PersistReceivedManifests_Call) Run(run func(spec *metadata.ReplicationSpecification, srcManifests map[uint64]*metadata.CollectionsManifest, tgtManifests map[uint64]*metadata.CollectionsManifest)) *CollectionsManifestSvc_PersistReceivedManifests_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(map[uint64]*metadata.CollectionsManifest), args[2].(map[uint64]*metadata.CollectionsManifest))
	})
	return _c
}

func (_c *CollectionsManifestSvc_PersistReceivedManifests_Call) Return(_a0 error) *CollectionsManifestSvc_PersistReceivedManifests_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CollectionsManifestSvc_PersistReceivedManifests_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, map[uint64]*metadata.CollectionsManifest, map[uint64]*metadata.CollectionsManifest) error) *CollectionsManifestSvc_PersistReceivedManifests_Call {
	_c.Call.Return(run)
	return _c
}

// ReplicationSpecChangeCallback provides a mock function with given fields: id, oldVal, newVal, wg
func (_m *CollectionsManifestSvc) ReplicationSpecChangeCallback(id string, oldVal interface{}, newVal interface{}, wg *sync.WaitGroup) error {
	ret := _m.Called(id, oldVal, newVal, wg)

	if len(ret) == 0 {
		panic("no return value specified for ReplicationSpecChangeCallback")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, interface{}, interface{}, *sync.WaitGroup) error); ok {
		r0 = rf(id, oldVal, newVal, wg)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CollectionsManifestSvc_ReplicationSpecChangeCallback_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ReplicationSpecChangeCallback'
type CollectionsManifestSvc_ReplicationSpecChangeCallback_Call struct {
	*mock.Call
}

// ReplicationSpecChangeCallback is a helper method to define mock.On call
//   - id string
//   - oldVal interface{}
//   - newVal interface{}
//   - wg *sync.WaitGroup
func (_e *CollectionsManifestSvc_Expecter) ReplicationSpecChangeCallback(id interface{}, oldVal interface{}, newVal interface{}, wg interface{}) *CollectionsManifestSvc_ReplicationSpecChangeCallback_Call {
	return &CollectionsManifestSvc_ReplicationSpecChangeCallback_Call{Call: _e.mock.On("ReplicationSpecChangeCallback", id, oldVal, newVal, wg)}
}

func (_c *CollectionsManifestSvc_ReplicationSpecChangeCallback_Call) Run(run func(id string, oldVal interface{}, newVal interface{}, wg *sync.WaitGroup)) *CollectionsManifestSvc_ReplicationSpecChangeCallback_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(interface{}), args[2].(interface{}), args[3].(*sync.WaitGroup))
	})
	return _c
}

func (_c *CollectionsManifestSvc_ReplicationSpecChangeCallback_Call) Return(_a0 error) *CollectionsManifestSvc_ReplicationSpecChangeCallback_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CollectionsManifestSvc_ReplicationSpecChangeCallback_Call) RunAndReturn(run func(string, interface{}, interface{}, *sync.WaitGroup) error) *CollectionsManifestSvc_ReplicationSpecChangeCallback_Call {
	_c.Call.Return(run)
	return _c
}

// SetMetadataChangeHandlerCallback provides a mock function with given fields: callBack
func (_m *CollectionsManifestSvc) SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback) {
	_m.Called(callBack)
}

// CollectionsManifestSvc_SetMetadataChangeHandlerCallback_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetMetadataChangeHandlerCallback'
type CollectionsManifestSvc_SetMetadataChangeHandlerCallback_Call struct {
	*mock.Call
}

// SetMetadataChangeHandlerCallback is a helper method to define mock.On call
//   - callBack base.MetadataChangeHandlerCallback
func (_e *CollectionsManifestSvc_Expecter) SetMetadataChangeHandlerCallback(callBack interface{}) *CollectionsManifestSvc_SetMetadataChangeHandlerCallback_Call {
	return &CollectionsManifestSvc_SetMetadataChangeHandlerCallback_Call{Call: _e.mock.On("SetMetadataChangeHandlerCallback", callBack)}
}

func (_c *CollectionsManifestSvc_SetMetadataChangeHandlerCallback_Call) Run(run func(callBack base.MetadataChangeHandlerCallback)) *CollectionsManifestSvc_SetMetadataChangeHandlerCallback_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(base.MetadataChangeHandlerCallback))
	})
	return _c
}

func (_c *CollectionsManifestSvc_SetMetadataChangeHandlerCallback_Call) Return() *CollectionsManifestSvc_SetMetadataChangeHandlerCallback_Call {
	_c.Call.Return()
	return _c
}

func (_c *CollectionsManifestSvc_SetMetadataChangeHandlerCallback_Call) RunAndReturn(run func(base.MetadataChangeHandlerCallback)) *CollectionsManifestSvc_SetMetadataChangeHandlerCallback_Call {
	_c.Call.Return(run)
	return _c
}

// NewCollectionsManifestSvc creates a new instance of CollectionsManifestSvc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewCollectionsManifestSvc(t interface {
	mock.TestingT
	Cleanup(func())
}) *CollectionsManifestSvc {
	mock := &CollectionsManifestSvc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
