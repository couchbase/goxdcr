// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/metadata"
	mock "github.com/stretchr/testify/mock"
)

// CollectionsManifestReqFunc is an autogenerated mock type for the CollectionsManifestReqFunc type
type CollectionsManifestReqFunc struct {
	mock.Mock
}

type CollectionsManifestReqFunc_Expecter struct {
	mock *mock.Mock
}

func (_m *CollectionsManifestReqFunc) EXPECT() *CollectionsManifestReqFunc_Expecter {
	return &CollectionsManifestReqFunc_Expecter{mock: &_m.Mock}
}

// Execute provides a mock function with given fields: manifestUid
func (_m *CollectionsManifestReqFunc) Execute(manifestUid uint64) (*metadata.CollectionsManifest, error) {
	ret := _m.Called(manifestUid)

	if len(ret) == 0 {
		panic("no return value specified for Execute")
	}

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

// CollectionsManifestReqFunc_Execute_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Execute'
type CollectionsManifestReqFunc_Execute_Call struct {
	*mock.Call
}

// Execute is a helper method to define mock.On call
//   - manifestUid uint64
func (_e *CollectionsManifestReqFunc_Expecter) Execute(manifestUid interface{}) *CollectionsManifestReqFunc_Execute_Call {
	return &CollectionsManifestReqFunc_Execute_Call{Call: _e.mock.On("Execute", manifestUid)}
}

func (_c *CollectionsManifestReqFunc_Execute_Call) Run(run func(manifestUid uint64)) *CollectionsManifestReqFunc_Execute_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint64))
	})
	return _c
}

func (_c *CollectionsManifestReqFunc_Execute_Call) Return(_a0 *metadata.CollectionsManifest, _a1 error) *CollectionsManifestReqFunc_Execute_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CollectionsManifestReqFunc_Execute_Call) RunAndReturn(run func(uint64) (*metadata.CollectionsManifest, error)) *CollectionsManifestReqFunc_Execute_Call {
	_c.Call.Return(run)
	return _c
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
