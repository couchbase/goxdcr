// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"
)

// CheckpointsServiceCache is an autogenerated mock type for the CheckpointsServiceCache type
type CheckpointsServiceCache struct {
	mock.Mock
}

type CheckpointsServiceCache_Expecter struct {
	mock *mock.Mock
}

func (_m *CheckpointsServiceCache) EXPECT() *CheckpointsServiceCache_Expecter {
	return &CheckpointsServiceCache_Expecter{mock: &_m.Mock}
}

// GetLatestDocs provides a mock function with given fields:
func (_m *CheckpointsServiceCache) GetLatestDocs() (metadata.VBsCkptsDocMap, error) {
	ret := _m.Called()

	var r0 metadata.VBsCkptsDocMap
	var r1 error
	if rf, ok := ret.Get(0).(func() (metadata.VBsCkptsDocMap, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() metadata.VBsCkptsDocMap); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(metadata.VBsCkptsDocMap)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CheckpointsServiceCache_GetLatestDocs_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetLatestDocs'
type CheckpointsServiceCache_GetLatestDocs_Call struct {
	*mock.Call
}

// GetLatestDocs is a helper method to define mock.On call
func (_e *CheckpointsServiceCache_Expecter) GetLatestDocs() *CheckpointsServiceCache_GetLatestDocs_Call {
	return &CheckpointsServiceCache_GetLatestDocs_Call{Call: _e.mock.On("GetLatestDocs")}
}

func (_c *CheckpointsServiceCache_GetLatestDocs_Call) Run(run func()) *CheckpointsServiceCache_GetLatestDocs_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CheckpointsServiceCache_GetLatestDocs_Call) Return(_a0 metadata.VBsCkptsDocMap, _a1 error) *CheckpointsServiceCache_GetLatestDocs_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CheckpointsServiceCache_GetLatestDocs_Call) RunAndReturn(run func() (metadata.VBsCkptsDocMap, error)) *CheckpointsServiceCache_GetLatestDocs_Call {
	_c.Call.Return(run)
	return _c
}

// GetOneVBDoc provides a mock function with given fields: vbno
func (_m *CheckpointsServiceCache) GetOneVBDoc(vbno uint16) (*metadata.CheckpointsDoc, error) {
	ret := _m.Called(vbno)

	var r0 *metadata.CheckpointsDoc
	var r1 error
	if rf, ok := ret.Get(0).(func(uint16) (*metadata.CheckpointsDoc, error)); ok {
		return rf(vbno)
	}
	if rf, ok := ret.Get(0).(func(uint16) *metadata.CheckpointsDoc); ok {
		r0 = rf(vbno)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CheckpointsDoc)
		}
	}

	if rf, ok := ret.Get(1).(func(uint16) error); ok {
		r1 = rf(vbno)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CheckpointsServiceCache_GetOneVBDoc_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetOneVBDoc'
type CheckpointsServiceCache_GetOneVBDoc_Call struct {
	*mock.Call
}

// GetOneVBDoc is a helper method to define mock.On call
//   - vbno uint16
func (_e *CheckpointsServiceCache_Expecter) GetOneVBDoc(vbno interface{}) *CheckpointsServiceCache_GetOneVBDoc_Call {
	return &CheckpointsServiceCache_GetOneVBDoc_Call{Call: _e.mock.On("GetOneVBDoc", vbno)}
}

func (_c *CheckpointsServiceCache_GetOneVBDoc_Call) Run(run func(vbno uint16)) *CheckpointsServiceCache_GetOneVBDoc_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint16))
	})
	return _c
}

func (_c *CheckpointsServiceCache_GetOneVBDoc_Call) Return(_a0 *metadata.CheckpointsDoc, _a1 error) *CheckpointsServiceCache_GetOneVBDoc_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CheckpointsServiceCache_GetOneVBDoc_Call) RunAndReturn(run func(uint16) (*metadata.CheckpointsDoc, error)) *CheckpointsServiceCache_GetOneVBDoc_Call {
	_c.Call.Return(run)
	return _c
}

// InvalidateCache provides a mock function with given fields:
func (_m *CheckpointsServiceCache) InvalidateCache() {
	_m.Called()
}

// CheckpointsServiceCache_InvalidateCache_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'InvalidateCache'
type CheckpointsServiceCache_InvalidateCache_Call struct {
	*mock.Call
}

// InvalidateCache is a helper method to define mock.On call
func (_e *CheckpointsServiceCache_Expecter) InvalidateCache() *CheckpointsServiceCache_InvalidateCache_Call {
	return &CheckpointsServiceCache_InvalidateCache_Call{Call: _e.mock.On("InvalidateCache")}
}

func (_c *CheckpointsServiceCache_InvalidateCache_Call) Run(run func()) *CheckpointsServiceCache_InvalidateCache_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CheckpointsServiceCache_InvalidateCache_Call) Return() *CheckpointsServiceCache_InvalidateCache_Call {
	_c.Call.Return()
	return _c
}

func (_c *CheckpointsServiceCache_InvalidateCache_Call) RunAndReturn(run func()) *CheckpointsServiceCache_InvalidateCache_Call {
	_c.Call.Return(run)
	return _c
}

// SpecChangeCb provides a mock function with given fields: oldSpec, newSpec
func (_m *CheckpointsServiceCache) SpecChangeCb(oldSpec *metadata.ReplicationSpecification, newSpec *metadata.ReplicationSpecification) {
	_m.Called(oldSpec, newSpec)
}

// CheckpointsServiceCache_SpecChangeCb_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SpecChangeCb'
type CheckpointsServiceCache_SpecChangeCb_Call struct {
	*mock.Call
}

// SpecChangeCb is a helper method to define mock.On call
//   - oldSpec *metadata.ReplicationSpecification
//   - newSpec *metadata.ReplicationSpecification
func (_e *CheckpointsServiceCache_Expecter) SpecChangeCb(oldSpec interface{}, newSpec interface{}) *CheckpointsServiceCache_SpecChangeCb_Call {
	return &CheckpointsServiceCache_SpecChangeCb_Call{Call: _e.mock.On("SpecChangeCb", oldSpec, newSpec)}
}

func (_c *CheckpointsServiceCache_SpecChangeCb_Call) Run(run func(oldSpec *metadata.ReplicationSpecification, newSpec *metadata.ReplicationSpecification)) *CheckpointsServiceCache_SpecChangeCb_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(*metadata.ReplicationSpecification))
	})
	return _c
}

func (_c *CheckpointsServiceCache_SpecChangeCb_Call) Return() *CheckpointsServiceCache_SpecChangeCb_Call {
	_c.Call.Return()
	return _c
}

func (_c *CheckpointsServiceCache_SpecChangeCb_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, *metadata.ReplicationSpecification)) *CheckpointsServiceCache_SpecChangeCb_Call {
	_c.Call.Return(run)
	return _c
}

// StoreLatestDocs provides a mock function with given fields: incoming
func (_m *CheckpointsServiceCache) StoreLatestDocs(incoming metadata.VBsCkptsDocMap) error {
	ret := _m.Called(incoming)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.VBsCkptsDocMap) error); ok {
		r0 = rf(incoming)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CheckpointsServiceCache_StoreLatestDocs_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StoreLatestDocs'
type CheckpointsServiceCache_StoreLatestDocs_Call struct {
	*mock.Call
}

// StoreLatestDocs is a helper method to define mock.On call
//   - incoming metadata.VBsCkptsDocMap
func (_e *CheckpointsServiceCache_Expecter) StoreLatestDocs(incoming interface{}) *CheckpointsServiceCache_StoreLatestDocs_Call {
	return &CheckpointsServiceCache_StoreLatestDocs_Call{Call: _e.mock.On("StoreLatestDocs", incoming)}
}

func (_c *CheckpointsServiceCache_StoreLatestDocs_Call) Run(run func(incoming metadata.VBsCkptsDocMap)) *CheckpointsServiceCache_StoreLatestDocs_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(metadata.VBsCkptsDocMap))
	})
	return _c
}

func (_c *CheckpointsServiceCache_StoreLatestDocs_Call) Return(_a0 error) *CheckpointsServiceCache_StoreLatestDocs_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointsServiceCache_StoreLatestDocs_Call) RunAndReturn(run func(metadata.VBsCkptsDocMap) error) *CheckpointsServiceCache_StoreLatestDocs_Call {
	_c.Call.Return(run)
	return _c
}

// StoreOneVbDoc provides a mock function with given fields: vbno, ckpt, internalId
func (_m *CheckpointsServiceCache) StoreOneVbDoc(vbno uint16, ckpt *metadata.CheckpointsDoc, internalId string) error {
	ret := _m.Called(vbno, ckpt, internalId)

	var r0 error
	if rf, ok := ret.Get(0).(func(uint16, *metadata.CheckpointsDoc, string) error); ok {
		r0 = rf(vbno, ckpt, internalId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CheckpointsServiceCache_StoreOneVbDoc_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StoreOneVbDoc'
type CheckpointsServiceCache_StoreOneVbDoc_Call struct {
	*mock.Call
}

// StoreOneVbDoc is a helper method to define mock.On call
//   - vbno uint16
//   - ckpt *metadata.CheckpointsDoc
//   - internalId string
func (_e *CheckpointsServiceCache_Expecter) StoreOneVbDoc(vbno interface{}, ckpt interface{}, internalId interface{}) *CheckpointsServiceCache_StoreOneVbDoc_Call {
	return &CheckpointsServiceCache_StoreOneVbDoc_Call{Call: _e.mock.On("StoreOneVbDoc", vbno, ckpt, internalId)}
}

func (_c *CheckpointsServiceCache_StoreOneVbDoc_Call) Run(run func(vbno uint16, ckpt *metadata.CheckpointsDoc, internalId string)) *CheckpointsServiceCache_StoreOneVbDoc_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint16), args[1].(*metadata.CheckpointsDoc), args[2].(string))
	})
	return _c
}

func (_c *CheckpointsServiceCache_StoreOneVbDoc_Call) Return(_a0 error) *CheckpointsServiceCache_StoreOneVbDoc_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *CheckpointsServiceCache_StoreOneVbDoc_Call) RunAndReturn(run func(uint16, *metadata.CheckpointsDoc, string) error) *CheckpointsServiceCache_StoreOneVbDoc_Call {
	_c.Call.Return(run)
	return _c
}

// ValidateCache provides a mock function with given fields: internalId
func (_m *CheckpointsServiceCache) ValidateCache(internalId string) {
	_m.Called(internalId)
}

// CheckpointsServiceCache_ValidateCache_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ValidateCache'
type CheckpointsServiceCache_ValidateCache_Call struct {
	*mock.Call
}

// ValidateCache is a helper method to define mock.On call
//   - internalId string
func (_e *CheckpointsServiceCache_Expecter) ValidateCache(internalId interface{}) *CheckpointsServiceCache_ValidateCache_Call {
	return &CheckpointsServiceCache_ValidateCache_Call{Call: _e.mock.On("ValidateCache", internalId)}
}

func (_c *CheckpointsServiceCache_ValidateCache_Call) Run(run func(internalId string)) *CheckpointsServiceCache_ValidateCache_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *CheckpointsServiceCache_ValidateCache_Call) Return() *CheckpointsServiceCache_ValidateCache_Call {
	_c.Call.Return()
	return _c
}

func (_c *CheckpointsServiceCache_ValidateCache_Call) RunAndReturn(run func(string)) *CheckpointsServiceCache_ValidateCache_Call {
	_c.Call.Return(run)
	return _c
}

// NewCheckpointsServiceCache creates a new instance of CheckpointsServiceCache. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewCheckpointsServiceCache(t interface {
	mock.TestingT
	Cleanup(func())
}) *CheckpointsServiceCache {
	mock := &CheckpointsServiceCache{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
