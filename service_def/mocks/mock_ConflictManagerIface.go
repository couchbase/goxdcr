// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/v8/base"
	hlv "github.com/couchbase/goxdcr/v8/hlv"

	mock "github.com/stretchr/testify/mock"
)

// ConflictManagerIface is an autogenerated mock type for the ConflictManagerIface type
type ConflictManagerIface struct {
	mock.Mock
}

type ConflictManagerIface_Expecter struct {
	mock *mock.Mock
}

func (_m *ConflictManagerIface) EXPECT() *ConflictManagerIface_Expecter {
	return &ConflictManagerIface_Expecter{mock: &_m.Mock}
}

// ResolveConflict provides a mock function with given fields: source, target, sourceId, targetId, uncompressFunc, recycler
func (_m *ConflictManagerIface) ResolveConflict(source *base.WrappedMCRequest, target *base.WrappedMCResponse, sourceId hlv.DocumentSourceId, targetId hlv.DocumentSourceId, uncompressFunc base.UncompressFunc, recycler func(*base.WrappedMCRequest)) error {
	ret := _m.Called(source, target, sourceId, targetId, uncompressFunc, recycler)

	if len(ret) == 0 {
		panic("no return value specified for ResolveConflict")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*base.WrappedMCRequest, *base.WrappedMCResponse, hlv.DocumentSourceId, hlv.DocumentSourceId, base.UncompressFunc, func(*base.WrappedMCRequest)) error); ok {
		r0 = rf(source, target, sourceId, targetId, uncompressFunc, recycler)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ConflictManagerIface_ResolveConflict_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ResolveConflict'
type ConflictManagerIface_ResolveConflict_Call struct {
	*mock.Call
}

// ResolveConflict is a helper method to define mock.On call
//   - source *base.WrappedMCRequest
//   - target *base.WrappedMCResponse
//   - sourceId hlv.DocumentSourceId
//   - targetId hlv.DocumentSourceId
//   - uncompressFunc base.UncompressFunc
//   - recycler func(*base.WrappedMCRequest)
func (_e *ConflictManagerIface_Expecter) ResolveConflict(source interface{}, target interface{}, sourceId interface{}, targetId interface{}, uncompressFunc interface{}, recycler interface{}) *ConflictManagerIface_ResolveConflict_Call {
	return &ConflictManagerIface_ResolveConflict_Call{Call: _e.mock.On("ResolveConflict", source, target, sourceId, targetId, uncompressFunc, recycler)}
}

func (_c *ConflictManagerIface_ResolveConflict_Call) Run(run func(source *base.WrappedMCRequest, target *base.WrappedMCResponse, sourceId hlv.DocumentSourceId, targetId hlv.DocumentSourceId, uncompressFunc base.UncompressFunc, recycler func(*base.WrappedMCRequest))) *ConflictManagerIface_ResolveConflict_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*base.WrappedMCRequest), args[1].(*base.WrappedMCResponse), args[2].(hlv.DocumentSourceId), args[3].(hlv.DocumentSourceId), args[4].(base.UncompressFunc), args[5].(func(*base.WrappedMCRequest)))
	})
	return _c
}

func (_c *ConflictManagerIface_ResolveConflict_Call) Return(_a0 error) *ConflictManagerIface_ResolveConflict_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ConflictManagerIface_ResolveConflict_Call) RunAndReturn(run func(*base.WrappedMCRequest, *base.WrappedMCResponse, hlv.DocumentSourceId, hlv.DocumentSourceId, base.UncompressFunc, func(*base.WrappedMCRequest)) error) *ConflictManagerIface_ResolveConflict_Call {
	_c.Call.Return(run)
	return _c
}

// SetBackToSource provides a mock function with given fields: source, target, sourceId, targetId, uncompressFunc, recycler
func (_m *ConflictManagerIface) SetBackToSource(source *base.WrappedMCRequest, target *base.WrappedMCResponse, sourceId hlv.DocumentSourceId, targetId hlv.DocumentSourceId, uncompressFunc base.UncompressFunc, recycler func(*base.WrappedMCRequest)) error {
	ret := _m.Called(source, target, sourceId, targetId, uncompressFunc, recycler)

	if len(ret) == 0 {
		panic("no return value specified for SetBackToSource")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*base.WrappedMCRequest, *base.WrappedMCResponse, hlv.DocumentSourceId, hlv.DocumentSourceId, base.UncompressFunc, func(*base.WrappedMCRequest)) error); ok {
		r0 = rf(source, target, sourceId, targetId, uncompressFunc, recycler)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ConflictManagerIface_SetBackToSource_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetBackToSource'
type ConflictManagerIface_SetBackToSource_Call struct {
	*mock.Call
}

// SetBackToSource is a helper method to define mock.On call
//   - source *base.WrappedMCRequest
//   - target *base.WrappedMCResponse
//   - sourceId hlv.DocumentSourceId
//   - targetId hlv.DocumentSourceId
//   - uncompressFunc base.UncompressFunc
//   - recycler func(*base.WrappedMCRequest)
func (_e *ConflictManagerIface_Expecter) SetBackToSource(source interface{}, target interface{}, sourceId interface{}, targetId interface{}, uncompressFunc interface{}, recycler interface{}) *ConflictManagerIface_SetBackToSource_Call {
	return &ConflictManagerIface_SetBackToSource_Call{Call: _e.mock.On("SetBackToSource", source, target, sourceId, targetId, uncompressFunc, recycler)}
}

func (_c *ConflictManagerIface_SetBackToSource_Call) Run(run func(source *base.WrappedMCRequest, target *base.WrappedMCResponse, sourceId hlv.DocumentSourceId, targetId hlv.DocumentSourceId, uncompressFunc base.UncompressFunc, recycler func(*base.WrappedMCRequest))) *ConflictManagerIface_SetBackToSource_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*base.WrappedMCRequest), args[1].(*base.WrappedMCResponse), args[2].(hlv.DocumentSourceId), args[3].(hlv.DocumentSourceId), args[4].(base.UncompressFunc), args[5].(func(*base.WrappedMCRequest)))
	})
	return _c
}

func (_c *ConflictManagerIface_SetBackToSource_Call) Return(_a0 error) *ConflictManagerIface_SetBackToSource_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ConflictManagerIface_SetBackToSource_Call) RunAndReturn(run func(*base.WrappedMCRequest, *base.WrappedMCResponse, hlv.DocumentSourceId, hlv.DocumentSourceId, base.UncompressFunc, func(*base.WrappedMCRequest)) error) *ConflictManagerIface_SetBackToSource_Call {
	_c.Call.Return(run)
	return _c
}

// NewConflictManagerIface creates a new instance of ConflictManagerIface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewConflictManagerIface(t interface {
	mock.TestingT
	Cleanup(func())
}) *ConflictManagerIface {
	mock := &ConflictManagerIface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
