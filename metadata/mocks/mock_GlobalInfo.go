// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/v8/metadata"
	mock "github.com/stretchr/testify/mock"
)

// GlobalInfo is an autogenerated mock type for the GlobalInfo type
type GlobalInfo struct {
	mock.Mock
}

type GlobalInfo_Expecter struct {
	mock *mock.Mock
}

func (_m *GlobalInfo) EXPECT() *GlobalInfo_Expecter {
	return &GlobalInfo_Expecter{mock: &_m.Mock}
}

// GetClone provides a mock function with no fields
func (_m *GlobalInfo) GetClone() metadata.GlobalInfo {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetClone")
	}

	var r0 metadata.GlobalInfo
	if rf, ok := ret.Get(0).(func() metadata.GlobalInfo); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(metadata.GlobalInfo)
		}
	}

	return r0
}

// GlobalInfo_GetClone_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetClone'
type GlobalInfo_GetClone_Call struct {
	*mock.Call
}

// GetClone is a helper method to define mock.On call
func (_e *GlobalInfo_Expecter) GetClone() *GlobalInfo_GetClone_Call {
	return &GlobalInfo_GetClone_Call{Call: _e.mock.On("GetClone")}
}

func (_c *GlobalInfo_GetClone_Call) Run(run func()) *GlobalInfo_GetClone_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *GlobalInfo_GetClone_Call) Return(_a0 metadata.GlobalInfo) *GlobalInfo_GetClone_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *GlobalInfo_GetClone_Call) RunAndReturn(run func() metadata.GlobalInfo) *GlobalInfo_GetClone_Call {
	_c.Call.Return(run)
	return _c
}

// Sha256 provides a mock function with no fields
func (_m *GlobalInfo) Sha256() ([32]byte, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Sha256")
	}

	var r0 [32]byte
	var r1 error
	if rf, ok := ret.Get(0).(func() ([32]byte, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() [32]byte); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([32]byte)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GlobalInfo_Sha256_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Sha256'
type GlobalInfo_Sha256_Call struct {
	*mock.Call
}

// Sha256 is a helper method to define mock.On call
func (_e *GlobalInfo_Expecter) Sha256() *GlobalInfo_Sha256_Call {
	return &GlobalInfo_Sha256_Call{Call: _e.mock.On("Sha256")}
}

func (_c *GlobalInfo_Sha256_Call) Run(run func()) *GlobalInfo_Sha256_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *GlobalInfo_Sha256_Call) Return(_a0 [32]byte, _a1 error) *GlobalInfo_Sha256_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *GlobalInfo_Sha256_Call) RunAndReturn(run func() ([32]byte, error)) *GlobalInfo_Sha256_Call {
	_c.Call.Return(run)
	return _c
}

// ToSnappyCompressable provides a mock function with no fields
func (_m *GlobalInfo) ToSnappyCompressable() metadata.SnappyCompressableVal {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for ToSnappyCompressable")
	}

	var r0 metadata.SnappyCompressableVal
	if rf, ok := ret.Get(0).(func() metadata.SnappyCompressableVal); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(metadata.SnappyCompressableVal)
		}
	}

	return r0
}

// GlobalInfo_ToSnappyCompressable_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ToSnappyCompressable'
type GlobalInfo_ToSnappyCompressable_Call struct {
	*mock.Call
}

// ToSnappyCompressable is a helper method to define mock.On call
func (_e *GlobalInfo_Expecter) ToSnappyCompressable() *GlobalInfo_ToSnappyCompressable_Call {
	return &GlobalInfo_ToSnappyCompressable_Call{Call: _e.mock.On("ToSnappyCompressable")}
}

func (_c *GlobalInfo_ToSnappyCompressable_Call) Run(run func()) *GlobalInfo_ToSnappyCompressable_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *GlobalInfo_ToSnappyCompressable_Call) Return(_a0 metadata.SnappyCompressableVal) *GlobalInfo_ToSnappyCompressable_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *GlobalInfo_ToSnappyCompressable_Call) RunAndReturn(run func() metadata.SnappyCompressableVal) *GlobalInfo_ToSnappyCompressable_Call {
	_c.Call.Return(run)
	return _c
}

// NewGlobalInfo creates a new instance of GlobalInfo. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewGlobalInfo(t interface {
	mock.TestingT
	Cleanup(func())
}) *GlobalInfo {
	mock := &GlobalInfo{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
