// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	crMeta "github.com/couchbase/goxdcr/v8/crMeta"
	mock "github.com/stretchr/testify/mock"
)

// ResolverSvcIface is an autogenerated mock type for the ResolverSvcIface type
type ResolverSvcIface struct {
	mock.Mock
}

type ResolverSvcIface_Expecter struct {
	mock *mock.Mock
}

func (_m *ResolverSvcIface) EXPECT() *ResolverSvcIface_Expecter {
	return &ResolverSvcIface_Expecter{mock: &_m.Mock}
}

// CheckMergeFunction provides a mock function with given fields: fname
func (_m *ResolverSvcIface) CheckMergeFunction(fname string) error {
	ret := _m.Called(fname)

	if len(ret) == 0 {
		panic("no return value specified for CheckMergeFunction")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(fname)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ResolverSvcIface_CheckMergeFunction_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CheckMergeFunction'
type ResolverSvcIface_CheckMergeFunction_Call struct {
	*mock.Call
}

// CheckMergeFunction is a helper method to define mock.On call
//   - fname string
func (_e *ResolverSvcIface_Expecter) CheckMergeFunction(fname interface{}) *ResolverSvcIface_CheckMergeFunction_Call {
	return &ResolverSvcIface_CheckMergeFunction_Call{Call: _e.mock.On("CheckMergeFunction", fname)}
}

func (_c *ResolverSvcIface_CheckMergeFunction_Call) Run(run func(fname string)) *ResolverSvcIface_CheckMergeFunction_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *ResolverSvcIface_CheckMergeFunction_Call) Return(_a0 error) *ResolverSvcIface_CheckMergeFunction_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ResolverSvcIface_CheckMergeFunction_Call) RunAndReturn(run func(string) error) *ResolverSvcIface_CheckMergeFunction_Call {
	_c.Call.Return(run)
	return _c
}

// InitDefaultFunc provides a mock function with no fields
func (_m *ResolverSvcIface) InitDefaultFunc() {
	_m.Called()
}

// ResolverSvcIface_InitDefaultFunc_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'InitDefaultFunc'
type ResolverSvcIface_InitDefaultFunc_Call struct {
	*mock.Call
}

// InitDefaultFunc is a helper method to define mock.On call
func (_e *ResolverSvcIface_Expecter) InitDefaultFunc() *ResolverSvcIface_InitDefaultFunc_Call {
	return &ResolverSvcIface_InitDefaultFunc_Call{Call: _e.mock.On("InitDefaultFunc")}
}

func (_c *ResolverSvcIface_InitDefaultFunc_Call) Run(run func()) *ResolverSvcIface_InitDefaultFunc_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ResolverSvcIface_InitDefaultFunc_Call) Return() *ResolverSvcIface_InitDefaultFunc_Call {
	_c.Call.Return()
	return _c
}

func (_c *ResolverSvcIface_InitDefaultFunc_Call) RunAndReturn(run func()) *ResolverSvcIface_InitDefaultFunc_Call {
	_c.Run(run)
	return _c
}

// ResolveAsync provides a mock function with given fields: params, finish_ch
func (_m *ResolverSvcIface) ResolveAsync(params *crMeta.ConflictParams, finish_ch chan bool) {
	_m.Called(params, finish_ch)
}

// ResolverSvcIface_ResolveAsync_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ResolveAsync'
type ResolverSvcIface_ResolveAsync_Call struct {
	*mock.Call
}

// ResolveAsync is a helper method to define mock.On call
//   - params *crMeta.ConflictParams
//   - finish_ch chan bool
func (_e *ResolverSvcIface_Expecter) ResolveAsync(params interface{}, finish_ch interface{}) *ResolverSvcIface_ResolveAsync_Call {
	return &ResolverSvcIface_ResolveAsync_Call{Call: _e.mock.On("ResolveAsync", params, finish_ch)}
}

func (_c *ResolverSvcIface_ResolveAsync_Call) Run(run func(params *crMeta.ConflictParams, finish_ch chan bool)) *ResolverSvcIface_ResolveAsync_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*crMeta.ConflictParams), args[1].(chan bool))
	})
	return _c
}

func (_c *ResolverSvcIface_ResolveAsync_Call) Return() *ResolverSvcIface_ResolveAsync_Call {
	_c.Call.Return()
	return _c
}

func (_c *ResolverSvcIface_ResolveAsync_Call) RunAndReturn(run func(*crMeta.ConflictParams, chan bool)) *ResolverSvcIface_ResolveAsync_Call {
	_c.Run(run)
	return _c
}

// Start provides a mock function with given fields: sourceKVHost, xdcrRestPort
func (_m *ResolverSvcIface) Start(sourceKVHost string, xdcrRestPort uint16) {
	_m.Called(sourceKVHost, xdcrRestPort)
}

// ResolverSvcIface_Start_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Start'
type ResolverSvcIface_Start_Call struct {
	*mock.Call
}

// Start is a helper method to define mock.On call
//   - sourceKVHost string
//   - xdcrRestPort uint16
func (_e *ResolverSvcIface_Expecter) Start(sourceKVHost interface{}, xdcrRestPort interface{}) *ResolverSvcIface_Start_Call {
	return &ResolverSvcIface_Start_Call{Call: _e.mock.On("Start", sourceKVHost, xdcrRestPort)}
}

func (_c *ResolverSvcIface_Start_Call) Run(run func(sourceKVHost string, xdcrRestPort uint16)) *ResolverSvcIface_Start_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(uint16))
	})
	return _c
}

func (_c *ResolverSvcIface_Start_Call) Return() *ResolverSvcIface_Start_Call {
	_c.Call.Return()
	return _c
}

func (_c *ResolverSvcIface_Start_Call) RunAndReturn(run func(string, uint16)) *ResolverSvcIface_Start_Call {
	_c.Run(run)
	return _c
}

// Started provides a mock function with no fields
func (_m *ResolverSvcIface) Started() bool {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Started")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// ResolverSvcIface_Started_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Started'
type ResolverSvcIface_Started_Call struct {
	*mock.Call
}

// Started is a helper method to define mock.On call
func (_e *ResolverSvcIface_Expecter) Started() *ResolverSvcIface_Started_Call {
	return &ResolverSvcIface_Started_Call{Call: _e.mock.On("Started")}
}

func (_c *ResolverSvcIface_Started_Call) Run(run func()) *ResolverSvcIface_Started_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ResolverSvcIface_Started_Call) Return(_a0 bool) *ResolverSvcIface_Started_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ResolverSvcIface_Started_Call) RunAndReturn(run func() bool) *ResolverSvcIface_Started_Call {
	_c.Call.Return(run)
	return _c
}

// NewResolverSvcIface creates a new instance of ResolverSvcIface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewResolverSvcIface(t interface {
	mock.TestingT
	Cleanup(func())
}) *ResolverSvcIface {
	mock := &ResolverSvcIface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
