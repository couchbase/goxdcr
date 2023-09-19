// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"

	mock "github.com/stretchr/testify/mock"
)

// Filter is an autogenerated mock type for the Filter type
type Filter struct {
	mock.Mock
}

type Filter_Expecter struct {
	mock *mock.Mock
}

func (_m *Filter) EXPECT() *Filter_Expecter {
	return &Filter_Expecter{mock: &_m.Mock}
}

// FilterUprEvent provides a mock function with given fields: wrappedUprEvent
func (_m *Filter) FilterUprEvent(wrappedUprEvent *base.WrappedUprEvent) (bool, error, string, int64, base.FilteringStatusType) {
	ret := _m.Called(wrappedUprEvent)

	var r0 bool
	var r1 error
	var r2 string
	var r3 int64
	var r4 base.FilteringStatusType
	if rf, ok := ret.Get(0).(func(*base.WrappedUprEvent) (bool, error, string, int64, base.FilteringStatusType)); ok {
		return rf(wrappedUprEvent)
	}
	if rf, ok := ret.Get(0).(func(*base.WrappedUprEvent) bool); ok {
		r0 = rf(wrappedUprEvent)
	} else {
		r0 = ret.Get(0).(bool)
	}

	if rf, ok := ret.Get(1).(func(*base.WrappedUprEvent) error); ok {
		r1 = rf(wrappedUprEvent)
	} else {
		r1 = ret.Error(1)
	}

	if rf, ok := ret.Get(2).(func(*base.WrappedUprEvent) string); ok {
		r2 = rf(wrappedUprEvent)
	} else {
		r2 = ret.Get(2).(string)
	}

	if rf, ok := ret.Get(3).(func(*base.WrappedUprEvent) int64); ok {
		r3 = rf(wrappedUprEvent)
	} else {
		r3 = ret.Get(3).(int64)
	}

	if rf, ok := ret.Get(4).(func(*base.WrappedUprEvent) base.FilteringStatusType); ok {
		r4 = rf(wrappedUprEvent)
	} else {
		r4 = ret.Get(4).(base.FilteringStatusType)
	}

	return r0, r1, r2, r3, r4
}

// Filter_FilterUprEvent_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'FilterUprEvent'
type Filter_FilterUprEvent_Call struct {
	*mock.Call
}

// FilterUprEvent is a helper method to define mock.On call
//   - wrappedUprEvent *base.WrappedUprEvent
func (_e *Filter_Expecter) FilterUprEvent(wrappedUprEvent interface{}) *Filter_FilterUprEvent_Call {
	return &Filter_FilterUprEvent_Call{Call: _e.mock.On("FilterUprEvent", wrappedUprEvent)}
}

func (_c *Filter_FilterUprEvent_Call) Run(run func(wrappedUprEvent *base.WrappedUprEvent)) *Filter_FilterUprEvent_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*base.WrappedUprEvent))
	})
	return _c
}

func (_c *Filter_FilterUprEvent_Call) Return(_a0 bool, _a1 error, _a2 string, _a3 int64, _a4 base.FilteringStatusType) *Filter_FilterUprEvent_Call {
	_c.Call.Return(_a0, _a1, _a2, _a3, _a4)
	return _c
}

func (_c *Filter_FilterUprEvent_Call) RunAndReturn(run func(*base.WrappedUprEvent) (bool, error, string, int64, base.FilteringStatusType)) *Filter_FilterUprEvent_Call {
	_c.Call.Return(run)
	return _c
}

// SetMobileCompatibility provides a mock function with given fields: val
func (_m *Filter) SetMobileCompatibility(val uint32) {
	_m.Called(val)
}

// Filter_SetMobileCompatibility_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetMobileCompatibility'
type Filter_SetMobileCompatibility_Call struct {
	*mock.Call
}

// SetMobileCompatibility is a helper method to define mock.On call
//   - val uint32
func (_e *Filter_Expecter) SetMobileCompatibility(val interface{}) *Filter_SetMobileCompatibility_Call {
	return &Filter_SetMobileCompatibility_Call{Call: _e.mock.On("SetMobileCompatibility", val)}
}

func (_c *Filter_SetMobileCompatibility_Call) Run(run func(val uint32)) *Filter_SetMobileCompatibility_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint32))
	})
	return _c
}

func (_c *Filter_SetMobileCompatibility_Call) Return() *Filter_SetMobileCompatibility_Call {
	_c.Call.Return()
	return _c
}

func (_c *Filter_SetMobileCompatibility_Call) RunAndReturn(run func(uint32)) *Filter_SetMobileCompatibility_Call {
	_c.Call.Return(run)
	return _c
}

// SetShouldSkipBinaryDocs provides a mock function with given fields: val
func (_m *Filter) SetShouldSkipBinaryDocs(val bool) {
	_m.Called(val)
}

// Filter_SetShouldSkipBinaryDocs_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetShouldSkipBinaryDocs'
type Filter_SetShouldSkipBinaryDocs_Call struct {
	*mock.Call
}

// SetShouldSkipBinaryDocs is a helper method to define mock.On call
//   - val bool
func (_e *Filter_Expecter) SetShouldSkipBinaryDocs(val interface{}) *Filter_SetShouldSkipBinaryDocs_Call {
	return &Filter_SetShouldSkipBinaryDocs_Call{Call: _e.mock.On("SetShouldSkipBinaryDocs", val)}
}

func (_c *Filter_SetShouldSkipBinaryDocs_Call) Run(run func(val bool)) *Filter_SetShouldSkipBinaryDocs_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(bool))
	})
	return _c
}

func (_c *Filter_SetShouldSkipBinaryDocs_Call) Return() *Filter_SetShouldSkipBinaryDocs_Call {
	_c.Call.Return()
	return _c
}

func (_c *Filter_SetShouldSkipBinaryDocs_Call) RunAndReturn(run func(bool)) *Filter_SetShouldSkipBinaryDocs_Call {
	_c.Call.Return(run)
	return _c
}

// SetShouldSkipUncommittedTxn provides a mock function with given fields: val
func (_m *Filter) SetShouldSkipUncommittedTxn(val bool) {
	_m.Called(val)
}

// Filter_SetShouldSkipUncommittedTxn_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetShouldSkipUncommittedTxn'
type Filter_SetShouldSkipUncommittedTxn_Call struct {
	*mock.Call
}

// SetShouldSkipUncommittedTxn is a helper method to define mock.On call
//   - val bool
func (_e *Filter_Expecter) SetShouldSkipUncommittedTxn(val interface{}) *Filter_SetShouldSkipUncommittedTxn_Call {
	return &Filter_SetShouldSkipUncommittedTxn_Call{Call: _e.mock.On("SetShouldSkipUncommittedTxn", val)}
}

func (_c *Filter_SetShouldSkipUncommittedTxn_Call) Run(run func(val bool)) *Filter_SetShouldSkipUncommittedTxn_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(bool))
	})
	return _c
}

func (_c *Filter_SetShouldSkipUncommittedTxn_Call) Return() *Filter_SetShouldSkipUncommittedTxn_Call {
	_c.Call.Return()
	return _c
}

func (_c *Filter_SetShouldSkipUncommittedTxn_Call) RunAndReturn(run func(bool)) *Filter_SetShouldSkipUncommittedTxn_Call {
	_c.Call.Return(run)
	return _c
}

// NewFilter creates a new instance of Filter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewFilter(t interface {
	mock.TestingT
	Cleanup(func())
}) *Filter {
	mock := &Filter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
