// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/v8/base"
	common "github.com/couchbase/goxdcr/v8/common"

	mock "github.com/stretchr/testify/mock"
)

// ThroughSeqnoTrackerSvc is an autogenerated mock type for the ThroughSeqnoTrackerSvc type
type ThroughSeqnoTrackerSvc struct {
	mock.Mock
}

type ThroughSeqnoTrackerSvc_Expecter struct {
	mock *mock.Mock
}

func (_m *ThroughSeqnoTrackerSvc) EXPECT() *ThroughSeqnoTrackerSvc_Expecter {
	return &ThroughSeqnoTrackerSvc_Expecter{mock: &_m.Mock}
}

// Attach provides a mock function with given fields: pipeline
func (_m *ThroughSeqnoTrackerSvc) Attach(pipeline common.Pipeline) error {
	ret := _m.Called(pipeline)

	if len(ret) == 0 {
		panic("no return value specified for Attach")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(common.Pipeline) error); ok {
		r0 = rf(pipeline)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ThroughSeqnoTrackerSvc_Attach_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Attach'
type ThroughSeqnoTrackerSvc_Attach_Call struct {
	*mock.Call
}

// Attach is a helper method to define mock.On call
//   - pipeline common.Pipeline
func (_e *ThroughSeqnoTrackerSvc_Expecter) Attach(pipeline interface{}) *ThroughSeqnoTrackerSvc_Attach_Call {
	return &ThroughSeqnoTrackerSvc_Attach_Call{Call: _e.mock.On("Attach", pipeline)}
}

func (_c *ThroughSeqnoTrackerSvc_Attach_Call) Run(run func(pipeline common.Pipeline)) *ThroughSeqnoTrackerSvc_Attach_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(common.Pipeline))
	})
	return _c
}

func (_c *ThroughSeqnoTrackerSvc_Attach_Call) Return(_a0 error) *ThroughSeqnoTrackerSvc_Attach_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ThroughSeqnoTrackerSvc_Attach_Call) RunAndReturn(run func(common.Pipeline) error) *ThroughSeqnoTrackerSvc_Attach_Call {
	_c.Call.Return(run)
	return _c
}

// GetThroughSeqno provides a mock function with given fields: vbno
func (_m *ThroughSeqnoTrackerSvc) GetThroughSeqno(vbno uint16) uint64 {
	ret := _m.Called(vbno)

	if len(ret) == 0 {
		panic("no return value specified for GetThroughSeqno")
	}

	var r0 uint64
	if rf, ok := ret.Get(0).(func(uint16) uint64); ok {
		r0 = rf(vbno)
	} else {
		r0 = ret.Get(0).(uint64)
	}

	return r0
}

// ThroughSeqnoTrackerSvc_GetThroughSeqno_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetThroughSeqno'
type ThroughSeqnoTrackerSvc_GetThroughSeqno_Call struct {
	*mock.Call
}

// GetThroughSeqno is a helper method to define mock.On call
//   - vbno uint16
func (_e *ThroughSeqnoTrackerSvc_Expecter) GetThroughSeqno(vbno interface{}) *ThroughSeqnoTrackerSvc_GetThroughSeqno_Call {
	return &ThroughSeqnoTrackerSvc_GetThroughSeqno_Call{Call: _e.mock.On("GetThroughSeqno", vbno)}
}

func (_c *ThroughSeqnoTrackerSvc_GetThroughSeqno_Call) Run(run func(vbno uint16)) *ThroughSeqnoTrackerSvc_GetThroughSeqno_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint16))
	})
	return _c
}

func (_c *ThroughSeqnoTrackerSvc_GetThroughSeqno_Call) Return(_a0 uint64) *ThroughSeqnoTrackerSvc_GetThroughSeqno_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ThroughSeqnoTrackerSvc_GetThroughSeqno_Call) RunAndReturn(run func(uint16) uint64) *ThroughSeqnoTrackerSvc_GetThroughSeqno_Call {
	_c.Call.Return(run)
	return _c
}

// GetThroughSeqnos provides a mock function with no fields
func (_m *ThroughSeqnoTrackerSvc) GetThroughSeqnos() map[uint16]uint64 {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetThroughSeqnos")
	}

	var r0 map[uint16]uint64
	if rf, ok := ret.Get(0).(func() map[uint16]uint64); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[uint16]uint64)
		}
	}

	return r0
}

// ThroughSeqnoTrackerSvc_GetThroughSeqnos_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetThroughSeqnos'
type ThroughSeqnoTrackerSvc_GetThroughSeqnos_Call struct {
	*mock.Call
}

// GetThroughSeqnos is a helper method to define mock.On call
func (_e *ThroughSeqnoTrackerSvc_Expecter) GetThroughSeqnos() *ThroughSeqnoTrackerSvc_GetThroughSeqnos_Call {
	return &ThroughSeqnoTrackerSvc_GetThroughSeqnos_Call{Call: _e.mock.On("GetThroughSeqnos")}
}

func (_c *ThroughSeqnoTrackerSvc_GetThroughSeqnos_Call) Run(run func()) *ThroughSeqnoTrackerSvc_GetThroughSeqnos_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ThroughSeqnoTrackerSvc_GetThroughSeqnos_Call) Return(_a0 map[uint16]uint64) *ThroughSeqnoTrackerSvc_GetThroughSeqnos_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ThroughSeqnoTrackerSvc_GetThroughSeqnos_Call) RunAndReturn(run func() map[uint16]uint64) *ThroughSeqnoTrackerSvc_GetThroughSeqnos_Call {
	_c.Call.Return(run)
	return _c
}

// GetThroughSeqnosAndManifestIds provides a mock function with no fields
func (_m *ThroughSeqnoTrackerSvc) GetThroughSeqnosAndManifestIds() (map[uint16]uint64, map[uint16]uint64, map[uint16]uint64) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetThroughSeqnosAndManifestIds")
	}

	var r0 map[uint16]uint64
	var r1 map[uint16]uint64
	var r2 map[uint16]uint64
	if rf, ok := ret.Get(0).(func() (map[uint16]uint64, map[uint16]uint64, map[uint16]uint64)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() map[uint16]uint64); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[uint16]uint64)
		}
	}

	if rf, ok := ret.Get(1).(func() map[uint16]uint64); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(map[uint16]uint64)
		}
	}

	if rf, ok := ret.Get(2).(func() map[uint16]uint64); ok {
		r2 = rf()
	} else {
		if ret.Get(2) != nil {
			r2 = ret.Get(2).(map[uint16]uint64)
		}
	}

	return r0, r1, r2
}

// ThroughSeqnoTrackerSvc_GetThroughSeqnosAndManifestIds_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetThroughSeqnosAndManifestIds'
type ThroughSeqnoTrackerSvc_GetThroughSeqnosAndManifestIds_Call struct {
	*mock.Call
}

// GetThroughSeqnosAndManifestIds is a helper method to define mock.On call
func (_e *ThroughSeqnoTrackerSvc_Expecter) GetThroughSeqnosAndManifestIds() *ThroughSeqnoTrackerSvc_GetThroughSeqnosAndManifestIds_Call {
	return &ThroughSeqnoTrackerSvc_GetThroughSeqnosAndManifestIds_Call{Call: _e.mock.On("GetThroughSeqnosAndManifestIds")}
}

func (_c *ThroughSeqnoTrackerSvc_GetThroughSeqnosAndManifestIds_Call) Run(run func()) *ThroughSeqnoTrackerSvc_GetThroughSeqnosAndManifestIds_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ThroughSeqnoTrackerSvc_GetThroughSeqnosAndManifestIds_Call) Return(throughSeqnos map[uint16]uint64, srcManifestIds map[uint16]uint64, tgtManifestIds map[uint16]uint64) *ThroughSeqnoTrackerSvc_GetThroughSeqnosAndManifestIds_Call {
	_c.Call.Return(throughSeqnos, srcManifestIds, tgtManifestIds)
	return _c
}

func (_c *ThroughSeqnoTrackerSvc_GetThroughSeqnosAndManifestIds_Call) RunAndReturn(run func() (map[uint16]uint64, map[uint16]uint64, map[uint16]uint64)) *ThroughSeqnoTrackerSvc_GetThroughSeqnosAndManifestIds_Call {
	_c.Call.Return(run)
	return _c
}

// PrintStatusSummary provides a mock function with no fields
func (_m *ThroughSeqnoTrackerSvc) PrintStatusSummary() {
	_m.Called()
}

// ThroughSeqnoTrackerSvc_PrintStatusSummary_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'PrintStatusSummary'
type ThroughSeqnoTrackerSvc_PrintStatusSummary_Call struct {
	*mock.Call
}

// PrintStatusSummary is a helper method to define mock.On call
func (_e *ThroughSeqnoTrackerSvc_Expecter) PrintStatusSummary() *ThroughSeqnoTrackerSvc_PrintStatusSummary_Call {
	return &ThroughSeqnoTrackerSvc_PrintStatusSummary_Call{Call: _e.mock.On("PrintStatusSummary")}
}

func (_c *ThroughSeqnoTrackerSvc_PrintStatusSummary_Call) Run(run func()) *ThroughSeqnoTrackerSvc_PrintStatusSummary_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ThroughSeqnoTrackerSvc_PrintStatusSummary_Call) Return() *ThroughSeqnoTrackerSvc_PrintStatusSummary_Call {
	_c.Call.Return()
	return _c
}

func (_c *ThroughSeqnoTrackerSvc_PrintStatusSummary_Call) RunAndReturn(run func()) *ThroughSeqnoTrackerSvc_PrintStatusSummary_Call {
	_c.Run(run)
	return _c
}

// SetStartSeqno provides a mock function with given fields: vbno, seqno, manifestIds
func (_m *ThroughSeqnoTrackerSvc) SetStartSeqno(vbno uint16, seqno uint64, manifestIds base.CollectionsManifestIdsTimestamp) {
	_m.Called(vbno, seqno, manifestIds)
}

// ThroughSeqnoTrackerSvc_SetStartSeqno_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetStartSeqno'
type ThroughSeqnoTrackerSvc_SetStartSeqno_Call struct {
	*mock.Call
}

// SetStartSeqno is a helper method to define mock.On call
//   - vbno uint16
//   - seqno uint64
//   - manifestIds base.CollectionsManifestIdsTimestamp
func (_e *ThroughSeqnoTrackerSvc_Expecter) SetStartSeqno(vbno interface{}, seqno interface{}, manifestIds interface{}) *ThroughSeqnoTrackerSvc_SetStartSeqno_Call {
	return &ThroughSeqnoTrackerSvc_SetStartSeqno_Call{Call: _e.mock.On("SetStartSeqno", vbno, seqno, manifestIds)}
}

func (_c *ThroughSeqnoTrackerSvc_SetStartSeqno_Call) Run(run func(vbno uint16, seqno uint64, manifestIds base.CollectionsManifestIdsTimestamp)) *ThroughSeqnoTrackerSvc_SetStartSeqno_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint16), args[1].(uint64), args[2].(base.CollectionsManifestIdsTimestamp))
	})
	return _c
}

func (_c *ThroughSeqnoTrackerSvc_SetStartSeqno_Call) Return() *ThroughSeqnoTrackerSvc_SetStartSeqno_Call {
	_c.Call.Return()
	return _c
}

func (_c *ThroughSeqnoTrackerSvc_SetStartSeqno_Call) RunAndReturn(run func(uint16, uint64, base.CollectionsManifestIdsTimestamp)) *ThroughSeqnoTrackerSvc_SetStartSeqno_Call {
	_c.Run(run)
	return _c
}

// NewThroughSeqnoTrackerSvc creates a new instance of ThroughSeqnoTrackerSvc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewThroughSeqnoTrackerSvc(t interface {
	mock.TestingT
	Cleanup(func())
}) *ThroughSeqnoTrackerSvc {
	mock := &ThroughSeqnoTrackerSvc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
