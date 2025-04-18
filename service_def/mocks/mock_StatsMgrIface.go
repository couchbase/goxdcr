// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/v8/base"
	metadata "github.com/couchbase/goxdcr/v8/metadata"

	mock "github.com/stretchr/testify/mock"
)

// StatsMgrIface is an autogenerated mock type for the StatsMgrIface type
type StatsMgrIface struct {
	mock.Mock
}

type StatsMgrIface_Expecter struct {
	mock *mock.Mock
}

func (_m *StatsMgrIface) EXPECT() *StatsMgrIface_Expecter {
	return &StatsMgrIface_Expecter{mock: &_m.Mock}
}

// GetCountMetrics provides a mock function with given fields: key
func (_m *StatsMgrIface) GetCountMetrics(key string) (int64, error) {
	ret := _m.Called(key)

	if len(ret) == 0 {
		panic("no return value specified for GetCountMetrics")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (int64, error)); ok {
		return rf(key)
	}
	if rf, ok := ret.Get(0).(func(string) int64); ok {
		r0 = rf(key)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// StatsMgrIface_GetCountMetrics_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetCountMetrics'
type StatsMgrIface_GetCountMetrics_Call struct {
	*mock.Call
}

// GetCountMetrics is a helper method to define mock.On call
//   - key string
func (_e *StatsMgrIface_Expecter) GetCountMetrics(key interface{}) *StatsMgrIface_GetCountMetrics_Call {
	return &StatsMgrIface_GetCountMetrics_Call{Call: _e.mock.On("GetCountMetrics", key)}
}

func (_c *StatsMgrIface_GetCountMetrics_Call) Run(run func(key string)) *StatsMgrIface_GetCountMetrics_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *StatsMgrIface_GetCountMetrics_Call) Return(_a0 int64, _a1 error) *StatsMgrIface_GetCountMetrics_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *StatsMgrIface_GetCountMetrics_Call) RunAndReturn(run func(string) (int64, error)) *StatsMgrIface_GetCountMetrics_Call {
	_c.Call.Return(run)
	return _c
}

// GetThroughSeqnosFromTsService provides a mock function with no fields
func (_m *StatsMgrIface) GetThroughSeqnosFromTsService() map[uint16]uint64 {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetThroughSeqnosFromTsService")
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

// StatsMgrIface_GetThroughSeqnosFromTsService_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetThroughSeqnosFromTsService'
type StatsMgrIface_GetThroughSeqnosFromTsService_Call struct {
	*mock.Call
}

// GetThroughSeqnosFromTsService is a helper method to define mock.On call
func (_e *StatsMgrIface_Expecter) GetThroughSeqnosFromTsService() *StatsMgrIface_GetThroughSeqnosFromTsService_Call {
	return &StatsMgrIface_GetThroughSeqnosFromTsService_Call{Call: _e.mock.On("GetThroughSeqnosFromTsService")}
}

func (_c *StatsMgrIface_GetThroughSeqnosFromTsService_Call) Run(run func()) *StatsMgrIface_GetThroughSeqnosFromTsService_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *StatsMgrIface_GetThroughSeqnosFromTsService_Call) Return(_a0 map[uint16]uint64) *StatsMgrIface_GetThroughSeqnosFromTsService_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *StatsMgrIface_GetThroughSeqnosFromTsService_Call) RunAndReturn(run func() map[uint16]uint64) *StatsMgrIface_GetThroughSeqnosFromTsService_Call {
	_c.Call.Return(run)
	return _c
}

// GetVBCountMetrics provides a mock function with given fields: vb
func (_m *StatsMgrIface) GetVBCountMetrics(vb uint16) (base.VBCountMetric, error) {
	ret := _m.Called(vb)

	if len(ret) == 0 {
		panic("no return value specified for GetVBCountMetrics")
	}

	var r0 base.VBCountMetric
	var r1 error
	if rf, ok := ret.Get(0).(func(uint16) (base.VBCountMetric, error)); ok {
		return rf(vb)
	}
	if rf, ok := ret.Get(0).(func(uint16) base.VBCountMetric); ok {
		r0 = rf(vb)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.VBCountMetric)
		}
	}

	if rf, ok := ret.Get(1).(func(uint16) error); ok {
		r1 = rf(vb)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// StatsMgrIface_GetVBCountMetrics_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetVBCountMetrics'
type StatsMgrIface_GetVBCountMetrics_Call struct {
	*mock.Call
}

// GetVBCountMetrics is a helper method to define mock.On call
//   - vb uint16
func (_e *StatsMgrIface_Expecter) GetVBCountMetrics(vb interface{}) *StatsMgrIface_GetVBCountMetrics_Call {
	return &StatsMgrIface_GetVBCountMetrics_Call{Call: _e.mock.On("GetVBCountMetrics", vb)}
}

func (_c *StatsMgrIface_GetVBCountMetrics_Call) Run(run func(vb uint16)) *StatsMgrIface_GetVBCountMetrics_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint16))
	})
	return _c
}

func (_c *StatsMgrIface_GetVBCountMetrics_Call) Return(_a0 base.VBCountMetric, _a1 error) *StatsMgrIface_GetVBCountMetrics_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *StatsMgrIface_GetVBCountMetrics_Call) RunAndReturn(run func(uint16) (base.VBCountMetric, error)) *StatsMgrIface_GetVBCountMetrics_Call {
	_c.Call.Return(run)
	return _c
}

// HandleLatestThroughSeqnos provides a mock function with given fields: SeqnoMap
func (_m *StatsMgrIface) HandleLatestThroughSeqnos(SeqnoMap map[uint16]uint64) {
	_m.Called(SeqnoMap)
}

// StatsMgrIface_HandleLatestThroughSeqnos_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'HandleLatestThroughSeqnos'
type StatsMgrIface_HandleLatestThroughSeqnos_Call struct {
	*mock.Call
}

// HandleLatestThroughSeqnos is a helper method to define mock.On call
//   - SeqnoMap map[uint16]uint64
func (_e *StatsMgrIface_Expecter) HandleLatestThroughSeqnos(SeqnoMap interface{}) *StatsMgrIface_HandleLatestThroughSeqnos_Call {
	return &StatsMgrIface_HandleLatestThroughSeqnos_Call{Call: _e.mock.On("HandleLatestThroughSeqnos", SeqnoMap)}
}

func (_c *StatsMgrIface_HandleLatestThroughSeqnos_Call) Run(run func(SeqnoMap map[uint16]uint64)) *StatsMgrIface_HandleLatestThroughSeqnos_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(map[uint16]uint64))
	})
	return _c
}

func (_c *StatsMgrIface_HandleLatestThroughSeqnos_Call) Return() *StatsMgrIface_HandleLatestThroughSeqnos_Call {
	_c.Call.Return()
	return _c
}

func (_c *StatsMgrIface_HandleLatestThroughSeqnos_Call) RunAndReturn(run func(map[uint16]uint64)) *StatsMgrIface_HandleLatestThroughSeqnos_Call {
	_c.Run(run)
	return _c
}

// SetVBCountMetrics provides a mock function with given fields: vb, metricKVs
func (_m *StatsMgrIface) SetVBCountMetrics(vb uint16, metricKVs base.VBCountMetric) error {
	ret := _m.Called(vb, metricKVs)

	if len(ret) == 0 {
		panic("no return value specified for SetVBCountMetrics")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(uint16, base.VBCountMetric) error); ok {
		r0 = rf(vb, metricKVs)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// StatsMgrIface_SetVBCountMetrics_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetVBCountMetrics'
type StatsMgrIface_SetVBCountMetrics_Call struct {
	*mock.Call
}

// SetVBCountMetrics is a helper method to define mock.On call
//   - vb uint16
//   - metricKVs base.VBCountMetric
func (_e *StatsMgrIface_Expecter) SetVBCountMetrics(vb interface{}, metricKVs interface{}) *StatsMgrIface_SetVBCountMetrics_Call {
	return &StatsMgrIface_SetVBCountMetrics_Call{Call: _e.mock.On("SetVBCountMetrics", vb, metricKVs)}
}

func (_c *StatsMgrIface_SetVBCountMetrics_Call) Run(run func(vb uint16, metricKVs base.VBCountMetric)) *StatsMgrIface_SetVBCountMetrics_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint16), args[1].(base.VBCountMetric))
	})
	return _c
}

func (_c *StatsMgrIface_SetVBCountMetrics_Call) Return(_a0 error) *StatsMgrIface_SetVBCountMetrics_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *StatsMgrIface_SetVBCountMetrics_Call) RunAndReturn(run func(uint16, base.VBCountMetric) error) *StatsMgrIface_SetVBCountMetrics_Call {
	_c.Call.Return(run)
	return _c
}

// Start provides a mock function with given fields: settings
func (_m *StatsMgrIface) Start(settings metadata.ReplicationSettingsMap) error {
	ret := _m.Called(settings)

	if len(ret) == 0 {
		panic("no return value specified for Start")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.ReplicationSettingsMap) error); ok {
		r0 = rf(settings)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// StatsMgrIface_Start_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Start'
type StatsMgrIface_Start_Call struct {
	*mock.Call
}

// Start is a helper method to define mock.On call
//   - settings metadata.ReplicationSettingsMap
func (_e *StatsMgrIface_Expecter) Start(settings interface{}) *StatsMgrIface_Start_Call {
	return &StatsMgrIface_Start_Call{Call: _e.mock.On("Start", settings)}
}

func (_c *StatsMgrIface_Start_Call) Run(run func(settings metadata.ReplicationSettingsMap)) *StatsMgrIface_Start_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(metadata.ReplicationSettingsMap))
	})
	return _c
}

func (_c *StatsMgrIface_Start_Call) Return(_a0 error) *StatsMgrIface_Start_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *StatsMgrIface_Start_Call) RunAndReturn(run func(metadata.ReplicationSettingsMap) error) *StatsMgrIface_Start_Call {
	_c.Call.Return(run)
	return _c
}

// Stop provides a mock function with no fields
func (_m *StatsMgrIface) Stop() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Stop")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// StatsMgrIface_Stop_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Stop'
type StatsMgrIface_Stop_Call struct {
	*mock.Call
}

// Stop is a helper method to define mock.On call
func (_e *StatsMgrIface_Expecter) Stop() *StatsMgrIface_Stop_Call {
	return &StatsMgrIface_Stop_Call{Call: _e.mock.On("Stop")}
}

func (_c *StatsMgrIface_Stop_Call) Run(run func()) *StatsMgrIface_Stop_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *StatsMgrIface_Stop_Call) Return(_a0 error) *StatsMgrIface_Stop_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *StatsMgrIface_Stop_Call) RunAndReturn(run func() error) *StatsMgrIface_Stop_Call {
	_c.Call.Return(run)
	return _c
}

// NewStatsMgrIface creates a new instance of StatsMgrIface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewStatsMgrIface(t interface {
	mock.TestingT
	Cleanup(func())
}) *StatsMgrIface {
	mock := &StatsMgrIface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
