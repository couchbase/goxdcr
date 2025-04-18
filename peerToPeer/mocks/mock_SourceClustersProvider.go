// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/v8/metadata"
	mock "github.com/stretchr/testify/mock"

	time "time"
)

// SourceClustersProvider is an autogenerated mock type for the SourceClustersProvider type
type SourceClustersProvider struct {
	mock.Mock
}

type SourceClustersProvider_Expecter struct {
	mock *mock.Mock
}

func (_m *SourceClustersProvider) EXPECT() *SourceClustersProvider_Expecter {
	return &SourceClustersProvider_Expecter{mock: &_m.Mock}
}

// GetSourceClustersInfoV1 provides a mock function with no fields
func (_m *SourceClustersProvider) GetSourceClustersInfoV1() (map[string]string, map[string][]*metadata.ReplicationSpecification, map[string][]string, map[string]time.Time, map[string]time.Time, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetSourceClustersInfoV1")
	}

	var r0 map[string]string
	var r1 map[string][]*metadata.ReplicationSpecification
	var r2 map[string][]string
	var r3 map[string]time.Time
	var r4 map[string]time.Time
	var r5 error
	if rf, ok := ret.Get(0).(func() (map[string]string, map[string][]*metadata.ReplicationSpecification, map[string][]string, map[string]time.Time, map[string]time.Time, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() map[string]string); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]string)
		}
	}

	if rf, ok := ret.Get(1).(func() map[string][]*metadata.ReplicationSpecification); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(map[string][]*metadata.ReplicationSpecification)
		}
	}

	if rf, ok := ret.Get(2).(func() map[string][]string); ok {
		r2 = rf()
	} else {
		if ret.Get(2) != nil {
			r2 = ret.Get(2).(map[string][]string)
		}
	}

	if rf, ok := ret.Get(3).(func() map[string]time.Time); ok {
		r3 = rf()
	} else {
		if ret.Get(3) != nil {
			r3 = ret.Get(3).(map[string]time.Time)
		}
	}

	if rf, ok := ret.Get(4).(func() map[string]time.Time); ok {
		r4 = rf()
	} else {
		if ret.Get(4) != nil {
			r4 = ret.Get(4).(map[string]time.Time)
		}
	}

	if rf, ok := ret.Get(5).(func() error); ok {
		r5 = rf()
	} else {
		r5 = ret.Error(5)
	}

	return r0, r1, r2, r3, r4, r5
}

// SourceClustersProvider_GetSourceClustersInfoV1_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSourceClustersInfoV1'
type SourceClustersProvider_GetSourceClustersInfoV1_Call struct {
	*mock.Call
}

// GetSourceClustersInfoV1 is a helper method to define mock.On call
func (_e *SourceClustersProvider_Expecter) GetSourceClustersInfoV1() *SourceClustersProvider_GetSourceClustersInfoV1_Call {
	return &SourceClustersProvider_GetSourceClustersInfoV1_Call{Call: _e.mock.On("GetSourceClustersInfoV1")}
}

func (_c *SourceClustersProvider_GetSourceClustersInfoV1_Call) Run(run func()) *SourceClustersProvider_GetSourceClustersInfoV1_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *SourceClustersProvider_GetSourceClustersInfoV1_Call) Return(_a0 map[string]string, _a1 map[string][]*metadata.ReplicationSpecification, _a2 map[string][]string, _a3 map[string]time.Time, _a4 map[string]time.Time, _a5 error) *SourceClustersProvider_GetSourceClustersInfoV1_Call {
	_c.Call.Return(_a0, _a1, _a2, _a3, _a4, _a5)
	return _c
}

func (_c *SourceClustersProvider_GetSourceClustersInfoV1_Call) RunAndReturn(run func() (map[string]string, map[string][]*metadata.ReplicationSpecification, map[string][]string, map[string]time.Time, map[string]time.Time, error)) *SourceClustersProvider_GetSourceClustersInfoV1_Call {
	_c.Call.Return(run)
	return _c
}

// NewSourceClustersProvider creates a new instance of SourceClustersProvider. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewSourceClustersProvider(t interface {
	mock.TestingT
	Cleanup(func())
}) *SourceClustersProvider {
	mock := &SourceClustersProvider{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
