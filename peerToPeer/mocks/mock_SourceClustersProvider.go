// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/v8/metadata"
	mock "github.com/stretchr/testify/mock"
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

// GetSourceClustersInfoV1 provides a mock function with given fields:
func (_m *SourceClustersProvider) GetSourceClustersInfoV1() (map[string][]*metadata.ReplicationSpecification, map[string][]string, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetSourceClustersInfoV1")
	}

	var r0 map[string][]*metadata.ReplicationSpecification
	var r1 map[string][]string
	var r2 error
	if rf, ok := ret.Get(0).(func() (map[string][]*metadata.ReplicationSpecification, map[string][]string, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() map[string][]*metadata.ReplicationSpecification); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string][]*metadata.ReplicationSpecification)
		}
	}

	if rf, ok := ret.Get(1).(func() map[string][]string); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(map[string][]string)
		}
	}

	if rf, ok := ret.Get(2).(func() error); ok {
		r2 = rf()
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
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

func (_c *SourceClustersProvider_GetSourceClustersInfoV1_Call) Return(_a0 map[string][]*metadata.ReplicationSpecification, _a1 map[string][]string, _a2 error) *SourceClustersProvider_GetSourceClustersInfoV1_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *SourceClustersProvider_GetSourceClustersInfoV1_Call) RunAndReturn(run func() (map[string][]*metadata.ReplicationSpecification, map[string][]string, error)) *SourceClustersProvider_GetSourceClustersInfoV1_Call {
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
