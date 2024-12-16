// Code generated by mockery v2.50.0. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/metadata"
	mock "github.com/stretchr/testify/mock"
)

// ReplicationSpecGetter is an autogenerated mock type for the ReplicationSpecGetter type
type ReplicationSpecGetter struct {
	mock.Mock
}

type ReplicationSpecGetter_Expecter struct {
	mock *mock.Mock
}

func (_m *ReplicationSpecGetter) EXPECT() *ReplicationSpecGetter_Expecter {
	return &ReplicationSpecGetter_Expecter{mock: &_m.Mock}
}

// Execute provides a mock function with given fields: specId
func (_m *ReplicationSpecGetter) Execute(specId string) (*metadata.ReplicationSpecification, error) {
	ret := _m.Called(specId)

	if len(ret) == 0 {
		panic("no return value specified for Execute")
	}

	var r0 *metadata.ReplicationSpecification
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (*metadata.ReplicationSpecification, error)); ok {
		return rf(specId)
	}
	if rf, ok := ret.Get(0).(func(string) *metadata.ReplicationSpecification); ok {
		r0 = rf(specId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReplicationSpecification)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(specId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReplicationSpecGetter_Execute_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Execute'
type ReplicationSpecGetter_Execute_Call struct {
	*mock.Call
}

// Execute is a helper method to define mock.On call
//   - specId string
func (_e *ReplicationSpecGetter_Expecter) Execute(specId interface{}) *ReplicationSpecGetter_Execute_Call {
	return &ReplicationSpecGetter_Execute_Call{Call: _e.mock.On("Execute", specId)}
}

func (_c *ReplicationSpecGetter_Execute_Call) Run(run func(specId string)) *ReplicationSpecGetter_Execute_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *ReplicationSpecGetter_Execute_Call) Return(_a0 *metadata.ReplicationSpecification, _a1 error) *ReplicationSpecGetter_Execute_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *ReplicationSpecGetter_Execute_Call) RunAndReturn(run func(string) (*metadata.ReplicationSpecification, error)) *ReplicationSpecGetter_Execute_Call {
	_c.Call.Return(run)
	return _c
}

// NewReplicationSpecGetter creates a new instance of ReplicationSpecGetter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewReplicationSpecGetter(t interface {
	mock.TestingT
	Cleanup(func())
}) *ReplicationSpecGetter {
	mock := &ReplicationSpecGetter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
