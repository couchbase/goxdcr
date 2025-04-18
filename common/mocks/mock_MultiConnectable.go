// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	common "github.com/couchbase/goxdcr/v8/common"
	mock "github.com/stretchr/testify/mock"
)

// MultiConnectable is an autogenerated mock type for the MultiConnectable type
type MultiConnectable struct {
	mock.Mock
}

type MultiConnectable_Expecter struct {
	mock *mock.Mock
}

func (_m *MultiConnectable) EXPECT() *MultiConnectable_Expecter {
	return &MultiConnectable_Expecter{mock: &_m.Mock}
}

// Connectors provides a mock function with no fields
func (_m *MultiConnectable) Connectors() []common.Connector {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Connectors")
	}

	var r0 []common.Connector
	if rf, ok := ret.Get(0).(func() []common.Connector); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]common.Connector)
		}
	}

	return r0
}

// MultiConnectable_Connectors_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Connectors'
type MultiConnectable_Connectors_Call struct {
	*mock.Call
}

// Connectors is a helper method to define mock.On call
func (_e *MultiConnectable_Expecter) Connectors() *MultiConnectable_Connectors_Call {
	return &MultiConnectable_Connectors_Call{Call: _e.mock.On("Connectors")}
}

func (_c *MultiConnectable_Connectors_Call) Run(run func()) *MultiConnectable_Connectors_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MultiConnectable_Connectors_Call) Return(_a0 []common.Connector) *MultiConnectable_Connectors_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MultiConnectable_Connectors_Call) RunAndReturn(run func() []common.Connector) *MultiConnectable_Connectors_Call {
	_c.Call.Return(run)
	return _c
}

// NewMultiConnectable creates a new instance of MultiConnectable. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMultiConnectable(t interface {
	mock.TestingT
	Cleanup(func())
}) *MultiConnectable {
	mock := &MultiConnectable{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
