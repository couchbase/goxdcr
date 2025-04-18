// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	common "github.com/couchbase/goxdcr/v8/common"
	mock "github.com/stretchr/testify/mock"
)

// Connectable is an autogenerated mock type for the Connectable type
type Connectable struct {
	mock.Mock
}

type Connectable_Expecter struct {
	mock *mock.Mock
}

func (_m *Connectable) EXPECT() *Connectable_Expecter {
	return &Connectable_Expecter{mock: &_m.Mock}
}

// Connector provides a mock function with no fields
func (_m *Connectable) Connector() common.Connector {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Connector")
	}

	var r0 common.Connector
	if rf, ok := ret.Get(0).(func() common.Connector); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.Connector)
		}
	}

	return r0
}

// Connectable_Connector_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Connector'
type Connectable_Connector_Call struct {
	*mock.Call
}

// Connector is a helper method to define mock.On call
func (_e *Connectable_Expecter) Connector() *Connectable_Connector_Call {
	return &Connectable_Connector_Call{Call: _e.mock.On("Connector")}
}

func (_c *Connectable_Connector_Call) Run(run func()) *Connectable_Connector_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *Connectable_Connector_Call) Return(_a0 common.Connector) *Connectable_Connector_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *Connectable_Connector_Call) RunAndReturn(run func() common.Connector) *Connectable_Connector_Call {
	_c.Call.Return(run)
	return _c
}

// SetConnector provides a mock function with given fields: connector
func (_m *Connectable) SetConnector(connector common.Connector) error {
	ret := _m.Called(connector)

	if len(ret) == 0 {
		panic("no return value specified for SetConnector")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(common.Connector) error); ok {
		r0 = rf(connector)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Connectable_SetConnector_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetConnector'
type Connectable_SetConnector_Call struct {
	*mock.Call
}

// SetConnector is a helper method to define mock.On call
//   - connector common.Connector
func (_e *Connectable_Expecter) SetConnector(connector interface{}) *Connectable_SetConnector_Call {
	return &Connectable_SetConnector_Call{Call: _e.mock.On("SetConnector", connector)}
}

func (_c *Connectable_SetConnector_Call) Run(run func(connector common.Connector)) *Connectable_SetConnector_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(common.Connector))
	})
	return _c
}

func (_c *Connectable_SetConnector_Call) Return(_a0 error) *Connectable_SetConnector_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *Connectable_SetConnector_Call) RunAndReturn(run func(common.Connector) error) *Connectable_SetConnector_Call {
	_c.Call.Return(run)
	return _c
}

// NewConnectable creates a new instance of Connectable. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewConnectable(t interface {
	mock.TestingT
	Cleanup(func())
}) *Connectable {
	mock := &Connectable{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
