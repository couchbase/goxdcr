// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/metadata"
	mock "github.com/stretchr/testify/mock"
)

// IncrementerFunc is an autogenerated mock type for the IncrementerFunc type
type IncrementerFunc struct {
	mock.Mock
}

type IncrementerFunc_Expecter struct {
	mock *mock.Mock
}

func (_m *IncrementerFunc) EXPECT() *IncrementerFunc_Expecter {
	return &IncrementerFunc_Expecter{mock: &_m.Mock}
}

// Execute provides a mock function with given fields: shaString, mapping
func (_m *IncrementerFunc) Execute(shaString string, mapping *metadata.CollectionNamespaceMapping) {
	_m.Called(shaString, mapping)
}

// IncrementerFunc_Execute_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Execute'
type IncrementerFunc_Execute_Call struct {
	*mock.Call
}

// Execute is a helper method to define mock.On call
//   - shaString string
//   - mapping *metadata.CollectionNamespaceMapping
func (_e *IncrementerFunc_Expecter) Execute(shaString interface{}, mapping interface{}) *IncrementerFunc_Execute_Call {
	return &IncrementerFunc_Execute_Call{Call: _e.mock.On("Execute", shaString, mapping)}
}

func (_c *IncrementerFunc_Execute_Call) Run(run func(shaString string, mapping *metadata.CollectionNamespaceMapping)) *IncrementerFunc_Execute_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(*metadata.CollectionNamespaceMapping))
	})
	return _c
}

func (_c *IncrementerFunc_Execute_Call) Return() *IncrementerFunc_Execute_Call {
	_c.Call.Return()
	return _c
}

func (_c *IncrementerFunc_Execute_Call) RunAndReturn(run func(string, *metadata.CollectionNamespaceMapping)) *IncrementerFunc_Execute_Call {
	_c.Call.Return(run)
	return _c
}

// NewIncrementerFunc creates a new instance of IncrementerFunc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewIncrementerFunc(t interface {
	mock.TestingT
	Cleanup(func())
}) *IncrementerFunc {
	mock := &IncrementerFunc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
