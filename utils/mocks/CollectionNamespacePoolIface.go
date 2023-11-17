// Code generated by mockery v2.35.4. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	mock "github.com/stretchr/testify/mock"
)

// CollectionNamespacePoolIface is an autogenerated mock type for the CollectionNamespacePoolIface type
type CollectionNamespacePoolIface struct {
	mock.Mock
}

// Get provides a mock function with given fields:
func (_m *CollectionNamespacePoolIface) Get() *base.CollectionNamespace {
	ret := _m.Called()

	var r0 *base.CollectionNamespace
	if rf, ok := ret.Get(0).(func() *base.CollectionNamespace); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*base.CollectionNamespace)
		}
	}

	return r0
}

// Put provides a mock function with given fields: ns
func (_m *CollectionNamespacePoolIface) Put(ns *base.CollectionNamespace) {
	_m.Called(ns)
}

// NewCollectionNamespacePoolIface creates a new instance of CollectionNamespacePoolIface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewCollectionNamespacePoolIface(t interface {
	mock.TestingT
	Cleanup(func())
}) *CollectionNamespacePoolIface {
	mock := &CollectionNamespacePoolIface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
