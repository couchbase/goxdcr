// Code generated by mockery v2.5.1. DO NOT EDIT.

package mocks

import (
	parts "github.com/couchbase/goxdcr/parts"
	mock "github.com/stretchr/testify/mock"
)

// CollectionsRoutingUpdater is an autogenerated mock type for the CollectionsRoutingUpdater type
type CollectionsRoutingUpdater struct {
	mock.Mock
}

// Execute provides a mock function with given fields: _a0
func (_m *CollectionsRoutingUpdater) Execute(_a0 parts.CollectionsRoutingInfo) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(parts.CollectionsRoutingInfo) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}