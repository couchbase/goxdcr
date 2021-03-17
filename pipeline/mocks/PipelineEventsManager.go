// Code generated by mockery v2.3.0. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"

	pipeline "github.com/couchbase/goxdcr/pipeline"
)

// PipelineEventsManager is an autogenerated mock type for the PipelineEventsManager type
type PipelineEventsManager struct {
	mock.Mock
}

// AddEvent provides a mock function with given fields: eventType, eventDesc, eventExtras
func (_m *PipelineEventsManager) AddEvent(eventType base.ErrorInfoType, eventDesc string, eventExtras base.EventsMap) {
	_m.Called(eventType, eventDesc, eventExtras)
}

// GetCurrentEvents provides a mock function with given fields:
func (_m *PipelineEventsManager) GetCurrentEvents() pipeline.PipelineEventList {
	ret := _m.Called()

	var r0 pipeline.PipelineEventList
	if rf, ok := ret.Get(0).(func() pipeline.PipelineEventList); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(pipeline.PipelineEventList)
	}

	return r0
}

// LoadLatestBrokenMap provides a mock function with given fields: mapping
func (_m *PipelineEventsManager) LoadLatestBrokenMap(mapping metadata.CollectionNamespaceMapping) {
	_m.Called(mapping)
}
