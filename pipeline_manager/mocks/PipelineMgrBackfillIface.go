package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	mock "github.com/stretchr/testify/mock"
)

// PipelineMgrBackfillIface is an autogenerated mock type for the PipelineMgrBackfillIface type
type PipelineMgrBackfillIface struct {
	mock.Mock
}

// CleanupBackfillCkpts provides a mock function with given fields: topic
func (_m *PipelineMgrBackfillIface) CleanupBackfillCkpts(topic string) error {
	ret := _m.Called(topic)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetMainPipelineThroughSeqnos provides a mock function with given fields: topic
func (_m *PipelineMgrBackfillIface) GetMainPipelineThroughSeqnos(topic string) (map[uint16]uint64, error) {
	ret := _m.Called(topic)

	var r0 map[uint16]uint64
	if rf, ok := ret.Get(0).(func(string) map[uint16]uint64); ok {
		r0 = rf(topic)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[uint16]uint64)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(topic)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// HaltBackfill provides a mock function with given fields: topic
func (_m *PipelineMgrBackfillIface) HaltBackfill(topic string) error {
	ret := _m.Called(topic)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// HaltBackfillWithCb provides a mock function with given fields: topic, callback, errCb
func (_m *PipelineMgrBackfillIface) HaltBackfillWithCb(topic string, callback base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback) error {
	ret := _m.Called(topic, callback, errCb)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, base.StoppedPipelineCallback, base.StoppedPipelineErrCallback) error); ok {
		r0 = rf(topic, callback, errCb)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReInitStreams provides a mock function with given fields: pipelineName
func (_m *PipelineMgrBackfillIface) ReInitStreams(pipelineName string) error {
	ret := _m.Called(pipelineName)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(pipelineName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RequestBackfill provides a mock function with given fields: topic
func (_m *PipelineMgrBackfillIface) RequestBackfill(topic string) error {
	ret := _m.Called(topic)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
