package mocks

import (
	metadata "github.com/couchbase/goxdcr/metadata"
	mock "github.com/stretchr/testify/mock"

	pipeline_svc "github.com/couchbase/goxdcr/pipeline_svc"
)

// StatsMgrIface is an autogenerated mock type for the StatsMgrIface type
type StatsMgrIface struct {
	mock.Mock
}

// GetCountMetrics provides a mock function with given fields: key
func (_m *StatsMgrIface) GetCountMetrics(key string) (int64, error) {
	ret := _m.Called(key)

	var r0 int64
	if rf, ok := ret.Get(0).(func(string) int64); ok {
		r0 = rf(key)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetVBCountMetrics provides a mock function with given fields: vb
func (_m *StatsMgrIface) GetVBCountMetrics(vb uint16) (pipeline_svc.VBCountMetricMap, error) {
	ret := _m.Called(vb)

	var r0 pipeline_svc.VBCountMetricMap
	if rf, ok := ret.Get(0).(func(uint16) pipeline_svc.VBCountMetricMap); ok {
		r0 = rf(vb)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(pipeline_svc.VBCountMetricMap)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint16) error); ok {
		r1 = rf(vb)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// HandleLatestThroughSeqnos provides a mock function with given fields: SeqnoMap
func (_m *StatsMgrIface) HandleLatestThroughSeqnos(SeqnoMap map[uint16]uint64) {
	_m.Called(SeqnoMap)
}

// SetVBCountMetrics provides a mock function with given fields: vb, metricKVs
func (_m *StatsMgrIface) SetVBCountMetrics(vb uint16, metricKVs pipeline_svc.VBCountMetricMap) error {
	ret := _m.Called(vb, metricKVs)

	var r0 error
	if rf, ok := ret.Get(0).(func(uint16, pipeline_svc.VBCountMetricMap) error); ok {
		r0 = rf(vb, metricKVs)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Start provides a mock function with given fields: settings
func (_m *StatsMgrIface) Start(settings metadata.ReplicationSettingsMap) error {
	ret := _m.Called(settings)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.ReplicationSettingsMap) error); ok {
		r0 = rf(settings)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Stop provides a mock function with given fields:
func (_m *StatsMgrIface) Stop() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
