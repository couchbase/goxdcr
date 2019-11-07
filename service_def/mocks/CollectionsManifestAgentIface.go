package mocks

import metadata "github.com/couchbase/goxdcr/metadata"
import mock "github.com/stretchr/testify/mock"

// CollectionsManifestAgentIface is an autogenerated mock type for the CollectionsManifestAgentIface type
type CollectionsManifestAgentIface struct {
	mock.Mock
}

// GetAndRecordSourceManifest provides a mock function with given fields: vblist
func (_m *CollectionsManifestAgentIface) GetAndRecordSourceManifest(vblist []uint16) *metadata.CollectionsManifest {
	ret := _m.Called(vblist)

	var r0 *metadata.CollectionsManifest
	if rf, ok := ret.Get(0).(func([]uint16) *metadata.CollectionsManifest); ok {
		r0 = rf(vblist)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifest)
		}
	}

	return r0
}

// GetLastPersistedManifests provides a mock function with given fields:
func (_m *CollectionsManifestAgentIface) GetLastPersistedManifests() (*metadata.CollectionsManifestPair, error) {
	ret := _m.Called()

	var r0 *metadata.CollectionsManifestPair
	if rf, ok := ret.Get(0).(func() *metadata.CollectionsManifestPair); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifestPair)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetOngoingManifests provides a mock function with given fields: vb
func (_m *CollectionsManifestAgentIface) GetOngoingManifests(vb uint16) *metadata.CollectionsManifestPair {
	ret := _m.Called(vb)

	var r0 *metadata.CollectionsManifestPair
	if rf, ok := ret.Get(0).(func(uint16) *metadata.CollectionsManifestPair); ok {
		r0 = rf(vb)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifestPair)
		}
	}

	return r0
}

// GetSourceManifest provides a mock function with given fields:
func (_m *CollectionsManifestAgentIface) GetSourceManifest() *metadata.CollectionsManifest {
	ret := _m.Called()

	var r0 *metadata.CollectionsManifest
	if rf, ok := ret.Get(0).(func() *metadata.CollectionsManifest); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifest)
		}
	}

	return r0
}

// GetTargetManifest provides a mock function with given fields:
func (_m *CollectionsManifestAgentIface) GetTargetManifest() *metadata.CollectionsManifest {
	ret := _m.Called()

	var r0 *metadata.CollectionsManifest
	if rf, ok := ret.Get(0).(func() *metadata.CollectionsManifest); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.CollectionsManifest)
		}
	}

	return r0
}

// PersistNeededManifests provides a mock function with given fields:
func (_m *CollectionsManifestAgentIface) PersistNeededManifests() (error, error, bool, bool) {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	var r2 bool
	if rf, ok := ret.Get(2).(func() bool); ok {
		r2 = rf()
	} else {
		r2 = ret.Get(2).(bool)
	}

	var r3 bool
	if rf, ok := ret.Get(3).(func() bool); ok {
		r3 = rf()
	} else {
		r3 = ret.Get(3).(bool)
	}

	return r0, r1, r2, r3
}

// Start provides a mock function with given fields:
func (_m *CollectionsManifestAgentIface) Start() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Stop provides a mock function with given fields:
func (_m *CollectionsManifestAgentIface) Stop() {
	_m.Called()
}