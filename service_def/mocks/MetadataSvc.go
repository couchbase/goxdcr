package mocks

import mock "github.com/stretchr/testify/mock"
import service_def "github.com/couchbase/goxdcr/service_def"

// MetadataSvc is an autogenerated mock type for the MetadataSvc type
type MetadataSvc struct {
	mock.Mock
}

// Add provides a mock function with given fields: key, value
func (_m *MetadataSvc) Add(key string, value []byte) error {
	ret := _m.Called(key, value)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []byte) error); ok {
		r0 = rf(key, value)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// AddSensitive provides a mock function with given fields: key, value
func (_m *MetadataSvc) AddSensitive(key string, value []byte) error {
	ret := _m.Called(key, value)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []byte) error); ok {
		r0 = rf(key, value)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// AddSensitiveWithCatalog provides a mock function with given fields: catalogKey, key, value
func (_m *MetadataSvc) AddSensitiveWithCatalog(catalogKey string, key string, value []byte) error {
	ret := _m.Called(catalogKey, key, value)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, []byte) error); ok {
		r0 = rf(catalogKey, key, value)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// AddWithCatalog provides a mock function with given fields: catalogKey, key, value
func (_m *MetadataSvc) AddWithCatalog(catalogKey string, key string, value []byte) error {
	ret := _m.Called(catalogKey, key, value)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, []byte) error); ok {
		r0 = rf(catalogKey, key, value)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Del provides a mock function with given fields: key, rev
func (_m *MetadataSvc) Del(key string, rev interface{}) error {
	ret := _m.Called(key, rev)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, interface{}) error); ok {
		r0 = rf(key, rev)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DelAllFromCatalog provides a mock function with given fields: catalogKey
func (_m *MetadataSvc) DelAllFromCatalog(catalogKey string) error {
	ret := _m.Called(catalogKey)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(catalogKey)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DelWithCatalog provides a mock function with given fields: catalogKey, key, rev
func (_m *MetadataSvc) DelWithCatalog(catalogKey string, key string, rev interface{}) error {
	ret := _m.Called(catalogKey, key, rev)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, interface{}) error); ok {
		r0 = rf(catalogKey, key, rev)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Get provides a mock function with given fields: key
func (_m *MetadataSvc) Get(key string) ([]byte, interface{}, error) {
	ret := _m.Called(key)

	var r0 []byte
	if rf, ok := ret.Get(0).(func(string) []byte); ok {
		r0 = rf(key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	var r1 interface{}
	if rf, ok := ret.Get(1).(func(string) interface{}); ok {
		r1 = rf(key)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(interface{})
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(string) error); ok {
		r2 = rf(key)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// GetAllKeysFromCatalog provides a mock function with given fields: catalogKey
func (_m *MetadataSvc) GetAllKeysFromCatalog(catalogKey string) ([]string, error) {
	ret := _m.Called(catalogKey)

	var r0 []string
	if rf, ok := ret.Get(0).(func(string) []string); ok {
		r0 = rf(catalogKey)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(catalogKey)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAllMetadataFromCatalog provides a mock function with given fields: catalogKey
func (_m *MetadataSvc) GetAllMetadataFromCatalog(catalogKey string) ([]*service_def.MetadataEntry, error) {
	ret := _m.Called(catalogKey)

	var r0 []*service_def.MetadataEntry
	if rf, ok := ret.Get(0).(func(string) []*service_def.MetadataEntry); ok {
		r0 = rf(catalogKey)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*service_def.MetadataEntry)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(catalogKey)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Set provides a mock function with given fields: key, value, rev
func (_m *MetadataSvc) Set(key string, value []byte, rev interface{}) error {
	ret := _m.Called(key, value, rev)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []byte, interface{}) error); ok {
		r0 = rf(key, value, rev)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetSensitive provides a mock function with given fields: key, value, rev
func (_m *MetadataSvc) SetSensitive(key string, value []byte, rev interface{}) error {
	ret := _m.Called(key, value, rev)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []byte, interface{}) error); ok {
		r0 = rf(key, value, rev)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
