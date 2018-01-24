package mocks

import base "github.com/couchbase/goxdcr/base"
import mock "github.com/stretchr/testify/mock"

// XDCRCompTopologySvc is an autogenerated mock type for the XDCRCompTopologySvc type
type XDCRCompTopologySvc struct {
	mock.Mock
}

// GetLocalHostName provides a mock function with given fields:
func (_m *XDCRCompTopologySvc) GetLocalHostName() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// IsKVNode provides a mock function with given fields:
func (_m *XDCRCompTopologySvc) IsKVNode() (bool, error) {
	ret := _m.Called()

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// IsMyClusterEnterprise provides a mock function with given fields:
func (_m *XDCRCompTopologySvc) IsMyClusterEnterprise() (bool, error) {
	ret := _m.Called()

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// IsMyClusterIpv6 provides a mock function with given fields:
func (_m *XDCRCompTopologySvc) IsMyClusterIpv6() bool {
	ret := _m.Called()

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// MyAdminPort provides a mock function with given fields:
func (_m *XDCRCompTopologySvc) MyAdminPort() (uint16, error) {
	ret := _m.Called()

	var r0 uint16
	if rf, ok := ret.Get(0).(func() uint16); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint16)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MyClusterUuid provides a mock function with given fields:
func (_m *XDCRCompTopologySvc) MyClusterUuid() (string, error) {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MyClusterVersion provides a mock function with given fields:
func (_m *XDCRCompTopologySvc) MyClusterVersion() (string, error) {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MyConnectionStr provides a mock function with given fields:
func (_m *XDCRCompTopologySvc) MyConnectionStr() (string, error) {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MyCredentials provides a mock function with given fields:
func (_m *XDCRCompTopologySvc) MyCredentials() (string, string, []byte, bool, []byte, []byte, base.ClientCertAuth, error) {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 string
	if rf, ok := ret.Get(1).(func() string); ok {
		r1 = rf()
	} else {
		r1 = ret.Get(1).(string)
	}

	var r2 []byte
	if rf, ok := ret.Get(2).(func() []byte); ok {
		r2 = rf()
	} else {
		if ret.Get(2) != nil {
			r2 = ret.Get(2).([]byte)
		}
	}

	var r3 bool
	if rf, ok := ret.Get(3).(func() bool); ok {
		r3 = rf()
	} else {
		r3 = ret.Get(3).(bool)
	}

	var r4 []byte
	if rf, ok := ret.Get(4).(func() []byte); ok {
		r4 = rf()
	} else {
		if ret.Get(4) != nil {
			r4 = ret.Get(4).([]byte)
		}
	}

	var r5 []byte
	if rf, ok := ret.Get(5).(func() []byte); ok {
		r5 = rf()
	} else {
		if ret.Get(5) != nil {
			r5 = ret.Get(5).([]byte)
		}
	}

	var r6 base.ClientCertAuth
	if rf, ok := ret.Get(6).(func() base.ClientCertAuth); ok {
		r6 = rf()
	} else {
		r6 = ret.Get(6).(base.ClientCertAuth)
	}

	var r7 error
	if rf, ok := ret.Get(7).(func() error); ok {
		r7 = rf()
	} else {
		r7 = ret.Error(7)
	}

	return r0, r1, r2, r3, r4, r5, r6, r7
}

// MyHost provides a mock function with given fields:
func (_m *XDCRCompTopologySvc) MyHost() (string, error) {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MyHostAddr provides a mock function with given fields:
func (_m *XDCRCompTopologySvc) MyHostAddr() (string, error) {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MyKVNodes provides a mock function with given fields:
func (_m *XDCRCompTopologySvc) MyKVNodes() ([]string, error) {
	ret := _m.Called()

	var r0 []string
	if rf, ok := ret.Get(0).(func() []string); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
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

// MyMemcachedAddr provides a mock function with given fields:
func (_m *XDCRCompTopologySvc) MyMemcachedAddr() (string, error) {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NumberOfKVNodes provides a mock function with given fields:
func (_m *XDCRCompTopologySvc) NumberOfKVNodes() (int, error) {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// XDCRCompToKVNodeMap provides a mock function with given fields:
func (_m *XDCRCompTopologySvc) XDCRCompToKVNodeMap() (map[string][]string, error) {
	ret := _m.Called()

	var r0 map[string][]string
	if rf, ok := ret.Get(0).(func() map[string][]string); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string][]string)
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