// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/v8/metadata"
	mock "github.com/stretchr/testify/mock"

	time "time"
)

// ClusterHeartbeatAPI is an autogenerated mock type for the ClusterHeartbeatAPI type
type ClusterHeartbeatAPI struct {
	mock.Mock
}

type ClusterHeartbeatAPI_Expecter struct {
	mock *mock.Mock
}

func (_m *ClusterHeartbeatAPI) EXPECT() *ClusterHeartbeatAPI_Expecter {
	return &ClusterHeartbeatAPI_Expecter{mock: &_m.Mock}
}

// GetHeartbeatsReceivedV1 provides a mock function with given fields:
func (_m *ClusterHeartbeatAPI) GetHeartbeatsReceivedV1() (map[string]string, map[string][]*metadata.ReplicationSpecification, map[string][]string, map[string]time.Time, map[string]time.Time, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetHeartbeatsReceivedV1")
	}

	var r0 map[string]string
	var r1 map[string][]*metadata.ReplicationSpecification
	var r2 map[string][]string
	var r3 map[string]time.Time
	var r4 map[string]time.Time
	var r5 error
	if rf, ok := ret.Get(0).(func() (map[string]string, map[string][]*metadata.ReplicationSpecification, map[string][]string, map[string]time.Time, map[string]time.Time, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() map[string]string); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]string)
		}
	}

	if rf, ok := ret.Get(1).(func() map[string][]*metadata.ReplicationSpecification); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(map[string][]*metadata.ReplicationSpecification)
		}
	}

	if rf, ok := ret.Get(2).(func() map[string][]string); ok {
		r2 = rf()
	} else {
		if ret.Get(2) != nil {
			r2 = ret.Get(2).(map[string][]string)
		}
	}

	if rf, ok := ret.Get(3).(func() map[string]time.Time); ok {
		r3 = rf()
	} else {
		if ret.Get(3) != nil {
			r3 = ret.Get(3).(map[string]time.Time)
		}
	}

	if rf, ok := ret.Get(4).(func() map[string]time.Time); ok {
		r4 = rf()
	} else {
		if ret.Get(4) != nil {
			r4 = ret.Get(4).(map[string]time.Time)
		}
	}

	if rf, ok := ret.Get(5).(func() error); ok {
		r5 = rf()
	} else {
		r5 = ret.Error(5)
	}

	return r0, r1, r2, r3, r4, r5
}

// ClusterHeartbeatAPI_GetHeartbeatsReceivedV1_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetHeartbeatsReceivedV1'
type ClusterHeartbeatAPI_GetHeartbeatsReceivedV1_Call struct {
	*mock.Call
}

// GetHeartbeatsReceivedV1 is a helper method to define mock.On call
func (_e *ClusterHeartbeatAPI_Expecter) GetHeartbeatsReceivedV1() *ClusterHeartbeatAPI_GetHeartbeatsReceivedV1_Call {
	return &ClusterHeartbeatAPI_GetHeartbeatsReceivedV1_Call{Call: _e.mock.On("GetHeartbeatsReceivedV1")}
}

func (_c *ClusterHeartbeatAPI_GetHeartbeatsReceivedV1_Call) Run(run func()) *ClusterHeartbeatAPI_GetHeartbeatsReceivedV1_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ClusterHeartbeatAPI_GetHeartbeatsReceivedV1_Call) Return(_a0 map[string]string, _a1 map[string][]*metadata.ReplicationSpecification, _a2 map[string][]string, _a3 map[string]time.Time, _a4 map[string]time.Time, _a5 error) *ClusterHeartbeatAPI_GetHeartbeatsReceivedV1_Call {
	_c.Call.Return(_a0, _a1, _a2, _a3, _a4, _a5)
	return _c
}

func (_c *ClusterHeartbeatAPI_GetHeartbeatsReceivedV1_Call) RunAndReturn(run func() (map[string]string, map[string][]*metadata.ReplicationSpecification, map[string][]string, map[string]time.Time, map[string]time.Time, error)) *ClusterHeartbeatAPI_GetHeartbeatsReceivedV1_Call {
	_c.Call.Return(run)
	return _c
}

// SendHeartbeatToRemoteV1 provides a mock function with given fields: reference, hbMetadata
func (_m *ClusterHeartbeatAPI) SendHeartbeatToRemoteV1(reference *metadata.RemoteClusterReference, hbMetadata *metadata.HeartbeatMetadata) error {
	ret := _m.Called(reference, hbMetadata)

	if len(ret) == 0 {
		panic("no return value specified for SendHeartbeatToRemoteV1")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.RemoteClusterReference, *metadata.HeartbeatMetadata) error); ok {
		r0 = rf(reference, hbMetadata)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ClusterHeartbeatAPI_SendHeartbeatToRemoteV1_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SendHeartbeatToRemoteV1'
type ClusterHeartbeatAPI_SendHeartbeatToRemoteV1_Call struct {
	*mock.Call
}

// SendHeartbeatToRemoteV1 is a helper method to define mock.On call
//   - reference *metadata.RemoteClusterReference
//   - hbMetadata *metadata.HeartbeatMetadata
func (_e *ClusterHeartbeatAPI_Expecter) SendHeartbeatToRemoteV1(reference interface{}, hbMetadata interface{}) *ClusterHeartbeatAPI_SendHeartbeatToRemoteV1_Call {
	return &ClusterHeartbeatAPI_SendHeartbeatToRemoteV1_Call{Call: _e.mock.On("SendHeartbeatToRemoteV1", reference, hbMetadata)}
}

func (_c *ClusterHeartbeatAPI_SendHeartbeatToRemoteV1_Call) Run(run func(reference *metadata.RemoteClusterReference, hbMetadata *metadata.HeartbeatMetadata)) *ClusterHeartbeatAPI_SendHeartbeatToRemoteV1_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.RemoteClusterReference), args[1].(*metadata.HeartbeatMetadata))
	})
	return _c
}

func (_c *ClusterHeartbeatAPI_SendHeartbeatToRemoteV1_Call) Return(_a0 error) *ClusterHeartbeatAPI_SendHeartbeatToRemoteV1_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ClusterHeartbeatAPI_SendHeartbeatToRemoteV1_Call) RunAndReturn(run func(*metadata.RemoteClusterReference, *metadata.HeartbeatMetadata) error) *ClusterHeartbeatAPI_SendHeartbeatToRemoteV1_Call {
	_c.Call.Return(run)
	return _c
}

// NewClusterHeartbeatAPI creates a new instance of ClusterHeartbeatAPI. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewClusterHeartbeatAPI(t interface {
	mock.TestingT
	Cleanup(func())
}) *ClusterHeartbeatAPI {
	mock := &ClusterHeartbeatAPI{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}