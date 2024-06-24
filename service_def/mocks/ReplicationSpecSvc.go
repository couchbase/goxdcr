// Code generated by mockery v2.42.1. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	metadata "github.com/couchbase/goxdcr/metadata"

	mock "github.com/stretchr/testify/mock"

	service_def "github.com/couchbase/goxdcr/service_def"
)

// ReplicationSpecSvc is an autogenerated mock type for the ReplicationSpecSvc type
type ReplicationSpecSvc struct {
	mock.Mock
}

// AddReplicationSpec provides a mock function with given fields: spec, additionalInfo
func (_m *ReplicationSpecSvc) AddReplicationSpec(spec *metadata.ReplicationSpecification, additionalInfo string) error {
	ret := _m.Called(spec, additionalInfo)

	if len(ret) == 0 {
		panic("no return value specified for AddReplicationSpec")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string) error); ok {
		r0 = rf(spec, additionalInfo)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// AllActiveReplicationSpecsReadOnly provides a mock function with given fields:
func (_m *ReplicationSpecSvc) AllActiveReplicationSpecsReadOnly() (map[string]*metadata.ReplicationSpecification, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for AllActiveReplicationSpecsReadOnly")
	}

	var r0 map[string]*metadata.ReplicationSpecification
	var r1 error
	if rf, ok := ret.Get(0).(func() (map[string]*metadata.ReplicationSpecification, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() map[string]*metadata.ReplicationSpecification); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]*metadata.ReplicationSpecification)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AllReplicationSpecIds provides a mock function with given fields:
func (_m *ReplicationSpecSvc) AllReplicationSpecIds() ([]string, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for AllReplicationSpecIds")
	}

	var r0 []string
	var r1 error
	if rf, ok := ret.Get(0).(func() ([]string, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() []string); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AllReplicationSpecIdsForBucket provides a mock function with given fields: bucket
func (_m *ReplicationSpecSvc) AllReplicationSpecIdsForBucket(bucket string) ([]string, error) {
	ret := _m.Called(bucket)

	if len(ret) == 0 {
		panic("no return value specified for AllReplicationSpecIdsForBucket")
	}

	var r0 []string
	var r1 error
	if rf, ok := ret.Get(0).(func(string) ([]string, error)); ok {
		return rf(bucket)
	}
	if rf, ok := ret.Get(0).(func(string) []string); ok {
		r0 = rf(bucket)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(bucket)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AllReplicationSpecs provides a mock function with given fields:
func (_m *ReplicationSpecSvc) AllReplicationSpecs() (map[string]*metadata.ReplicationSpecification, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for AllReplicationSpecs")
	}

	var r0 map[string]*metadata.ReplicationSpecification
	var r1 error
	if rf, ok := ret.Get(0).(func() (map[string]*metadata.ReplicationSpecification, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() map[string]*metadata.ReplicationSpecification); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]*metadata.ReplicationSpecification)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AllReplicationSpecsWithRemote provides a mock function with given fields: remoteClusterRef
func (_m *ReplicationSpecSvc) AllReplicationSpecsWithRemote(remoteClusterRef *metadata.RemoteClusterReference) ([]*metadata.ReplicationSpecification, error) {
	ret := _m.Called(remoteClusterRef)

	if len(ret) == 0 {
		panic("no return value specified for AllReplicationSpecsWithRemote")
	}

	var r0 []*metadata.ReplicationSpecification
	var r1 error
	if rf, ok := ret.Get(0).(func(*metadata.RemoteClusterReference) ([]*metadata.ReplicationSpecification, error)); ok {
		return rf(remoteClusterRef)
	}
	if rf, ok := ret.Get(0).(func(*metadata.RemoteClusterReference) []*metadata.ReplicationSpecification); ok {
		r0 = rf(remoteClusterRef)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*metadata.ReplicationSpecification)
		}
	}

	if rf, ok := ret.Get(1).(func(*metadata.RemoteClusterReference) error); ok {
		r1 = rf(remoteClusterRef)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ConstructNewReplicationSpec provides a mock function with given fields: sourceBucketName, targetClusterUUID, targetBucketName
func (_m *ReplicationSpecSvc) ConstructNewReplicationSpec(sourceBucketName string, targetClusterUUID string, targetBucketName string) (*metadata.ReplicationSpecification, error) {
	ret := _m.Called(sourceBucketName, targetClusterUUID, targetBucketName)

	if len(ret) == 0 {
		panic("no return value specified for ConstructNewReplicationSpec")
	}

	var r0 *metadata.ReplicationSpecification
	var r1 error
	if rf, ok := ret.Get(0).(func(string, string, string) (*metadata.ReplicationSpecification, error)); ok {
		return rf(sourceBucketName, targetClusterUUID, targetBucketName)
	}
	if rf, ok := ret.Get(0).(func(string, string, string) *metadata.ReplicationSpecification); ok {
		r0 = rf(sourceBucketName, targetClusterUUID, targetBucketName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReplicationSpecification)
		}
	}

	if rf, ok := ret.Get(1).(func(string, string, string) error); ok {
		r1 = rf(sourceBucketName, targetClusterUUID, targetBucketName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// DelReplicationSpec provides a mock function with given fields: replicationId
func (_m *ReplicationSpecSvc) DelReplicationSpec(replicationId string) (*metadata.ReplicationSpecification, error) {
	ret := _m.Called(replicationId)

	if len(ret) == 0 {
		panic("no return value specified for DelReplicationSpec")
	}

	var r0 *metadata.ReplicationSpecification
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (*metadata.ReplicationSpecification, error)); ok {
		return rf(replicationId)
	}
	if rf, ok := ret.Get(0).(func(string) *metadata.ReplicationSpecification); ok {
		r0 = rf(replicationId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReplicationSpecification)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(replicationId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// DelReplicationSpecWithReason provides a mock function with given fields: replicationId, reason
func (_m *ReplicationSpecSvc) DelReplicationSpecWithReason(replicationId string, reason string) (*metadata.ReplicationSpecification, error) {
	ret := _m.Called(replicationId, reason)

	if len(ret) == 0 {
		panic("no return value specified for DelReplicationSpecWithReason")
	}

	var r0 *metadata.ReplicationSpecification
	var r1 error
	if rf, ok := ret.Get(0).(func(string, string) (*metadata.ReplicationSpecification, error)); ok {
		return rf(replicationId, reason)
	}
	if rf, ok := ret.Get(0).(func(string, string) *metadata.ReplicationSpecification); ok {
		r0 = rf(replicationId, reason)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReplicationSpecification)
		}
	}

	if rf, ok := ret.Get(1).(func(string, string) error); ok {
		r1 = rf(replicationId, reason)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetDerivedObj provides a mock function with given fields: specId
func (_m *ReplicationSpecSvc) GetDerivedObj(specId string) (interface{}, error) {
	ret := _m.Called(specId)

	if len(ret) == 0 {
		panic("no return value specified for GetDerivedObj")
	}

	var r0 interface{}
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (interface{}, error)); ok {
		return rf(specId)
	}
	if rf, ok := ret.Get(0).(func(string) interface{}); ok {
		r0 = rf(specId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(specId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// IsReplicationValidationError provides a mock function with given fields: err
func (_m *ReplicationSpecSvc) IsReplicationValidationError(err error) bool {
	ret := _m.Called(err)

	if len(ret) == 0 {
		panic("no return value specified for IsReplicationValidationError")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func(error) bool); ok {
		r0 = rf(err)
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// ReplicationSpec provides a mock function with given fields: replicationId
func (_m *ReplicationSpecSvc) ReplicationSpec(replicationId string) (*metadata.ReplicationSpecification, error) {
	ret := _m.Called(replicationId)

	if len(ret) == 0 {
		panic("no return value specified for ReplicationSpec")
	}

	var r0 *metadata.ReplicationSpecification
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (*metadata.ReplicationSpecification, error)); ok {
		return rf(replicationId)
	}
	if rf, ok := ret.Get(0).(func(string) *metadata.ReplicationSpecification); ok {
		r0 = rf(replicationId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReplicationSpecification)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(replicationId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReplicationSpecReadOnly provides a mock function with given fields: replicationId
func (_m *ReplicationSpecSvc) ReplicationSpecReadOnly(replicationId string) (*metadata.ReplicationSpecification, error) {
	ret := _m.Called(replicationId)

	if len(ret) == 0 {
		panic("no return value specified for ReplicationSpecReadOnly")
	}

	var r0 *metadata.ReplicationSpecification
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (*metadata.ReplicationSpecification, error)); ok {
		return rf(replicationId)
	}
	if rf, ok := ret.Get(0).(func(string) *metadata.ReplicationSpecification); ok {
		r0 = rf(replicationId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReplicationSpecification)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(replicationId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReplicationSpecServiceCallback provides a mock function with given fields: path, value, rev
func (_m *ReplicationSpecSvc) ReplicationSpecServiceCallback(path string, value []byte, rev interface{}) error {
	ret := _m.Called(path, value, rev)

	if len(ret) == 0 {
		panic("no return value specified for ReplicationSpecServiceCallback")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []byte, interface{}) error); ok {
		r0 = rf(path, value, rev)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetDerivedObj provides a mock function with given fields: specId, derivedObj
func (_m *ReplicationSpecSvc) SetDerivedObj(specId string, derivedObj interface{}) error {
	ret := _m.Called(specId, derivedObj)

	if len(ret) == 0 {
		panic("no return value specified for SetDerivedObj")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, interface{}) error); ok {
		r0 = rf(specId, derivedObj)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetMetadataChangeHandlerCallback provides a mock function with given fields: id, callBack, add, del, mod
func (_m *ReplicationSpecSvc) SetMetadataChangeHandlerCallback(id string, callBack base.MetadataChangeHandlerCallbackWithWg, add base.MetadataChangeHandlerPriority, del base.MetadataChangeHandlerPriority, mod base.MetadataChangeHandlerPriority) {
	_m.Called(id, callBack, add, del, mod)
}

// SetReplicationSpec provides a mock function with given fields: spec
func (_m *ReplicationSpecSvc) SetReplicationSpec(spec *metadata.ReplicationSpecification) error {
	ret := _m.Called(spec)

	if len(ret) == 0 {
		panic("no return value specified for SetReplicationSpec")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification) error); ok {
		r0 = rf(spec)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ValidateAndGC provides a mock function with given fields: spec
func (_m *ReplicationSpecSvc) ValidateAndGC(spec *metadata.ReplicationSpecification) {
	_m.Called(spec)
}

// ValidateNewReplicationSpec provides a mock function with given fields: sourceBucket, targetCluster, targetBucket, settings, performRemoteValidation
func (_m *ReplicationSpecSvc) ValidateNewReplicationSpec(sourceBucket string, targetCluster string, targetBucket string, settings metadata.ReplicationSettingsMap, performRemoteValidation bool) (string, string, *metadata.RemoteClusterReference, base.ErrorMap, error, service_def.UIWarnings) {
	ret := _m.Called(sourceBucket, targetCluster, targetBucket, settings, performRemoteValidation)

	if len(ret) == 0 {
		panic("no return value specified for ValidateNewReplicationSpec")
	}

	var r0 string
	var r1 string
	var r2 *metadata.RemoteClusterReference
	var r3 base.ErrorMap
	var r4 error
	var r5 service_def.UIWarnings
	if rf, ok := ret.Get(0).(func(string, string, string, metadata.ReplicationSettingsMap, bool) (string, string, *metadata.RemoteClusterReference, base.ErrorMap, error, service_def.UIWarnings)); ok {
		return rf(sourceBucket, targetCluster, targetBucket, settings, performRemoteValidation)
	}
	if rf, ok := ret.Get(0).(func(string, string, string, metadata.ReplicationSettingsMap, bool) string); ok {
		r0 = rf(sourceBucket, targetCluster, targetBucket, settings, performRemoteValidation)
	} else {
		r0 = ret.Get(0).(string)
	}

	if rf, ok := ret.Get(1).(func(string, string, string, metadata.ReplicationSettingsMap, bool) string); ok {
		r1 = rf(sourceBucket, targetCluster, targetBucket, settings, performRemoteValidation)
	} else {
		r1 = ret.Get(1).(string)
	}

	if rf, ok := ret.Get(2).(func(string, string, string, metadata.ReplicationSettingsMap, bool) *metadata.RemoteClusterReference); ok {
		r2 = rf(sourceBucket, targetCluster, targetBucket, settings, performRemoteValidation)
	} else {
		if ret.Get(2) != nil {
			r2 = ret.Get(2).(*metadata.RemoteClusterReference)
		}
	}

	if rf, ok := ret.Get(3).(func(string, string, string, metadata.ReplicationSettingsMap, bool) base.ErrorMap); ok {
		r3 = rf(sourceBucket, targetCluster, targetBucket, settings, performRemoteValidation)
	} else {
		if ret.Get(3) != nil {
			r3 = ret.Get(3).(base.ErrorMap)
		}
	}

	if rf, ok := ret.Get(4).(func(string, string, string, metadata.ReplicationSettingsMap, bool) error); ok {
		r4 = rf(sourceBucket, targetCluster, targetBucket, settings, performRemoteValidation)
	} else {
		r4 = ret.Error(4)
	}

	if rf, ok := ret.Get(5).(func(string, string, string, metadata.ReplicationSettingsMap, bool) service_def.UIWarnings); ok {
		r5 = rf(sourceBucket, targetCluster, targetBucket, settings, performRemoteValidation)
	} else {
		if ret.Get(5) != nil {
			r5 = ret.Get(5).(service_def.UIWarnings)
		}
	}

	return r0, r1, r2, r3, r4, r5
}

// ValidateReplicationSettings provides a mock function with given fields: sourceBucket, targetCluster, targetBucket, settings, performRemoteValidation
func (_m *ReplicationSpecSvc) ValidateReplicationSettings(sourceBucket string, targetCluster string, targetBucket string, settings metadata.ReplicationSettingsMap, performRemoteValidation bool) (base.ErrorMap, error, service_def.UIWarnings) {
	ret := _m.Called(sourceBucket, targetCluster, targetBucket, settings, performRemoteValidation)

	if len(ret) == 0 {
		panic("no return value specified for ValidateReplicationSettings")
	}

	var r0 base.ErrorMap
	var r1 error
	var r2 service_def.UIWarnings
	if rf, ok := ret.Get(0).(func(string, string, string, metadata.ReplicationSettingsMap, bool) (base.ErrorMap, error, service_def.UIWarnings)); ok {
		return rf(sourceBucket, targetCluster, targetBucket, settings, performRemoteValidation)
	}
	if rf, ok := ret.Get(0).(func(string, string, string, metadata.ReplicationSettingsMap, bool) base.ErrorMap); ok {
		r0 = rf(sourceBucket, targetCluster, targetBucket, settings, performRemoteValidation)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(base.ErrorMap)
		}
	}

	if rf, ok := ret.Get(1).(func(string, string, string, metadata.ReplicationSettingsMap, bool) error); ok {
		r1 = rf(sourceBucket, targetCluster, targetBucket, settings, performRemoteValidation)
	} else {
		r1 = ret.Error(1)
	}

	if rf, ok := ret.Get(2).(func(string, string, string, metadata.ReplicationSettingsMap, bool) service_def.UIWarnings); ok {
		r2 = rf(sourceBucket, targetCluster, targetBucket, settings, performRemoteValidation)
	} else {
		if ret.Get(2) != nil {
			r2 = ret.Get(2).(service_def.UIWarnings)
		}
	}

	return r0, r1, r2
}

// NewReplicationSpecSvc creates a new instance of ReplicationSpecSvc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewReplicationSpecSvc(t interface {
	mock.TestingT
	Cleanup(func())
}) *ReplicationSpecSvc {
	mock := &ReplicationSpecSvc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
