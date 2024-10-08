// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/v8/metadata"
	mock "github.com/stretchr/testify/mock"

	service_def "github.com/couchbase/goxdcr/v8/service_def"

	sync "sync"

	time "time"
)

// BucketTopologySvc is an autogenerated mock type for the BucketTopologySvc type
type BucketTopologySvc struct {
	mock.Mock
}

type BucketTopologySvc_Expecter struct {
	mock *mock.Mock
}

func (_m *BucketTopologySvc) EXPECT() *BucketTopologySvc_Expecter {
	return &BucketTopologySvc_Expecter{mock: &_m.Mock}
}

// RegisterGarbageCollect provides a mock function with given fields: specId, srcBucketName, vbno, requestId, gcFunc, timeToFire
func (_m *BucketTopologySvc) RegisterGarbageCollect(specId string, srcBucketName string, vbno uint16, requestId string, gcFunc func() error, timeToFire time.Duration) error {
	ret := _m.Called(specId, srcBucketName, vbno, requestId, gcFunc, timeToFire)

	if len(ret) == 0 {
		panic("no return value specified for RegisterGarbageCollect")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, uint16, string, func() error, time.Duration) error); ok {
		r0 = rf(specId, srcBucketName, vbno, requestId, gcFunc, timeToFire)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// BucketTopologySvc_RegisterGarbageCollect_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RegisterGarbageCollect'
type BucketTopologySvc_RegisterGarbageCollect_Call struct {
	*mock.Call
}

// RegisterGarbageCollect is a helper method to define mock.On call
//   - specId string
//   - srcBucketName string
//   - vbno uint16
//   - requestId string
//   - gcFunc func() error
//   - timeToFire time.Duration
func (_e *BucketTopologySvc_Expecter) RegisterGarbageCollect(specId interface{}, srcBucketName interface{}, vbno interface{}, requestId interface{}, gcFunc interface{}, timeToFire interface{}) *BucketTopologySvc_RegisterGarbageCollect_Call {
	return &BucketTopologySvc_RegisterGarbageCollect_Call{Call: _e.mock.On("RegisterGarbageCollect", specId, srcBucketName, vbno, requestId, gcFunc, timeToFire)}
}

func (_c *BucketTopologySvc_RegisterGarbageCollect_Call) Run(run func(specId string, srcBucketName string, vbno uint16, requestId string, gcFunc func() error, timeToFire time.Duration)) *BucketTopologySvc_RegisterGarbageCollect_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string), args[2].(uint16), args[3].(string), args[4].(func() error), args[5].(time.Duration))
	})
	return _c
}

func (_c *BucketTopologySvc_RegisterGarbageCollect_Call) Return(_a0 error) *BucketTopologySvc_RegisterGarbageCollect_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *BucketTopologySvc_RegisterGarbageCollect_Call) RunAndReturn(run func(string, string, uint16, string, func() error, time.Duration) error) *BucketTopologySvc_RegisterGarbageCollect_Call {
	_c.Call.Return(run)
	return _c
}

// ReplicationSpecChangeCallback provides a mock function with given fields: id, oldVal, newVal, wg
func (_m *BucketTopologySvc) ReplicationSpecChangeCallback(id string, oldVal interface{}, newVal interface{}, wg *sync.WaitGroup) error {
	ret := _m.Called(id, oldVal, newVal, wg)

	if len(ret) == 0 {
		panic("no return value specified for ReplicationSpecChangeCallback")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, interface{}, interface{}, *sync.WaitGroup) error); ok {
		r0 = rf(id, oldVal, newVal, wg)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// BucketTopologySvc_ReplicationSpecChangeCallback_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ReplicationSpecChangeCallback'
type BucketTopologySvc_ReplicationSpecChangeCallback_Call struct {
	*mock.Call
}

// ReplicationSpecChangeCallback is a helper method to define mock.On call
//   - id string
//   - oldVal interface{}
//   - newVal interface{}
//   - wg *sync.WaitGroup
func (_e *BucketTopologySvc_Expecter) ReplicationSpecChangeCallback(id interface{}, oldVal interface{}, newVal interface{}, wg interface{}) *BucketTopologySvc_ReplicationSpecChangeCallback_Call {
	return &BucketTopologySvc_ReplicationSpecChangeCallback_Call{Call: _e.mock.On("ReplicationSpecChangeCallback", id, oldVal, newVal, wg)}
}

func (_c *BucketTopologySvc_ReplicationSpecChangeCallback_Call) Run(run func(id string, oldVal interface{}, newVal interface{}, wg *sync.WaitGroup)) *BucketTopologySvc_ReplicationSpecChangeCallback_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(interface{}), args[2].(interface{}), args[3].(*sync.WaitGroup))
	})
	return _c
}

func (_c *BucketTopologySvc_ReplicationSpecChangeCallback_Call) Return(_a0 error) *BucketTopologySvc_ReplicationSpecChangeCallback_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *BucketTopologySvc_ReplicationSpecChangeCallback_Call) RunAndReturn(run func(string, interface{}, interface{}, *sync.WaitGroup) error) *BucketTopologySvc_ReplicationSpecChangeCallback_Call {
	_c.Call.Return(run)
	return _c
}

// SubscribeToLocalBucketFeed provides a mock function with given fields: spec, subscriberId
func (_m *BucketTopologySvc) SubscribeToLocalBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) (chan service_def.SourceNotification, error) {
	ret := _m.Called(spec, subscriberId)

	if len(ret) == 0 {
		panic("no return value specified for SubscribeToLocalBucketFeed")
	}

	var r0 chan service_def.SourceNotification
	var r1 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string) (chan service_def.SourceNotification, error)); ok {
		return rf(spec, subscriberId)
	}
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string) chan service_def.SourceNotification); ok {
		r0 = rf(spec, subscriberId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(chan service_def.SourceNotification)
		}
	}

	if rf, ok := ret.Get(1).(func(*metadata.ReplicationSpecification, string) error); ok {
		r1 = rf(spec, subscriberId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BucketTopologySvc_SubscribeToLocalBucketFeed_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SubscribeToLocalBucketFeed'
type BucketTopologySvc_SubscribeToLocalBucketFeed_Call struct {
	*mock.Call
}

// SubscribeToLocalBucketFeed is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - subscriberId string
func (_e *BucketTopologySvc_Expecter) SubscribeToLocalBucketFeed(spec interface{}, subscriberId interface{}) *BucketTopologySvc_SubscribeToLocalBucketFeed_Call {
	return &BucketTopologySvc_SubscribeToLocalBucketFeed_Call{Call: _e.mock.On("SubscribeToLocalBucketFeed", spec, subscriberId)}
}

func (_c *BucketTopologySvc_SubscribeToLocalBucketFeed_Call) Run(run func(spec *metadata.ReplicationSpecification, subscriberId string)) *BucketTopologySvc_SubscribeToLocalBucketFeed_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(string))
	})
	return _c
}

func (_c *BucketTopologySvc_SubscribeToLocalBucketFeed_Call) Return(_a0 chan service_def.SourceNotification, _a1 error) *BucketTopologySvc_SubscribeToLocalBucketFeed_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *BucketTopologySvc_SubscribeToLocalBucketFeed_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, string) (chan service_def.SourceNotification, error)) *BucketTopologySvc_SubscribeToLocalBucketFeed_Call {
	_c.Call.Return(run)
	return _c
}

// SubscribeToLocalBucketHighSeqnosFeed provides a mock function with given fields: spec, subscriberId, requestedInterval
func (_m *BucketTopologySvc) SubscribeToLocalBucketHighSeqnosFeed(spec *metadata.ReplicationSpecification, subscriberId string, requestedInterval time.Duration) (chan service_def.SourceNotification, func(time.Duration), error) {
	ret := _m.Called(spec, subscriberId, requestedInterval)

	if len(ret) == 0 {
		panic("no return value specified for SubscribeToLocalBucketHighSeqnosFeed")
	}

	var r0 chan service_def.SourceNotification
	var r1 func(time.Duration)
	var r2 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string, time.Duration) (chan service_def.SourceNotification, func(time.Duration), error)); ok {
		return rf(spec, subscriberId, requestedInterval)
	}
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string, time.Duration) chan service_def.SourceNotification); ok {
		r0 = rf(spec, subscriberId, requestedInterval)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(chan service_def.SourceNotification)
		}
	}

	if rf, ok := ret.Get(1).(func(*metadata.ReplicationSpecification, string, time.Duration) func(time.Duration)); ok {
		r1 = rf(spec, subscriberId, requestedInterval)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(func(time.Duration))
		}
	}

	if rf, ok := ret.Get(2).(func(*metadata.ReplicationSpecification, string, time.Duration) error); ok {
		r2 = rf(spec, subscriberId, requestedInterval)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// BucketTopologySvc_SubscribeToLocalBucketHighSeqnosFeed_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SubscribeToLocalBucketHighSeqnosFeed'
type BucketTopologySvc_SubscribeToLocalBucketHighSeqnosFeed_Call struct {
	*mock.Call
}

// SubscribeToLocalBucketHighSeqnosFeed is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - subscriberId string
//   - requestedInterval time.Duration
func (_e *BucketTopologySvc_Expecter) SubscribeToLocalBucketHighSeqnosFeed(spec interface{}, subscriberId interface{}, requestedInterval interface{}) *BucketTopologySvc_SubscribeToLocalBucketHighSeqnosFeed_Call {
	return &BucketTopologySvc_SubscribeToLocalBucketHighSeqnosFeed_Call{Call: _e.mock.On("SubscribeToLocalBucketHighSeqnosFeed", spec, subscriberId, requestedInterval)}
}

func (_c *BucketTopologySvc_SubscribeToLocalBucketHighSeqnosFeed_Call) Run(run func(spec *metadata.ReplicationSpecification, subscriberId string, requestedInterval time.Duration)) *BucketTopologySvc_SubscribeToLocalBucketHighSeqnosFeed_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(string), args[2].(time.Duration))
	})
	return _c
}

func (_c *BucketTopologySvc_SubscribeToLocalBucketHighSeqnosFeed_Call) Return(_a0 chan service_def.SourceNotification, _a1 func(time.Duration), _a2 error) *BucketTopologySvc_SubscribeToLocalBucketHighSeqnosFeed_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *BucketTopologySvc_SubscribeToLocalBucketHighSeqnosFeed_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, string, time.Duration) (chan service_def.SourceNotification, func(time.Duration), error)) *BucketTopologySvc_SubscribeToLocalBucketHighSeqnosFeed_Call {
	_c.Call.Return(run)
	return _c
}

// SubscribeToLocalBucketHighSeqnosLegacyFeed provides a mock function with given fields: spec, subscriberId, requestedInterval
func (_m *BucketTopologySvc) SubscribeToLocalBucketHighSeqnosLegacyFeed(spec *metadata.ReplicationSpecification, subscriberId string, requestedInterval time.Duration) (chan service_def.SourceNotification, func(time.Duration), error) {
	ret := _m.Called(spec, subscriberId, requestedInterval)

	if len(ret) == 0 {
		panic("no return value specified for SubscribeToLocalBucketHighSeqnosLegacyFeed")
	}

	var r0 chan service_def.SourceNotification
	var r1 func(time.Duration)
	var r2 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string, time.Duration) (chan service_def.SourceNotification, func(time.Duration), error)); ok {
		return rf(spec, subscriberId, requestedInterval)
	}
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string, time.Duration) chan service_def.SourceNotification); ok {
		r0 = rf(spec, subscriberId, requestedInterval)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(chan service_def.SourceNotification)
		}
	}

	if rf, ok := ret.Get(1).(func(*metadata.ReplicationSpecification, string, time.Duration) func(time.Duration)); ok {
		r1 = rf(spec, subscriberId, requestedInterval)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(func(time.Duration))
		}
	}

	if rf, ok := ret.Get(2).(func(*metadata.ReplicationSpecification, string, time.Duration) error); ok {
		r2 = rf(spec, subscriberId, requestedInterval)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// BucketTopologySvc_SubscribeToLocalBucketHighSeqnosLegacyFeed_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SubscribeToLocalBucketHighSeqnosLegacyFeed'
type BucketTopologySvc_SubscribeToLocalBucketHighSeqnosLegacyFeed_Call struct {
	*mock.Call
}

// SubscribeToLocalBucketHighSeqnosLegacyFeed is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - subscriberId string
//   - requestedInterval time.Duration
func (_e *BucketTopologySvc_Expecter) SubscribeToLocalBucketHighSeqnosLegacyFeed(spec interface{}, subscriberId interface{}, requestedInterval interface{}) *BucketTopologySvc_SubscribeToLocalBucketHighSeqnosLegacyFeed_Call {
	return &BucketTopologySvc_SubscribeToLocalBucketHighSeqnosLegacyFeed_Call{Call: _e.mock.On("SubscribeToLocalBucketHighSeqnosLegacyFeed", spec, subscriberId, requestedInterval)}
}

func (_c *BucketTopologySvc_SubscribeToLocalBucketHighSeqnosLegacyFeed_Call) Run(run func(spec *metadata.ReplicationSpecification, subscriberId string, requestedInterval time.Duration)) *BucketTopologySvc_SubscribeToLocalBucketHighSeqnosLegacyFeed_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(string), args[2].(time.Duration))
	})
	return _c
}

func (_c *BucketTopologySvc_SubscribeToLocalBucketHighSeqnosLegacyFeed_Call) Return(_a0 chan service_def.SourceNotification, _a1 func(time.Duration), _a2 error) *BucketTopologySvc_SubscribeToLocalBucketHighSeqnosLegacyFeed_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *BucketTopologySvc_SubscribeToLocalBucketHighSeqnosLegacyFeed_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, string, time.Duration) (chan service_def.SourceNotification, func(time.Duration), error)) *BucketTopologySvc_SubscribeToLocalBucketHighSeqnosLegacyFeed_Call {
	_c.Call.Return(run)
	return _c
}

// SubscribeToLocalBucketMaxVbCasStatFeed provides a mock function with given fields: spec, subscriberId
func (_m *BucketTopologySvc) SubscribeToLocalBucketMaxVbCasStatFeed(spec *metadata.ReplicationSpecification, subscriberId string) (chan service_def.SourceNotification, chan error, error) {
	ret := _m.Called(spec, subscriberId)

	if len(ret) == 0 {
		panic("no return value specified for SubscribeToLocalBucketMaxVbCasStatFeed")
	}

	var r0 chan service_def.SourceNotification
	var r1 chan error
	var r2 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string) (chan service_def.SourceNotification, chan error, error)); ok {
		return rf(spec, subscriberId)
	}
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string) chan service_def.SourceNotification); ok {
		r0 = rf(spec, subscriberId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(chan service_def.SourceNotification)
		}
	}

	if rf, ok := ret.Get(1).(func(*metadata.ReplicationSpecification, string) chan error); ok {
		r1 = rf(spec, subscriberId)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(chan error)
		}
	}

	if rf, ok := ret.Get(2).(func(*metadata.ReplicationSpecification, string) error); ok {
		r2 = rf(spec, subscriberId)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// BucketTopologySvc_SubscribeToLocalBucketMaxVbCasStatFeed_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SubscribeToLocalBucketMaxVbCasStatFeed'
type BucketTopologySvc_SubscribeToLocalBucketMaxVbCasStatFeed_Call struct {
	*mock.Call
}

// SubscribeToLocalBucketMaxVbCasStatFeed is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - subscriberId string
func (_e *BucketTopologySvc_Expecter) SubscribeToLocalBucketMaxVbCasStatFeed(spec interface{}, subscriberId interface{}) *BucketTopologySvc_SubscribeToLocalBucketMaxVbCasStatFeed_Call {
	return &BucketTopologySvc_SubscribeToLocalBucketMaxVbCasStatFeed_Call{Call: _e.mock.On("SubscribeToLocalBucketMaxVbCasStatFeed", spec, subscriberId)}
}

func (_c *BucketTopologySvc_SubscribeToLocalBucketMaxVbCasStatFeed_Call) Run(run func(spec *metadata.ReplicationSpecification, subscriberId string)) *BucketTopologySvc_SubscribeToLocalBucketMaxVbCasStatFeed_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(string))
	})
	return _c
}

func (_c *BucketTopologySvc_SubscribeToLocalBucketMaxVbCasStatFeed_Call) Return(_a0 chan service_def.SourceNotification, _a1 chan error, _a2 error) *BucketTopologySvc_SubscribeToLocalBucketMaxVbCasStatFeed_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *BucketTopologySvc_SubscribeToLocalBucketMaxVbCasStatFeed_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, string) (chan service_def.SourceNotification, chan error, error)) *BucketTopologySvc_SubscribeToLocalBucketMaxVbCasStatFeed_Call {
	_c.Call.Return(run)
	return _c
}

// SubscribeToRemoteBucketFeed provides a mock function with given fields: spec, subscriberId
func (_m *BucketTopologySvc) SubscribeToRemoteBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) (chan service_def.TargetNotification, error) {
	ret := _m.Called(spec, subscriberId)

	if len(ret) == 0 {
		panic("no return value specified for SubscribeToRemoteBucketFeed")
	}

	var r0 chan service_def.TargetNotification
	var r1 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string) (chan service_def.TargetNotification, error)); ok {
		return rf(spec, subscriberId)
	}
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string) chan service_def.TargetNotification); ok {
		r0 = rf(spec, subscriberId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(chan service_def.TargetNotification)
		}
	}

	if rf, ok := ret.Get(1).(func(*metadata.ReplicationSpecification, string) error); ok {
		r1 = rf(spec, subscriberId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BucketTopologySvc_SubscribeToRemoteBucketFeed_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SubscribeToRemoteBucketFeed'
type BucketTopologySvc_SubscribeToRemoteBucketFeed_Call struct {
	*mock.Call
}

// SubscribeToRemoteBucketFeed is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - subscriberId string
func (_e *BucketTopologySvc_Expecter) SubscribeToRemoteBucketFeed(spec interface{}, subscriberId interface{}) *BucketTopologySvc_SubscribeToRemoteBucketFeed_Call {
	return &BucketTopologySvc_SubscribeToRemoteBucketFeed_Call{Call: _e.mock.On("SubscribeToRemoteBucketFeed", spec, subscriberId)}
}

func (_c *BucketTopologySvc_SubscribeToRemoteBucketFeed_Call) Run(run func(spec *metadata.ReplicationSpecification, subscriberId string)) *BucketTopologySvc_SubscribeToRemoteBucketFeed_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(string))
	})
	return _c
}

func (_c *BucketTopologySvc_SubscribeToRemoteBucketFeed_Call) Return(_a0 chan service_def.TargetNotification, _a1 error) *BucketTopologySvc_SubscribeToRemoteBucketFeed_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *BucketTopologySvc_SubscribeToRemoteBucketFeed_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, string) (chan service_def.TargetNotification, error)) *BucketTopologySvc_SubscribeToRemoteBucketFeed_Call {
	_c.Call.Return(run)
	return _c
}

// SubscribeToRemoteKVStatsFeed provides a mock function with given fields: spec, subscriberId
func (_m *BucketTopologySvc) SubscribeToRemoteKVStatsFeed(spec *metadata.ReplicationSpecification, subscriberId string) (chan service_def.TargetNotification, chan error, error) {
	ret := _m.Called(spec, subscriberId)

	if len(ret) == 0 {
		panic("no return value specified for SubscribeToRemoteKVStatsFeed")
	}

	var r0 chan service_def.TargetNotification
	var r1 chan error
	var r2 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string) (chan service_def.TargetNotification, chan error, error)); ok {
		return rf(spec, subscriberId)
	}
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string) chan service_def.TargetNotification); ok {
		r0 = rf(spec, subscriberId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(chan service_def.TargetNotification)
		}
	}

	if rf, ok := ret.Get(1).(func(*metadata.ReplicationSpecification, string) chan error); ok {
		r1 = rf(spec, subscriberId)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(chan error)
		}
	}

	if rf, ok := ret.Get(2).(func(*metadata.ReplicationSpecification, string) error); ok {
		r2 = rf(spec, subscriberId)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// BucketTopologySvc_SubscribeToRemoteKVStatsFeed_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SubscribeToRemoteKVStatsFeed'
type BucketTopologySvc_SubscribeToRemoteKVStatsFeed_Call struct {
	*mock.Call
}

// SubscribeToRemoteKVStatsFeed is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - subscriberId string
func (_e *BucketTopologySvc_Expecter) SubscribeToRemoteKVStatsFeed(spec interface{}, subscriberId interface{}) *BucketTopologySvc_SubscribeToRemoteKVStatsFeed_Call {
	return &BucketTopologySvc_SubscribeToRemoteKVStatsFeed_Call{Call: _e.mock.On("SubscribeToRemoteKVStatsFeed", spec, subscriberId)}
}

func (_c *BucketTopologySvc_SubscribeToRemoteKVStatsFeed_Call) Run(run func(spec *metadata.ReplicationSpecification, subscriberId string)) *BucketTopologySvc_SubscribeToRemoteKVStatsFeed_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(string))
	})
	return _c
}

func (_c *BucketTopologySvc_SubscribeToRemoteKVStatsFeed_Call) Return(_a0 chan service_def.TargetNotification, _a1 chan error, _a2 error) *BucketTopologySvc_SubscribeToRemoteKVStatsFeed_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *BucketTopologySvc_SubscribeToRemoteKVStatsFeed_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, string) (chan service_def.TargetNotification, chan error, error)) *BucketTopologySvc_SubscribeToRemoteKVStatsFeed_Call {
	_c.Call.Return(run)
	return _c
}

// UnSubscribeLocalBucketFeed provides a mock function with given fields: spec, subscriberId
func (_m *BucketTopologySvc) UnSubscribeLocalBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) error {
	ret := _m.Called(spec, subscriberId)

	if len(ret) == 0 {
		panic("no return value specified for UnSubscribeLocalBucketFeed")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string) error); ok {
		r0 = rf(spec, subscriberId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// BucketTopologySvc_UnSubscribeLocalBucketFeed_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UnSubscribeLocalBucketFeed'
type BucketTopologySvc_UnSubscribeLocalBucketFeed_Call struct {
	*mock.Call
}

// UnSubscribeLocalBucketFeed is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - subscriberId string
func (_e *BucketTopologySvc_Expecter) UnSubscribeLocalBucketFeed(spec interface{}, subscriberId interface{}) *BucketTopologySvc_UnSubscribeLocalBucketFeed_Call {
	return &BucketTopologySvc_UnSubscribeLocalBucketFeed_Call{Call: _e.mock.On("UnSubscribeLocalBucketFeed", spec, subscriberId)}
}

func (_c *BucketTopologySvc_UnSubscribeLocalBucketFeed_Call) Run(run func(spec *metadata.ReplicationSpecification, subscriberId string)) *BucketTopologySvc_UnSubscribeLocalBucketFeed_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(string))
	})
	return _c
}

func (_c *BucketTopologySvc_UnSubscribeLocalBucketFeed_Call) Return(_a0 error) *BucketTopologySvc_UnSubscribeLocalBucketFeed_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *BucketTopologySvc_UnSubscribeLocalBucketFeed_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, string) error) *BucketTopologySvc_UnSubscribeLocalBucketFeed_Call {
	_c.Call.Return(run)
	return _c
}

// UnSubscribeRemoteBucketFeed provides a mock function with given fields: spec, subscriberId
func (_m *BucketTopologySvc) UnSubscribeRemoteBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) error {
	ret := _m.Called(spec, subscriberId)

	if len(ret) == 0 {
		panic("no return value specified for UnSubscribeRemoteBucketFeed")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string) error); ok {
		r0 = rf(spec, subscriberId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// BucketTopologySvc_UnSubscribeRemoteBucketFeed_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UnSubscribeRemoteBucketFeed'
type BucketTopologySvc_UnSubscribeRemoteBucketFeed_Call struct {
	*mock.Call
}

// UnSubscribeRemoteBucketFeed is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - subscriberId string
func (_e *BucketTopologySvc_Expecter) UnSubscribeRemoteBucketFeed(spec interface{}, subscriberId interface{}) *BucketTopologySvc_UnSubscribeRemoteBucketFeed_Call {
	return &BucketTopologySvc_UnSubscribeRemoteBucketFeed_Call{Call: _e.mock.On("UnSubscribeRemoteBucketFeed", spec, subscriberId)}
}

func (_c *BucketTopologySvc_UnSubscribeRemoteBucketFeed_Call) Run(run func(spec *metadata.ReplicationSpecification, subscriberId string)) *BucketTopologySvc_UnSubscribeRemoteBucketFeed_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(string))
	})
	return _c
}

func (_c *BucketTopologySvc_UnSubscribeRemoteBucketFeed_Call) Return(_a0 error) *BucketTopologySvc_UnSubscribeRemoteBucketFeed_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *BucketTopologySvc_UnSubscribeRemoteBucketFeed_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, string) error) *BucketTopologySvc_UnSubscribeRemoteBucketFeed_Call {
	_c.Call.Return(run)
	return _c
}

// UnSubscribeToLocalBucketHighSeqnosFeed provides a mock function with given fields: spec, subscriberId
func (_m *BucketTopologySvc) UnSubscribeToLocalBucketHighSeqnosFeed(spec *metadata.ReplicationSpecification, subscriberId string) error {
	ret := _m.Called(spec, subscriberId)

	if len(ret) == 0 {
		panic("no return value specified for UnSubscribeToLocalBucketHighSeqnosFeed")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string) error); ok {
		r0 = rf(spec, subscriberId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosFeed_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UnSubscribeToLocalBucketHighSeqnosFeed'
type BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosFeed_Call struct {
	*mock.Call
}

// UnSubscribeToLocalBucketHighSeqnosFeed is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - subscriberId string
func (_e *BucketTopologySvc_Expecter) UnSubscribeToLocalBucketHighSeqnosFeed(spec interface{}, subscriberId interface{}) *BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosFeed_Call {
	return &BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosFeed_Call{Call: _e.mock.On("UnSubscribeToLocalBucketHighSeqnosFeed", spec, subscriberId)}
}

func (_c *BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosFeed_Call) Run(run func(spec *metadata.ReplicationSpecification, subscriberId string)) *BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosFeed_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(string))
	})
	return _c
}

func (_c *BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosFeed_Call) Return(_a0 error) *BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosFeed_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosFeed_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, string) error) *BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosFeed_Call {
	_c.Call.Return(run)
	return _c
}

// UnSubscribeToLocalBucketHighSeqnosLegacyFeed provides a mock function with given fields: spec, subscriberId
func (_m *BucketTopologySvc) UnSubscribeToLocalBucketHighSeqnosLegacyFeed(spec *metadata.ReplicationSpecification, subscriberId string) error {
	ret := _m.Called(spec, subscriberId)

	if len(ret) == 0 {
		panic("no return value specified for UnSubscribeToLocalBucketHighSeqnosLegacyFeed")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string) error); ok {
		r0 = rf(spec, subscriberId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosLegacyFeed_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UnSubscribeToLocalBucketHighSeqnosLegacyFeed'
type BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosLegacyFeed_Call struct {
	*mock.Call
}

// UnSubscribeToLocalBucketHighSeqnosLegacyFeed is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - subscriberId string
func (_e *BucketTopologySvc_Expecter) UnSubscribeToLocalBucketHighSeqnosLegacyFeed(spec interface{}, subscriberId interface{}) *BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosLegacyFeed_Call {
	return &BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosLegacyFeed_Call{Call: _e.mock.On("UnSubscribeToLocalBucketHighSeqnosLegacyFeed", spec, subscriberId)}
}

func (_c *BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosLegacyFeed_Call) Run(run func(spec *metadata.ReplicationSpecification, subscriberId string)) *BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosLegacyFeed_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(string))
	})
	return _c
}

func (_c *BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosLegacyFeed_Call) Return(_a0 error) *BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosLegacyFeed_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosLegacyFeed_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, string) error) *BucketTopologySvc_UnSubscribeToLocalBucketHighSeqnosLegacyFeed_Call {
	_c.Call.Return(run)
	return _c
}

// UnSubscribeToLocalBucketMaxVbCasStatFeed provides a mock function with given fields: spec, subscriberId
func (_m *BucketTopologySvc) UnSubscribeToLocalBucketMaxVbCasStatFeed(spec *metadata.ReplicationSpecification, subscriberId string) error {
	ret := _m.Called(spec, subscriberId)

	if len(ret) == 0 {
		panic("no return value specified for UnSubscribeToLocalBucketMaxVbCasStatFeed")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string) error); ok {
		r0 = rf(spec, subscriberId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// BucketTopologySvc_UnSubscribeToLocalBucketMaxVbCasStatFeed_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UnSubscribeToLocalBucketMaxVbCasStatFeed'
type BucketTopologySvc_UnSubscribeToLocalBucketMaxVbCasStatFeed_Call struct {
	*mock.Call
}

// UnSubscribeToLocalBucketMaxVbCasStatFeed is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - subscriberId string
func (_e *BucketTopologySvc_Expecter) UnSubscribeToLocalBucketMaxVbCasStatFeed(spec interface{}, subscriberId interface{}) *BucketTopologySvc_UnSubscribeToLocalBucketMaxVbCasStatFeed_Call {
	return &BucketTopologySvc_UnSubscribeToLocalBucketMaxVbCasStatFeed_Call{Call: _e.mock.On("UnSubscribeToLocalBucketMaxVbCasStatFeed", spec, subscriberId)}
}

func (_c *BucketTopologySvc_UnSubscribeToLocalBucketMaxVbCasStatFeed_Call) Run(run func(spec *metadata.ReplicationSpecification, subscriberId string)) *BucketTopologySvc_UnSubscribeToLocalBucketMaxVbCasStatFeed_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(string))
	})
	return _c
}

func (_c *BucketTopologySvc_UnSubscribeToLocalBucketMaxVbCasStatFeed_Call) Return(_a0 error) *BucketTopologySvc_UnSubscribeToLocalBucketMaxVbCasStatFeed_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *BucketTopologySvc_UnSubscribeToLocalBucketMaxVbCasStatFeed_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, string) error) *BucketTopologySvc_UnSubscribeToLocalBucketMaxVbCasStatFeed_Call {
	_c.Call.Return(run)
	return _c
}

// UnSubscribeToRemoteKVStatsFeed provides a mock function with given fields: spec, subscriberId
func (_m *BucketTopologySvc) UnSubscribeToRemoteKVStatsFeed(spec *metadata.ReplicationSpecification, subscriberId string) error {
	ret := _m.Called(spec, subscriberId)

	if len(ret) == 0 {
		panic("no return value specified for UnSubscribeToRemoteKVStatsFeed")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*metadata.ReplicationSpecification, string) error); ok {
		r0 = rf(spec, subscriberId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// BucketTopologySvc_UnSubscribeToRemoteKVStatsFeed_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UnSubscribeToRemoteKVStatsFeed'
type BucketTopologySvc_UnSubscribeToRemoteKVStatsFeed_Call struct {
	*mock.Call
}

// UnSubscribeToRemoteKVStatsFeed is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
//   - subscriberId string
func (_e *BucketTopologySvc_Expecter) UnSubscribeToRemoteKVStatsFeed(spec interface{}, subscriberId interface{}) *BucketTopologySvc_UnSubscribeToRemoteKVStatsFeed_Call {
	return &BucketTopologySvc_UnSubscribeToRemoteKVStatsFeed_Call{Call: _e.mock.On("UnSubscribeToRemoteKVStatsFeed", spec, subscriberId)}
}

func (_c *BucketTopologySvc_UnSubscribeToRemoteKVStatsFeed_Call) Run(run func(spec *metadata.ReplicationSpecification, subscriberId string)) *BucketTopologySvc_UnSubscribeToRemoteKVStatsFeed_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(string))
	})
	return _c
}

func (_c *BucketTopologySvc_UnSubscribeToRemoteKVStatsFeed_Call) Return(_a0 error) *BucketTopologySvc_UnSubscribeToRemoteKVStatsFeed_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *BucketTopologySvc_UnSubscribeToRemoteKVStatsFeed_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, string) error) *BucketTopologySvc_UnSubscribeToRemoteKVStatsFeed_Call {
	_c.Call.Return(run)
	return _c
}

// NewBucketTopologySvc creates a new instance of BucketTopologySvc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewBucketTopologySvc(t interface {
	mock.TestingT
	Cleanup(func())
}) *BucketTopologySvc {
	mock := &BucketTopologySvc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
