// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/v8/metadata"
	mock "github.com/stretchr/testify/mock"
)

// ReplicaReplicator is an autogenerated mock type for the ReplicaReplicator type
type ReplicaReplicator struct {
	mock.Mock
}

type ReplicaReplicator_Expecter struct {
	mock *mock.Mock
}

func (_m *ReplicaReplicator) EXPECT() *ReplicaReplicator_Expecter {
	return &ReplicaReplicator_Expecter{mock: &_m.Mock}
}

// HandleSpecChange provides a mock function with given fields: oldSpec, newSpec
func (_m *ReplicaReplicator) HandleSpecChange(oldSpec *metadata.ReplicationSpecification, newSpec *metadata.ReplicationSpecification) {
	_m.Called(oldSpec, newSpec)
}

// ReplicaReplicator_HandleSpecChange_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'HandleSpecChange'
type ReplicaReplicator_HandleSpecChange_Call struct {
	*mock.Call
}

// HandleSpecChange is a helper method to define mock.On call
//   - oldSpec *metadata.ReplicationSpecification
//   - newSpec *metadata.ReplicationSpecification
func (_e *ReplicaReplicator_Expecter) HandleSpecChange(oldSpec interface{}, newSpec interface{}) *ReplicaReplicator_HandleSpecChange_Call {
	return &ReplicaReplicator_HandleSpecChange_Call{Call: _e.mock.On("HandleSpecChange", oldSpec, newSpec)}
}

func (_c *ReplicaReplicator_HandleSpecChange_Call) Run(run func(oldSpec *metadata.ReplicationSpecification, newSpec *metadata.ReplicationSpecification)) *ReplicaReplicator_HandleSpecChange_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(*metadata.ReplicationSpecification))
	})
	return _c
}

func (_c *ReplicaReplicator_HandleSpecChange_Call) Return() *ReplicaReplicator_HandleSpecChange_Call {
	_c.Call.Return()
	return _c
}

func (_c *ReplicaReplicator_HandleSpecChange_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, *metadata.ReplicationSpecification)) *ReplicaReplicator_HandleSpecChange_Call {
	_c.Run(run)
	return _c
}

// HandleSpecCreation provides a mock function with given fields: spec
func (_m *ReplicaReplicator) HandleSpecCreation(spec *metadata.ReplicationSpecification) {
	_m.Called(spec)
}

// ReplicaReplicator_HandleSpecCreation_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'HandleSpecCreation'
type ReplicaReplicator_HandleSpecCreation_Call struct {
	*mock.Call
}

// HandleSpecCreation is a helper method to define mock.On call
//   - spec *metadata.ReplicationSpecification
func (_e *ReplicaReplicator_Expecter) HandleSpecCreation(spec interface{}) *ReplicaReplicator_HandleSpecCreation_Call {
	return &ReplicaReplicator_HandleSpecCreation_Call{Call: _e.mock.On("HandleSpecCreation", spec)}
}

func (_c *ReplicaReplicator_HandleSpecCreation_Call) Run(run func(spec *metadata.ReplicationSpecification)) *ReplicaReplicator_HandleSpecCreation_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification))
	})
	return _c
}

func (_c *ReplicaReplicator_HandleSpecCreation_Call) Return() *ReplicaReplicator_HandleSpecCreation_Call {
	_c.Call.Return()
	return _c
}

func (_c *ReplicaReplicator_HandleSpecCreation_Call) RunAndReturn(run func(*metadata.ReplicationSpecification)) *ReplicaReplicator_HandleSpecCreation_Call {
	_c.Run(run)
	return _c
}

// HandleSpecDeletion provides a mock function with given fields: oldSpec
func (_m *ReplicaReplicator) HandleSpecDeletion(oldSpec *metadata.ReplicationSpecification) {
	_m.Called(oldSpec)
}

// ReplicaReplicator_HandleSpecDeletion_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'HandleSpecDeletion'
type ReplicaReplicator_HandleSpecDeletion_Call struct {
	*mock.Call
}

// HandleSpecDeletion is a helper method to define mock.On call
//   - oldSpec *metadata.ReplicationSpecification
func (_e *ReplicaReplicator_Expecter) HandleSpecDeletion(oldSpec interface{}) *ReplicaReplicator_HandleSpecDeletion_Call {
	return &ReplicaReplicator_HandleSpecDeletion_Call{Call: _e.mock.On("HandleSpecDeletion", oldSpec)}
}

func (_c *ReplicaReplicator_HandleSpecDeletion_Call) Run(run func(oldSpec *metadata.ReplicationSpecification)) *ReplicaReplicator_HandleSpecDeletion_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification))
	})
	return _c
}

func (_c *ReplicaReplicator_HandleSpecDeletion_Call) Return() *ReplicaReplicator_HandleSpecDeletion_Call {
	_c.Call.Return()
	return _c
}

func (_c *ReplicaReplicator_HandleSpecDeletion_Call) RunAndReturn(run func(*metadata.ReplicationSpecification)) *ReplicaReplicator_HandleSpecDeletion_Call {
	_c.Run(run)
	return _c
}

// RequestImmediatePush provides a mock function with given fields: replId
func (_m *ReplicaReplicator) RequestImmediatePush(replId string) error {
	ret := _m.Called(replId)

	if len(ret) == 0 {
		panic("no return value specified for RequestImmediatePush")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(replId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReplicaReplicator_RequestImmediatePush_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RequestImmediatePush'
type ReplicaReplicator_RequestImmediatePush_Call struct {
	*mock.Call
}

// RequestImmediatePush is a helper method to define mock.On call
//   - replId string
func (_e *ReplicaReplicator_Expecter) RequestImmediatePush(replId interface{}) *ReplicaReplicator_RequestImmediatePush_Call {
	return &ReplicaReplicator_RequestImmediatePush_Call{Call: _e.mock.On("RequestImmediatePush", replId)}
}

func (_c *ReplicaReplicator_RequestImmediatePush_Call) Run(run func(replId string)) *ReplicaReplicator_RequestImmediatePush_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *ReplicaReplicator_RequestImmediatePush_Call) Return(_a0 error) *ReplicaReplicator_RequestImmediatePush_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ReplicaReplicator_RequestImmediatePush_Call) RunAndReturn(run func(string) error) *ReplicaReplicator_RequestImmediatePush_Call {
	_c.Call.Return(run)
	return _c
}

// Start provides a mock function with no fields
func (_m *ReplicaReplicator) Start() {
	_m.Called()
}

// ReplicaReplicator_Start_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Start'
type ReplicaReplicator_Start_Call struct {
	*mock.Call
}

// Start is a helper method to define mock.On call
func (_e *ReplicaReplicator_Expecter) Start() *ReplicaReplicator_Start_Call {
	return &ReplicaReplicator_Start_Call{Call: _e.mock.On("Start")}
}

func (_c *ReplicaReplicator_Start_Call) Run(run func()) *ReplicaReplicator_Start_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ReplicaReplicator_Start_Call) Return() *ReplicaReplicator_Start_Call {
	_c.Call.Return()
	return _c
}

func (_c *ReplicaReplicator_Start_Call) RunAndReturn(run func()) *ReplicaReplicator_Start_Call {
	_c.Run(run)
	return _c
}

// Stop provides a mock function with no fields
func (_m *ReplicaReplicator) Stop() {
	_m.Called()
}

// ReplicaReplicator_Stop_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Stop'
type ReplicaReplicator_Stop_Call struct {
	*mock.Call
}

// Stop is a helper method to define mock.On call
func (_e *ReplicaReplicator_Expecter) Stop() *ReplicaReplicator_Stop_Call {
	return &ReplicaReplicator_Stop_Call{Call: _e.mock.On("Stop")}
}

func (_c *ReplicaReplicator_Stop_Call) Run(run func()) *ReplicaReplicator_Stop_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ReplicaReplicator_Stop_Call) Return() *ReplicaReplicator_Stop_Call {
	_c.Call.Return()
	return _c
}

func (_c *ReplicaReplicator_Stop_Call) RunAndReturn(run func()) *ReplicaReplicator_Stop_Call {
	_c.Run(run)
	return _c
}

// NewReplicaReplicator creates a new instance of ReplicaReplicator. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewReplicaReplicator(t interface {
	mock.TestingT
	Cleanup(func())
}) *ReplicaReplicator {
	mock := &ReplicaReplicator{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
