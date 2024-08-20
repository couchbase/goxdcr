// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	metadata "github.com/couchbase/goxdcr/v8/metadata"
	mock "github.com/stretchr/testify/mock"

	peerToPeer "github.com/couchbase/goxdcr/v8/peerToPeer"
)

// Handler is an autogenerated mock type for the Handler type
type Handler struct {
	mock.Mock
}

type Handler_Expecter struct {
	mock *mock.Mock
}

func (_m *Handler) EXPECT() *Handler_Expecter {
	return &Handler_Expecter{mock: &_m.Mock}
}

// GetReqAndClearOpaque provides a mock function with given fields: opaque
func (_m *Handler) GetReqAndClearOpaque(opaque uint32) (*peerToPeer.Request, chan peerToPeer.ReqRespPair, bool) {
	ret := _m.Called(opaque)

	if len(ret) == 0 {
		panic("no return value specified for GetReqAndClearOpaque")
	}

	var r0 *peerToPeer.Request
	var r1 chan peerToPeer.ReqRespPair
	var r2 bool
	if rf, ok := ret.Get(0).(func(uint32) (*peerToPeer.Request, chan peerToPeer.ReqRespPair, bool)); ok {
		return rf(opaque)
	}
	if rf, ok := ret.Get(0).(func(uint32) *peerToPeer.Request); ok {
		r0 = rf(opaque)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*peerToPeer.Request)
		}
	}

	if rf, ok := ret.Get(1).(func(uint32) chan peerToPeer.ReqRespPair); ok {
		r1 = rf(opaque)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(chan peerToPeer.ReqRespPair)
		}
	}

	if rf, ok := ret.Get(2).(func(uint32) bool); ok {
		r2 = rf(opaque)
	} else {
		r2 = ret.Get(2).(bool)
	}

	return r0, r1, r2
}

// Handler_GetReqAndClearOpaque_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetReqAndClearOpaque'
type Handler_GetReqAndClearOpaque_Call struct {
	*mock.Call
}

// GetReqAndClearOpaque is a helper method to define mock.On call
//   - opaque uint32
func (_e *Handler_Expecter) GetReqAndClearOpaque(opaque interface{}) *Handler_GetReqAndClearOpaque_Call {
	return &Handler_GetReqAndClearOpaque_Call{Call: _e.mock.On("GetReqAndClearOpaque", opaque)}
}

func (_c *Handler_GetReqAndClearOpaque_Call) Run(run func(opaque uint32)) *Handler_GetReqAndClearOpaque_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint32))
	})
	return _c
}

func (_c *Handler_GetReqAndClearOpaque_Call) Return(_a0 *peerToPeer.Request, _a1 chan peerToPeer.ReqRespPair, _a2 bool) *Handler_GetReqAndClearOpaque_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *Handler_GetReqAndClearOpaque_Call) RunAndReturn(run func(uint32) (*peerToPeer.Request, chan peerToPeer.ReqRespPair, bool)) *Handler_GetReqAndClearOpaque_Call {
	_c.Call.Return(run)
	return _c
}

// GetSpecDelNotification provides a mock function with given fields: specId, internalId
func (_m *Handler) GetSpecDelNotification(specId string, internalId string) (chan bool, error) {
	ret := _m.Called(specId, internalId)

	if len(ret) == 0 {
		panic("no return value specified for GetSpecDelNotification")
	}

	var r0 chan bool
	var r1 error
	if rf, ok := ret.Get(0).(func(string, string) (chan bool, error)); ok {
		return rf(specId, internalId)
	}
	if rf, ok := ret.Get(0).(func(string, string) chan bool); ok {
		r0 = rf(specId, internalId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(chan bool)
		}
	}

	if rf, ok := ret.Get(1).(func(string, string) error); ok {
		r1 = rf(specId, internalId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Handler_GetSpecDelNotification_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSpecDelNotification'
type Handler_GetSpecDelNotification_Call struct {
	*mock.Call
}

// GetSpecDelNotification is a helper method to define mock.On call
//   - specId string
//   - internalId string
func (_e *Handler_Expecter) GetSpecDelNotification(specId interface{}, internalId interface{}) *Handler_GetSpecDelNotification_Call {
	return &Handler_GetSpecDelNotification_Call{Call: _e.mock.On("GetSpecDelNotification", specId, internalId)}
}

func (_c *Handler_GetSpecDelNotification_Call) Run(run func(specId string, internalId string)) *Handler_GetSpecDelNotification_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string))
	})
	return _c
}

func (_c *Handler_GetSpecDelNotification_Call) Return(_a0 chan bool, _a1 error) *Handler_GetSpecDelNotification_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *Handler_GetSpecDelNotification_Call) RunAndReturn(run func(string, string) (chan bool, error)) *Handler_GetSpecDelNotification_Call {
	_c.Call.Return(run)
	return _c
}

// HandleSpecChange provides a mock function with given fields: oldSpec, newSpec
func (_m *Handler) HandleSpecChange(oldSpec *metadata.ReplicationSpecification, newSpec *metadata.ReplicationSpecification) {
	_m.Called(oldSpec, newSpec)
}

// Handler_HandleSpecChange_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'HandleSpecChange'
type Handler_HandleSpecChange_Call struct {
	*mock.Call
}

// HandleSpecChange is a helper method to define mock.On call
//   - oldSpec *metadata.ReplicationSpecification
//   - newSpec *metadata.ReplicationSpecification
func (_e *Handler_Expecter) HandleSpecChange(oldSpec interface{}, newSpec interface{}) *Handler_HandleSpecChange_Call {
	return &Handler_HandleSpecChange_Call{Call: _e.mock.On("HandleSpecChange", oldSpec, newSpec)}
}

func (_c *Handler_HandleSpecChange_Call) Run(run func(oldSpec *metadata.ReplicationSpecification, newSpec *metadata.ReplicationSpecification)) *Handler_HandleSpecChange_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification), args[1].(*metadata.ReplicationSpecification))
	})
	return _c
}

func (_c *Handler_HandleSpecChange_Call) Return() *Handler_HandleSpecChange_Call {
	_c.Call.Return()
	return _c
}

func (_c *Handler_HandleSpecChange_Call) RunAndReturn(run func(*metadata.ReplicationSpecification, *metadata.ReplicationSpecification)) *Handler_HandleSpecChange_Call {
	_c.Call.Return(run)
	return _c
}

// HandleSpecCreation provides a mock function with given fields: newSpec
func (_m *Handler) HandleSpecCreation(newSpec *metadata.ReplicationSpecification) {
	_m.Called(newSpec)
}

// Handler_HandleSpecCreation_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'HandleSpecCreation'
type Handler_HandleSpecCreation_Call struct {
	*mock.Call
}

// HandleSpecCreation is a helper method to define mock.On call
//   - newSpec *metadata.ReplicationSpecification
func (_e *Handler_Expecter) HandleSpecCreation(newSpec interface{}) *Handler_HandleSpecCreation_Call {
	return &Handler_HandleSpecCreation_Call{Call: _e.mock.On("HandleSpecCreation", newSpec)}
}

func (_c *Handler_HandleSpecCreation_Call) Run(run func(newSpec *metadata.ReplicationSpecification)) *Handler_HandleSpecCreation_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification))
	})
	return _c
}

func (_c *Handler_HandleSpecCreation_Call) Return() *Handler_HandleSpecCreation_Call {
	_c.Call.Return()
	return _c
}

func (_c *Handler_HandleSpecCreation_Call) RunAndReturn(run func(*metadata.ReplicationSpecification)) *Handler_HandleSpecCreation_Call {
	_c.Call.Return(run)
	return _c
}

// HandleSpecDeletion provides a mock function with given fields: oldSpec
func (_m *Handler) HandleSpecDeletion(oldSpec *metadata.ReplicationSpecification) {
	_m.Called(oldSpec)
}

// Handler_HandleSpecDeletion_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'HandleSpecDeletion'
type Handler_HandleSpecDeletion_Call struct {
	*mock.Call
}

// HandleSpecDeletion is a helper method to define mock.On call
//   - oldSpec *metadata.ReplicationSpecification
func (_e *Handler_Expecter) HandleSpecDeletion(oldSpec interface{}) *Handler_HandleSpecDeletion_Call {
	return &Handler_HandleSpecDeletion_Call{Call: _e.mock.On("HandleSpecDeletion", oldSpec)}
}

func (_c *Handler_HandleSpecDeletion_Call) Run(run func(oldSpec *metadata.ReplicationSpecification)) *Handler_HandleSpecDeletion_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*metadata.ReplicationSpecification))
	})
	return _c
}

func (_c *Handler_HandleSpecDeletion_Call) Return() *Handler_HandleSpecDeletion_Call {
	_c.Call.Return()
	return _c
}

func (_c *Handler_HandleSpecDeletion_Call) RunAndReturn(run func(*metadata.ReplicationSpecification)) *Handler_HandleSpecDeletion_Call {
	_c.Call.Return(run)
	return _c
}

// RegisterOpaque provides a mock function with given fields: req, opts
func (_m *Handler) RegisterOpaque(req peerToPeer.Request, opts *peerToPeer.SendOpts) error {
	ret := _m.Called(req, opts)

	if len(ret) == 0 {
		panic("no return value specified for RegisterOpaque")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(peerToPeer.Request, *peerToPeer.SendOpts) error); ok {
		r0 = rf(req, opts)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Handler_RegisterOpaque_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RegisterOpaque'
type Handler_RegisterOpaque_Call struct {
	*mock.Call
}

// RegisterOpaque is a helper method to define mock.On call
//   - req peerToPeer.Request
//   - opts *peerToPeer.SendOpts
func (_e *Handler_Expecter) RegisterOpaque(req interface{}, opts interface{}) *Handler_RegisterOpaque_Call {
	return &Handler_RegisterOpaque_Call{Call: _e.mock.On("RegisterOpaque", req, opts)}
}

func (_c *Handler_RegisterOpaque_Call) Run(run func(req peerToPeer.Request, opts *peerToPeer.SendOpts)) *Handler_RegisterOpaque_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(peerToPeer.Request), args[1].(*peerToPeer.SendOpts))
	})
	return _c
}

func (_c *Handler_RegisterOpaque_Call) Return(_a0 error) *Handler_RegisterOpaque_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *Handler_RegisterOpaque_Call) RunAndReturn(run func(peerToPeer.Request, *peerToPeer.SendOpts) error) *Handler_RegisterOpaque_Call {
	_c.Call.Return(run)
	return _c
}

// Start provides a mock function with given fields:
func (_m *Handler) Start() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Start")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Handler_Start_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Start'
type Handler_Start_Call struct {
	*mock.Call
}

// Start is a helper method to define mock.On call
func (_e *Handler_Expecter) Start() *Handler_Start_Call {
	return &Handler_Start_Call{Call: _e.mock.On("Start")}
}

func (_c *Handler_Start_Call) Run(run func()) *Handler_Start_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *Handler_Start_Call) Return(_a0 error) *Handler_Start_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *Handler_Start_Call) RunAndReturn(run func() error) *Handler_Start_Call {
	_c.Call.Return(run)
	return _c
}

// Stop provides a mock function with given fields:
func (_m *Handler) Stop() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Stop")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Handler_Stop_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Stop'
type Handler_Stop_Call struct {
	*mock.Call
}

// Stop is a helper method to define mock.On call
func (_e *Handler_Expecter) Stop() *Handler_Stop_Call {
	return &Handler_Stop_Call{Call: _e.mock.On("Stop")}
}

func (_c *Handler_Stop_Call) Run(run func()) *Handler_Stop_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *Handler_Stop_Call) Return(_a0 error) *Handler_Stop_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *Handler_Stop_Call) RunAndReturn(run func() error) *Handler_Stop_Call {
	_c.Call.Return(run)
	return _c
}

// NewHandler creates a new instance of Handler. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewHandler(t interface {
	mock.TestingT
	Cleanup(func())
}) *Handler {
	mock := &Handler{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
