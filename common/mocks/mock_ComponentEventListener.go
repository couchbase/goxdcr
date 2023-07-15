// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	common "github.com/couchbase/goxdcr/common"
	mock "github.com/stretchr/testify/mock"
)

// ComponentEventListener is an autogenerated mock type for the ComponentEventListener type
type ComponentEventListener struct {
	mock.Mock
}

type ComponentEventListener_Expecter struct {
	mock *mock.Mock
}

func (_m *ComponentEventListener) EXPECT() *ComponentEventListener_Expecter {
	return &ComponentEventListener_Expecter{mock: &_m.Mock}
}

// OnEvent provides a mock function with given fields: event
func (_m *ComponentEventListener) OnEvent(event *common.Event) {
	_m.Called(event)
}

// ComponentEventListener_OnEvent_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'OnEvent'
type ComponentEventListener_OnEvent_Call struct {
	*mock.Call
}

// OnEvent is a helper method to define mock.On call
//   - event *common.Event
func (_e *ComponentEventListener_Expecter) OnEvent(event interface{}) *ComponentEventListener_OnEvent_Call {
	return &ComponentEventListener_OnEvent_Call{Call: _e.mock.On("OnEvent", event)}
}

func (_c *ComponentEventListener_OnEvent_Call) Run(run func(event *common.Event)) *ComponentEventListener_OnEvent_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*common.Event))
	})
	return _c
}

func (_c *ComponentEventListener_OnEvent_Call) Return() *ComponentEventListener_OnEvent_Call {
	_c.Call.Return()
	return _c
}

func (_c *ComponentEventListener_OnEvent_Call) RunAndReturn(run func(*common.Event)) *ComponentEventListener_OnEvent_Call {
	_c.Call.Return(run)
	return _c
}

// NewComponentEventListener creates a new instance of ComponentEventListener. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewComponentEventListener(t interface {
	mock.TestingT
	Cleanup(func())
}) *ComponentEventListener {
	mock := &ComponentEventListener{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
