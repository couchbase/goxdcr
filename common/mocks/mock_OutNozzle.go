// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	common "github.com/couchbase/goxdcr/v8/common"
	metadata "github.com/couchbase/goxdcr/v8/metadata"

	mock "github.com/stretchr/testify/mock"
)

// OutNozzle is an autogenerated mock type for the OutNozzle type
type OutNozzle struct {
	mock.Mock
}

type OutNozzle_Expecter struct {
	mock *mock.Mock
}

func (_m *OutNozzle) EXPECT() *OutNozzle_Expecter {
	return &OutNozzle_Expecter{mock: &_m.Mock}
}

// AsyncComponentEventListeners provides a mock function with no fields
func (_m *OutNozzle) AsyncComponentEventListeners() map[string]common.AsyncComponentEventListener {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for AsyncComponentEventListeners")
	}

	var r0 map[string]common.AsyncComponentEventListener
	if rf, ok := ret.Get(0).(func() map[string]common.AsyncComponentEventListener); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]common.AsyncComponentEventListener)
		}
	}

	return r0
}

// OutNozzle_AsyncComponentEventListeners_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AsyncComponentEventListeners'
type OutNozzle_AsyncComponentEventListeners_Call struct {
	*mock.Call
}

// AsyncComponentEventListeners is a helper method to define mock.On call
func (_e *OutNozzle_Expecter) AsyncComponentEventListeners() *OutNozzle_AsyncComponentEventListeners_Call {
	return &OutNozzle_AsyncComponentEventListeners_Call{Call: _e.mock.On("AsyncComponentEventListeners")}
}

func (_c *OutNozzle_AsyncComponentEventListeners_Call) Run(run func()) *OutNozzle_AsyncComponentEventListeners_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *OutNozzle_AsyncComponentEventListeners_Call) Return(_a0 map[string]common.AsyncComponentEventListener) *OutNozzle_AsyncComponentEventListeners_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_AsyncComponentEventListeners_Call) RunAndReturn(run func() map[string]common.AsyncComponentEventListener) *OutNozzle_AsyncComponentEventListeners_Call {
	_c.Call.Return(run)
	return _c
}

// Close provides a mock function with no fields
func (_m *OutNozzle) Close() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Close")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// OutNozzle_Close_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Close'
type OutNozzle_Close_Call struct {
	*mock.Call
}

// Close is a helper method to define mock.On call
func (_e *OutNozzle_Expecter) Close() *OutNozzle_Close_Call {
	return &OutNozzle_Close_Call{Call: _e.mock.On("Close")}
}

func (_c *OutNozzle_Close_Call) Run(run func()) *OutNozzle_Close_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *OutNozzle_Close_Call) Return(_a0 error) *OutNozzle_Close_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_Close_Call) RunAndReturn(run func() error) *OutNozzle_Close_Call {
	_c.Call.Return(run)
	return _c
}

// Connector provides a mock function with no fields
func (_m *OutNozzle) Connector() common.Connector {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Connector")
	}

	var r0 common.Connector
	if rf, ok := ret.Get(0).(func() common.Connector); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.Connector)
		}
	}

	return r0
}

// OutNozzle_Connector_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Connector'
type OutNozzle_Connector_Call struct {
	*mock.Call
}

// Connector is a helper method to define mock.On call
func (_e *OutNozzle_Expecter) Connector() *OutNozzle_Connector_Call {
	return &OutNozzle_Connector_Call{Call: _e.mock.On("Connector")}
}

func (_c *OutNozzle_Connector_Call) Run(run func()) *OutNozzle_Connector_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *OutNozzle_Connector_Call) Return(_a0 common.Connector) *OutNozzle_Connector_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_Connector_Call) RunAndReturn(run func() common.Connector) *OutNozzle_Connector_Call {
	_c.Call.Return(run)
	return _c
}

// GetConflictLogger provides a mock function with no fields
func (_m *OutNozzle) GetConflictLogger() interface{} {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetConflictLogger")
	}

	var r0 interface{}
	if rf, ok := ret.Get(0).(func() interface{}); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	return r0
}

// OutNozzle_GetConflictLogger_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetConflictLogger'
type OutNozzle_GetConflictLogger_Call struct {
	*mock.Call
}

// GetConflictLogger is a helper method to define mock.On call
func (_e *OutNozzle_Expecter) GetConflictLogger() *OutNozzle_GetConflictLogger_Call {
	return &OutNozzle_GetConflictLogger_Call{Call: _e.mock.On("GetConflictLogger")}
}

func (_c *OutNozzle_GetConflictLogger_Call) Run(run func()) *OutNozzle_GetConflictLogger_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *OutNozzle_GetConflictLogger_Call) Return(_a0 interface{}) *OutNozzle_GetConflictLogger_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_GetConflictLogger_Call) RunAndReturn(run func() interface{}) *OutNozzle_GetConflictLogger_Call {
	_c.Call.Return(run)
	return _c
}

// Id provides a mock function with no fields
func (_m *OutNozzle) Id() string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Id")
	}

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// OutNozzle_Id_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Id'
type OutNozzle_Id_Call struct {
	*mock.Call
}

// Id is a helper method to define mock.On call
func (_e *OutNozzle_Expecter) Id() *OutNozzle_Id_Call {
	return &OutNozzle_Id_Call{Call: _e.mock.On("Id")}
}

func (_c *OutNozzle_Id_Call) Run(run func()) *OutNozzle_Id_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *OutNozzle_Id_Call) Return(_a0 string) *OutNozzle_Id_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_Id_Call) RunAndReturn(run func() string) *OutNozzle_Id_Call {
	_c.Call.Return(run)
	return _c
}

// IsOpen provides a mock function with no fields
func (_m *OutNozzle) IsOpen() bool {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for IsOpen")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// OutNozzle_IsOpen_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'IsOpen'
type OutNozzle_IsOpen_Call struct {
	*mock.Call
}

// IsOpen is a helper method to define mock.On call
func (_e *OutNozzle_Expecter) IsOpen() *OutNozzle_IsOpen_Call {
	return &OutNozzle_IsOpen_Call{Call: _e.mock.On("IsOpen")}
}

func (_c *OutNozzle_IsOpen_Call) Run(run func()) *OutNozzle_IsOpen_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *OutNozzle_IsOpen_Call) Return(_a0 bool) *OutNozzle_IsOpen_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_IsOpen_Call) RunAndReturn(run func() bool) *OutNozzle_IsOpen_Call {
	_c.Call.Return(run)
	return _c
}

// Open provides a mock function with no fields
func (_m *OutNozzle) Open() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Open")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// OutNozzle_Open_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Open'
type OutNozzle_Open_Call struct {
	*mock.Call
}

// Open is a helper method to define mock.On call
func (_e *OutNozzle_Expecter) Open() *OutNozzle_Open_Call {
	return &OutNozzle_Open_Call{Call: _e.mock.On("Open")}
}

func (_c *OutNozzle_Open_Call) Run(run func()) *OutNozzle_Open_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *OutNozzle_Open_Call) Return(_a0 error) *OutNozzle_Open_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_Open_Call) RunAndReturn(run func() error) *OutNozzle_Open_Call {
	_c.Call.Return(run)
	return _c
}

// RaiseEvent provides a mock function with given fields: event
func (_m *OutNozzle) RaiseEvent(event *common.Event) {
	_m.Called(event)
}

// OutNozzle_RaiseEvent_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RaiseEvent'
type OutNozzle_RaiseEvent_Call struct {
	*mock.Call
}

// RaiseEvent is a helper method to define mock.On call
//   - event *common.Event
func (_e *OutNozzle_Expecter) RaiseEvent(event interface{}) *OutNozzle_RaiseEvent_Call {
	return &OutNozzle_RaiseEvent_Call{Call: _e.mock.On("RaiseEvent", event)}
}

func (_c *OutNozzle_RaiseEvent_Call) Run(run func(event *common.Event)) *OutNozzle_RaiseEvent_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*common.Event))
	})
	return _c
}

func (_c *OutNozzle_RaiseEvent_Call) Return() *OutNozzle_RaiseEvent_Call {
	_c.Call.Return()
	return _c
}

func (_c *OutNozzle_RaiseEvent_Call) RunAndReturn(run func(*common.Event)) *OutNozzle_RaiseEvent_Call {
	_c.Run(run)
	return _c
}

// Receive provides a mock function with given fields: data
func (_m *OutNozzle) Receive(data interface{}) error {
	ret := _m.Called(data)

	if len(ret) == 0 {
		panic("no return value specified for Receive")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(data)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// OutNozzle_Receive_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Receive'
type OutNozzle_Receive_Call struct {
	*mock.Call
}

// Receive is a helper method to define mock.On call
//   - data interface{}
func (_e *OutNozzle_Expecter) Receive(data interface{}) *OutNozzle_Receive_Call {
	return &OutNozzle_Receive_Call{Call: _e.mock.On("Receive", data)}
}

func (_c *OutNozzle_Receive_Call) Run(run func(data interface{})) *OutNozzle_Receive_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(interface{}))
	})
	return _c
}

func (_c *OutNozzle_Receive_Call) Return(_a0 error) *OutNozzle_Receive_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_Receive_Call) RunAndReturn(run func(interface{}) error) *OutNozzle_Receive_Call {
	_c.Call.Return(run)
	return _c
}

// RecycleDataObj provides a mock function with given fields: obj
func (_m *OutNozzle) RecycleDataObj(obj interface{}) {
	_m.Called(obj)
}

// OutNozzle_RecycleDataObj_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RecycleDataObj'
type OutNozzle_RecycleDataObj_Call struct {
	*mock.Call
}

// RecycleDataObj is a helper method to define mock.On call
//   - obj interface{}
func (_e *OutNozzle_Expecter) RecycleDataObj(obj interface{}) *OutNozzle_RecycleDataObj_Call {
	return &OutNozzle_RecycleDataObj_Call{Call: _e.mock.On("RecycleDataObj", obj)}
}

func (_c *OutNozzle_RecycleDataObj_Call) Run(run func(obj interface{})) *OutNozzle_RecycleDataObj_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(interface{}))
	})
	return _c
}

func (_c *OutNozzle_RecycleDataObj_Call) Return() *OutNozzle_RecycleDataObj_Call {
	_c.Call.Return()
	return _c
}

func (_c *OutNozzle_RecycleDataObj_Call) RunAndReturn(run func(interface{})) *OutNozzle_RecycleDataObj_Call {
	_c.Run(run)
	return _c
}

// RegisterComponentEventListener provides a mock function with given fields: eventType, listener
func (_m *OutNozzle) RegisterComponentEventListener(eventType common.ComponentEventType, listener common.ComponentEventListener) error {
	ret := _m.Called(eventType, listener)

	if len(ret) == 0 {
		panic("no return value specified for RegisterComponentEventListener")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(common.ComponentEventType, common.ComponentEventListener) error); ok {
		r0 = rf(eventType, listener)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// OutNozzle_RegisterComponentEventListener_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RegisterComponentEventListener'
type OutNozzle_RegisterComponentEventListener_Call struct {
	*mock.Call
}

// RegisterComponentEventListener is a helper method to define mock.On call
//   - eventType common.ComponentEventType
//   - listener common.ComponentEventListener
func (_e *OutNozzle_Expecter) RegisterComponentEventListener(eventType interface{}, listener interface{}) *OutNozzle_RegisterComponentEventListener_Call {
	return &OutNozzle_RegisterComponentEventListener_Call{Call: _e.mock.On("RegisterComponentEventListener", eventType, listener)}
}

func (_c *OutNozzle_RegisterComponentEventListener_Call) Run(run func(eventType common.ComponentEventType, listener common.ComponentEventListener)) *OutNozzle_RegisterComponentEventListener_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(common.ComponentEventType), args[1].(common.ComponentEventListener))
	})
	return _c
}

func (_c *OutNozzle_RegisterComponentEventListener_Call) Return(_a0 error) *OutNozzle_RegisterComponentEventListener_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_RegisterComponentEventListener_Call) RunAndReturn(run func(common.ComponentEventType, common.ComponentEventListener) error) *OutNozzle_RegisterComponentEventListener_Call {
	_c.Call.Return(run)
	return _c
}

// ResponsibleVBs provides a mock function with no fields
func (_m *OutNozzle) ResponsibleVBs() []uint16 {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for ResponsibleVBs")
	}

	var r0 []uint16
	if rf, ok := ret.Get(0).(func() []uint16); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]uint16)
		}
	}

	return r0
}

// OutNozzle_ResponsibleVBs_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ResponsibleVBs'
type OutNozzle_ResponsibleVBs_Call struct {
	*mock.Call
}

// ResponsibleVBs is a helper method to define mock.On call
func (_e *OutNozzle_Expecter) ResponsibleVBs() *OutNozzle_ResponsibleVBs_Call {
	return &OutNozzle_ResponsibleVBs_Call{Call: _e.mock.On("ResponsibleVBs")}
}

func (_c *OutNozzle_ResponsibleVBs_Call) Run(run func()) *OutNozzle_ResponsibleVBs_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *OutNozzle_ResponsibleVBs_Call) Return(_a0 []uint16) *OutNozzle_ResponsibleVBs_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_ResponsibleVBs_Call) RunAndReturn(run func() []uint16) *OutNozzle_ResponsibleVBs_Call {
	_c.Call.Return(run)
	return _c
}

// SetConflictLogger provides a mock function with given fields: _a0
func (_m *OutNozzle) SetConflictLogger(_a0 interface{}) error {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for SetConflictLogger")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// OutNozzle_SetConflictLogger_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetConflictLogger'
type OutNozzle_SetConflictLogger_Call struct {
	*mock.Call
}

// SetConflictLogger is a helper method to define mock.On call
//   - _a0 interface{}
func (_e *OutNozzle_Expecter) SetConflictLogger(_a0 interface{}) *OutNozzle_SetConflictLogger_Call {
	return &OutNozzle_SetConflictLogger_Call{Call: _e.mock.On("SetConflictLogger", _a0)}
}

func (_c *OutNozzle_SetConflictLogger_Call) Run(run func(_a0 interface{})) *OutNozzle_SetConflictLogger_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(interface{}))
	})
	return _c
}

func (_c *OutNozzle_SetConflictLogger_Call) Return(_a0 error) *OutNozzle_SetConflictLogger_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_SetConflictLogger_Call) RunAndReturn(run func(interface{}) error) *OutNozzle_SetConflictLogger_Call {
	_c.Call.Return(run)
	return _c
}

// SetConnector provides a mock function with given fields: connector
func (_m *OutNozzle) SetConnector(connector common.Connector) error {
	ret := _m.Called(connector)

	if len(ret) == 0 {
		panic("no return value specified for SetConnector")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(common.Connector) error); ok {
		r0 = rf(connector)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// OutNozzle_SetConnector_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetConnector'
type OutNozzle_SetConnector_Call struct {
	*mock.Call
}

// SetConnector is a helper method to define mock.On call
//   - connector common.Connector
func (_e *OutNozzle_Expecter) SetConnector(connector interface{}) *OutNozzle_SetConnector_Call {
	return &OutNozzle_SetConnector_Call{Call: _e.mock.On("SetConnector", connector)}
}

func (_c *OutNozzle_SetConnector_Call) Run(run func(connector common.Connector)) *OutNozzle_SetConnector_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(common.Connector))
	})
	return _c
}

func (_c *OutNozzle_SetConnector_Call) Return(_a0 error) *OutNozzle_SetConnector_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_SetConnector_Call) RunAndReturn(run func(common.Connector) error) *OutNozzle_SetConnector_Call {
	_c.Call.Return(run)
	return _c
}

// SetUpstreamErrReporter provides a mock function with given fields: _a0
func (_m *OutNozzle) SetUpstreamErrReporter(_a0 func(interface{})) {
	_m.Called(_a0)
}

// OutNozzle_SetUpstreamErrReporter_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetUpstreamErrReporter'
type OutNozzle_SetUpstreamErrReporter_Call struct {
	*mock.Call
}

// SetUpstreamErrReporter is a helper method to define mock.On call
//   - _a0 func(interface{})
func (_e *OutNozzle_Expecter) SetUpstreamErrReporter(_a0 interface{}) *OutNozzle_SetUpstreamErrReporter_Call {
	return &OutNozzle_SetUpstreamErrReporter_Call{Call: _e.mock.On("SetUpstreamErrReporter", _a0)}
}

func (_c *OutNozzle_SetUpstreamErrReporter_Call) Run(run func(_a0 func(interface{}))) *OutNozzle_SetUpstreamErrReporter_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(func(interface{})))
	})
	return _c
}

func (_c *OutNozzle_SetUpstreamErrReporter_Call) Return() *OutNozzle_SetUpstreamErrReporter_Call {
	_c.Call.Return()
	return _c
}

func (_c *OutNozzle_SetUpstreamErrReporter_Call) RunAndReturn(run func(func(interface{}))) *OutNozzle_SetUpstreamErrReporter_Call {
	_c.Run(run)
	return _c
}

// SetUpstreamObjRecycler provides a mock function with given fields: _a0
func (_m *OutNozzle) SetUpstreamObjRecycler(_a0 func(interface{})) {
	_m.Called(_a0)
}

// OutNozzle_SetUpstreamObjRecycler_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetUpstreamObjRecycler'
type OutNozzle_SetUpstreamObjRecycler_Call struct {
	*mock.Call
}

// SetUpstreamObjRecycler is a helper method to define mock.On call
//   - _a0 func(interface{})
func (_e *OutNozzle_Expecter) SetUpstreamObjRecycler(_a0 interface{}) *OutNozzle_SetUpstreamObjRecycler_Call {
	return &OutNozzle_SetUpstreamObjRecycler_Call{Call: _e.mock.On("SetUpstreamObjRecycler", _a0)}
}

func (_c *OutNozzle_SetUpstreamObjRecycler_Call) Run(run func(_a0 func(interface{}))) *OutNozzle_SetUpstreamObjRecycler_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(func(interface{})))
	})
	return _c
}

func (_c *OutNozzle_SetUpstreamObjRecycler_Call) Return() *OutNozzle_SetUpstreamObjRecycler_Call {
	_c.Call.Return()
	return _c
}

func (_c *OutNozzle_SetUpstreamObjRecycler_Call) RunAndReturn(run func(func(interface{}))) *OutNozzle_SetUpstreamObjRecycler_Call {
	_c.Run(run)
	return _c
}

// Start provides a mock function with given fields: settings
func (_m *OutNozzle) Start(settings metadata.ReplicationSettingsMap) error {
	ret := _m.Called(settings)

	if len(ret) == 0 {
		panic("no return value specified for Start")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.ReplicationSettingsMap) error); ok {
		r0 = rf(settings)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// OutNozzle_Start_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Start'
type OutNozzle_Start_Call struct {
	*mock.Call
}

// Start is a helper method to define mock.On call
//   - settings metadata.ReplicationSettingsMap
func (_e *OutNozzle_Expecter) Start(settings interface{}) *OutNozzle_Start_Call {
	return &OutNozzle_Start_Call{Call: _e.mock.On("Start", settings)}
}

func (_c *OutNozzle_Start_Call) Run(run func(settings metadata.ReplicationSettingsMap)) *OutNozzle_Start_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(metadata.ReplicationSettingsMap))
	})
	return _c
}

func (_c *OutNozzle_Start_Call) Return(_a0 error) *OutNozzle_Start_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_Start_Call) RunAndReturn(run func(metadata.ReplicationSettingsMap) error) *OutNozzle_Start_Call {
	_c.Call.Return(run)
	return _c
}

// State provides a mock function with no fields
func (_m *OutNozzle) State() common.PartState {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for State")
	}

	var r0 common.PartState
	if rf, ok := ret.Get(0).(func() common.PartState); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(common.PartState)
	}

	return r0
}

// OutNozzle_State_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'State'
type OutNozzle_State_Call struct {
	*mock.Call
}

// State is a helper method to define mock.On call
func (_e *OutNozzle_Expecter) State() *OutNozzle_State_Call {
	return &OutNozzle_State_Call{Call: _e.mock.On("State")}
}

func (_c *OutNozzle_State_Call) Run(run func()) *OutNozzle_State_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *OutNozzle_State_Call) Return(_a0 common.PartState) *OutNozzle_State_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_State_Call) RunAndReturn(run func() common.PartState) *OutNozzle_State_Call {
	_c.Call.Return(run)
	return _c
}

// Stop provides a mock function with no fields
func (_m *OutNozzle) Stop() error {
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

// OutNozzle_Stop_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Stop'
type OutNozzle_Stop_Call struct {
	*mock.Call
}

// Stop is a helper method to define mock.On call
func (_e *OutNozzle_Expecter) Stop() *OutNozzle_Stop_Call {
	return &OutNozzle_Stop_Call{Call: _e.mock.On("Stop")}
}

func (_c *OutNozzle_Stop_Call) Run(run func()) *OutNozzle_Stop_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *OutNozzle_Stop_Call) Return(_a0 error) *OutNozzle_Stop_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_Stop_Call) RunAndReturn(run func() error) *OutNozzle_Stop_Call {
	_c.Call.Return(run)
	return _c
}

// UnRegisterComponentEventListener provides a mock function with given fields: eventType, listener
func (_m *OutNozzle) UnRegisterComponentEventListener(eventType common.ComponentEventType, listener common.ComponentEventListener) error {
	ret := _m.Called(eventType, listener)

	if len(ret) == 0 {
		panic("no return value specified for UnRegisterComponentEventListener")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(common.ComponentEventType, common.ComponentEventListener) error); ok {
		r0 = rf(eventType, listener)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// OutNozzle_UnRegisterComponentEventListener_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UnRegisterComponentEventListener'
type OutNozzle_UnRegisterComponentEventListener_Call struct {
	*mock.Call
}

// UnRegisterComponentEventListener is a helper method to define mock.On call
//   - eventType common.ComponentEventType
//   - listener common.ComponentEventListener
func (_e *OutNozzle_Expecter) UnRegisterComponentEventListener(eventType interface{}, listener interface{}) *OutNozzle_UnRegisterComponentEventListener_Call {
	return &OutNozzle_UnRegisterComponentEventListener_Call{Call: _e.mock.On("UnRegisterComponentEventListener", eventType, listener)}
}

func (_c *OutNozzle_UnRegisterComponentEventListener_Call) Run(run func(eventType common.ComponentEventType, listener common.ComponentEventListener)) *OutNozzle_UnRegisterComponentEventListener_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(common.ComponentEventType), args[1].(common.ComponentEventListener))
	})
	return _c
}

func (_c *OutNozzle_UnRegisterComponentEventListener_Call) Return(_a0 error) *OutNozzle_UnRegisterComponentEventListener_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_UnRegisterComponentEventListener_Call) RunAndReturn(run func(common.ComponentEventType, common.ComponentEventListener) error) *OutNozzle_UnRegisterComponentEventListener_Call {
	_c.Call.Return(run)
	return _c
}

// UpdateSettings provides a mock function with given fields: settings
func (_m *OutNozzle) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	ret := _m.Called(settings)

	if len(ret) == 0 {
		panic("no return value specified for UpdateSettings")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.ReplicationSettingsMap) error); ok {
		r0 = rf(settings)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// OutNozzle_UpdateSettings_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UpdateSettings'
type OutNozzle_UpdateSettings_Call struct {
	*mock.Call
}

// UpdateSettings is a helper method to define mock.On call
//   - settings metadata.ReplicationSettingsMap
func (_e *OutNozzle_Expecter) UpdateSettings(settings interface{}) *OutNozzle_UpdateSettings_Call {
	return &OutNozzle_UpdateSettings_Call{Call: _e.mock.On("UpdateSettings", settings)}
}

func (_c *OutNozzle_UpdateSettings_Call) Run(run func(settings metadata.ReplicationSettingsMap)) *OutNozzle_UpdateSettings_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(metadata.ReplicationSettingsMap))
	})
	return _c
}

func (_c *OutNozzle_UpdateSettings_Call) Return(_a0 error) *OutNozzle_UpdateSettings_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *OutNozzle_UpdateSettings_Call) RunAndReturn(run func(metadata.ReplicationSettingsMap) error) *OutNozzle_UpdateSettings_Call {
	_c.Call.Return(run)
	return _c
}

// NewOutNozzle creates a new instance of OutNozzle. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewOutNozzle(t interface {
	mock.TestingT
	Cleanup(func())
}) *OutNozzle {
	mock := &OutNozzle{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
