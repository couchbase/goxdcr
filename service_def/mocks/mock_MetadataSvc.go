// Code generated by mockery v2.50.0. DO NOT EDIT.

package mocks

import (
	service_def "github.com/couchbase/goxdcr/service_def"
	mock "github.com/stretchr/testify/mock"
)

// MetadataSvc is an autogenerated mock type for the MetadataSvc type
type MetadataSvc struct {
	mock.Mock
}

type MetadataSvc_Expecter struct {
	mock *mock.Mock
}

func (_m *MetadataSvc) EXPECT() *MetadataSvc_Expecter {
	return &MetadataSvc_Expecter{mock: &_m.Mock}
}

// Add provides a mock function with given fields: key, value
func (_m *MetadataSvc) Add(key string, value []byte) error {
	ret := _m.Called(key, value)

	if len(ret) == 0 {
		panic("no return value specified for Add")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []byte) error); ok {
		r0 = rf(key, value)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MetadataSvc_Add_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Add'
type MetadataSvc_Add_Call struct {
	*mock.Call
}

// Add is a helper method to define mock.On call
//   - key string
//   - value []byte
func (_e *MetadataSvc_Expecter) Add(key interface{}, value interface{}) *MetadataSvc_Add_Call {
	return &MetadataSvc_Add_Call{Call: _e.mock.On("Add", key, value)}
}

func (_c *MetadataSvc_Add_Call) Run(run func(key string, value []byte)) *MetadataSvc_Add_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].([]byte))
	})
	return _c
}

func (_c *MetadataSvc_Add_Call) Return(_a0 error) *MetadataSvc_Add_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MetadataSvc_Add_Call) RunAndReturn(run func(string, []byte) error) *MetadataSvc_Add_Call {
	_c.Call.Return(run)
	return _c
}

// AddSensitive provides a mock function with given fields: key, value
func (_m *MetadataSvc) AddSensitive(key string, value []byte) error {
	ret := _m.Called(key, value)

	if len(ret) == 0 {
		panic("no return value specified for AddSensitive")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []byte) error); ok {
		r0 = rf(key, value)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MetadataSvc_AddSensitive_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AddSensitive'
type MetadataSvc_AddSensitive_Call struct {
	*mock.Call
}

// AddSensitive is a helper method to define mock.On call
//   - key string
//   - value []byte
func (_e *MetadataSvc_Expecter) AddSensitive(key interface{}, value interface{}) *MetadataSvc_AddSensitive_Call {
	return &MetadataSvc_AddSensitive_Call{Call: _e.mock.On("AddSensitive", key, value)}
}

func (_c *MetadataSvc_AddSensitive_Call) Run(run func(key string, value []byte)) *MetadataSvc_AddSensitive_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].([]byte))
	})
	return _c
}

func (_c *MetadataSvc_AddSensitive_Call) Return(_a0 error) *MetadataSvc_AddSensitive_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MetadataSvc_AddSensitive_Call) RunAndReturn(run func(string, []byte) error) *MetadataSvc_AddSensitive_Call {
	_c.Call.Return(run)
	return _c
}

// AddSensitiveWithCatalog provides a mock function with given fields: catalogKey, key, value
func (_m *MetadataSvc) AddSensitiveWithCatalog(catalogKey string, key string, value []byte) error {
	ret := _m.Called(catalogKey, key, value)

	if len(ret) == 0 {
		panic("no return value specified for AddSensitiveWithCatalog")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, []byte) error); ok {
		r0 = rf(catalogKey, key, value)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MetadataSvc_AddSensitiveWithCatalog_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AddSensitiveWithCatalog'
type MetadataSvc_AddSensitiveWithCatalog_Call struct {
	*mock.Call
}

// AddSensitiveWithCatalog is a helper method to define mock.On call
//   - catalogKey string
//   - key string
//   - value []byte
func (_e *MetadataSvc_Expecter) AddSensitiveWithCatalog(catalogKey interface{}, key interface{}, value interface{}) *MetadataSvc_AddSensitiveWithCatalog_Call {
	return &MetadataSvc_AddSensitiveWithCatalog_Call{Call: _e.mock.On("AddSensitiveWithCatalog", catalogKey, key, value)}
}

func (_c *MetadataSvc_AddSensitiveWithCatalog_Call) Run(run func(catalogKey string, key string, value []byte)) *MetadataSvc_AddSensitiveWithCatalog_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string), args[2].([]byte))
	})
	return _c
}

func (_c *MetadataSvc_AddSensitiveWithCatalog_Call) Return(_a0 error) *MetadataSvc_AddSensitiveWithCatalog_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MetadataSvc_AddSensitiveWithCatalog_Call) RunAndReturn(run func(string, string, []byte) error) *MetadataSvc_AddSensitiveWithCatalog_Call {
	_c.Call.Return(run)
	return _c
}

// AddWithCatalog provides a mock function with given fields: catalogKey, key, value
func (_m *MetadataSvc) AddWithCatalog(catalogKey string, key string, value []byte) error {
	ret := _m.Called(catalogKey, key, value)

	if len(ret) == 0 {
		panic("no return value specified for AddWithCatalog")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, []byte) error); ok {
		r0 = rf(catalogKey, key, value)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MetadataSvc_AddWithCatalog_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AddWithCatalog'
type MetadataSvc_AddWithCatalog_Call struct {
	*mock.Call
}

// AddWithCatalog is a helper method to define mock.On call
//   - catalogKey string
//   - key string
//   - value []byte
func (_e *MetadataSvc_Expecter) AddWithCatalog(catalogKey interface{}, key interface{}, value interface{}) *MetadataSvc_AddWithCatalog_Call {
	return &MetadataSvc_AddWithCatalog_Call{Call: _e.mock.On("AddWithCatalog", catalogKey, key, value)}
}

func (_c *MetadataSvc_AddWithCatalog_Call) Run(run func(catalogKey string, key string, value []byte)) *MetadataSvc_AddWithCatalog_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string), args[2].([]byte))
	})
	return _c
}

func (_c *MetadataSvc_AddWithCatalog_Call) Return(_a0 error) *MetadataSvc_AddWithCatalog_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MetadataSvc_AddWithCatalog_Call) RunAndReturn(run func(string, string, []byte) error) *MetadataSvc_AddWithCatalog_Call {
	_c.Call.Return(run)
	return _c
}

// Del provides a mock function with given fields: key, rev
func (_m *MetadataSvc) Del(key string, rev interface{}) error {
	ret := _m.Called(key, rev)

	if len(ret) == 0 {
		panic("no return value specified for Del")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, interface{}) error); ok {
		r0 = rf(key, rev)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MetadataSvc_Del_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Del'
type MetadataSvc_Del_Call struct {
	*mock.Call
}

// Del is a helper method to define mock.On call
//   - key string
//   - rev interface{}
func (_e *MetadataSvc_Expecter) Del(key interface{}, rev interface{}) *MetadataSvc_Del_Call {
	return &MetadataSvc_Del_Call{Call: _e.mock.On("Del", key, rev)}
}

func (_c *MetadataSvc_Del_Call) Run(run func(key string, rev interface{})) *MetadataSvc_Del_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(interface{}))
	})
	return _c
}

func (_c *MetadataSvc_Del_Call) Return(_a0 error) *MetadataSvc_Del_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MetadataSvc_Del_Call) RunAndReturn(run func(string, interface{}) error) *MetadataSvc_Del_Call {
	_c.Call.Return(run)
	return _c
}

// DelAllFromCatalog provides a mock function with given fields: catalogKey
func (_m *MetadataSvc) DelAllFromCatalog(catalogKey string) error {
	ret := _m.Called(catalogKey)

	if len(ret) == 0 {
		panic("no return value specified for DelAllFromCatalog")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(catalogKey)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MetadataSvc_DelAllFromCatalog_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DelAllFromCatalog'
type MetadataSvc_DelAllFromCatalog_Call struct {
	*mock.Call
}

// DelAllFromCatalog is a helper method to define mock.On call
//   - catalogKey string
func (_e *MetadataSvc_Expecter) DelAllFromCatalog(catalogKey interface{}) *MetadataSvc_DelAllFromCatalog_Call {
	return &MetadataSvc_DelAllFromCatalog_Call{Call: _e.mock.On("DelAllFromCatalog", catalogKey)}
}

func (_c *MetadataSvc_DelAllFromCatalog_Call) Run(run func(catalogKey string)) *MetadataSvc_DelAllFromCatalog_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *MetadataSvc_DelAllFromCatalog_Call) Return(_a0 error) *MetadataSvc_DelAllFromCatalog_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MetadataSvc_DelAllFromCatalog_Call) RunAndReturn(run func(string) error) *MetadataSvc_DelAllFromCatalog_Call {
	_c.Call.Return(run)
	return _c
}

// DelWithCatalog provides a mock function with given fields: catalogKey, key, rev
func (_m *MetadataSvc) DelWithCatalog(catalogKey string, key string, rev interface{}) error {
	ret := _m.Called(catalogKey, key, rev)

	if len(ret) == 0 {
		panic("no return value specified for DelWithCatalog")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, interface{}) error); ok {
		r0 = rf(catalogKey, key, rev)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MetadataSvc_DelWithCatalog_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DelWithCatalog'
type MetadataSvc_DelWithCatalog_Call struct {
	*mock.Call
}

// DelWithCatalog is a helper method to define mock.On call
//   - catalogKey string
//   - key string
//   - rev interface{}
func (_e *MetadataSvc_Expecter) DelWithCatalog(catalogKey interface{}, key interface{}, rev interface{}) *MetadataSvc_DelWithCatalog_Call {
	return &MetadataSvc_DelWithCatalog_Call{Call: _e.mock.On("DelWithCatalog", catalogKey, key, rev)}
}

func (_c *MetadataSvc_DelWithCatalog_Call) Run(run func(catalogKey string, key string, rev interface{})) *MetadataSvc_DelWithCatalog_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string), args[2].(interface{}))
	})
	return _c
}

func (_c *MetadataSvc_DelWithCatalog_Call) Return(_a0 error) *MetadataSvc_DelWithCatalog_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MetadataSvc_DelWithCatalog_Call) RunAndReturn(run func(string, string, interface{}) error) *MetadataSvc_DelWithCatalog_Call {
	_c.Call.Return(run)
	return _c
}

// Get provides a mock function with given fields: key
func (_m *MetadataSvc) Get(key string) ([]byte, interface{}, error) {
	ret := _m.Called(key)

	if len(ret) == 0 {
		panic("no return value specified for Get")
	}

	var r0 []byte
	var r1 interface{}
	var r2 error
	if rf, ok := ret.Get(0).(func(string) ([]byte, interface{}, error)); ok {
		return rf(key)
	}
	if rf, ok := ret.Get(0).(func(string) []byte); ok {
		r0 = rf(key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	if rf, ok := ret.Get(1).(func(string) interface{}); ok {
		r1 = rf(key)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(interface{})
		}
	}

	if rf, ok := ret.Get(2).(func(string) error); ok {
		r2 = rf(key)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// MetadataSvc_Get_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Get'
type MetadataSvc_Get_Call struct {
	*mock.Call
}

// Get is a helper method to define mock.On call
//   - key string
func (_e *MetadataSvc_Expecter) Get(key interface{}) *MetadataSvc_Get_Call {
	return &MetadataSvc_Get_Call{Call: _e.mock.On("Get", key)}
}

func (_c *MetadataSvc_Get_Call) Run(run func(key string)) *MetadataSvc_Get_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *MetadataSvc_Get_Call) Return(_a0 []byte, _a1 interface{}, _a2 error) *MetadataSvc_Get_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *MetadataSvc_Get_Call) RunAndReturn(run func(string) ([]byte, interface{}, error)) *MetadataSvc_Get_Call {
	_c.Call.Return(run)
	return _c
}

// GetAllKeysFromCatalog provides a mock function with given fields: catalogKey
func (_m *MetadataSvc) GetAllKeysFromCatalog(catalogKey string) ([]string, error) {
	ret := _m.Called(catalogKey)

	if len(ret) == 0 {
		panic("no return value specified for GetAllKeysFromCatalog")
	}

	var r0 []string
	var r1 error
	if rf, ok := ret.Get(0).(func(string) ([]string, error)); ok {
		return rf(catalogKey)
	}
	if rf, ok := ret.Get(0).(func(string) []string); ok {
		r0 = rf(catalogKey)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(catalogKey)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MetadataSvc_GetAllKeysFromCatalog_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetAllKeysFromCatalog'
type MetadataSvc_GetAllKeysFromCatalog_Call struct {
	*mock.Call
}

// GetAllKeysFromCatalog is a helper method to define mock.On call
//   - catalogKey string
func (_e *MetadataSvc_Expecter) GetAllKeysFromCatalog(catalogKey interface{}) *MetadataSvc_GetAllKeysFromCatalog_Call {
	return &MetadataSvc_GetAllKeysFromCatalog_Call{Call: _e.mock.On("GetAllKeysFromCatalog", catalogKey)}
}

func (_c *MetadataSvc_GetAllKeysFromCatalog_Call) Run(run func(catalogKey string)) *MetadataSvc_GetAllKeysFromCatalog_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *MetadataSvc_GetAllKeysFromCatalog_Call) Return(_a0 []string, _a1 error) *MetadataSvc_GetAllKeysFromCatalog_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MetadataSvc_GetAllKeysFromCatalog_Call) RunAndReturn(run func(string) ([]string, error)) *MetadataSvc_GetAllKeysFromCatalog_Call {
	_c.Call.Return(run)
	return _c
}

// GetAllMetadataFromCatalog provides a mock function with given fields: catalogKey
func (_m *MetadataSvc) GetAllMetadataFromCatalog(catalogKey string) ([]*service_def.MetadataEntry, error) {
	ret := _m.Called(catalogKey)

	if len(ret) == 0 {
		panic("no return value specified for GetAllMetadataFromCatalog")
	}

	var r0 []*service_def.MetadataEntry
	var r1 error
	if rf, ok := ret.Get(0).(func(string) ([]*service_def.MetadataEntry, error)); ok {
		return rf(catalogKey)
	}
	if rf, ok := ret.Get(0).(func(string) []*service_def.MetadataEntry); ok {
		r0 = rf(catalogKey)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*service_def.MetadataEntry)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(catalogKey)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MetadataSvc_GetAllMetadataFromCatalog_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetAllMetadataFromCatalog'
type MetadataSvc_GetAllMetadataFromCatalog_Call struct {
	*mock.Call
}

// GetAllMetadataFromCatalog is a helper method to define mock.On call
//   - catalogKey string
func (_e *MetadataSvc_Expecter) GetAllMetadataFromCatalog(catalogKey interface{}) *MetadataSvc_GetAllMetadataFromCatalog_Call {
	return &MetadataSvc_GetAllMetadataFromCatalog_Call{Call: _e.mock.On("GetAllMetadataFromCatalog", catalogKey)}
}

func (_c *MetadataSvc_GetAllMetadataFromCatalog_Call) Run(run func(catalogKey string)) *MetadataSvc_GetAllMetadataFromCatalog_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *MetadataSvc_GetAllMetadataFromCatalog_Call) Return(_a0 []*service_def.MetadataEntry, _a1 error) *MetadataSvc_GetAllMetadataFromCatalog_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MetadataSvc_GetAllMetadataFromCatalog_Call) RunAndReturn(run func(string) ([]*service_def.MetadataEntry, error)) *MetadataSvc_GetAllMetadataFromCatalog_Call {
	_c.Call.Return(run)
	return _c
}

// Set provides a mock function with given fields: key, value, rev
func (_m *MetadataSvc) Set(key string, value []byte, rev interface{}) error {
	ret := _m.Called(key, value, rev)

	if len(ret) == 0 {
		panic("no return value specified for Set")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []byte, interface{}) error); ok {
		r0 = rf(key, value, rev)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MetadataSvc_Set_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Set'
type MetadataSvc_Set_Call struct {
	*mock.Call
}

// Set is a helper method to define mock.On call
//   - key string
//   - value []byte
//   - rev interface{}
func (_e *MetadataSvc_Expecter) Set(key interface{}, value interface{}, rev interface{}) *MetadataSvc_Set_Call {
	return &MetadataSvc_Set_Call{Call: _e.mock.On("Set", key, value, rev)}
}

func (_c *MetadataSvc_Set_Call) Run(run func(key string, value []byte, rev interface{})) *MetadataSvc_Set_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].([]byte), args[2].(interface{}))
	})
	return _c
}

func (_c *MetadataSvc_Set_Call) Return(_a0 error) *MetadataSvc_Set_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MetadataSvc_Set_Call) RunAndReturn(run func(string, []byte, interface{}) error) *MetadataSvc_Set_Call {
	_c.Call.Return(run)
	return _c
}

// SetSensitive provides a mock function with given fields: key, value, rev
func (_m *MetadataSvc) SetSensitive(key string, value []byte, rev interface{}) error {
	ret := _m.Called(key, value, rev)

	if len(ret) == 0 {
		panic("no return value specified for SetSensitive")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []byte, interface{}) error); ok {
		r0 = rf(key, value, rev)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MetadataSvc_SetSensitive_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetSensitive'
type MetadataSvc_SetSensitive_Call struct {
	*mock.Call
}

// SetSensitive is a helper method to define mock.On call
//   - key string
//   - value []byte
//   - rev interface{}
func (_e *MetadataSvc_Expecter) SetSensitive(key interface{}, value interface{}, rev interface{}) *MetadataSvc_SetSensitive_Call {
	return &MetadataSvc_SetSensitive_Call{Call: _e.mock.On("SetSensitive", key, value, rev)}
}

func (_c *MetadataSvc_SetSensitive_Call) Run(run func(key string, value []byte, rev interface{})) *MetadataSvc_SetSensitive_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].([]byte), args[2].(interface{}))
	})
	return _c
}

func (_c *MetadataSvc_SetSensitive_Call) Return(_a0 error) *MetadataSvc_SetSensitive_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MetadataSvc_SetSensitive_Call) RunAndReturn(run func(string, []byte, interface{}) error) *MetadataSvc_SetSensitive_Call {
	_c.Call.Return(run)
	return _c
}

// NewMetadataSvc creates a new instance of MetadataSvc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMetadataSvc(t interface {
	mock.TestingT
	Cleanup(func())
}) *MetadataSvc {
	mock := &MetadataSvc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
