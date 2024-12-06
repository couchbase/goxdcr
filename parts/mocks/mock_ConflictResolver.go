// Code generated by mockery v2.43.2. DO NOT EDIT.

package mocks

import (
	base "github.com/couchbase/goxdcr/base"
	log "github.com/couchbase/goxdcr/log"

	mock "github.com/stretchr/testify/mock"
)

// ConflictResolver is an autogenerated mock type for the ConflictResolver type
type ConflictResolver struct {
	mock.Mock
}

type ConflictResolver_Expecter struct {
	mock *mock.Mock
}

func (_m *ConflictResolver) EXPECT() *ConflictResolver_Expecter {
	return &ConflictResolver_Expecter{mock: &_m.Mock}
}

// Execute provides a mock function with given fields: doc_metadata_source, doc_metadata_target, source_cr_mode, xattrEnabled, logger
func (_m *ConflictResolver) Execute(doc_metadata_source parts.documentMetadata, doc_metadata_target parts.documentMetadata, source_cr_mode base.ConflictResolutionMode, xattrEnabled bool, logger *log.CommonLogger) bool {
	ret := _m.Called(doc_metadata_source, doc_metadata_target, source_cr_mode, xattrEnabled, logger)

	if len(ret) == 0 {
		panic("no return value specified for Execute")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func(parts.documentMetadata, parts.documentMetadata, base.ConflictResolutionMode, bool, *log.CommonLogger) bool); ok {
		r0 = rf(doc_metadata_source, doc_metadata_target, source_cr_mode, xattrEnabled, logger)
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// ConflictResolver_Execute_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Execute'
type ConflictResolver_Execute_Call struct {
	*mock.Call
}

// Execute is a helper method to define mock.On call
//   - doc_metadata_source parts.documentMetadata
//   - doc_metadata_target parts.documentMetadata
//   - source_cr_mode base.ConflictResolutionMode
//   - xattrEnabled bool
//   - logger *log.CommonLogger
func (_e *ConflictResolver_Expecter) Execute(doc_metadata_source interface{}, doc_metadata_target interface{}, source_cr_mode interface{}, xattrEnabled interface{}, logger interface{}) *ConflictResolver_Execute_Call {
	return &ConflictResolver_Execute_Call{Call: _e.mock.On("Execute", doc_metadata_source, doc_metadata_target, source_cr_mode, xattrEnabled, logger)}
}

func (_c *ConflictResolver_Execute_Call) Run(run func(doc_metadata_source parts.documentMetadata, doc_metadata_target parts.documentMetadata, source_cr_mode base.ConflictResolutionMode, xattrEnabled bool, logger *log.CommonLogger)) *ConflictResolver_Execute_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(parts.documentMetadata), args[1].(parts.documentMetadata), args[2].(base.ConflictResolutionMode), args[3].(bool), args[4].(*log.CommonLogger))
	})
	return _c
}

func (_c *ConflictResolver_Execute_Call) Return(_a0 bool) *ConflictResolver_Execute_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ConflictResolver_Execute_Call) RunAndReturn(run func(parts.documentMetadata, parts.documentMetadata, base.ConflictResolutionMode, bool, *log.CommonLogger) bool) *ConflictResolver_Execute_Call {
	_c.Call.Return(run)
	return _c
}

// NewConflictResolver creates a new instance of ConflictResolver. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewConflictResolver(t interface {
	mock.TestingT
	Cleanup(func())
}) *ConflictResolver {
	mock := &ConflictResolver{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
