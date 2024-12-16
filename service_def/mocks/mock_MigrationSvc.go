// Code generated by mockery v2.50.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// MigrationSvc is an autogenerated mock type for the MigrationSvc type
type MigrationSvc struct {
	mock.Mock
}

type MigrationSvc_Expecter struct {
	mock *mock.Mock
}

func (_m *MigrationSvc) EXPECT() *MigrationSvc_Expecter {
	return &MigrationSvc_Expecter{mock: &_m.Mock}
}

// Migrate provides a mock function with no fields
func (_m *MigrationSvc) Migrate() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Migrate")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MigrationSvc_Migrate_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Migrate'
type MigrationSvc_Migrate_Call struct {
	*mock.Call
}

// Migrate is a helper method to define mock.On call
func (_e *MigrationSvc_Expecter) Migrate() *MigrationSvc_Migrate_Call {
	return &MigrationSvc_Migrate_Call{Call: _e.mock.On("Migrate")}
}

func (_c *MigrationSvc_Migrate_Call) Run(run func()) *MigrationSvc_Migrate_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MigrationSvc_Migrate_Call) Return(_a0 error) *MigrationSvc_Migrate_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MigrationSvc_Migrate_Call) RunAndReturn(run func() error) *MigrationSvc_Migrate_Call {
	_c.Call.Return(run)
	return _c
}

// NewMigrationSvc creates a new instance of MigrationSvc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMigrationSvc(t interface {
	mock.TestingT
	Cleanup(func())
}) *MigrationSvc {
	mock := &MigrationSvc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
