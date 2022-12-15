// Code generated by mockery v2.14.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// ExponentialOpFunc2 is an autogenerated mock type for the ExponentialOpFunc2 type
type ExponentialOpFunc2 struct {
	mock.Mock
}

// Execute provides a mock function with given fields: _a0
func (_m *ExponentialOpFunc2) Execute(_a0 interface{}) (interface{}, error) {
	ret := _m.Called(_a0)

	var r0 interface{}
	if rf, ok := ret.Get(0).(func(interface{}) interface{}); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(interface{}) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewExponentialOpFunc2 interface {
	mock.TestingT
	Cleanup(func())
}

// NewExponentialOpFunc2 creates a new instance of ExponentialOpFunc2. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewExponentialOpFunc2(t mockConstructorTestingTNewExponentialOpFunc2) *ExponentialOpFunc2 {
	mock := &ExponentialOpFunc2{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
