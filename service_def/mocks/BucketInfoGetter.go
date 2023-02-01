// Code generated by mockery v2.14.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// BucketInfoGetter is an autogenerated mock type for the BucketInfoGetter type
type BucketInfoGetter struct {
	mock.Mock
}

// Execute provides a mock function with given fields:
func (_m *BucketInfoGetter) Execute() (map[string]interface{}, bool, string, error) {
	ret := _m.Called()

	var r0 map[string]interface{}
	if rf, ok := ret.Get(0).(func() map[string]interface{}); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]interface{})
		}
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func() bool); ok {
		r1 = rf()
	} else {
		r1 = ret.Get(1).(bool)
	}

	var r2 string
	if rf, ok := ret.Get(2).(func() string); ok {
		r2 = rf()
	} else {
		r2 = ret.Get(2).(string)
	}

	var r3 error
	if rf, ok := ret.Get(3).(func() error); ok {
		r3 = rf()
	} else {
		r3 = ret.Error(3)
	}

	return r0, r1, r2, r3
}

type mockConstructorTestingTNewBucketInfoGetter interface {
	mock.TestingT
	Cleanup(func())
}

// NewBucketInfoGetter creates a new instance of BucketInfoGetter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewBucketInfoGetter(t mockConstructorTestingTNewBucketInfoGetter) *BucketInfoGetter {
	mock := &BucketInfoGetter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}