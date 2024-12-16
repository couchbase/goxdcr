// Code generated by mockery v2.50.0. DO NOT EDIT.

package mocks

import (
	common "github.com/couchbase/goxdcr/common"
	mock "github.com/stretchr/testify/mock"

	peerToPeer "github.com/couchbase/goxdcr/peerToPeer"
)

// VBMasterCheck is an autogenerated mock type for the VBMasterCheck type
type VBMasterCheck struct {
	mock.Mock
}

type VBMasterCheck_Expecter struct {
	mock *mock.Mock
}

func (_m *VBMasterCheck) EXPECT() *VBMasterCheck_Expecter {
	return &VBMasterCheck_Expecter{mock: &_m.Mock}
}

// CheckVBMaster provides a mock function with given fields: _a0, _a1
func (_m *VBMasterCheck) CheckVBMaster(_a0 peerToPeer.BucketVBMapType, _a1 common.Pipeline) (map[string]*peerToPeer.VBMasterCheckResp, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for CheckVBMaster")
	}

	var r0 map[string]*peerToPeer.VBMasterCheckResp
	var r1 error
	if rf, ok := ret.Get(0).(func(peerToPeer.BucketVBMapType, common.Pipeline) (map[string]*peerToPeer.VBMasterCheckResp, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(peerToPeer.BucketVBMapType, common.Pipeline) map[string]*peerToPeer.VBMasterCheckResp); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]*peerToPeer.VBMasterCheckResp)
		}
	}

	if rf, ok := ret.Get(1).(func(peerToPeer.BucketVBMapType, common.Pipeline) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// VBMasterCheck_CheckVBMaster_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CheckVBMaster'
type VBMasterCheck_CheckVBMaster_Call struct {
	*mock.Call
}

// CheckVBMaster is a helper method to define mock.On call
//   - _a0 peerToPeer.BucketVBMapType
//   - _a1 common.Pipeline
func (_e *VBMasterCheck_Expecter) CheckVBMaster(_a0 interface{}, _a1 interface{}) *VBMasterCheck_CheckVBMaster_Call {
	return &VBMasterCheck_CheckVBMaster_Call{Call: _e.mock.On("CheckVBMaster", _a0, _a1)}
}

func (_c *VBMasterCheck_CheckVBMaster_Call) Run(run func(_a0 peerToPeer.BucketVBMapType, _a1 common.Pipeline)) *VBMasterCheck_CheckVBMaster_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(peerToPeer.BucketVBMapType), args[1].(common.Pipeline))
	})
	return _c
}

func (_c *VBMasterCheck_CheckVBMaster_Call) Return(_a0 map[string]*peerToPeer.VBMasterCheckResp, _a1 error) *VBMasterCheck_CheckVBMaster_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *VBMasterCheck_CheckVBMaster_Call) RunAndReturn(run func(peerToPeer.BucketVBMapType, common.Pipeline) (map[string]*peerToPeer.VBMasterCheckResp, error)) *VBMasterCheck_CheckVBMaster_Call {
	_c.Call.Return(run)
	return _c
}

// NewVBMasterCheck creates a new instance of VBMasterCheck. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewVBMasterCheck(t interface {
	mock.TestingT
	Cleanup(func())
}) *VBMasterCheck {
	mock := &VBMasterCheck{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
