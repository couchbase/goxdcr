package cng

import (
	"context"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"google.golang.org/grpc"
)

var _ internal_xdcr_v1.XdcrServiceClient = (*MockCngXdcrClient)(nil)

// MockCngXdcrClient is a mock implementation of XDCRClient for testing purposes
type MockCngXdcrClient struct {
	CheckDocRsp         CheckDocumentRsp
	PushDocRsp          PushDocumentRsp
	GetBucketInfoRsp    GetBucketInfoRsp
	GetClusterInfoRsp   GetClusterInfoRsp
	GetDocRsp           GetDocumentRsp
	GetVbucketInfoRsp   GetVbucketInfoRsp
	HeartbeatRsp        HeartbeatRsp
	WatchCollectionsRsp WatchCollectionsRsp
}

type CheckDocumentRsp struct {
	Err error
	Rsp internal_xdcr_v1.CheckDocumentResponse
}

func (m *MockCngXdcrClient) CheckDocument(ctx context.Context, in *internal_xdcr_v1.CheckDocumentRequest, opts ...grpc.CallOption) (*internal_xdcr_v1.CheckDocumentResponse, error) {
	return &m.CheckDocRsp.Rsp, m.CheckDocRsp.Err
}

type PushDocumentRsp struct {
	Err error
	Rsp internal_xdcr_v1.PushDocumentResponse
}

func (m *MockCngXdcrClient) PushDocument(ctx context.Context, in *internal_xdcr_v1.PushDocumentRequest, opts ...grpc.CallOption) (*internal_xdcr_v1.PushDocumentResponse, error) {
	return &m.PushDocRsp.Rsp, m.PushDocRsp.Err
}

type GetBucketInfoRsp struct {
	Err error
	Rsp internal_xdcr_v1.GetBucketInfoResponse
}

func (m *MockCngXdcrClient) GetBucketInfo(ctx context.Context, in *internal_xdcr_v1.GetBucketInfoRequest, opts ...grpc.CallOption) (*internal_xdcr_v1.GetBucketInfoResponse, error) {
	return &m.GetBucketInfoRsp.Rsp, m.GetBucketInfoRsp.Err
}

type GetClusterInfoRsp struct {
	Err error
	Rsp internal_xdcr_v1.GetClusterInfoResponse
}

func (m *MockCngXdcrClient) GetClusterInfo(ctx context.Context, in *internal_xdcr_v1.GetClusterInfoRequest, opts ...grpc.CallOption) (*internal_xdcr_v1.GetClusterInfoResponse, error) {
	return &m.GetClusterInfoRsp.Rsp, m.GetClusterInfoRsp.Err
}

type GetDocumentRsp struct {
	Err error
	Rsp internal_xdcr_v1.GetDocumentResponse
}

func (m *MockCngXdcrClient) GetDocument(ctx context.Context, in *internal_xdcr_v1.GetDocumentRequest, opts ...grpc.CallOption) (*internal_xdcr_v1.GetDocumentResponse, error) {
	return &m.GetDocRsp.Rsp, m.GetDocRsp.Err
}

type GetVbucketInfoRsp struct {
	Err    error
	Stream grpc.ServerStreamingClient[internal_xdcr_v1.GetVbucketInfoResponse]
}

func (m *MockCngXdcrClient) GetVbucketInfo(ctx context.Context, in *internal_xdcr_v1.GetVbucketInfoRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[internal_xdcr_v1.GetVbucketInfoResponse], error) {
	return m.GetVbucketInfoRsp.Stream, m.GetVbucketInfoRsp.Err
}

type HeartbeatRsp struct {
	Err error
	Rsp internal_xdcr_v1.HeartbeatResponse
}

func (m *MockCngXdcrClient) Heartbeat(ctx context.Context, in *internal_xdcr_v1.HeartbeatRequest, opts ...grpc.CallOption) (*internal_xdcr_v1.HeartbeatResponse, error) {
	return &m.HeartbeatRsp.Rsp, m.HeartbeatRsp.Err
}

type WatchCollectionsRsp struct {
	Err    error
	Stream grpc.ServerStreamingClient[internal_xdcr_v1.WatchCollectionsResponse]
}

func (m *MockCngXdcrClient) WatchCollections(ctx context.Context, in *internal_xdcr_v1.WatchCollectionsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[internal_xdcr_v1.WatchCollectionsResponse], error) {
	return m.WatchCollectionsRsp.Stream, m.WatchCollectionsRsp.Err
}
