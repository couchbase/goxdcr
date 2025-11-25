package utils

import (
	"context"
	"fmt"
	"io"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"google.golang.org/grpc"
)

// CngGetClusterInfo fetches cluster information from the target couchbase cluster via CNG
func (u *Utilities) CngGetClusterInfo(client base.CngClient, request *base.GrpcRequest[*internal_xdcr_v1.GetClusterInfoRequest]) *base.GrpcResponse[*internal_xdcr_v1.GetClusterInfoResponse] {
	return grpcCall(request, client.GetClusterInfo)
}

// CngGetBucketInfo fetches bucket information from the target couchbase cluster via CNG
func (u *Utilities) CngGetBucketInfo(client base.CngClient, request *base.GrpcRequest[*internal_xdcr_v1.GetBucketInfoRequest]) *base.GrpcResponse[*internal_xdcr_v1.GetBucketInfoResponse] {
	return grpcCall(request, client.GetBucketInfo)
}

// CngHeartbeat sends a heartbeat to the target couchbase cluster via CNG
func (u *Utilities) CngHeartbeat(client base.CngClient, request *base.GrpcRequest[*internal_xdcr_v1.HeartbeatRequest]) *base.GrpcResponse[*internal_xdcr_v1.HeartbeatResponse] {
	return grpcCall(request, client.Heartbeat)
}

func (u *Utilities) CngGetVbucketInfo(client base.CngClient, request *base.GrpcRequest[*internal_xdcr_v1.GetVbucketInfoRequest], handler GrpcStreamHandler[*internal_xdcr_v1.GetVbucketInfoResponse]) {
	grpcServerStreamCall(request, handler, client.GetVbucketInfo)
}

func (u *Utilities) CngWatchCollections(client base.CngClient, request *base.GrpcRequest[*internal_xdcr_v1.WatchCollectionsRequest], handler GrpcStreamHandler[*internal_xdcr_v1.WatchCollectionsResponse]) {
	grpcServerStreamCall(request, handler, client.WatchCollections)
}

// CngGetVbucketInfoOnce is a helper function that wraps the streaming RPC GetVbucketInfo call
// to return the first response message only.
func (u *Utilities) CngGetVbucketInfoOnce(ctx context.Context, c base.CngClient, req *internal_xdcr_v1.GetVbucketInfoRequest, opts ...grpc.CallOption) (rsp map[uint16]*internal_xdcr_v1.GetVbucketInfoResponse, err error) {
	stream, err := c.GetVbucketInfo(ctx, req, opts...)
	if err != nil {
		return
	}

	rsp = make(map[uint16]*internal_xdcr_v1.GetVbucketInfoResponse)

	for range len(req.VbucketIds) {
		var msg internal_xdcr_v1.GetVbucketInfoResponse
		err = stream.RecvMsg(&msg)
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}

		rsp[uint16(msg.Vbuckets[0].VbucketId)] = &msg
	}

	if len(rsp) != len(req.VbucketIds) {
		err = fmt.Errorf("expected %d responses, but got %d", len(req.VbucketIds), len(rsp))
		return
	}

	return rsp, nil
}

// bucketInfo is assumed to be allocated
func IsBucketInfoFromCng(bucketInfo map[string]interface{}) bool {
	val, ok := bucketInfo[BucketInfoSourceKey]
	if !ok {
		return false
	}

	// We don't need to type check here as the remoteType key
	// is controlled by utils code itself
	remoteType := val.(base.XDCROutgoingNozzleType)
	return remoteType == base.CNG
}

// GetCngKVVBMap extracts the CNG kv vb map from the bucket info
func GetCngKVVBMap(bucketInfo map[string]interface{}) (map[string][]uint16, error) {
	tmp, ok := bucketInfo[BucketInfoCngKvVbMapKey]
	if !ok {
		return nil, fmt.Errorf("unable to find cng kv vb map key")
	}

	kvVbMap, ok := tmp.(map[string][]uint16)
	if !ok {
		return nil, fmt.Errorf("cng kv vb map is of wrong type: %T", tmp)
	}

	return kvVbMap, nil
}

// MapCngConflictResolutionType maps CNG conflict resolution type to base conflict resolution type
func MapCngConflictResolutionType(cngConflictType internal_xdcr_v1.ConflictResolutionType) (out string, err error) {
	switch cngConflictType {
	case internal_xdcr_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_TIMESTAMP:
		out = base.ConflictResolutionType_Lww
	case internal_xdcr_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER:
		out = base.ConflictResolutionType_Seqno
	case internal_xdcr_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_CUSTOM:
		out = base.ConflictResolutionType_Custom
	default:
		err = fmt.Errorf("unknown conflict resolution type from cng: %v", cngConflictType)
	}
	return
}

// GetBucketInfoFromCNG gets bucket info from CNG
// Not
func (u *Utilities) GetBucketInfoFromCNG(logger *log.CommonLogger, req *GetBucketInfoReq) (map[string]interface{}, error) {
	creds := func() *base.Credentials {
		return &base.Credentials{
			UserName_: req.Username,
			Password_: req.Password,
		}
	}
	grpcOptions, err := base.NewGrpcOptions(req.HostAddr, creds, req.Certificate, true)
	if err != nil {
		return nil, err
	}

	conn, err := base.NewCngConn(grpcOptions)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rsp, err := conn.Client().GetBucketInfo(context.Background(), &internal_xdcr_v1.GetBucketInfoRequest{
		BucketName: req.BucketName,
	})
	if err != nil {
		return nil, err
	}

	bucketInfo := make(map[string]interface{})
	bucketInfo[BucketInfoSourceKey] = base.CNG
	bucketInfo[base.UUIDKey] = rsp.BucketUuid
	bucketInfo[base.EnableCrossClusterVersioningKey] = rsp.CrossClusterVersioningEnabled
	bucketInfo[base.NumVBucketsKey] = rsp.NumVbuckets
	bucketInfo[base.ConflictResolutionTypeKey], err = MapCngConflictResolutionType(rsp.ConflictResolutionType)
	if err != nil {
		bucketInfo = nil
		return nil, err
	}

	vblist := make([]uint16, rsp.NumVbuckets)
	for i := range rsp.NumVbuckets {
		vblist[i] = uint16(i)
	}
	bucketInfo[BucketInfoCngKvVbMapKey] = map[string][]uint16{
		req.HostAddr: vblist,
	}

	// CNG TODO: Read bucket type from response
	// Ticket on ING team: https://jira.issues.couchbase.com/browse/ING-1382
	bucketInfo[base.BucketTypeKey] = base.CouchbaseBucketType
	return bucketInfo, nil
}
